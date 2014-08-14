using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using N2.Engine;

// ReSharper disable CheckNamespace
namespace N2.Edit.FileSystem {
// ReSharper restore CheckNamespace 
	[Service(typeof(IFileSystem))]
	public class S3FileSystem : IFileSystem {
		private readonly string _accessKeyId;
		private readonly string _secretAccessKey;
		private readonly AmazonS3 _s3;
		private readonly string _bucketName;
		private const string RootURL = @"https://s3.amazonaws.com/{0}/{1}";
		private const string EmptyFilename = @"__empty";

		public S3FileSystem() {
			_accessKeyId = ConfigurationManager.AppSettings["AWSAccessKeyID"];
			_secretAccessKey = ConfigurationManager.AppSettings["AWSSecretAccessKey"];
			_bucketName = ConfigurationManager.AppSettings["AWSBucketName"];
			_s3 = AWSClientFactory.CreateAmazonS3Client(_accessKeyId, _secretAccessKey);
		}

		#region Implementation of IFileSystem

		public IEnumerable<FileData> GetFiles(string parentVirtualPath) {
			var request = new ListObjectsRequest()
				.WithBucketName(_bucketName)
				.WithPrefix(FixVirtualPath(parentVirtualPath))
				.WithDelimiter(@"/");

			IEnumerable<FileData> files;
			using (var response = _s3.ListObjects(request)) {
				files = response.S3Objects.Where(file => file.Size > 0).Select(file => new FileData {
					VirtualPath = file.Key,
					Name = file.Key.Substring(file.Key.LastIndexOf('/') + 1),
					Length = file.Size,
					Created = DateTime.Parse(file.LastModified),
					Updated = DateTime.Parse(file.LastModified)
				});
			}
			return files;
		}

		public FileData GetFile(string virtualPath) {
			var request = new GetObjectRequest()
				.WithBucketName(_bucketName)
				.WithKey(FixVirtualPath(virtualPath));
			FileData file;
			using (var response = _s3.GetObject(request)) {
				file = new FileData {
					Name = response.Key.Substring(response.Key.LastIndexOf('/') + 1),
					Updated = DateTime.Now,
					Created = DateTime.Now,
					Length = response.ContentLength,
					VirtualPath = response.Key
				};
			}
			return file;
		}

		public IEnumerable<DirectoryData> GetDirectories(string parentVirtualPath) {
			parentVirtualPath = FixVirtualPath(parentVirtualPath);

			var request = new ListObjectsRequest()
				.WithBucketName(_bucketName)
				.WithPrefix(parentVirtualPath)
				.WithDelimiter(@"/");

			IEnumerable<DirectoryData> directories;
			using (var response = _s3.ListObjects(request)) {
				directories = response.CommonPrefixes.Select(dir => new DirectoryData {
					Created = DateTime.Now,
					Updated = DateTime.Now,
					Name = dir.TrimEnd('/').Substring(dir.TrimEnd('/').LastIndexOf('/') + 1),
					VirtualPath = dir
				});
			}
			return directories;
		}

		public DirectoryData GetDirectory(string virtualPath) {
			virtualPath = FixVirtualPath(virtualPath);
			return new DirectoryData {
				Name = string.Format(RootURL, _bucketName, virtualPath),
				VirtualPath = virtualPath,
				Created = DateTime.Now,
				Updated = DateTime.Now
			};
		}

		public bool FileExists(string virtualPath) {
			var request = new GetObjectMetadataRequest()
				.WithBucketName(_bucketName)
				.WithKey(FixVirtualPath(virtualPath));

			try {
				using (_s3.GetObjectMetadata(request)) { }
			} catch (AmazonS3Exception) {
				return false;
			}
			return true;
		}

		public void MoveFile(string fromVirtualPath, string destinationVirtualPath) {
			CopyFile(fromVirtualPath, destinationVirtualPath);
			DeleteFile(fromVirtualPath);
		}

		public void DeleteFile(string virtualPath) {
			var request = new DeleteObjectRequest()
				.WithBucketName(_bucketName)
				.WithKey(FixVirtualPath(virtualPath));

			using (_s3.DeleteObject(request)) { }

			if (FileDeleted != null)
				FileDeleted.Invoke(this, new FileEventArgs(virtualPath, null));
		}

		public void CopyFile(string fromVirtualPath, string destinationVirtualPath) {
			var copyRequest = new CopyObjectRequest()
        .WithMetaData("Expires", DateTime.Now.AddYears(10).ToString("R"))
				.WithSourceBucket(_bucketName)
				.WithSourceKey(fromVirtualPath)
				.WithDestinationBucket(_bucketName)
				.WithDestinationKey(destinationVirtualPath)
				.WithCannedACL(S3CannedACL.PublicRead);

			using (_s3.CopyObject(copyRequest)) { }
		}


		public Stream OpenFile(string virtualPath, bool readOnly = false) {
			var request = new GetObjectRequest()
				.WithBucketName(_bucketName)
				.WithKey(FixVirtualPath(virtualPath));

			var stream = new MemoryStream();

			using (var response = _s3.GetObject(request)) {
				var buffer = new byte[32768];
				while (true) {
					var read = response.ResponseStream.Read(buffer, 0, buffer.Length);
					if (read <= 0)
						break;
					stream.Write(buffer, 0, read);
				}
			}
			return stream;
		}

		public void WriteFile(string virtualPath, Stream inputStream) {
			var request = new PutObjectRequest()
        .WithMetaData("Expires", DateTime.Now.AddYears(10).ToString("R"))
				.WithBucketName(_bucketName)
				.WithCannedACL(S3CannedACL.PublicRead)
        .WithTimeout(60 * 60 * 1000) // 1 hour
        .WithReadWriteTimeout(60 * 60 * 1000) // 1 hour
				.WithKey(FixVirtualPath(virtualPath));

			request.WithInputStream(inputStream);
			using (_s3.PutObject(request)) { }
		}

		public void ReadFileContents(string virtualPath, Stream outputStream) {
			var request = new GetObjectRequest()
				.WithBucketName(_bucketName)
				.WithKey(FixVirtualPath(virtualPath));

			using (var response = _s3.GetObject(request)) {
				var buffer = new byte[32768];
				while (true) {
					var read = response.ResponseStream.Read(buffer, 0, buffer.Length);
					if (read <= 0) break;
					outputStream.Write(buffer, 0, read);
				}
			}
		}

		public bool DirectoryExists(string virtualPath) { // ~/upload/28/
			virtualPath = FixVirtualPath(virtualPath) + EmptyFilename;

			var request = new GetObjectMetadataRequest()
				.WithBucketName(_bucketName)
				.WithKey(virtualPath);

			try {
				using (_s3.GetObjectMetadata(request)) { }
			} catch (AmazonS3Exception) {
				return false;
			}
			return true;
		}

		public void MoveDirectory(string fromVirtualPath, string destinationVirtualPath) {
			throw new NotImplementedException();
		}

		public void DeleteDirectory(string virtualPath) { //upload/test/
			virtualPath = FixVirtualPath(virtualPath);
			DeleteDirectoryAndChildren(virtualPath);
			if (DirectoryDeleted != null)
				DirectoryDeleted.Invoke(this, new FileEventArgs(virtualPath, null));
		}

		public void CreateDirectory(string virtualPath) {
			virtualPath = string.Format("{0}/{1}", FixVirtualPath(virtualPath), EmptyFilename);

			var request = new PutObjectRequest()
				.WithBucketName(_bucketName)
				.WithKey(virtualPath)
				.WithContentBody(string.Empty)
				.WithContentType("text");
			using (_s3.PutObject(request)) { }
		}

		//TODO: implement these (need to figure out exactly what to set in FileEventArgs)
		public event EventHandler<FileEventArgs> FileWritten;
		public event EventHandler<FileEventArgs> FileCopied;
		public event EventHandler<FileEventArgs> FileMoved;
		public event EventHandler<FileEventArgs> FileDeleted;
		public event EventHandler<FileEventArgs> DirectoryCreated;
		public event EventHandler<FileEventArgs> DirectoryMoved;
		public event EventHandler<FileEventArgs> DirectoryDeleted;

		#endregion

		private string FixVirtualPath(string virtualPath) {
			if (virtualPath.StartsWith(@"~/"))
				return virtualPath.Replace(@"~/", string.Empty).TrimStart('/');

			if (virtualPath.StartsWith(@"/N2/"))
				return virtualPath.Replace(@"/N2/", string.Empty).TrimStart('/');

			if (virtualPath.StartsWith(@"/upload/"))
				return virtualPath;

			return virtualPath.TrimStart('/');
		}

		private void DeleteDirectoryAndChildren(string virtualPath) {
			var directories = GetDirectories(virtualPath);
			foreach (var directory in directories)
				DeleteDirectoryAndChildren(directory.VirtualPath);

			var files = GetFiles(virtualPath);
			foreach (var file in files)
				DeleteFile(file.VirtualPath);

			var request = new DeleteObjectRequest()
				.WithBucketName(_bucketName)
				.WithKey(virtualPath + EmptyFilename);
			using (_s3.DeleteObject(request)) { }
		}

	}
}
