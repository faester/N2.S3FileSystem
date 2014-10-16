using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using N2.Edit.Install;
using N2.Engine;

// ReSharper disable CheckNamespace
namespace N2.Edit.FileSystem
{
    // ReSharper restore CheckNamespace 
    [Service(typeof(IFileSystem))]
    public class S3FileSystem : IFileSystem
    {
        private readonly string _accessKeyId;
        private readonly string _secretAccessKey;
        private readonly AmazonS3 _s3;
        private readonly string _bucketName;
        private const string RootURL = @"https://s3.amazonaws.com/{0}/{1}";
        private const string EmptyFilename = @"__empty";

        public S3FileSystem()
        {
            _accessKeyId = ConfigurationManager.AppSettings["AWSAccessKeyID"];
            _secretAccessKey = ConfigurationManager.AppSettings["AWSSecretAccessKey"];
            _bucketName = ConfigurationManager.AppSettings["AWSBucketName"];
            _s3 = AWSClientFactory.CreateAmazonS3Client(_accessKeyId, _secretAccessKey);
        }

        #region Implementation of IFileSystem

        public IEnumerable<FileData> GetFiles(string parentVirtualPath)
        {
            var request = new ListObjectsRequest()
              .WithBucketName(_bucketName)
              .WithPrefix(FixPathForS3(parentVirtualPath))
              .WithDelimiter(@"/");

            IEnumerable<FileData> files;
            using (var response = _s3.ListObjects(request))
            {
                files = response.S3Objects.Where(file => file.Size > 0).Select(file => new FileData
                {
                    VirtualPath = FixPathForN2(file.Key),
                    Name = file.Key.Substring(file.Key.LastIndexOf('/') + 1),
                    Length = file.Size,
                    Created = DateTime.Parse(file.LastModified),
                    Updated = DateTime.Parse(file.LastModified)
                });
            }
            return files;
        }

        public FileData GetFile(string virtualPath)
        {
            var request = new GetObjectRequest()
              .WithBucketName(_bucketName)
              .WithKey(FixPathForS3(virtualPath));
            FileData file;
            using (var response = _s3.GetObject(request))
            {
                file = new FileData
                {
                    Name = response.Key.Substring(response.Key.LastIndexOf('/') + 1),
                    Updated = DateTime.Now,
                    Created = DateTime.Now,
                    Length = response.ContentLength,
                    VirtualPath = FixPathForN2(response.Key)
                };
            }
            return file;
        }

        public IEnumerable<DirectoryData> GetDirectories(string parentVirtualPath)
        {
            parentVirtualPath = FixPathForS3(parentVirtualPath);

            var request = new ListObjectsRequest()
              .WithBucketName(_bucketName)
              .WithPrefix(parentVirtualPath)
              .WithDelimiter(@"/");

            IEnumerable<DirectoryData> directories;
            using (var response = _s3.ListObjects(request))
            {
                directories = response.CommonPrefixes.Select(dir => new DirectoryData
                {
                    Created = DateTime.Now,
                    Updated = DateTime.Now,
                    Name = dir.TrimEnd('/').Substring(dir.TrimEnd('/').LastIndexOf('/') + 1),
                    VirtualPath = FixPathForN2(dir)
                });
            }
            return directories;
        }

        public DirectoryData GetDirectory(string virtualPath)
        {
            //virtualPath = FixVirtualPath(virtualPath);
            return new DirectoryData
            {
                Name = string.Format(RootURL, _bucketName, virtualPath),
                VirtualPath = virtualPath,
                Created = DateTime.Now,
                Updated = DateTime.Now
            };
        }

        public bool FileExists(string virtualPath)
        {
            var request = new GetObjectMetadataRequest()
              .WithBucketName(_bucketName)
              .WithKey(FixPathForS3(virtualPath));

            try
            {
                using (_s3.GetObjectMetadata(request)) { }
            }
            catch (AmazonS3Exception)
            {
                return false;
            }
            return true;
        }

        public void MoveFile(string fromVirtualPath, string destinationVirtualPath)
        {
            CopyFile(fromVirtualPath, destinationVirtualPath);
            DeleteFile(fromVirtualPath);
            if (FileMoved != null)
                FileMoved.Invoke(this, new FileEventArgs(FixPathForN2(fromVirtualPath), FixPathForN2(destinationVirtualPath)));
        }

        public void DeleteFile(string virtualPath)
        {
            var request = new DeleteObjectRequest()
              .WithBucketName(_bucketName)
              .WithKey(FixPathForS3(virtualPath));

            using (_s3.DeleteObject(request)) { }

            if (FileDeleted != null)
                FileDeleted.Invoke(this, new FileEventArgs(virtualPath, null));
        }

        public void CopyFile(string fromVirtualPath, string destinationVirtualPath)
        {
            var copyRequest = new CopyObjectRequest()
              .WithMetaData("Expires", DateTime.Now.AddYears(10).ToString("R"))
              .WithSourceBucket(_bucketName)
              .WithSourceKey(fromVirtualPath)
              .WithDestinationBucket(_bucketName)
              .WithDestinationKey(destinationVirtualPath)
              .WithCannedACL(S3CannedACL.PublicRead);

            using (_s3.CopyObject(copyRequest)) { }
            if (FileCopied != null)
                FileCopied.Invoke(this, new FileEventArgs(FixPathForN2(fromVirtualPath), FixPathForN2(destinationVirtualPath)));
        }


        public Stream OpenFile(string virtualPath, bool readOnly = false)
        {
            var request = new GetObjectRequest()
              .WithBucketName(_bucketName)
              .WithKey(FixPathForS3(virtualPath));

            var stream = new MemoryStream();

            using (var response = _s3.GetObject(request))
            {
                var buffer = new byte[32768];
                while (true)
                {
                    var read = response.ResponseStream.Read(buffer, 0, buffer.Length);
                    if (read <= 0)
                        break;
                    stream.Write(buffer, 0, read);
                }
            }
            return stream;
        }

        public void WriteFile(string virtualPath, Stream inputStream)
        {
            var request = new PutObjectRequest()
              .WithMetaData("Expires", DateTime.Now.AddYears(10).ToString("R"))
              .WithBucketName(_bucketName)
              .WithCannedACL(S3CannedACL.PublicRead)
              .WithTimeout(60 * 60 * 1000) // 1 hour
              .WithReadWriteTimeout(60 * 60 * 1000) // 1 hour
              .WithKey(FixPathForS3(virtualPath));

            var contentType = virtualPath.Substring(virtualPath.LastIndexOf(".", StringComparison.Ordinal));
            if (string.IsNullOrWhiteSpace(contentType))
            {
                request.ContentType = contentType;
            }

            request.WithInputStream(inputStream);
            using (_s3.PutObject(request)) { }

            if (FileWritten != null)
                FileWritten.Invoke(this, new FileEventArgs(FixPathForN2(virtualPath), null));
        }

        public void ReadFileContents(string virtualPath, Stream outputStream)
        {
            var request = new GetObjectRequest()
              .WithBucketName(_bucketName)
              .WithKey(FixPathForS3(virtualPath));

            using (var response = _s3.GetObject(request))
            {
                var buffer = new byte[32768];
                while (true)
                {
                    var read = response.ResponseStream.Read(buffer, 0, buffer.Length);
                    if (read <= 0) break;
                    outputStream.Write(buffer, 0, read);
                }
            }
        }

        public bool DirectoryExists(string virtualPath)
        { // ~/upload/28/
            virtualPath = FixPathForS3(virtualPath) + EmptyFilename;

            var request = new GetObjectMetadataRequest()
              .WithBucketName(_bucketName)
              .WithKey(virtualPath);

            try
            {
                using (_s3.GetObjectMetadata(request)) { }
            }
            catch (AmazonS3Exception)
            {
                return false;
            }
            return true;
        }

        public void MoveDirectory(string fromVirtualPath, string destinationVirtualPath)
        {
            if (DirectoryMoved != null) // To avoid warnings when building.
            {
                //  DirectoryMoved.Invoke(this, new FileEventArgs(FixPathForN2(fromVirtualPath), FixPathForN2(destinationVirtualPath)));    
                //  Move down when implemented.
            }
            throw new NotImplementedException();

        }

        public void DeleteDirectory(string virtualPath)
        { //upload/test/
            virtualPath = FixPathForS3(virtualPath);
            DeleteDirectoryAndChildren(virtualPath);
            if (DirectoryDeleted != null)
                DirectoryDeleted.Invoke(this, new FileEventArgs(FixPathForN2(virtualPath), null));
        }

        public void CreateDirectory(string virtualPath)
        {
            virtualPath = string.Format("{0}/{1}", FixPathForS3(virtualPath), EmptyFilename);

            var request = new PutObjectRequest()
              .WithBucketName(_bucketName)
              .WithKey(virtualPath)
              .WithContentBody(string.Empty)
              .WithContentType("text");
            using (_s3.PutObject(request)) { }

            if (DirectoryCreated != null)
                DirectoryCreated.Invoke(this, new FileEventArgs(FixPathForN2(virtualPath), null));
        }

        public event EventHandler<FileEventArgs> FileWritten;
        public event EventHandler<FileEventArgs> FileCopied;
        public event EventHandler<FileEventArgs> FileMoved;
        public event EventHandler<FileEventArgs> FileDeleted;
        public event EventHandler<FileEventArgs> DirectoryCreated;
        public event EventHandler<FileEventArgs> DirectoryMoved;
        public event EventHandler<FileEventArgs> DirectoryDeleted;

        #endregion

        private string FixPathForN2(string virtualPath)
        {
            return @"~/" + virtualPath;
        }

        private string FixPathForS3(string virtualPath)
        {
            return virtualPath.Replace(@"~/", string.Empty).TrimStart('/');
        }

        private void DeleteDirectoryAndChildren(string virtualPath)
        {
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
