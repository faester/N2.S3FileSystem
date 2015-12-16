using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using N2.Collections;
using N2.Engine;

// ReSharper disable CheckNamespace
namespace N2.Edit.FileSystem
{
    // ReSharper restore CheckNamespace 
    [Service(typeof(IFileSystem))]
    public class S3FileSystem : IFileSystem
    {
        private readonly string accessKeyId;
        private readonly string secretAccessKey;
        private readonly IAmazonS3 s3;
        private readonly string bucketName;
        private readonly string RootURL;
        private const string EmptyFilename = @"__empty";
        private readonly RegionEndpoint regionEndpoint;

        public S3FileSystem()
        {
            this.accessKeyId = GetAppSetting("AWSAccessKeyID");
            this.secretAccessKey = GetAppSetting("AWSSecretAccessKey");
            this.bucketName = GetAppSetting("AWSBucketName");
            this.regionEndpoint = RegionEndpoint.GetBySystemName(GetAppSetting("AWSRegionEndpoint"));
            this.RootURL = string.Format(@"https://{0}.s3.amazonaws.com/{{0}}/{{1}}", this.regionEndpoint.SystemName);
            this.s3 = AWSClientFactory.CreateAmazonS3Client(this.accessKeyId, this.secretAccessKey, this.regionEndpoint);
        }

        private static string GetAppSetting(string key)
        {
            string value = ConfigurationManager.AppSettings[key];
            if (string.IsNullOrEmpty(value))
            {
                throw new ConfigurationErrorsException(string.Format("Missing required app setting '{0}'. Please fix it.", key));
            }
            return value;
        }

        #region Implementation of IFileSystem

        public IEnumerable<FileData> GetFiles(string parentVirtualPath)
        {
            var request = new ListObjectsRequest
            {
                BucketName = this.bucketName,
                Prefix = FixPathForS3(parentVirtualPath),
                Delimiter = @"/",
            };


            IEnumerable<FileData> files;
            var response = this.s3.ListObjects(request);
            files = response.S3Objects.Where(file => file.Size > 0).Select(file => new FileData
            {
                VirtualPath = FixPathForN2(file.Key),
                Name = file.Key.Substring(file.Key.LastIndexOf('/') + 1),
                Length = file.Size,
                Created = file.LastModified,
                Updated = file.LastModified,
            });

            return files;
        }

        public FileData GetFile(string virtualPath)
        {
            var request = new GetObjectRequest()
            {
                BucketName = bucketName,
                Key = FixPathForS3(virtualPath),
            };

            FileData file;
            using (var response = this.s3.GetObject(request))
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

            var request = new ListObjectsRequest
            {
                BucketName = this.bucketName,
                Prefix = parentVirtualPath,
                Delimiter = @"/",
            };

            IEnumerable<DirectoryData> directories;
            var response = this.s3.ListObjects(request);

            directories = response.CommonPrefixes.Select(dir => new DirectoryData
            {
                Created = DateTime.Now,
                Updated = DateTime.Now,
                Name = dir.TrimEnd('/').Substring(dir.TrimEnd('/').LastIndexOf('/') + 1),
                VirtualPath = FixPathForN2(dir)
            });

            return directories;
        }

        public DirectoryData GetDirectory(string virtualPath)
        {
            //virtualPath = FixVirtualPath(virtualPath);
            return new DirectoryData
            {
                Name = string.Format(RootURL, this.bucketName, virtualPath),
                VirtualPath = virtualPath,
                Created = DateTime.Now,
                Updated = DateTime.Now
            };
        }

        public bool FileExists(string virtualPath)
        {
            var request = new GetObjectMetadataRequest
            {
                BucketName = this.bucketName,
                Key = FixPathForS3(virtualPath),
            };

            try
            {
                this.s3.GetObjectMetadata(request);
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
            var request = new DeleteObjectRequest
            {
                BucketName = this.bucketName,
                Key = FixPathForS3(virtualPath),
            };

            this.s3.DeleteObject(request);

            if (FileDeleted != null)
                FileDeleted.Invoke(this, new FileEventArgs(virtualPath, null));
        }

        public void CopyFile(string fromVirtualPath, string destinationVirtualPath)
        {
            var copyRequest = new CopyObjectRequest
            {
                SourceBucket = this.bucketName,
                SourceKey = fromVirtualPath,
                DestinationBucket = this.bucketName, 
                DestinationKey = destinationVirtualPath, 
                CannedACL = S3CannedACL.PublicRead,
                ReadWriteTimeout = TimeSpan.FromMinutes(10),
                Timeout = TimeSpan.FromMinutes(10),
            };

            this.s3.CopyObject(copyRequest);

            if (FileCopied != null)
                FileCopied.Invoke(this, new FileEventArgs(FixPathForN2(fromVirtualPath), FixPathForN2(destinationVirtualPath)));
        }


        public Stream OpenFile(string virtualPath, bool readOnly = false)
        {
            var request = new GetObjectRequest
            {
                BucketName = this.bucketName,
                Key = FixPathForS3(virtualPath),
            };

            var stream = new MemoryStream();
            try {
                using (var response = this.s3.GetObject(request))
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
            catch(Exception exception)
            {
                throw new ArgumentException(string.Format("There was an error retrieving '{0}' from bucket '{1}', see inner exception for details", request.Key, request.BucketName), exception);
            }
        }

        public void WriteFile(string virtualPath, Stream inputStream)
        {
            var request = new PutObjectRequest
            {
                BucketName = this.bucketName,
                CannedACL = S3CannedACL.PublicRead,
                Timeout = TimeSpan.FromHours(1),
                ReadWriteTimeout = TimeSpan.FromHours(1),
                Key = FixPathForS3(virtualPath),
                InputStream = inputStream,
            };

            var lastDot = virtualPath.LastIndexOf(".", StringComparison.Ordinal);
            request.ContentType = lastDot > 0 ? virtualPath.Substring(lastDot) : "txt";

            s3.PutObject(request);
            
            if (FileWritten != null)
                FileWritten.Invoke(this, new FileEventArgs(FixPathForN2(virtualPath), null));
        }

        public void ReadFileContents(string virtualPath, Stream outputStream)
        {
            var request = new GetObjectRequest
            {
                BucketName = this.bucketName,
                Key = FixPathForS3(virtualPath),
            };

            using (var response = this.s3.GetObject(request))
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

            var request = new GetObjectMetadataRequest
            {
                BucketName = this.bucketName,
                Key = virtualPath,
            };

            try
            {
                this.s3.GetObjectMetadata(request);
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

            var request = new PutObjectRequest
            {
                BucketName = this.bucketName,
                Key = virtualPath,
                ContentBody = string.Empty,
                ContentType = "text",
            };

            this.s3.PutObject(request);

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

            var request = new DeleteObjectRequest
            {
                BucketName = this.bucketName,
                Key = virtualPath + EmptyFilename,
            };

            this.s3.DeleteObject(request);
        }

        public IEnumerable<FileData> SearchFiles(string query, List<HierarchyNode<ContentItem>> uploadDirectories)
        {
            return Enumerable.Empty<FileData>();
        }
    }
}
