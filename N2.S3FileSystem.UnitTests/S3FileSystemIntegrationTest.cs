using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace N2.S3FileSystem.UnitTests
{
    [TestFixture]
    public class S3FileSystemIntegrationTest
    {
        private N2.Edit.FileSystem.S3FileSystem _subject;

        [SetUp]
        public void Setup()
        {
            _subject = new Edit.FileSystem.S3FileSystem();
        }

        [Test]
        public void WriteFile()
        {
            Stream inputStream = new MemoryStream(Encoding.UTF8.GetBytes("Hello world!"));
            _subject.WriteFile("/test.txt", inputStream);
        }
    }
}
