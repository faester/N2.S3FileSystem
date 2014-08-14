using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Hosting;
using Microsoft.Web.Administration;

namespace N2.S3FileSystem {
  internal class MimeExtractor {
    private const string CacheKey = "mime_types_cache";
    private const string CacheDictionaryKey = "mime_types_cache_dictionary";

    public string GetMimeTypeForExtension(string extension) {
      if (HttpContext.Current.Cache[CacheKey] == null) {
        HttpContext.Current.Cache.Insert(CacheDictionaryKey, ParseMimeTypes());
      }
      if (!extension.StartsWith(".")) {
        extension = "." + extension;
      }

      var mimeDictionary = (IDictionary<string, string>)HttpContext.Current.Cache[CacheDictionaryKey];
      return mimeDictionary.ContainsKey(extension) ? mimeDictionary[extension] : null;
    }

    private static IDictionary<string, string> ParseMimeTypes() {
      using (var serverManager = new ServerManager()) {
        var siteName = HostingEnvironment.ApplicationHost.GetSiteName();
        var config = serverManager.GetWebConfiguration(siteName);
        var staticContentSection = config.GetSection("system.webServer/staticContent");
        var staticContentCollection = staticContentSection.GetCollection();

        var mimeMaps = staticContentCollection.Where(c =>
          c.ElementTagName == "mimeMap"
          && c.GetAttributeValue("fileExtension") != null
          && !string.IsNullOrWhiteSpace(c.GetAttributeValue("fileExtension").ToString())
          && c.GetAttributeValue("mimeType") != null
          && !string.IsNullOrWhiteSpace(c.GetAttributeValue("mimeType").ToString())
        );
        var results = mimeMaps.Select(m => new KeyValuePair<string, string>(m.GetAttributeValue("fileExtension").ToString(), m.GetAttributeValue("mimeType").ToString()));
        return results.ToDictionary(pair => pair.Key, pair => pair.Value);
      }
    }
  }
}
