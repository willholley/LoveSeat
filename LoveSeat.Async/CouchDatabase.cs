using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using System.Web;
using LoveSeat.Async.Interfaces;
using LoveSeat.Async.Support;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace LoveSeat.Async
{
    public class CouchDatabase : CouchBase, IDocumentDatabase
    {
        public IObjectSerializer ObjectSerializer = new DefaultSerializer();

        private readonly string databaseBaseUri;
        private string defaultDesignDoc = null;
        internal CouchDatabase(string baseUri, string databaseName, string username, string password, AuthenticationType aT)
            : base(username, password, aT)
        {
            this.baseUri = baseUri;
            this.databaseBaseUri = baseUri + databaseName;
        }

        /// <summary>
        /// Creates a document using the json provided. 
        /// No validation or smarts attempted here by design for simplicities sake
        /// </summary>
        /// <param name="id">Id of Document</param>
        /// <param name="jsonForDocument"></param>
        /// <returns></returns>
        public async Task<CouchResponse> CreateDocumentAsync(string id, string jsonForDocument)
        {
            var jobj = JObject.Parse(jsonForDocument);
            if (jobj.Value<object>("_rev") == null)
                jobj.Remove("_rev");

            var request = await GetRequestAsync(databaseBaseUri + "/" + id);
            var resp = await request.Put().Form().Data(jobj.ToString(Formatting.None)).GetResponseAsync();
            return resp.GetJObject();
        }

        public async Task<CouchResponse> CreateDocumentAsync(IBaseObject doc) 
        {
            var serialized = ObjectSerializer.Serialize(doc);
            if (doc.Id != null)
            {
                return await CreateDocumentAsync(doc.Id, serialized);
            }
            return await CreateDocumentAsync(serialized);
        }
        /// <summary>
        /// Creates a document when you intend for Couch to generate the id for you.
        /// </summary>
        /// <param name="jsonForDocument">Json for creating the document</param>
        /// <returns>The response as a JObject</returns>
        public async Task<CouchResponse> CreateDocumentAsync(string jsonForDocument)
        {
            JObject.Parse(jsonForDocument); //to make sure it's valid json
                
            var request = await GetRequestAsync(databaseBaseUri + "/");
            var response = await request.Post().Json().Data(jsonForDocument).GetResponseAsync();

            return response.GetJObject();
        }        
        public async Task<CouchResponse> DeleteDocumentAsync(string id, string rev)
        {
            if (string.IsNullOrEmpty(id) || string.IsNullOrEmpty(rev))
                throw new Exception("Both id and rev must have a value that is not empty");

            var request = await GetRequestAsync(databaseBaseUri + "/" + id + "?rev=" + rev);
            var response = await request.Delete().Form().GetResponseAsync();

            return response.GetJObject();
        }
        /// <summary>
        /// Returns null if document is not found
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<T> GetDocumentAsync<T>(string id, bool attachments, IObjectSerializer objectSerializer)
        {
            var request = await GetRequestAsync(String.Format("{0}/{1}{2}", databaseBaseUri, id, attachments ? "?attachments=true" : string.Empty));
            var response = await request.Get().Json().GetResponseAsync();

            if (response.StatusCode == HttpStatusCode.NotFound) return default(T);

            return objectSerializer.Deserialize<T>(response.GetResponseString());
        }
        public async Task<T> GetDocumentAsync<T>(string id, IObjectSerializer objectSerializer)
        {
            return await GetDocumentAsync<T>(id, false, objectSerializer);
        }
        public async Task<T> GetDocumentAsync<T>(string id, bool attachments)
        {
            return await GetDocumentAsync<T>(id, attachments, ObjectSerializer);
        }
        public async Task<T> GetDocumentAsync<T>(string id)
        {
            return await GetDocumentAsync<T>(id, false);
        }
        public async Task<T> GetDocumentAsync<T>(Guid id, bool attachments, IObjectSerializer objectSerializer)
        {
            return await GetDocumentAsync<T>(id.ToString(), attachments, objectSerializer);
        }
        public async Task<T> GetDocumentAsync<T>(Guid id, IObjectSerializer objectSerializer)
        {
            return await GetDocumentAsync<T>(id, false, objectSerializer);
        }
        public async Task<T> GetDocumentAsync<T>(Guid id, bool attachments)
        {
            return await GetDocumentAsync<T>(id.ToString(), attachments);
        }
        public async Task<T> GetDocumentAsync<T>(Guid id)
        {
            return await GetDocumentAsync<T>(id, false);
        }
        public async Task<Document> GetDocumentAsync(string id, bool attachments)
        {
            var request = await GetRequestAsync(String.Format("{0}/{1}{2}", databaseBaseUri, id, attachments ? "?attachments=true" : string.Empty));
            var response = await request.Get().Json().GetResponseAsync();

            if (response.StatusCode == HttpStatusCode.NotFound) return null;
            return response.GetCouchDocument();
        }
        public async Task<Document> GetDocumentAsync(string id)
        {
            return await GetDocumentAsync(id, false);
        }

        /// <summary>
        /// Request multiple documents 
        /// in a single request.
        /// </summary>
        /// <param name="keyLst"></param>
        /// <returns></returns>
        public async Task<ViewResult> GetDocumentsAsync(Keys keyLst)
        {
            // serialize list of keys to json
            string data = JsonConvert.SerializeObject(keyLst);
            
            var request = await GetRequestAsync(databaseBaseUri + "/_all_docs");
            var response = await request.Post().Json().Data(data).GetResponseAsync();

            if (response == null) return null;

            if (response.StatusCode == HttpStatusCode.NotFound) return null;

            return new ViewResult(response, null);
        }
 
        /// <summary>
        /// Using the bulk API for the loading of documents.
        /// </summary>
        /// <param name="docs"></param>
        /// <remarks>Here we assume you have either added the correct rev, id, or _deleted attribute to each document.  The response will indicate if there were any errors.
        /// Please note that the max_document_size configuration variable in CouchDB limits the maximum request size to CouchDB.</remarks>
        /// <returns>JSON of updated documents in the BulkDocumentResponse class.  </returns>
        public async Task<BulkDocumentResponses> SaveDocumentsAsync(Documents docs, bool all_or_nothing)
        {
            string uri = databaseBaseUri + "/_bulk_docs";

            string data = Newtonsoft.Json.JsonConvert.SerializeObject(docs);

            if (all_or_nothing == true)
            {
                uri = uri + "?all_or_nothing=true";
            }

            var request = await GetRequestAsync(uri);
            var response = await request.Post().Json().Data(data).GetResponseAsync();

            if (response == null)
            {
                throw new System.Exception("Response returned null.");
            }

            if (response.StatusCode != HttpStatusCode.Created)
            {
                throw new System.Exception("Response returned with a HTTP status code of " + response.StatusCode + " - " + response.StatusDescription);    
            }

            // Get response
            string x = response.GetResponseString();
                        
            // Convert to Bulk response
            return JsonConvert.DeserializeObject<BulkDocumentResponses>(x);
        }

        
        /// <summary>
        /// Adds an attachment to a document.  If revision is not specified then the most recent will be fetched and used.  Warning: if you need document update conflicts to occur please use the method that specifies the revision
        /// </summary>
        /// <param name="id">id of the couch Document</param>
        /// <param name="attachment">byte[] of of the attachment.  Use File.ReadAllBytes()</param>
        /// <param name="filename">filename of the attachment</param>
        /// <param name="contentType">Content Type must be specifed</param>	
        public async Task<CouchResponse> AddAttachmentAsync(string id, byte[] attachment, string filename, string contentType)
        {
            var doc = await GetDocumentAsync(id);
            return await AddAttachmentAsync(id, doc.Rev, attachment, filename, contentType);
        }
        /// <summary>
        /// Adds an attachment to the documnet.  Rev must be specified on this signature.  If you want to attach no matter what then use the method without the rev param
        /// </summary>
        /// <param name="id">id of the couch Document</param>
        /// <param name="rev">revision _rev of the Couch Document</param>
        /// <param name="attachment">byte[] of of the attachment.  Use File.ReadAllBytes()</param>
        /// <param name="filename">filename of the attachment</param>
        /// <param name="contentType">Content Type must be specifed</param>			
        /// <returns></returns>
        public async Task<CouchResponse> AddAttachmentAsync(string id, string rev, byte[] attachment, string filename, string contentType)
        {
            var request = await GetRequestAsync(string.Format("{0}/{1}/{2}?rev={3}", databaseBaseUri, id, filename, rev));
            var response = await request.Put().ContentType(contentType).Data(attachment).GetResponseAsync();

            return response.GetJObject();
        }
        /// <summary>
        /// Adds an attachment to a document.  If revision is not specified then the most recent will be fetched and used.  Warning: if you need document update conflicts to occur please use the method that specifies the revision
        /// </summary>
        /// <param name="id">id of the couch Document</param>
        /// <param name="attachmentStream">Stream of the attachment.</param>
        /// <param name="contentType">Content Type must be specifed</param>	
        public async Task<CouchResponse> AddAttachmentAsync(string id, Stream attachmentStream, string filename, string contentType)
        {
            var doc = await GetDocumentAsync(id);
            return await AddAttachmentAsync(id, doc.Rev, attachmentStream, filename, contentType);
        }
        /// <summary>
        /// Adds an attachment to the documnet.  Rev must be specified on this signature.  If you want to attach no matter what then use the method without the rev param
        /// </summary>
        /// <param name="id">id of the couch Document</param>
        /// <param name="rev">revision _rev of the Couch Document</param>
        /// <param name="attachmentStream">Stream of of the attachment.  Use File.ReadAllBytes()</param>
        /// <param name="filename">filename of the attachment</param>
        /// <param name="contentType">Content Type must be specifed</param>			
        /// <returns></returns>
        public async Task<CouchResponse> AddAttachmentAsync(string id, string rev, Stream attachmentStream, string filename, string contentType)
        {
            var request = await GetRequestAsync(string.Format("{0}/{1}/{2}?rev={3}", databaseBaseUri, id, filename, rev));
            request = await request.Put().ContentType(contentType).DataAsync(attachmentStream);

            var response = await request.GetResponseAsync();

            return response.GetJObject();
        }

        public async Task<Stream> GetAttachmentStreamAsync(Document doc, string attachmentName)
        {
            return await GetAttachmentStreamAsync(doc.Id, doc.Rev, attachmentName);
        }

        public async Task<Stream> GetAttachmentStreamAsync(string docId, string rev, string attachmentName)
        {
            var request = await GetRequestAsync(string.Format("{0}/{1}/{2}", databaseBaseUri, docId, HttpUtility.UrlEncode(attachmentName)));
            var response = await request.Get().GetResponseAsync();
            
            return response.GetResponseStream();
        }

        public async Task<Stream> GetAttachmentStreamAsync(string docId, string attachmentName)
        {
            var doc = await GetDocumentAsync(docId);
            if (doc == null) return null;
            return await GetAttachmentStreamAsync(docId, doc.Rev, attachmentName);
        }

        public async Task<CouchResponse> DeleteAttachmentAsync(string id, string rev, string attachmentName)
        {
            var request = await GetRequestAsync(string.Format("{0}/{1}/{2}?rev={3}", databaseBaseUri, id, attachmentName, rev));
            var response = await request.Json().Delete().GetResponseAsync();
            
            return response.GetJObject();
        }
        public async Task<CouchResponse> DeleteAttachmentAsync(string id, string attachmentName)
        {
            var doc = await GetDocumentAsync(id);
            return await DeleteAttachmentAsync(doc.Id, doc.Rev, attachmentName);
        }

        public async Task<CouchResponse> SaveDocumentAsync(Document document)
        {
            if (document.Rev == null)
                return await CreateDocumentAsync(document);
                    
            var request = await GetRequestAsync(string.Format("{0}/{1}?rev={2}", databaseBaseUri, document.Id, document.Rev));
            var response = await request.Put().Form().Data(document).GetResponseAsync();

            return response.GetJObject();
        }

        /// <summary>
        /// Gets the results of a view with no view parameters.  Use the overload to pass parameters
        /// </summary>
        /// <param name="viewName">The name of the view</param>
        /// <param name="designDoc">The design doc on which the view resides</param>
        /// <returns></returns>
        public async Task<ViewResult<T>> ViewAsync<T>(string viewName, string designDoc)
        {
            return await ViewAsync<T>(viewName, null, designDoc);
        }

        /// <summary>
        /// Gets the results of the view using the defaultDesignDoc and no view parameters.  Use the overloads to specify options.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="viewName"></param>
        /// <returns></returns>
        public async Task<ViewResult<T>> ViewAsync<T>(string viewName)
        {
            ThrowDesignDocException();
            return await ViewAsync<T>(viewName, defaultDesignDoc);
        }
        public async Task<ViewResult> ViewAsync(string viewName)
        {
            ThrowDesignDocException();
            return await ViewAsync(viewName, new ViewOptions());
        }

        /// <summary>
        /// Call view cleanup for a database
        /// </summary>
        /// <returns>JSON success statement if the response code is Accepted</returns>
        public async Task<JObject> ViewCleanupAsync()
        {
            return await DoCommandAsync("_view_cleanup");
        }

        private async Task<JObject> DoCommandAsync(string command)
        {
            var request = await GetRequestAsync(string.Format("{0}/{1}", databaseBaseUri, command));
            var response = await request.Post().Json().GetResponseAsync();

            return CheckAccepted(response);
        }

        /// <summary>
        /// Compact the current database
        /// </summary>
        /// <returns></returns>
        public async Task<JObject> CompactAsync()
        {
            return await DoCommandAsync("_compact");
        }

        /// <summary>
        /// Compact a view.
        /// </summary>
        /// <param name="designDoc">The view to compact</param>
        /// <returns></returns>
        /// <remarks>Requires admin permissions.</remarks>
        public async Task<JObject> CompactAsync(string designDoc)
        {
            return await DoCommandAsync("_compact/" + designDoc);
        }

        private static JObject CheckAccepted(HttpWebResponse resp)
        {
            if (resp == null) {
                throw new Exception("Response returned null.");
            }

            if (resp.StatusCode != HttpStatusCode.Accepted) {
                throw new Exception(string.Format("Response return with a HTTP Code of {0} - {1}", resp.StatusCode, resp.StatusDescription));
            }

            return resp.GetJObject();

        }


        public async Task<string> ShowAsync(string showName, string docId)
        {
            ThrowDesignDocException();
            return await ShowAsync(showName, docId,  defaultDesignDoc);
        }

        private void ThrowDesignDocException()
        {
            if (string.IsNullOrEmpty(defaultDesignDoc))
                throw new Exception("You must use SetDefaultDesignDoc prior to using this signature.  Otherwise explicitly specify the design doc in the other overloads.");
        }

        public async Task<string> ShowAsync(string showName, string docId, string designDoc)
        {
            //TODO:  add in Etag support for Shows
            var uri = string.Format("{0}/_design/{1}/_show/{2}/{3}", databaseBaseUri, designDoc, showName, docId);
            var request = await GetRequestAsync(uri);
            var response = await request.GetResponseAsync();
            
            return response.GetResponseString();
        }
        public async Task<IListResult> ListAsync(string listName, string viewName, ViewOptions options, string designDoc)
        {            
			var uri = string.Format("{0}/_design/{1}/_list/{2}/{3}{4}", databaseBaseUri, designDoc, listName, viewName, options.ToString());
            
            var request = await GetRequestAsync(uri);
            return new ListResult(request.GetRequest(), await request.GetResponseAsync());
        }

        public async Task<IListResult> ListAsync(string listName, string viewName, ViewOptions options)
        {
            ThrowDesignDocException();
            return await ListAsync(listName, viewName, options, defaultDesignDoc);
        }

        public void SetDefaultDesignDoc(string designDoc)
        {
            this.defaultDesignDoc = designDoc;
        }

        private async Task<ViewResult<T>> ProcessGenericResultsAsync<T>(string uri, ViewOptions options) {
            var req = await GetRequestAsync(options, uri);
            var resp = await req.GetResponseAsync();
            if (resp.StatusCode == HttpStatusCode.BadRequest) {
                throw new CouchException(req.GetRequest(), resp, resp.GetResponseString() + "\n" + req.GetRequest().RequestUri);
            }

            bool includeDocs = false;
            if (options != null)
            {
                includeDocs = options.IncludeDocs ?? false;
            }

            return new ViewResult<T>(resp, req.GetRequest(), ObjectSerializer, includeDocs);
        }
        /// <summary>
        /// Gets the results of the view using any and all parameters
        /// </summary>
        /// <param name="viewName">The name of the view</param>
        /// <param name="options">Options such as startkey etc.</param>
        /// <param name="designDoc">The design doc on which the view resides</param>
        /// <returns></returns>
        public async Task<ViewResult<T>> ViewAsync<T>(string viewName, ViewOptions options, string designDoc)
        {
            var uri = string.Format("{0}/_design/{1}/_view/{2}", databaseBaseUri, designDoc, viewName);
            return await ProcessGenericResultsAsync<T>(uri, options);
        }
        /// <summary>
        /// Allows you to specify options and uses the defaultDesignDoc Specified.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="viewName"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public async Task<ViewResult<T>> ViewAsync<T>(string viewName, ViewOptions options)
        {
            ThrowDesignDocException();
             return await ViewAsync<T>(viewName, options, defaultDesignDoc);
        }

        public async Task<ViewResult> ViewAsync(string viewName, ViewOptions options, string designDoc)
        {
            var uri = string.Format("{0}/_design/{1}/_view/{2}", databaseBaseUri, designDoc, viewName);
            return await ProcessResultsAsync(uri, options);
        }

        public async Task<ViewResult> ViewAsync(string viewName, ViewOptions options)
        {
            ThrowDesignDocException();
            return await ViewAsync(viewName, options, this.defaultDesignDoc);
        }
        private async Task<ViewResult> ProcessResultsAsync(string uri, ViewOptions options)
        {
            var req = await GetRequestAsync(options, uri);
            var resp = await req.GetResponseAsync();
            return new ViewResult(resp, req.GetRequest());
        }
        
        private async Task<CouchRequest> GetRequestAsync(ViewOptions options, string uri)
        {
            if (options != null)
                uri +=  options.ToString();
            
            var request = await GetRequestAsync(uri, options == null ? null : options.Etag);
            return request.Get().Json();
        }


        /// <summary>
        /// Gets all the documents in the database using the _all_docs uri
        /// </summary>
        /// <returns></returns>
        public async Task<ViewResult> GetAllDocumentsAsync()
        {
            var uri = databaseBaseUri + "/_all_docs";
            return await ProcessResultsAsync(uri, null);
        }
        public async Task<ViewResult> GetAllDocumentsAsync(ViewOptions options)
        {
            var uri = databaseBaseUri + "/_all_docs";
            return await ProcessResultsAsync(uri, options);
        }




        #region Security
        public async Task<SecurityDocument> getSecurityConfigurationAsync()
        {
            var request = await GetRequestAsync(databaseBaseUri + "/_security");
            var response = await request.Get().Json().GetResponseAsync();
            
            var docResult = response.GetJObject();

            return JsonConvert.DeserializeObject<SecurityDocument>(docResult.ToString());
        }

        /// <summary>
        /// Updates security configuration for the database
        /// </summary>
        /// <param name="sDoc"></param>
        public async Task UpdateSecurityDocumentAsync(SecurityDocument sDoc)
        {
            // serialize SecurityDocument to json
            string data = JsonConvert.SerializeObject(sDoc);

            var request = await GetRequestAsync(databaseBaseUri + "/_security");
            var result = await request.Put().Json().Data(data).GetResponseAsync();

            if (result.StatusCode != HttpStatusCode.OK) //Check if okay
            {
                throw new WebException("An error occurred while trying to update the security document. StatusDescription: " + result.StatusDescription);
            }
        }

        #endregion
    }

    #region Security Configuration

    // Example: {"admins":{},"readers":{"names":["dave"],"roles":[]}}
    /// <summary>
    /// Security configuration for the database
    /// </summary>
    public class SecurityDocument
    {
        public SecurityDocument()
        {
            admins = new UserType();
            readers = new UserType();
        }


        public UserType admins;
        public UserType readers;
    }

    public class UserType
    {
        public UserType()
        {
            names = new List<string>();
            roles = new List<string>();
        }

        public List<string> names { get; set; }
        public List<string> roles { get; set; }
    }
    #endregion

}