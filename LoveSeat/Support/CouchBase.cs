using System;
using System.Net;

namespace LoveSeat.Support
{
	public abstract class CouchBase
	{
		protected readonly string username;
		protected readonly string password;
		protected string baseUri;

		protected CouchBase()
		{
			throw new Exception("Should not be used.");
		}
		protected CouchBase(string username, string password)
		{
			this.username = username;
			this.password = password;
		}
        public static bool Authenticate(string baseUri, string userName, string password)
        {
            try
            {
                if (!baseUri.Contains("http://"))
                    baseUri = "http://" + baseUri;
            var request = new CouchRequest( baseUri + "/_session");
            request.Timeout = 3000;
            var response = request.Post()
                .ContentType("application/x-www-form-urlencoded")
                .Data("name=" + userName + "&password=" + password)
                .GetResponse();
                return response.StatusCode == HttpStatusCode.OK;
            }
            catch (Exception ce)
            {
                return false;
            }
            
        }
		
		protected Cookie GetSession()
		{
			if (string.IsNullOrEmpty(username)) return null; 
			var request = new CouchRequest(baseUri + "_session");
			var response = request.Post()
				.ContentType("application/x-www-form-urlencoded")
				.Data("name=" + username + "&password=" + password)
				.GetResponse();

			var header = response.Headers.Get("Set-Cookie");
			if (header != null)
			{
				var parts = header.Split(';')[0].Split('=');
				var authCookie = new Cookie(parts[0], parts[1]);
				authCookie.Domain = response.Server;
				return authCookie;
			}
			return null;
		}

		protected CouchRequest GetRequest(string uri)
		{
			var request = new CouchRequest(uri, GetSession());			
			return request;
		}
	}
}