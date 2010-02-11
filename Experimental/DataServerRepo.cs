using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;

namespace Data.Subscriptions
{
	public class Server
	{
		public string Host { get; set; }
		public string OrmType { get; set; }

		public Server (string host)
		{
			Host = host;
		}
	}

	public class ServerRepo
	{
		string _prefix;
		HttpListener _listener;
		
		public ServerRepo (string prefix)
		{
			_prefix = prefix;
		}

		public void Start ()
		{
			_listener = new HttpListener ();
			_listener.Prefixes.Add (_prefix);
			_listener.Start ();
			BeginGet();	
		}
		void BeginGet() {
			_listener.BeginGetContext (HandleGetContext, null);
		}

		void HandleGetContext (IAsyncResult ar)
		{
			var c = _listener.EndGetContext (ar);
			HandleRequest (c);
			BeginGet();
		}

		void HandleRequest (HttpListenerContext c)
		{
			try {
				var r = new Request (c.Request);
				
				Console.WriteLine (r);
				
				if (!r.IsValid) {
					c.Response.StatusCode = 400; // Bad Request
				}
				else if (!r.IsAuthenticated) {
					c.Response.StatusCode = 403; // Forbidden
				}
				else {
					c.Response.StatusCode = 200;
					c.Response.SendChunked = true;
					var w = new StreamWriter (c.Response.OutputStream);
					for (int i = 0; i < 100; i++) {
						w.Write ("hi({A:\"wow\"},90,-02.2);\r\nthere();\r\n");
						w.Flush ();
						c.Response.OutputStream.Flush ();
						System.Threading.Thread.Sleep (500);
					}
					w.Flush ();
				}
			}
			catch (System.IO.IOException) {
			}
			catch (Exception error) {
				LogRequestError(error);
			}
		}
		
		void LogRequestError(Exception error) {
			Console.WriteLine ("REQUEST ERROR " + error.Message);
		}

		public class Request
		{
			public bool IsValid { get; private set; }
			public bool IsAuthenticated { get; private set; }
			public string ClientId { get; private set; }
			public string Method { get; private set; }			

			public Request (HttpListenerRequest req)
			{
				try {
					var queryParams = req.QueryString;
					var path = req.Url.AbsolutePath;
					
					Method = req.HttpMethod.ToUpperInvariant();
					
					var key = queryParams["key"];
					IsAuthenticated = Keys.IsKeyValid(key);
					
					ClientId = path.Substring(1);
					
					IsValid = true;
				}
				catch (Exception) {
					IsValid = false;
				}
			}
			
			public override string ToString ()
			{
				return string.Format("[Request: IsValid={0}, IsAuthenticated={1}, ClientId={2}, Method={3}]", IsValid, IsAuthenticated, ClientId, Method);
			}

			
			
		}
		
		
	}
	
	
	
	
}
