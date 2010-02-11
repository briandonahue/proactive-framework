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

        public Server(string host) {
            Host = host;
        }
    }

    public class ServerRepo
    {
		string _prefix;
		public ServerRepo(string prefix) {
			_prefix = prefix;
		}
		
		public void Start() {
			var listener = new HttpListener();
			listener.Prefixes.Add(_prefix);
			listener.Start();
			listener.BeginGetContext(ar => {
				HandleGetContext(ar, listener);
			}, null);
		}
		
		void HandleGetContext(IAsyncResult ar, HttpListener listener) {
			var c = listener.EndGetContext(ar);
			HandleRequest(c);
		}
		
		void HandleRequest(HttpListenerContext c) {
			
			var queryParams = c.Request.QueryString.Count;
			var path = c.Request.Url.AbsolutePath;
			
			var r = new Request(c.Request);
			
			if (r.IsValid) {
				c.Response.StatusCode = 200;
				c.Response.SendChunked = true;
				var w = new StreamWriter(c.Response.OutputStream);
				for (int i = 0; i < 100; i++) {
					w.Write("hello();\r\nthere();\r\n");
					w.Flush();
					c.Response.OutputStream.Flush();
					System.Threading.Thread.Sleep(500);
				}
				w.Flush();
			}
			else {
				c.Response.StatusCode = 500;
			}
		}

		public class Request {
			
			public string ClientId { get; private set; }
			
			public bool IsValid { get; private set; }
			
			public Request(HttpListenerRequest req) {
				IsValid = true;
			}
		}
    }
	
	

}
