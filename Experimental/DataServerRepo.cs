using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;

namespace Data.Subscriptions
{
	public class ServerRef
	{
		public string Host { get; set; }
		public string OrmType { get; set; }

		public ServerRef (string host)
		{
			Host = host;
		}
	}

	public class ServerRepo
	{
		Server _server;
		System.Reflection.Assembly _typesAsm;

		Dictionary<string, ClientHandler> _handlersByClientId;
		Dictionary<string, Dictionary<SqlQuery, QueryInfo>> _queryInfos;

		public ServerRepo (string prefix, System.Reflection.Assembly typesAsm)
		{
			_typesAsm = typesAsm;
			_server = new Server (this, prefix);
			_handlersByClientId = new Dictionary<string, ClientHandler> ();
			_queryInfos = new Dictionary<string, Dictionary<SqlQuery, QueryInfo>> ();
		}

		public void Start ()
		{
			_server.Start ();
		}

		void Respond (Request r)
		{
			ClientHandler handler;
			if (!_handlersByClientId.TryGetValue (r.ClientId, out handler)) {
				handler = new ClientHandler (r.ClientId);
				_handlersByClientId.Add (r.ClientId, handler);
			}
			handler.Respond (r);
		}

		QueryInfo GetQueryInfo (SqlQuery query)
		{
			var fullTypeName = query.FullTypeName;
			
			Dictionary<SqlQuery, QueryInfo> tableQueries;
			if (!_queryInfos.TryGetValue (fullTypeName, out tableQueries)) {
				tableQueries = new Dictionary<SqlQuery, QueryInfo> ();
				_queryInfos.Add (fullTypeName, tableQueries);
			}
			
			QueryInfo info;
			if (!tableQueries.TryGetValue (query, out info)) {
				info = new QueryInfo (query);
			}
			
			return info;
		}

		class QueryInfo
		{
			SqlQuery Query;

			public QueryInfo (SqlQuery query)
			{
				Query = query;
			}
		}

		class ClientHandler
		{
			public bool StreamActive { get { return GetStream() != null; } }
			
			public string ClientId { get; private set; }
			
			System.IO.Stream _clientStream;
			object _clientStreamLock = new object();
			
			public ClientHandler(string clientId) {
				ClientId = clientId;
				var streamThread = new System.Threading.Thread((System.Threading.ThreadStart)delegate {
					StreamLoop();
				});
				streamThread.Start();
				Console.WriteLine (ClientId + " started streaming thread");
			}

			public void Respond (Request r)
			{
				if (r.Resource == "rpc") {
				} else if (r.Resource == "stream") {
					ResetStream(r.ResponseStream);
				}
			}
			
			System.IO.Stream GetStream() {
				lock (_clientStreamLock) {
					return _clientStream;
				}
			}
			
			void ResetStream(System.IO.Stream newStream) {
				lock (_clientStreamLock) {
					if (_clientStream != null) {						
						try {
							_clientStream.Close();
						}
						catch (Exception) {
						}
					}
					_clientStream = newStream;
				}
			}
						
			void StreamLoop() {
				for (;;) {					
					try {						
						var s = GetStream();
						
						if (s != null) {							
							var w = new StreamWriter(s);							
							
							w.Write("all(\"Tweet\",[{Id:348957,From:\"Frank\",Text:\"Hello World!\"}]);\r\n");
							w.Write("inserted(\"Tweet\",{Id:348958,From:\"Frank\",Text:\"Hello World, again!\"});\r\n");
							
							w.Flush();
							s.Flush();
							
							System.Threading.Thread.Sleep(500);
						}
						else {
							System.Threading.Thread.Sleep(5000);
						}
					}
					catch (Exception error) {						
						ResetStream(null);
						LogStreamingError(error);
					}
				}
			}
			
			void LogStreamingError(Exception error) {
				Console.WriteLine (ClientId + " stopped streaming: " + error.Message);
			}
		}

		class Server
		{
			string _prefix;
			HttpListener _listener;
			ServerRepo _repo;

			public Server (ServerRepo repo, string prefix)
			{
				_repo = repo;
				_prefix = prefix;
			}

			public void Start ()
			{
				_listener = new HttpListener ();
				_listener.Prefixes.Add (_prefix);
				_listener.Start ();
				BeginGet ();
			}

			void BeginGet ()
			{
				_listener.BeginGetContext (HandleGetContext, null);
			}

			void HandleGetContext (IAsyncResult ar)
			{
				var c = _listener.EndGetContext (ar);
				HandleRequest (c);
				BeginGet ();
			}

			void HandleRequest (HttpListenerContext c)
			{
				try {
					var r = new Request (c);
					
					Console.WriteLine (r);
					
					if (!r.IsValid) {
						c.Response.StatusCode = 400;
						// Bad Request
					} else if (!r.IsAuthenticated) {
						c.Response.StatusCode = 403;
						// Forbidden
					} else {
						c.Response.StatusCode = 200;
						c.Response.SendChunked = true;
						_repo.Respond (r);
					}
				} catch (System.IO.IOException) {
				} catch (Exception error) {
					LogRequestError (error);
				}
			}

			void LogRequestError (Exception error)
			{
				Console.WriteLine ("REQUEST ERROR " + error.Message);
			}
			
		}

		class Request
		{
			public bool IsValid { get; private set; }
			public bool IsAuthenticated { get; private set; }
			public string ClientId { get; private set; }
			public string Resource { get; private set; }
			public string RequestBody { get; private set; }
			public string Method { get; private set; }

			public System.IO.Stream ResponseStream { get; private set; }

			public Request (HttpListenerContext c)
			{
				try {
					var req = c.Request;
					var queryParams = req.QueryString;
					var path = req.Url.AbsolutePath;
					
					Method = req.HttpMethod.ToUpperInvariant ();
					
					var key = queryParams["key"];
					IsAuthenticated = Keys.IsKeyValid (key);
					
					var parts = path.Split (new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
					
					ClientId = parts[0];
					
					Resource = parts[1];
					
					if (Resource == "stream") {
						if (Method != "GET") {
							throw new Exception ("stream should be GET");
						}
					} else if (Resource == "rpc") {
						if (Method != "POST") {
							throw new Exception ("rpc should be POST");
						}
					} else {
						throw new Exception ("Unknown resource");
					}
					
					if (Method == "POST" && IsAuthenticated) {
						using (var r = new System.IO.StreamReader (c.Request.InputStream, System.Text.Encoding.UTF8)) {
							RequestBody = r.ReadToEnd ();
						}
					} else {
						RequestBody = "";
					}
					
					ResponseStream = c.Response.OutputStream;
					
					IsValid = true;
				} catch (Exception error) {
					LogError (error);
					IsValid = false;
				}
			}

			void LogError (Exception error)
			{
				Console.WriteLine ("REQUEST ERROR " + error.Message);
			}

			public override string ToString ()
			{
				return string.Format ("[Request: IsValid={0}, IsAuthenticated={1}, ClientId={2}, Resource={3}, RequestBody={4}, Method={5}]", IsValid, IsAuthenticated, ClientId, Resource, RequestBody, Method);
			}
		}
	}
}
