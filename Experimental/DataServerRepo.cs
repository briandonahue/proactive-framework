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

	public class ServerRepo : ITypeResolver
	{
		Server _server;
		List<Type> _types;

		Dictionary<string, ClientHandler> _handlersByClientId;
		Dictionary<string, Dictionary<SqlQuery, QueryInfo>> _queryInfos;
		object _queryInfosLock = new object();
		
		Dictionary<string, Subscription> _subscriptions;

		public ServerRepo (string prefix, System.Reflection.Assembly typesAsm)
		{
			_subscriptions = new Dictionary<string, Subscription>();
			_types = new List<Type>(typesAsm.GetTypes());
			_server = new Server (this, prefix);
			_handlersByClientId = new Dictionary<string, ClientHandler> ();
			_queryInfos = new Dictionary<string, Dictionary<SqlQuery, QueryInfo>> ();
		}

		public void Start ()
		{
			_server.Start ();
		}
		
		public Type FindType(string name) {
			foreach (var ty in _types) {
				if (ty.Name == name) {
					return ty;
				}
			}
			throw new ApplicationException("Could not find type named " + name);
		}

		void Respond (Request r)
		{
			ClientHandler handler;
			if (!_handlersByClientId.TryGetValue (r.ClientId, out handler)) {
				handler = new ClientHandler (r.ClientId, this);
				_handlersByClientId.Add (r.ClientId, handler);
			}
			handler.Respond (r);
		}

		QueryInfo GetQueryInfo (SqlQuery query)
		{
			var fullTypeName = query.TypeName;
			
			QueryInfo info = null;
			
			lock (_queryInfosLock) {			
				Dictionary<SqlQuery, QueryInfo> tableQueries;
				if (!_queryInfos.TryGetValue (fullTypeName, out tableQueries)) {
					tableQueries = new Dictionary<SqlQuery, QueryInfo> ();
					_queryInfos.Add (fullTypeName, tableQueries);
				}
				
				if (!tableQueries.TryGetValue (query, out info)) {
					info = new QueryInfo (query);
					tableQueries.Add(query, info);
				}
			}
			
			return info;
		}
		
		Dictionary<string, Subscription> _subs = new Dictionary<string, Subscription>();
		object _subsLock = new object();
		
		Subscription Subscribe(ClientHandler client, SqlQuery query) {
			var info = GetQueryInfo(query);
			var sub = info.Subscribe(client);
			lock (_subsLock) {
				if (!_subs.ContainsKey(sub.Id)) {
					_subs.Add(sub.Id, sub);
				}
			}
			return sub;
		}
		
		void Unsubscribe(ClientHandler client, string subId) {			
			Subscription sub = null;
			lock (_subsLock) {
				_subs.TryGetValue(subId, out sub);
				if (sub != null) {
					_subs.Remove(subId);
				}
			}
			if (sub != null) {
				var info = GetQueryInfo(sub.Query);
				info.Unsubscribe(sub);
			}
		}
		
		class Subscription
		{
			static int _nextId = 1;
			
			public string Id { get; private set; }
			public ClientHandler Client { get; private set; }
			public SqlQuery Query { get; private set; }
			
			public DateTime LastStreamTime { get; set; }
			
			public bool AllRequested { get; private set; }
			public void RequestAll() {
				AllRequested = true;
			}
			public void SentAll() {
				AllRequested = false;
			}
			
			public Subscription(ClientHandler client, SqlQuery query) {
				Id = System.Threading.Interlocked.Increment(ref _nextId).ToString();

				Client = client;
				Query = query;
			}
		}

		class QueryInfo
		{
			SqlQuery Query;
			
			Dictionary<string, Subscription> _subsByClientId = new Dictionary<string, Subscription>();
			Dictionary<string, Subscription> _subsById = new Dictionary<string, Subscription>();
			object _subsLock = new object();

			public QueryInfo (SqlQuery query)
			{
				Query = query;
			}
			
			public Subscription Subscribe(ClientHandler client) {
				Subscription sub = null;
				lock (_subsLock) {
					if (!_subsByClientId.TryGetValue(client.ClientId, out sub)) {
						
						sub = new Subscription(client, Query);
						
						_subsByClientId[client.ClientId] = sub;
						_subsById[sub.Id] = sub;
					}
				}
				return sub;
			}
			
			public void Unsubscribe(Subscription sub) {
				lock (_subsLock) {
					_subsByClientId.Remove(sub.Client.ClientId);
					_subsById.Remove(sub.Id);
				}
			}
		}

		class ClientHandler
		{
			public bool StreamActive { get { return GetStream() != null; } }
			
			public string ClientId { get; private set; }
			
			System.IO.Stream _clientStream;
			object _clientStreamLock = new object();
			ServerRepo _repo;
			
			Dictionary<string, Subscription> _subsById = new Dictionary<string, Subscription>();
			object _subsLock = new object();
			
			public ClientHandler(string clientId, ServerRepo repo) {
				
				_repo = repo;
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
					
					Console.WriteLine (ClientId + " RPC " + r);
					
					var rpcTexts = r.RequestBody.Split(new char[] { '\r','\n' }, StringSplitOptions.RemoveEmptyEntries);
					var resps = new string[rpcTexts.Length];
					for (int i = 0; i < rpcTexts.Length; i++) {
						var rpcText = rpcTexts[i];
						var resp = "";
						try {
							var rpc = Rpc.Parse(rpcText, _repo);
							resp = ProcessRpc(rpc);
						}
						catch (Exception error) {
							LogRpcError(error);
							resp = "";
						}
						resps[i] = resp;
						
					}
					var respBody = string.Join(";", resps);
					Console.WriteLine (ClientId + " -> " + respBody);
					var respBytes = Encoding.UTF8.GetBytes(respBody);
					
					r.ResponseStream.Write(respBytes, 0, respBytes.Length);
					r.ResponseStream.Close();
					
				} else if (r.Resource == "stream") {
					
					Console.WriteLine (ClientId + " Stream " + r);
					ResetStream(r.ResponseStream);
				
				}
			}
			
			string ProcessRpc(Rpc rpc) {				
				if (rpc.Procedure == "subscribe") {
					
					var sub = _repo.Subscribe(this, new SqlQuery(rpc.ValueType.Name, rpc.Arguments[0].ToString(), (object[])rpc.Arguments[1]));

					lock (_subsById) {
						_subsById[sub.Id] = sub;
					}
					
					SetHasDataToStream();
					
					return sub.Id;				
				}
				else if (rpc.Procedure == "unsubscribe") {
					
					var subId = rpc.Arguments[0].ToString();
					
					lock (_subsLock) {
						_subsById.Remove(subId);
					}
					
					return "1";
				}
				else if (rpc.Procedure == "all") {
					
					var subId = rpc.Arguments[0].ToString();
					var sub = GetSubscription(subId);
					sub.RequestAll();
					SetHasDataToStream();
					
					return "1";
				}
				else {
					throw new NotSupportedException("Server RPC " + rpc.Procedure);
				}				
			}
			
			Subscription GetSubscription(string subId) {
				lock (_subsLock) {
					Subscription sub = null;
					_subsById.TryGetValue(subId, out sub);
					return sub;
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
			
			System.Threading.AutoResetEvent _hasDataToStream = new System.Threading.AutoResetEvent(false);

			public void SetHasDataToStream() {
				_hasDataToStream.Set();
			}
						
			void StreamLoop() {
				
				byte[] padding = new byte[4*1024];
				for (int i = 0; i < padding.Length; i++) {
					padding[i] = 0x20;
				}
				
				for (;;) {
					try {
						_hasDataToStream.WaitOne();
						
						var s = GetStream();
						
						if (s != null) {
							var numBytes = 0;
							
							Action<string> write = str => {
								var bytes = Encoding.UTF8.GetBytes(str);
								numBytes += bytes.Length;
								s.Write(bytes, 0, bytes.Length);
							};
						
							Subscription[] subs = null;
							lock (_subsLock) {
								subs = _subsById.Values.ToArray();
							}
							
							foreach (var sub in subs) {
								if (sub.AllRequested) {
									sub.SentAll();
									write("all(\""+sub.Query.TypeName+"\","+sub.Id+",[{Id:348957,From:\"Frank\",Text:\"Hello World!\"}]);\r\n");
								}
							}
							
							//w.Write("updated(\""+sub.Query.TypeName+"\","+sub.Id+",[{Id:348957,From:\"Frank\",Text:\"Hello World!\"}]);\r\n");
							
							if (numBytes < padding.Length) {
								s.Write(padding, 0, padding.Length - numBytes);
							}
							
							s.Flush();
						}
					}
					catch (Exception error) {
						ResetStream(null);
						LogStreamingError(error);
					}
				}
			}
			
			void LogRpcError(Exception error) {
				Console.WriteLine (ClientId + " ERROR rpc: " + error);
			}
			
			void LogStreamingError(Exception error) {
				Console.WriteLine (ClientId + " ERROR stopped streaming: " + error.Message);
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
				BeginGet ();
				HandleRequest (c);
			}

			void HandleRequest (HttpListenerContext c)
			{
				try {
					var r = new Request (c);
					
					//Console.WriteLine (r);
					
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
