using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;
using JsonSerialization;

namespace Data.Subscriptions	
{
	public class ClientRepo
	{
		public ServerRef Server { get; private set; }
		public string ClientId { get; private set; }
		public Dbi Dbi { get; private set; }

		public ClientRepo (ServerRef server, string clientId)
		{
			Server = server;
			ClientId = clientId;
			Dbi = new MySqlDbi ();
		}

		public IDataChannel<T> Subscribe<T> (TableQuery<T> query) where T : new()
		{
			var cq = query.Compile ();
			
			var cs = ClientServerInterfaces.GetInterface (Server, ClientId);
			
			return cs.Subscribe<T> (cq);
		}

		public TableQuery<T> Table<T> () where T : new()
		{
			return new TableQuery<T> (null, Dbi, new TableMapping (typeof(T)));
		}
	}

	public interface IServerSubscription
	{
		string SubscriptionId { get; set; }
		SqlQuery Query { get; }
		void ProcessAll(object[] all);
		void ProcessInserted(object value);
		void ProcessUpdated(object value);
		void ProcessDeleted(object value);
	}

	public class ServerSubscription<T> : DataChannelBase<T>, IServerSubscription
	{
		public string SubscriptionId { get; set; }

		public SqlQuery Query { get; private set; }

		ClientServerInterface _client;
		
		public ServerSubscription (ClientServerInterface client, string subscriptionId, SqlQuery cq)
		{
			_client = client;
			Query = cq;
			SubscriptionId = subscriptionId;
		}
		
		public override void BeginGetAll ()
		{
			_client.GetAll(this);
		}
		
		public void ProcessAll(object[] all) {
			throw new NotImplementedException("ProcessAll");
		}
		public void ProcessInserted(object value) {
			throw new NotImplementedException("ProcessInserted");
		}
		public void ProcessUpdated(object value) {
			throw new NotImplementedException("ProcessUpdated");
		}
		public void ProcessDeleted(object value) {
			throw new NotImplementedException("ProcessDeleted");
		}
	}
	
	public interface ITypeResolver {
		Type FindType(string name);
	}

	public class ClientServerInterface : ITypeResolver
	{
		Dictionary<string, IServerSubscription> _subsById = new Dictionary<string, IServerSubscription> ();
		object _subsLock = new object();

		ServerRef Server { get; set; }
		string ClientId { get; set; }

		HttpWebResponse _recvResp;
		Stream _recvRespStream;
		DateTime _lastConnectTryTime;
		
		Dictionary<string, Type> _subTypes;

		TimeSpan ConnectRetryTimeSpan { get; set; }

		string _streamUrl;
		string _rpcUrl;

		public ClientServerInterface (ServerRef server, string clientId)
		{
			_subTypes = new Dictionary<string, Type>();
			
			Server = server;
			ClientId = clientId;
			ConnectRetryTimeSpan = TimeSpan.FromSeconds (5);
			
			_streamUrl = Server.Host + "/" + ClientId + "/stream?key=";
			_rpcUrl = Server.Host + "/" + ClientId + "/rpc?key=";
			
			var recvTh = new System.Threading.Thread ((System.Threading.ThreadStart)delegate { RecvLoop (); });
			recvTh.Start ();
			var sendTh = new System.Threading.Thread ((System.Threading.ThreadStart)delegate { SendLoop (); });
			sendTh.Start ();
		}

		public ServerSubscription<T> Subscribe<T> (SqlQuery query)
		{
			IServerSubscription sub = null;
			var needsSubscribeRpc = false;
			
			lock (_subsLock) {
				foreach (var ch in _subsById.Values) {
					if (ch.Query == query) {
						sub = ch;
						break;
					}
				}
				if (sub == null) {
					needsSubscribeRpc = true;
					
					var tempId = "?" + query.GetHashCode();
					sub = new ServerSubscription<T>(this, tempId, query);
					_subsById.Add(tempId, sub);
					
					var ty = typeof(T);
					_subTypes[ty.Name] = ty;
				}
			}
			
			if (needsSubscribeRpc) {
				QueueRpc("subscribe", query.TypeName, null, SubscribeArgs(query), true, result => {
					SetSubscriptionId(sub, result.Trim());				
				});
			}
			
			return (ServerSubscription<T>)sub;
		}
		
		string SubscribeArgs(SqlQuery query) {
			var s = Json.Encode(query.SqlText) + ",[";
			s += string.Join(",", query.Arguments.Select(a => Json.Encode(a)).ToArray());
			s += "]";
			return s;
		}
		
		/*string UnsubscribeRpc(IServerChannel ch) {
			var s = "unsubscribe(\"" + ch.Query.TypeName + "\"";
			s += "," + ch.SubscriptionId;
			s += ");";
			return s;
		}*/
		
		public void GetAll(IServerSubscription sub) {
			QueueRpc("all", sub.Query.TypeName, sub, "0", true, result => {				
			});
		}
		
		void SetSubscriptionId(IServerSubscription sub, string id) {
			lock (_subsLock) {
				_subsById.Remove(sub.SubscriptionId);
				sub.SubscriptionId = id;
				_subsById[id] = sub;
			}
			_newRpcsAvailable.Set();
		}
		
		IServerSubscription GetChannel(string subscriptionId) {
			IServerSubscription channel;
			lock (_subsLock) {
				if (_subsById.TryGetValue(subscriptionId, out channel)) {
					return channel;
				}
			}
			throw new ApplicationException("Channel not found (subscriptionId = " + subscriptionId + ")");
		}
		
		public Type FindType(string name) {
			lock (_subsLock) {
				foreach (var ty in _subTypes.Values) {
					if (ty.Name == name) {
						return ty;
					}
				}
			}
			throw new ApplicationException("Type " + name + " not found");
		}

		bool IsRecving {
			get { return _recvRespStream != null; }
		}

		void CloseRecv ()
		{
			try {
				if (_recvRespStream != null)
					_recvRespStream.Close ();
				_recvRespStream = null;
			} catch (Exception) {
			}
			try {
				if (_recvResp != null)
					_recvResp.Close ();
				_recvResp = null;
			} catch (Exception) {
			}
		}

		void ProcessRecvdMessage (string msg)
		{
			try {
				var rpc = Rpc.Parse (msg, this);
				
				var subId = Convert.ToString (rpc.Arguments[0]);
				
				var channel = GetChannel (subId);
				
				Console.WriteLine (	"PROCESSING " + msg);
				
				if (rpc.Procedure == "all") {
					channel.ProcessAll((object[])rpc.Arguments[1]);
				} else if (rpc.Procedure == "inserted") {
					channel.ProcessInserted(rpc.Arguments[1]);
				} else if (rpc.Procedure == "updated") {
					channel.ProcessUpdated(rpc.Arguments[1]);
				} else if (rpc.Procedure == "deleted") {
					channel.ProcessDeleted(rpc.Arguments[1]);
				} else {
					throw new NotSupportedException ("Unknown RPC " + rpc.Procedure);
				}
			} catch (Exception error) {
				LogProcessError (msg, error);
			}
		}

		int ParseAndProcessRecvdMessages (byte[] buffer, int bufferLength)
		{
			var endIdx = 0;
			var beginIdx = 0;
			
			for (;;) {
				for (beginIdx = endIdx; (beginIdx < bufferLength) && (buffer[beginIdx] <= 0x20); beginIdx++) {
				}
				if (beginIdx >= bufferLength) {
					return 0;
				}
				
				endIdx = -1;
				for (int i = beginIdx; i < bufferLength - 3 && endIdx < 0; i++) {
					if (buffer[i] == ')' && buffer[i + 1] == ';' && buffer[i + 2] == '\r' && buffer[i + 3] == '\n') {
						endIdx = i + 4;
					}
				}
				if (endIdx < 0) {
					var len = bufferLength - beginIdx;
					Array.Copy(buffer, beginIdx, buffer, 0, len);
					return len;
				}
				
				var msgLen = endIdx - beginIdx;
				var msg = System.Text.Encoding.UTF8.GetString (buffer, beginIdx, msgLen);				
				
				ProcessRecvdMessage (msg);
			}
		}

		string NewStreamUrl ()
		{
			return _streamUrl + Keys.GenKey (ClientId);
		}
		
		string NewRpcUrl ()
		{
			return _rpcUrl + Keys.GenKey (ClientId);
		}

		void RecvLoop ()
		{
			byte[] msgBuffer = new byte[64 * 1024];
			int msgOffset = 0;
			
			for (;;) {
				if (IsRecving) {
					try {
						var n = _recvRespStream.Read (msgBuffer, msgOffset, msgBuffer.Length - msgOffset);
						
						if (n > 0) {
							msgOffset = ParseAndProcessRecvdMessages (msgBuffer, msgOffset + n);
						} else if (n == 0) {
						} else {
							CloseRecv ();
						}
					} catch (Exception ex) {
						LogRecvError (ex);
						CloseRecv ();
					}
				} else {
					var now = DateTime.Now;
					var dt = now - _lastConnectTryTime;
					if (dt > ConnectRetryTimeSpan) {
						_lastConnectTryTime = now;
						
						try {
							var req = (HttpWebRequest)WebRequest.Create (NewStreamUrl ());
							req.KeepAlive = true;
							_recvResp = (HttpWebResponse)req.GetResponse ();
							if (_recvResp.StatusCode != HttpStatusCode.OK) {
								throw new ApplicationException ("Server returned status code " + _recvResp.StatusCode);
							}
							_recvRespStream = _recvResp.GetResponseStream ();
						} catch (Exception ex) {
							LogConnectError (ex);
							CloseRecv ();
						}
					} else {
						System.Threading.Thread.Sleep (ConnectRetryTimeSpan - dt);
					}
				}
			}
		}
		
		class PendingRpc {
			public string Procedure { get; set; }
			public string TypeName { get; set; }
			public IServerSubscription Sub { get; set; }
			public string JsonArgs { get; set; }
			public bool SafeToResend { get; set; }
			public Action<string> Callback { get; set; }
			
			public string Json {
				get {
					var j = Procedure + "(";
					j += "\"" + TypeName + "\",";
					if (Sub != null) {
						j += "\"" + Sub.SubscriptionId + "\",";
					}
					j += JsonArgs;
					j += ");";
					return j;
				}
			}
		}
		
		List<PendingRpc> _queuedRpcs = new List<PendingRpc>();
		object _sendsLock = new object();
		System.Threading.AutoResetEvent _newRpcsAvailable = new System.Threading.AutoResetEvent(false);
		
		void QueueRpc(string procedure, string typeName, IServerSubscription sub, string jsonArgs, bool safeToResend, Action<string> k) {
			lock (_sendsLock) {
				_queuedRpcs.Add(new PendingRpc() { 
					Procedure = procedure,
					TypeName = typeName,
					Sub = sub,
					JsonArgs = jsonArgs, 
					SafeToResend = safeToResend, 
					Callback = k});
				_newRpcsAvailable.Set();
			}
		}

		void SendLoop ()
		{
			for (;;) {
				
				List<PendingRpc> workingRpcs = new List<PendingRpc>();
				
				try {
					workingRpcs.Clear();
					var remainingRpcs = new List<PendingRpc>();
					
					lock (_sendsLock) {
						foreach (var rpc in _queuedRpcs) {
							if (rpc.Sub != null && rpc.Sub.SubscriptionId.StartsWith("?")) {
								remainingRpcs.Add(rpc);
							}
							else {
								workingRpcs.Add(rpc);
							}
						}
						_queuedRpcs = remainingRpcs;
					}
					
					if (workingRpcs.Count > 0) {
						var bodyQ = from rpc in workingRpcs
									select rpc.Json;
						var body = string.Join("\r\n", bodyQ.ToArray());
						
						Console.WriteLine ("SENDING RPC: " + body);
						
						var bodyBytes = System.Text.Encoding.UTF8.GetBytes(body);
						
						var req = (HttpWebRequest)WebRequest.Create(NewRpcUrl());
						req.Method = "POST";
						req.ContentLength = bodyBytes.Length;
						
						var s = req.GetRequestStream();
						s.Write(bodyBytes, 0, bodyBytes.Length);
						s.Flush();
						
						var respText = "";
						
						using (var resp = req.GetResponse()) {
							using (var rs = resp.GetResponseStream()) {
								using (var rr = new StreamReader(rs, Encoding.UTF8)) {
									respText = rr.ReadToEnd();
								}
							}
						}
						
						var results = respText.Split(';');
						
						if (results.Length == workingRpcs.Count) {
							for (int i = 0; i < results.Length; i++) {
								try {
									if (workingRpcs[i].Callback != null) {
										workingRpcs[i].Callback(results[i]);
									}
								}
								catch (Exception) {
								}
							}
						}
						else {
							Console.WriteLine ("BAD RESPONSE FROM SERVER: " + respText);
						}
						workingRpcs = null;
					}
					else {
						_newRpcsAvailable.WaitOne();
					}
				}
				catch (Exception error) {
					
					if (workingRpcs.Count > 0) {
						lock (_sendsLock) {							
							_queuedRpcs.InsertRange(0, workingRpcs.Where(s => s.SafeToResend));
						}
						workingRpcs.Clear();
					}
					
					System.Threading.Thread.Sleep(5000);
					
					LogSendError(error);
				}
			}
		}

		void LogProcessError (string rawMsg, Exception error)
		{
			Console.WriteLine ("PROC ERROR: " + error.Message);
		}

		void LogSendError (Exception error)
		{
			Console.WriteLine ("SEND ERROR: " + error.Message);
		}

		void LogRecvError (Exception error)
		{
			Console.WriteLine ("RECV ERROR: " + error.Message);
		}

		void LogConnectError (Exception error)
		{
			Console.WriteLine ("CONNECT ERROR: " + error.Message);
		}
	}

	public class ClientServerInterfaces
	{
		static Dictionary<string, ClientServerInterface> _pollers = new Dictionary<string, ClientServerInterface> ();

		public static ClientServerInterface GetInterface (ServerRef server, string clientId)
		{
			var key = server.Host + "/" + clientId;
			ClientServerInterface p;
			if (!_pollers.TryGetValue (key, out p)) {
				p = new ClientServerInterface (server, clientId);
				_pollers.Add (key, p);
			}
			return p;
		}
	}
}
