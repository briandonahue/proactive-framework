using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;

namespace Data.Subscriptions
{
	public class ClientRepo
	{
		public ServerRef Server { get; private set; }
		public string ClientId { get; private set; }
		public Orm Orm { get; private set; }

		public ClientRepo (ServerRef server, string clientId)
		{
			Server = server;
			ClientId = clientId;
			Orm = new MySqlOrm ();
		}

		public IDataChannel<T> Subscribe<T> (TableQuery<T> query) where T : new()
		{
			var cq = query.Compile ();
			
			var cs = ClientServerInterfaces.GetInterface (Server, ClientId);
			
			return cs.Subscribe<T> (cq);
		}

		public TableQuery<T> Table<T> () where T : new()
		{
			return new TableQuery<T> (null, Orm, new TableMapping (typeof(T)));
		}
	}

	public interface IServerChannel
	{
		string ChannelId { get; set; }
		SqlQuery Query { get; }
		void ProcessAll(object[] all);
		void ProcessInserted(object value);
		void ProcessUpdated(object value);
		void ProcessDeleted(object value);
	}

	public class ServerChannel<T> : DataChannelBase<T>, IServerChannel
	{
		public string ChannelId { get; set; }

		public SqlQuery Query { get; private set; }

		public ServerChannel (string channelId, SqlQuery cq)
		{
			Query = cq;
			ChannelId = channelId;
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
		Dictionary<string, IServerChannel> _channels = new Dictionary<string, IServerChannel> ();
		object _channelsLock = new object();

		ServerRef Server { get; set; }
		string ClientId { get; set; }

		HttpWebResponse _recvResp;
		Stream _recvRespStream;
		DateTime _lastConnectTryTime;
		
		Dictionary<string, Type> _subsTypes;

		TimeSpan ConnectRetryTimeSpan { get; set; }

		string _streamUrl;
		string _rpcUrl;

		public ClientServerInterface (ServerRef server, string clientId)
		{
			_subsTypes = new Dictionary<string, Type>();
			
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

		public ServerChannel<T> Subscribe<T> (SqlQuery query)
		{
			IServerChannel channel = null;
			var needsSubscribeRpc = false;
			
			lock (_channelsLock) {
				foreach (var ch in _channels.Values) {
					if (ch.Query == query) {
						channel = ch;
						break;
					}
				}
				if (channel == null) {
					needsSubscribeRpc = true;
					
					var tempId = "?" + query.GetHashCode();
					channel = new ServerChannel<T>(tempId, query);
					_channels.Add(tempId, channel);
					
					var ty = typeof(T);
					_subsTypes[ty.FullName] = ty;
				}
			}
			
			if (needsSubscribeRpc) {
				QueueRpc(GetSubscribeRpc(query), true, result => {				
					SetChannelId(channel, result.Trim());				
				});
			}
			
			return (ServerChannel<T>)channel;
		}
		
		string GetSubscribeRpc(SqlQuery query) {
			var s = "subscribe(\"" + query.TypeName + "\"";
			s += ");";
			return s;
		}
		
		/*string GetUnsubscribeRpc(IServerChannel ch) {
			var s = "unsubscribe(\"" + ch.Query.TypeName + "\"";
			s += "," + ch.ChannelId;
			s += ");";
			return s;
		}*/
		
		void SetChannelId(IServerChannel channel, string id) {
			lock (_channelsLock) {
				if (_channels.ContainsKey(channel.ChannelId)) {
					_channels.Remove(channel.ChannelId);
				}
				channel.ChannelId = id;
				_channels[id] = channel;
			}
		}
		
		IServerChannel GetChannel(string channelId) {
			IServerChannel channel;
			lock (_channelsLock) {
				if (_channels.TryGetValue(channelId, out channel)) {
					return channel;
				}
			}
			throw new ApplicationException("Channel not found (channelId = " + channelId + ")");
		}
		
		public Type FindType(string name) {
			lock (_channelsLock) {
				foreach (var ty in _subsTypes.Values) {
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
				
				var channelId = Convert.ToString (rpc.Arguments[0]);
				
				var channel = GetChannel (channelId);
				
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
			var n = bufferLength;
			
			for (;;) {
				var endIdx = -1;
				for (int i = 0; i < n - 3 && endIdx < 0; i++) {
					if (buffer[i] == ')' && buffer[i + 1] == ';' && buffer[i + 2] == '\r' && buffer[i + 3] == '\n') {
						endIdx = i;
					}
				}
				if (endIdx < 0) {
					return n;
				}
				var msgLen = endIdx + 4;
				var msg = System.Text.Encoding.UTF8.GetString (buffer, 0, msgLen);
				
				Array.Copy (buffer, msgLen, buffer, 0, n - msgLen);
				
				n -= msgLen;
				
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
			public string Json { get; set; }
			public bool SafeToResend { get; set; }
			public Action<string> Callback { get; set; }
		}
		
		List<PendingRpc> _queuedRpcs = new List<PendingRpc>();
		object _sendsLock = new object();
		System.Threading.AutoResetEvent _newRpcs = new System.Threading.AutoResetEvent(false);
		
		void QueueRpc(string json, bool safeToResend, Action<string> k) {
			lock (_sendsLock) {
				_queuedRpcs.Add(new PendingRpc() { Json = json, SafeToResend = safeToResend, Callback = k});
				_newRpcs.Set();
			}
		}

		void SendLoop ()
		{
			for (;;) {
				
				PendingRpc[] workingRpcs = null;
				
				try {
					lock (_sendsLock) {
						if (_queuedRpcs.Count > 0) {
							workingRpcs = _queuedRpcs.ToArray();
							_queuedRpcs.Clear();
						}						
					}
					
					if (workingRpcs != null) {
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
									rr.ReadToEnd();
								}
							}
						}
						
						var results = respText.Split(';');
						
						if (results.Length == workingRpcs.Length) {
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
						_newRpcs.WaitOne();
					}
				}
				catch (Exception error) {
					
					if (workingRpcs != null) {
						lock (_sendsLock) {							
							_queuedRpcs.InsertRange(0, workingRpcs.Where(s => s.SafeToResend));
						}
						workingRpcs = null;
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
