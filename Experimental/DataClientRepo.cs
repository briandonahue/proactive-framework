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
		Dictionary<SqlQuery, IChannel> _channels = new Dictionary<SqlQuery, IChannel> ();

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
			IChannel ch;
			
			var cq = query.Compile ();
			
			if (!_channels.TryGetValue (cq, out ch)) {
				ch = new ServerChannel<T> (Server, ClientId, cq);
			}
			
			return (IDataChannel<T>)ch;
		}

		public TableQuery<T> Table<T> () where T : new()
		{
			
			return new TableQuery<T> (null, Orm, new TableMapping (typeof(T)));
			
		}
		
	}

	public class ServerChannel<T> : DataChannelBase<T>
	{
		ServerRef Server { get; set; }
		string ChannelId { get; set; }
		string ClientId { get; set; }

		SqlQuery Query { get; set; }

		public ServerChannel (ServerRef server, string clientId, SqlQuery cq)
		{
			Server = server;
			ClientId = clientId;
			Query = cq;
			var s = ClientServerInterfaces.GetInterface (server, clientId, typeof(T).Assembly);
			s.Register (this);
		}
	}

	public class ClientServerInterface
	{
		List<object> _channels = new List<object> ();

		ServerRef Server { get; set; }
		string ClientId { get; set; }

		HttpWebResponse _resp;
		Stream _respStream;
		DateTime _lastConnectTryTime;
		System.Reflection.Assembly _typesAsm;

		TimeSpan ConnectRetryTimeSpan { get; set; }

		string _streamUrl;
		string _rpcUrl;

		public ClientServerInterface (ServerRef server, string clientId, System.Reflection.Assembly typesAsm)
		{
			_typesAsm = typesAsm;
			
			Server = server;
			ClientId = clientId;
			ConnectRetryTimeSpan = TimeSpan.FromSeconds (5);
			
			_streamUrl = Server.Host + "/" + ClientId + "/stream?key=";
			_rpcUrl = Server.Host + "/" + ClientId + "/rpc?key=";
			
			var recvTh = new System.Threading.Thread ((System.Threading.ThreadStart)delegate { RecvLoop (); });
			recvTh.Start ();
		}

		public void Register<T> (ServerChannel<T> ch)
		{
			_channels.Add (ch);
		}

		bool IsRecving {
			get { return _respStream != null; }
		}

		void CloseRecv ()
		{
			try {
				if (_respStream != null)
					_respStream.Close ();
				_respStream = null;
			} catch (Exception) {
			}
			try {
				if (_resp != null)
					_resp.Close ();
				_resp = null;
			} catch (Exception) {
			}
		}

		void Process (string msg)
		{
			try {
				Console.WriteLine ("READING " + msg);
				
				var rpc = Rpc.Parse(msg, _typesAsm);
				
				Console.WriteLine ("PROCESSING " + rpc);
			} catch (Exception error) {
				OnProcessError (error);
			}
		}

		int ParseAndProcessMessages (byte[] buffer, int bufferLength)
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
				
				Process (msg);
			}
		}
		
		string NewStreamUrl() {
			return _streamUrl + Keys.GenKey(ClientId);
		}

		void RecvLoop ()
		{
			byte[] msgBuffer = new byte[64 * 1024];
			int msgOffset = 0;
			
			for (;;) {
				if (IsRecving) {
					try {
						var n = _respStream.Read (msgBuffer, msgOffset, msgBuffer.Length - msgOffset);
						
						if (n > 0) {
							msgOffset = ParseAndProcessMessages (msgBuffer, msgOffset + n);
						} else if (n == 0) {
						} else {
							CloseRecv ();
						}
					} catch (Exception ex) {
						OnRecvError (ex);
						CloseRecv ();
					}
				} else {
					var now = DateTime.Now;
					var dt = now - _lastConnectTryTime;
					if (dt > ConnectRetryTimeSpan) {
						_lastConnectTryTime = now;
						
						try {
							var req = (HttpWebRequest)WebRequest.Create (NewStreamUrl());
							req.KeepAlive = true;
							_resp = (HttpWebResponse)req.GetResponse ();
							if (_resp.StatusCode != HttpStatusCode.OK) {
								throw new ApplicationException ("Server return status code " + _resp.StatusCode);
							}
							_respStream = _resp.GetResponseStream ();
						} catch (Exception ex) {
							OnConnectError (ex);
							CloseRecv ();
						}
					} else {
						System.Threading.Thread.Sleep (ConnectRetryTimeSpan - dt);
					}
				}
			}
		}

		void SendLoop ()
		{
			//
			// Register the channels that haven't been
			//
			throw new NotImplementedException ();
		}

		void OnProcessError (Exception error)
		{
			Console.WriteLine ("PROC ERROR: " + error.Message);
		}

		void OnSendError (Exception error)
		{
			Console.WriteLine ("SEND ERROR: " + error.Message);
		}

		void OnRecvError (Exception error)
		{
			Console.WriteLine ("RECV ERROR: " + error.Message);
		}

		void OnConnectError (Exception error)
		{
			Console.WriteLine ("CONNECT ERROR: " + error.Message);
		}
	}

	public class ClientServerInterfaces
	{
		static Dictionary<string, ClientServerInterface> _pollers = new Dictionary<string, ClientServerInterface> ();

		public static ClientServerInterface GetInterface (ServerRef server, string clientId, System.Reflection.Assembly typesAsm)
		{
			var key = server.Host + "/" + clientId;
			ClientServerInterface p;
			if (!_pollers.TryGetValue (key, out p)) {
				p = new ClientServerInterface (server, clientId, typesAsm);
				_pollers.Add (key, p);
			}
			return p;
		}
	}
}
