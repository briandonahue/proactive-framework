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

        public Server(string host) {
            Host = host;
        }
    }

    public class ClientRepo
    {
        Dictionary<CompiledQuery, IChannel> _channels = new Dictionary<CompiledQuery, IChannel>();

        public Server Server { get; private set; }
        public string ClientId { get; private set; }

        public ClientRepo(Server server, string clientId) {
            Server = server;
            ClientId = clientId;
        }

        public IDataChannel<T> Subscribe<T>(TableQuery<T> query) where T : new() {
            IChannel ch;

            var cq = query.Compile();

            if (!_channels.TryGetValue(cq, out ch)) {
                ch = new ServerChannel<T>(Server, ClientId, cq);
            }

            return (IDataChannel<T>)ch;
        }

        public TableQuery<T> Table<T>() where T : new() {

            return new TableQuery<T>(null, new TableMapping(typeof(T)));

        }

    }

    public class Post
    {
        public string From { get; private set; }
        public string Text { get; set; }
    }

    public class ServerChannel<T> : DataChannelBase<T>
    {
        Server Server { get; set; }
        string ChannelId { get; set; }
        string ClientId { get; set; }

        CompiledQuery Query { get; set; }

        public ServerChannel(Server server, string clientId, CompiledQuery cq) {
            Server = server;
            ClientId = clientId;
            Query = cq;
            var s = ServerInterfaces.GetInterface(server, clientId);
        }
    }

    public class ServerInterface
    {
        List<object> _channels = new List<object>();

        Server Server { get; set; }
        string ClientId { get; set; }

        WebResponse _resp;
        Stream _respStream;
        DateTime _lastConnectTryTime;

        TimeSpan ConnectRetryTimeSpan { get; set; }

        public ServerInterface(Server server, string clientId) {
            Server = server;
            ClientId = clientId;
            ConnectRetryTimeSpan = TimeSpan.FromSeconds(10);

            var recvTh = new System.Threading.Thread((System.Threading.ThreadStart)delegate {
                RecvLoop();
            });
            recvTh.Start();
        }

        string Url {
            get {
                return Server.Host + "/" + ClientId;
            }
        }

        bool IsRecving { get { return _respStream != null; } }

        void CloseRecv() {
            try {
                if (_respStream != null) _respStream.Close();
                _respStream = null;
            }
            catch (Exception) { }
            try {
                if (_resp != null) _resp.Close();
                _resp = null;
            }
            catch (Exception) { }
        }

        void Process(string msg) {

            Console.WriteLine("PROCESSING " + msg);

        }

        int ParseAndProcessMessages(byte[] buffer, int bufferLength) {
            var endIdx = -1;
            for (int i = 0; i < bufferLength-3 && endIdx < 0; i++) {
                if (buffer[i] == ')' && buffer[i + 1] == ';' && buffer[i + 2] == '\r' && buffer[i + 3] == '\n') {
                    endIdx = i;
                }
            }
            if (endIdx < 0) {
                return bufferLength;
            }
            var msgLen = endIdx + 4;
            var msg = System.Text.Encoding.UTF8.GetString(buffer, 0, msgLen);

            Array.Copy(buffer, msgLen, buffer, 0, bufferLength - msgLen);

            Process(msg);

            return msgLen;
        }

        void RecvLoop() {
            byte[] msgBuffer = new byte[64 * 1024];
            int msgOffset = 0;

            for (; ; ) {
                if (IsRecving) {
                    try {
                        var n = _respStream.Read(msgBuffer, msgOffset, msgBuffer.Length - msgOffset);

                        if (n > 0) {
                            msgOffset = ParseAndProcessMessages(msgBuffer, msgOffset + n);
                        }
                        else if (n == 0) {
                        }
                        else {
                            CloseRecv();
                        }
                    }
                    catch (Exception ex) {
                        OnRecvError(ex);
                        CloseRecv();
                    }
                }
                else {
                    var now = DateTime.Now;
                    var dt = now - _lastConnectTryTime;
                    if (dt > ConnectRetryTimeSpan) {
                        _lastConnectTryTime = now;

                        try {
                            var req = (HttpWebRequest)WebRequest.Create(Url);
                            req.KeepAlive = true;
                            _resp = req.GetResponse();
                            _respStream = _resp.GetResponseStream();
                        }
                        catch (Exception ex) {
                            OnConnectError(ex);
                            CloseRecv();
                        }
                    }
                    else {
                        System.Threading.Thread.Sleep(ConnectRetryTimeSpan - dt);
                    }
                }
            }
        }

        void SendLoop() {
            //
            // Register the channels that haven't been
            //
        }

        void OnProcessError(Exception error) {
            Console.WriteLine("PROC ERROR: " + error.Message);
        }

        void OnSendError(Exception error) {
            Console.WriteLine("SEND ERROR: " + error.Message);
        }

        void OnRecvError(Exception error) {
            Console.WriteLine("RECV ERROR: " + error.Message);
        }

        void OnConnectError(Exception error) {
            Console.WriteLine("CONNECT ERROR: " + error.Message);
        }
    }

    public class ServerInterfaces
    {
        static Dictionary<string, ServerInterface> _pollers = new Dictionary<string, ServerInterface>();

        public static ServerInterface GetInterface(Server server, string clientId) {
            var key = server.Host + "/" + clientId;
            ServerInterface p;
            if (!_pollers.TryGetValue(key, out p)) {
                p = new ServerInterface(server, clientId);
                _pollers.Add(key, p);
            }
            return p;
        }
    }
}
