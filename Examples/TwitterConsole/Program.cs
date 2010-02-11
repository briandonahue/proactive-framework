using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Data;
using Data.Subscriptions;

namespace Examples.TwitterConsole
{
    class Program
    {
        static void Main(string[] args) {

            if (args.Length > 0 && args[0] == "server") {
                RunServer(args.Skip(1).ToArray());
            }
            else {
                RunClient(args);
            }

        }

        static void RunServer(string[] args) {
        }

        static void RunClient(string[] args) {

            var clientId = args.Length > 0 ? args[0] : "client0";

            var client = new ClientRepo(new Server("http://localhost:31337"), clientId);

            var frankPosts = client.Table<Post>().Where(f => f.From == "Frank");

            var ch = client.Subscribe(frankPosts);
			
            System.Threading.Thread.Sleep(10000);
        }

    }
}
