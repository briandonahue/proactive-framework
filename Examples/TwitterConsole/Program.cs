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
		static void Main (string[] args)
		{
			
			var clientId = args.Length > 0 ? args[0] : "client0";
			
			var client = new ClientRepo (new ServerRef ("http://localhost:31337"), clientId);
			
			var frankPosts = client.Table<Tweet> ().Where (f => f.From == "Frank");
			
			var ch = client.Subscribe (frankPosts);
			
			ch.AllValues += tweets => {
				foreach (var p in tweets) {
					ShowPost (p);
				}
			};
			
			ch.BeginGetAll();
			
			ch.ValueInserted += ShowPost;
			
			System.Threading.Thread.Sleep (20000);
		}

		static void ShowPost (Tweet p)
		{
			Console.WriteLine (p.Text);
			Console.WriteLine ("  -" + p.From);
		}
		
	}
	
}
