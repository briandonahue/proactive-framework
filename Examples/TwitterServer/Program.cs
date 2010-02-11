using System;

using Data;
using Data.Subscriptions;

namespace Examples.TwitterServer
{
	class MainClass
	{
		public static void Main (string[] args)
		{
			var prefix = "http://*:31337/";
			var s = new ServerRepo(prefix);
			s.Start();
			
			Console.WriteLine ("Bound to " + prefix);
			
			System.Threading.Thread.Sleep(1000000);
		}
	}
}
