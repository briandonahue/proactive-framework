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
			var s = new ServerRepo (prefix, typeof(Tweet).Assembly);
			s.Start ();
			
			Console.WriteLine ("Bound to " + prefix);
			
			Console.WriteLine ("Hit Return to exit");
			Console.ReadLine();
		}
	}
}
