using System;

using Data;

namespace Examples
{
	public class Tweet
	{
		[PrimaryKey, AutoIncrement]
		public int Id { get; private set; }
		public string From { get; private set; }		
		public string Text { get; private set; }		
	}
}
