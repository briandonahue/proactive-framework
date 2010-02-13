using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using JsonSerialization;

namespace Data.Subscriptions
{
	public interface IChannel
	{
		void Pause ();
		void Resume ();
		void Close ();
	}

	public interface IDataChannel<T> : IChannel
	{
		event Action<T[]> AllValues;
		event Action<T> ValueInserted;
		event Action<T> ValueUpdated;
		event Action<T> ValueDeleted;
		
		void BeginGetAll();

		void Update (T value);
		void Delete (T value);
	}

	public abstract class DataChannelBase<T> : IDataChannel<T>
	{
		protected bool Active { get; private set; }

		bool _gotAll = false;
		List<T> _allValues = new List<T> ();

		public event Action<T[]> AllValues;

		public event Action<T> ValueInserted;
		public event Action<T> ValueUpdated;
		public event Action<T> ValueDeleted;
		
		public DataChannelBase() {
		}
		
		public virtual void BeginGetAll() {
		}
		
		protected void SetAll(IEnumerable<T> values) {
			_allValues = new List<T>(values);
		}

		protected void RaiseInsert (T value)
		{
			_allValues.Add (value);
			if (Active && ValueInserted != null) {
				ValueInserted (value);
			}
		}

		protected void RaiseUpdated (T value)
		{
			if (Active && ValueUpdated != null) {
				ValueUpdated (value);
			}
		}

		protected void RaiseDeleted (T value)
		{
			if (Active && ValueDeleted != null) {
				ValueDeleted (value);
			}
		}

		public void Pause ()
		{
			Active = false;
		}

		public void Resume ()
		{
			Active = true;
		}

		public virtual void Close ()
		{
		}

		public void Update (T obj)
		{
			DoUpdate (obj);
			RaiseUpdated (obj);
		}

		public void Delete (T obj)
		{
			_allValues.Remove (obj);
			DoDelete (obj);
			RaiseDeleted (obj);
		}

		protected virtual void DoUpdate (T obj)
		{
		}

		protected virtual void DoDelete (T obj)
		{
		}
	}

	public static class Keys
	{
		public static string GenKey (string clientId)
		{
			return "foo";
		}

		public static bool IsKeyValid (string key)
		{
			if (string.IsNullOrEmpty (key))
				return false;
			return key == "foo";
		}
	}
	
	public class Rpc {
		public string Procedure { get; private set; }
		public Type ValueType { get; private set; }
		public object[] Arguments { get; private set; }
		
		public override string ToString ()
		{
			var args = string.Join(", ", Arguments.Select(a => a.ToString()).ToArray());			
			return string.Format("[Rpc: Procedure={0}, ValueType={1}, Arguments=[{2}]]", Procedure, ValueType, args);
		}

		
		public static Rpc Parse(string json, ITypeResolver types) {
			var rpc = new Rpc();
			var toks = new Json.Tokens(json);
			
			if (toks.Current != Json.Tokens.Identifier) {
				throw new JsonParseException("Expected Identifier got " + toks.Current);
			}
			rpc.Procedure = toks.CurrentValue;
			toks.Next();
			if (toks.Current != "(") {
				throw new JsonParseException("Expected ( got " + toks.Current);
			}
			toks.Next();
			if (toks.Current != Json.Tokens.String) {
				throw new JsonParseException("Expected String got " + toks.Current);
			}
			var typeName = toks.CurrentValue;
			rpc.ValueType = types.FindType(typeName);
			toks.Next();
			if (toks.Current != ",") {
				throw new JsonParseException("Expected , got " + toks.Current);
			}
			toks.Next();
			var args = new List<object>();
			while (toks.Current != ")" && toks.Current != Json.Tokens.EOF) {
				var arg = Json.ParseValue(rpc.ValueType, toks);
				if (toks.Current != "," && toks.Current != ")") {
					throw new JsonParseException("Expected , or ) got " + toks.Current);
				}
				if (toks.Current == ",") toks.Next();
				args.Add(arg);
			}
			rpc.Arguments = args.ToArray();
			toks.Next();
			if (toks.Current != ";") {
				throw new JsonParseException("Expected ; got " + toks.Current);
			}
			
			return rpc;
		}		
	}
	
	
	/*
	
	SERVER -> CLIENT Streaming
	
	all("TypeFullName", 29348, [{Id:4,Name:"foo"},{Id:12,Name:"bar"}]);
	inserted("TypeFullName", 29348, {Id:13,Name:"baz"});
	updated("TypeFullName", 29348, {Id:4,Name:"foofoo"});
	deleted("TypeFullName", 29348, {Id:12,Name:"bar"});
	
	
	CLIENT -> SERVER
	
	subscribe("TypeFullName", "select `foo` from `bar` where (`id` = ?)", [42]);
		-> Channel ID = 29348
	unsubscribe("TypeFullName", 29348);
	
	insert("TypeFullName", {Id:0,Name:"baz"});
		-> {Id:879,Name:"baz"}
	update("TypeFullName", {Id:4,Name:"foofoo"});
	delete("TypeFullName", {Id:12,Name:"bar"});
	
	*/


}
