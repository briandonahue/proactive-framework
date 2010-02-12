using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
		IEnumerable<T> All { get; }

		event Action<T> ValueInserted;
		event Action<T> ValueUpdated;
		event Action<T> ValueDeleted;

		void Update (T value);
		void Delete (T value);
	}

	public abstract class DataChannelBase<T> : IDataChannel<T>
	{
		protected bool Active { get; private set; }

		List<T> _allValues = new List<T> ();

		public IEnumerable<T> All {
			get { return _allValues; }
		}

		public event Action<T> ValueInserted;
		public event Action<T> ValueUpdated;
		public event Action<T> ValueDeleted;

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
	
	public class JsonParseException : Exception {
		public JsonParseException(string msg) : base(msg) {
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
	
	public class Json {
		
		public static object ParseValue(Type valueType, string str) {
			return ParseValue(valueType, new Tokens(str));
		}
		
		public static object ParseValue(Type valueType, Tokens toks) {
			var tok = toks.Current;
			object val = null;
			
			if (tok == Tokens.EOF) {
				throw new JsonParseException("End Reached");
			}
			else if (tok == "{") {
				val = Activator.CreateInstance(valueType);
				var props = PropsForType(valueType);
				toks.Next();
				while (toks.Current != "}" && toks.Current != Tokens.EOF) {					
					if (toks.Current != Tokens.Identifier && toks.Current != Tokens.String) {
						Console.WriteLine ("ww");
						throw new JsonParseException("Expected Identifier or String got " + toks.Current);
					}
					var propName = toks.CurrentValue;
					System.Reflection.PropertyInfo prop = null;
					if (!props.TryGetValue(propName, out prop)) {
						throw new JsonParseException(valueType + " does not have a property named " + propName);
					}					
					toks.Next();
					if (toks.Current != ":") {
						throw new JsonParseException("Expected : got " + toks.Current);
					}
					toks.Next();
					var propVal = ParseValue(prop.PropertyType, toks);
					prop.SetValue(val, propVal, null);
					if (toks.Current != "," && toks.Current != "}") {
						throw new JsonParseException("Expected , or } got " + toks.Current);
					}
					if (toks.Current == ",") toks.Next();
				}
				toks.Next();
			}
			else if (tok == "[") {
				var objs = new List<object>();
				toks.Next();
				while (toks.Current != "]" && toks.Current != Tokens.EOF) {
					objs.Add(ParseValue(valueType, toks));
					if (toks.Current != "," && toks.Current != "]") {
						throw new JsonParseException("Expected , or ] got " + toks.Current);
					}
					if (toks.Current == ",") toks.Next();
				}
				toks.Next();
				val = objs.ToArray();
			}
			else if (tok == Tokens.Number) {	
				if (valueType == typeof(int)) {
					val = int.Parse(toks.CurrentValue);					
				}
				else if (valueType == typeof(byte)) {
					val = byte.Parse(toks.CurrentValue);					
				}
				else if (valueType == typeof(long)) {
					val = long.Parse(toks.CurrentValue);					
				}
				else if (valueType == typeof(float)) {
					val = float.Parse(toks.CurrentValue);					
				}
				else if (valueType == typeof(decimal)) {
					val = decimal.Parse(toks.CurrentValue);					
				}
				else if (valueType == typeof(TimeSpan)) {
					val = TimeSpan.FromSeconds(double.Parse(toks.CurrentValue));
				}
				else {
					var intval = 0;
					if (int.TryParse(toks.CurrentValue, out intval)) {
						val = intval;
					}
					else {
						val = double.Parse(toks.CurrentValue);
					}
				}
				toks.Next();
			}
			else if (tok == Tokens.Boolean) {
				val = (toks.CurrentValue == "true");
				toks.Next();
			}
			else if (tok == Tokens.String) {
				if (valueType == typeof(DateTime)) {
					val = DateTime.Parse(toks.CurrentValue);
				}
				else {
					val = toks.CurrentValue;
				}
				toks.Next();
			}
			else {
				throw new JsonParseException(toks.Current + " not supported");
			}
			
			return val;
		}
		
		static Dictionary<string, Dictionary<string, System.Reflection.PropertyInfo>> _typeProps = new Dictionary<string, Dictionary<string, System.Reflection.PropertyInfo>>();
		static object _typePropsLock = new object();
		static Dictionary<string, System.Reflection.PropertyInfo> PropsForType(Type type) {
			Dictionary<string, System.Reflection.PropertyInfo> r = null;
			lock (_typePropsLock) {
				if (!_typeProps.TryGetValue(type.FullName, out r)) {
					r = new Dictionary<string, System.Reflection.PropertyInfo>();
					foreach (var p in type.GetProperties()) {
						if (p.CanWrite) {
							r.Add(p.Name, p);
						}
					}
					_typeProps.Add(type.FullName, r);
				}
			}
			return r;
		}
		
		public class Tokens {
			public static readonly string BOF = "BOF";
			public static readonly string EOF = "EOF";
			
			public static readonly string Identifier = "Identifier";
			public static readonly string Number = "Number";
			public static readonly string Boolean = "Boolean";
			public static readonly string String = "String";
			
			public string Current { get; private set; }
			
			public string CurrentValue { get; private set; }
			string ParseString;
			int Index;
			
			public Tokens(string str) {
				Current = "BOF";
				CurrentValue = "";
				Index = 0;
				ParseString = str;
				Next();
			}
			
			public void Next() {
				if (Current == EOF) return;				
				
				while (Index < ParseString.Length && char.IsWhiteSpace(ParseString[Index])) {
					Index++;
				}
				
				if (Index >= ParseString.Length) {
					Current = EOF;
					return;
				}
				
				var ch = ParseString[Index];
				
				if (char.IsLetter(ch)) {
					var i = Index;
					for (; i < ParseString.Length && (char.IsLetterOrDigit(ParseString[i]) || ParseString[i]=='_'); i++) {
					}
					CurrentValue = ParseString.Substring(Index, i - Index);
					Index = i;
					Current = Identifier;
				}
				else if (char.IsDigit(ch) || ch == '-') {
					var i = Index;
					for (; i < ParseString.Length && (char.IsDigit(ParseString[i]) || ParseString[i]=='-' || ParseString[i]=='.' || ParseString[i]=='e' || ParseString[i]=='E' || ParseString[i]=='f' || ParseString[i]=='F'); i++) {
					}
					CurrentValue = ParseString.Substring(Index, i - Index);
					Index = i;
					Current = Number;
				}
				else if (ch == '\"') {
					
					var sb = new System.Text.StringBuilder();
					var done = false;
					
					Index++;
					
					while (!done && Index < ParseString.Length) {
						ch = ParseString[Index];
						
						if (ch == '\"') {
							done = true;
							Index++;
						}
						else if (ch == '\\') {
							if (Index+1 < ParseString.Length) {
								ch = ParseString[Index+1];
								sb.Append(Unescape(ch));
								Index += 2;
							}
							else {
								throw new JsonParseException("\\ must be followed by character in string");
							}
						}
						else {
							sb.Append(ch);
							Index++;
						}
					}
					
					CurrentValue = sb.ToString();
					Current = String;
				}
				else {
					Current = ch.ToString();
					CurrentValue = "";
					Index++;
				}
			}
			
			char Unescape(char ch) {
				switch (ch) {
				case 'n': return '\n';
				case 'r': return '\r';
				case 't': return '\t';
				case '\\': return '\\';
				case '\"': return '\"';
				case '\'': return '\'';
				default: throw new JsonParseException("Don't know how to interpret \\" + ch);
				}
			}
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
