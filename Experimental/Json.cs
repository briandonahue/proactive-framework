using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace JsonSerialization
{
	public class JsonParseException : Exception {
		public JsonParseException(string msg) : base(msg) {
		}
	}
		
	public static class Json {	
		public static string Encode(object o) {
			if (o == null) {
				return "null";
			}
			else if (o is int || o is byte || o is long || o is float || o is double || o is decimal) {
				return o.ToString();
			}
			else if (o is string) {
				var sb = new StringBuilder();
				sb.Append("\"");
				foreach (var ch in (string)o) {
					if (ch == '\\') {
						sb.Append("\\\\");
					}
					else if (ch == '\r') {
						sb.Append("\\r");
					}
					else if (ch == '\n') {
						sb.Append("\\n");
					}
					else if (ch == '\t') {
						sb.Append("\\t");
					}
					else if (ch == '\"') {
						sb.Append("\\\"");
					}
					else if (ch == '\'') {
						sb.Append("\\\'");
					}
					else {
						sb.Append(ch);
					}
				}
				sb.Append("\"");
				return sb.ToString();
			}
			else if (o is TimeSpan) {
				return ((TimeSpan)o).TotalSeconds.ToString();
			}
			else if (o is DateTime) {
				return ((DateTime)o).ToString();
			}
			else if (o is bool) {
				return ((bool)o) ? "true" : "false";
			}
			else if (o is System.Collections.IEnumerable) {
				var sb = new StringBuilder();
				sb.Append("[");
				var head = "";
				foreach (var e in (System.Collections.IEnumerable)o) {
					sb.Append(head);
					sb.Append(Encode(e));
					head = ",";
				}
				sb.Append("]");
				return sb.ToString();
			}
			else if (o.GetType().IsEnum) {
				return "\"" + Enum.GetName(o.GetType(), o) + "\"";
			}
			else if (o.GetType().IsClass) {
				var props = PropsForType(o.GetType());
				var sb = new StringBuilder();
				sb.Append("{");
				var head = "";
				foreach (var prop in props.Values) {
					sb.Append(head);
					sb.Append(prop.Name);
					sb.Append(":");
					sb.Append(Encode(prop.GetValue(o, null)));
					head = ",";
				}
				sb.Append("}");
				return sb.ToString();
			}
			else {
				throw new NotSupportedException("Don't know how to Json encode " + o);
			}
		}
		
		public static T ParseValue<T>(string str) {
			return (T)ParseValue(typeof(T), new Tokens(str));
		}
		
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
			else if (tok == Tokens.Null) {
				val = null;
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
				else if (valueType.IsEnum) {
					val = Enum.Parse(valueType, toks.CurrentValue);
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
			public static readonly string Null = "Null";
			
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
					if (CurrentValue == "true" || CurrentValue == "false") {
						Current = Boolean;
					}
					else if (CurrentValue == "null") {
						Current = Null;
					}
					else {
						Current = Identifier;
					}
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
}
