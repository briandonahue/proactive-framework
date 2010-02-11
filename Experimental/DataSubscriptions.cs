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
}
