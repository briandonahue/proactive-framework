using System;
using System.Collections.Generic;


namespace Rx
{
	public interface IObservable<T>
	{
		void Subscribe(IObserver<T> observer);
	}
	
	public interface IObserver<T>
	{
		void OnError(Exception error);
		void OnNext(T value);
		void OnCompleted();
	}
	
	public static class ObservableQueries
	{
		public static IObservable<TSource> Where<TSource> (this IObservable<TSource> source, Func<TSource, bool> predicate)
		{
			return new DelegateObservable<TSource>((onNext, onCompleted) => {
				source.Subscribe(value => {
					if (predicate(value)) {					
						onNext(value);					
					}
				}, onCompleted);
			});
		}

		public static IObservable<TResult> Select<TSource, TResult> (this IObservable<TSource> source, Func<TSource, TResult> selector)
		{
			return new DelegateObservable<TResult>((onNext, onCompleted) => {
				source.Subscribe(value => {
					onNext(selector(value));
				}, onCompleted);
			});
		}

		public static IObservable<TResult> SelectMany<TSource, TResult> (this IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector)
		{
			return new DelegateObservable<TResult>((onNext, onCompleted) => {
				source.Subscribe(value => {
					var o = selector(value);
					o.Subscribe(value2 => {
						onNext(value2);
					}, ()=>{});
				}, onCompleted);
			});
		}

		public static IObservable<TResult> SelectMany<TSource, TCollection, TResult> (this IObservable<TSource> source, Func<TSource, IObservable<TCollection>> collectionSelector, Func<TSource, TCollection, TResult> resultSelector)
		{
			return new DelegateObservable<TResult>((onNext, onCompleted) => {
				source.Subscribe(value => {
					var o = collectionSelector(value);
					o.Subscribe(value2 => {
						var r = resultSelector(value, value2);
						onNext(r);
					}, () => {});
				}, onCompleted);
			});
		}
		
		public static void Subscribe<TSource> (this IObservable<TSource> source, Action<TSource> onNext, Action onCompleted)
		{
			source.Subscribe(new DelegateObserver<TSource>(onNext, onCompleted));
		}		
	}

	public partial class Observable {
		static SynchronizationContext _context;
		
		static Observable() {
			_context = new NoSynchronizationContext();
		}
		
		public static void Synchronize(Action action) {
			Context.Synchronize(action);
		}
		
		public static SynchronizationContext Context {
			get {
				return _context;
			}
			set {
				_context = value;
			}
		}
	}
	
	public class DelegateObserver<T> : IObserver<T> {
		
		Action<Exception> _onError;
		Action<T> _onNext;
		Action _onCompleted;

		public DelegateObserver(Action<T> onNext, Action onCompleted) {
			_onNext = onNext;
			_onError = null;
			_onCompleted = onCompleted;
		}
		
		public void OnError (Exception error)
		{
			if (_onError != null) {
				_onError(error);
			}
		}
		
		public void OnNext (T value)
		{
			if (_onNext != null) {
				_onNext(value);
			}
		}
		
		public void OnCompleted ()
		{
			if (_onCompleted != null) {
				_onCompleted();
			}
		}
	}

	public abstract class SynchronizationContext {		
		public abstract void Synchronize(Action action);		
	}
	
	public class NoSynchronizationContext : SynchronizationContext {
		public override void Synchronize (Action action)
		{
			action();
		}
	}
	
	public class DelegateObservable<T> : AbstractObservable<T> {
		
		public delegate void RunAction(Action<T> onNext, Action onCompleted, Action<Exception> onError);
		
		RunAction _run;
		
		public DelegateObservable(Action<Action<T>, Action> run) {
			_run = (n, c, e) => {
				run(n, c);
			};
		}
		
		protected override void Run ()
		{
			_run(OnNext, OnCompleted, OnError);
		}		
	}
	
	public abstract class AbstractObservable<T> : IObservable<T> {
		
		List<IObserver<T>> _observers = new List<IObserver<T>>();
		IObserver<T>[] _observersCache = null;
		object _observersLock = new object();
		
		bool _started = false;
		object _startedLock = new object();
		
		public AbstractObservable() {
		}
			
		public void Subscribe (IObserver<T> observer)
		{
			lock (_observersLock) {
				_observers.Add(observer);
				_observersCache = null;
			}
			Start();
		}
				
		protected abstract void Run();
		
		protected void OnNext(T value) {
			Observable.Synchronize(delegate {
				foreach (var o in Observers) {
					o.OnNext(value);
				}
			});
		}
		
		protected void OnCompleted() {
			Observable.Synchronize(delegate {
				foreach (var o in Observers) {
					o.OnCompleted();
				}
			});
		}
		
		protected void OnError(Exception error) {
			Observable.Synchronize(delegate {
				foreach (var o in Observers) {
					o.OnError(error);
				}
			});
		}
		
		IObserver<T>[] Observers {
			get {
				var cache = _observersCache;
				if (cache == null) {
					lock (_observersLock) {
						cache = _observersCache;
						if (cache == null) {
							cache = _observers.ToArray();
							_observersCache = cache;
						}
					}
				}
				return cache;				
			}
		}
		
		void Start() {
			lock (_startedLock) {
				if (!_started) {
					System.Threading.ThreadPool.QueueUserWorkItem(delegate {
						Run();
					});
					_started = true;
				}		
		    }
		}
	}
}
