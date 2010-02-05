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
			return new DelegateObservable<TSource>(onNext => {
				source.Subscribe(value => {
					if (predicate(value)) {					
						onNext(value);					
					}
				});
			});
		}

		public static IObservable<TResult> Select<TSource, TResult> (this IObservable<TSource> source, Func<TSource, TResult> selector)
		{
			return new DelegateObservable<TResult>(onNext => {
				source.Subscribe(value => {
					onNext(selector(value));
				});
			});
		}

		//public static IObservable<TResult> SelectMany<TSource, TResult> (this IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector)
		//{
		//}

		public static IObservable<TResult> SelectMany<TSource, TCollection, TResult> (this IObservable<TSource> source, Func<TSource, IObservable<TCollection>> collectionSelector, Func<TSource, TCollection, TResult> resultSelector)
		{
			return new DelegateObservable<TResult>(onNext => {
				source.Subscribe(value => {
					var o = collectionSelector(value);
					o.Subscribe(value2 => {
						var r = resultSelector(value, value2);
						onNext(r);
					});
				});
			});
		}
		
		public static void Subscribe<TSource> (this IObservable<TSource> source, Action<TSource> onNext)
		{
			source.Subscribe(new DelegateObserver<TSource>(onNext));
		}		
	}

	public class Observable {
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

		public DelegateObserver(Action<T> onNext) {
			_onNext = onNext;
			_onError = null;
			_onCompleted = null;
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
		
		public DelegateObservable(Action<Action<T>> run) {
			_run = (n, c, e) => {
				run(n);
			};
		}
		
		protected override void Run ()
		{
			_run(OnNext, OnCompleted, OnError);
		}		
	}
	
	public abstract class AbstractObservable<T> : IObservable<T> {
		
		List<IObserver<T>> _observers = new List<IObserver<T>>();		
		object _observersLock = new object();
		
		public AbstractObservable() {
		}
			
		public virtual void Subscribe (IObserver<T> observer)
		{
			lock (_observersLock) {
				_observers.Add(observer);
			}
			Start();
		}
		
		void Start() {
			System.Threading.ThreadPool.QueueUserWorkItem(delegate {
				Run();
			});
		}
		
		protected abstract void Run();
		
		protected IObserver<T>[] GetObservers() {
			lock (_observersLock) {
				return _observers.ToArray();
			}
		}
		
		protected virtual void OnNext(T value) {
			Observable.Synchronize(delegate {
				foreach (var o in GetObservers()) {
					o.OnNext(value);
				}
			});
		}
		
		protected virtual void OnCompleted() {
			Observable.Synchronize(delegate {
				foreach (var o in GetObservers()) {
					o.OnCompleted();
				}
			});
		}
		
		protected virtual void OnError(Exception error) {
			Observable.Synchronize(delegate {
				foreach (var o in GetObservers()) {
					o.OnError(error);
				}
			});
		}
	}
}
