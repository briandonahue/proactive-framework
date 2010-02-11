using System;
using System.Collections.Generic;

using MonoTouch.Foundation;
using MonoTouch.UIKit;

namespace Rx
{

	public class UIViewRx : UIView
	{
		SignaledObservable<UITouch[]> TouchesBeganEvents = new SignaledObservable<UITouch[]> ();
		SignaledObservable<UITouch[]> TouchesMovedEvents = new SignaledObservable<UITouch[]> ();
		SignaledObservable<UITouch[]> TouchesEndedEvents = new SignaledObservable<UITouch[]> ();

		public override void TouchesBegan (NSSet touches, UIEvent evt)
		{
			TouchesBeganEvents.Signal (touches.ToArray<UITouch> ());
		}
		public override void TouchesMoved (NSSet touches, UIEvent evt)
		{
			TouchesMovedEvents.Signal (touches.ToArray<UITouch> ());
		}
		public override void TouchesEnded (NSSet touches, UIEvent evt)
		{
			TouchesEndedEvents.Signal (touches.ToArray<UITouch> ());
		}
	}

	public class SignaledObservable<T> : AbstractObservable<T>
	{
		public void Signal (T next)
		{
			OnNext (next);
		}
		protected override void Run ()
		{
		}
	}

	public static class TouchRx
	{
	}
}
