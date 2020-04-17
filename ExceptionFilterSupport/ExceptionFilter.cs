using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Mono {
	public abstract class ExceptionFilter {
		private class HasFilterRunTable : Dictionary<ExceptionFilter, bool> {
		}

		private class ThreadState {
			public readonly ConditionalWeakTable<object, HasFilterRunTable> HasFilterRun = 
				new ConditionalWeakTable<object, HasFilterRunTable> ();
			public readonly List<ExceptionFilter> ExceptionFilters =
				new List<ExceptionFilter> (128);

			public object LastEvaluatedException = null;
		}

		public static readonly int exception_continue_search = 0;
		public static readonly int exception_execute_handler = 1;

		public int Result;

		private static readonly object FilterHasRunSentinel = new object();

		private static readonly ThreadLocal<ThreadState> ThreadStates =
			new ThreadLocal<ThreadState> (() => new ThreadState ());

		public abstract int Evaluate (object exc);

		public static void Push (ExceptionFilter filter) {
			filter.Result = exception_continue_search;
			ThreadStates.Value.ExceptionFilters.Add(filter);
		}

		public static void Pop (ExceptionFilter filter) {
			var ef = ThreadStates.Value.ExceptionFilters;
			if (ef.Count == 0)
				throw new Exception("Corrupt exception filter stack");
			var current = ef[ef.Count - 1];
			ef.RemoveAt(ef.Count - 1);
			if (current != filter)
				throw new Exception("Corrupt exception filter stack");
		}

		/// <summary>
		/// Checks whether the last filter evaluation selected this handler to run.
		/// </summary>
		/// <param name="exc">The exception being filtered.</param>
		/// <returns>true if this filter selected the exception handler to run</returns>
		public bool ShouldRunHandler (object exc) {
			var ts = ThreadStates.Value;

			if (exc == null)
				throw new ArgumentNullException("exc");
			if (ts.LastEvaluatedException != exc)
				throw new ArgumentException ("Passed exception was not evaluated yet");

			var result = Result == exception_execute_handler;
			return result;
		}

		/// <summary>
		/// Runs all active exception filters until one of them returns execute_handler.
		/// Afterward, the filters will have an initialized Result and the selected one will have
		///  a result with the value exception_continue_search.
		/// If filters have already been run for the active exception they will not be run again.
		/// </summary>
		/// <param name="exc">The exception filters are being run for.</param>
		public static void PerformEvaluate (object exc) {
			var ts = ThreadStates.Value;

			// Attempt to avoid running filters multiple times when unwinding.
			// FIXME: This may not be correct for rethrow
			if (ts.LastEvaluatedException == exc)
				return;

			var hasLocatedValidHandler = false;

			// Set in advance in case the filter throws.
			// These two state variables allow us to early out in the case where Evaluate() is triggered
			//  in multiple stack frames while unwinding even though filters have already run.
			ts.LastEvaluatedException = exc;

			var hfrByException = ts.HasFilterRun;
			HasFilterRunTable hfrt;
			if (!hfrByException.TryGetValue(exc, out hfrt)) {
				hfrt = new HasFilterRunTable();
				hfrByException.Add(exc, hfrt);
			}

			for (int i = ts.ExceptionFilters.Count - 1; i >= 0; i--) {
				var filter = ts.ExceptionFilters[i];
				if (hasLocatedValidHandler) {
					filter.Result = exception_continue_search;
					continue;
				}

				if (hfrt.ContainsKey(filter))
					continue;

				// When an exception filter throws on windows netframework, the filter's exception is
				//  silently discarded and search for an exception handler continues as if it returned false
				try {
					var result = filter.Evaluate(exc);
					hfrt[filter] = result == exception_execute_handler;
					filter.Result = result;
					if (result == exception_execute_handler)
						hasLocatedValidHandler = true;
				} catch {
					filter.Result = exception_continue_search;
				}
			}
		}
	}
}
