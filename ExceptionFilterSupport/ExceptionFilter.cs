﻿using System;
using System.Collections.Generic;
using System.Threading;

namespace Mono.Runtime.Internal {
    public abstract class ExceptionFilter {
        public static readonly int exception_continue_search = 0;
        public static readonly int exception_execute_handler = 1;

        public int Result;

        public static readonly ThreadLocal<List<ExceptionFilter>> ExceptionFilters = 
            new ThreadLocal<List<ExceptionFilter>>(() => new List<ExceptionFilter>(128));

        private static object LastEvaluatedException = null;
        private static bool HasEvaluatedFiltersAlready = false;

        public abstract int Evaluate (object exc);

        public static void Push (ExceptionFilter filter) {
            filter.Result = exception_continue_search;
            ExceptionFilters.Value.Add(filter);
        }

        public static void Pop (ExceptionFilter filter) {
            var ef = ExceptionFilters.Value;
            if (ef.Count == 0)
                throw new ThreadStateException("Corrupt exception filter stack");
            var current = ef[ef.Count - 1];
            ef.RemoveAt(ef.Count - 1);
            if (current != filter)
                throw new ThreadStateException("Corrupt exception filter stack");
        }

        /// <summary>
        /// Resets the state of all valid exception filters so that we can handle any
        ///  new exceptions. This is invoked when a filtered block finally processes an
        ///  exception.
        /// </summary>
        public static void Reset () {
            var ef = ExceptionFilters.Value;
            foreach (var filter in ef)
                filter.Result = exception_continue_search;
            LastEvaluatedException = null;
        }

        /// <summary>
        /// Automatically runs any active exception filters for the exception exc, 
        ///  then returns true if the provided filter indicated that the current block
        ///  should run.
        /// </summary>
        /// <param name="exc">The exception to pass to the filters</param>
        /// <param name="filter">The exception filter for the current exception handler</param>
        /// <returns>true if this filter selected the exception handler to run</returns>
        public static bool ShouldRunHandler (object exc, ExceptionFilter filter) {
            if (exc == null)
                throw new ArgumentNullException("exc");
            if (filter == null)
                throw new ArgumentNullException("filter");

            PerformEvaluate(exc);
            return filter.Result == exception_execute_handler;
        }

        /// <summary>
        /// Runs all active exception filters until one of them returns execute_handler.
        /// Afterward, the filters will have an initialized Result and the selected one will have
        ///  a result with the value exception_continue_search.
        /// If filters have already been run for the active exception they will not be run again.
        /// </summary>
        /// <param name="exc">The exception filters are being run for.</param>
        public static void PerformEvaluate (object exc) {
            if (HasEvaluatedFiltersAlready)
                return;
            // FIXME: Attempt to avoid running filters multiple times when unwinding.
            // I think this doesn't work right for rethrow?
            if (LastEvaluatedException == exc)
                return;

            var ef = ExceptionFilters.Value;
            var hasLocatedValidHandler = false;

            // Set in advance in case the filter throws.
            // These two state variables allow us to early out in the case where Evaluate() is triggered
            //  in multiple stack frames while unwinding even though filters have already run.
            LastEvaluatedException = exc;
            HasEvaluatedFiltersAlready = true;

            for (int i = ef.Count - 1; i >= 0; i--) {
                var filter = ef[i];
                if ((filter.Result = filter.Evaluate(exc)) == exception_execute_handler) {
                    hasLocatedValidHandler = true;
                    break;
                }
            }

            if (!hasLocatedValidHandler)
                Console.WriteLine("Located no valid filtered handler for exception");
        }
    }
}