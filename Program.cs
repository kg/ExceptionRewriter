using System;
using System.Diagnostics;
using System.Linq;

namespace ExceptionRewriter {
    class Program {
        public static int Main (string[] args) {
            try {
                var argv = args.ToList();
                if (argv.Count != 2) {
                    Usage();
                    return 1;
                }

                Console.WriteLine("Not implemented");
                return 2;
            } finally {
                if (Debugger.IsAttached) {
                    Console.WriteLine("Press enter to exit");
                    Console.ReadLine();
                }
            }
        }

        static void Usage () {
            Console.WriteLine("Expected: exceptionrewriter [input] [output]");
        }
    }
}
