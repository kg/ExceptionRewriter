using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Mono.Cecil;

namespace ExceptionRewriter {
    class Program {
        public static int Main (string[] args) {
            try {
                var argv = args.ToList();
                if (argv.Count != 2) {
                    Usage();
                    return 1;
                }

                File.Delete(argv[1] + ".tmp");
                File.Copy(argv[0], argv[1] + ".tmp", true);

                using (var def = AssemblyDefinition.ReadAssembly(argv[1] + ".tmp", new ReaderParameters {
                    ReadWrite = true,
                    ReadingMode = ReadingMode.Immediate
                })) {
                    var aa = new AssemblyAnalyzer(def);
                    aa.Analyze();

                    Console.WriteLine("====");

                    var arw = new AssemblyRewriter(aa);
                    arw.Rewrite();

                    def.Write();
                }

                File.Copy(argv[1] + ".tmp", argv[1], true);

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
