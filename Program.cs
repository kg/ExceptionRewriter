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

                var assemblyResolver = new DefaultAssemblyResolver();
                assemblyResolver.AddSearchDirectory(Path.GetDirectoryName(argv[0]));

                using (var def = AssemblyDefinition.ReadAssembly(argv[0], new ReaderParameters {
                    ReadWrite = true,
                    ReadingMode = ReadingMode.Immediate,
                    AssemblyResolver = assemblyResolver
                })) {
                    var aa = new AssemblyAnalyzer(def);
                    aa.Analyze();

                    Console.WriteLine("====");

                    var arw = new AssemblyRewriter(aa);
                    arw.Rewrite();

                    def.Write(argv[1] + ".tmp");
                }

                File.Copy(argv[1] + ".tmp", argv[1], true);
                File.Delete(argv[1] + ".tmp");

                Console.WriteLine("Done");
                return 0;
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
