using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace ExceptionRewriter {
    class Program {
        public static int Main (string[] args) {
            try {
                var argv = args.ToList();
                if (argv.Count < 2) {
                    Usage();
                    return 1;
                }

                for (int i = 0; i < args.Length; i += 2) {
                    var src = args[i];
                    var dst = args[i + 1];

                    Console.WriteLine($"{Path.GetFileName(src)} -> {Path.GetFileName(dst)}...");

                    var assemblyResolver = new DefaultAssemblyResolver();
                    assemblyResolver.AddSearchDirectory(Path.GetDirectoryName(src));

                    using (var def = AssemblyDefinition.ReadAssembly(src, new ReaderParameters {
                        ReadWrite = false,
                        ReadingMode = ReadingMode.Immediate,
                        AssemblyResolver = assemblyResolver,
                        ReadSymbols = true,
                        SymbolReaderProvider = new DefaultSymbolReaderProvider(throwIfNoSymbol: false)
                    })) {
                        Console.WriteLine("====");

                        var arw = new AssemblyRewriter(def);
                        arw.Rewrite();

                        var shouldWriteSymbols = def.MainModule.SymbolReader != null;

                        def.Write(dst + ".tmp", new WriterParameters {
                            WriteSymbols = shouldWriteSymbols
                        });
                    }

                    File.Copy(dst + ".tmp", dst, true);
                    if (File.Exists(dst + ".pdb")) {
                        File.Copy(dst + ".pdb", dst.Replace(".exe", ".pdb"), true);
                        File.Delete(dst + ".pdb");
                    }
                    File.Delete(dst + ".tmp");
                }

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
            Console.WriteLine("Expected: exceptionrewriter [input] [output] ...");
        }
    }
}
