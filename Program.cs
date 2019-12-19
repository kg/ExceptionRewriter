using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace ExceptionRewriter {
    class Program {
        public static int Main (string[] _args) {
            try {
                var options = new RewriteOptions();

                foreach (var arg in _args)
                    ParseArgument(arg, options);

                var argv = _args.Where(arg => !arg.StartsWith("-")).ToArray();

                if (argv.Length < 2) {
                    Usage();
                    return 1;
                }

                for (int i = 0; i < argv.Length; i += 2) {
                    var src = argv[i];
                    var dst = argv[i + 1];

                    if (options.Verbose)
                        Console.WriteLine($"{Path.GetFileName(src)} -> {Path.GetFileName(dst)}...{Environment.NewLine}====");

                    var assemblyResolver = new DefaultAssemblyResolver();
                    assemblyResolver.AddSearchDirectory(Path.GetDirectoryName(src));

                    using (var def = AssemblyDefinition.ReadAssembly(src, new ReaderParameters {
                        ReadWrite = false,
                        ReadingMode = ReadingMode.Immediate,
                        AssemblyResolver = assemblyResolver,
                        ReadSymbols = true,
                        SymbolReaderProvider = new DefaultSymbolReaderProvider(throwIfNoSymbol: false)
                    })) {
                        var arw = new AssemblyRewriter(def, options);
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

                return 0;
            } finally {
                if (Debugger.IsAttached) {
                    Console.WriteLine("Press enter to exit");
                    Console.ReadLine();
                }
            }
        }

        static void ParseArgument (string arg, RewriteOptions options) {
            if (!arg.StartsWith("-"))
                return;

            switch (arg) {
                case "--abort":
                    options.ThrowOnError = true;
                    break;
                case "--warn":
                    options.ThrowOnError = false;
                    break;
                case "--generics":
                    options.EnableGenerics = true;
                    break;
                case "--no-generics":
                    options.EnableGenerics = false;
                    break;
                case "--verbose":
                    options.Verbose = true;
                    break;
                case "--quiet":
                    options.Verbose = false;
                    break;
                default:
                    throw new Exception("Unsupported argument: " + arg);
            }
        }

        static void Usage () {
            Console.WriteLine(@"Expected: exceptionrewriter [options] input output ...
--abort       Abort on error (default)
--warn        On error, output warning instead of aborting
--generics    Enable rewriting filters for generics (currently broken)
--no-generics Disable rewriting filters for generics (default)
--verbose     Output name of every rewritten method (default)
--quiet       Do not output anything");

        }
    }
}
