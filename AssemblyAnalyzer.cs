using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace ExceptionRewriter {
    public class AssemblyAnalyzer {
        public readonly AssemblyDefinition Input;
        public readonly Dictionary<MethodReference, AnalyzedMethod> Methods = 
            new Dictionary<MethodReference, AnalyzedMethod>();

        public AssemblyAnalyzer (AssemblyDefinition input) {
            Input = input;
        }

        public void Analyze () {
            foreach (var m in Input.Modules)
                foreach (var t in m.Types)
                    Analyze(m, t);

            SecondPass();
        }

        private void SecondPass () {
            foreach (var m in Methods.Values)
                m.ShouldRewrite = m.MayNeedRewriting && !m.SuppressRewriting &&
                    m.ReferencedMethods.Any(rm =>
                        (GetResult(rm)?.CanThrow ?? true) || (GetResult(rm)?.SuppressRewriting ?? true)
                    );
        }

        private void Analyze (ModuleDefinition module, TypeDefinition type) {
            foreach (var m in type.Methods)
                Analyze(module, type, m);
        }

        private void Analyze (ModuleDefinition module, TypeDefinition type, MethodDefinition method) {
            if (!method.HasBody)
                return;

            var result = new AnalyzedMethod {
                Method = method,
                SuppressRewriting = method.CustomAttributes.Any(ca => ca.AttributeType.Name == "SuppressRewritingAttribute") ||
                    // Rewriting ctors seems generally impossible.
                    method.Name.StartsWith(".") ||
                    // Rewriting operators might be feasible but I suspect it is not.
                    method.Name.StartsWith("op_") ||
                    // Can we rewrite virtual or abstract? Since the impl could be in another assembly,
                    //  I'm not sure it's possible to do this.
                    method.IsVirtual ||
                    method.IsAbstract ||
                    // FIXME: Same as above
                    method.IsGenericInstance ||
                    // FIXME: Same as above
                    method.HasGenericParameters ||
                    method.Module.EntryPoint == method,
                OriginalName = method.Name
            };

            var body = method.Body;
            var ilp = method.Body.GetILProcessor();
            foreach (var insn in body.Instructions) {
                if (
                    (insn.OpCode == OpCodes.Throw) ||
                    (insn.OpCode == OpCodes.Rethrow)
                )
                    result.HasThrowStatement = true;
                else if (
                    (insn.OpCode == OpCodes.Call) ||
                    (insn.OpCode == OpCodes.Calli) ||
                    (insn.OpCode == OpCodes.Callvirt) ||
                    (insn.OpCode == OpCodes.Newobj)
                ) {
                    var mr = insn.Operand as MethodReference;
                    if (mr != null)
                        result.ReferencedMethods.Add(mr);
                    else
                        Console.WriteLine("Unrecognized call operand {0}", insn.Operand);
                    result.HasCalls = true;
                }
            }

            foreach (var eh in body.ExceptionHandlers) {
                if (eh.FilterStart != null)
                    result.HasExceptionFilter = true;
                else
                    result.HasTryBlock = true;
            }

            // FIXME: Extract filter into method, extract referenced locals into a
            //  closure object
            // Also, this is not possible period for methods with ref/out parameters
            if (result.HasExceptionFilter)
                result.SuppressRewriting = true;

            result.MayNeedRewriting =
                result.HasTryBlock ||
                result.HasThrowStatement ||
                result.HasCalls ||
                result.HasExceptionFilter;

            if (result.SuppressRewriting)
                result.MayNeedRewriting = false;

            result.CanThrow = result.HasThrowStatement || result.HasExceptionFilter || result.HasCalls;

            Console.WriteLine($"{type.Name}::{method.Name} try={result.HasTryBlock} filter={result.HasExceptionFilter} throw={result.HasThrowStatement} rewrite={!result.SuppressRewriting} calls={result.HasCalls} mayneedrewrite={result.MayNeedRewriting}");
            Methods.Add(method, result);
        }

        public AnalyzedMethod GetResult (MethodReference method) {
            AnalyzedMethod result;
            Methods.TryGetValue(method, out result);
            return result;
        }
    }

    public class AnalyzedMethod {
        public MethodDefinition Method;
        public MethodDefinition BackingMethod;
        public string OriginalName;

        public HashSet<MethodReference> ReferencedMethods = new HashSet<MethodReference>();

        public bool HasCalls;
        public bool HasTryBlock;
        public bool HasExceptionFilter;
        public bool HasThrowStatement;
        public bool SuppressRewriting;
        public bool MayNeedRewriting;
        public bool CanThrow;

        public bool ShouldRewrite;
    }
}
