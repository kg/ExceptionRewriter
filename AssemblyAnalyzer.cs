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
                m.ShouldRewrite = m.MayNeedRewriting && !m.SuppressRewriting;
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
                    // FIXME: This is complicated so it's not being handled yet
                    method.IsGenericInstance ||
                    // FIXME: Same as above
                    method.HasGenericParameters
            };

            var body = method.Body;
            var ilp = method.Body.GetILProcessor();
            foreach (var insn in body.Instructions) {
                if (
                    (insn.OpCode == OpCodes.Throw) ||
                    (insn.OpCode == OpCodes.Rethrow)
                )
                    result.HasThrowStatement = true;
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
            if (!result.HasExceptionFilter)
                result.SuppressRewriting = true;

            result.MayNeedRewriting =
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

        public Dictionary<ExceptionHandler, MethodDefinition> Filters = 
            new Dictionary<ExceptionHandler, MethodDefinition>();

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
