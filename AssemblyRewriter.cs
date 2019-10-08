using System;
using System.Collections.Generic;
using System.Text;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace ExceptionRewriter {
    public class AssemblyRewriter {
        public readonly AssemblyDefinition Assembly;
        public readonly AssemblyAnalyzer Analyzer;

        public AssemblyRewriter (AssemblyAnalyzer analyzer) {
            Assembly = analyzer.Input;
            Analyzer = analyzer;
        }

        public void Rewrite () {
            var queue = new HashSet<AnalyzedMethod>();

            // First, collect a full set of all the methods we will rewrite
            // The rewriting process needs to know this in order to identify whether
            //  a given method call needs to be wrapped in a try block.
            foreach (var m in Analyzer.Methods.Values) {
                if (!m.ShouldRewrite)
                    continue;
                queue.Add(m);
            }

            foreach (var m in queue) {
                Console.WriteLine("Rewriting {0}", m.Method.FullName);
                Rewrite(m, queue);
            }
        }

        private void Rewrite (AnalyzedMethod am, HashSet<AnalyzedMethod> methods) {
            var method = am.Method;
            var backing = CloneMethod(method);
            ConvertToOutException(backing);
            am.Method.DeclaringType.Methods.Add(backing);
            // var backing = method.Clone();
        }

        private Instruction MakeDefault (TypeReference t) {
            if (t.FullName == "System.Void")
                return Instruction.Create(OpCodes.Nop);

            if (t.IsByReference || !t.IsValueType)
                return Instruction.Create(OpCodes.Ldnull);

            switch (t.FullName) {
                case "System.Int32":
                case "System.UInt32":
                case "System.Boolean":
                    return Instruction.Create(OpCodes.Ldc_I4_0);
                default:
                    throw new NotImplementedException();
            }
        }

        private Instruction Patch (Instruction i, Instruction old, Instruction replacement) {
            if (i == old)
                return replacement;
            else
                return i;
        }

        private void Patch (MethodDefinition method, Instruction old, Instruction replacement) {
            var body = method.Body.Instructions;
            for (int i = 0; i < body.Count; i++) {
                if (body[i].Operand == old)
                    body[i] = Instruction.Create(body[i].OpCode, replacement);
            }

            foreach (var eh in method.Body.ExceptionHandlers) {
                eh.TryStart = Patch(eh.TryStart, old, replacement);
                eh.TryEnd = Patch(eh.TryEnd, old, replacement);
                eh.HandlerStart = Patch(eh.HandlerStart, old, replacement);
                eh.HandlerEnd = Patch(eh.HandlerEnd, old, replacement);
                eh.FilterStart = Patch(eh.FilterStart, old, replacement);
            }
        }

        private void InsertOps (
            Mono.Collections.Generic.Collection<Instruction> body, int offset, params Instruction[] ops
        ) {
            for (int i = ops.Length - 1; i >= 0; i--)
                body.Insert(offset, ops[i]);
        }

        private void ConvertToOutException (MethodDefinition method) {
            var excType = method.Module.ImportReference(typeof(Exception));
            var refType = new ByReferenceType(excType);
            var outParam = new ParameterDefinition("_error", ParameterAttributes.Out, refType);
            var tempLocal = new VariableDefinition(excType);
            var resultLocal = method.ReturnType.FullName != "System.Void" 
                ? new VariableDefinition(method.ReturnType) : null;
            if (resultLocal != null)
                method.Body.Variables.Add(resultLocal);
            method.Body.Variables.Add(tempLocal);
            method.Parameters.Add(outParam);

            var defaultException = Instruction.Create(OpCodes.Ldnull);
            var insns = method.Body.Instructions;

            insns.Insert(0, Instruction.Create(OpCodes.Nop));

            // At method body entry we always initialize out parameters to ensure that 
            //  all rets will be valid
            for (int i = 0; i < method.Parameters.Count; i++) {
                var param = method.Parameters[i];
                if (!param.Attributes.HasFlag(ParameterAttributes.Out))
                    continue;
                if (!param.ParameterType.IsByReference)
                    continue;

                var valueType = param.ParameterType.GetElementType();

                InsertOps(insns, 1, new[] {
                    Instruction.Create(OpCodes.Ldarg, param),
                    MakeDefault(valueType),
                    Instruction.Create(OpCodes.Stind_Ref)
                });
            }

            if (resultLocal != null) {
                InsertOps(insns, 1, new[] {
                    MakeDefault(method.ReturnType),
                    Instruction.Create(OpCodes.Stloc, resultLocal)
                });
            }

            // We generate an exit point at the end of the function where the result local is
            //  loaded and returned + any finally blocks are run
            var exitLoad = resultLocal != null ? Instruction.Create(OpCodes.Ldloc, resultLocal) : null;
            var exitRet = Instruction.Create(OpCodes.Ret);
            var exitPoint = exitLoad ?? exitRet;

            for (int i = 0; i < insns.Count; i++) {
                var insn = insns[i];

                switch (insn.OpCode.Code) {
                    case Code.Throw:
                        // store the exc, load the address of the outparam, then load + restore the exc
                        insns[i] = Instruction.Create(OpCodes.Stloc, tempLocal);
                        Patch(method, insn, insns[i]);
                        InsertOps(insns, i + 1, new[] { 
                            Instruction.Create(OpCodes.Ldarg, outParam),
                            Instruction.Create(OpCodes.Ldloc, tempLocal),
                            Instruction.Create(OpCodes.Stind_Ref),
                            Instruction.Create(OpCodes.Br, exitPoint)
                        });
                        break;
                    case Code.Ret:
                        // Jump to the exit point where we will run finally blocks
                        insns[i] = Instruction.Create(OpCodes.Br, exitRet);
                        Patch(method, insn, insns[i]);
                        if (resultLocal != null) {
                            // Stash the retval from the stack into the result local
                            insns.Insert(i, Instruction.Create(OpCodes.Stloc, resultLocal));
                        }
                        break;
                    default:
                        continue;
                }
            }

            insns.Add(Instruction.Create(OpCodes.Nop));
            if (exitLoad != null)
                insns.Add(exitLoad);
            insns.Add(exitRet);
        }

        private Instruction RemapInstruction (
            Instruction old,
            MethodBody oldBody, Mono.Collections.Generic.Collection<Instruction> newBody
        ) {
            if (old == null)
                return null;

            int idx = oldBody.Instructions.IndexOf(old);
            return newBody[idx];
        }

        private MethodDefinition CloneMethod (MethodDefinition source) {
            MethodDefinition targetMethod = new MethodDefinition(
                source.Name + "_impl", source.Attributes, 
                source.ReturnType
            );

            // Copy the parameters; 
            foreach (var p in source.Parameters) {
                ParameterDefinition nP = new ParameterDefinition(p.Name, p.Attributes, p.ParameterType);
                targetMethod.Parameters.Add(nP);
            }

            // copy the body
            var nBody = targetMethod.Body;
            var oldBody = source.Body;

            nBody.InitLocals = oldBody.InitLocals;

            // copy the local variable definition
            foreach (var v in oldBody.Variables) {
                var nv = new VariableDefinition(v.VariableType);
                nBody.Variables.Add(nv);
            }

            // copy the IL; we only need to take care of reference and method definitions
            Mono.Collections.Generic.Collection<Instruction> col = 
nBody.Instructions;
            foreach (Instruction i in oldBody.Instructions)
            {
                object operand = i.Operand;
                if (operand == null)
                {
                    col.Add(Instruction.Create(i.OpCode));
                    continue;
                }

                if (operand is FieldReference)
                {
                    FieldReference fref = operand as FieldReference;
                    col.Add(Instruction.Create(i.OpCode, fref));
                    continue;
                }
                else if (operand is TypeReference)
                {
                    TypeReference tref = operand as TypeReference;
                    col.Add(Instruction.Create(i.OpCode, tref));
                    continue;
                }
                else if (operand is TypeDefinition)
                {
                    TypeDefinition tdef = operand as TypeDefinition;
                    col.Add(Instruction.Create(i.OpCode, tdef));
                    continue;
                }
                else if (operand is MethodReference)
                {
                    // FIXME: Swap around if this is a reference to a rewritten method
                    MethodReference mref = operand as MethodReference;
                    col.Add(Instruction.Create(i.OpCode, mref));
                    continue;
                }

                // FIXME: Cecil doesn't let you clone Instruction objects
                col.Add(i);
            }

            // Fixup branches
            for (int i = 0; i < col.Count; i++) {
                var insn = col[i];
                var operand = insn.Operand as Instruction;
                if (operand == null)
                    continue;

                var newOperand = RemapInstruction(operand, oldBody, col);
                var newInsn = Instruction.Create(insn.OpCode, newOperand);
                col[i] = newInsn;
            }

            // copy the exception handler blocks

            foreach (ExceptionHandler eh in oldBody.ExceptionHandlers)
            {
                ExceptionHandler neh = new ExceptionHandler(eh.HandlerType);
                neh.CatchType = eh.CatchType;
                neh.HandlerType = eh.HandlerType;

                neh.TryStart = RemapInstruction(eh.TryStart, oldBody, col);
                neh.TryEnd = RemapInstruction(eh.TryEnd, oldBody, col);
                neh.HandlerStart = RemapInstruction(eh.HandlerStart, oldBody, col);
                neh.HandlerEnd = RemapInstruction(eh.HandlerEnd, oldBody, col);
                neh.FilterStart = RemapInstruction(eh.FilterStart, oldBody, col);

                nBody.ExceptionHandlers.Add(neh);
            }
            
            targetMethod.DeclaringType = source.DeclaringType;
            return targetMethod;
        }
    }
}
