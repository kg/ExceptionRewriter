using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace ExceptionRewriter {
    public class AssemblyRewriter {
        public readonly AssemblyDefinition Assembly;
        public readonly AssemblyAnalyzer Analyzer;

        private TypeDefinition ExceptionFilter;

        public AssemblyRewriter (AssemblyAnalyzer analyzer) {
            Assembly = analyzer.Input;
            Analyzer = analyzer;
        }

        // The encouraged typeof() based import isn't valid because it will import
        //  corelib types into netframework apps. yay
        private TypeReference ImportCorlibType (ModuleDefinition module, string @namespace, string name) {
            foreach (var m in Assembly.Modules) {
                var ts = m.TypeSystem;
                var mLookup = ts.GetType().GetMethod("LookupType", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                var result = mLookup.Invoke(ts, new object[] { @namespace, name });
                if (result != null)
                    return module.ImportReference((TypeReference)result);
            }

            return null;
        }

        private TypeReference GetException (ModuleDefinition module) {
            return ImportCorlibType(module, "System", "Exception");
        }

        private TypeReference GetExceptionDispatchInfo (ModuleDefinition module) {
            return ImportCorlibType(module, "System.Runtime.ExceptionServices", "ExceptionDispatchInfo");
        }

        public void Rewrite () {
            ExceptionFilter = Assembly.MainModule.GetType("Mono.Runtime.Internal.ExceptionFilter");

            var queue = new HashSet<AnalyzedMethod>();

            foreach (var m in Analyzer.Methods.Values) {
                if (!m.ShouldRewrite)
                    continue;

                queue.Add(m);
            }

            foreach (var m in queue) {
                Console.WriteLine("Rewriting {0}", m.Method.FullName);
                Rewrite(m);
            }
        }

        /*

        private TypeDefinition CreateRewriterUtilType () {
            var mod = Assembly.MainModule;
            var td = new TypeDefinition(
                "Mono.AssemblyRewriter", "Internal",
                TypeAttributes.Sealed | TypeAttributes.Public,
                mod.TypeSystem.Object
            );

            var tException = GetException(mod);
            var tDispatchInfo = GetExceptionDispatchInfo(mod);
            var method = new MethodDefinition(
                "CaptureStackTrace", MethodAttributes.Static | MethodAttributes.Public | MethodAttributes.Final, tDispatchInfo
            );
            var result = new VariableDefinition(tDispatchInfo);

            var excParam = new ParameterDefinition("exc", ParameterAttributes.None, tException);
            method.Parameters.Add(excParam);

            var body = method.Body = new MethodBody(method);
            body.Variables.Add(result);

            var loadResult = Instruction.Create(OpCodes.Ldloc, result);

            var tryStart = Instruction.Create(OpCodes.Ldarg_0);
            body.Instructions.Add(tryStart);
            body.Instructions.Add(Instruction.Create(OpCodes.Throw));
            body.Instructions.Add(Instruction.Create(OpCodes.Leave, loadResult));

            var catchOp = Instruction.Create(OpCodes.Ldnull);

            // catch { result = ExceptionDispatchInfo.Capture(exc); }
            body.Instructions.Add(catchOp);
            body.Instructions.Add(Instruction.Create(OpCodes.Ldarg_0));
            body.Instructions.Add(Instruction.Create(OpCodes.Call, GetCapture(mod)));
            body.Instructions.Add(Instruction.Create(OpCodes.Stloc, result));
            body.Instructions.Add(Instruction.Create(OpCodes.Leave, loadResult));

            // return result;
            body.Instructions.Add(loadResult);
            body.Instructions.Add(Instruction.Create(OpCodes.Ret));

            body.ExceptionHandlers.Add(new ExceptionHandler(ExceptionHandlerType.Catch) {
                TryStart = tryStart,
                TryEnd = catchOp,
                HandlerStart = catchOp,
                HandlerEnd = loadResult,
                CatchType = tException
            });

            td.Methods.Add(method);
            mod.Types.Add(td);
            return td;
        }

        */

        private void Rewrite (AnalyzedMethod am) {
            var method = am.Method;
            ExtractExceptionFilters(method);
        }

        private Instruction[] MakeDefault (
            TypeReference t,
            Dictionary<TypeReference, VariableDefinition> tempLocals
        ) {
            if (t.FullName == "System.Void")
                return new Instruction[0];

            if (t.IsByReference || !t.IsValueType)
                return new[] { Instruction.Create(OpCodes.Ldnull) };

            switch (t.FullName) {
                case "System.Int32":
                case "System.UInt32":
                case "System.Boolean":
                    return new[] { Instruction.Create(OpCodes.Ldc_I4_0) };
                default:
                    VariableDefinition tempLocal;
                    if (!tempLocals.TryGetValue(t, out tempLocal)) {
                        tempLocals[t] = tempLocal = new VariableDefinition(t);
                        return new[] {
                            Instruction.Create(OpCodes.Ldloca_S, tempLocal),
                            Instruction.Create(OpCodes.Initobj, t),
                            Instruction.Create(OpCodes.Ldloc, tempLocal)
                        };
                    } else
                        return new[] { Instruction.Create(OpCodes.Ldloc, tempLocal) };
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

        private void ExtractExceptionFilters (MethodDefinition method) {
            var tempStructLocals = new Dictionary<TypeReference, VariableDefinition>();
            var excType = GetException(method.Module);
            var tempExceptionLocal = new VariableDefinition(excType);
            method.Body.Variables.Add(tempExceptionLocal);

            var defaultException = Instruction.Create(OpCodes.Ldnull);
            var insns = method.Body.Instructions;
            insns.Insert(0, Instruction.Create(OpCodes.Nop));

            var handlersByExit = method.Body.ExceptionHandlers.ToLookup((eh => eh.HandlerEnd));
            var exceptionFilters = (from eh in method.Body.ExceptionHandlers where eh.FilterStart != null select eh).ToList();

            var deadExceptionHandlers = new HashSet<ExceptionHandler>();
            var filterIndex = 0;

            foreach (var group in handlersByExit) {
                foreach (var eh in group) {
                    if (eh.FilterStart == null)
                        continue;

                    method.Body.ExceptionHandlers.Remove(eh);

                    var filterMethod = new MethodDefinition(
                        method.Name + "__filter" + (filterIndex++),
                        MethodAttributes.Static | MethodAttributes.Public,
                        this.ImportCorlibType(method.Module, "System", "Int32")
                    );

                    Instruction filter = null;
                    var filterReplacement = Instruction.Create(OpCodes.Ret);

                    foreach (var loc in method.Body.Variables)
                        filterMethod.Body.Variables.Add(loc);

                    method.DeclaringType.Methods.Add(filterMethod);

                    int i1 = insns.IndexOf(eh.FilterStart), i2 = -1;
                    for (int i = i1; i < insns.Count; i++) {
                        var insn = insns[i];
                        if (insn.OpCode == OpCodes.Endfilter) {
                            i2 = i;
                            break;
                        }
                    }

                    CloneInstructions(insns, i1, i2 - i1 + 1, filterMethod.Body.Instructions, 0);
                }
            }

            /*
            foreach (var eh in method.Body.ExceptionHandlers) {
                if (eh.FilterStart == null)
                    continue;

                Instruction catchStart = eh.HandlerStart, catchEnd = eh.HandlerEnd, tryStart = eh.TryStart;
                int i = insns.IndexOf(eh.HandlerStart), i2 = insns.IndexOf(eh.HandlerEnd), i0 = insns.IndexOf(tryStart);
            }

            foreach (var eh in deadExceptionHandlers)
                method.Body.ExceptionHandlers.Remove(eh);

            // We generate an exit point at the end of the function where the result local is
            //  loaded and returned + any finally blocks are run
            var exitLoad = resultLocal != null ? Instruction.Create(OpCodes.Ldloc, resultLocal) : null;
            var exitRet = Instruction.Create(OpCodes.Ret);
            var exitPoint = exitLoad ?? exitRet;
            ExceptionHandler tryBlock;

            for (int i = 0; i < insns.Count; i++) {
                var insn = insns[i];

                switch (insn.OpCode.Code) {
                    /*
                    case Code.Call:
                        var target = (MethodReference)insn.Operand;
                        var am = Analyzer.GetResult(target);
                        if ((am == null) || !am.ShouldRewrite)
                            continue;
                        var newTarget = am.BackingMethod;
                        insns[i] = Instruction.Create(OpCodes.Call, newTarget);
                        Patch(method, insn, insns[i]);
                        // Generate an error check immediately after the call
                        tryBlocksByInsn.TryGetValue(insn, out tryBlock);
                        InsertErrorCheck(insns, i + 1, method.Module, outParam, exitPoint, tryBlock);
                        // Push the out param onto the args list since we're now calling the backing method
                        //  which takes an out param
                        insns.Insert(i, Instruction.Create(OpCodes.Ldarg, outParam));
                        break;
                    case Code.Rethrow:
                        // FIXME
                        if (!tryBlocksByInsn.TryGetValue(insn, out tryBlock)) {
                            insns[i] = Instruction.Create(OpCodes.Nop);
                            Patch(method, insn, insns[i]);
                            continue;
                        }

                        if (deadExceptionHandlers.Contains(tryBlock)) {
                            insns[i] = Instruction.Create(OpCodes.Nop);
                            Patch(method, insn, insns[i]);
                        } else {
                            // Load the address of the output before doing the rethrow conversion
                            insns[i] = Instruction.Create(OpCodes.Ldarg, outParam);
                            Patch(method, insn, insns[i]);
                            InsertOps(insns, i + 1, new[] {
                                // Load the exception we're rethrowing (we stored it at try entry) and convert it
                                Instruction.Create(OpCodes.Ldloc, tempExceptionLocal),
                                Instruction.Create(OpCodes.Call, CaptureStackImpl),
                                // Write the info into the output
                                Instruction.Create(OpCodes.Stind_Ref),
                                // Branch to exit, clearing the stack.
                                Instruction.Create(OpCodes.Leave, exitPoint)
                            });
                        }
                        break;
                    case Code.Throw:
                        // Pass the exc through capture to store the stack trace on it
                        insns[i] = Instruction.Create(OpCodes.Call, CaptureStackImpl);
                        Patch(method, insn, insns[i]);
                        tryBlocksByInsn.TryGetValue(insn, out tryBlock);
                        // If this throw is inside a try/catch then we need to clear the stack w/leave
                        var exitOpcode = tryBlock != null ? OpCodes.Leave : OpCodes.Br;
                        InsertOps(insns, i + 1, new[] {
                            // Store it into a temp exception local
                            Instruction.Create(OpCodes.Stloc, tempEdiLocal),
                            // Prepare to store the output
                            Instruction.Create(OpCodes.Ldarg, outParam),
                            // Now convert it into an ExceptionDispatchInfo
                            Instruction.Create(OpCodes.Ldloc, tempEdiLocal),
                            // Write the info into the output
                            Instruction.Create(OpCodes.Stind_Ref),
                            // Branch to exit
                            Instruction.Create(exitOpcode, exitPoint)
                        });
                        break;
                    case Code.Ret:
                        // Jump to the exit point where we will run finally blocks
                        insns[i] = Instruction.Create(OpCodes.Br, exitPoint);
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
            */

            foreach (var kvp in tempStructLocals)
                method.Body.Variables.Add(kvp.Value);

            insns.Add(Instruction.Create(OpCodes.Nop));
        }

        private bool ShouldPreserveTryBlock (
            ExceptionHandler eh
        ) {
            // FIXME: Detect whether the try block contains any operations that can throw,
            //  and if it does, preserve it.
            return false;
        }

        private Instruction RemapInstruction (
            Instruction old,
            Mono.Collections.Generic.Collection<Instruction> oldBody, 
            Mono.Collections.Generic.Collection<Instruction> newBody,
            int offset = 0
        ) {
            if (old == null)
                return null;

            int idx = oldBody.IndexOf(old);
            var newIdx = idx + offset;
            return newBody[newIdx];
        }

        private Instruction CloneInstruction (Instruction i) {
            object operand = i.Operand;
            if (operand == null)
                return Instruction.Create(i.OpCode);

            if (operand is FieldReference) {
                FieldReference fref = operand as FieldReference;
                return Instruction.Create(i.OpCode, fref);
            } else if (operand is TypeReference) {
                TypeReference tref = operand as TypeReference;
                return Instruction.Create(i.OpCode, tref);
            } else if (operand is TypeDefinition) {
                TypeDefinition tdef = operand as TypeDefinition;
                return Instruction.Create(i.OpCode, tdef);
            } else if (operand is MethodReference) {
                // FIXME: Swap around if this is a reference to a rewritten method
                MethodReference mref = operand as MethodReference;
                return Instruction.Create(i.OpCode, mref);
            } else if (operand is Instruction) {
                var insn = operand as Instruction;
                return Instruction.Create(i.OpCode, insn);
            } else if (operand is string) {
                var insn = operand as string;
                return Instruction.Create(i.OpCode, insn);
            } else {
                throw new NotImplementedException(i.OpCode.ToString());
            }
        }

        private void CloneInstructions (
            Mono.Collections.Generic.Collection<Instruction> source,
            int sourceIndex, int count,
            Mono.Collections.Generic.Collection<Instruction> target,
            int targetIndex
        ) {
            for (int n = 0; n < count; n++) {
                var i = source[n + sourceIndex];
                var newInsn = CloneInstruction(i);

                // FIXME: Cecil doesn't let you clone Instruction objects
                target.Add(i);
            }

            // Fixup branches
            for (int i = 0; i < target.Count; i++) {
                var insn = target[i];
                var operand = insn.Operand as Instruction;
                if (operand == null)
                    continue;

                var newOperand = RemapInstruction(operand, source, target, targetIndex - sourceIndex);
                var newInsn = Instruction.Create(insn.OpCode, newOperand);
                target[i] = newInsn;
            }
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

            CloneInstructions(oldBody.Instructions, 0, oldBody.Instructions.Count, nBody.Instructions, 0);

            // copy the exception handler blocks

            foreach (ExceptionHandler eh in oldBody.ExceptionHandlers) {
                ExceptionHandler neh = new ExceptionHandler(eh.HandlerType);
                neh.CatchType = eh.CatchType;
                neh.HandlerType = eh.HandlerType;

                neh.TryStart = RemapInstruction(eh.TryStart, oldBody.Instructions, col);
                neh.TryEnd = RemapInstruction(eh.TryEnd, oldBody.Instructions, col);
                neh.HandlerStart = RemapInstruction(eh.HandlerStart, oldBody.Instructions, col);
                neh.HandlerEnd = RemapInstruction(eh.HandlerEnd, oldBody.Instructions, col);
                neh.FilterStart = RemapInstruction(eh.FilterStart, oldBody.Instructions, col);

                nBody.ExceptionHandlers.Add(neh);
            }

            targetMethod.DeclaringType = source.DeclaringType;
            return targetMethod;
        }
    }
}
