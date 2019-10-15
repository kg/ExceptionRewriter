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

        private TypeDefinition RewriterUtilType;
        private MethodDefinition CaptureStackImpl;

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
            RewriterUtilType = Assembly.MainModule.GetType("Mono.AssemblyRewriter.Internal");
            if (RewriterUtilType == null)
                RewriterUtilType = CreateRewriterUtilType();
            CaptureStackImpl = RewriterUtilType.Methods.First(m => m.Name == "CaptureStackTrace");

            var queue = new HashSet<AnalyzedMethod>();

            // First, collect a full set of all the methods we will rewrite
            // The rewriting process needs to know this in order to identify whether
            //  a given method call needs to be wrapped in a try block.

            foreach (var m in Analyzer.Methods.Values) {
                if (!m.ShouldRewrite)
                    continue;

                // Create empty backing method definitions for each method, so that
                //  we can reference them when modifying other methods.
                m.BackingMethod = CloneMethod(m.Method);
                m.Method.DeclaringType.Methods.Add(m.BackingMethod);

                queue.Add(m);
            }

            foreach (var m in queue) {
                Console.WriteLine("Rewriting {0}", m.Method.FullName);
                Rewrite(m);
            }
        }

        private MethodReference GetCapture (ModuleDefinition module) {
            var tDispatchInfo = GetExceptionDispatchInfo(module);
            MethodReference mCapture = new MethodReference(
                "Capture", tDispatchInfo, tDispatchInfo
            ) {
                Parameters = {
                    new ParameterDefinition("source", ParameterAttributes.None, GetException(module))
                },
                HasThis = false
            };
            return module.ImportReference(mCapture);
        }

        private MethodReference GetThrow (ModuleDefinition module) {
            var tDispatchInfo = GetExceptionDispatchInfo(module);
            return module.ImportReference(new MethodReference(
                "Throw", module.TypeSystem.Void, tDispatchInfo
            ) {
                HasThis = true
            });
        }

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

        private void Rewrite (AnalyzedMethod am) {
            var method = am.Method;
            var backing = am.BackingMethod;
            ConvertToOutException(backing);
            ReplaceWithBackingCall(method, backing);
        }

        private void ReplaceWithBackingCall (
            MethodDefinition method, MethodDefinition target
        ) {
            var body = method.Body.Instructions;
            body.Clear();
            method.Body.ExceptionHandlers.Clear();

            var ediType = GetExceptionDispatchInfo(method.Module);
            var refType = new ByReferenceType(ediType);

            var excVariable = new VariableDefinition(ediType);
            method.Body.Variables.Add(excVariable);

            // Null-init the exc outvar (is this necessary?)
            body.Add(Instruction.Create(OpCodes.Ldnull));
            body.Add(Instruction.Create(OpCodes.Stloc, excVariable));

            // Push the arguments onto the stack
            for (int i = 0; i < method.Parameters.Count; i++)
                body.Add(Instruction.Create(OpCodes.Ldarg, method.Parameters[i]));
            // Push address of our exc local for the out exc parameter
            body.Add(Instruction.Create(OpCodes.Ldloca, excVariable));
            // Now invoke, leaving retval on the stack.
            body.Add(Instruction.Create(OpCodes.Call, target));

            var ret = Instruction.Create(OpCodes.Ret);

            // Now read exc local and throw if != null
            body.Add(Instruction.Create(OpCodes.Ldloc, excVariable));
            body.Add(Instruction.Create(OpCodes.Ldnull));
            body.Add(Instruction.Create(OpCodes.Ceq));
            body.Add(Instruction.Create(OpCodes.Brtrue, ret));
            body.Add(Instruction.Create(OpCodes.Ldloc, excVariable));
            body.Add(Instruction.Create(OpCodes.Call, GetThrow(method.Module)));

            body.Add(ret);
        }

        private Instruction[] MakeDefault (
            TypeReference t,
            Dictionary<TypeReference, VariableDefinition> tempLocals
        ) {
            if (t.FullName == "System.Void")
                return new Instruction[0];

            if (t.IsByReference || !t.IsValueType)
                return new [] { Instruction.Create(OpCodes.Ldnull) };

            switch (t.FullName) {
                case "System.Int32":
                case "System.UInt32":
                case "System.Boolean":
                    return new [] { Instruction.Create(OpCodes.Ldc_I4_0) };
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

        private void InsertErrorCheck (
            Mono.Collections.Generic.Collection<Instruction> body, int offset, ModuleDefinition module, 
            ParameterDefinition outParam, Instruction exitPoint, ExceptionHandler tryBlock
        ) {
            var skipTarget = Instruction.Create(OpCodes.Nop);
            var onError = (tryBlock != null) ? tryBlock.HandlerStart : exitPoint;
            InsertOps(body, offset, new[] {
                Instruction.Create(OpCodes.Ldarg, outParam),
                Instruction.Create(OpCodes.Ldind_Ref),
                Instruction.Create(OpCodes.Ldnull),
                Instruction.Create(OpCodes.Ceq),
                Instruction.Create(OpCodes.Brtrue, skipTarget),
                Instruction.Create(OpCodes.Br, onError),
                skipTarget
                /*
                Instruction.Create(OpCodes.Ldarg, outParam),
                Instruction.Create(OpCodes.Ldind_Ref),
                Instruction.Create(OpCodes.Call, GetThrow(module)),
                */
            });
        }

        private void ConvertToOutException (MethodDefinition method) {
            var tempStructLocals = new Dictionary<TypeReference, VariableDefinition>();
            var excType = GetException(method.Module);
            var ediType = GetExceptionDispatchInfo(method.Module);
            var refType = new ByReferenceType(ediType);
            var outParam = new ParameterDefinition("_error", ParameterAttributes.Out, refType);
            var tempExceptionLocal = new VariableDefinition(excType);
            var tempEdiLocal = new VariableDefinition(ediType);
            var resultLocal = method.ReturnType.FullName != "System.Void" 
                ? new VariableDefinition(method.ReturnType) : null;
            if (resultLocal != null)
                method.Body.Variables.Add(resultLocal);
            method.Body.Variables.Add(tempExceptionLocal);
            method.Body.Variables.Add(tempEdiLocal);
            method.Parameters.Add(outParam);

            var defaultException = Instruction.Create(OpCodes.Ldnull);
            var insns = method.Body.Instructions;
            var tryBlocksByInsn = new Dictionary<Instruction, ExceptionHandler>();

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

                InsertOps(insns, 1, 
                    (new[] { Instruction.Create(OpCodes.Ldarg, param) })
                    .Concat(MakeDefault(valueType, tempStructLocals))
                    .Concat(new [] { Instruction.Create(OpCodes.Stind_Ref)})
                    .ToArray()
                );
            }

            if (resultLocal != null) {
                InsertOps(insns, 1, 
                    MakeDefault(method.ReturnType, tempStructLocals)
                    .Concat(new[] { Instruction.Create(OpCodes.Stloc, resultLocal) })
                    .ToArray()
                );
            }

            var deadExceptionHandlers = new HashSet<ExceptionHandler>();

            foreach (var eh in method.Body.ExceptionHandlers) {
                if (eh.HandlerStart == null)
                    continue;
                if (eh.HandlerType != ExceptionHandlerType.Catch)
                    continue;

                Instruction catchStart = eh.HandlerStart, catchEnd = eh.HandlerEnd, tryStart = eh.TryStart;
                int i = insns.IndexOf(eh.HandlerStart), i2 = insns.IndexOf(eh.HandlerEnd), i0 = insns.IndexOf(tryStart);

                if (ShouldPreserveTryBlock(eh)) {
                    // Store a copy of the exception at entry. We need this to implement rethrow
                    // We also want to erase the current error value here since the catch is handling
                    //  it and is either erasing it or writing something new into it by throw/rethrow
                    insns[i] = Instruction.Create(OpCodes.Dup);
                    Patch(method, catchStart, insns[i]);
                    InsertOps(insns, i + 1, new[] {
                        Instruction.Create(OpCodes.Stloc, tempExceptionLocal),
                        Instruction.Create(OpCodes.Ldarg, outParam),
                        Instruction.Create(OpCodes.Ldnull),
                        Instruction.Create(OpCodes.Stind_Ref),
                        catchStart,
                    });
                } else {
                    // Replace leaves with branches to the end of the catch block.
                    for (int j = i0; j < i2; j++) {
                        var insn = insns[j];
                        tryBlocksByInsn[insn] = eh;

                        if ((insn.OpCode != OpCodes.Leave) && (insn.OpCode != OpCodes.Leave_S))
                            continue;

                        // We unconditionally use leave here even though the EH is gone
                        //  because it clears the stack.
                        insns[j] = Instruction.Create(OpCodes.Leave, catchEnd);
                        Patch(method, insn, insns[j]);
                    }

                    // Push the current contents of the error arg onto the stack, then zero it.
                    // This matches try blocks where the ex is on the stack at entry.
                    insns[i] = Instruction.Create(OpCodes.Ldarg, outParam);
                    Patch(method, catchStart, insns[i]);
                    InsertOps(insns, i + 1, new[] {
                        Instruction.Create(OpCodes.Ldind_Ref),
                        Instruction.Create(OpCodes.Ldarg, outParam),
                        Instruction.Create(OpCodes.Ldnull),
                        Instruction.Create(OpCodes.Stind_Ref),
                    });

                    deadExceptionHandlers.Add(eh);
                }
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

            foreach (var kvp in tempStructLocals)
                method.Body.Variables.Add(kvp.Value);

            insns.Add(Instruction.Create(OpCodes.Nop));
            if (exitLoad != null)
                insns.Add(exitLoad);
            insns.Add(exitRet);
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
