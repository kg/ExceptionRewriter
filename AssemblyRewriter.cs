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

        private int ClosureIndex, FilterIndex;

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
                if (body[i].OpCode.Code == Code.Brtrue_S)
                    ;
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

        private Instruction ExtractExceptionHandlerExitTarget (ExceptionHandler eh) {
            var leave = eh.HandlerEnd.Previous;
            if (leave.OpCode == OpCodes.Rethrow)
                return leave;

            var leaveTarget = leave.Operand as Instruction;
            if (leaveTarget == null)
                throw new Exception("Exception handler did not end with a 'leave'");
            return leaveTarget;
        }

        private bool IsStoreOperation (Code opcode) {
            switch (opcode) {
                case Code.Stloc:
                case Code.Stloc_S:
                case Code.Stloc_0:
                case Code.Stloc_1:
                case Code.Stloc_2:
                case Code.Stloc_3:
                case Code.Starg:
                case Code.Starg_S:
                    return true;

                case Code.Ldloca:
                case Code.Ldloca_S:
                case Code.Ldloc:
                case Code.Ldloc_S:
                case Code.Ldloc_0:
                case Code.Ldloc_1:
                case Code.Ldloc_2:
                case Code.Ldloc_3:
                case Code.Ldarg:
                case Code.Ldarg_S:
                case Code.Ldarga:
                case Code.Ldarga_S:
                case Code.Ldarg_0:
                case Code.Ldarg_1:
                case Code.Ldarg_2:
                case Code.Ldarg_3:
                    return false;
            }

            throw new NotImplementedException(opcode.ToString());
        }

        private VariableDefinition LookupNumberedVariable (
            Code opcode, Mono.Collections.Generic.Collection<VariableDefinition> variables
        ) {
            switch (opcode) {
                case Code.Ldloc_0:
                case Code.Stloc_0:
                    return variables[0];
                case Code.Ldloc_1:
                case Code.Stloc_1:
                    return variables[1];
                case Code.Ldloc_2:
                case Code.Stloc_2:
                    return variables[2];
                case Code.Ldloc_3:
                case Code.Stloc_3:
                    return variables[3];
            }

            return null;
        }

        private ParameterDefinition LookupNumberedArgument (
            Code opcode, ParameterDefinition fakeThis, Mono.Collections.Generic.Collection<ParameterDefinition> parameters
        ) {
            int staticOffset = fakeThis == null ? 0 : 1;
            switch (opcode) {
                case Code.Ldarg_0:
                    if (fakeThis == null)
                        return parameters[0];
                    else
                        return fakeThis;
                case Code.Ldarg_1:
                    return parameters[1 - staticOffset];
                case Code.Ldarg_2:
                    return parameters[2 - staticOffset];
                case Code.Ldarg_3:
                    return parameters[3 - staticOffset];
            }

            return null;
        }

        private MethodDefinition CreateConstructor (TypeDefinition type) {
            var ctorMethod = new MethodDefinition(
                ".ctor", MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName, 
                type.Module.TypeSystem.Void
            );
            type.Methods.Add(ctorMethod);
            InsertOps(ctorMethod.Body.Instructions, 0, new[] {
                Instruction.Create(OpCodes.Ldarg_0),
                Instruction.Create(OpCodes.Call, 
                    new MethodReference(
                        ".ctor", type.Module.TypeSystem.Void, 
                        type.BaseType
                    ) { HasThis = true }),
                Instruction.Create(OpCodes.Nop),
                Instruction.Create(OpCodes.Ret)
            });
            return ctorMethod;
        }

        private VariableDefinition ConvertToClosure (MethodDefinition method, out TypeDefinition closureType) {
            var insns = method.Body.Instructions;
            closureType = new TypeDefinition(
                method.DeclaringType.Namespace, method.Name + "__closure" + (ClosureIndex++).ToString("X4"),
                TypeAttributes.Class | TypeAttributes.NestedPrivate
            );
            closureType.BaseType = method.Module.TypeSystem.Object;
            method.DeclaringType.NestedTypes.Add(closureType);

            var ctorMethod = CreateConstructor(closureType);

            var isStatic = method.IsStatic;

            var localCount = 0;
            var closureVar = new VariableDefinition(closureType);

            var extractedVariables = method.Body.Variables.ToDictionary(
                v => (object)v, 
                v => new FieldDefinition("local" + localCount++, FieldAttributes.Public, v.VariableType)
            );

            method.Body.Variables.Add(closureVar);

            for (int i = 0; i < method.Parameters.Count; i++) {
                var p = method.Parameters[i];
                var name = (p.Name != null) ? "arg_" + p.Name : "arg" + i;
                extractedVariables[p] = new FieldDefinition(name, FieldAttributes.Public, p.ParameterType);
            }

            var fakeThis = new ParameterDefinition("__this", ParameterAttributes.None, method.DeclaringType);
            if (!isStatic)
                extractedVariables[fakeThis] = new FieldDefinition("__this", FieldAttributes.Public, method.DeclaringType);

            foreach (var kvp in extractedVariables)
                closureType.Fields.Add(kvp.Value);

            for (int i = 0; i < insns.Count; i++) {
                var insn = insns[i];

                var variable = (insn.Operand as VariableDefinition) 
                    ?? LookupNumberedVariable(insn.OpCode.Code, method.Body.Variables);
                var arg = (insn.Operand as ParameterDefinition)
                    ?? LookupNumberedArgument(insn.OpCode.Code, isStatic ? null : fakeThis, method.Parameters);

                // FIXME
                if (variable == closureVar)
                    continue;

                if ((variable == null) && (arg == null))
                    continue;

                var matchingField = extractedVariables[(object)variable ?? arg];

                if (IsStoreOperation(insn.OpCode.Code)) {
                    // HACK: Because we have no way to swap values on the stack, we have to keep the
                    //  existing local but use it as a temporary store point before flushing into the
                    //  closure
                    Instruction reload;
                    if (variable != null)
                        reload = Instruction.Create(OpCodes.Ldloc, variable);
                    else
                        reload = Instruction.Create(OpCodes.Ldarg, arg);

                    InsertOps(insns, i + 1, new[] {
                        Instruction.Create(OpCodes.Ldloc, closureVar),
                        reload,
                        Instruction.Create(OpCodes.Stfld, matchingField)
                    });
                    i += 3;
                } else {
                    var newInsn = Instruction.Create(OpCodes.Ldloc, closureVar);
                    insns[i] = newInsn;
                    Patch(method, insn, newInsn);
                    var loadOp =
                        ((insn.OpCode.Code == Code.Ldloca) ||
                        (insn.OpCode.Code == Code.Ldloca_S))
                            ? OpCodes.Ldflda
                            : OpCodes.Ldfld;
                    insns.Insert(i + 1, Instruction.Create(loadOp, matchingField));
                    i++;
                }
            }

            var toInject = new List<Instruction>() {
                Instruction.Create(OpCodes.Newobj, closureType.Methods.First(m => m.Name == ".ctor")),
                Instruction.Create(OpCodes.Stloc, closureVar)
            };

            if (!isStatic) {
                toInject.AddRange(new[] {
                    Instruction.Create(OpCodes.Ldloc, closureVar),
                    Instruction.Create(OpCodes.Ldarg_0),
                    Instruction.Create(OpCodes.Stfld, extractedVariables[fakeThis])
                });
            }

            foreach (var p in method.Parameters) {
                toInject.AddRange(new[] {
                    Instruction.Create(OpCodes.Ldloc, closureVar),
                    Instruction.Create(OpCodes.Ldarg, p),
                    Instruction.Create(OpCodes.Stfld, extractedVariables[p])
                });
            }

            InsertOps(insns, 0, toInject.ToArray());

            return closureVar;
        }

        private int CatchCount;

        private ExcHandler ExtractCatch (
            MethodDefinition method, ExceptionHandler eh, VariableDefinition closure, ExcGroup group
        ) {
            var insns = method.Body.Instructions;
            var closureType = closure.VariableType;

            var catchMethod = new MethodDefinition(
                method.Name + "__catch" + (CatchCount++),
                MethodAttributes.Static | MethodAttributes.Private,
                method.Module.TypeSystem.Int32
            );
            catchMethod.Body.InitLocals = true;
            var closureParam = new ParameterDefinition("__closure", ParameterAttributes.None, closureType);
            var excParam = new ParameterDefinition("__exc", ParameterAttributes.None, method.Module.TypeSystem.Object);
            // Exc goes first because catch blocks start with it on the stack and this makes it convenient to spam dup
            catchMethod.Parameters.Add(excParam);
            catchMethod.Parameters.Add(closureParam);

            var catchInsns = catchMethod.Body.Instructions;

            var newVariables = ExtractRangeToMethod(
                method, catchMethod, 
                insns.IndexOf(eh.HandlerStart), insns.IndexOf(eh.HandlerEnd) - 1,
                false, 
                (insn, operand) => {
                    switch (insn.OpCode.Code) {
                        case Code.Leave:
                        case Code.Leave_S:
                            return insn;
                        default:
                            return null;
                    }
                }
            );

            InsertOps(
                catchInsns, 0, new[] {
                    Instruction.Create(OpCodes.Ldarg, closureParam),
                    Instruction.Create(OpCodes.Stloc, newVariables[closure]),
                    // HACK: Compensate for weird generated code from roslyn
                    (catchInsns[0].OpCode.Code == Code.Pop)
                        ? Instruction.Create(OpCodes.Ldarg, excParam)
                        : Instruction.Create(OpCodes.Nop)
                }
            );

            for (int i = 0; i < catchInsns.Count; i++) {
                var insn = catchInsns[i];
                switch (insn.OpCode.Code) {
                    case Code.Leave:
                    case Code.Leave_S: {
                        catchInsns[i] = Instruction.Create(OpCodes.Ldc_I4_0);
                        Patch(catchMethod, insn, catchInsns[i]);
                        InsertOps(catchInsns, i + 1, new[] {
                            Instruction.Create(OpCodes.Ret)
                        });
                        i += 1;
                    }
                        break;
                    case Code.Rethrow: {
                        catchInsns[i] = Instruction.Create(OpCodes.Ldc_I4_1);
                        Patch(catchMethod, insn, catchInsns[i]);
                        InsertOps(catchInsns, i + 1, new[] {
                            Instruction.Create(OpCodes.Ret)
                        });
                        i += 1;
                    }
                        break;
                }
            }

            method.DeclaringType.Methods.Add(catchMethod);

            var handler = new ExcHandler {
                Handler = eh,
                Method = catchMethod
            };
            group.Handlers.Add(handler);
            return handler;
        }

        private ExcHandler ExtractFilterAndCatch (
            MethodDefinition method, ExceptionHandler eh, VariableDefinition closure, ExcGroup group
        ) {
            var insns = method.Body.Instructions;
            var closureType = closure.VariableType;
            var filterType = new TypeDefinition(
                method.DeclaringType.Namespace, method.Name + "__filter" + (FilterIndex++).ToString("X4"),
                TypeAttributes.NestedPublic | TypeAttributes.Class,
                ExceptionFilter
            );
            filterType.BaseType = method.Module.TypeSystem.Object;
            method.DeclaringType.NestedTypes.Add(filterType);
            CreateConstructor(filterType);

            var closureField = new FieldDefinition(
                "closure", FieldAttributes.Public, closureType
            );
            filterType.Fields.Add(closureField);

            var filterMethod = new MethodDefinition(
                "Evaluate",
                MethodAttributes.Virtual | MethodAttributes.Public,
                method.Module.TypeSystem.Int32
            );
            filterMethod.Body.InitLocals = true;

            filterType.Methods.Add(filterMethod);

            var filterReplacement = Instruction.Create(OpCodes.Ret);

            int i1 = insns.IndexOf(eh.FilterStart), i2 = -1;
            Instruction endfilter = null;

            for (int i = i1; i < insns.Count; i++) {
                var insn = insns[i];
                if (insn.OpCode == OpCodes.Endfilter) {
                    endfilter = insn;
                    i2 = i;
                    break;
                }
            }

            var excArg = new ParameterDefinition("exc", default(ParameterAttributes), method.Module.TypeSystem.Object);
            filterMethod.Parameters.Add(excArg);

            var newVariables = ExtractRangeToMethod(method, filterMethod, i1, i2, false);

            var filterInsns = filterMethod.Body.Instructions;
            var oldInsn = eh.HandlerStart;
            var oldStartIdx = insns.IndexOf(eh.HandlerStart);
            insns[oldStartIdx] = Instruction.Create(OpCodes.Nop);
            Patch(method, oldInsn, insns[oldStartIdx]);

            var oldFilterInsn = filterInsns[filterInsns.Count - 1];
            filterInsns[filterInsns.Count - 1] = filterReplacement;
            Patch(filterMethod, oldFilterInsn, filterReplacement);

            InsertOps(
                filterInsns, 0, new[] {
                    // Load the exception from arg1 since exception handlers are entered with it on the stack
                    Instruction.Create(OpCodes.Ldarg, excArg)
                }
            );

            for (int i = 0; i < filterInsns.Count; i++) {
                var insn = filterInsns[i];
                if (insn.Operand != closure)
                    continue;

                filterInsns[i] = Instruction.Create(OpCodes.Ldarg_0);
                Patch(filterMethod, insn, filterInsns[i]);
                filterInsns.Insert(i + 1, Instruction.Create(OpCodes.Ldfld, closureField));
            }

            var handler = ExtractCatch(method, eh, closure, group);

            handler.FilterMethod = filterMethod;
            handler.FilterType = filterType;
            handler.FilterVariable = new VariableDefinition(filterType);
            handler.FirstFilterInsn = eh.FilterStart;
            handler.LastFilterInsn = endfilter;

            return handler;
        }

        private Dictionary<VariableDefinition, VariableDefinition> ExtractRangeToMethod (
            MethodDefinition sourceMethod, MethodDefinition targetMethod, 
            int firstIndex, int lastIndex, bool deleteThem,
            Func<Instruction, Instruction, Instruction> onFailedRemap = null
        ) {
            var insns = sourceMethod.Body.Instructions;
            var filterInsns = targetMethod.Body.Instructions;

            var variables = new Dictionary<VariableDefinition, VariableDefinition>();
            foreach (var loc in sourceMethod.Body.Variables) {
                var newLoc = new VariableDefinition(loc.VariableType);
                targetMethod.Body.Variables.Add(newLoc);
                variables[loc] = newLoc;
            }

            CloneInstructions(
                insns, firstIndex, lastIndex - firstIndex + 1, filterInsns, 0, variables, onFailedRemap
            );

            var oldInsn = insns[firstIndex];
            insns[firstIndex] = Instruction.Create(OpCodes.Nop);
            // FIXME: This should not be necessary
            Patch(sourceMethod, oldInsn, insns[firstIndex]);

            if (deleteThem)
                RemoveRange(sourceMethod, firstIndex, lastIndex);

            return variables;
        }

        private void RemoveRange (
            MethodDefinition method, 
            Instruction first, Instruction last, bool inclusive
        ) {
            var coll = method.Body.Instructions;
            RemoveRange(method, coll.IndexOf(first), coll.IndexOf(last) - (inclusive ? 0 : 1));
        }

        private void RemoveRange (MethodDefinition method, int firstIndex, int lastIndex) {
            var coll = method.Body.Instructions;
            var lastOne = coll[lastIndex];
            var newLast = Instruction.Create(OpCodes.Nop);

            for (int i = lastIndex; i > firstIndex; i--) {
                Patch(method, coll[i], newLast);
                if (i == lastIndex)
                    coll[lastIndex] = newLast;
                else
                    coll.RemoveAt(i);
            }
        }

        public class ExcGroup {
            public Instruction tryStart, tryEnd;
            public List<ExcHandler> Handlers = new List<ExcHandler>();
        }

        public class ExcHandler {
            public ExceptionHandler Handler;
            public TypeDefinition FilterType;
            public VariableDefinition FilterVariable;
            public MethodDefinition Method, FilterMethod;

            public Instruction FirstFilterInsn, LastFilterInsn;
        }

        private void ExtractExceptionFilters (MethodDefinition method) {
            var excType = GetException(method.Module);
            TypeDefinition closureType;
            var closure = ConvertToClosure(method, out closureType);
            var excVar = new VariableDefinition(excType);
            method.Body.Variables.Add(excVar);

            var insns = method.Body.Instructions;
            insns.Insert(0, Instruction.Create(OpCodes.Nop));

            var handlersByTry = method.Body.ExceptionHandlers.ToLookup(eh => (eh.TryStart, eh.TryEnd));

            var newGroups = new List<ExcGroup>();
            var filterIndex = 0;
            var filtersToInsert = new List<(TypeDefinition, ExceptionHandler)>();

            foreach (var group in handlersByTry) {
                var excGroup = new ExcGroup {
                    tryStart = group.Key.Item1,
                    tryEnd = insns[insns.IndexOf(group.Key.Item2) - 1],
                };

                foreach (var eh in group) {
                    if (eh.FilterStart != null)
                        ExtractFilterAndCatch(method, eh, closure, excGroup);
                    else
                        ExtractCatch(method, eh, closure, excGroup);
                }

                newGroups.Add(excGroup);
            }

            foreach (var eg in newGroups) {
                var finallyInsns = new List<Instruction>();

                foreach (var h in eg.Handlers) {
                    var fv = h.FilterVariable;
                    if (fv != null) {
                        method.Body.Variables.Add(fv);
                        var filterInitInsns = new Instruction[] {
                            Instruction.Create(OpCodes.Newobj, h.FilterType.Methods.First(m => m.Name == ".ctor")),
                            Instruction.Create(OpCodes.Stloc, fv),
                            Instruction.Create(OpCodes.Ldloc, fv),
                            Instruction.Create(OpCodes.Call, ExceptionFilter.Methods.First(m => m.Name == "Push")),
                            h.Handler.TryStart,
                        };

                        var oldIndex = insns.IndexOf(h.Handler.TryStart);
                        insns[oldIndex] = Instruction.Create(OpCodes.Nop);
                        Patch(method, h.Handler.TryStart, insns[oldIndex]);
                        InsertOps(insns, oldIndex + 1, filterInitInsns);

                        finallyInsns.Add(Instruction.Create(OpCodes.Ldloc, fv));
                        finallyInsns.Add(Instruction.Create(OpCodes.Call, ExceptionFilter.Methods.First(m => m.Name == "Pop")));
                    }

                    if (h.LastFilterInsn != null)
                        RemoveRange(method, h.FirstFilterInsn, h.LastFilterInsn, true);

                    RemoveRange(method, h.Handler.HandlerStart, h.Handler.HandlerEnd, false);

                    method.Body.ExceptionHandlers.Remove(h.Handler);
                }

                var tryExit = insns[insns.IndexOf(eg.tryEnd) + 1];
                var newHandlerStart = Instruction.Create(OpCodes.Nop);
                var newHandlerEnd = Instruction.Create(OpCodes.Leave, tryExit);
                var newHandlerOffset = insns.IndexOf(eg.tryEnd);
                if (newHandlerOffset < 0)
                    throw new Exception();

                var handlerBody = new List<Instruction> {
                    newHandlerStart
                };

                handlerBody.Add(Instruction.Create(OpCodes.Stloc, excVar));

                var breakOut = Instruction.Create(OpCodes.Nop);

                foreach (var h in eg.Handlers) {
                    var skip = Instruction.Create(OpCodes.Nop);

                    var fv = h.FilterVariable;
                    if (fv != null) {
                        handlerBody.Add(Instruction.Create(OpCodes.Ldloc, fv));
                        handlerBody.Add(Instruction.Create(OpCodes.Call, ExceptionFilter.Methods.First(m => m.Name == "get_Result")));
                        handlerBody.Add(Instruction.Create(OpCodes.Brfalse, skip));
                    }

                    if ((h.Handler.CatchType != null) && (h.Handler.CatchType.FullName != "System.Object")) {
                        handlerBody.Add(Instruction.Create(OpCodes.Ldloc, excVar));
                        handlerBody.Add(Instruction.Create(OpCodes.Isinst, h.Handler.CatchType));
                        handlerBody.Add(Instruction.Create(OpCodes.Brfalse, skip));
                    }

                    handlerBody.Add(Instruction.Create(OpCodes.Ldloc, excVar));
                    handlerBody.Add(Instruction.Create(OpCodes.Ldloc, closure));
                    handlerBody.Add(Instruction.Create(OpCodes.Call, h.Method));
                    handlerBody.Add(Instruction.Create(OpCodes.Brfalse, newHandlerEnd));
                    handlerBody.Add(Instruction.Create(OpCodes.Rethrow));

                    handlerBody.Add(skip);
                }

                handlerBody.Add(newHandlerEnd);

                InsertOps(insns, newHandlerOffset + 1, handlerBody.ToArray());

                Renumber(insns);

                var originalExitPoint = insns[insns.IndexOf(newHandlerEnd) + 1];
                Instruction handlerEnd;
                if (finallyInsns.Count > 0)
                    handlerEnd = finallyInsns[0];
                else
                    handlerEnd = originalExitPoint;

                var newEh = new ExceptionHandler(ExceptionHandlerType.Catch) {
                    TryStart = eg.tryStart,                    
                    TryEnd = newHandlerStart,
                    HandlerStart = newHandlerStart,
                    HandlerEnd = handlerEnd,
                    CatchType = method.Module.TypeSystem.Object,
                };
                method.Body.ExceptionHandlers.Add(newEh);

                if (finallyInsns.Count > 0) {
                    finallyInsns.Add(Instruction.Create(OpCodes.Leave, originalExitPoint));

                    InsertOps(insns, insns.IndexOf(originalExitPoint), finallyInsns.ToArray());

                    var newFinally = new ExceptionHandler(ExceptionHandlerType.Finally) {
                        TryStart = newEh.TryStart,
                        TryEnd = newEh.TryEnd,
                        HandlerStart = finallyInsns[0],
                        HandlerEnd = originalExitPoint
                    };
                    method.Body.ExceptionHandlers.Add(newFinally);
                }
            }

            foreach (var toInsert in filtersToInsert) {
            }

        }

        private void Renumber (Mono.Collections.Generic.Collection<Instruction> insns) {
            foreach (var i in insns)
                i.Offset = insns.IndexOf(i);
        }

        private bool TryRemapInstruction (
            Instruction old,
            Mono.Collections.Generic.Collection<Instruction> oldBody, 
            Mono.Collections.Generic.Collection<Instruction> newBody,
            int offset,
            out Instruction result
        ) {
            result = null;
            if (old == null)
                return false;

            int idx = oldBody.IndexOf(old);
            var newIdx = idx + offset;
            if ((newIdx < 0) || (newIdx >= newBody.Count))
                return false;

            result = newBody[newIdx];
            return true;
        }

        private Instruction RemapInstruction (
            Instruction old,
            Mono.Collections.Generic.Collection<Instruction> oldBody, 
            Mono.Collections.Generic.Collection<Instruction> newBody,
            int offset = 0
        ) {
            Instruction result;
            if (!TryRemapInstruction(old, oldBody, newBody, offset, out result))
                return null;

            return result;
        }

        private Instruction CloneInstruction (Instruction i, Dictionary<VariableDefinition, VariableDefinition> variables) {
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
                MethodReference mref = operand as MethodReference;
                return Instruction.Create(i.OpCode, mref);
            } else if (operand is Instruction) {
                var insn = operand as Instruction;
                return Instruction.Create(i.OpCode, insn);
            } else if (operand is string) {
                var s = operand as string;
                return Instruction.Create(i.OpCode, s);
            } else if (operand is VariableDefinition) {
                var v = operand as VariableDefinition;
                if (variables != null)
                    v = variables[v];
                return Instruction.Create(i.OpCode, v);
            } else {
                throw new NotImplementedException(i.OpCode.ToString());
            }
        }

        private void CloneInstructions (
            Mono.Collections.Generic.Collection<Instruction> source,
            int sourceIndex, int count,
            Mono.Collections.Generic.Collection<Instruction> target,
            int targetIndex,
            Dictionary<VariableDefinition, VariableDefinition> variables = null,
            Func<Instruction, Instruction, Instruction> onFailedRemap = null
        ) {
            if (sourceIndex < 0)
                throw new ArgumentOutOfRangeException("sourceIndex");

            for (int n = 0; n < count; n++) {
                var i = source[n + sourceIndex];
                var newInsn = CloneInstruction(i, variables);
                target.Add(i);
            }

            // Fixup branches
            for (int i = 0; i < target.Count; i++) {
                var insn = target[i];
                var operand = insn.Operand as Instruction;
                if (operand == null)
                    continue;

                Instruction newOperand, newInsn;
                if (!TryRemapInstruction(operand, source, target, targetIndex - sourceIndex, out newOperand)) {
                    if (onFailedRemap != null)
                        newInsn = onFailedRemap(insn, operand);
                    else
                        throw new Exception("Could not remap instruction operand for " + insn);
                } else {
                    newInsn = Instruction.Create(insn.OpCode, newOperand);
                }
                target[i] = newInsn;
            }
        }
    }
}
