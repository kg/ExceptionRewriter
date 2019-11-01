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

        private int ClosureIndex, FilterIndex;

        private readonly Dictionary<Code, OpCode> ShortFormRemappings = new Dictionary<Code, OpCode>();

        public AssemblyRewriter (AssemblyAnalyzer analyzer) {
            Assembly = analyzer.Input;
            Analyzer = analyzer;

            var tOpcodes = typeof(OpCodes);

            foreach (var n in typeof(Code).GetEnumNames()) {
                if (!n.EndsWith("_S"))
                    continue;

                var full = n.Replace("_S", "");
                var m = tOpcodes.GetField(full);
                ShortFormRemappings[Enum.Parse<Code>(n)] = (OpCode)m.GetValue(null);
            }
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

        private TypeReference ImportReferencedType (ModuleDefinition module, string assemblyName, string @namespace, string name) {
            var s = module.TypeSystem.String;

            foreach (var m in Assembly.Modules) {
                foreach (var ar in m.AssemblyReferences) {
                    if (!ar.FullName.Contains(assemblyName))
                        continue;

                    var ad = Assembly.MainModule.AssemblyResolver.Resolve(ar);

                    var result = new TypeReference(
                        @namespace, name, ad.MainModule, ad.MainModule
                    );
                    return module.ImportReference(result);
                    /*
                    var result = new TypeReference(
                        @namespace, name
                    );
                    if (result != null)
                        return module.ImportReference((TypeReference)result);
                        */
                }
            }

            return null;
        }

        private TypeReference GetExceptionFilter (ModuleDefinition module) {
            return ImportReferencedType(module, "ExceptionFilterSupport", "Mono.Runtime.Internal", "ExceptionFilter");
        }

        private TypeReference GetException (ModuleDefinition module) {
            return ImportCorlibType(module, "System", "Exception");
        }

        private TypeReference GetExceptionDispatchInfo (ModuleDefinition module) {
            return ImportCorlibType(module, "System.Runtime.ExceptionServices", "ExceptionDispatchInfo");
        }

        public void Rewrite () {
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

            FilterRange(
                method, 0, insns.Count - 1, (insn) => {
                    var variable = (insn.Operand as VariableDefinition) 
                    ?? LookupNumberedVariable(insn.OpCode.Code, method.Body.Variables);
                    var arg = (insn.Operand as ParameterDefinition)
                        ?? LookupNumberedArgument(insn.OpCode.Code, isStatic ? null : fakeThis, method.Parameters);

                    // FIXME
                    if (variable == closureVar)
                        return null;

                    if ((variable == null) && (arg == null))
                        return null;

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

                        return new[] {
                            insn, 
                            Instruction.Create(OpCodes.Ldloc, closureVar),
                            reload,
                            Instruction.Create(OpCodes.Stfld, matchingField)
                        };
                    } else {
                        var newInsn = Instruction.Create(OpCodes.Ldloc, closureVar);
                        var loadOp =
                            ((insn.OpCode.Code == Code.Ldloca) ||
                            (insn.OpCode.Code == Code.Ldloca_S))
                                ? OpCodes.Ldflda
                                : OpCodes.Ldfld;
                        return new[] {
                            newInsn, 
                            Instruction.Create(loadOp, matchingField)
                        };
                    }
                }
            );

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

            CleanMethodBody(method);

            return closureVar;
        }

        private int CatchCount;

        private Instruction PostFilterRange (
            Dictionary<Instruction, Instruction> remapTable, Instruction oldValue
        ) {
            if (oldValue == null)
                return null;

            Instruction result;
            if (remapTable.TryGetValue(oldValue, out result))
                return result;

            return oldValue;
        }

        private void FilterRange (
            MethodDefinition method,
            int firstIndex, int lastIndex, Func<Instruction, Instruction[]> filter
        ) {
            var remapTableFirst = new Dictionary<Instruction, Instruction>();
            var remapTableLast = new Dictionary<Instruction, Instruction>();
            var instructions = method.Body.Instructions;

            var firstRemovedInstruction = instructions[firstIndex];

            for (int i = firstIndex; i <= lastIndex; i++) {
                var insn = instructions[i];
                var result = filter(insn);
                if (result == null)
                    continue;
                if (result.Length == 1 && result[0] == insn)
                    continue;

                if (insn != result[0]) {
                    remapTableFirst[insn] = result[0];
                    instructions[i] = result[0];
                }
                for (int j = result.Length - 1; j >= 1; j--)
                    instructions.Insert(i + 1, result[j]);

                remapTableLast[insn] = result[result.Length - 1];

                lastIndex += (result.Length - 1);
                i += (result.Length - 1);
            }

            for (int i = 0; i < instructions.Count; i++) {
                var insn = instructions[i];
                var operand = insn.Operand as Instruction;
                if (operand == null)
                    continue;
                Instruction newOperand;
                if (!remapTableFirst.TryGetValue(operand, out newOperand))
                    continue;

                insn.Operand = newOperand;
            }

            CleanMethodBody(method);

            foreach (var eh in method.Body.ExceptionHandlers) {
                eh.FilterStart = PostFilterRange(remapTableFirst, eh.FilterStart);
                eh.TryStart = PostFilterRange(remapTableFirst, eh.TryStart);
                eh.TryEnd = PostFilterRange(remapTableFirst, eh.TryEnd);
                eh.HandlerStart = PostFilterRange(remapTableFirst, eh.HandlerStart);
                eh.HandlerEnd = PostFilterRange(remapTableFirst, eh.HandlerEnd);
            }
        }

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
            var excParam = new ParameterDefinition("__exc", ParameterAttributes.None, eh.CatchType ?? method.Module.TypeSystem.Object);
            // Exc goes first because catch blocks start with it on the stack and this makes it convenient to spam dup
            catchMethod.Parameters.Add(excParam);
            catchMethod.Parameters.Add(closureParam);

            var catchInsns = catchMethod.Body.Instructions;

            var handlerFirstIndex = insns.IndexOf(eh.HandlerStart);
            var handlerLastIndex = insns.IndexOf(eh.HandlerEnd) - 1;
            FilterRange(
                method, handlerFirstIndex, handlerLastIndex,
                (insn) => {
                    switch (insn.OpCode.Code) {
                        case Code.Leave:
                        case Code.Leave_S:
                            return new[] {
                                Instruction.Create(OpCodes.Ldc_I4_0),
                                Instruction.Create(OpCodes.Ret)
                            };
                        case Code.Rethrow:
                            return new[] {
                                Instruction.Create(OpCodes.Ldc_I4_1),
                                Instruction.Create(OpCodes.Ret)
                            };
                        default:
                            return null;
                    }
                }
            );

            var newVariables = ExtractRangeToMethod(
                method, catchMethod, 
                insns.IndexOf(eh.HandlerStart), 
                insns.IndexOf(eh.HandlerEnd) - 1,
                true, 
                (insn, operand) => {
                    throw new Exception();
                }
            );

            var first = catchInsns[0];

            InsertOps(
                catchInsns, 0, new[] {
                    Instruction.Create(OpCodes.Ldarg, closureParam),
                    Instruction.Create(OpCodes.Stloc, newVariables[closure]),
                    Instruction.Create(OpCodes.Ldarg, excParam)
                }
            );

            for (int i = 0; i < catchInsns.Count; i++) {
                var insn = catchInsns[i];
            }

            method.DeclaringType.Methods.Add(catchMethod);

            var isCatchAll = (eh.HandlerType == ExceptionHandlerType.Catch) && (eh.CatchType?.FullName == "System.Object");
            var handler = new ExcHandler {
                Handler = eh,
                Method = catchMethod,
                IsCatchAll = isCatchAll
            };
            group.Handlers.Add(handler);
            return handler;
        }

        private ExcHandler ExtractFilterAndCatch (
            MethodDefinition method, ExceptionHandler eh, VariableDefinition closure, ExcGroup group
        ) {
            var insns = method.Body.Instructions;
            var closureType = closure.VariableType;
            var filterIndex = FilterIndex++;
            var filterType = new TypeDefinition(
                method.DeclaringType.Namespace, method.Name + "__filter" + filterIndex.ToString("X4"),
                TypeAttributes.NestedPublic | TypeAttributes.Class,
                GetExceptionFilter(method.Module)
            );
            filterType.BaseType = GetExceptionFilter(method.Module);
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

            var excArg = new ParameterDefinition("exc", default(ParameterAttributes), method.Module.TypeSystem.Object);
            filterMethod.Parameters.Add(excArg);

            int i1 = insns.IndexOf(eh.FilterStart), i2 = insns.IndexOf(eh.HandlerStart);
            if (i2 < 0)
                throw new Exception();
            i2--;

            var newVariables = ExtractRangeToMethod(method, filterMethod, i1, i2, true);

            var filterInsns = filterMethod.Body.Instructions;

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
            handler.FilterField = new FieldDefinition(
                "__filter" + filterIndex.ToString("X4"), 
                FieldAttributes.Public, filterType
            );
            ((TypeDefinition)closureType).Fields.Add(handler.FilterField);
            handler.FirstFilterInsn = eh.FilterStart;

            return handler;
        }

        private Dictionary<VariableDefinition, VariableDefinition> ExtractRangeToMethod (
            MethodDefinition sourceMethod, MethodDefinition targetMethod, 
            int firstIndex, int lastIndex, bool deleteThem,
            Func<Instruction, Instruction, Instruction> onFailedRemap = null
        ) {
            var insns = sourceMethod.Body.Instructions;
            var targetInsns = targetMethod.Body.Instructions;

            var variables = new Dictionary<VariableDefinition, VariableDefinition>();
            foreach (var loc in sourceMethod.Body.Variables) {
                var newLoc = new VariableDefinition(loc.VariableType);
                targetMethod.Body.Variables.Add(newLoc);
                variables[loc] = newLoc;
            }

            CloneInstructions(
                insns, firstIndex, lastIndex - firstIndex + 1, targetInsns, 0, variables, onFailedRemap
            );

            CleanMethodBody(targetMethod);

            var oldInsn = insns[firstIndex];
            var newInsn = Instruction.Create(OpCodes.Nop);
            insns[firstIndex] = newInsn;
            // FIXME: This should not be necessary
            Patch(sourceMethod, oldInsn, newInsn);

            if (deleteThem) {
                for (int i = lastIndex; i > firstIndex; i--)
                    insns.RemoveAt(i);
                CleanMethodBody(sourceMethod);
            }

            return variables;
        }

        private void RemoveRange (
            MethodDefinition method, 
            Instruction first, Instruction last, bool inclusive
        ) {
            var coll = method.Body.Instructions;
            var firstIndex = coll.IndexOf(first);
            var lastIndex = coll.IndexOf(last);
            if (firstIndex < 0)
                throw new Exception($"Instruction {first} not found in method");
            if (lastIndex < 0)
                throw new Exception($"Instruction {last} not found in method");
            RemoveRange(method, firstIndex, lastIndex - (inclusive ? 0 : 1));
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
            internal Instruction FirstPushInstruction;
        }

        public class ExcHandler {
            public bool IsCatchAll;

            public ExceptionHandler Handler;
            public TypeDefinition FilterType;
            public FieldDefinition FilterField;
            public MethodDefinition Method, FilterMethod;

            public Instruction FirstFilterInsn;
            internal Instruction FirstPushInstruction;
        }

        private void ExtractExceptionFilters (MethodDefinition method) {
            CleanMethodBody(method);

            var efilt = GetExceptionFilter(method.Module);
            var excType = GetException(method.Module);
            TypeDefinition closureType;
            var closure = ConvertToClosure(method, out closureType);

            var excVar = new VariableDefinition(method.Module.TypeSystem.Object);
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

                var hasAnyCatchAll = eg.Handlers.Any(h => h.IsCatchAll);

                foreach (var h in eg.Handlers) {
                    var ff = h.FilterField;
                    if (ff != null) {
                        var filterInitInsns = new Instruction[] {
                            // Construct a filter instance and store it into the closure
                            Instruction.Create(OpCodes.Ldloc, closure),
                            Instruction.Create(OpCodes.Newobj, h.FilterType.Methods.First(m => m.Name == ".ctor")),
                            Instruction.Create(OpCodes.Stfld, h.FilterField),
                            // Then store the closure into the filter instance so it can access locals
                            Instruction.Create(OpCodes.Ldloc, closure),
                            Instruction.Create(OpCodes.Ldfld, h.FilterField),
                            Instruction.Create(OpCodes.Ldloc, closure),
                            Instruction.Create(OpCodes.Stfld, h.FilterType.Fields.First(m => m.Name == "closure")),
                            // Then call Push on the filter instance
                            Instruction.Create(OpCodes.Ldloc, closure),
                            Instruction.Create(OpCodes.Ldfld, h.FilterField),
                            Instruction.Create(OpCodes.Castclass, efilt),
                            Instruction.Create(OpCodes.Call, new MethodReference(
                                    "Push", method.Module.TypeSystem.Void, efilt
                            ) { HasThis = false, Parameters = {
                                    new ParameterDefinition(efilt)
                            } }),
                            h.Handler.TryStart,
                        };

                        var oldIndex = insns.IndexOf(h.Handler.TryStart);
                        var nop = Instruction.Create(OpCodes.Nop);
                        insns[oldIndex] = nop;
                        Patch(method, h.Handler.TryStart, insns[oldIndex]);
                        InsertOps(insns, oldIndex + 1, filterInitInsns);

                        int lowestIndex = int.MaxValue;
                        if (eg.FirstPushInstruction != null)
                            lowestIndex = insns.IndexOf(eg.FirstPushInstruction);
                        int newIndex = insns.IndexOf(nop);
                        eg.FirstPushInstruction = (newIndex < lowestIndex)
                            ? nop
                            : eg.FirstPushInstruction;
                        eg.FirstPushInstruction = insns[oldIndex];

                        // At the end of the scope remove all our filters.
                        // FIXME: Should we do this earlier?
                        finallyInsns.Add(Instruction.Create(OpCodes.Ldloc, closure));
                        finallyInsns.Add(Instruction.Create(OpCodes.Ldfld, ff));
                        finallyInsns.Add(Instruction.Create(OpCodes.Castclass, efilt));
                        finallyInsns.Add(Instruction.Create(OpCodes.Call, new MethodReference(
                                "Pop", method.Module.TypeSystem.Void, efilt
                        ) { HasThis = false, Parameters = {
                                new ParameterDefinition(efilt)
                        }}));
                    }

                    method.Body.ExceptionHandlers.Remove(h.Handler);
                }

                var tryExit = insns[insns.IndexOf(eg.tryEnd) + 1];
                var newHandlerStart = Instruction.Create(OpCodes.Nop);
                Instruction newHandlerEnd, handlerFallthroughRethrow;
                handlerFallthroughRethrow = hasAnyCatchAll ? Instruction.Create(OpCodes.Nop) : Instruction.Create(OpCodes.Rethrow);
                newHandlerEnd = Instruction.Create(OpCodes.Leave, tryExit);

                var newHandlerOffset = insns.IndexOf(eg.tryEnd);
                if (newHandlerOffset < 0)
                    throw new Exception();

                var handlerBody = new List<Instruction> {
                    newHandlerStart,
                    Instruction.Create(OpCodes.Stloc, excVar)
                };

                var breakOut = Instruction.Create(OpCodes.Nop);

                foreach (var h in eg.Handlers) {
                    var skip = Instruction.Create(OpCodes.Nop);

                    var ff = h.FilterField;
                    if (ff != null) {
                        // If we have a filter, check the Result to see if the filter returned execute_handler
                        handlerBody.Add(Instruction.Create(OpCodes.Ldloc, closure));
                        handlerBody.Add(Instruction.Create(OpCodes.Ldfld, ff));
                        handlerBody.Add(Instruction.Create(OpCodes.Castclass, efilt));
                        handlerBody.Add(Instruction.Create(OpCodes.Ldloc, excVar));
                        var mref = new MethodReference(
                            "ShouldRunHandler", method.Module.TypeSystem.Boolean, efilt
                        ) { HasThis = true, Parameters = {
                            new ParameterDefinition(method.Module.TypeSystem.Object)
                        } };
                        handlerBody.Add(Instruction.Create(OpCodes.Call, method.Module.ImportReference(mref)));
                        handlerBody.Add(Instruction.Create(OpCodes.Brfalse, skip));
                    }

                    if ((h.Handler.CatchType != null) && (h.Handler.CatchType.FullName != "System.Object")) {
                        // If the handler has a type check do an isinst to check whether it should run
                        handlerBody.Add(Instruction.Create(OpCodes.Ldloc, excVar));
                        handlerBody.Add(Instruction.Create(OpCodes.Isinst, h.Handler.CatchType));
                        handlerBody.Add(Instruction.Create(OpCodes.Brfalse, skip));
                        // If the isinst passed we need to cast the exception value to the appropriate type
                        handlerBody.Add(Instruction.Create(OpCodes.Ldloc, excVar));
                        handlerBody.Add(Instruction.Create(OpCodes.Castclass, h.Handler.CatchType));
                    } else {
                        handlerBody.Add(Instruction.Create(OpCodes.Ldloc, excVar));
                    }

                    // Run the handler, then if it returns true, throw.
                    // If it returned false, we leave the entire handler.
                    handlerBody.Add(Instruction.Create(OpCodes.Ldloc, closure));
                    handlerBody.Add(Instruction.Create(OpCodes.Call, h.Method));
                    handlerBody.Add(Instruction.Create(OpCodes.Brfalse, newHandlerEnd));
                    handlerBody.Add(Instruction.Create(OpCodes.Rethrow));
                    handlerBody.Add(skip);
                }

                handlerBody.Add(handlerFallthroughRethrow);
                handlerBody.Add(newHandlerEnd);

                InsertOps(insns, newHandlerOffset + 1, handlerBody.ToArray());

                var originalExitPoint = insns[insns.IndexOf(newHandlerEnd) + 1];
                Instruction handlerEnd;

                Instruction preFinallyBr;
                // If there was a catch-all block we can jump to the original exit point, because
                //  the catch-all block handler would have returned 1 to trigger a rethrow - it didn't.
                // If no catch-all block existed we need to rethrow at the end of our coalesced handler.
                if (hasAnyCatchAll)
                    preFinallyBr = Instruction.Create(OpCodes.Leave, originalExitPoint);
                else
                    preFinallyBr = Instruction.Create(OpCodes.Rethrow);

                if (finallyInsns.Count > 0)
                    handlerEnd = preFinallyBr;
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
                    finallyInsns.Insert(0, Instruction.Create(OpCodes.Call, new MethodReference(
                        "Reset", method.Module.TypeSystem.Void, efilt
                    ) { HasThis = false }));
                    finallyInsns.Add(Instruction.Create(OpCodes.Endfinally));

                    var newLeave = Instruction.Create(OpCodes.Leave, originalExitPoint);
                    if (!hasAnyCatchAll)
                        newLeave = Instruction.Create(OpCodes.Rethrow);
                    var originalExitIndex = insns.IndexOf(originalExitPoint);
                    InsertOps(insns, originalExitIndex, finallyInsns.ToArray());

                    var newFinally = new ExceptionHandler(ExceptionHandlerType.Finally) {
                        TryStart = eg.FirstPushInstruction,
                        TryEnd = finallyInsns[0],
                        HandlerStart = finallyInsns[0],
                        HandlerEnd = originalExitPoint
                    };
                    method.Body.ExceptionHandlers.Add(newFinally);

                    insns.Insert(insns.IndexOf(finallyInsns[0]), preFinallyBr);

                    var handlerEndIndex = insns.IndexOf(handlerEnd);
                    if (handlerEndIndex < 0)
                        throw new Exception();

                    insns.Insert(handlerEndIndex + 1, newLeave);
                }

                CleanMethodBody(method);
            }
        }

        private void CleanMethodBody (MethodDefinition method) {
            var insns = method.Body.Instructions;
            foreach (var i in insns)
                i.Offset = insns.IndexOf(i);

            foreach (var i in insns) {
                OpCode newOpcode;
                if (ShortFormRemappings.TryGetValue(i.OpCode.Code, out newOpcode))
                    i.OpCode = newOpcode;

                if (i.Operand is Instruction) {
                    if (insns.IndexOf((Instruction)i.Operand) < 0)
                        throw new Exception($"Branch target {i.Operand} of opcode {i} is missing");
                }
            }

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
