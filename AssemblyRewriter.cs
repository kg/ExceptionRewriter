using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Mono.Cecil;
using Mono.Cecil.Cil;
using Mono.Collections.Generic;

namespace ExceptionRewriter {
    public class AssemblyRewriter {
        public readonly AssemblyDefinition Assembly;
        public readonly AssemblyAnalyzer Analyzer;

        private int ClosureIndex, FilterIndex;

        private readonly Dictionary<Code, OpCode> ShortFormRemappings = new Dictionary<Code, OpCode>();
        private readonly Dictionary<Code, OpCode> Denumberings = new Dictionary<Code, OpCode> {
            {Code.Ldarg_0, OpCodes.Ldarg },
            {Code.Ldarg_1, OpCodes.Ldarg },
            {Code.Ldarg_2, OpCodes.Ldarg },
            {Code.Ldarg_3, OpCodes.Ldarg },
            {Code.Ldloc_0, OpCodes.Ldloc },
            {Code.Ldloc_1, OpCodes.Ldloc },
            {Code.Ldloc_2, OpCodes.Ldloc },
            {Code.Ldloc_3, OpCodes.Ldloc },
            {Code.Stloc_0, OpCodes.Stloc },
            {Code.Stloc_1, OpCodes.Stloc },
            {Code.Stloc_2, OpCodes.Stloc },
            {Code.Stloc_3, OpCodes.Stloc },
            {Code.Ldarg_S, OpCodes.Ldarg },
            {Code.Ldarga_S, OpCodes.Ldarga },
            {Code.Starg_S, OpCodes.Starg },
            {Code.Ldloc_S, OpCodes.Ldloc },
            {Code.Ldloca_S, OpCodes.Ldloca }
        };
        private readonly Dictionary<Code, OpCode> LocalParameterRemappings = new Dictionary<Code, OpCode> {
            {Code.Ldloc, OpCodes.Ldarg },
            {Code.Ldloca, OpCodes.Ldarga },
            {Code.Ldloc_S, OpCodes.Ldarg },
            {Code.Ldloca_S, OpCodes.Ldarga },
            {Code.Stloc, OpCodes.Starg },
            {Code.Stloc_S, OpCodes.Starg },
            {Code.Ldarg, OpCodes.Ldloc },
            {Code.Ldarga, OpCodes.Ldloca },
            {Code.Ldarg_S, OpCodes.Ldloc },
            {Code.Ldarga_S, OpCodes.Ldloca },
            {Code.Starg, OpCodes.Stloc },
            {Code.Starg_S, OpCodes.Stloc }
        };

        public AssemblyRewriter (AssemblyAnalyzer analyzer) {
            Assembly = analyzer.Input;
            Analyzer = analyzer;

            var tOpcodes = typeof(OpCodes);

            foreach (var n in typeof(Code).GetEnumNames()) {
                if (!n.EndsWith("_S"))
                    continue;
                if (n.StartsWith("Ld") || n.StartsWith("St"))
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

        private TypeReference GetExceptionFilter (ModuleDefinition module, bool autoAddReference = true) {
            var result = ImportReferencedType(module, "ExceptionFilterSupport", "Mono.Runtime.Internal", "ExceptionFilter");
            if (result == null) {
                if (!autoAddReference)
                    throw new Exception("ExceptionFilterSupport is not referenced");

                var anr = new AssemblyNameReference("ExceptionFilterSupport", new Version(1, 0, 0, 0));
                module.AssemblyReferences.Add(anr);
                return GetExceptionFilter(module, false);
            }
            return result;
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
                            Instruction.Create(OpCodes.Ldloca, tempLocal),
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

        private VariableDefinition ConvertToClosure (
            MethodDefinition method, ParameterDefinition fakeThis, HashSet<VariableReference> variables, 
            HashSet<ParameterReference> parameters, out TypeDefinition closureType
        ) {
            var insns = method.Body.Instructions;
            closureType = new TypeDefinition(
                method.DeclaringType.Namespace, method.Name + "__closure" + (ClosureIndex++).ToString(),
                TypeAttributes.Class | TypeAttributes.NestedPrivate
            );
            closureType.BaseType = method.Module.TypeSystem.Object;
            method.DeclaringType.NestedTypes.Add(closureType);

            var ctorMethod = CreateConstructor(closureType);

            var isStatic = method.IsStatic;

            var localCount = 0;
            var closureVar = new VariableDefinition(closureType);

            var extractedVariables = variables.ToDictionary(
                v => (object)v, 
                v => new FieldDefinition("local_" + localCount++, FieldAttributes.Public, v.VariableType)
            );

            method.Body.Variables.Add(closureVar);

            for (int i = 0; i < method.Parameters.Count; i++) {
                var p = method.Parameters[i];
                if (!parameters.Contains(p))
                    continue;

                var name = (p.Name != null) ? "arg_" + p.Name : "arg" + i;
                extractedVariables[p] = new FieldDefinition(name, FieldAttributes.Public, p.ParameterType);
            }

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

                    FieldDefinition matchingField;
                    var lookupKey = (object)variable ?? arg;
                    if (!extractedVariables.TryGetValue(lookupKey, out matchingField))
                        return null;

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

            CleanMethodBody(method, null, false);

            var toInject = new List<Instruction>() {
                Instruction.Create(OpCodes.Newobj, closureType.Methods.First(m => m.Name == ".ctor")),
                Instruction.Create(OpCodes.Stloc, closureVar)
            };

            if (!isStatic) {
                toInject.AddRange(new[] {
                    Instruction.Create(OpCodes.Ldloc, closureVar),
                    Instruction.Create(OpCodes.Ldarg, fakeThis),
                    Instruction.Create(OpCodes.Stfld, extractedVariables[fakeThis])
                });
            }

            foreach (var p in method.Parameters) {
                if (!parameters.Contains(p))
                    continue;

                toInject.AddRange(new[] {
                    Instruction.Create(OpCodes.Ldloc, closureVar),
                    Instruction.Create(OpCodes.Ldarg, p),
                    Instruction.Create(OpCodes.Stfld, extractedVariables[p])
                });
            }

            InsertOps(insns, 0, toInject.ToArray());

            CleanMethodBody(method, null, true);

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

            CleanMethodBody(method, null, false);

            foreach (var eh in method.Body.ExceptionHandlers) {
                eh.FilterStart = PostFilterRange(remapTableFirst, eh.FilterStart);
                eh.TryStart = PostFilterRange(remapTableFirst, eh.TryStart);
                eh.TryEnd = PostFilterRange(remapTableFirst, eh.TryEnd);
                eh.HandlerStart = PostFilterRange(remapTableFirst, eh.HandlerStart);
                eh.HandlerEnd = PostFilterRange(remapTableFirst, eh.HandlerEnd);
            }
        }

        private void GenerateParameters (
            MethodDefinition newMethod, HashSet<VariableReference> variables, 
            Dictionary<object, object> mapping, HashSet<object> needsLdind
        ) {
            int i = 0;
            foreach (var vr in variables) {
                var newParamType =
                    vr.VariableType.IsByReference
                        ? vr.VariableType
                        : new ByReferenceType(vr.VariableType);
                var newParam = new ParameterDefinition("loc_" + i++.ToString(), ParameterAttributes.None, newParamType);
                newMethod.Parameters.Add(newParam);
                mapping[vr] = newParam;
                if (newParamType != vr.VariableType)
                    needsLdind.Add(newParam);
            }
        }

        private void GenerateParameters (
            MethodDefinition newMethod, HashSet<ParameterReference> parameters, 
            Dictionary<object, object> mapping, HashSet<object> needsLdind
        ) {
            foreach (var pr in parameters) {
                var newParamType =
                    pr.ParameterType.IsByReference
                        ? pr.ParameterType
                        : new ByReferenceType(pr.ParameterType);
                var newParam = new ParameterDefinition("arg_" + pr.Name, ParameterAttributes.None, newParamType);
                newMethod.Parameters.Add(newParam);
                mapping[pr] = newParam;
                if (newParamType != pr.ParameterType)
                    needsLdind.Add(newParam);
            }
        }

        private ExcHandler ExtractCatch (
            MethodDefinition method, ExceptionHandler eh, VariableDefinition closure, ParameterDefinition fakeThis, ExcGroup group
        ) {
            var insns = method.Body.Instructions;
            var closureType = closure.VariableType;

            var catchReferencedVariables = new HashSet<VariableReference>();
            var catchReferencedArguments = new HashSet<ParameterReference>();
            CollectReferencedLocals(method, fakeThis, eh.HandlerStart, eh.HandlerEnd, catchReferencedVariables, catchReferencedArguments);

            var catchMethod = new MethodDefinition(
                method.Name + "__catch" + (CatchCount++),
                MethodAttributes.Static | MethodAttributes.Private,
                method.Module.TypeSystem.Int32
            );
            catchMethod.Body.InitLocals = true;
            var closureParam = new ParameterDefinition("__closure", ParameterAttributes.None, closureType);
            var excParam = new ParameterDefinition("__exc", ParameterAttributes.None, eh.CatchType ?? method.Module.TypeSystem.Object);
            var paramMapping = new Dictionary<object, object> {
                {closure, closureParam }
            };
            var closureVariable = new VariableDefinition(closureType);
            var needsLdind = new HashSet<object>();
            GenerateParameters(catchMethod, catchReferencedArguments, paramMapping, needsLdind);
            GenerateParameters(catchMethod, catchReferencedVariables, paramMapping, needsLdind);
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
                        case Code.Ldarg:
                        default:
                            return null;
                    }
                }
            );

            CleanMethodBody(catchMethod, method, false);

            var newMapping = ExtractRangeToMethod(
                method, catchMethod, fakeThis,
                insns.IndexOf(eh.HandlerStart), 
                insns.IndexOf(eh.HandlerEnd) - 1,
                deleteThem: true,
                mapping: paramMapping
            );

            var first = catchInsns[0];

            InsertOps(
                catchInsns, 0, new[] {
                    Instruction.Create(OpCodes.Ldarg, excParam)
                }
            );

            FilterRange(catchMethod, 0, catchMethod.Body.Instructions.Count - 1, (i) => {
                if (needsLdind.Contains(i.Operand)) {
                    if (IsStoreOperation(i.OpCode.Code)) {
                        var operandVariable = i.Operand as VariableReference;
                        var operandParameter = i.Operand as ParameterReference;
                        var operandType = operandVariable?.VariableType ?? operandParameter.ParameterType;

                        // ugh
                        var newTempLocal = new VariableDefinition(operandType.IsByReference ? operandType.GetElementType() : operandType);
                        catchMethod.Body.Variables.Add(newTempLocal);

                        return new[] {
                            Instruction.Create(OpCodes.Stloc, newTempLocal),
                            i.Operand is VariableReference
                                ? Instruction.Create(OpCodes.Ldloc, (VariableDefinition)i.Operand)
                                : Instruction.Create(OpCodes.Ldarg, (ParameterDefinition)i.Operand),
                            Instruction.Create(OpCodes.Ldloc, newTempLocal),
                            Instruction.Create(SelectStindForOperand(i.Operand))
                        };
                    } else
                        return new[] {
                            i,
                            Instruction.Create(SelectLdindForOperand(i.Operand))
                        };
                } else
                    return null;
            });

            CleanMethodBody(catchMethod, method, true);

            method.DeclaringType.Methods.Add(catchMethod);

            var isCatchAll = (eh.HandlerType == ExceptionHandlerType.Catch) && (eh.CatchType?.FullName == "System.Object");
            var handler = new ExcHandler {
                Handler = eh,
                Method = catchMethod,
                IsCatchAll = isCatchAll,
                Mapping = newMapping,
                CatchReferencedVariables = catchReferencedVariables,
                CatchReferencedArguments = catchReferencedArguments
            };
            group.Handlers.Add(handler);
            return handler;
        }

        private OpCode SelectStindForOperand (object operand) {
            var vr = operand as VariableReference;
            var pr = operand as ParameterReference;
            var operandType = (vr != null) ? vr.VariableType : pr.ParameterType;

            switch (operandType.FullName) {
                case "System.Byte":
                    // FIXME
                    return OpCodes.Stind_I1;
                case "System.UInt16":
                    // FIXME
                    return OpCodes.Stind_I2;
                case "System.UInt32":
                    // FIXME
                    return OpCodes.Stind_I4;
                case "System.UInt64":
                    // FIXME
                    return OpCodes.Stind_I8;
                case "System.SByte":
                    return OpCodes.Stind_I1;
                case "System.Int16":
                    return OpCodes.Stind_I2;
                case "System.Int32":
                    return OpCodes.Stind_I4;
                case "System.Int64":
                    return OpCodes.Stind_I8;
                case "System.Single":
                    return OpCodes.Stind_R4;
                case "System.Double":
                    return OpCodes.Stind_R8;
                default:
                    return OpCodes.Stind_Ref;
            }
        }

        private OpCode SelectLdindForOperand (object operand) {
            var vr = operand as VariableReference;
            var pr = operand as ParameterReference;
            var operandType = (vr != null) ? vr.VariableType : pr.ParameterType;

            switch (operandType.FullName) {
                case "System.Byte":
                    return OpCodes.Ldind_U1;
                case "System.UInt16":
                    return OpCodes.Ldind_U2;
                case "System.UInt32":
                    return OpCodes.Ldind_U4;
                case "System.UInt64":
                    // FIXME
                    return OpCodes.Ldind_I8;
                case "System.SByte":
                    return OpCodes.Ldind_I1;
                case "System.Int16":
                    return OpCodes.Ldind_I2;
                case "System.Int32":
                    return OpCodes.Ldind_I4;
                case "System.Int64":
                    return OpCodes.Ldind_I8;
                case "System.Single":
                    return OpCodes.Ldind_R4;
                case "System.Double":
                    return OpCodes.Ldind_R8;
                default:
                    return OpCodes.Ldind_Ref;
            }
        }

        private ExcHandler ExtractFilterAndCatch (
            MethodDefinition method, ExceptionHandler eh, VariableDefinition closure, ParameterDefinition fakeThis, ExcGroup group
        ) {
            var insns = method.Body.Instructions;
            var closureType = closure.VariableType;
            var filterIndex = FilterIndex++;
            var filterType = new TypeDefinition(
                method.DeclaringType.Namespace, method.Name + "__filter" + filterIndex.ToString(),
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

            var mapping = new Dictionary<object, object> {
                // FIXME
                // {closure, closureField }
            };
            var newVariables = ExtractRangeToMethod(method, filterMethod, fakeThis, i1, i2, true, mapping);

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

                filterInsns[i] = Instruction.Create(OpCodes.Ldarg, fakeThis);
                Patch(filterMethod, insn, filterInsns[i]);
                filterInsns.Insert(i + 1, Instruction.Create(OpCodes.Ldfld, closureField));
            }

            CleanMethodBody(filterMethod, method, true);

            var handler = ExtractCatch(method, eh, closure, fakeThis, group);

            handler.FilterMethod = filterMethod;
            handler.FilterType = filterType;
            handler.FilterVariable = new VariableDefinition(filterType);
            method.Body.Variables.Add(handler.FilterVariable);
            handler.FirstFilterInsn = eh.FilterStart;

            return handler;
        }

        private Dictionary<object, object> ExtractRangeToMethod (
            MethodDefinition sourceMethod, MethodDefinition targetMethod, 
            ParameterDefinition fakeThis,
            int firstIndex, int lastIndex, bool deleteThem,
            Dictionary<object, object> mapping,
            Func<Instruction, Instruction, Instruction> onFailedRemap = null
        ) {
            var insns = sourceMethod.Body.Instructions;
            var targetInsns = targetMethod.Body.Instructions;

            foreach (var loc in sourceMethod.Body.Variables) {
                if (mapping.ContainsKey(loc))
                    continue;
                var newLoc = new VariableDefinition(loc.VariableType);
                targetMethod.Body.Variables.Add(newLoc);
                mapping[loc] = newLoc;
            }

            CloneInstructions(
                sourceMethod, fakeThis, firstIndex, lastIndex - firstIndex + 1, targetInsns, 0, mapping, onFailedRemap
            );

            CleanMethodBody(targetMethod, sourceMethod, false);

            var oldInsn = insns[firstIndex];
            var newInsn = Instruction.Create(OpCodes.Nop);
            insns[firstIndex] = newInsn;
            // FIXME: This should not be necessary
            Patch(sourceMethod, oldInsn, newInsn);

            if (deleteThem) {
                for (int i = lastIndex; i > firstIndex; i--)
                    insns.RemoveAt(i);
                CleanMethodBody(sourceMethod, null, false);
            }

            return mapping;
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
            internal VariableDefinition FilterVariable;
            public MethodDefinition Method, FilterMethod;

            public Instruction FirstFilterInsn;
            internal HashSet<VariableReference> CatchReferencedVariables;
            internal HashSet<ParameterReference> CatchReferencedArguments;
            internal Dictionary<object, object> Mapping;
        }

        private void ExtractExceptionFilters (MethodDefinition method) {
            CleanMethodBody(method, null, false);

            var efilt = GetExceptionFilter(method.Module);
            var excType = GetException(method.Module);
            TypeDefinition closureType;

            var fakeThis = new ParameterDefinition("__this", ParameterAttributes.None, method.DeclaringType);

            var filterReferencedVariables = new HashSet<VariableReference>();
            var filterReferencedArguments = new HashSet<ParameterReference>();
            CollectReferencedLocals(method, method.IsStatic ? null : fakeThis, filterReferencedVariables, filterReferencedArguments);

            var closure = ConvertToClosure(method, fakeThis, filterReferencedVariables, filterReferencedArguments, out closureType);

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
                        ExtractFilterAndCatch(method, eh, closure, fakeThis, excGroup);
                    else
                        ExtractCatch(method, eh, closure, fakeThis, excGroup);
                }

                newGroups.Add(excGroup);
            }

            foreach (var eg in newGroups) {
                var finallyInsns = new List<Instruction>();

                var hasAnyCatchAll = eg.Handlers.Any(h => h.IsCatchAll);

                foreach (var h in eg.Handlers) {
                    var fv = h.FilterVariable;
                    if (fv != null) {
                        InsertOps(insns, 0, new Instruction[] {
                            Instruction.Create(OpCodes.Newobj, h.FilterType.Methods.First(m => m.Name == ".ctor")),
                            Instruction.Create(OpCodes.Stloc, fv),
                        });

                        var filterInitInsns = new Instruction[] {
                            // Store the closure into the filter instance so it can access locals
                            Instruction.Create(OpCodes.Ldloc, fv),
                            Instruction.Create(OpCodes.Ldloc, closure),
                            Instruction.Create(OpCodes.Stfld, h.FilterType.Fields.First(m => m.Name == "closure")),
                            // Then call Push on the filter instance
                            Instruction.Create(OpCodes.Ldloc, fv),
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
                        finallyInsns.Add(Instruction.Create(OpCodes.Ldloc, fv));
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

                    var fv = h.FilterVariable;
                    if (fv != null) {
                        // If we have a filter, check the Result to see if the filter returned execute_handler
                        handlerBody.Add(Instruction.Create(OpCodes.Ldloc, fv));
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

                    var needsTypeCheck = (h.Handler.CatchType != null) && (h.Handler.CatchType.FullName != "System.Object");
                    if (needsTypeCheck) {
                        // If the handler has a type check do an isinst to check whether it should run
                        handlerBody.Add(Instruction.Create(OpCodes.Ldloc, excVar));
                        handlerBody.Add(Instruction.Create(OpCodes.Isinst, h.Handler.CatchType));
                        handlerBody.Add(Instruction.Create(OpCodes.Brfalse, skip));
                    }

                    // Load anything the catch referenced onto the stack. If it wasn't a byref type,
                    //  we need to load its address because we convert all referenced values into refs
                    //  (so that the catch can modify them)
                    foreach (var a in h.CatchReferencedArguments)
                        if (a.ParameterType.IsByReference)
                            handlerBody.Add(Instruction.Create(OpCodes.Ldarg, (ParameterDefinition)a));
                        else
                            handlerBody.Add(Instruction.Create(OpCodes.Ldarga, (ParameterDefinition)a));

                    foreach (var v in h.CatchReferencedVariables)
                        if (v.VariableType.IsByReference)
                            handlerBody.Add(Instruction.Create(OpCodes.Ldloc, (VariableDefinition)v));
                        else
                            handlerBody.Add(Instruction.Create(OpCodes.Ldloca, (VariableDefinition)v));

                    // Now load the exception
                    handlerBody.Add(Instruction.Create(OpCodes.Ldloc, excVar));
                    // If the isinst passed we need to cast the exception value to the appropriate type
                    if (needsTypeCheck)
                        handlerBody.Add(Instruction.Create(OpCodes.Castclass, h.Handler.CatchType));

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

                CleanMethodBody(method, null, true);
                foreach (var g in newGroups) {
                    foreach (var h in g.Handlers) {
                        if (h.FilterMethod != null)
                            CleanMethodBody(h.FilterMethod, method, true);
                        if (h.Method != null)
                            CleanMethodBody(h.Method, method, true);
                    }
                }
            }
        }

        private void CollectReferencedLocals (MethodDefinition method, ParameterDefinition fakeThis, HashSet<VariableReference> referencedVariables, HashSet<ParameterReference> referencedArguments) {
            foreach (var eh in method.Body.ExceptionHandlers)
                if (eh.FilterStart != null)
                    CollectReferencedLocals(method, fakeThis, eh.FilterStart, eh.HandlerStart, referencedVariables, referencedArguments);
        }

        private void CollectReferencedLocals (
            MethodDefinition method, ParameterDefinition fakeThis, Instruction first, Instruction last, 
            HashSet<VariableReference> referencedVariables, HashSet<ParameterReference> referencedArguments
        ) {
            var insns = method.Body.Instructions;
            int i = insns.IndexOf(first), i2 = insns.IndexOf(last);
            if ((i < 0) || (i2 < 0))
                throw new ArgumentException();

            for (; i <= i2; i++) {
                var insn = insns[i];
                var vd = (insn.Operand as VariableReference) 
                    ?? LookupNumberedVariable(insn.OpCode.Code, method.Body.Variables); 
                var pd = insn.Operand as ParameterReference
                    ?? LookupNumberedArgument(insn.OpCode.Code, fakeThis, method.Parameters);

                // FIXME
                if (vd?.VariableType.FullName.Contains("__closure") ?? false)
                    continue;
                if (pd?.Name.Contains("__closure") ?? false)
                    continue;

                if (vd != null)
                    referencedVariables.Add(vd);
                if (pd != null)
                    referencedArguments.Add(pd);
            }
        }

        private void CleanMethodBody (MethodDefinition method, MethodDefinition oldMethod, bool verify) {
            var insns = method.Body.Instructions;
            foreach (var i in insns)
                i.Offset = insns.IndexOf(i);

            foreach (var i in insns) {
                OpCode newOpcode;
                if (ShortFormRemappings.TryGetValue(i.OpCode.Code, out newOpcode))
                    i.OpCode = newOpcode;

                if (!verify)
                    continue;

                var opInsn = i.Operand as Instruction;
                var opArg = i.Operand as ParameterDefinition;
                var opVar = i.Operand as VariableDefinition;

                if (opInsn != null) {
                    if (insns.IndexOf(opInsn) < 0)
                        throw new Exception($"Branch target {i.Operand} of opcode {i} is missing");
                    else if (oldMethod != null && oldMethod.Body.Instructions.IndexOf(opInsn) >= 0)
                        throw new Exception($"Branch target {i.Operand} of opcode {i} is present in old method");
                } else if (opArg != null) {
                    if ((opArg.Name == "__this") && method.HasThis) {
                        // HACK: method.Body.ThisParameter is unreliable for confusing reasons, and isn't
                        //  present in .Parameters so just ignore the check here
                    } else if (method.Parameters.IndexOf(opArg) < 0)
                        throw new Exception($"Parameter {opArg.Name} for opcode {i} is missing");
                    else if (oldMethod != null && oldMethod.Parameters.IndexOf(opArg) >= 0)
                        throw new Exception($"Parameter {opArg.Name} for opcode {i} is present in old method");
                } else if (opVar != null) {
                    if (method.Body.Variables.IndexOf(opVar) < 0)
                        throw new Exception($"Local {opVar} for opcode {i} is missing");
                    else if (oldMethod != null && oldMethod.Body.Variables.IndexOf(opVar) >= 0)
                        throw new Exception($"Local {opVar} for opcode {i} is present in old method");
                }
            }

            foreach (var i in insns)
                i.Offset = insns.IndexOf(i);

            if (verify)
            foreach (var p in method.Parameters)
                if (p.Index != method.Parameters.IndexOf(p))
                    throw new Exception("parameter index mismatch");

            if (verify)
            foreach (var v in method.Body.Variables)
                if (v.Index != method.Body.Variables.IndexOf(v))
                    throw new Exception("variable index mismatch");
        }

        private bool TryRemapInstruction (
            Instruction old,
            Collection<Instruction> oldBody, 
            Collection<Instruction> newBody,
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
            Collection<Instruction> oldBody, 
            Collection<Instruction> newBody,
            int offset = 0
        ) {
            Instruction result;
            if (!TryRemapInstruction(old, oldBody, newBody, offset, out result))
                return null;

            return result;
        }

        private Instruction CreateRemappedInstruction (
            object oldOperand, OpCode oldCode, object operand
        ) {
            if (operand == null)
                throw new ArgumentNullException();

            OpCode code = oldCode;
            if (
                (operand != null) && 
                (oldOperand != null) &&
                (operand.GetType() != oldOperand.GetType())
            ) {
                if (!LocalParameterRemappings.TryGetValue(oldCode.Code, out code))
                    throw new Exception(oldCode.ToString());
            }

            if (operand is ParameterDefinition)
                return Instruction.Create(code, (ParameterDefinition)operand);
            else if (operand is VariableDefinition)
                return Instruction.Create(code, (VariableDefinition)operand);
            else
                throw new Exception(operand.ToString());
        }

        private Instruction CloneInstruction (
            Instruction i, 
            ParameterDefinition fakeThis,
            MethodDefinition method,
            Dictionary<object, object> mapping = null
        ) {
            object operand = i.Operand ??
                (object)LookupNumberedVariable(i.OpCode.Code, method.Body.Variables) ??
                (object)LookupNumberedArgument(i.OpCode.Code, fakeThis, method.Parameters);

            var code = i.OpCode;
            if (Denumberings.ContainsKey(i.OpCode.Code))
                code = Denumberings[i.OpCode.Code];

            if (operand == null)
                return Instruction.Create(code);

            var newOperand = (mapping != null) && mapping.ContainsKey(operand) ? mapping[operand] : operand;

            if (newOperand is FieldReference) {
                FieldReference fref = newOperand as FieldReference;
                return Instruction.Create(code, fref);
            } else if (newOperand is TypeReference) {
                TypeReference tref = newOperand as TypeReference;
                return Instruction.Create(code, tref);
            } else if (newOperand is TypeDefinition) {
                TypeDefinition tdef = newOperand as TypeDefinition;
                return Instruction.Create(code, tdef);
            } else if (newOperand is MethodReference) {
                MethodReference mref = newOperand as MethodReference;
                return Instruction.Create(code, mref);
            } else if (newOperand is Instruction) {
                var insn = newOperand as Instruction;
                return Instruction.Create(code, insn);
            } else if (newOperand is string) {
                var s = newOperand as string;
                return Instruction.Create(code, s);
            } else if (newOperand is VariableReference) {
                var v = newOperand as VariableReference;
                if (operand.GetType() != v.GetType())
                    return CreateRemappedInstruction(operand, code, newOperand);
                else
                    return Instruction.Create(code, (VariableDefinition)v);
            } else if (newOperand is ParameterDefinition) {
                var p = newOperand as ParameterDefinition;
                if (operand.GetType() != p.GetType())
                    return CreateRemappedInstruction(operand, code, newOperand);
                else
                    return Instruction.Create(code, p);
            } else {
                throw new NotImplementedException(i.OpCode.ToString());
            }
        }

        private void CloneInstructions (
            MethodDefinition sourceMethod,
            ParameterDefinition fakeThis,
            int sourceIndex, int count,
            Mono.Collections.Generic.Collection<Instruction> target,
            int targetIndex,
            Dictionary<object, object> mapping = null,
            Func<Instruction, Instruction, Instruction> onFailedRemap = null
        ) {
            if (sourceIndex < 0)
                throw new ArgumentOutOfRangeException("sourceIndex");

            for (int n = 0; n < count; n++) {
                var i = sourceMethod.Body.Instructions[n + sourceIndex];
                var newInsn = CloneInstruction(i, fakeThis, sourceMethod, mapping);
                target.Add(newInsn);
            }

            // Fixup branches
            for (int i = 0; i < target.Count; i++) {
                var insn = target[i];
                var operand = insn.Operand as Instruction;
                if (operand == null)
                    continue;

                Instruction newOperand, newInsn;
                if (!TryRemapInstruction(operand, sourceMethod.Body.Instructions, target, targetIndex - sourceIndex, out newOperand)) {
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
