using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;
using Mono.Collections.Generic;

namespace ExceptionRewriter {
	public class RewriteOptions {
        public bool Mono = false;
		public bool EnableGenerics = false;
		public bool Verbose = false;
		public bool AbortOnError = true;
		public bool EnableSymbols = false;
		internal bool Overwrite;
	}

	public class AssemblyRewriter {
		public readonly RewriteOptions Options;
		public readonly AssemblyDefinition Assembly;

		private int ClosureIndex, FilterIndex;

		private readonly Dictionary<Code, OpCode> ShortFormRemappings = new Dictionary<Code, OpCode> ();
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

		public AssemblyRewriter (AssemblyDefinition assembly, RewriteOptions options) 
		{
			Assembly = assembly;
			Options = options;

			var tOpcodes = typeof (OpCodes);

			// Table to convert Br_S, Brfalse_S, etc into full-length forms
			//  because if you don't do this mono.cecil will silently generate bad IL
			foreach (var n in typeof (Code).GetEnumNames ()) {
				if (!n.EndsWith ("_S"))
					continue;
				if (n.StartsWith ("Ld") || n.StartsWith ("St"))
					continue;

				var full = n.Replace ("_S", "");
				var m = tOpcodes.GetField (full);
				ShortFormRemappings[(Code)Enum.Parse (typeof(Code), n)] = (OpCode)m.GetValue (null);
			}
		}

		// The encouraged typeof() based import isn't valid because it will import
		//  netcore corelib types into netframework apps and vice-versa
		private TypeReference ImportCorlibType (ModuleDefinition module, string @namespace, string name) {
			foreach (var m in Assembly.Modules) {
				var ts = m.TypeSystem;
				// Cecil uses this API internally to lookup corlib types by name before exposing them in TypeSystem
				var mLookup = ts.GetType ().GetMethod ("LookupType", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
				var result = mLookup.Invoke (ts, new object[] { @namespace, name });
				if (result != null)
					return module.ImportReference ((TypeReference)result);
			}

			return null;
		}

		// Locate an existing assembly reference to the specified assembly, then reference the
		//  specified type by name from that assembly and import it
		private TypeReference ImportReferencedType (ModuleDefinition module, string assemblyName, string @namespace, string name) 
		{
			var s = module.TypeSystem.String;

			foreach (var m in Assembly.Modules) {
				foreach (var ar in m.AssemblyReferences) {
					if (!ar.FullName.Contains (assemblyName))
						continue;

					var ad = Assembly.MainModule.AssemblyResolver.Resolve (ar);

					var result = new TypeReference (
						@namespace, name, ad.MainModule, ad.MainModule
					);
					return module.ImportReference (result);
				}
			}

			return null;
		}

		private TypeReference GetExceptionFilter (ModuleDefinition module, bool autoAddReference = true) 
		{
            TypeReference result;

            if (Options.Mono) {
			    result = ImportCorlibType (module, "Mono", "ExceptionFilter");
			    if (result != null)
				    return result;
            }

			result = ImportReferencedType (module, "ExceptionFilterSupport", "Mono", "ExceptionFilter");
			if (result == null) {
				if (!autoAddReference)
					throw new Exception ("ExceptionFilterSupport is not referenced");

				var anr = new AssemblyNameReference ("ExceptionFilterSupport", new Version (1, 0, 0, 0));
				module.AssemblyReferences.Add (anr);
				return GetExceptionFilter (module, false);
			}

			return result;
		}

		private TypeReference GetException (ModuleDefinition module) 
		{
			return ImportCorlibType (module, "System", "Exception");
		}

		private TypeReference GetExceptionDispatchInfo (ModuleDefinition module) 
		{
			return ImportCorlibType (module, "System.Runtime.ExceptionServices", "ExceptionDispatchInfo");
		}

		public int Rewrite () 
		{
			int errorCount = 0;

			foreach (var mod in Assembly.Modules) {
				// Make temporary copy of the types and methods lists because we mutate them while iterating
				foreach (var type in mod.GetTypes ())
					foreach (var meth in type.Methods.ToArray ())
						errorCount += RewriteMethod (meth);
			}

			return errorCount;
		}

		private Instruction[] MakeDefault (
			TypeReference t,
			Dictionary<TypeReference, VariableDefinition> tempLocals
		) {
			if (t.FullName == "System.Void")
				return new Instruction[0];

			if (t.IsByReference || !t.IsValueType)
				return new[] { Instruction.Create (OpCodes.Ldnull) };

			switch (t.FullName) {
				case "System.Int32":
				case "System.UInt32":
				case "System.Boolean":
					return new[] { Instruction.Create (OpCodes.Ldc_I4_0) };
				default:
					VariableDefinition tempLocal;
					if (!tempLocals.TryGetValue (t, out tempLocal)) {
						tempLocals[t] = tempLocal = new VariableDefinition (t);
						return new[] {
							Instruction.Create (OpCodes.Ldloca, tempLocal),
							Instruction.Create (OpCodes.Initobj, t),
							Instruction.Create (OpCodes.Ldloc, tempLocal)
						};
					} else
						return new[] { Instruction.Create (OpCodes.Ldloc, tempLocal) };
			}
		}

		private Instruction Patch (Instruction i, Instruction old, Instruction replacement) 
		{
			if (i == old)
				return replacement;
			else
				return i;
		}

		private void Patch (MethodDefinition method, RewriteContext context, Instruction old, Instruction replacement) 
		{
			var body = method.Body.Instructions;
			for (int i = 0; i < body.Count; i++) {
				if (body[i].Operand == old)
					body[i] = Instruction.Create (body[i].OpCode, replacement);

                var opInsns = body[i].Operand as Instruction[];
                if (opInsns != null) {
                    for (int j = 0; j < opInsns.Length; j++) {
                        if (opInsns[j] == old)
                            opInsns[j] = replacement;
                    }
                }
			}

            foreach (var p in context.Pairs) {
                p.A = Patch (p.A, old, replacement);
                p.B = Patch (p.B, old, replacement);
            }

            foreach (var g in context.NewGroups) {
                g.FirstPushInstruction = Patch (g.FirstPushInstruction, old, replacement);
                g.TryStart = Patch (g.TryStart, old, replacement);
                g.TryEnd = Patch (g.TryEnd, old, replacement);
                g.TryEndPredecessor = Patch (g.TryEndPredecessor, old, replacement);

                foreach (var h in g.Blocks) {
                    for (int l = 0; l < h.LeaveTargets.Count; l++)
                        h.LeaveTargets[l] = Patch (h.LeaveTargets[l], old, replacement);
                    h.FirstFilterInsn = Patch (h.FirstFilterInsn, old, replacement);
                    Patch (h.Handler, old, replacement);
                }
            }

			foreach (var eh in method.Body.ExceptionHandlers)
                Patch (eh, old, replacement);
		}

        private void Patch (ExceptionHandler eh,  Instruction old, Instruction replacement) 
        {
            eh.TryStart = Patch (eh.TryStart, old, replacement);
			eh.TryEnd = Patch (eh.TryEnd, old, replacement);
			eh.HandlerStart = Patch (eh.HandlerStart, old, replacement);
			eh.HandlerEnd = Patch (eh.HandlerEnd, old, replacement);
			eh.FilterStart = Patch (eh.FilterStart, old, replacement);
        }

		private Instruction Patch (Instruction i, Dictionary<Instruction, Instruction> pairs) 
		{
            if (i == null)
                return null;
            Instruction result;
            if (!pairs.TryGetValue (i, out result))
                result = i;
            return result;
		}

        private void Patch (ExceptionHandler eh, Dictionary<Instruction, Instruction> pairs) 
        {
            eh.TryStart = Patch (eh.TryStart, pairs);
			eh.TryEnd = Patch (eh.TryEnd, pairs);
			eh.HandlerStart = Patch (eh.HandlerStart, pairs);
			eh.HandlerEnd = Patch (eh.HandlerEnd, pairs);
			eh.FilterStart = Patch (eh.FilterStart, pairs);
        }

		private void PatchMany (MethodDefinition method, RewriteContext context, Dictionary<Instruction, Instruction> pairs) 
        {
			var body = method.Body.Instructions;
            Instruction replacement;

			for (int i = 0; i < body.Count; i++) {
                var opInsn = body[i].Operand as Instruction;
                if (opInsn != null && pairs.TryGetValue (opInsn, out replacement))
                    body[i].Operand = replacement;

                var opInsns = body[i].Operand as Instruction[];
                if (opInsns != null) {
                    for (int j = 0; j < opInsns.Length; j++) {
                        if (pairs.TryGetValue (opInsns[j], out replacement))
                            opInsns[j] = replacement;
                    }
                }
			}

            foreach (var p in context.Pairs) {
                p.A = Patch (p.A, pairs);
                p.B = Patch (p.B, pairs);
            }

            foreach (var g in context.NewGroups) {
                g.FirstPushInstruction = Patch (g.FirstPushInstruction, pairs);
                g.TryStart = Patch (g.TryStart, pairs);
                g.TryEnd = Patch (g.TryEnd, pairs);
                g.TryEndPredecessor = Patch (g.TryEndPredecessor, pairs);

                foreach (var h in g.Blocks) {
                    for (int l = 0; l < h.LeaveTargets.Count; l++)
                        h.LeaveTargets[l] = Patch (h.LeaveTargets[l], pairs);
                    h.FirstFilterInsn = Patch (h.FirstFilterInsn, pairs);
                    Patch (h.Handler, pairs);
                }
            }

			foreach (var eh in method.Body.ExceptionHandlers)
                Patch (eh, pairs);
		}

		private void InsertOps (
			Collection<Instruction> body, int offset, params Instruction[] ops
		) {
			for (int i = ops.Length - 1; i >= 0; i--)
				body.Insert (offset, ops[i]);
		}

		private Instruction ExtractExceptionHandlerExitTarget (ExceptionHandler eh) 
		{
			var leave = eh.HandlerEnd.Previous;
			if (leave.OpCode == OpCodes.Rethrow)
				return leave;

			var leaveTarget = leave.Operand as Instruction;
			if (leaveTarget == null)
				throw new Exception ("Exception handler did not end with a 'leave'");
			return leaveTarget;
		}

		private bool IsStoreOperation (Code opcode) 
		{
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

			throw new NotImplementedException (opcode.ToString ());
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

		private GenericInstanceType FilterGenericInstanceType<T, U> (GenericInstanceType git, Dictionary<T, U> replacementTable)
			where T : TypeReference
			where U : TypeReference
		{
			List<TypeReference> newArgs = null;

			for (int i = 0; i < git.GenericArguments.Count; i++) {
				var ga = git.GenericArguments[i];
				var newGa = FilterTypeReference (ga, replacementTable);

				if (newGa != ga) {
					if (newArgs == null) {
						newArgs = new List<TypeReference> ();
						for (int j = 0; j < i; j++)
							newArgs[j] = git.GenericArguments[j];
					}

					newArgs.Add (newGa);
				} else if (newArgs != null)
					newArgs.Add (ga);
			}

			if (newArgs != null) {
				var result = new GenericInstanceType (git.ElementType);
				foreach (var arg in newArgs)
					result.GenericArguments.Add (arg);

				return result;
			} else {
				return git;
			}
		}

		private TypeReference FilterByReferenceType<T, U> (ByReferenceType brt, Dictionary<T, U> replacementTable)
			where T : TypeReference
			where U : TypeReference
		{
			var et = FilterTypeReference<T, U> (brt.ElementType, replacementTable);
			if (et != brt.ElementType)
				return new ByReferenceType (et);
			else
				return brt;
		}

		private TypeReference FilterPointerType<T, U> (PointerType pt, Dictionary<T, U> replacementTable)
			where T : TypeReference
			where U : TypeReference
		{
			var et = FilterTypeReference<T, U> (pt.ElementType, replacementTable);
			if (et != pt.ElementType)
				return new PointerType (et);
			else
				return pt;
		}

		private TypeReference FilterTypeReference<T, U> (TypeReference tr, Dictionary<T, U> replacementTable)
			where T : TypeReference
			where U : TypeReference
		{
			if ((replacementTable == null) || (replacementTable.Count == 0))
				return tr;

			TypeReference result;
			U temp;

			if (replacementTable.TryGetValue ((T)tr, out temp))
				result = temp;
			else
				result = tr;

			for (int i = 0; i < 50; i++) {
				var prev = result;
				var git = result as GenericInstanceType;
				var brt = result as ByReferenceType;
				var pt = result as PointerType;
				var at = result as ArrayType;

				if (git != null)
					result = FilterGenericInstanceType<T, U> (git, replacementTable);
				else if (brt != null) {
					var newEt = FilterTypeReference<T, U> (brt.ElementType, replacementTable);
					if (newEt != brt.ElementType)
						result = new ByReferenceType (newEt);
				} else if (pt != null) {
					var newEt = FilterTypeReference<T, U> (pt.ElementType, replacementTable);
					if (newEt != pt.ElementType)
						result = new PointerType (newEt);
				} else if (at != null) {
					var newEt = FilterTypeReference<T, U> (at.ElementType, replacementTable);
					if (newEt != at.ElementType)
						result = new ArrayType (newEt, at.Rank);
				}

				if (prev == result)
					return result;
			}

			throw new Exception ("FilterTypeReference iterated 50 times without completing");
		}

		private T FilterMemberReference<T, U, V> (T mr, Dictionary<U, V> replacementTable)
			where T : MemberReference
			where U : TypeReference
			where V : TypeReference
		{
			if ((replacementTable == null) || (replacementTable.Count == 0))
				return mr;

			var field = mr as FieldReference;
			var meth = mr as MethodReference;
			var prop = mr as PropertyReference;

			if (field != null)
				return FilterFieldReference<U, V> (field, replacementTable) as T;
			else if (meth != null)
				return FilterMethodReference<U, V> (meth, replacementTable) as T;
			else if (prop != null)
				return FilterPropertyReference<U, V> (prop, replacementTable) as T;
			else
				throw new Exception ("Unhandled reference type");

			return mr;
		}

		private MemberReference FilterPropertyReference<U, V> (PropertyReference prop, Dictionary<U, V> replacementTable)
			where U : TypeReference
			where V : TypeReference 
		{
			throw new NotImplementedException ("FilterPropertyReference not implemented");
		}

		private MemberReference FilterMethodReference<U, V> (MethodReference meth, Dictionary<U, V> replacementTable)
			where U : TypeReference
			where V : TypeReference 
		{
			var result = new MethodReference (
				meth.Name, 
				FilterTypeReference (meth.ReturnType, replacementTable),
				// FIXME: Is this correct?
				FilterTypeReference (meth.DeclaringType, replacementTable)
			) {
			};
			foreach (var p in meth.Parameters)
				result.Parameters.Add (new ParameterDefinition (p.Name, p.Attributes, FilterTypeReference (p.ParameterType, replacementTable)));
			return result;
		}

		private MemberReference FilterFieldReference<U, V> (FieldReference field, Dictionary<U, V> replacementTable)
			where U : TypeReference
			where V : TypeReference 
		{
			var result = new FieldReference (
				field.Name,
				FilterTypeReference (field.FieldType, replacementTable),
				FilterTypeReference (field.DeclaringType, replacementTable)
			) {
			};
			return result;
		}

        private Instruction GeneratedRet ()
        {
            return Instruction.Create (OpCodes.Ret);
        }

        private Instruction Nop (string description = null) 
        {
            var result = Instruction.Create (OpCodes.Nop);
            result.Operand = description;
            return result;
        }

        private Instruction Rethrow (string description = null) 
        {
            var result = Instruction.Create (OpCodes.Rethrow);
            result.Operand = description;
            return result;
        }

		private MethodDefinition CreateConstructor (TypeDefinition type) 
		{
			var ctorMethod = new MethodDefinition (
				".ctor", MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName, 
				type.Module.TypeSystem.Void
			);
			type.Methods.Add (ctorMethod);
			InsertOps (ctorMethod.Body.Instructions, 0, new[] {
				Instruction.Create (OpCodes.Ldarg_0),
				Instruction.Create (OpCodes.Call, 
					new MethodReference (
						".ctor", type.Module.TypeSystem.Void, 
						type.BaseType
					) { HasThis = true }),
				Nop (),
				Instruction.Create (OpCodes.Ret)
			});
			return ctorMethod;
		}

		private VariableDefinition ConvertToClosure (
			MethodDefinition method, ParameterDefinition fakeThis, 
			HashSet<VariableReference> variables, HashSet<ParameterReference> parameters, 
			out TypeDefinition closureTypeDefinition, out TypeReference closureTypeReference
		) {
			var insns = method.Body.Instructions;
			closureTypeDefinition = new TypeDefinition (
				method.DeclaringType.Namespace, method.Name + "__closure" + (ClosureIndex++).ToString (),
				TypeAttributes.Class | TypeAttributes.NestedPublic
			);
			closureTypeDefinition.BaseType = method.Module.TypeSystem.Object;
			method.DeclaringType.NestedTypes.Add (closureTypeDefinition);

			var functionGpMapping = new Dictionary<TypeReference, GenericParameter> ();
			CopyGenericParameters (method.DeclaringType, closureTypeDefinition, functionGpMapping);
			CopyGenericParameters (method, closureTypeDefinition, functionGpMapping);

			var isGeneric = method.DeclaringType.HasGenericParameters || method.HasGenericParameters;
			var thisGenType = new GenericInstanceType (method.DeclaringType);
			var thisType = isGeneric ? thisGenType : (TypeReference)method.DeclaringType;
			var genClosureTypeReference = new GenericInstanceType (closureTypeDefinition);

			foreach (var p in method.DeclaringType.GenericParameters) {
				thisGenType.GenericArguments.Add (functionGpMapping[p]);
				genClosureTypeReference.GenericArguments.Add (functionGpMapping[p]);
			}

			foreach (var p in method.GenericParameters)
				genClosureTypeReference.GenericArguments.Add (functionGpMapping[p]);

			if ((method.DeclaringType.GenericParameters.Count + method.GenericParameters.Count) > 0)
				closureTypeReference = genClosureTypeReference;
			else
				closureTypeReference = closureTypeDefinition;

			var ctorMethod = CreateConstructor (closureTypeDefinition);

			var isStatic = method.IsStatic;

			var localCount = 0;
			var closureVar = new VariableDefinition (closureTypeReference);

			var extractedVariables = variables.ToDictionary (
				v => (object)v, 
				v => {
                    var vd = v.Resolve();

                    string variableName;
                    if ((method.DebugInformation == null) || !method.DebugInformation.TryGetName(vd, out variableName))
                        variableName = "__local_" + method.Body.Variables.IndexOf(vd);

                    return new FieldDefinition (variableName, FieldAttributes.Public, FilterTypeReference (v.VariableType, functionGpMapping));
                }
			);

			method.Body.Variables.Add (closureVar);

			for (int i = 0; i < method.Parameters.Count; i++) {
				var p = method.Parameters[i];
				if (!parameters.Contains (p))
					continue;

				var name = (p.Name != null) ? "arg_" + p.Name : "arg" + i;
				extractedVariables[p] = new FieldDefinition (name, FieldAttributes.Public, FilterTypeReference (p.ParameterType, functionGpMapping));
			}

			if (!isStatic)
				extractedVariables[fakeThis] = new FieldDefinition ("__this", FieldAttributes.Public, thisType);

			foreach (var kvp in extractedVariables)
				closureTypeDefinition.Fields.Add (kvp.Value);

			FilterRange (
				method, 0, insns.Count - 1, (insn) => {
					var variable = (insn.Operand as VariableDefinition) 
						?? LookupNumberedVariable (insn.OpCode.Code, method.Body.Variables);
					var arg = (insn.Operand as ParameterDefinition)
						?? LookupNumberedArgument (insn.OpCode.Code, isStatic ? null : fakeThis, method.Parameters);

					// FIXME
					if (variable == closureVar)
						return null;

					if ((variable == null) && (arg == null))
						return null;

					FieldDefinition matchingField;
					var lookupKey = (object)variable ?? arg;
					if (!extractedVariables.TryGetValue (lookupKey, out matchingField))
						return null;

					if (IsStoreOperation (insn.OpCode.Code)) {
						// HACK: Because we have no way to swap values on the stack, we have to keep the
						//  existing local but use it as a temporary store point before flushing into the
						//  closure
						Instruction reload;
						if (variable != null)
							reload = Instruction.Create (OpCodes.Ldloc, variable);
						else
							reload = Instruction.Create (OpCodes.Ldarg, arg);

						return new[] {
							insn, 
							Instruction.Create (OpCodes.Ldloc, closureVar),
							reload,
							Instruction.Create (OpCodes.Stfld, matchingField)
						};
					} else {
						var newInsn = Instruction.Create (OpCodes.Ldloc, closureVar);
						var loadOp =
							((insn.OpCode.Code == Code.Ldloca) ||
							(insn.OpCode.Code == Code.Ldloca_S))
								? OpCodes.Ldflda
								: OpCodes.Ldfld;
						return new[] {
							newInsn, 
							Instruction.Create (loadOp, matchingField)
						};
					}
				}
			);

			var ctorRef = new MethodReference (
				".ctor", method.DeclaringType.Module.TypeSystem.Void, closureTypeReference
			) {
				// CallingConvention = MethodCallingConvention.ThisCall,
				ExplicitThis = false,
				HasThis = true
			};

			var toInject = new List<Instruction> () {
				Instruction.Create (OpCodes.Newobj, ctorRef),
				Instruction.Create (OpCodes.Stloc, closureVar)
			};

			if (!isStatic) {
				toInject.AddRange (new[] {
					Instruction.Create (OpCodes.Ldloc, closureVar),
					Instruction.Create (OpCodes.Ldarg, fakeThis),
					Instruction.Create (OpCodes.Stfld, extractedVariables[fakeThis])
				});
			}

			foreach (var p in method.Parameters) {
				if (!parameters.Contains (p))
					continue;

				toInject.AddRange (new[] {
					Instruction.Create (OpCodes.Ldloc, closureVar),
					Instruction.Create (OpCodes.Ldarg, p),
					Instruction.Create (OpCodes.Stfld, extractedVariables[p])
				});
			}

			InsertOps (insns, 0, toInject.ToArray ());

			CleanMethodBody (method, null, true);

			return closureVar;
		}

		private int CatchCount;

		private Instruction PostFilterRange (
			Dictionary<Instruction, Instruction> remapTable, Instruction oldValue
		) {
			if (oldValue == null)
				return null;

			Instruction result;
			if (remapTable.TryGetValue (oldValue, out result))
				return result;

			return oldValue;
		}

		private void FilterRange (
			MethodDefinition method,
			int firstIndex, int lastIndex, Func<Instruction, Instruction[]> filter
		) {
			var remapTableFirst = new Dictionary<Instruction, Instruction> ();
			var remapTableLast = new Dictionary<Instruction, Instruction> ();
			var instructions = method.Body.Instructions;

			var firstRemovedInstruction = instructions[firstIndex];

			for (int i = firstIndex; i <= lastIndex; i++) {
				var insn = instructions[i];
				var result = filter (insn);
				if (result == null)
					continue;
				if (result.Length == 1 && result[0] == insn)
					continue;

				if (insn != result[0])
					remapTableFirst[insn] = result[0];
				for (int j = result.Length - 1; j >= 1; j--)
					instructions.Insert (i + 1, result[j]);

				remapTableLast[insn] = result[result.Length - 1];

				lastIndex += (result.Length - 1);
				i += (result.Length - 1);
			}

			for (int i = 0; i < instructions.Count; i++) {
				var insn = instructions[i];

                Instruction newInsn;
                if (remapTableFirst.TryGetValue (insn, out newInsn)) {
                    instructions[i] = newInsn;
                    insn = newInsn;
                }

				var operand = insn.Operand as Instruction;
				if (operand == null)
					continue;

				Instruction newOperand;
				if (!remapTableFirst.TryGetValue (operand, out newOperand))
					continue;

				insn.Operand = newOperand;
			}

			foreach (var eh in method.Body.ExceptionHandlers) {
				eh.FilterStart = PostFilterRange (remapTableFirst, eh.FilterStart);
				eh.TryStart = PostFilterRange (remapTableFirst, eh.TryStart);
				eh.TryEnd = PostFilterRange (remapTableFirst, eh.TryEnd);
				eh.HandlerStart = PostFilterRange (remapTableFirst, eh.HandlerStart);
				eh.HandlerEnd = PostFilterRange (remapTableFirst, eh.HandlerEnd);
			}

			CleanMethodBody (method, null, true);
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
						: new ByReferenceType (vr.VariableType);
				var newParam = new ParameterDefinition ("loc_" + i++.ToString (), ParameterAttributes.None, newParamType);
				newMethod.Parameters.Add (newParam);
				mapping[vr] = newParam;
				if (newParamType != vr.VariableType)
					needsLdind.Add (newParam);
			}
		}

		private void GenerateParameters (
			MethodDefinition newMethod, HashSet<ParameterReference> parameters, 
			Dictionary<object, object> mapping, HashSet<object> needsLdind
		) {
			foreach (var pr in parameters) {
				// FIXME: replacementTable
				var filteredParamType = FilterTypeReference<TypeReference, TypeReference> (pr.ParameterType, null);
				var newParamType =
					filteredParamType.IsByReference
						? filteredParamType
						: new ByReferenceType (filteredParamType);
				var newParam = new ParameterDefinition ("arg_" + pr.Name, ParameterAttributes.None, newParamType);
				newMethod.Parameters.Add (newParam);
				mapping[pr] = newParam;
				if (newParamType != filteredParamType)
					needsLdind.Add (newParam);
			}
		}

		private void CopyGenericParameters (TypeDefinition sourceType, TypeDefinition owner, Dictionary<TypeReference, GenericParameter> result) 
		{
			foreach (var gp in sourceType.GenericParameters) {
				result[gp] = new GenericParameter (gp.Name, owner);
				owner.GenericParameters.Add (result[gp]);
			}
		}

		private void CopyGenericParameters (MethodDefinition sourceMethod, TypeDefinition owner, Dictionary<TypeReference, GenericParameter> result) 
		{
			foreach (var gp in sourceMethod.GenericParameters) {
				result[gp] = new GenericParameter (gp.Name, owner);
				owner.GenericParameters.Add (result[gp]);
			}
		}

		private void CopyGenericParameters (MethodDefinition sourceMethod, MethodDefinition owner, Dictionary<TypeReference, GenericParameter> result) 
		{
			foreach (var gp in sourceMethod.GenericParameters) {
				result[gp] = new GenericParameter (gp.Name, owner);
				owner.GenericParameters.Add (result[gp]);
			}
		}

        /// <summary>
        /// Extract a catch block into an independent method that accepts an exception parameter and a closure
        /// The catch method returns 0 to indicate that the exception should be rethrown or [1-n] to indicate a target within the
        ///  parent function that should be leave'd to
        /// </summary>
		private ExcBlock ExtractCatch (
			MethodDefinition method, ExceptionHandler eh, 
            VariableDefinition closure, ParameterDefinition fakeThis, 
            ExcGroup group, RewriteContext context
		) {
			var insns = method.Body.Instructions;
			var closureType = closure.VariableType;

			var catchMethod = new MethodDefinition (
				method.Name + "__catch" + (CatchCount++),
				MethodAttributes.Static | MethodAttributes.Private,
				method.Module.TypeSystem.Int32
			);

			var gpMapping = new Dictionary<TypeReference, GenericParameter> ();
			CopyGenericParameters (method, catchMethod, gpMapping);

			catchMethod.Body.InitLocals = true;
			var closureParam = new ParameterDefinition ("__closure", ParameterAttributes.None, closureType);
			var excParam = new ParameterDefinition ("__exc", ParameterAttributes.None, eh.CatchType ?? method.Module.TypeSystem.Object);
			var paramMapping = new Dictionary<object, object> {
				{closure, closureParam }
			};
			var closureVariable = new VariableDefinition (closureType);
			var needsLdind = new HashSet<object> ();

			catchMethod.Parameters.Add (excParam);
			catchMethod.Parameters.Add (closureParam);

			var catchInsns = catchMethod.Body.Instructions;

			var handlerFirstIndex = insns.IndexOf (eh.HandlerStart);
			var handlerLastIndex = insns.IndexOf (eh.HandlerEnd) - 1;

            Console.WriteLine($"Extracting catch [{eh.HandlerStart}] - [{insns[handlerLastIndex]}]");

			// CHANGE #4: Adding method earlier
			method.DeclaringType.Methods.Add (catchMethod);

            var i1 = insns.IndexOf (eh.HandlerStart);
            var i2 = insns.IndexOf (eh.HandlerEnd);

            var endsWithRethrow = insns[i2].OpCode.Code == Code.Rethrow;

            if (i2 <= i1)
                throw new Exception("Hit beginning of handler while rewinding past rethrows");

            var leaveTargets = new List<Instruction> ();

			// FIXME: Use generic parameter mapping to replace GP type references
			var newMapping = ExtractRangeToMethod (
				method, catchMethod, fakeThis,
				i1, i2 - 1,
				variableMapping: paramMapping,
				typeMapping: gpMapping,
                context: context,
                // FIXME: Identify when we want to preserve control flow and when we don't
                preserveControlFlow: false,
                filter: (insn, range) => {
					var operandTr = insn.Operand as TypeReference;
					if (operandTr != null) {
						var newOperandTr = FilterTypeReference (operandTr, gpMapping);
						if (newOperandTr != operandTr)
							insn.Operand = newOperandTr;
					}

					var operandMr = insn.Operand as MemberReference;
					if (operandMr != null) {
						var newOperandMr = FilterMemberReference (operandMr, gpMapping);
						if (newOperandMr != operandMr)
							insn.Operand = newOperandMr;
					}

                    var operandInsn = insn.Operand as Instruction;

					switch (insn.OpCode.Code) {
						case Code.Leave:
						case Code.Leave_S:
                            if (
                                (range == null)
                            ) {
                                var targetIndex = leaveTargets.IndexOf(operandInsn) + 1;
                                if (targetIndex <= 0) {
                                    targetIndex = leaveTargets.Count + 1;
                                    leaveTargets.Add (operandInsn);
                                }

                                return new[] {
                                    Nop ($"Leave to target #{targetIndex} ({operandInsn})"),
                                    Instruction.Create (OpCodes.Ldc_I4, targetIndex),
                                    GeneratedRet ()
                                };
                            } else
                                break;

						case Code.Rethrow:
                            if (range == null)
                                return new[] {
                                    Nop ("Rethrow"),
                                    Instruction.Create (OpCodes.Ldc_I4_0),
                                    GeneratedRet ()
                                };
                            else
                                break;

                        case Code.Ret:
                            // It's not valid to ret from inside a filter or catch: https://docs.microsoft.com/en-us/dotnet/api/system.reflection.emit.opcodes.ret
                            throw new Exception("Unexpected ret inside catch block");
                    }

                    return null;
				}
			);

			CleanMethodBody (catchMethod, method, false);

            if (catchInsns.Count > 0) {
			    var first = catchInsns[0];

			    InsertOps (
				    catchInsns, 0, new[] {
					    Instruction.Create (OpCodes.Ldarg, excParam)
				    }
			    );

			    CleanMethodBody (catchMethod, method, true);
            }

			var isCatchAll = (eh.HandlerType == ExceptionHandlerType.Catch) && (eh.CatchType?.FullName == "System.Object");
			var handler = new ExcBlock {
				Handler = eh,
				CatchMethod = catchMethod,
				IsCatchAll = isCatchAll,
				Mapping = newMapping,
                LeaveTargets = leaveTargets
			};
			return handler;
		}

		private OpCode SelectStindForOperand (object operand) 
		{
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

		private OpCode SelectLdindForOperand (object operand) 
		{
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

        private bool IsEndfilter (Instruction insn) {
            return (insn.OpCode.Code == Code.Endfilter) ||
                (insn.Operand as string == "extracted endfilter");
        }

		private void ExtractFilter (
			MethodDefinition method, ExceptionHandler eh, 
            VariableDefinition closure, ParameterDefinition fakeThis, 
            ExcGroup group, RewriteContext context,
            ExcBlock catchBlock
		) {
			var insns = method.Body.Instructions;
			var closureType = closure.VariableType;
			var filterIndex = FilterIndex++;
			var filterTypeDefinition = new TypeDefinition (
				method.DeclaringType.Namespace, method.Name + "__filter" + filterIndex.ToString (),
				TypeAttributes.NestedPublic | TypeAttributes.Class,
				GetExceptionFilter (method.Module)
			);

			var gpMapping = new Dictionary<TypeReference, GenericParameter> ();
			CopyGenericParameters (method.DeclaringType, filterTypeDefinition, gpMapping);
			CopyGenericParameters (method, filterTypeDefinition, gpMapping);

			filterTypeDefinition.BaseType = GetExceptionFilter (method.Module);
			method.DeclaringType.NestedTypes.Add (filterTypeDefinition);
			CreateConstructor (filterTypeDefinition);

			var closureField = new FieldDefinition (
				"closure", FieldAttributes.Public, closureType
			);
			filterTypeDefinition.Fields.Add (closureField);

			var filterMethod = new MethodDefinition (
				"Evaluate",
				MethodAttributes.Virtual | MethodAttributes.Public,
				method.Module.TypeSystem.Int32
			);
			filterMethod.Body.InitLocals = true;

			filterTypeDefinition.Methods.Add (filterMethod);

			var excArg = new ParameterDefinition ("exc", default (ParameterAttributes), method.Module.TypeSystem.Object);
			filterMethod.Parameters.Add (excArg);

			int i1 = insns.IndexOf (eh.FilterStart), i2 = insns.IndexOf (eh.HandlerStart);
			if (i2 < 0)
				throw new Exception ($"Handler start instruction {eh.HandlerStart} not found in method body");

            if (i2 <= i1)
                throw new Exception("Handler size was 0 or less");

            var endfilter = insns[i2 - 1];
            if (!IsEndfilter(endfilter))
                throw new Exception("Filter did not end with an endfilter");

            {
                Console.WriteLine($"Extracting filter [{eh.FilterStart}] - [{endfilter}]");

                var variableMapping = new Dictionary<object, object> ();
			    var newVariables = ExtractRangeToMethod (
				    method, filterMethod, fakeThis, i1, i2 - 1, 
				    variableMapping: variableMapping, typeMapping: gpMapping, 
                    context: context,
                    preserveControlFlow: false
			    );
			    var newClosureLocal = (VariableDefinition)newVariables[closure];

			    var filterInsns = filterMethod.Body.Instructions;
                if (filterInsns.Count <= 0)
                    throw new Exception("Filter body was empty");

			    var oldFilterInsn = filterInsns[filterInsns.Count - 1];
                if (!IsEndfilter(oldFilterInsn))
                    throw new Exception("Unexpected last instruction");

                var filterReplacement = Instruction.Create (OpCodes.Ret);
			    filterInsns[filterInsns.Count - 1] = filterReplacement;
			    Patch (filterMethod, context, oldFilterInsn, filterReplacement);

			    InsertOps (
				    filterInsns, 0, new[] {
					    // Load the closure from this and store it into our temporary
					    Instruction.Create (OpCodes.Ldarg_0),
					    Instruction.Create (OpCodes.Ldfld, closureField),
					    Instruction.Create (OpCodes.Stloc, newClosureLocal),
					    // Load the exception from arg1 since exception handlers are entered with it on the stack
					    Instruction.Create (OpCodes.Ldarg, excArg)
				    }
			    );

                // Scan through the extracted method body to find references to the closure object local
                //  since it is an instance field inside the filter object instead of a local variable
			    for (int i = 0; i < filterInsns.Count; i++) {
				    var insn = filterInsns[i];
				    if (insn.Operand != closure)
					    continue;

                    if (insn.OpCode.Code != Code.Ldloc)
                        throw new Exception ("Invalid reference to closure");

                    // Replace the ldloc with a ldfld this.closure
				    filterInsns[i] = Instruction.Create (OpCodes.Ldarg, fakeThis);
				    Patch (filterMethod, context, insn, filterInsns[i]);
				    filterInsns.Insert (i + 1, Instruction.Create (OpCodes.Ldfld, closureField));
			    }

			    CleanMethodBody (filterMethod, method, true);
            }

            catchBlock.FilterMethod = filterMethod;
            catchBlock.FilterType = filterTypeDefinition;
            catchBlock.FilterVariable = new VariableDefinition(filterTypeDefinition);
            catchBlock.FirstFilterInsn = eh.FilterStart;

            method.Body.Variables.Add (catchBlock.FilterVariable);
		}

        private class EhRange {
            public ExceptionHandler Handler;
            public int MinIndex, MaxIndex;
            public Instruction OldTryStart, OldTryEnd, OldHandlerStart, OldHandlerEnd;
            public Instruction NewTryStart, NewTryEnd, NewHandlerStart, NewHandlerEnd;
        }

        private EhRange FindRangeForOffset (List<EhRange> ranges, int offset) {
            foreach (var range in ranges) {
                if ((offset >= range.MinIndex) && (offset <= range.MaxIndex))
                    return range;
            }

            return null;
        }

		private Dictionary<object, object> ExtractRangeToMethod<T, U> (
			MethodDefinition sourceMethod, MethodDefinition targetMethod, 
			ParameterDefinition fakeThis,
			int firstIndex, int lastIndex,
			Dictionary<object, object> variableMapping,
			Dictionary<T, U> typeMapping,
            RewriteContext context, 
            bool preserveControlFlow,
            Func<Instruction, EhRange, Instruction[]> filter = null
		)
			where T : TypeReference
			where U : TypeReference
		{
			var insns = sourceMethod.Body.Instructions;
			var targetInsns = targetMethod.Body.Instructions;

			foreach (var loc in sourceMethod.Body.Variables) {
				if (variableMapping.ContainsKey (loc))
					continue;
				var newLoc = new VariableDefinition (FilterTypeReference (loc.VariableType, typeMapping));
				targetMethod.Body.Variables.Add (newLoc);
				variableMapping[loc] = newLoc;
			}

            var ranges = new List<EhRange> ();
            foreach (var eh in sourceMethod.Body.ExceptionHandlers) {
                var range = new EhRange {
                    Handler = eh,
                    OldTryStart = eh.TryStart,
                    OldTryEnd = eh.TryEnd,
                    OldHandlerStart = eh.HandlerStart,
                    OldHandlerEnd = eh.HandlerEnd
                };

                int
                    tryStartIndex = insns.IndexOf(eh.TryStart),
                    tryEndIndex = insns.IndexOf(eh.TryEnd),
                    handlerStartIndex = insns.IndexOf(eh.HandlerStart),
                    handlerEndIndex = insns.IndexOf(eh.HandlerEnd);

                range.MinIndex = Math.Min(tryStartIndex, handlerStartIndex);
                range.MaxIndex = Math.Max(tryEndIndex, handlerEndIndex);

                // Skip any handlers that span or contain the region we're extracting
                if (range.MinIndex <= firstIndex)
                    continue;
                if (range.MaxIndex >= lastIndex)
                    continue;

                ranges.Add(range);
            }

            var pairs = new Dictionary<Instruction, Instruction> ();
            var key = "extracted(" + targetMethod.DeclaringType?.Name + "." + targetMethod.Name + ") ";

            // Scan the range we're extracting and prepare to erase it after the clone
            // We need to do this now because the clone process can mutate the instructions being copied (yuck)
            for (int i = firstIndex; i <= lastIndex; i++) {
                var oldInsn = insns[i];
                
                if (oldInsn.OpCode.Code == Code.Nop)
                    continue;

                var isExceptionControlFlow = (oldInsn.OpCode.Code == Code.Rethrow) ||
                    (oldInsn.OpCode.Code == Code.Leave) || (oldInsn.OpCode.Code == Code.Leave_S);

                if (preserveControlFlow && isExceptionControlFlow)
                    continue;

                var nopText = oldInsn.OpCode.Code == Code.Endfilter
                    ? "extracted endfilter"
                    : key + oldInsn.ToString();
                var newInsn = Nop(nopText);
                pairs.Add(oldInsn, newInsn);
            }

            CloneInstructions (
				sourceMethod, fakeThis, firstIndex, lastIndex - firstIndex + 1, 
				targetInsns, variableMapping, typeMapping, ranges, filter
			);

			CleanMethodBody (targetMethod, sourceMethod, false);

            for (int i = firstIndex; i <= lastIndex; i++) {
                var oldInsn = insns[i];
                Instruction newInsn;
                if (!pairs.TryGetValue(oldInsn, out newInsn))
                    continue;

                insns[i] = newInsn;
            }

            PatchMany(sourceMethod, context, pairs);

			CleanMethodBody (targetMethod, sourceMethod, false);

            // Copy over any exception handlers that were contained by the source range, remapping
            //  the start/end instructions of the handler and try blocks appropriately post-transform
            foreach (var range in ranges) {
                if (
                    (range.Handler.HandlerType != ExceptionHandlerType.Catch) &&
                    (range.Handler.HandlerType != ExceptionHandlerType.Finally)
                )
                    continue;

                var newEh = new ExceptionHandler (range.Handler.HandlerType) {
                    CatchType = range.Handler.CatchType,
                    FilterStart = null,
                    HandlerType = range.Handler.HandlerType,
                    HandlerStart = range.NewHandlerStart,
                    HandlerEnd = range.NewHandlerEnd,
                    TryStart = range.NewTryStart,
                    TryEnd = range.NewTryEnd
                };

                targetMethod.Body.ExceptionHandlers.Add(newEh);

                // Since the handler was copied over, we want to remove it from the source, 
                //  because replacing the source instruction range with nops has corrupted
                //  any remaining catch or filter blocks
                sourceMethod.Body.ExceptionHandlers.Remove(range.Handler);
            }

			return variableMapping;
		}

        public class RewriteContext {
            public List<InstructionPair> Pairs;
            public List<ExcGroup> NewGroups = new List<ExcGroup>();
            public List<FilterToInsert> FiltersToInsert = new List<FilterToInsert>();
        }

		public class ExcGroup {
            private static int NextID = 0;

            public readonly int ID;
			public Instruction TryStart, TryEnd, TryEndPredecessor, OriginalTryEndPredecessor;
			public List<ExcBlock> Blocks = new List<ExcBlock> ();
			internal Instruction FirstPushInstruction;

            public ExcGroup () {
                ID = NextID++;
            }

            public override string ToString () {
                return $"Group #{ID}";
            }
		}

		public class ExcBlock {
            private static int NextID = 0;

            public readonly int ID;
			public bool IsCatchAll;

			public ExceptionHandler Handler;
			public TypeDefinition FilterType;
			public VariableDefinition FilterVariable;
			public MethodDefinition CatchMethod, FilterMethod;
			public Instruction FirstFilterInsn;
            public List<Instruction> LeaveTargets;
			public Dictionary<object, object> Mapping;

            public ExcBlock () {
                ID = NextID++;
            }

            public override string ToString () {
                return $"Handler #{ID} {(FilterMethod ?? CatchMethod).FullName}";
            }
        }

		public class InstructionPair {
			public class Comparer : IEqualityComparer<InstructionPair> {
				public bool Equals (InstructionPair lhs, InstructionPair rhs) 
				{
					return lhs.Equals (rhs);
				}

				public int GetHashCode (InstructionPair ip) 
				{
					return ip.GetHashCode ();
				}
			}

			public Instruction A, B;

			public override int GetHashCode () 
			{
				return (A?.GetHashCode () ^ B?.GetHashCode ()) ?? 0;
			}

			public bool Equals (InstructionPair rhs) 
			{
				return (A == rhs.A) && (B == rhs.B);
			}

			public override bool Equals (object o) 
			{
				var ip = o as InstructionPair;
				if (ip == null)
					return false;
				return Equals (ip);
			}

            public override string ToString () {
                return $"{{{A} {B}}}";
            }
        }

		public class FilterToInsert {
			public TypeDefinition Type;
			public ExceptionHandler Handler;

			public bool Equals (FilterToInsert rhs)
			{
				return (Type == rhs.Type) && (Handler == rhs.Handler);
			}

			public override int GetHashCode () 
			{
				return (Type?.GetHashCode () ^ Handler?.GetHashCode ()) ?? 0;
			}

			public override bool Equals (object o) 
			{
				var fk = o as FilterToInsert;
				if (fk == null)
					return false;
				return Equals (fk);
			}
		}

		private int RewriteMethod (MethodDefinition method)
		{
			if (!method.HasBody)
				return 0;

			if (method.Body.ExceptionHandlers.Count == 0)
				return 0;

			if (!method.Body.ExceptionHandlers.Any (eh => eh.FilterStart != null))
				return 0;

			if (!Options.EnableGenerics) {
				if (method.HasGenericParameters || method.DeclaringType.HasGenericParameters) {
					var msg = $"Method {method.FullName} contains an exception filter and generics are disabled";
					if (Options.AbortOnError)
						throw new Exception (msg);

					Console.Error.WriteLine (msg);
                    // If abortOnError is off we don't want to abort the rewrite operation, it's safe to skip the method
					return 0;
				}
			}

			if (Options.Verbose)
				Console.WriteLine ($"Rewriting {method.FullName}");

			try {
				RewriteMethodImpl (method);
				return 0;
			} catch (Exception exc) {
				Console.Error.WriteLine ($"Error rewriting {method.FullName}:");
				Console.Error.WriteLine (exc);

				if (Options.AbortOnError)
					throw;
				else
					return 1;
			}
		}

		private void RewriteMethodImpl (MethodDefinition method) {
            if (
                false &&
                !method.FullName.Contains("NestedFiltersIn") && 
                !method.FullName.Contains("Lopsided") &&
                !method.FullName.Contains("TestReturns") &&
                !method.FullName.Contains("TestReturnValue")
            )
                return;

            // Clean up the method body and verify it before rewriting so that any existing violations of
            //  expectations aren't erroneously blamed on later transforms
            CleanMethodBody(method, null, true);

            var efilt = GetExceptionFilter(method.Module);
            var excType = GetException(method.Module);
            TypeDefinition closureTypeDefinition;
            TypeReference closureTypeReference;

            var fakeThis = method.IsStatic
                ? null
                : new ParameterDefinition("__this", ParameterAttributes.None, method.DeclaringType);

            var filterReferencedVariables = new HashSet<VariableReference>();
            var filterReferencedArguments = new HashSet<ParameterReference>();
            CollectReferencedLocals(method, fakeThis, filterReferencedVariables, filterReferencedArguments);

            var closure = ConvertToClosure(
                method, fakeThis, filterReferencedVariables, filterReferencedArguments,
                out closureTypeDefinition, out closureTypeReference
            );

            CleanMethodBody(method, null, true);

            var insns = method.Body.Instructions;
            /*
            insns.Insert (0, Nop ("header"));
            insns.Append (Nop ("footer"));
            */

            ExtractFiltersAndCatchBlocks (method, efilt, fakeThis, closure, insns);

            StripUnreferencedNops (method);

            CleanMethodBody (method, null, true);

            // FIXME: Cecil currently throws inside the native PDB writer on methods we've modified
            //  presumably because we need to manually update the debugging information after removing
            //  instructions from the method body.
            method.DebugInformation = null;
        }

        private void StripUnreferencedNops (MethodDefinition method) {
            var referenced = new HashSet<Instruction> ();

            foreach (var eh in method.Body.ExceptionHandlers) {
                referenced.Add (eh.HandlerStart);
                referenced.Add (eh.HandlerEnd);
                referenced.Add (eh.FilterStart);
                referenced.Add (eh.TryStart);
                referenced.Add (eh.TryEnd);
            }

            var insns = method.Body.Instructions;
            foreach (var insn in insns) {
                var operand = insn.Operand as Instruction;
                if (operand != null)
                    referenced.Add (operand);
            }

            var old = insns.ToArray ();
            insns.Clear ();

            foreach (var insn in old) {
                if ((insn.OpCode.Code == Code.Nop) && !referenced.Contains (insn))
                    continue;

                insns.Add (insn);
            }
        }

        private static ILookup<InstructionPair, ExceptionHandler> GetHandlersByTry (MethodDefinition method) {
            return method.Body.ExceptionHandlers.ToLookup(
                eh => {
                    var p = new InstructionPair { A = eh.TryStart, B = eh.TryEnd };
                    return p;
                },
                new InstructionPair.Comparer()
            );
        }

        private static IOrderedEnumerable<IGrouping<InstructionPair, ExceptionHandler>> GetOrderedFilters (MethodDefinition method) {
            var handlersByTry = GetHandlersByTry(method);

            return handlersByTry.Where(g =>
               g.Any(eh => eh.HandlerType == ExceptionHandlerType.Filter)
            // Sort the groups such that the smallest ones come first. This ensures that for
            //  nested filters we process the innermost filters first.
            ).OrderBy(g => {
                return g.Key.B.Offset - g.Key.A.Offset;
            });
        }

        private void ExtractFiltersAndCatchBlocks (
            MethodDefinition method, TypeReference efilt, 
            ParameterDefinition fakeThis, VariableDefinition closure, 
            Collection<Instruction> insns
        ) {
            var groups = GetOrderedFilters (method).ToList ();
            var pairs = (from k in groups select k.Key).ToList ();
            var context = new RewriteContext { Pairs = pairs };

            var deadHandlers = new List<ExceptionHandler>();

            var excGroups = (from @group in groups
                             let a = @group.Key.A
                             let b = @group.Key.B
                             let endIndex = insns.IndexOf(b)
                             let predecessor = insns[endIndex - 1]
                             select new {
                                 @group,
                                 excGroup = new ExcGroup {
                                     TryStart = a,
                                     OriginalTryEndPredecessor = predecessor,
                                     TryEndPredecessor = predecessor,
                                     TryEnd = b,
                                 },
                                 endIndex                                 
                             }).ToList();

            foreach (var eg in excGroups)
                context.NewGroups.Add(eg.excGroup);

            foreach (var eg in excGroups) {
                var excGroup = eg.excGroup;
                Instruction exitPoint;

                switch (excGroup.OriginalTryEndPredecessor.OpCode.Code) {
                    case Code.Throw:
                    case Code.Rethrow:
                        exitPoint = null;
                        break;
                    case Code.Leave:
                    case Code.Leave_S:
                        exitPoint = (excGroup.TryEndPredecessor.Operand as Instruction) ?? 
                            (excGroup.OriginalTryEndPredecessor.Operand as Instruction);
                        break;
                    default:
                        throw new Exception("Try block does not end with a leave or throw instruction");
                        break;
                }

                foreach (var eh in eg.group) {
                    var catchBlock = ExtractCatch (method, eh, closure, fakeThis, excGroup, context);
                    excGroup.Blocks.Add (catchBlock);

                    if (eh.FilterStart != null)
                        ExtractFilter (method, eh, closure, fakeThis, excGroup, context, catchBlock);
                }

                var teardownEnclosureLeaveTarget = Nop ("Leave target outside of teardown");
                var teardownEnclosureLeave = Instruction.Create (OpCodes.Leave, teardownEnclosureLeaveTarget);
                var teardownPrologue = Nop ("Beginning of teardown");
                var teardownEpilogue = Nop ("End of teardown");

                {
                    // Upon block exit we need to deactivate all our filters. We create teardown code
                    //  and inject it at the exit point for the try block to ensure we tear down before
                    //  anything else happens.

                    var teardownInstructions = new List<Instruction> { teardownEnclosureLeave, teardownPrologue };
                    foreach (var eh in excGroup.Blocks.ToArray ().Reverse ()) {
                        if (eh.FilterType == null)
                            continue;

                        teardownInstructions.Add (Instruction.Create (OpCodes.Ldloc, eh.FilterVariable));
                        teardownInstructions.Add (Instruction.Create (OpCodes.Castclass, efilt));
                        teardownInstructions.Add (Instruction.Create (OpCodes.Call, new MethodReference (
                                "Pop", method.Module.TypeSystem.Void, efilt
                        ) { HasThis = false, Parameters = {
                                new ParameterDefinition (efilt)
                        } }));
                    }

                    teardownInstructions.Add (Instruction.Create (OpCodes.Endfinally));
                    teardownInstructions.Add (teardownEpilogue);
                    teardownInstructions.Add (teardownEnclosureLeaveTarget);

                    int insertOffset;
                    if (exitPoint != null) {
                        insertOffset = insns.IndexOf (exitPoint);
                        if (insertOffset < 0)
                            throw new Exception ("Exit point not found");
                    } else {
                        var nextInsn = (from eh in eg.@group orderby eh.HandlerEnd.Offset descending select eh.HandlerEnd).First();
                        insertOffset = insns.IndexOf (nextInsn);
                    }
                    InsertOps (insns, insertOffset, teardownInstructions.ToArray());

                    // exitPoint = teardownPrologue;
                }

                var newHandler = new List<Instruction> ();
                var newStart = Nop ("Constructed handler start");

                newHandler.Add (newStart);
                ConstructNewExceptionHandler (method, eg.@group, excGroup, newHandler, closure, exitPoint);

                // We'd put a helpful nop here but peverify gets angry
                var newEnd = newHandler[newHandler.Count - 1];

                var targetIndex = insns.IndexOf(excGroup.TryEndPredecessor);
                if (targetIndex < 0)
                    throw new Exception ("Failed to find TryEndPredecessor");
                targetIndex += 1;

                InsertOps (insns, targetIndex, newHandler.ToArray());

                var newEh = new ExceptionHandler (ExceptionHandlerType.Catch) {
                    CatchType = method.Module.TypeSystem.Object,
                    HandlerStart = newStart,
                    HandlerEnd = newEnd,
                    TryStart = excGroup.TryStart,
                    TryEnd = newStart,
                    FilterStart = null
                };
                method.Body.ExceptionHandlers.Add (newEh);

                // Ensure we initialize and activate all exception filters for the try block before entering it
                foreach (var eh in excGroup.Blocks) {
                    if (eh.FilterType == null)
                        continue;

                    var filterInitInstructions = InitializeExceptionFilter(method, eh.FilterType, eh.FilterVariable, closure);
                    var insertOffset = insns.IndexOf(excGroup.TryStart);
                    InsertOps(insns, insertOffset, filterInitInstructions);
                }

                // Wrap everything in a try/finally to ensure that the exception filters are deactivated even if
                //  we throw or return
                var teardownEh = new ExceptionHandler(ExceptionHandlerType.Finally) {
                    TryStart = excGroup.TryStart,
                    TryEnd = teardownPrologue,
                    HandlerStart = teardownPrologue,
                    HandlerEnd = teardownEpilogue
                };

                method.Body.ExceptionHandlers.Add (teardownEh);

                foreach (var eh in eg.@group)
                    method.Body.ExceptionHandlers.Remove(eh);
            }

            foreach (var eg in context.NewGroups) {
                foreach (var eb in eg.Blocks) {
                    if (eb.FilterMethod != null)
                        StripUnreferencedNops(eb.FilterMethod);
                    if (eb.CatchMethod != null)
                        StripUnreferencedNops(eb.CatchMethod);
                }
            }
        }

        private Instruction[] InitializeExceptionFilter (
            MethodDefinition method, 
            TypeDefinition filterType, 
            VariableDefinition filterVariable,
            VariableDefinition closureVariable
        ) {
            var efilt = GetExceptionFilter (method.Module);
            var skipInit = Nop ("Skip initializing filter " + filterType.Name);

            var result = new Instruction[] {
                // If the filter is already initialized (we're running in a loop, etc) don't create a new instance
                Nop ("Initializing filter " + filterType.Name),
				Instruction.Create (OpCodes.Ldloc, filterVariable),
                Instruction.Create (OpCodes.Brtrue, skipInit),

                // Create a new instance of the filter
                Instruction.Create (OpCodes.Newobj, filterType.Methods.First (m => m.Name == ".ctor")),
                Instruction.Create (OpCodes.Stloc, filterVariable),
				// Store the closure into the filter instance so it can access locals
				Instruction.Create (OpCodes.Ldloc, filterVariable),
                Instruction.Create (OpCodes.Ldloc, closureVariable),
                Instruction.Create (OpCodes.Stfld, filterType.Fields.First (m => m.Name == "closure")),
                skipInit,

				// Then call Push on the filter instance to activate it
				Instruction.Create (OpCodes.Ldloc, filterVariable),
                Instruction.Create (OpCodes.Castclass, efilt),
                Instruction.Create (OpCodes.Call, new MethodReference (
                        "Push", method.Module.TypeSystem.Void, efilt
                ) { HasThis = false, Parameters = {
                        new ParameterDefinition (efilt)
                } }),
            };

            return result;
        }

        private void ConstructNewExceptionHandler (
            MethodDefinition method, IGrouping<InstructionPair, ExceptionHandler> group, 
            ExcGroup excGroup, List<Instruction> newInstructions, VariableDefinition closureVar,
            Instruction exitPoint
        ) {
            var excVar = new VariableDefinition (method.Module.TypeSystem.Object);
            method.Body.Variables.Add(excVar);

            newInstructions.Add (Instruction.Create (OpCodes.Stloc, excVar));

            excGroup.Blocks.Sort(
                (lhs, rhs) => {
                    var l = (lhs.FilterMethod != null) ? 0 : 1;
                    var r = (rhs.FilterMethod != null) ? 0 : 1;
                    return l.CompareTo(r);
                }
            );

            var hasFallthrough = excGroup.Blocks.Any(h => h.FilterMethod == null);
            var efilt = GetExceptionFilter (method.Module);

            foreach (var h in excGroup.Blocks) {
                var callCatchPrologue = Nop ("Before call catch " + h.CatchMethod.Name);
                var callCatchEpilogue = Nop ("After call catch " + h.CatchMethod.Name);

                newInstructions.Add (callCatchPrologue);

                if (h.FilterMethod != null) {
                    // Invoke the filter method and skip past the catch if it rejected the exception
                    var callFilterInsn = Instruction.Create (OpCodes.Call, h.FilterMethod);
                    newInstructions.Add (Instruction.Create (OpCodes.Ldloc, h.FilterVariable));
                    newInstructions.Add (Instruction.Create (OpCodes.Castclass, efilt));
                    newInstructions.Add (Instruction.Create (OpCodes.Ldloc, excVar));
                    var mref = new MethodReference(
                        "ShouldRunHandler", method.Module.TypeSystem.Boolean, efilt
                    ) {
                        HasThis = true,
                        Parameters = {
                            new ParameterDefinition (method.Module.TypeSystem.Object)
                        }
                    };
                    newInstructions.Add (Instruction.Create (OpCodes.Call, method.Module.ImportReference(mref)));
                    newInstructions.Add (Instruction.Create (OpCodes.Brfalse, callCatchEpilogue));
                } else if (!h.IsCatchAll) {
                    // Skip past the catch if the exception is not an instance of the catch type
                    newInstructions.Add (Instruction.Create (OpCodes.Ldloc, excVar));
                    newInstructions.Add (Instruction.Create (OpCodes.Isinst, h.Handler.CatchType));
                    newInstructions.Add (Instruction.Create (OpCodes.Brfalse, callCatchEpilogue));
                } else {
                    // Never skip the catch, it's a catch-all block.
                }

                if (!h.CatchMethod.IsStatic)
                    newInstructions.Add (Instruction.Create (OpCodes.Ldarg_0));

                newInstructions.Add (Instruction.Create (OpCodes.Ldloc, excVar));
                if (h.Handler.CatchType != null)
                    newInstructions.Add (Instruction.Create (OpCodes.Castclass, h.Handler.CatchType));

                newInstructions.Add (Instruction.Create (OpCodes.Ldloc, closureVar));
                newInstructions.Add (Instruction.Create (OpCodes.Call, h.CatchMethod));

                // Either rethrow or leave depending on the value returned by the handler
                var rethrow = Instruction.Create (OpCodes.Rethrow);
                // Create instructions for handling each possible leave target (in addition to 0 which is rethrow)
                var switchTargets = new Instruction[h.LeaveTargets.Count + 1];
                switchTargets[0] = rethrow;

                for (int l = 0; l < h.LeaveTargets.Count; l++)
                    switchTargets[l + 1] = Instruction.Create (OpCodes.Leave, h.LeaveTargets[l]);

                // Use the return value from the handler to select one of the targets we just created
                newInstructions.Add (Instruction.Create (OpCodes.Switch, switchTargets));

                // After the switch we fall-through to the rethrow, but this shouldn't ever happen
                newInstructions.Add (rethrow);

                // After the fallback, add each of our leave targets.
                // These need to be after the fallthrough so they aren't hit unless targeted by the switch
                // It's okay to add them by themselves since they're all either rethrow or leave opcodes.
                for (int l = 1; l < switchTargets.Length; l++)
                    newInstructions.Add (switchTargets[l]);

                newInstructions.Add (callCatchEpilogue);

                // FIXME: Insert an extra padding rethrow after the epilogue because jumps will target it sometimes :/
                newInstructions.Add (Nop("Padding after epilogue"));
            }
        }

        private bool RewriteSingleFilter (
            MethodDefinition method, TypeReference efilt, 
            ParameterDefinition fakeThis, VariableDefinition closure, 
            VariableDefinition excVar, Collection<Instruction> insns,
            IGrouping<InstructionPair, ExceptionHandler> group,
            List<InstructionPair> pairs
        ) {
            var context = new RewriteContext { Pairs = pairs };
            var newGroups = context.NewGroups;
            var filtersToInsert = context.FiltersToInsert;

            var endIndex = insns.IndexOf(group.Key.B);
            if (endIndex < 0)
                throw new Exception($"End instruction {group.Key.B} not found in method body");
            var excGroup = new ExcGroup {
                TryStart = group.Key.A,
                TryEndPredecessor = insns[endIndex - 1],
                TryEnd = group.Key.B
            };

            newGroups.Add(excGroup);

            /*

            foreach (var eg in newGroups) {
                var finallyInsns = new List<Instruction>();
                finallyInsns.Add (Nop ("Finally header"));

                var hasAnyCatchAll = eg.Handlers.Any(h => h.IsCatchAll);

                foreach (var h in eg.Handlers) {
                    var fv = h.FilterVariable;
                    if (fv != null) {
                        // Create each filter instance at function entry so it's always present during the finally blocks
                        // FIXME: It'd be better to do this right before entering try blocks but doing that precisely is
                        //  complicated
                        InsertOps(insns, 0, new Instruction[] {
                            Instruction.Create (OpCodes.Newobj, h.FilterType.Methods.First (m => m.Name == ".ctor")),
                            Instruction.Create (OpCodes.Stloc, fv),
                        });

                        var filterInitInsns = new Instruction[] {
							// Store the closure into the filter instance so it can access locals
							Instruction.Create (OpCodes.Ldloc, fv),
                            Instruction.Create (OpCodes.Ldloc, closure),
                            Instruction.Create (OpCodes.Stfld, h.FilterType.Fields.First (m => m.Name == "closure")),
							// Then call Push on the filter instance
							Instruction.Create (OpCodes.Ldloc, fv),
                            Instruction.Create (OpCodes.Castclass, efilt),
                            Instruction.Create (OpCodes.Call, new MethodReference (
                                    "Push", method.Module.TypeSystem.Void, efilt
                            ) { HasThis = false, Parameters = {
                                    new ParameterDefinition (efilt)
                            } }),
                            h.Handler.TryStart,
                        };

                        var oldIndex = insns.IndexOf(h.Handler.TryStart);
                        if (oldIndex < 0)
                            throw new Exception($"Handler trystart not found in method body: {h.Handler.TryStart}");
                        var nop = Nop ("filter block start");
                        insns[oldIndex] = nop;
                        Patch(method, context, h.Handler.TryStart, insns[oldIndex]);
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
                        ) {
                            HasThis = false,
                            Parameters = {
                                new ParameterDefinition (efilt)
                        }
                        }));
                    }
                }

                var newHandlerOffset = insns.IndexOf(eg.TryEnd);
                if (newHandlerOffset < 0)
                    throw new Exception($"Handler end instruction {eg.TryEnd} not found in method body");

                var tryExit = insns[newHandlerOffset + 1];
                var newHandlerStart = Nop ("new handler start");
                Instruction newHandlerEnd, handlerFallthroughRethrow;
                handlerFallthroughRethrow = hasAnyCatchAll ? Nop ("fallthrough rethrow (dead)") : Rethrow ("fallthrough rethrow");
                newHandlerEnd = Instruction.Create(OpCodes.Leave, tryExit);

                var handlerBody = new List<Instruction> {
                    newHandlerStart,
                    Instruction.Create (OpCodes.Stloc, excVar)
                };

                var breakOut = Nop ("breakOut");

                foreach (var h in eg.Handlers) {
                    var skip = Nop ("skip");

                    var fv = h.FilterVariable;
                    if (fv != null) {
                        // If we have a filter, check the Result to see if the filter returned execute_handler
                        handlerBody.Add(Instruction.Create (OpCodes.Ldloc, fv));
                        handlerBody.Add(Instruction.Create (OpCodes.Castclass, efilt));
                        handlerBody.Add(Instruction.Create (OpCodes.Ldloc, excVar));
                        var mref = new MethodReference(
                            "ShouldRunHandler", method.Module.TypeSystem.Boolean, efilt
                        ) {
                            HasThis = true,
                            Parameters = {
                            new ParameterDefinition (method.Module.TypeSystem.Object)
                        }
                        };
                        handlerBody.Add(Instruction.Create (OpCodes.Call, method.Module.ImportReference(mref)));
                        handlerBody.Add(Instruction.Create (OpCodes.Brfalse, skip));
                    }

                    var needsTypeCheck = (h.Handler.CatchType != null) && (h.Handler.CatchType.FullName != "System.Object");
                    if (needsTypeCheck) {
                        // If the handler has a type check do an isinst to check whether it should run
                        handlerBody.Add(Instruction.Create (OpCodes.Ldloc, excVar));
                        handlerBody.Add(Instruction.Create (OpCodes.Isinst, h.Handler.CatchType));
                        handlerBody.Add(Instruction.Create (OpCodes.Brfalse, skip));
                    }

                    // Load anything the catch referenced onto the stack. If it wasn't a byref type,
                    //  we need to load its address because we convert all referenced values into refs
                    //  (so that the catch can modify them)
                    foreach (var a in h.CatchReferencedArguments)
                        if (a.ParameterType.IsByReference)
                            handlerBody.Add(Instruction.Create (OpCodes.Ldarg, (ParameterDefinition)a));
                        else
                            handlerBody.Add(Instruction.Create (OpCodes.Ldarga, (ParameterDefinition)a));

                    foreach (var v in h.CatchReferencedVariables)
                        if (v.VariableType.IsByReference)
                            handlerBody.Add(Instruction.Create (OpCodes.Ldloc, (VariableDefinition)v));
                        else
                            handlerBody.Add(Instruction.Create (OpCodes.Ldloca, (VariableDefinition)v));

                    // Now load the exception
                    handlerBody.Add(Instruction.Create (OpCodes.Ldloc, excVar));
                    // If the isinst passed we need to cast the exception value to the appropriate type
                    if (needsTypeCheck)
                        handlerBody.Add(Instruction.Create (OpCodes.Castclass, h.Handler.CatchType));

                    // Run the handler, then if it returns true, throw.
                    // If it returned false, we leave the entire handler.
                    handlerBody.Add(Instruction.Create (OpCodes.Ldloc, closure));
                    handlerBody.Add(Instruction.Create (OpCodes.Call, h.Method));
                    handlerBody.Add(Instruction.Create (OpCodes.Brfalse, newHandlerEnd));
                    handlerBody.Add(Rethrow ("end of handler"));
                    handlerBody.Add(skip);
                }

                handlerBody.Add(handlerFallthroughRethrow);
                handlerBody.Add(newHandlerEnd);

                InsertOps(insns, newHandlerOffset + 1, handlerBody.ToArray());

                var originalExitPoint = insns[insns.IndexOf(newHandlerEnd) + 1];
                Instruction handlerEnd = Nop ("handlerEnd");

				// CHANGE GROUP: The preFinallyBr and handler end stuff changed a lot here

                Instruction preFinallyBr;
                // If there was a catch-all block we can jump to the original exit point, because
                //  the catch-all block handler would have returned 1 to trigger a rethrow - it didn't.
                // If no catch-all block existed we need to rethrow at the end of our coalesced handler.
                if (hasAnyCatchAll)
                    preFinallyBr = Instruction.Create(OpCodes.Leave, originalExitPoint);
                else
                    preFinallyBr = Rethrow ("preFinallyBr");

                if (finallyInsns.Count == 0)
                    handlerEnd = originalExitPoint;

                if (finallyInsns.Count > 0) {
                    finallyInsns.Add(Instruction.Create(OpCodes.Endfinally));

                    var newLeave = Instruction.Create(OpCodes.Leave, originalExitPoint);
                    if (!hasAnyCatchAll)
                        newLeave = Rethrow ("newLeave");

                    var originalExitIndex = insns.IndexOf(originalExitPoint);
                    InsertOps(insns, originalExitIndex, finallyInsns.ToArray());

                    var newFinally = new ExceptionHandler(ExceptionHandlerType.Finally) {
                        TryStart = eg.FirstPushInstruction,
                        TryEnd = finallyInsns[0],
                        HandlerStart = finallyInsns[0],
                        HandlerEnd = originalExitPoint
                    };
                    method.Body.ExceptionHandlers.Add(newFinally);

                    InsertOps(
                        insns, insns.IndexOf(finallyInsns[0]),
                        new[] {
                            preFinallyBr, newLeave, handlerEnd
                        }
                    );
                }

                var newEh = new ExceptionHandler(ExceptionHandlerType.Catch) {
                    TryStart = eg.TryStart,
                    TryEnd = newHandlerStart,
                    HandlerStart = newHandlerStart,
                    HandlerEnd = handlerEnd,
                    CatchType = method.Module.TypeSystem.Object,
                };
                method.Body.ExceptionHandlers.Add(newEh);

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
            */

            return true;
        }

        private void CollectReferencedLocals (
			MethodDefinition method, ParameterDefinition fakeThis, 
			HashSet<VariableReference> referencedVariables, HashSet<ParameterReference> referencedArguments
		) {
			foreach (var eh in method.Body.ExceptionHandlers) {
				if (eh.FilterStart != null)
					CollectReferencedLocals (method, fakeThis, eh.FilterStart, eh.HandlerStart, referencedVariables, referencedArguments);

                // Also collect anything referenced by handlers because they get hoisted out
                // FIXME: Only do this to blocks that have a filter hanging off them
                CollectReferencedLocals (method, fakeThis, eh.HandlerStart, eh.HandlerEnd, referencedVariables, referencedArguments);
            }
		}

		private void CollectReferencedLocals (
			MethodDefinition method, ParameterDefinition fakeThis, Instruction first, Instruction last, 
			HashSet<VariableReference> referencedVariables, HashSet<ParameterReference> referencedArguments
		) {
			var insns = method.Body.Instructions;
			int i = insns.IndexOf (first), i2 = insns.IndexOf (last);
			if ((i < 0) || (i2 < 0))
				throw new ArgumentException ("First and/or last instruction(s) not found in method body");

			for (; i <= i2; i++) {
				var insn = insns[i];
				var vd = (insn.Operand as VariableReference) 
					?? LookupNumberedVariable (insn.OpCode.Code, method.Body.Variables); 
				var pd = insn.Operand as ParameterReference
					?? LookupNumberedArgument (insn.OpCode.Code, fakeThis, method.Parameters);

				// FIXME
				if (vd?.VariableType.FullName.Contains ("__closure") ?? false)
					continue;
				if (pd?.Name.Contains ("__closure") ?? false)
					continue;

				if (vd != null)
					referencedVariables.Add (vd);
				if (pd != null)
					referencedArguments.Add (pd);
			}
		}

        private void CheckInRange (Instruction insn, MethodDefinition method, MethodDefinition oldMethod, List<Instruction> removedInstructions) {
            if (insn == null)
                return;

            var s = ((removedInstructions != null) && removedInstructions.Contains(insn))
                ? "Removed instruction"
                : "Instruction";

            // FIXME
            if (false) {
			    if (method.Body.Instructions.IndexOf (insn) < 0)
				    throw new Exception ($"{s} {insn} is missing from method {method.FullName}");
			    else if (oldMethod != null && oldMethod.Body.Instructions.IndexOf (insn) >= 0)
				    throw new Exception ($"{s} {insn} is present in old method for method {method.FullName}");
            }
        }

		private void CleanMethodBody (MethodDefinition method, MethodDefinition oldMethod, bool verify, List<Instruction> removedInstructions = null) 
		{
			var insns = method.Body.Instructions;

            // NOTE: Disabling this will break the leave target ordering check
            // FIXME: Turning this on breaks everything? Why is this possible?
            bool renumber = false;

            int offset = 0;
			foreach (var i in insns) {
                if (renumber)
				    i.Offset = offset;
                offset += i.GetSize();
            }

			for (int idx = 0; idx < insns.Count; idx++) {
                var i = insns[idx];

				OpCode newOpcode;
				if (ShortFormRemappings.TryGetValue (i.OpCode.Code, out newOpcode))
					i.OpCode = newOpcode;

                switch (i.OpCode.Code) {
                    case Code.Rethrow: {
                        bool foundRange = false;

                        foreach (var eh in method.Body.ExceptionHandlers) {
                            if (eh.HandlerType == ExceptionHandlerType.Finally)
                                continue;

                            int startIndex = insns.IndexOf(eh.HandlerStart),
                                endIndex = insns.IndexOf(eh.HandlerEnd);

                            if ((idx >= startIndex) && (idx <= endIndex)) {
                                foundRange = true;
                                break;
                            }
                        }

                        if (!foundRange && false)
                            throw new Exception($"Found rethrow instruction outside of catch block: {i}");

                        break;
                    }
                }

				if (!verify)
					continue;

				var opInsn = i.Operand as Instruction;
                var opInsns = i.Operand as Instruction[];
				var opArg = i.Operand as ParameterDefinition;
				var opVar = i.Operand as VariableDefinition;

				if (opInsn != null) {
                    CheckInRange(opInsn, method, oldMethod, removedInstructions);

                    if ((i.OpCode.Code == Code.Leave) || (i.OpCode.Code == Code.Leave_S)) {
                        if (renumber && (i.Offset > opInsn.Offset))
                            throw new Exception ($"Leave target {opInsn} precedes leave instruction");
                    }
				} else if (opArg != null) {
					if ((opArg.Name == "__this") && method.HasThis) {
						// HACK: method.Body.ThisParameter is unreliable for confusing reasons, and isn't
						//  present in .Parameters so just ignore the check here
					} else if (method.Parameters.IndexOf (opArg) < 0) {
						throw new Exception ($"Parameter {opArg.Name} for opcode {i} is missing for method {method.FullName}");
					} else if (oldMethod != null && oldMethod.Parameters.IndexOf (opArg) >= 0)
						throw new Exception ($"Parameter {opArg.Name} for opcode {i} is present in old method for method {method.FullName}");
				} else if (opVar != null) {
					if (method.Body.Variables.IndexOf (opVar) < 0)
						throw new Exception ($"Local {opVar} for opcode {i} is missing for method {method.FullName}");
					else if (oldMethod != null && oldMethod.Body.Variables.IndexOf (opVar) >= 0)
						throw new Exception ($"Local {opVar} for opcode {i} is present in old method for method {method.FullName}");
				} else if (opInsns != null) {
                    foreach (var target in opInsns)
                        CheckInRange(target, method, oldMethod, removedInstructions);
                }
			}

            offset = 0;
			foreach (var i in insns) {
                if (renumber)
				    i.Offset = offset;
                offset += i.GetSize();
            }

			if (verify)
			foreach (var p in method.Parameters)
				if (p.Index != method.Parameters.IndexOf (p))
					throw new Exception ($"parameter index mismatch for method {method.FullName}");

			if (verify)
			foreach (var v in method.Body.Variables)
				if (v.Index != method.Body.Variables.IndexOf (v))
					throw new Exception ($"variable index mismatch for method {method.FullName}");

            if (verify)
            foreach (var eh in method.Body.ExceptionHandlers) {
                CheckInRange(eh.HandlerStart, method, oldMethod, removedInstructions);
                CheckInRange(eh.HandlerEnd, method, oldMethod, removedInstructions);
                CheckInRange(eh.FilterStart, method, oldMethod, removedInstructions);
                CheckInRange(eh.TryStart, method, oldMethod, removedInstructions);
                CheckInRange(eh.TryEnd, method, oldMethod, removedInstructions);

                if (eh.TryStart != null) {
                    var tryStartIndex = insns.IndexOf(eh.TryStart);
                    var tryEndIndex = insns.IndexOf(eh.TryEnd);
                    if (tryEndIndex <= tryStartIndex)
                        throw new Exception($"Try block contains no instructions at {eh.TryStart}");
                }
            }
		}
        
		private Instruction CreateRemappedInstruction (
			object oldOperand, OpCode oldCode, object operand
		) {
			if (operand == null)
				throw new ArgumentNullException ("operand");

			OpCode code = oldCode;
			if (
				(operand != null) && 
				(oldOperand != null) &&
				(operand.GetType () != oldOperand.GetType ())
			) {
				if (!LocalParameterRemappings.TryGetValue (oldCode.Code, out code))
					throw new Exception (oldCode.ToString ());
			}

			if (operand is ParameterDefinition)
				return Instruction.Create (code, (ParameterDefinition)operand);
			else if (operand is VariableDefinition)
				return Instruction.Create (code, (VariableDefinition)operand);
			else
				throw new Exception (operand.ToString ());
		}

        private object RemapOperandForClone<T, U> (
            object operand,
			Dictionary<object, object> variableMapping,
			Dictionary<T, U> typeMapping
        )
			where T : TypeReference
			where U : TypeReference
		{
			object newOperand = operand;
			if (variableMapping != null && variableMapping.ContainsKey (operand))
				newOperand = variableMapping[operand];
			else if (typeMapping != null) {
				var operandTr = operand as T;
				if (operandTr != null && typeMapping.ContainsKey (operandTr))
					newOperand = typeMapping[operandTr];
			}
            return newOperand;
        }

		private Instruction CloneInstruction<T, U> (
			Instruction i, 
			ParameterDefinition fakeThis,
			MethodDefinition method,
			Dictionary<object, object> variableMapping,
			Dictionary<T, U> typeMapping
		)
			where T : TypeReference
			where U : TypeReference
		{
			object operand = i.Operand ??
				(object)LookupNumberedVariable (i.OpCode.Code, method.Body.Variables) ??
				(object)LookupNumberedArgument (i.OpCode.Code, fakeThis, method.Parameters);

			var code = i.OpCode;
			if (Denumberings.ContainsKey (i.OpCode.Code))
				code = Denumberings[i.OpCode.Code];

			if (operand == null)
				return Instruction.Create (code);

            var newOperand = RemapOperandForClone(operand, variableMapping, typeMapping);

            if (code.Code == Code.Nop) {
                var result = Instruction.Create(OpCodes.Nop);
                // HACK: Manually preserve any operand that was tucked inside the nop for bookkeeping
                result.Operand = operand;
                return result;
            } else if (code.Code == Code.Rethrow) {
                var result = Instruction.Create(OpCodes.Rethrow);
                // HACK: Manually preserve any operand that was tucked inside the rethrow for bookkeeping
                result.Operand = operand;
                return result;
            } else if (newOperand is FieldReference) {
				FieldReference fref = newOperand as FieldReference;
				return Instruction.Create (code, fref);
			} else if (newOperand is TypeReference) {
				TypeReference tref = newOperand as TypeReference;
				return Instruction.Create (code, tref);
			} else if (newOperand is TypeDefinition) {
				TypeDefinition tdef = newOperand as TypeDefinition;
				return Instruction.Create (code, tdef);
			} else if (newOperand is MethodReference) {
				MethodReference mref = newOperand as MethodReference;
				return Instruction.Create (code, mref);
			} else if (newOperand is Instruction) {
				var insn = newOperand as Instruction;
				return Instruction.Create (code, insn);
			} else if (newOperand is string) {
				var s = newOperand as string;
				return Instruction.Create (code, s);
			} else if (newOperand is VariableReference) {
				var v = newOperand as VariableReference;
				if (operand.GetType () != v.GetType ())
					return CreateRemappedInstruction (operand, code, newOperand);
				else
					return Instruction.Create (code, (VariableDefinition)v);
			} else if (newOperand is ParameterDefinition) {
				var p = newOperand as ParameterDefinition;
				if (operand.GetType () != p.GetType ())
					return CreateRemappedInstruction (operand, code, newOperand);
				else
					return Instruction.Create (code, p);
			} else if ((newOperand != null) && (newOperand.GetType ().IsValueType)) {
				var m = typeof(Instruction).GetMethod ("Create", new Type[] {
					code.GetType(), newOperand.GetType()
				});
				if (m == null)
				throw new Exception("Could not find Instruction.Create overload for operand " + newOperand.GetType ().Name);
				return (Instruction)m.Invoke (null, new object[] {
					code, newOperand
				});
            } else if (newOperand is Instruction[]) {
                var insns = (Instruction[])newOperand;
                var newInsns = new Instruction[insns.Length];
                insns.CopyTo (newInsns, 0);
                return Instruction.Create (code, newInsns);
			} else if (newOperand != null) {
				throw new NotImplementedException (i.OpCode.ToString () + " " + newOperand.GetType().FullName);
			} else {
				throw new NotImplementedException (i.OpCode.ToString ());
			}
		}

		private void CloneInstructions<T, U> (
			MethodDefinition sourceMethod,
			ParameterDefinition fakeThis,
			int sourceIndex, int count,
			Mono.Collections.Generic.Collection<Instruction> target,
			Dictionary<object, object> variableMapping,
			Dictionary<T, U> typeMapping,
            List<EhRange> ranges,
            Func<Instruction, EhRange, Instruction[]> filter
		)
			where T : TypeReference
			where U : TypeReference
		{
            var sourceInsns = sourceMethod.Body.Instructions;

			if (sourceIndex < 0)
				throw new ArgumentOutOfRangeException ("sourceIndex");

            var mapping = new Dictionary<Instruction, Instruction> ();

            int newOffset = 0;
			for (int n = 0; n < count; n++) {
                var absoluteIndex = n + sourceIndex;
				var insn = sourceInsns[absoluteIndex];
				var newInsn = CloneInstruction (insn, fakeThis, sourceMethod, variableMapping, typeMapping);

                if (filter != null) {
                    var range = FindRangeForOffset (ranges, absoluteIndex);

                    var filtered = filter (newInsn, range);
                    if (filtered != null) {
                        mapping[insn] = filtered.First();

                        foreach (var filteredInsn in filtered) {
                            filteredInsn.Offset = newOffset;
    				        target.Add (filteredInsn);
                            newOffset += filteredInsn.GetSize();
                        }

                        UpdateRangeReferences (ranges, insn, filtered.First(), filtered.Last());
                    } else {
                        mapping[insn] = newInsn;

                        newInsn.Offset = newOffset;
    				    target.Add (newInsn);
                        newOffset += newInsn.GetSize();

                        UpdateRangeReferences (ranges, insn, newInsn, newInsn);
                    }
                } else {
                    mapping[insn] = newInsn;
                    newInsn.Offset = newOffset;
    				target.Add (newInsn);
                    newOffset += newInsn.GetSize();

                    UpdateRangeReferences (ranges, insn, newInsn, newInsn);
                }
			}

			// Fixup branches
			for (int i = 0; i < target.Count; i++) {
				var insn = target[i];
				var operand = insn.Operand as Instruction;
                var operands = insn.Operand as Instruction[];

                if (operand != null) {
                    Instruction newOperand, newInsn;
				    if (!mapping.TryGetValue(operand, out newOperand)) {
						throw new Exception ("Could not remap instruction operand for " + insn);
				    } else {
					    newInsn = Instruction.Create (insn.OpCode, newOperand);
				    }
    				target[i] = newInsn;
                } else if (operands != null) {
                    for (int j = 0; j < operands.Length; j++) {
                        Instruction newElt, elt = operands[j];
                        if (!mapping.TryGetValue(elt, out newElt))
                            throw new Exception ($"Switch target {elt} not found in table of cloned instructions");

                        operands[j] = newElt;
                    }
                } else
                    continue;
			}
		}

        private void UpdateRangeReference (ref Instruction target, Instruction compareWith, Instruction oldInsn, Instruction newInsn) {
            if (oldInsn != compareWith)
                return;

            target = newInsn;
        }

        private void UpdateRangeReferences (List<EhRange> ranges, Instruction oldInsn, Instruction firstNewInsn, Instruction lastNewInsn) {
            foreach (var range in ranges) {
                UpdateRangeReference(ref range.NewTryStart, range.OldTryStart, oldInsn, firstNewInsn);
                UpdateRangeReference(ref range.NewHandlerStart, range.OldHandlerStart, oldInsn, firstNewInsn);
                UpdateRangeReference(ref range.NewTryEnd, range.OldTryEnd, oldInsn, lastNewInsn);
                UpdateRangeReference(ref range.NewHandlerEnd, range.OldHandlerEnd, oldInsn, lastNewInsn);
            }
        }
    }
}
