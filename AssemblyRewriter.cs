﻿using System;
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
						errorCount += ProcessMethod (meth);
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

                foreach (var h in g.Handlers) {
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
                    body[i] = Instruction.Create (body[i].OpCode, replacement);
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

                foreach (var h in g.Handlers) {
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
				v => new FieldDefinition ("local_" + method.Body.Variables.IndexOf((VariableDefinition)v), FieldAttributes.Public, FilterTypeReference (v.VariableType, functionGpMapping))
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

			CleanMethodBody (method, null, false);

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

				if (insn != result[0]) {
					remapTableFirst[insn] = result[0];
					instructions[i] = result[0];
				}
				for (int j = result.Length - 1; j >= 1; j--)
					instructions.Insert (i + 1, result[j]);

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
				if (!remapTableFirst.TryGetValue (operand, out newOperand))
					continue;

				insn.Operand = newOperand;
			}

			// CHANGE #2: Removed clean
			foreach (var eh in method.Body.ExceptionHandlers) {
				eh.FilterStart = PostFilterRange (remapTableFirst, eh.FilterStart);
				eh.TryStart = PostFilterRange (remapTableFirst, eh.TryStart);
				eh.TryEnd = PostFilterRange (remapTableFirst, eh.TryEnd);
				eh.HandlerStart = PostFilterRange (remapTableFirst, eh.HandlerStart);
				eh.HandlerEnd = PostFilterRange (remapTableFirst, eh.HandlerEnd);
			}

			// CHANGE #3: Added clean
			CleanMethodBody (method, null, false);
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

		private ExcBlock ExtractCatch (
			MethodDefinition method, ExceptionHandler eh, VariableDefinition closure, ParameterDefinition fakeThis, ExcGroup group, RewriteContext context
		) {
			var insns = method.Body.Instructions;
			var closureType = closure.VariableType;

			var catchReferencedVariables = new HashSet<VariableReference> ();
			var catchReferencedArguments = new HashSet<ParameterReference> ();
			CollectReferencedLocals (method, fakeThis, eh.HandlerStart, eh.HandlerEnd, catchReferencedVariables, catchReferencedArguments);

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
			GenerateParameters (catchMethod, catchReferencedArguments, paramMapping, needsLdind);
			GenerateParameters (catchMethod, catchReferencedVariables, paramMapping, needsLdind);
			catchMethod.Parameters.Add (excParam);
			catchMethod.Parameters.Add (closureParam);

			var catchInsns = catchMethod.Body.Instructions;

			var handlerFirstIndex = insns.IndexOf (eh.HandlerStart);
			var handlerLastIndex = insns.IndexOf (eh.HandlerEnd) - 1;

            Console.WriteLine($"Extracting catch [{eh.HandlerStart}] - [{eh.HandlerEnd} - 1]");

			// CHANGE #4: Adding method earlier
			method.DeclaringType.Methods.Add (catchMethod);

            var i1 = insns.IndexOf (eh.HandlerStart);
            var i2 = insns.IndexOf (eh.HandlerEnd);

            var endsWithRethrow = insns[i2].OpCode.Code == Code.Rethrow;

            if (i2 <= i1)
                throw new Exception("Hit beginning of handler while rewinding past rethrows");

			// FIXME: Use generic parameter mapping to replace GP type references
			var newMapping = ExtractRangeToMethod (
				method, catchMethod, fakeThis,
				i1, i2 - 1,
				variableMapping: paramMapping,
				typeMapping: gpMapping,
                context: context,
                // FIXME: Identify when we want to preserve control flow and when we don't
                preserveControlFlow: true,
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

                    if (range == null)
					    switch (insn.OpCode.Code) {
						    case Code.Leave:
						    case Code.Leave_S:
							    return new[] {
								    Instruction.Create (OpCodes.Ldc_I4_0),
								    Instruction.Create (OpCodes.Ret)
							    };
						    case Code.Rethrow:
							    return new[] {
								    Instruction.Create (OpCodes.Ldc_I4_1),
								    Instruction.Create (OpCodes.Ret)
							    };
					    }

                    return null;
				}
			);

			CleanMethodBody (catchMethod, method, false);
            /*
            if (endsWithRethrow) {
                var oldInsn = insns[i2 - 1];
                insns[i2 - 1] = Rethrow ("Preserved rethrow");
                Patch (method, context, oldInsn, insns[i2 - 1]);
            }
            */

            if (catchInsns.Count > 0) {
			    var first = catchInsns[0];

			    InsertOps (
				    catchInsns, 0, new[] {
					    Instruction.Create (OpCodes.Ldarg, excParam)
				    }
			    );

			    FilterRange (catchMethod, 0, catchMethod.Body.Instructions.Count - 1, (i) => {
				    if (needsLdind.Contains (i.Operand)) {
					    if (IsStoreOperation (i.OpCode.Code)) {
						    var operandVariable = i.Operand as VariableReference;
						    var operandParameter = i.Operand as ParameterReference;
						    var operandType = operandVariable?.VariableType ?? operandParameter.ParameterType;
						    var newOperandType = FilterTypeReference (operandType, gpMapping);
						    var byRefNewOperand = newOperandType as ByReferenceType;
						
						    var newTempLocal = new VariableDefinition (byRefNewOperand?.ElementType ?? newOperandType);
						    catchMethod.Body.Variables.Add (newTempLocal);

						    return new[] {
							    Instruction.Create (OpCodes.Stloc, newTempLocal),
							    i.Operand is VariableReference
								    ? Instruction.Create (OpCodes.Ldloc, (VariableDefinition)i.Operand)
								    : Instruction.Create (OpCodes.Ldarg, (ParameterDefinition)i.Operand),
							    Instruction.Create (OpCodes.Ldloc, newTempLocal),
							    Instruction.Create (SelectStindForOperand (i.Operand))
						    };
					    } else
						    return new[] {
							    i,
							    Instruction.Create (SelectLdindForOperand (i.Operand))
						    };
				    } else
					    return null;
			    });

			    CleanMethodBody (catchMethod, method, true);
            }

			var isCatchAll = (eh.HandlerType == ExceptionHandlerType.Catch) && (eh.CatchType?.FullName == "System.Object");
			var handler = new ExcBlock {
				Handler = eh,
				Method = catchMethod,
				IsCatchAll = isCatchAll,
				Mapping = newMapping,
				CatchReferencedVariables = catchReferencedVariables,
				CatchReferencedArguments = catchReferencedArguments
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

		private ExcBlock ExtractFilter (
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
            if (endfilter.OpCode.Code != Code.Endfilter)
                throw new Exception("Filter did not end with an endfilter");

            {
                Console.WriteLine($"Extracting filter [{eh.FilterStart}] - [{endfilter}]");

                var variableMapping = new Dictionary<object, object> ();
			    var newVariables = ExtractRangeToMethod (
				    method, filterMethod, fakeThis, i1, i2 - 1, 
				    variableMapping: variableMapping, typeMapping: gpMapping, 
                    context: context,
                    preserveControlFlow: false
                    /*,
                    newFirstInstruction: Instruction.Create(OpCodes.Pop),
                    newLastInstruction: Instruction.Create(OpCodes.Endfilter)
                    */
			    );
			    var newClosureLocal = (VariableDefinition)newVariables[closure];

			    var filterInsns = filterMethod.Body.Instructions;
                if (filterInsns.Count <= 0)
                    throw new Exception("Filter body was empty");

			    var oldFilterInsn = filterInsns[filterInsns.Count - 1];
                if (oldFilterInsn.OpCode.Code != Code.Endfilter)
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

			var handler = new ExcBlock {
				Handler = eh,
				FilterMethod = filterMethod,
                FilterType = filterTypeDefinition,
                FilterVariable = new VariableDefinition (filterTypeDefinition),
                FirstFilterInsn = eh.FilterStart,
			};

            method.Body.Variables.Add (handler.FilterVariable);

			return handler;
		}

        private class EhRange {
            public ExceptionHandler Handler;
            public int TryStartIndex, TryEndIndex, HandlerStartIndex, HandlerEndIndex;
            public int MinIndex, MaxIndex;
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
			Func<Instruction, Instruction, Instruction> onFailedRemap = null,
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
                if (eh.HandlerType == ExceptionHandlerType.Filter)
                    continue;

                var range = new EhRange {
                    Handler = eh,
                    TryStartIndex = insns.IndexOf(eh.TryStart),
                    TryEndIndex = insns.IndexOf(eh.TryEnd),
                    HandlerStartIndex = insns.IndexOf(eh.HandlerStart),
                    HandlerEndIndex = insns.IndexOf(eh.HandlerEnd)
                };

                range.MinIndex = Math.Min(range.TryStartIndex, range.HandlerStartIndex);
                range.MaxIndex = Math.Max(range.TryEndIndex, range.HandlerEndIndex);

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
                
                // Do not erase existing placeholder nops
                if ((oldInsn.OpCode.Code == Code.Nop) && (oldInsn.Operand != null))
                    continue;
                    // throw new Exception($"Extracting already-extracted instruction {oldInsn}");

                var isExceptionControlFlow = (oldInsn.OpCode.Code == Code.Rethrow) ||
                    (oldInsn.OpCode.Code == Code.Leave) || (oldInsn.OpCode.Code == Code.Leave_S);
               
                var newInsn = Nop(key + oldInsn.ToString());
                pairs.Add(oldInsn, newInsn);
            }

            CloneInstructions (
				sourceMethod, fakeThis, firstIndex, lastIndex - firstIndex + 1, 
				targetInsns, 0, variableMapping, typeMapping, ranges, onFailedRemap, filter
			);

			CleanMethodBody (targetMethod, sourceMethod, false);

            for (int i = firstIndex; i <= lastIndex; i++) {
                var oldInsn = insns[i];
                Instruction newInsn;
                if (!pairs.TryGetValue(oldInsn, out newInsn))
                    continue;

                insns[i] = newInsn;
            }

            // FIXME: Copy over any try/catch blocks from the source range

            PatchMany(sourceMethod, context, pairs);

            StripUnreferencedNops(targetMethod);

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
			public Instruction TryStart, TryEnd, TryEndPredecessor;
			public List<ExcBlock> Handlers = new List<ExcBlock> ();
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
			internal VariableDefinition FilterVariable;
			public MethodDefinition Method, FilterMethod;

			public Instruction FirstFilterInsn;
			internal HashSet<VariableReference> CatchReferencedVariables;
			internal HashSet<ParameterReference> CatchReferencedArguments;
			internal Dictionary<object, object> Mapping;

            public ExcBlock () {
                ID = NextID++;
            }

            public override string ToString () {
                return $"Handler #{ID} {(FilterMethod ?? Method).FullName}";
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

		private int ProcessMethod (MethodDefinition method)
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
				ExtractExceptionFilters (method);
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

		private void ExtractExceptionFilters (MethodDefinition method) {
            // FIXME: Cecil currently throws inside the native PDB writer on methods we've modified
            //  presumably because we need to manually update the debugging information after removing
            //  instructions from the method body.
            method.DebugInformation = null;

            if (!method.FullName.Contains("NestedFiltersIn"))
                return;

            CleanMethodBody(method, null, false);

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

            var excVar = new VariableDefinition(method.Module.TypeSystem.Object);
            method.Body.Variables.Add(excVar);

            var insns = method.Body.Instructions;
            insns.Insert (0, Nop ("header"));
            insns.Append (Nop ("footer"));

            bool iterating = true;
            int passNumber = 0;

            ExtractFiltersAndCatchBlocks (method, efilt, fakeThis, closure, excVar, insns);

            /*
            foreach (var group in groups) {
                iterating = RewriteSingleFilter (method, efilt, fakeThis, closure, excVar, insns, group, pairs);
                passNumber += 1;
                if (!iterating)
                    break;
            }
            */

            StripUnreferencedNops (method);

            CleanMethodBody (method, null, true);
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
            VariableDefinition excVar, Collection<Instruction> insns
        ) {
            var groups = GetOrderedFilters (method).ToList ();
            var pairs = (from k in groups select k.Key).ToList ();
            var context = new RewriteContext { Pairs = pairs };

            foreach (var group in groups) {
                var endIndex = insns.IndexOf(group.Key.B);
                if (endIndex < 0)
                    throw new Exception($"End instruction [{group.Key.B}] not found in method body");

                var excGroup = new ExcGroup {
                    TryStart = group.Key.A,
                    TryEndPredecessor = insns[endIndex - 1],
                    TryEnd = group.Key.B,
                };

                foreach (var eh in group) {
                    var catchBlock = ExtractCatch (method, eh, closure, fakeThis, excGroup, context);
                    excGroup.Handlers.Add (catchBlock);

                    if (eh.FilterStart != null) {
                        var filterBlock = ExtractFilter (method, eh, closure, fakeThis, excGroup, context, catchBlock);
                        excGroup.Handlers.Add (filterBlock);
                    }
                }

                var newInstructions = new List<Instruction> ();
                var newStart = Nop ("Constructed handler start");
                var newEnd = Nop ("Constructed handler end");
                newInstructions.Add (newStart);
                newInstructions.Add (Rethrow ("Constructed handler rethrow"));
                newInstructions.Add (newEnd);

                var targetIndex = insns.IndexOf(excGroup.TryEndPredecessor);
                if (targetIndex < 0)
                    throw new Exception ("Failed to find TryEndPredecessor");
                targetIndex += 1;
                InsertOps (insns, targetIndex, newInstructions.ToArray());

                var newEh = new ExceptionHandler (ExceptionHandlerType.Catch) {
                    CatchType = method.Module.TypeSystem.Object,
                    HandlerType = ExceptionHandlerType.Catch,
                    HandlerStart = newStart,
                    HandlerEnd = newEnd,
                    TryStart = excGroup.TryStart,
                    TryEnd = newStart,
                    FilterStart = null
                };
                method.Body.ExceptionHandlers.Add (newEh);
                
                foreach (var eh in group) {
                    // FIXME
                    if (eh.HandlerType == ExceptionHandlerType.Filter) {
                        eh.HandlerType = ExceptionHandlerType.Catch;
                        eh.FilterStart = null;
                        eh.CatchType = method.Module.TypeSystem.Object;
                        method.Body.ExceptionHandlers.Remove (eh);
                    }
                }
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
			foreach (var eh in method.Body.ExceptionHandlers)
				if (eh.FilterStart != null)
					CollectReferencedLocals (method, fakeThis, eh.FilterStart, eh.HandlerStart, referencedVariables, referencedArguments);
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

			if (method.Body.Instructions.IndexOf (insn) < 0)
				throw new Exception ($"{s} {insn} is missing from method {method.FullName}");
			else if (oldMethod != null && oldMethod.Body.Instructions.IndexOf (insn) >= 0)
				throw new Exception ($"{s} {insn} is present in old method for method {method.FullName}");

            return;
        }

		private void CleanMethodBody (MethodDefinition method, MethodDefinition oldMethod, bool verify, List<Instruction> removedInstructions = null) 
		{
			var insns = method.Body.Instructions;
            bool renumber = false;

			foreach (var i in insns) {
                if (renumber || i.Offset == 0)
				    i.Offset = insns.IndexOf (i);
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
				var opArg = i.Operand as ParameterDefinition;
				var opVar = i.Operand as VariableDefinition;

				if (opInsn != null) {
                    CheckInRange(opInsn, method, oldMethod, removedInstructions);
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
				}
			}

			foreach (var i in insns) {
                if (renumber || i.Offset == 0)
				    i.Offset = insns.IndexOf (i);
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

			int idx = oldBody.IndexOf (old);
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
			if (!TryRemapInstruction (old, oldBody, newBody, offset, out result))
				return null;

			return result;
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

			object newOperand = operand;
			if (variableMapping != null && variableMapping.ContainsKey (operand))
				newOperand = variableMapping[operand];
			else if (typeMapping != null) {
				var operandTr = operand as T;
				if (operandTr != null && typeMapping.ContainsKey (operandTr))
					newOperand = typeMapping[operandTr];
			}

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
			int targetIndex,
			Dictionary<object, object> variableMapping,
			Dictionary<T, U> typeMapping,
            List<EhRange> ranges,
			Func<Instruction, Instruction, Instruction> onFailedRemap = null,
            Func<Instruction, EhRange, Instruction[]> filter = null
		)
			where T : TypeReference
			where U : TypeReference
		{
			if (sourceIndex < 0)
				throw new ArgumentOutOfRangeException ("sourceIndex");

			for (int n = 0; n < count; n++) {
				var i = sourceMethod.Body.Instructions[n + sourceIndex];
				var newInsn = CloneInstruction (i, fakeThis, sourceMethod, variableMapping, typeMapping);
                if (filter != null) {
                    var range = FindRangeForOffset (ranges, n + sourceIndex);

                    var filtered = filter (newInsn, range);
                    if (filtered != null) {
                        foreach (var filteredInsn in filtered)
                            target.Add (filteredInsn);
                    } else {
                        target.Add (newInsn);
                    }
                } else {
    				target.Add (newInsn);
                }
			}

			// Fixup branches
			for (int i = 0; i < target.Count; i++) {
				var insn = target[i];
				var operand = insn.Operand as Instruction;
				if (operand == null)
					continue;

				Instruction newOperand, newInsn;
				if (!TryRemapInstruction (operand, sourceMethod.Body.Instructions, target, targetIndex - sourceIndex, out newOperand)) {
					if (onFailedRemap != null)
						newInsn = onFailedRemap (insn, operand);
					else
						throw new Exception ("Could not remap instruction operand for " + insn);
				} else {
					newInsn = Instruction.Create (insn.OpCode, newOperand);
				}

				target[i] = newInsn;
			}
		}
	}
}
