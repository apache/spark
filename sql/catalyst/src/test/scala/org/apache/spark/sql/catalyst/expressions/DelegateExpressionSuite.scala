/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{RemoveInputTypeMarkers, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Validates [[DelegateExpression]] transparency (eval + codegen, via `checkEvaluation` which runs
 * both paths) and that [[DelegateFunction]] supports all three input-type contracts.
 */
class DelegateExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  // ---- transparency: every behavior delegates to `definition` ----

  test("delegates eval and codegen to its definition (foldable)") {
    val expr = DelegateExpression("inc", Seq(Literal(10)), Add(Literal(10), Literal(1)))
    checkEvaluation(expr, 11)
  }

  test("delegates eval and codegen with a non-foldable input") {
    val ref = BoundReference(0, IntegerType, nullable = true)
    val expr = DelegateExpression("inc", Seq(ref), Add(ref, Literal(1)))
    checkEvaluation(expr, 11, InternalRow(10))
    checkEvaluation(expr, null, InternalRow(null))
  }

  test("delegates type/nullability/foldability/determinism and canonicalizes to its definition") {
    val ref = BoundReference(0, IntegerType, nullable = true)
    val expr = DelegateExpression("inc", Seq(ref), Add(ref, Literal(1)))
    assert(expr.dataType == IntegerType)
    assert(expr.nullable)
    assert(!expr.foldable)
    assert(expr.deterministic)
    assert(expr.canonicalized == Add(ref, Literal(1)).canonicalized)
    assert(DelegateExpression("inc", Seq(Literal(10)), Add(Literal(10), Literal(1))).foldable)
  }

  // ---- input-type contracts ----

  private object CastFn extends DelegateFunction {
    override val name = "castfn"
    override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
    override def implicitCast: Boolean = true
    override def lower(args: Seq[Expression]): Expression = args.head
  }

  private object CheckFn extends DelegateFunction {
    override val name = "checkfn"
    override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType)
    override def implicitCast: Boolean = false
    override def lower(args: Seq[Expression]): Expression = args.head
  }

  private object AnyFn extends DelegateFunction {
    override val name = "anyfn"
    // no inputTypes -> accepts any type
    override def lower(args: Seq[Expression]): Expression = args.head
  }

  // The input-type contracts are the analysis-time `build` path, which inserts the markers.
  private def buildDef(fn: DelegateFunction, args: Expression*): Expression =
    fn.build(fn.name, args).asInstanceOf[DelegateExpression].definition

  test("contract 1 (implicit cast): args wrapped in ImplicitCastInput (cast happens via " +
    "the standard coercion rule)") {
    val shim = buildDef(CastFn, Literal(1)).asInstanceOf[ImplicitCastInput]
    assert(shim.expectedType == StringType)
    // The shim IS an ImplicitCastInputTypes node, so TypeCoercion will cast its child.
    assert(shim.isInstanceOf[ImplicitCastInputTypes])
  }

  test("contract 2 (type check only): args wrapped in TypeCheckInput, mismatch rejected, " +
    "no cast") {
    val ok = buildDef(CheckFn, Literal(1)).asInstanceOf[TypeCheckInput]
    assert(ok.checkInputDataTypes().isSuccess)
    // A Long is NOT cast down to Int -- it is rejected.
    val bad = buildDef(CheckFn, Literal(1L)).asInstanceOf[TypeCheckInput]
    assert(bad.checkInputDataTypes().isFailure)
    assert(!bad.isInstanceOf[ImplicitCastInputTypes])
  }

  test("contract 3 (any type): no inputTypes -> no shim, arg passed through unchanged") {
    assert(buildDef(AnyFn, Literal(1L)) == Literal(1L))
  }

  test("nullIntolerant is delegated to the definition") {
    // An Invoke with the default propagateNull = true is null-intolerant; a bare Literal is not.
    val invoke = Invoke(Literal("x"), "toString", StringType)
    assert(invoke.nullIntolerant)
    assert(DelegateExpression("f", Seq(Literal("x")), invoke).nullIntolerant,
      "the wrapper should report its null-intolerant definition's null-intolerance")
    assert(!DelegateExpression("g", Seq(Literal(1)), Literal(1)).nullIntolerant)
  }

  test("build validates argument count against the inputTypes arity") {
    // CheckFn declares one typed input, so build rejects any other arity with WRONG_NUM_ARGS rather
    // than indexing past the args (too few) or silently ignoring extras (too many).
    Seq(Seq.empty[Expression], Seq(Literal(1), Literal(2))).foreach { args =>
      val e = intercept[AnalysisException](CheckFn.build(CheckFn.name, args))
      assert(e.getCondition == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION")
    }
    // AnyFn has no inputTypes -> it is variadic and `lower` owns the arg handling, so no arity
    // check.
    assert(AnyFn.build(AnyFn.name, Seq(Literal(1), Literal(2))).isInstanceOf[DelegateExpression])
  }

  test("RemoveInputTypeMarkers keeps a failed type-check marker for CheckAnalysis to report") {
    // A resolved marker has served its purpose and is unwrapped to its child ...
    val okDelegate = CheckFn.build(CheckFn.name, Seq(Literal(1)))
    assert(!RemoveInputTypeMarkers.removeMarkers(okDelegate).exists(_.isInstanceOf[TypeCheckInput]),
      "a resolved TypeCheckInput should be unwrapped")
    // ... but a type-mismatched (unresolved) marker is left in place, so its ExpectsInputTypes
    // failure stays visible to CheckAnalysis instead of exposing a resolved child of a wrong type.
    val badDelegate = CheckFn.build(CheckFn.name, Seq(Literal(1L)))
    val cleaned = RemoveInputTypeMarkers.removeMarkers(badDelegate)
    assert(cleaned.exists(_.isInstanceOf[TypeCheckInput]),
      s"a failed TypeCheckInput must be preserved for CheckAnalysis, got $cleaned")
  }

  private object MixedFn extends DelegateFunction {
    override val name = "mixedfn"
    override def inputTypes: Seq[AbstractDataType] = Seq(StringType, AnyDataType)
    override def lower(args: Seq[Expression]): Expression = CreateArray(args)
  }

  test("input-type contract is per argument: AnyDataType position opts out of shimming") {
    val args = buildDef(MixedFn, Literal(1), Literal(2)).asInstanceOf[CreateArray].children
    assert(args(0).isInstanceOf[ImplicitCastInput]) // StringType -> shimmed
    assert(args(1) == Literal(2)) // AnyDataType -> raw
  }

  test("apply (direct construction) inserts no markers; args must already be typed") {
    // Unlike `build`, `apply` is construct-anywhere and never produces input-type markers.
    assert(CastFn(Literal("x")).definition == Literal("x"))
    assert(
      MixedFn(Literal("s"), Literal(2)).definition == CreateArray(Seq(Literal("s"), Literal(2))))
  }

  test("apply rejects unresolved arguments") {
    intercept[IllegalArgumentException](CastFn(UnresolvedAttribute("x")))
  }

  // ---- definition is a real child (the safety property the whole design rests on) ----

  test("transform reaches into the definition and withNewChildren replaces it") {
    val ref = BoundReference(0, IntegerType, nullable = true)
    val expr = DelegateExpression("inc", Seq(ref), Add(ref, Literal(1)))
    // tree traversal descends into `definition`
    val bumped = expr.transform { case Literal(1, IntegerType) => Literal(2) }
    assert(bumped == DelegateExpression("inc", Seq(ref), Add(ref, Literal(2))))
    // withNewChildren swaps the single child, which is the definition
    val replaced = expr.withNewChildren(Seq(Literal(99))).asInstanceOf[DelegateExpression]
    assert(replaced.definition == Literal(99))
    assert(replaced.inputs == Seq(ref)) // inputs are metadata, untouched
  }

  test("references come from the definition, not from inputs") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    // `b` appears only as display metadata; the real child references `a`
    val expr = DelegateExpression("f", Seq(b), Add(a, Literal(1)))
    assert(expr.references == AttributeSet(a))
  }

  test("sql and prettyName reflect the high-level call") {
    val expr = DelegateExpression("myfunc", Seq(Literal(1), Literal("x")), Literal(0))
    assert(expr.prettyName == "myfunc")
    assert(expr.sql == "myfunc(1, 'x')")
  }

  test("DelegateFunction.unapply round-trips apply") {
    assert(CastFn.unapply(CastFn(Literal("x"))).contains(Seq(Literal("x"))))
    assert(AnyFn.unapply(Literal(1)).isEmpty)
  }

  // ---- input-type markers are transient, unevaluable, and transparent in type ----

  test("input-type markers are Unevaluable and delegate type/nullability to their child") {
    val marker = ImplicitCastInput(BoundReference(0, IntegerType, nullable = true), StringType)
    assert(marker.dataType == IntegerType) // delegates to child until coercion casts it
    assert(marker.nullable)
    intercept[Exception](marker.eval(InternalRow(1)))
  }

  // ---- MultiGetJsonObject: the optimizer-constructed delegate ----

  test("MultiGetJsonObject builds a typed delegate over an Invoke and round-trips paths") {
    val e = MultiGetJsonObject(Literal("{}"), Seq("$.a", "$.b"))
    assert(e.name == "multi_get_json_object")
    assert(e.definition.isInstanceOf[Invoke])
    assert(e.dataType == StructType(Seq(
      StructField("_0", StringType), StructField("_1", StringType))))
    assert(MultiGetJsonObject.unapply(e).map(_._2).contains(Seq("$.a", "$.b")))
  }

  test("MultiGetJsonObject evaluates by delegating through the Invoke") {
    val e = MultiGetJsonObject(Literal("""{"a":1,"b":2}"""), Seq("$.a", "$.b"))
    val row = e.eval(null).asInstanceOf[InternalRow]
    assert(row.getUTF8String(0) == UTF8String.fromString("1"))
    assert(row.getUTF8String(1) == UTF8String.fromString("2"))
  }
}
