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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, LongType}

/**
 * Unit tests for the ConvertToCatalyst optimizer rule, which rewrites
 * TranspiledPythonUDF nodes to their Catalyst equivalents.
 *
 * These tests exercise the rule directly via applyExpr rather than running the
 * full optimizer pipeline, which means no JVM/Python bridge is required.
 */
class ConvertToCatalystSuite extends PlanTest {

  private val attrA = $"a".long

  // A leaf PythonUDF that takes one column argument. func=null is intentional:
  // structural tests don't need an executable PythonFunction.
  private def makePyUDF(input: Expression = attrA): PythonUDF =
    PythonUDF("udf", null, LongType, Seq(input),
      PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)

  // A leaf PythonUDAF (grouped-agg pandas eval type, return type Long for parity
  // with Count's output). func=null is intentional, as with makePyUDF.
  private def makePyUDAF(input: Expression = attrA): PythonUDAF =
    PythonUDAF("agg", null, LongType, Seq(input),
      udfDeterministic = true,
      evalType = PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF)

  // A TranspiledPythonUDF wrapping pyUDF with a single Catalyst option.
  private def makeTPUDF(pyUDF: PythonUDF, catalystOpt: Expression): TranspiledPythonUDF =
    TranspiledPythonUDF("udf", pyUDF, List(catalystOpt))

  private val catalystExpr: Expression = Add(attrA, Literal(4L))

  // ---- helpers ----

  // Both ANSI and ATTEMPT_TRANSPILATION must be true for the transpile path to fire.
  private def transpileOn[T](block: => T): T =
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true") { block }

  private def ansiOff[T](block: => T): T =
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true") { block }

  private def transpileOff[T](block: => T): T =
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "false") { block }

  // ---- tests ----

  test("transpiles when not nested (parentIsUdf = false)") {
    transpileOn {
      val tpudf = makeTPUDF(makePyUDF(), catalystExpr)
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      assert(!result.isInstanceOf[TranspiledPythonUDF])
      assert(!result.isInstanceOf[PythonUDF])
    }
  }

  test("prevents transpilation when parentIsUdf=true and inputs are plain PythonUDFs") {
    // PythonUDF -> TranspiledPythonUDF -> PythonUDF: the middle node should NOT be
    // transpiled when called from an outer UDF context, to preserve the batch pipeline.
    transpileOn {
      val innerPyUDF = makePyUDF(attrA)
      val outerPyUDF = makePyUDF(innerPyUDF)
      val outerTPUDF = makeTPUDF(outerPyUDF, Add(innerPyUDF, Literal(4L)))
      val result = ConvertToCatalyst.applyExpr(outerTPUDF, parentIsUdf = true)
      assert(result.isInstanceOf[PythonUDF])
      assert(!result.isInstanceOf[TranspiledPythonUDF])
    }
  }

  test("does not prevent transpilation when input to pythonUDFExpr is a TranspiledPythonUDF") {
    // When the input to a TPUDF is itself a TranspiledPythonUDF (has a Catalyst alternative),
    // hasOnlyPythonUDFInputs returns false so the outer TPUDF still transpiles.
    transpileOn {
      val innerPyUDF = makePyUDF(attrA)
      val innerTPUDF = makeTPUDF(innerPyUDF, catalystExpr)
      val outerPyUDF = makePyUDF(innerTPUDF)
      val outerTPUDF = makeTPUDF(outerPyUDF, Add(innerTPUDF, Literal(4L)))
      val result = ConvertToCatalyst.applyExpr(outerTPUDF, parentIsUdf = true)
      assert(!result.isInstanceOf[TranspiledPythonUDF])
      assert(!result.isInstanceOf[PythonUDF])
    }
  }

  test("hasOnlyPythonUDFInputs unit test") {
    val innerPyUDF = makePyUDF(attrA)
    val innerTPUDF = makeTPUDF(innerPyUDF, catalystExpr)

    // pythonUDFExpr's child is a plain PythonUDF -> true
    assert(makeTPUDF(makePyUDF(innerPyUDF), catalystExpr).hasOnlyPythonUDFInputs)
    // pythonUDFExpr's child is a TranspiledPythonUDF -> false
    assert(!makeTPUDF(makePyUDF(innerTPUDF), catalystExpr).hasOnlyPythonUDFInputs)
    // pythonUDFExpr's child is a plain column (leaf) -> false
    assert(!makeTPUDF(makePyUDF(attrA), catalystExpr).hasOnlyPythonUDFInputs)
    // zero-arg pythonUDFExpr -> false (nonEmpty guard)
    val zeroPyUDF = PythonUDF("udf", null, LongType, Seq.empty,
      PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)
    assert(!TranspiledPythonUDF("udf", zeroPyUDF, List(Literal(42L))).hasOnlyPythonUDFInputs)
  }

  test("falls back to PythonUDF when ANSI is disabled") {
    ansiOff {
      val tpudf = makeTPUDF(makePyUDF(), catalystExpr)
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      assert(result.isInstanceOf[PythonUDF])
      assert(!result.isInstanceOf[TranspiledPythonUDF])
    }
  }

  test("falls back to PythonUDF when transpilation is disabled") {
    transpileOff {
      val tpudf = makeTPUDF(makePyUDF(), catalystExpr)
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      assert(result.isInstanceOf[PythonUDF])
      assert(!result.isInstanceOf[TranspiledPythonUDF])
    }
  }

  test("falls back to PythonUDF when transpiledOptions is empty") {
    transpileOn {
      val pyUDF = makePyUDF()
      val tpudf = TranspiledPythonUDF("udf", pyUDF, List())
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      assert(result.isInstanceOf[PythonUDF])
      assert(!result.isInstanceOf[TranspiledPythonUDF])
    }
  }

  test("apply(plan) reaches TranspiledPythonUDF nodes below the root") {
    // Regression test for the traversal bug where ``plan.mapExpressions`` only
    // walks expressions on the root plan node. With that bug, a TPUDF inside a
    // Filter (or any non-root node) would survive the optimizer rule as an
    // ``Unevaluable`` expression and crash at execution. The fix uses
    // ``transformAllExpressionsWithPruning`` which descends through child
    // plans; this test pins that contract.
    transpileOn {
      val attrB = $"b".long
      val relation = LocalRelation(attrA, attrB)
      // The TPUDF lives in the Filter's condition (boolean), not at the root.
      val booleanTPUDF = TranspiledPythonUDF(
        "udf",
        PythonUDF("udf", null, BooleanType, Seq(attrA),
          PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
        List(GreaterThan(attrA, Literal(0L))))
      val plan = Project(Seq(attrB), Filter(booleanTPUDF, relation))
      val rewritten = ConvertToCatalyst.apply(plan)
      // No TranspiledPythonUDF should remain anywhere in the rewritten plan.
      val leftover = rewritten.collect {
        case p if p.expressions.exists(_.find(_.isInstanceOf[TranspiledPythonUDF]).isDefined) =>
          p
      }
      assert(leftover.isEmpty,
        s"TranspiledPythonUDF survived ConvertToCatalyst.apply: $rewritten")
      // The Filter's condition must be the resolved Catalyst expression, not a fallback PythonUDF.
      val filterCond = rewritten.asInstanceOf[Project].child.asInstanceOf[Filter].condition
      assert(filterCond == GreaterThan(attrA, Literal(0L)),
        s"Filter condition was not rewritten to GreaterThan: $filterCond")
    }
  }

  test("uses pre-coerced transpiledOptions as-is (analysis is responsible for coercion)") {
    // The Analyzer coerces transpiledOptions before the optimizer runs, because
    // TranspiledPythonUDF.children exposes them to the resolver's generic coercion pass.
    // ConvertToCatalyst must not re-run coercion; it simply selects the first non-null option.
    // This test simulates what analysis would produce for `def f(x: Long): return x + 4`
    // where the integer literal has already been cast to LongType.
    transpileOn {
      val preCoerced = Add(attrA, Cast(Literal(4, IntegerType), LongType))
      val tpudf = makeTPUDF(makePyUDF(), preCoerced)
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      assert(result == preCoerced,
        s"Expected pre-coerced expression unchanged, got: $result")
    }
  }

  // ---- UDAF cases (post-fromUDFExpr shape) ----
  //
  // After UserDefinedPythonFunction.fromUDFExpr lifts a PythonUDAF inside a
  // TranspiledPythonUDF, the wrapper holds an AggregateExpression instead of a
  // bare PythonUDAF. These tests pin the optimizer rule's behavior on that shape.

  test("transpiles TranspiledPythonUDF wrapping AggregateExpression(PythonUDAF)") {
    transpileOn {
      val pyAgg = makePyUDAF().toAggregateExpression()
      val catalystAgg = Count(Seq(attrA)).toAggregateExpression()
      val tpudf = TranspiledPythonUDF("agg", pyAgg, List(catalystAgg))
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      assert(result == catalystAgg,
        s"Expected catalyst aggregate alternative, got: $result")
    }
  }

  test("falls back to AggregateExpression(PythonUDAF) when ANSI is off (UDAF)") {
    ansiOff {
      val pyAgg = makePyUDAF().toAggregateExpression()
      val catalystAgg = Count(Seq(attrA)).toAggregateExpression()
      val tpudf = TranspiledPythonUDF("agg", pyAgg, List(catalystAgg))
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      result match {
        case ae: AggregateExpression =>
          assert(ae.aggregateFunction.isInstanceOf[PythonUDAF],
            s"Expected aggregateFunction to be PythonUDAF, got: ${ae.aggregateFunction}")
        case other => fail(s"Expected AggregateExpression(PythonUDAF, ...), got: $other")
      }
    }
  }

  // ---- Input sharing (SPARK-58626) ----
  //
  // An option is the body with each `_udf_param_N` replaced by the bound argument, so a parameter
  // used N times is spliced in N times, while the UDF it replaces evaluates each argument once. The
  // builder tags the copies of any parameter it splices in more than once, and ConvertToCatalyst
  // turns each tagged index into one `With` definition -- per parameter, never across them, since
  // two parameters are two columns to Python. Tags are always unwrapped, shared or not.
  //
  // `p(arg, i)` below is the tag the builder would have emitted for parameter `i`.
  private def p(arg: Expression, index: Int): Expression = TranspiledUDFParameter(arg, index)

  // A DoubleType single-argument UDF plus a transpiled option, both over the given argument.
  private def makeDoubleTypedTPUDF(arg: Expression, option: Expression): TranspiledPythonUDF =
    TranspiledPythonUDF(
      "udf",
      PythonUDF("udf", null, DoubleType, Seq(arg),
        PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
      List(option))

  // A two-argument UDF plus a transpiled option, for the cross-parameter cases.
  private def makeTwoArgTPUDF(
      args: Seq[Expression], option: Expression, dt: DataType = LongType): TranspiledPythonUDF =
    TranspiledPythonUDF(
      "udf",
      PythonUDF("udf", null, dt, args,
        PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
      List(option))

  private def assertNoTags(e: Expression): Unit =
    assert(!e.exists(_.isInstanceOf[TranspiledUDFParameter]), s"Parameter tag survived in: $e")

  test("shares a duplicated non-literal input via a With expression") {
    transpileOn {
      // `lambda x: x if x > 0.5 else 0.0` over rand(): the copy in the branch is only evaluated
      // when the branch is taken, so it drifts out of step with the one in the condition.
      val arg = Rand(Literal(1L))
      val option = If(GreaterThan(p(arg, 0), Literal(0.5)), p(arg, 0), Literal(0.0))
      val result = ConvertToCatalyst.applyExpr(
        makeDoubleTypedTPUDF(arg, option), parentIsUdf = false)
      result match {
        case With(child, Seq(exprDef)) =>
          assert(exprDef.child == arg, s"Expected the argument as the definition, got: $exprDef")
          assert(child.collect { case r: CommonExpressionRef => r }.size == 2,
            s"Expected both uses of the argument to be references, got: $child")
          assert(!child.exists(_ == arg), s"Argument still evaluated inline in: $child")
          assertNoTags(result)
        case other => fail(s"Expected a With expression, got: $other")
      }
    }
  }

  test("keeps one evaluation per parameter rather than sharing across them") {
    transpileOn {
      // `lambda a, b: a * b` called as f(a + 1, a + 1). Each parameter is used once so the builder
      // tags nothing, and Python would evaluate both columns anyway.
      val arg = Add(attrA, Literal(1L))
      val option = Multiply(arg, arg)
      val result = ConvertToCatalyst.applyExpr(
        makeTwoArgTPUDF(Seq(arg, arg), option), parentIsUdf = false)
      assert(result == option, s"Expected the option unchanged, got: $result")
    }
  }

  test("shares one parameter and leaves its structurally equal twin inline") {
    transpileOn {
      // `lambda a, b: (a + a) - b` over f(rand(1), rand(1)). The three copies are identical, so
      // only the tags say which two are `a`: `a` gets one definition and `b` keeps its own draw.
      val arg = Rand(Literal(1L))
      val option = Subtract(Add(p(arg, 0), p(arg, 0)), arg)
      val result = ConvertToCatalyst.applyExpr(
        makeTwoArgTPUDF(Seq(arg, arg), option, DoubleType), parentIsUdf = false)
      result match {
        case With(child, Seq(exprDef)) =>
          assert(exprDef.child == arg, s"Expected one definition for a, got: $exprDef")
          assert(child.collect { case r: CommonExpressionRef => r }.size == 2,
            s"Expected a's two uses to be references, got: $child")
          assert(child.collect { case r: Rand => r }.size == 1,
            s"Expected b to keep exactly one inline draw, got: $child")
          assertNoTags(result)
        case other => fail(s"Expected a With expression, got: $other")
      }
    }
  }

  test("shares per parameter and re-evaluates a nested argument") {
    transpileOn {
      // `lambda a, b: a * a + b` as f(a + 1, (a + 1) + 2). Parameter `a` is tagged twice and gets a
      // definition; parameter `b` embeds a copy of `a`'s argument, which stays put -- Python
      // evaluates `b`'s column independently, so that copy is `b`'s own work, not a third use.
      val inner = Add(attrA, Literal(1L))
      val outer = Add(inner, Literal(2L))
      val option = Add(Multiply(p(inner, 0), p(inner, 0)), outer)
      val result = ConvertToCatalyst.applyExpr(
        makeTwoArgTPUDF(Seq(inner, outer), option), parentIsUdf = false)
      result match {
        case With(child, Seq(exprDef)) =>
          assert(exprDef.child == inner, s"Expected the inner argument as the def, got: $exprDef")
          assert(child.collect { case r: CommonExpressionRef => r }.size == 2,
            s"Expected only parameter a's two uses to be references, got: $child")
          assert(child.exists(_ == outer), s"Parameter b's argument was rewritten in: $child")
          assertNoTags(result)
        case other => fail(s"Expected a With expression, got: $other")
      }
    }
  }

  test("unwraps a tag it does not share") {
    transpileOn {
      // A lone tag (an earlier rewrite dropped the other copy) has nothing to share, but the marker
      // still must not reach execution.
      val arg = Rand(Literal(1L))
      val option = Add(p(arg, 0), Literal(1.0))
      val result = ConvertToCatalyst.applyExpr(
        makeDoubleTypedTPUDF(arg, option), parentIsUdf = false)
      assert(result == Add(arg, Literal(1.0)), s"Expected the tag unwrapped, got: $result")
      assertNoTags(result)
    }
  }

  test("leaves a single-use input inline") {
    transpileOn {
      // Nothing is tagged, so the plan must not grow a With expression.
      val arg = Rand(Literal(1L))
      val option = Add(arg, Literal(1.0))
      val result = ConvertToCatalyst.applyExpr(
        makeDoubleTypedTPUDF(arg, option), parentIsUdf = false)
      assert(result == option, s"Expected the option unchanged, got: $result")
    }
  }

  test("drops an input the option never uses") {
    transpileOn {
      // `lambda a, b: a` over f(a, rand()): substitution already dropped the second argument, so it
      // is never evaluated and never becomes a definition. That differs from the Python path, which
      // computes every argument column -- an accepted difference, pinned so a change is deliberate.
      val unused = Rand(Literal(1L))
      val option = Add(attrA, Literal(1L))
      val result = ConvertToCatalyst.applyExpr(
        makeTwoArgTPUDF(Seq(attrA, unused), option), parentIsUdf = false)
      assert(result == option, s"Expected the option unchanged, got: $result")
      assert(!result.exists(_.isInstanceOf[Rand]), s"Unused argument survived in: $result")
    }
  }

  test("leaves a duplicated foldable input inline") {
    transpileOn {
      // Constant folding collapses the literal at every use site, which beats a shared column, so
      // the builder leaves a foldable argument untagged.
      val arg = Literal(3L)
      val option = Multiply(arg, arg)
      val tpudf = TranspiledPythonUDF(
        "udf",
        PythonUDF("udf", null, LongType, Seq(arg),
          PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
        List(option))
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      assert(result == option, s"Expected the option unchanged, got: $result")
    }
  }

  test("leaves an aggregating option inline but still unwraps its tags") {
    transpileOn {
      // `With` forbids a common expression ref inside an AggregateExpression from the same scope,
      // so a transpiled grouped-agg UDF keeps its inputs inline -- tags and all removed.
      val arg = Add(attrA, Literal(1L))
      val pyAgg = makePyUDAF(arg).toAggregateExpression()
      val catalystAgg = Count(Seq(arg, arg)).toAggregateExpression()
      val taggedAgg = catalystAgg.transformUp { case e if e == arg => p(arg, 0) }
      val tpudf = TranspiledPythonUDF("agg", pyAgg, List(taggedAgg))
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      assert(result == catalystAgg, s"Expected the option inline and untagged, got: $result")
      assertNoTags(result)
    }
  }

  test("does not mix tags with a nested transpiled UDF's own parameters") {
    transpileOn {
      // udf1(udf2(a), udf2(a)) with udf1's body using parameter 0 twice. Both calls tag an index 0,
      // so the outer rule must ignore the inner option's tags or it would share across the two.
      val innerPyUDF = makePyUDF(attrA)
      val innerOption = Add(p(attrA, 0), p(attrA, 0))
      val innerTPUDF = TranspiledPythonUDF("udf2", innerPyUDF, List(innerOption))
      val outerPyUDF = PythonUDF("udf1", null, LongType, Seq(innerTPUDF, innerTPUDF),
        PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)
      val outerOption = Add(Multiply(p(innerTPUDF, 0), p(innerTPUDF, 0)), innerTPUDF)
      val result = ConvertToCatalyst.applyExpr(
        TranspiledPythonUDF("udf1", outerPyUDF, List(outerOption)), parentIsUdf = false)
      result match {
        case With(child, Seq(exprDef)) =>
          // Only the outer parameter 0 is shared here; the inline copy carries the inner call's own
          // With, so count just the refs belonging to this scope.
          assert(child.collect { case r: CommonExpressionRef if r.id == exprDef.id => r }.size == 2,
            s"Expected the outer parameter's two uses to be references, got: $child")
          // The other copy stays a separate evaluation of the inner call rather than a third ref.
          assert(child.exists(_.isInstanceOf[With]),
            s"Expected parameter 1 to keep its own inner call, got: $child")
          assert(!result.exists(_.isInstanceOf[TranspiledPythonUDF]),
            s"Inner call left unconverted in: $result")
          assertNoTags(result)
        case other => fail(s"Expected a With expression, got: $other")
      }
    }
  }

  test("RewriteWithExpression pre-evaluates the shared input in a Project") {
    // End to end: the argument is computed once in a Project below, the option reads it back as a
    // column, and no With survives. ConvertToLocalRelation is excluded so the projection stays.
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
      val arg = Rand(Literal(1L))
      val option = If(GreaterThan(p(arg, 0), Literal(0.5)), p(arg, 0), Literal(0.0))
      val tpudf = makeDoubleTypedTPUDF(arg, option)
      val relation = LocalRelation.fromExternalRows(Seq(attrA), Seq(Row(1L)))
      val optimized = SimpleTestOptimizer.execute(Project(Seq(Alias(tpudf, "v")()), relation))
      val expressions = optimized.flatMap(_.expressions)
      assert(!expressions.exists(_.exists(_.isInstanceOf[With])),
        s"With expression survived the optimizer: $optimized")
      assert(expressions.map(_.collect { case r: Rand => r }.size).sum == 1,
        s"Expected the argument to be evaluated once, got: $optimized")
      assert(optimized.output.map(_.name) == Seq("v"),
        s"Extra columns leaked into the output: $optimized")
      expressions.foreach(assertNoTags)
    }
  }
}
