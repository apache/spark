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
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType}

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
  // A transpiled option is the option body with each `_udf_param_N` placeholder replaced by the
  // bound argument, so an argument referenced N times in the body is spliced in N times. The
  // Python UDF being replaced evaluates each argument once, so ConvertToCatalyst wraps the option
  // in a `With` expression that defines each duplicated argument once. RewriteWithExpression
  // (a couple of batches later) pre-evaluates those definitions in a Project below the operator.

  // A DoubleType single-argument UDF plus a transpiled option, both over the given argument.
  private def makeDoubleTypedTPUDF(arg: Expression, option: Expression): TranspiledPythonUDF =
    TranspiledPythonUDF(
      "udf",
      PythonUDF("udf", null, DoubleType, Seq(arg),
        PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
      List(option))

  test("shares a duplicated non-literal input via a With expression") {
    transpileOn {
      // `lambda x: x if x > 0.5 else 0.0` over rand(): the two copies of the argument are
      // separate Rand instances, and the one in the conditional branch is only evaluated when
      // the branch is taken, so it drifts out of step with the one in the condition.
      val arg = Rand(Literal(1L))
      val option = If(GreaterThan(arg, Literal(0.5)), arg, Literal(0.0))
      val result = ConvertToCatalyst.applyExpr(
        makeDoubleTypedTPUDF(arg, option), parentIsUdf = false)
      result match {
        case With(child, Seq(exprDef)) =>
          assert(exprDef.child == arg, s"Expected the argument as the definition, got: $exprDef")
          assert(child.collect { case r: CommonExpressionRef => r }.size == 2,
            s"Expected both uses of the argument to be references, got: $child")
          assert(!child.exists(_ == arg), s"Argument still evaluated inline in: $child")
        case other => fail(s"Expected a With expression, got: $other")
      }
    }
  }

  test("shares a duplicated non-literal input passed in more than one position") {
    transpileOn {
      // `lambda a, b: a * b` called as f(a + 1, a + 1): one definition serves both parameters,
      // which is safe because the argument is deterministic.
      val arg = Add(attrA, Literal(1L))
      val tpudf = TranspiledPythonUDF(
        "udf",
        PythonUDF("udf", null, LongType, Seq(arg, arg),
          PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
        List(Multiply(arg, arg)))
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      result match {
        case With(child, Seq(exprDef)) =>
          assert(exprDef.child == arg, s"Expected the argument as the definition, got: $exprDef")
          assert(child.collect { case r: CommonExpressionRef => r }.size == 2,
            s"Expected both uses of the argument to be references, got: $child")
        case other => fail(s"Expected a With expression, got: $other")
      }
    }
  }

  test("leaves a single-use input inline") {
    transpileOn {
      // Nothing is duplicated, so the plan must not grow a With expression.
      val arg = Rand(Literal(1L))
      val option = Add(arg, Literal(1.0))
      val result = ConvertToCatalyst.applyExpr(
        makeDoubleTypedTPUDF(arg, option), parentIsUdf = false)
      assert(result == option, s"Expected the option unchanged, got: $result")
    }
  }

  test("leaves a duplicated foldable input inline") {
    transpileOn {
      // Constant folding collapses the literal at every use site, which beats reading it from a
      // shared column.
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

  test("leaves an identical nondeterministic input passed in two positions inline") {
    transpileOn {
      // f(rand(1), rand(1)): the spliced copies are structurally identical, so there is no way
      // to tell which parameter an occurrence came from. Sharing one definition between the two
      // positions would collapse two independent draws into one, so leave them alone.
      val arg = Rand(Literal(1L))
      val option = Subtract(arg, arg)
      val tpudf = TranspiledPythonUDF(
        "udf",
        PythonUDF("udf", null, DoubleType, Seq(arg, arg),
          PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
        List(option))
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      assert(result == option, s"Expected the option unchanged, got: $result")
    }
  }

  test("leaves an aggregating option inline") {
    transpileOn {
      // `With` forbids a common expression reference inside an AggregateExpression defined in the
      // same scope, so a transpiled grouped-agg UDF keeps its inputs inline.
      val arg = Add(attrA, Literal(1L))
      val pyAgg = makePyUDAF(arg).toAggregateExpression()
      val catalystAgg = Count(Seq(arg, arg)).toAggregateExpression()
      val tpudf = TranspiledPythonUDF("agg", pyAgg, List(catalystAgg))
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      assert(result == catalystAgg, s"Expected the option unchanged, got: $result")
    }
  }

  test("RewriteWithExpression pre-evaluates the shared input in a Project") {
    // End-to-end through the optimizer: the shared argument is computed once in a Project below,
    // the option reads it back as a column, and no With expression survives. ConvertToLocalRelation
    // is excluded so the projection is not evaluated away.
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
      val arg = Rand(Literal(1L))
      val option = If(GreaterThan(arg, Literal(0.5)), arg, Literal(0.0))
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
    }
  }
}
