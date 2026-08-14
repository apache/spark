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
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, LocalRelation, LogicalPlan, Project}
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
    // A TPUDF in a non-root node (here a Project under a Filter) must still be converted; if it
    // were left behind it would reach execution as an ``Unevaluable`` and crash. ``apply`` descends
    // the plan (and its subqueries) with ``transformDownWithSubqueriesAndPruning`` and walks each
    // node's expressions with ``mapExpressions``, so this pins that non-root nodes are reached.
    // The Filter's own condition is deliberately plain: a transpiled call in a predicate keeps the
    // interpreted UDF (see the predicate tests below), which would not exercise this.
    transpileOn {
      val attrB = $"b".long
      val relation = LocalRelation(attrA, attrB)
      val tpudf = makeTPUDF(makePyUDF(attrA), Add(attrA, Literal(4L)))
      val plan = Filter(GreaterThan(attrB, Literal(0L)),
        Project(Seq(attrB, Alias(tpudf, "v")()), relation))
      val rewritten = ConvertToCatalyst.apply(plan)
      // No TranspiledPythonUDF should remain anywhere in the rewritten plan.
      val leftover = rewritten.collect {
        case p if p.expressions.exists(_.find(_.isInstanceOf[TranspiledPythonUDF]).isDefined) =>
          p
      }
      assert(leftover.isEmpty,
        s"TranspiledPythonUDF survived ConvertToCatalyst.apply: $rewritten")
      // The projection must hold the resolved Catalyst expression, not a fallback PythonUDF.
      val projected = rewritten.asInstanceOf[Filter].child.asInstanceOf[Project]
        .projectList.last.asInstanceOf[Alias].child
      assert(projected == Add(attrA, Literal(4L)),
        s"The projected UDF was not rewritten to its option: $projected")
    }
  }

  // ---- Predicate positions (SPARK-58626) ----
  //
  // Pre-evaluating an option's inputs is worse than useless in a `Filter` condition or a join
  // condition: predicate pushdown inlines a column that is not `Expression.expensive` -- which
  // arithmetic is not -- straight back into the predicate it pushes down, putting a repeated input
  // back at every use site and back inside the body's branches, where the interpreted UDF would
  // have computed it once per row. So an option that needs a column keeps the interpreted UDF
  // there, while one whose arguments are all cheap needs no column and still transpiles.

  // A boolean call over `arg` whose option reads the parameter twice, so it needs a column unless
  // the argument is cheap.
  private def predicateTPUDF(arg: Expression): TranspiledPythonUDF = {
    val id = NamedExpression.newExprId
    val option = GreaterThan(
      Add(marker(arg, 0, id), marker(arg, 0, id)), Literal(0L))
    TranspiledPythonUDF(
      "udf",
      PythonUDF("udf", null, BooleanType, Seq(arg),
        PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
      List(option))
  }

  test("keeps the interpreted UDF for a Filter condition whose input needs a column") {
    transpileOn {
      val tpudf = predicateTPUDF(Add(attrA, Literal(1L)))
      val rewritten = ConvertToCatalyst(Filter(tpudf, LocalRelation(attrA)))
      assert(rewritten.asInstanceOf[Filter].condition == tpudf.pythonUDFExpr,
        s"Expected the interpreted UDF in the condition, got: $rewritten")
    }
  }

  test("keeps the interpreted UDF for a join condition whose input needs a column") {
    transpileOn {
      val attrC = $"c".long
      val tpudf = predicateTPUDF(Add(attrC, Literal(1L)))
      val plan =
        Join(LocalRelation(attrA), LocalRelation(attrC), Inner, Some(tpudf), JoinHint.NONE)
      val rewritten = ConvertToCatalyst(plan)
      assert(rewritten.asInstanceOf[Join].condition.contains(tpudf.pythonUDFExpr),
        s"Expected the interpreted UDF in the join condition, got: $rewritten")
    }
  }

  test("transpiles in a predicate when every input is cheap") {
    transpileOn {
      // A plain column is read, not computed, so no column is created, pushdown has nothing
      // to inline, and the filter stays Python-free -- what the plan-elision test relies on.
      val tpudf = predicateTPUDF(attrA)
      val rewritten = ConvertToCatalyst(Filter(tpudf, LocalRelation(attrA)))
      assert(rewritten.asInstanceOf[Filter].condition ==
        GreaterThan(Add(attrA, attrA), Literal(0L)),
        s"Expected the option in the condition, got: $rewritten")
    }
  }

  test("decides a nested call in a predicate on its own inputs") {
    transpileOn {
      // udf1(udf2(a)) as a condition, where udf1's body reads its parameter twice. udf1's input
      // is the inner call, which is not cheap, so udf1 keeps interpreted Python -- but udf2's own
      // input is a plain column, so it needs no column and still transpiles in the predicate.
      val inner = makeTPUDF(makePyUDF(attrA), Add(attrA, Literal(1L)))
      val id = NamedExpression.newExprId
      val outer = TranspiledPythonUDF(
        "udf1",
        PythonUDF("udf1", null, BooleanType, Seq(inner),
          PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
        List(GreaterThan(Add(marker(inner, 0, id), marker(inner, 0, id)), Literal(0L))))
      val condition =
        ConvertToCatalyst(Filter(outer, LocalRelation(attrA))).asInstanceOf[Filter].condition
      assert(!condition.exists(_.isInstanceOf[TranspiledPythonUDF]),
        s"A TranspiledPythonUDF survived in the condition: $condition")
      val pythonUDFs = condition.collect { case u: PythonUDF => u }
      assert(pythonUDFs.length == 1, s"Expected only udf1 to stay Python, got: $condition")
      assert(pythonUDFs.head.children == Seq(Add(attrA, Literal(1L))),
        s"Expected udf2 transpiled into udf1's argument, got: $condition")
    }
  }

  test("keeps both calls Python in a predicate when each input needs a column") {
    transpileOn {
      // As above, but udf2's body reads its parameter twice over `a + 1`, so udf2 needs a column of
      // its own and stops transpiling too.
      val innerArg = Add(attrA, Literal(1L))
      val innerId = NamedExpression.newExprId
      val inner = TranspiledPythonUDF(
        "udf2",
        makePyUDF(innerArg),
        List(Add(marker(innerArg, 0, innerId), marker(innerArg, 0, innerId))))
      val outerId = NamedExpression.newExprId
      val outer = TranspiledPythonUDF(
        "udf1",
        PythonUDF("udf1", null, BooleanType, Seq(inner),
          PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
        List(GreaterThan(Add(marker(inner, 0, outerId), marker(inner, 0, outerId)), Literal(0L))))
      val condition =
        ConvertToCatalyst(Filter(outer, LocalRelation(attrA))).asInstanceOf[Filter].condition
      assert(!condition.exists(_.isInstanceOf[TranspiledPythonUDF]),
        s"A TranspiledPythonUDF survived in the condition: $condition")
      assert(condition.collect { case u: PythonUDF => u }.size == 2,
        s"Expected both calls to stay Python, got: $condition")
    }
  }

  test("transpiles the same call outside a predicate") {
    // The control: the position is what decides, not the UDF.
    transpileOn {
      val arg = Add(attrA, Literal(1L))
      val tpudf = predicateTPUDF(arg)
      val rewritten =
        ConvertToCatalyst(Project(Seq(Alias(tpudf, "v")()), LocalRelation(attrA)))
      assert(!rewritten.exists(_.expressions.exists(_.exists(_.isInstanceOf[PythonUDF]))),
        s"Expected the option in a Project, got: $rewritten")
      val columns = rewritten.collect {
        case Project(projectList, _) =>
          projectList.collect { case a: Alias if a.name.startsWith("_udf_input") => a.child }
      }.flatten
      assert(columns == Seq(arg), s"Expected the input pre-evaluated, got: $rewritten")
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

  // ---- Parameter markers (SPARK-58626) ----
  //
  // An option is the body with each `_udf_param_N` replaced by the bound argument, so a parameter
  // used N times is spliced in N times, where the Python eval operator it replaces computes one
  // column per argument. `UserDefinedPythonFunction.builder` marks every copy with a
  // [[TranspiledUDFParameter]] and `PreEvaluateTranspiledUDFInputs` turns those marks into
  // pre-evaluated columns; which copies share a column, and which inputs cannot be pre-evaluated at
  // all, is pinned in PreEvaluateTranspiledUDFInputsSuite. What is pinned here is the division of
  // labour: `applyExpr` substitutes and leaves the marks alone -- pre-evaluation needs the whole
  // operator, not one expression -- while `apply` does both.

  private def marker(arg: Expression, index: Int, id: ExprId): Expression =
    TranspiledUDFParameter(arg, index, id)

  private def hasMarkers(plan: LogicalPlan): Boolean =
    plan.exists(_.expressions.exists(_.exists(_.isInstanceOf[TranspiledUDFParameter])))

  test("applyExpr substitutes the option and leaves its parameter markers in place") {
    transpileOn {
      val arg = Add(attrA, Literal(1L))
      val id = NamedExpression.newExprId
      val option = Multiply(marker(arg, 0, id), marker(arg, 0, id))
      val result = ConvertToCatalyst.applyExpr(makeTPUDF(makePyUDF(arg), option),
        parentIsUdf = false)
      assert(result == option, s"Expected the option with its markers intact, got: $result")
    }
  }

  test("apply pre-evaluates the marked inputs and leaves no marker behind") {
    transpileOn {
      val arg = Add(attrA, Literal(1L))
      val id = NamedExpression.newExprId
      val option = Multiply(marker(arg, 0, id), marker(arg, 0, id))
      val plan =
        Project(Seq(Alias(makeTPUDF(makePyUDF(arg), option), "v")()), LocalRelation(attrA))
      val optimized = ConvertToCatalyst(plan)
      assert(!hasMarkers(optimized), s"A parameter marker survived: $optimized")
      val preEvaluated = optimized.collect {
        case Project(projectList, _) =>
          projectList.collect { case a: Alias if a.name.startsWith("_udf_input") => a.child }
      }.flatten
      assert(preEvaluated == Seq(arg), s"Expected the input pre-evaluated once, got: $optimized")
    }
  }

  test("drops an input the option never uses") {
    transpileOn {
      // `lambda a, b: a` over f(a, rand()): substitution already dropped the second argument, so it
      // is never evaluated and never becomes a column. That differs from the Python path, which
      // computes every argument column -- an accepted difference, pinned so a change is deliberate.
      val unused = Rand(Literal(1L))
      val option = Add(attrA, Literal(1L))
      val tpudf = TranspiledPythonUDF(
        "udf",
        PythonUDF("udf", null, LongType, Seq(attrA, unused),
          PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
        List(option))
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      assert(result == option, s"Expected the option unchanged, got: $result")
      assert(!result.exists(_.isInstanceOf[Rand]), s"Unused argument survived in: $result")
    }
  }

  test("apply keeps a transpilable UDF Python when wrapped by a non-transpiled Python UDF") {
    // The walk starts at the top expression and threads parentIsUdf down, so a transpilable UDF
    // whose inputs are all plain Python UDFs and which is itself an argument to a non-transpiled
    // Python UDF stays Python to preserve the batch pipeline, rather than being converted and
    // splitting the chain Python -> Catalyst -> Python. Exercised through apply, which the direct
    // applyExpr tests bypass.
    transpileOn {
      val plain = makePyUDF(attrA)
      val midPy = makePyUDF(plain)
      val midTPUDF = makeTPUDF(midPy, Add(plain, Literal(4L)))
      assert(midTPUDF.hasOnlyPythonUDFInputs)
      val outerPy = makePyUDF(midTPUDF)
      val optimized = ConvertToCatalyst(Project(Seq(Alias(outerPy, "v")()), LocalRelation(attrA)))
      val exprs = optimized.flatMap(_.expressions)
      assert(!exprs.exists(_.exists(_.isInstanceOf[TranspiledPythonUDF])),
        s"TranspiledPythonUDF survived: $optimized")
      // Three Python UDFs remain (outer, mid, plain); the mid was not converted to its Add option.
      assert(exprs.map(_.collect { case u: PythonUDF => u }.size).sum == 3,
        s"Expected the mid UDF to stay Python (3 PythonUDFs), got: $optimized")
    }
  }

  test("converts a nested transpiled UDF sitting at the root of the option") {
    transpileOn {
      // An option whose root is a substituted argument (a body that is nothing but
      // `_udf_param_0`), where that argument is itself a transpiled call. Recursing only into the
      // children would walk past the root and leave a TranspiledPythonUDF behind -- Unevaluable,
      // and this rule is the only one that strips them.
      //
      // The built-in transpiler casts every option to the UDF's return type
      // (`transpile.py: converted.cast(returnType)`), so its option roots are always Casts and it
      // cannot produce this shape; this guards the custom-transpiler path, whose only stated
      // contract is that the option's dataType already matches.
      val innerTPUDF = makeTPUDF(makePyUDF(attrA), Add(attrA, Literal(1L)))
      val outerTPUDF = makeTPUDF(makePyUDF(innerTPUDF),
        marker(innerTPUDF, 0, NamedExpression.newExprId))
      val result = ConvertToCatalyst.applyExpr(outerTPUDF, parentIsUdf = false)
      assert(!result.exists(_.isInstanceOf[TranspiledPythonUDF]),
        s"TranspiledPythonUDF survived at the option root: $result")
    }
  }

  test("the optimizer keeps a single evaluation of a pre-evaluated input") {
    // End to end: the argument is computed once in a Project below, the option reads it back as a
    // column, and nothing widens the output schema. ConvertToLocalRelation is excluded so the
    // projection stays.
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
      val arg = Rand(Literal(1L))
      val id = NamedExpression.newExprId
      val option =
        If(GreaterThan(marker(arg, 0, id), Literal(0.5)), marker(arg, 0, id), Literal(0.0))
      val tpudf = TranspiledPythonUDF(
        "udf",
        PythonUDF("udf", null, DoubleType, Seq(arg),
          PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
        List(option))
      val relation = LocalRelation.fromExternalRows(Seq(attrA), Seq(Row(1L)))
      val optimized = SimpleTestOptimizer.execute(Project(Seq(Alias(tpudf, "v")()), relation))
      val expressions = optimized.flatMap(_.expressions)
      assert(expressions.map(_.collect { case r: Rand => r }.size).sum == 1,
        s"Expected the argument to be evaluated once, got: $optimized")
      assert(optimized.output.map(_.name) == Seq("v"),
        s"Extra columns leaked into the output: $optimized")
      assert(!hasMarkers(optimized), s"A parameter marker survived: $optimized")
    }
  }
}
