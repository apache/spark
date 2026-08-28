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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Sum}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DeleteFromTable, Filter, Join, JoinHint, LocalRelation, LogicalPlan, LogicalPlanIntegrity, Project, Sort}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, LongType}

/**
 * Unit tests for the ConvertToCatalyst optimizer rule, which rewrites
 * TranspiledPythonUDF nodes to their Catalyst equivalents.
 *
 * Nodes are built by hand, so no JVM/Python bridge is needed.
 *
 * Which entry point a test uses matters. `applyExpr` takes a bare expression with pre-evaluation
 * off, so it only covers the fallback of leaving arguments at their use sites. `convert` goes
 * through `apply`, the only caller that turns pre-evaluation on, so column tests need it.
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

  // A reference to the UDF's `index`th argument, typed the way the analyzer would type it.
  private def pref(index: Int): TranspiledUDFParameter =
    TranspiledUDFParameter(index, Some(LongType))

  // A nondeterministic argument, cast so it types like every other argument here. Nondeterministic
  // because Rand is: an expression is only deterministic if all of its children are.
  private def draw(seed: Long = 1L): Expression = Cast(Rand(Literal(seed)), LongType)

  /** The pre-evaluated argument columns ConvertToCatalyst added anywhere in `plan`. */
  private def paramColumns(plan: LogicalPlan): Seq[Alias] =
    plan.collect { case Project(list, _) => list }.flatten.collect {
      case a: Alias if a.name.startsWith("_udf_param_") => a
    }

  /**
   * Runs the rule, plus the plan checks the optimizer would have run for us -- applying a rule
   * object directly skips them, and stranding a reference or widening a schema is exactly how this
   * rewrite would go wrong.
   */
  private def convert(plan: LogicalPlan): LogicalPlan = {
    val converted = ConvertToCatalyst(plan)
    // What the real Optimizer checks around every rule when `spark.sql.planChangeValidation` is on
    // (it defaults to `Utils.isTesting`) and what applying a rule object directly skips: dangling
    // references, duplicate ExprIds, special expressions in the wrong operator, aggregate shape,
    // nullability and schema. Duplicate ExprIds is the one worth naming -- it is what catches a
    // rewrite that mints two aliases with one id.
    //
    // Dangling references get their own walk, on every plan and per operator, because
    // `validateNoDanglingReferences` is no help here: a resolved root matches its last case, so its
    // `collectFirst` returns there and never visits a child -- and the root is never the Project
    // this rule inserts.
    converted.foreach {
      case n if n.resolved && n.children.nonEmpty =>
        assert(n.missingInput.isEmpty,
          s"${n.missingInput.mkString(", ")} dangling in ${n.nodeName}:\n$converted")
      case _ =>
    }
    // The rest of it on resolved plans only: the full check asks every output attribute for its
    // dataType, and a hand-built plan with an unbound lambda hasn't got one.
    if (plan.resolved) {
      LogicalPlanIntegrity.validateOptimizedPlan(plan, converted, lightweight = false)
        .foreach(failure => fail(s"$failure\n$converted"))
    }
    // A leftover reference is Unevaluable and throws at execution. Check it here so no test has to
    // remember to.
    assert(!converted.exists(_.expressions.exists(_.exists {
      case _: TranspiledPythonUDF | _: TranspiledUDFParameter => true
      case _ => false
    })), s"A transpiled node survived conversion: $converted")
    converted
  }

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
    // An aggregate UDF's arguments sit under the AggregateExpression, not beside it: reading
    // pythonUDFExpr.children saw (aggregateFunction, filter) and said false for every UDAF.
    val aggOverPyUDF = makePyUDAF(innerPyUDF).toAggregateExpression()
    assert(TranspiledPythonUDF("agg", aggOverPyUDF, List(catalystExpr)).hasOnlyPythonUDFInputs)
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

  test("computes one column per distinct argument, read by every reference") {
    transpileOn {
      // `lambda a, b: a * a + b` over `f(a + 1, a + 1)`: three refs, two parameters, one argument.
      // Keyed on the argument, so that's one column read three times.
      val arg = Add(attrA, Literal(1L))
      val option = Add(Multiply(pref(0), pref(0)), pref(1))
      val pyUDF = PythonUDF("udf", null, LongType, Seq(arg, arg),
        PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)
      val tpudf = TranspiledPythonUDF("udf", pyUDF, List(option))
      val converted = convert(Project(Seq(Alias(tpudf, "v")()), LocalRelation(attrA)))
      val columns = paramColumns(converted)
      assert(columns.map(_.child) == Seq(arg),
        s"Expected one column for the shared argument: $converted")
      // By exprId, not name -- an alias whose id doesn't match what the body reads is the dangling
      // attribute bug, and names wouldn't show it.
      val reads = converted.expressions.head.collect {
        case r: AttributeReference if r.name.startsWith("_udf_param_") => r.exprId
      }
      assert(reads == Seq.fill(3)(columns.head.toAttribute.exprId),
        s"Expected all three references to read that one column: $converted")
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

  test("drops an input the option never uses") {
    transpileOn {
      // `lambda a, b: a` over f(a, rand()): substitution dropped b, so nothing evaluates it. The
      // Python path computes every argument column -- an accepted difference, pinned here.
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

  test("leaves an argument an aggregate function wraps at each use site") {
    transpileOn {
      // Only a custom transpiler emits an option body that is itself an aggregate. Through
      // `applyExpr`, so this is the fallback path only -- nothing could host such a body anyway.
      //
      // A bare column, since anything more is owed one evaluation and would keep the Python UDF
      // here rather than substitute.
      val arg = attrA
      val option = Count(Seq(Multiply(pref(0), pref(0)))).toAggregateExpression()
      val tpudf = makeTPUDF(makePyUDF(arg), option)
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      // Compare the aggregate function, not the AggregateExpression: `toAggregateExpression` mints
      // a fresh resultId per call, so the wrappers never compare equal.
      assert(result.asInstanceOf[AggregateExpression].aggregateFunction ==
        Count(Seq(Multiply(arg, arg))),
        s"Expected the argument left at both use sites: $result")
    }
  }

  test("keeps the Python UDF inside a higher-order function's lambda") {
    transpileOn {
      // Lambdas are out of scope for lowering, whatever the argument costs:
      // ExtractPythonUDFFromLambda already applies a Python UDF over the whole array and this rule
      // leaves that to it. Through `apply`, since `applyExpr` alone never sees the lambda.
      Seq(attrA, Divide(attrA, attrA), draw()).foreach { arg =>
        val call = makeTPUDF(makePyUDF(arg), Add(pref(0), pref(0)))
        val lambdaVar = NamedLambdaVariable("x", LongType, nullable = false)
        val arr = AttributeReference("arr", ArrayType(LongType))()
        val body = ArrayTransform(arr, LambdaFunction(Add(call, lambdaVar), Seq(lambdaVar)))
        val converted = convert(Project(Seq(Alias(body, "v")()), LocalRelation(attrA, arr)))
        assert(paramColumns(converted).isEmpty, s"Expected no column for $arg: $converted")
        assert(converted.expressions.head.collect { case u: PythonUDF => u }.length == 1,
          s"Expected the Python UDF kept for $arg: $converted")
        assert(converted.expressions.head.collect { case e if e == arg => e }.length == 1,
          s"Expected the one evaluation the Python UDF makes for $arg: $converted")
      }
    }
  }

  test("keeps the Python UDF when the option body builds a lambda") {
    transpileOn {
      // The other half of the same rule, which only a custom transpiler can reach: an option that
      // lowers to a higher-order function of its own. One place to reason about lambdas, not two.
      val param = TranspiledUDFParameter(0, Some(ArrayType(LongType)))
      val lambdaVar = NamedLambdaVariable("x", LongType, nullable = false)
      val option = Cast(
        Size(ArrayTransform(param, LambdaFunction(Add(lambdaVar, Literal(1L)), Seq(lambdaVar)))),
        LongType)
      val arr = AttributeReference("arr", ArrayType(LongType))()
      val call = makeTPUDF(makePyUDF(arr), option)
      val converted = convert(Project(Seq(Alias(call, "v")()), LocalRelation(arr)))
      assert(converted.expressions.head.exists(_.isInstanceOf[PythonUDF]),
        s"Expected the Python UDF kept: $converted")
    }
  }

  test("gives one call in two of an operator's slots a single column") {
    transpileOn {
      // The same node, twice in one Sort. An operator can do this on its own -- Window copies its
      // spec into every WindowSpecDefinition, MergeRows has five slots -- and each slot converts
      // separately, so an id minted per visit gave this call two columns and two draws.
      val call = makeTPUDF(makePyUDF(draw()), Add(pref(0), pref(0)))
      val converted = convert(Sort(
        Seq(SortOrder(call, Ascending), SortOrder(Multiply(call, Literal(2L)), Descending)),
        global = true,
        Project(Seq(Alias(attrA, "a")()), LocalRelation(attrA))))
      assert(paramColumns(converted).length == 1,
        s"Expected one column for one call: $converted")
    }
  }

  test("gives two calls' nondeterministic arguments a column each") {
    transpileOn {
      // Two calls in one Project, each reading its own draw twice. Keying a column on the parameter
      // index alone collided them and put both bodies on one draw.
      val option = Add(pref(0), pref(0))
      def call(seed: Long): TranspiledPythonUDF = makeTPUDF(makePyUDF(draw(seed)), option)
      val converted = convert(
        Project(Seq(Alias(call(1L), "x")(), Alias(call(2L), "y")()), LocalRelation(attrA)))
      assert(paramColumns(converted).length == 2, s"Expected one column per call: $converted")
    }
  }

  test("keeps the Python UDF under an Aggregate, where no column can go") {
    transpileOn {
      // An Aggregate holds no Project of its own -- a result expression no aggregate function wraps
      // has to be built from the grouping expressions -- so a call owed an evaluation goes back to
      // Python. Both shapes: inside an aggregate function and beside one.
      val call = makeTPUDF(makePyUDF(Add(attrA, Literal(1L))), Add(pref(0), pref(0)))
      val insideSum = Aggregate(Seq(attrA),
        Seq(Alias(attrA, "a")(), Alias(Sum(call).toAggregateExpression(), "s")()),
        LocalRelation(attrA))
      val beside = Aggregate(Seq(attrA),
        Seq(Alias(attrA, "a")(), Alias(call, "v")()), LocalRelation(attrA))
      Seq(insideSum, beside).foreach { agg =>
        val converted = convert(agg)
        assert(paramColumns(converted).isEmpty, s"Expected no column: $converted")
        assert(converted.expressions.exists(_.exists(_.isInstanceOf[PythonUDF])),
          s"Expected the Python UDF kept: $converted")
      }
    }
  }

  test("puts one argument in a column though another can't go in one") {
    transpileOn {
      // `f(sum(a), a + 1)`, the aggregate read once and `a + 1` twice: no Project can hold the
      // aggregate, which is no reason to leave `a + 1` at both use sites too.
      val aggArg = Sum(attrA).toAggregateExpression()
      val plainArg = Add(attrA, Literal(1L))
      val option = Add(pref(0), Multiply(pref(1), pref(1)))
      val pyUDF = PythonUDF("udf", null, LongType, Seq(aggArg, plainArg),
        PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)
      val converted = convert(Project(
        Seq(Alias(TranspiledPythonUDF("udf", pyUDF, List(option)), "v")()), LocalRelation(attrA)))
      assert(paramColumns(converted).map(_.child) == Seq(plainArg),
        s"Expected a column for the argument that can have one: $converted")
    }
  }

  test("keeps the Python UDF in a Join condition, where no column can go") {
    transpileOn {
      // A join has no side to compute on, so an argument owed one evaluation goes back to Python --
      // which is what a UDF predicate in a join condition gets without transpilation anyway.
      // Repeating it instead would make it lazy again and swallow an ANSI error the interpreted UDF
      // raises.
      val arg = Add(attrA, Literal(1L))
      val attrB = AttributeReference("b", LongType)()
      val call = makeTPUDF(makePyUDF(arg), Multiply(pref(0), pref(0)))
      val converted = convert(Join(LocalRelation(attrA), LocalRelation(attrB), Inner,
        Some(GreaterThan(call, Literal(0L))), JoinHint.NONE))
      assert(paramColumns(converted).isEmpty, s"Expected no column under a join: $converted")
      assert(converted.expressions.exists(_.exists(_.isInstanceOf[PythonUDF])),
        s"Expected the Python UDF kept: $converted")
      assert(converted.expressions.head.collect { case e if e == arg => e }.length == 1,
        s"Expected the one evaluation the Python UDF makes: $converted")
    }
  }

  test("keeps the Python UDF when an analyzer rule has taken the arguments away") {
    transpileOn {
      // PullOutNondeterministic replaces a nondeterministic call with an attribute for the
      // projection it moved the call to, leaving no arguments for the option's references. Shaped
      // by hand here; `orderBy(u(rand()))` is the query that produces it.
      val stripped = TranspiledPythonUDF(
        "udf", AttributeReference("_nondeterministic", LongType)(), List(Add(pref(0), pref(0))))
      val result = ConvertToCatalyst.applyExpr(stripped, parentIsUdf = false)
      assert(result == stripped.pythonUDFExpr,
        s"Expected the attribute the analyzer left, got: $result")
    }
  }

  test("still pre-evaluates a call sitting beside an unrelated lambda") {
    transpileOn {
      // Pre-evaluation turns off going into a lambda, not for any expression that holds one, so a
      // call outside it still gets a column.
      val arg = Add(attrA, Literal(1L))
      val call = makeTPUDF(makePyUDF(arg), Multiply(pref(0), pref(0)))
      val lambdaVar = NamedLambdaVariable("x", LongType, nullable = false)
      val arr = AttributeReference("arr", ArrayType(LongType))()
      val unrelated = ArrayTransform(arr, LambdaFunction(lambdaVar, Seq(lambdaVar)))
      val converted = convert(
        Project(Seq(Alias(Add(call, Size(unrelated)), "v")()), LocalRelation(attrA, arr)))
      assert(paramColumns(converted).map(_.child) == Seq(arg),
        s"Expected the call outside the lambda to keep its column: $converted")
    }
  }

  test("keeps the Python UDF for an argument carrying an outer reference, decorrelation off") {
    // A column holding an OuterReference lands inside the subquery. Decorrelation carries it out;
    // the fallback rewrites Filters only and would strand it, so with that off we turn it down.
    val arg = Add(OuterReference(attrA), Literal(1L))
    val tpudf = makeTPUDF(makePyUDF(arg), Multiply(pref(0), pref(0)))
    val plan = Project(Seq(Alias(tpudf, "v")()), LocalRelation(attrA))
    transpileOn {
      assert(paramColumns(convert(plan)).map(_.child) == Seq(arg),
        "Expected a column when decorrelation is on")
    }
    withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "true",
        SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true",
        SQLConf.DECORRELATE_INNER_QUERY_ENABLED.key -> "false") {
      val converted = convert(plan)
      assert(paramColumns(converted).isEmpty, s"Expected no column: $converted")
      // And with nowhere to put it, the call is left to Python rather than evaluating `a + 1`
      // twice per row.
      assert(converted.expressions.head.exists(_.isInstanceOf[PythonUDF]),
        s"Expected the Python UDF kept: $converted")
    }
  }

  test("keeps the Python UDF for an argument that is itself an aggregate") {
    transpileOn {
      // `udf(sum(a))` read twice: no Project can hold an aggregate, and running it at each use site
      // is not the answer either, so the call keeps the Python UDF. Under a real Aggregate the
      // operator itself is on the deny list, so the same call never gets this far.
      val arg = Sum(attrA).toAggregateExpression()
      val tpudf = makeTPUDF(makePyUDF(arg), Multiply(pref(0), pref(0)))
      val converted = convert(Project(Seq(Alias(tpudf, "v")()), LocalRelation(attrA)))
      assert(paramColumns(converted).isEmpty, s"Expected no column: $converted")
      assert(converted.expressions.head.exists(_.isInstanceOf[PythonUDF]),
        s"Expected the Python UDF kept: $converted")
      assert(converted.expressions.head.collect { case a: AggregateExpression => a }.length == 1,
        s"Expected the one aggregate the Python UDF reads: $converted")
    }
  }

  test("leaves a Command's cheap arguments at each use site") {
    transpileOn {
      // No column under a Command: the Project would land between DeleteFromTable and its relation,
      // hiding what DataSourceV2Strategy matches on. A bare column costs nothing to read twice, so
      // the call still lowers; anything more keeps the Python UDF.
      val arg = attrA
      val relation = LocalRelation(attrA)
      val option = Multiply(pref(0), pref(0))
      val tpudf = makeTPUDF(makePyUDF(arg), option)
      val converted = ConvertToCatalyst(DeleteFromTable(relation, GreaterThan(tpudf, Literal(0L))))
      assert(converted == DeleteFromTable(relation, GreaterThan(Multiply(arg, arg), Literal(0L))),
        s"Expected the argument left at both use sites: $converted")

      val costly = makeTPUDF(makePyUDF(Add(attrA, Literal(1L))), option)
      val fellBack =
        ConvertToCatalyst(DeleteFromTable(relation, GreaterThan(costly, Literal(0L))))
      assert(fellBack.expressions.exists(_.exists(_.isInstanceOf[PythonUDF])),
        s"Expected the Python UDF kept for `a + 1`: $fellBack")
    }
  }

  test("apply keeps a transpilable UDF Python when wrapped by a non-transpiled Python UDF") {
    // `apply` threads parentIsUdf down from the top of each expression, so the mid UDF stays Python
    // rather than splitting the batch pipeline into Python -> Catalyst -> Python. The applyExpr
    // tests above pass parentIsUdf in directly and so bypass that threading.
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

  test("projects the pre-evaluated column away again below a Filter") {
    transpileOn {
      // A Filter takes its output from its child, so without the restoring Project the schema
      // would change. A Project-hosted call never reaches that branch, hence a Filter here.
      val arg = Add(attrA, Literal(1L))
      val relation = LocalRelation(attrA)
      val tpudf = makeTPUDF(makePyUDF(arg), Multiply(pref(0), pref(0)))
      val converted = convert(Filter(GreaterThan(tpudf, Literal(0L)), relation))
      assert(paramColumns(converted).map(_.child) == Seq(arg), s"Expected a column: $converted")
      assert(converted.output == relation.output,
        s"Expected the column projected away again: ${converted.output}")
    }
  }

  test("adds no column for an argument the body never reads") {
    transpileOn {
      // `f(g(a + 1), a)` where f's body only ever reads parameter 1. Converting g would register a
      // column for `a + 1` that nothing reads -- harmless enough until you remember `transpile.py`
      // promises an unused argument is not computed at all, and that under ANSI the column runs and
      // can raise. So we only convert the arguments the body actually asks for.
      val unread = Add(attrA, Literal(1L))
      val inner = makeTPUDF(makePyUDF(unread), Multiply(pref(0), pref(0)))
      val pyUDF = PythonUDF("udf", null, LongType, Seq(inner, attrA),
        PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)
      val outer = TranspiledPythonUDF("udf", pyUDF, List(Add(pref(1), pref(1))))
      val converted = convert(Project(Seq(Alias(outer, "v")()), LocalRelation(attrA)))
      assert(paramColumns(converted).forall(_.child != unread),
        s"Pre-evaluated an argument the body never reads: $converted")
    }
  }

  test("leaves an argument still holding an enclosing call's reference at each use site") {
    transpileOn {
      // Inverse nesting of the test below: the outer option lowers to another transpiled call whose
      // argument is the outer call's own parameter ref. Custom transpilers only. The inner call has
      // to give up -- aliased into a Project that ref is stranded where nobody can substitute it.
      val arg = Add(attrA, Literal(1L))
      val innerCall = makeTPUDF(makePyUDF(pref(0)), Multiply(pref(0), pref(0)))
      val outerCall = makeTPUDF(makePyUDF(arg), innerCall)
      val converted = convert(Project(Seq(Alias(outerCall, "v")()), LocalRelation(attrA)))
      // One column, for the outer call's own argument; the inner call gave up and inlined.
      assert(paramColumns(converted).map(_.child) == Seq(arg),
        s"Expected only the outer argument pre-evaluated: $converted")
    }
  }

  test("converts a nested transpiled UDF used as an argument") {
    transpileOn {
      // `f(g(a + 1))`, `f`'s option just `_udf_param_0` and `g`'s repeating its own parameter. We
      // convert arguments before pre-evaluating, so `g`'s argument gets the column and `f`'s -- by
      // then `g`'s body reading it -- can't, since a Project can't read an alias it's defining.
      val arg = Add(attrA, Literal(1L))
      val innerTPUDF = makeTPUDF(makePyUDF(arg), Multiply(pref(0), pref(0)))
      val outerTPUDF = makeTPUDF(makePyUDF(innerTPUDF), pref(0))
      val converted = convert(Project(Seq(Alias(outerTPUDF, "v")()), LocalRelation(attrA)))
      val columns = paramColumns(converted)
      assert(columns.map(_.child) == Seq(arg),
        s"Expected only the inner argument pre-evaluated: $converted")
      // Everything read has to be something the Project below actually defines.
      val defined = columns.map(_.toAttribute.exprId).toSet
      val read = converted.expressions.flatMap(_.collect {
        case r: AttributeReference if r.name.startsWith("_udf_param_") => r.exprId
      })
      assert(read.nonEmpty && read.forall(defined.contains),
        s"A read column was never defined: $converted")
    }
  }

  test("substitute leaves a nested call's own options alone") {
    // A nested call's references count against *its* arguments, so ours must not touch them.
    // `referencedIndexes` and `resolveTypes` both stop there; `substitute` used to walk in, and
    // reading a nested index as ours means an out-of-range argument or, worse, the wrong one.
    // The rule converts a nested call before substituting so it never hits this, but `substitute`
    // is what a custom transpiler's own rule calls.
    val inner = makeTPUDF(makePyUDF(attrA), Add(pref(1), Literal(2L)))
    val option = Add(pref(0), inner)
    val substituted = TranspiledUDFParameter.substitute(option, Seq[Expression](attrA))
    val nested = substituted.collectFirst { case n: TranspiledPythonUDF => n }
    assert(nested.map(_.transpiledOptions).contains(inner.transpiledOptions),
      s"A nested call's options were substituted out of our index space: $substituted")
    assert(substituted.asInstanceOf[Add].left == attrA,
      s"Our own reference was not substituted: $substituted")
  }

  test("an option reading an argument the call hasn't got is a classed error") {
    // Only a hand-built option gets here -- the transpiler bounds-checks what it emits, and a
    // pre-typed reference slips past the analyzer's check as well. Left to resolve on its own the
    // node never does, and CheckAnalysis blames an internal error, which tells nobody anything.
    val readsTooFar = makeTPUDF(makePyUDF(attrA), Add(pref(1), Literal(1L)))
    transpileOn {
      checkError(
        exception = intercept[AnalysisException] {
          convert(Project(Seq(Alias(readsTooFar, "v")()), LocalRelation(attrA)))
        },
        condition = "INVALID_UDF_PARAMETER_PLACEHOLDER_INDEX",
        parameters = Map("index" -> "1", "numParams" -> "1"))
    }

    // Same error from the analyzer's side, where the type comes off the argument.
    checkError(
      exception = intercept[AnalysisException] {
        TranspiledUDFParameter.resolveTypes(TranspiledUDFParameter(2, None), Seq(attrA))
      },
      condition = "INVALID_UDF_PARAMETER_PLACEHOLDER_INDEX",
      parameters = Map("index" -> "2", "numParams" -> "1"))
  }
}
