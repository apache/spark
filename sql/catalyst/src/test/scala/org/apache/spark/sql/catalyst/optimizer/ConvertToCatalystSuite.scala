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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Sum}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, Filter, LocalRelation, LogicalPlan, LogicalPlanIntegrity, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, LongType}

/**
 * Unit tests for the ConvertToCatalyst optimizer rule, which rewrites
 * TranspiledPythonUDF nodes to their Catalyst equivalents.
 *
 * No JVM/Python bridge is required: the nodes are built by hand.
 *
 * Two entry points, and which one a test uses matters. `applyExpr` rewrites a bare expression with
 * argument pre-evaluation off, so it covers only the fallback of leaving each argument at its use
 * site. `convert` below goes through `apply`, which is the only caller that turns pre-evaluation on
 * and builds the Project -- so any test about a column has to use it, and it also runs the plan
 * checks the optimizer would have run for us.
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

  /** The pre-evaluated argument columns ConvertToCatalyst added anywhere in `plan`. */
  private def paramColumns(plan: LogicalPlan): Seq[Alias] =
    plan.collect { case Project(list, _) => list }.flatten.collect {
      case a: Alias if a.name.startsWith("_udf_param_") => a
    }

  /**
   * Runs the rule and checks the invariants the optimizer's own plan validation would, which does
   * not run when a rule object is applied directly. Both matter here: adding a column below an
   * operator is exactly how a rewrite strands a reference or widens a schema.
   */
  private def convert(plan: LogicalPlan): LogicalPlan = {
    val converted = ConvertToCatalyst(plan)
    LogicalPlanIntegrity.validateNoDanglingReferences(converted).foreach { failure =>
      fail(s"Dangling reference after conversion: $failure\n$converted")
    }
    // Only when the input plan is resolved: comparing schemas asks for every output attribute's
    // dataType, and a hand-built plan holding an unbound lambda has none to give.
    if (plan.resolved) {
      LogicalPlanIntegrity.validateSchemaOutput(plan, converted).foreach { failure =>
        fail(s"Schema changed by conversion: $failure\n$converted")
      }
    }
    // A reference left behind is Unevaluable and would throw at execution, so no test should have
    // to remember to check for it.
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
      // `lambda a, b: a * a + b` over `f(a + 1, a + 1)`: three references to two parameters, both
      // bound to the same deterministic argument. That is one column, read three times -- keyed on
      // the argument rather than the parameter, so equal arguments do not each get their own.
      val arg = Add(attrA, Literal(1L))
      val option = Add(Multiply(pref(0), pref(0)), pref(1))
      val pyUDF = PythonUDF("udf", null, LongType, Seq(arg, arg),
        PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)
      val tpudf = TranspiledPythonUDF("udf", pyUDF, List(option))
      val converted = convert(Project(Seq(Alias(tpudf, "v")()), LocalRelation(attrA)))
      val columns = paramColumns(converted)
      assert(columns.map(a => (a.name, a.child)) == Seq(("_udf_param_0", arg)),
        s"Expected one column for the shared argument: $converted")
      // By exprId, not by name: an alias whose id does not match what the body reads is exactly the
      // dangling-attribute bug this rewrite could have, and names would not show it.
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
      // Only a custom transpiler can emit an option body that is itself an aggregate (the built-in
      // one emits scalar bodies). Driven through `applyExpr`, so this covers the fallback path only
      // -- there is no operator that could host such a body in a Project anyway. The sibling test
      // above is the one that pins an aggregate *argument* being declined a column.
      val arg = Add(attrA, Literal(1L))
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

  test("leaves an argument inside a lambda at each use site") {
    transpileOn {
      // A lambda body runs per element where a Project below the operator runs per row, so sharing
      // in there would move a nondeterministic draw to the wrong granularity -- and make the
      // argument eager, raising under ANSI on a row whose array is empty and whose body never ran.
      // Driven through `apply` so pre-evaluation is actually ON for the operator -- via `applyExpr`
      // it defaults to off and the guard under test would be unobservable. The argument reads no
      // lambda variable, so every placement check passes and only the lambda guard declines it.
      val arg = Divide(attrA, attrA)
      val option = Add(pref(0), pref(0))
      val lambdaVar = NamedLambdaVariable("x", LongType, nullable = false)
      val call = makeTPUDF(makePyUDF(arg), option)
      val arr = AttributeReference("arr", ArrayType(LongType))()
      val body = ArrayTransform(arr, LambdaFunction(Add(call, lambdaVar), Seq(lambdaVar)))
      val converted = convert(
        Project(Seq(Alias(body, "v")()), LocalRelation(attrA, arr)))
      assert(paramColumns(converted).isEmpty, s"Expected no column inside a lambda: $converted")
      assert(converted.expressions.head.collect { case e if e == arg => e }.length == 2,
        s"Expected the argument left at both use sites: $converted")
    }
  }

  test("still pre-evaluates a call sitting beside an unrelated lambda") {
    transpileOn {
      // Pre-evaluation turns off on the way into a lambda, not for a whole expression that holds
      // one somewhere. A call outside the lambda is unaffected, so it still gets its column.
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

  test("leaves an argument carrying an outer reference at each use site with decorrelation off") {
    // A column holding an OuterReference lands inside the subquery. Decorrelation carries it back
    // out; its `decorrelateInnerQuery.enabled=false` fallback rewrites Filters only and strands it,
    // so under that config the argument stays at its use sites.
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
      assert(converted.expressions.head.find(_ == Multiply(arg, arg)).isDefined,
        s"Expected the argument at both use sites: $converted")
    }
  }

  test("leaves an argument that is itself an aggregate at each use site") {
    transpileOn {
      // `udf(sum(a))`: a Project cannot hold an aggregate, so there is nowhere to pre-evaluate it.
      // PlanHelper.specialExpressionsInUnsupportedOperator is what tells us, the same check
      // RewriteWithExpression uses.
      val arg = Sum(attrA).toAggregateExpression()
      val tpudf = makeTPUDF(makePyUDF(arg), Multiply(pref(0), pref(0)))
      val converted = convert(Project(Seq(Alias(tpudf, "v")()), LocalRelation(attrA)))
      assert(paramColumns(converted).isEmpty, s"Expected no column: $converted")
      assert(converted.expressions.head.find(_ == Multiply(arg, arg)).isDefined,
        s"Expected the aggregate at both use sites: $converted")
    }
  }

  test("leaves a Command's arguments at each use site") {
    transpileOn {
      // No pre-evaluated column under a Command: it has no output of its own, so there is nothing
      // to project the extra column away with, and the Project would land between DeleteFromTable
      // and its relation, hiding the relation DataSourceV2Strategy matches on -- an internal error
      // rather than a DELETE. The references still have to come off.
      val arg = Add(attrA, Literal(1L))
      val relation = LocalRelation(attrA)
      val option = Multiply(pref(0), pref(0))
      val tpudf = makeTPUDF(makePyUDF(arg), option)
      val converted = ConvertToCatalyst(DeleteFromTable(relation, GreaterThan(tpudf, Literal(0L))))
      assert(converted == DeleteFromTable(relation, GreaterThan(Multiply(arg, arg), Literal(0L))),
        s"Expected the argument left at both use sites: $converted")
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
      // A Filter takes its output from its child, so the widened child would change the plan's
      // schema. Only the restoring Project keeps it, and a Project-hosted call never exercises
      // that branch because its output length does not change.
      val arg = Add(attrA, Literal(1L))
      val relation = LocalRelation(attrA)
      val tpudf = makeTPUDF(makePyUDF(arg), Multiply(pref(0), pref(0)))
      val converted = convert(Filter(GreaterThan(tpudf, Literal(0L)), relation))
      assert(paramColumns(converted).map(_.child) == Seq(arg), s"Expected a column: $converted")
      assert(converted.output == relation.output,
        s"Expected the column projected away again: ${converted.output}")
    }
  }

  test("leaves an argument still holding an enclosing call's reference at each use site") {
    transpileOn {
      // The inverse nesting of the test below: here the OUTER option lowers to another transpiled
      // call whose argument is the outer call's own parameter reference. Only a custom transpiler
      // emits it. The inner call must decline a column: a reference is Unevaluable until the outer
      // call substitutes it, and it can only do that while the reference is still in the option
      // body -- aliased into a Project it would be stranded there and throw at execution.
      val arg = Add(attrA, Literal(1L))
      val innerCall = makeTPUDF(makePyUDF(pref(0)), Multiply(pref(0), pref(0)))
      val outerCall = makeTPUDF(makePyUDF(arg), innerCall)
      val converted = convert(Project(Seq(Alias(outerCall, "v")()), LocalRelation(attrA)))
      // One column, for the outer call's own argument; the inner call declined and inlined.
      assert(paramColumns(converted).map(_.child) == Seq(arg),
        s"Expected only the outer argument pre-evaluated: $converted")
    }
  }

  test("converts a nested transpiled UDF used as an argument") {
    transpileOn {
      // `f(g(a + 1))`, where `f`'s option is nothing but `_udf_param_0` and `g`'s repeats a
      // parameter of its own. The arguments are converted before being pre-evaluated, so `g`'s
      // argument gets the column and `f`'s -- which by then is `g`'s converted body, reading that
      // column -- cannot get one, since a Project cannot read an alias it is itself defining.
      val arg = Add(attrA, Literal(1L))
      val innerTPUDF = makeTPUDF(makePyUDF(arg), Multiply(pref(0), pref(0)))
      val outerTPUDF = makeTPUDF(makePyUDF(innerTPUDF), pref(0))
      val converted = convert(Project(Seq(Alias(outerTPUDF, "v")()), LocalRelation(attrA)))
      val columns = paramColumns(converted)
      assert(columns.map(_.child) == Seq(arg),
        s"Expected only the inner argument pre-evaluated: $converted")
      // Every column the plan reads has to be one the Project below actually defines.
      val defined = columns.map(_.toAttribute.exprId).toSet
      val read = converted.expressions.flatMap(_.collect {
        case r: AttributeReference if r.name.startsWith("_udf_param_") => r.exprId
      })
      assert(read.nonEmpty && read.forall(defined.contains),
        s"A read column was never defined: $converted")
    }
  }
}
