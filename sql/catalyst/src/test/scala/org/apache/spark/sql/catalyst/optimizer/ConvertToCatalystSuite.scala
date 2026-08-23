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
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, Filter, LocalRelation, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, LongType}

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

  test("gives each parameter one common expression, shared by every copy") {
    transpileOn {
      // `lambda a, b: a * a + b` over `f(a + 1, a + 1)`: two parameters bound to equal arguments,
      // so shape alone cannot tell the three copies apart -- only the marker indexes can. Two
      // parameters means two common expressions, and `a`'s two copies share one of them.
      val arg = Add(attrA, Literal(1L))
      val option = Add(
        Multiply(TranspiledUDFParameter(arg, 0), TranspiledUDFParameter(arg, 0)),
        TranspiledUDFParameter(arg, 1))
      val tpudf = makeTPUDF(makePyUDF(arg), option)
      ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false) match {
        case With(child, defs) =>
          assert(defs.map(_.child) == Seq(arg, arg), s"Expected one def per parameter: $defs")
          // Two refs to a's common expression and one to b's, in the option body's order.
          val refIds = child.collect { case r: CommonExpressionRef => r.id }
          assert(refIds == Seq(defs.head.id, defs.head.id, defs(1).id),
            s"Expected a's copies to share one common expr and b to get its own: $child")
        case other => fail(s"Expected the option wrapped in a With, got: $other")
      }
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
      // A `With` may not have an aggregate function wrapping a ref to one of its own common
      // expressions -- RewriteWithExpression pulls the aggregate out and the ref is left dangling,
      // and `With` asserts against it. Only a custom transpiler can emit this shape (the built-in
      // one emits scalar bodies), so this pins that we notice rather than trip the assert.
      val arg = Add(attrA, Literal(1L))
      val option = Count(Seq(Multiply(
        TranspiledUDFParameter(arg, 0), TranspiledUDFParameter(arg, 0)))).toAggregateExpression()
      val tpudf = makeTPUDF(makePyUDF(arg), option)
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      // Compare the aggregate function, not the AggregateExpression: `toAggregateExpression` mints
      // a fresh resultId per call, so the wrappers never compare equal.
      assert(result.asInstanceOf[AggregateExpression].aggregateFunction ==
        Count(Seq(Multiply(arg, arg))),
        s"Expected the argument left at both use sites, no With: $result")
    }
  }

  test("leaves an argument inside a lambda at each use site") {
    transpileOn {
      // A lambda body runs per element where a Project below the operator runs per row, so sharing
      // in there would move a nondeterministic draw to the wrong granularity -- and make the
      // argument eager, raising under ANSI on a row whose array is empty and whose body never ran.
      // The argument reads no lambda variable, so RewriteWithExpression would happily place it.
      // Checked through `applyExpr`, not just `apply`: `applyExpr` is public and a custom
      // transpiler's ConvertToX may call it with `shareArguments` left at its default, so the guard
      // has to live in the recursion rather than at the operator.
      val arg = Divide(attrA, attrA)
      val option = Add(TranspiledUDFParameter(arg, 0), TranspiledUDFParameter(arg, 0))
      val lambdaVar = NamedLambdaVariable("x", LongType, nullable = false)
      val call = makeTPUDF(makePyUDF(arg), option)
      val arr = AttributeReference("arr", ArrayType(LongType))()
      val body = ArrayTransform(arr, LambdaFunction(Add(call, lambdaVar), Seq(lambdaVar)))
      val result = ConvertToCatalyst.applyExpr(body, parentIsUdf = false)
      assert(!result.exists(_.isInstanceOf[With]), s"Expected no With inside a lambda: $result")
      assert(result.collect { case e if e == arg => e }.length == 2,
        s"Expected the argument left at both use sites: $result")
    }
  }

  test("still shares for a call sitting beside an unrelated lambda") {
    transpileOn {
      // The lambda guard turns off on the way into a lambda, not for any expression that holds one
      // somewhere. A call outside the lambda is unaffected, so it keeps its common expression.
      val arg = Add(attrA, Literal(1L))
      val option = Multiply(TranspiledUDFParameter(arg, 0), TranspiledUDFParameter(arg, 0))
      val call = makeTPUDF(makePyUDF(arg), option)
      val lambdaVar = NamedLambdaVariable("x", LongType, nullable = false)
      val arr = AttributeReference("arr", ArrayType(LongType))()
      val unrelated = ArrayTransform(arr, LambdaFunction(lambdaVar, Seq(lambdaVar)))
      val result = ConvertToCatalyst.applyExpr(Add(call, Size(unrelated)), parentIsUdf = false)
      assert(result.exists(_.isInstanceOf[With]),
        s"Expected the call outside the lambda to keep sharing: $result")
    }
  }

  test("leaves an argument carrying an outer reference at each use site") {
    transpileOn {
      // Decorrelation carries a pre-evaluated OuterReference out of the subquery, but only while it
      // is enabled: with `decorrelateInnerQuery.enabled=false` the fallback rewrites Filters only
      // and it is stranded in the Project. `EXISTS` over an outer argument fails that way with
      // sharing on and passes with it off, so we decline.
      val arg = Add(OuterReference(attrA), Literal(1L))
      val option = Multiply(TranspiledUDFParameter(arg, 0), TranspiledUDFParameter(arg, 0))
      val tpudf = makeTPUDF(makePyUDF(arg), option)
      val result = ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false)
      assert(result == Multiply(arg, arg), s"Expected no With: $result")
    }
  }

  test("shares an argument whose own subtree holds an aggregate") {
    transpileOn {
      // `udf(sum(a))`: the aggregate is below the marker, not above it, so it does not hit the
      // `With` assert. RewriteWithExpression inlines it anyway since a Project cannot hold an
      // aggregate, but that is its call, not ours.
      val arg = Sum(attrA).toAggregateExpression()
      val option = Multiply(TranspiledUDFParameter(arg, 0), TranspiledUDFParameter(arg, 0))
      val tpudf = makeTPUDF(makePyUDF(arg), option)
      ConvertToCatalyst.applyExpr(tpudf, parentIsUdf = false) match {
        case With(_, defs) => assert(defs.length == 1, s"Expected one def: $defs")
        case other => fail(s"Expected a With, got: $other")
      }
    }
  }

  test("leaves a Command's arguments at each use site") {
    transpileOn {
      // No `With` under a Command: it has no output of its own, so RewriteWithExpression cannot
      // project away the extra column, and the Project it would add between DeleteFromTable and
      // its relation hides the relation DataSourceV2Strategy matches on -- an internal error
      // rather than a DELETE. The markers still have to come off.
      val arg = Add(attrA, Literal(1L))
      val relation = LocalRelation(attrA)
      val option = Multiply(TranspiledUDFParameter(arg, 0), TranspiledUDFParameter(arg, 0))
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

  test("converts a nested transpiled UDF inside a marked argument") {
    transpileOn {
      // An outer option that is nothing but `_udf_param_0`, bound to a transpiled call that repeats
      // a parameter of its own. Conversion is depth-first, so the inner option's markers are
      // already common expression refs when the outer option's are collected -- which is why
      // `shareParameters` never has to look for a marker inside a marked argument.
      val arg = Add(attrA, Literal(1L))
      val innerOption = Multiply(TranspiledUDFParameter(arg, 0), TranspiledUDFParameter(arg, 0))
      val innerTPUDF = makeTPUDF(makePyUDF(arg), innerOption)
      val outerTPUDF = makeTPUDF(makePyUDF(innerTPUDF), TranspiledUDFParameter(innerTPUDF, 0))
      val result = ConvertToCatalyst.applyExpr(outerTPUDF, parentIsUdf = false)
      assert(!result.exists(_.isInstanceOf[TranspiledPythonUDF]),
        s"TranspiledPythonUDF survived at the option root: $result")
      assert(!result.exists(_.isInstanceOf[TranspiledUDFParameter]),
        s"A marker survived: $result")
      // The outer With's definition is the inner call's own With, and every ref resolves inside the
      // With that defines it -- no ref left over from the inner one.
      val withs = result.collect { case w: With => w }
      assert(withs.length == 2, s"Expected a With per call, got: $result")
      assert(withs.forall(w => w.child.collect { case r: CommonExpressionRef => r.id }
        .forall(w.defs.map(_.id).contains)), s"A ref outlived its definition: $result")
    }
  }
}
