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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Max, Sum}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DeleteFromTable, Filter, Join,
  JoinHint, LocalRelation, LogicalPlan, Project, Sort}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, LongType, MapType, StringType}

/**
 * Tests that every input a transpiled UDF option uses is evaluated once per row (SPARK-58626).
 *
 * The rewrite under test is [[PreEvaluateTranspiledUDFInputs]], which `ConvertToCatalyst` runs on
 * each plan node once that node's options have been substituted, so these tests drive it through
 * `ConvertToCatalyst` -- the shape of the plan it produces is the contract.
 *
 * `UserDefinedPythonFunction.builder` puts a [[TranspiledUDFParameter]] on every copy of every
 * non-foldable argument an option uses, with one id per parameter per call; `marker` stands in
 * for it. Which copies share a column is the interesting part: all copies of one parameter do, and
 * so do two parameters bound to the same deterministic argument, but two parameters bound to
 * separately-drawn `rand()`s do not -- Python would compute two columns there.
 */
class PreEvaluateTranspiledUDFInputsSuite extends PlanTest {

  private val attrA = $"a".long
  private val attrB = $"b".long
  private val attrArr = $"arr".array(LongType)
  private val attrStruct = $"s".struct($"f".long)
  private val attrMap = AttributeReference("m", MapType(StringType, LongType))()
  private val relation = LocalRelation(attrA, attrB, attrArr, attrStruct, attrMap)

  // ---- helpers ----

  private def marker(arg: Expression, index: Int, id: ExprId): Expression =
    TranspiledUDFParameter(arg, index, id)

  private def newId: ExprId = NamedExpression.newExprId

  /** A transpiled call over `args` whose single option is `option`, returning `dt`. */
  private def tpudf(option: Expression, dt: DataType, args: Expression*): TranspiledPythonUDF =
    TranspiledPythonUDF(
      "udf",
      PythonUDF("udf", null, dt, args,
        PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
      List(option))

  /** An option using one argument twice, as `lambda x: x * x` would. */
  private def usedTwice(arg: Expression, index: Int = 0, id: ExprId = newId): Expression =
    Multiply(marker(arg, index, id), marker(arg, index, id))

  private def convert(plan: LogicalPlan): LogicalPlan =
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true") {
      val converted = ConvertToCatalyst(plan)
      assert(!converted.exists(_.expressions.exists(_.exists {
        case _: TranspiledPythonUDF | _: TranspiledUDFParameter => true
        case _ => false
      })), s"A transpiled node or parameter marker survived: $converted")
      converted
    }

  /** The columns the rewrite added, in plan order. */
  private def preEvaluated(plan: LogicalPlan): Seq[Alias] = plan.collect {
    case Project(projectList, _) =>
      projectList.collect {
        case a: Alias if a.name.startsWith(PreEvaluateTranspiledUDFInputs.INPUT_ALIAS_PREFIX) => a
      }
  }.flatten

  /** How many times `plan` evaluates something matching `f` (as opposed to reading its column). */
  private def countEvaluations(plan: LogicalPlan)(f: Expression => Boolean): Int =
    plan.expressions.map(_.collect { case e if f(e) => e }.size).sum +
      plan.children.map(countEvaluations(_)(f)).sum

  /** A one-column Project over `relation` selecting `e`. */
  private def select(e: Expression): LogicalPlan = Project(Seq(Alias(e, "v")()), relation)

  /**
   * The property that makes an Aggregate valid, and that pre-evaluating one side of a grouping
   * expression would break: everything no aggregate function wraps reads only grouping
   * expressions. Analysis rejects an Aggregate that fails this.
   */
  private def assertGroupByIntact(agg: Aggregate): Unit = {
    val nonAggregating = agg.aggregateExpressions
      .filterNot(_.exists(_.isInstanceOf[AggregateExpression]))
    assert(nonAggregating.forall(_.references.subsetOf(
      AttributeSet(agg.groupingExpressions.flatMap(_.references)))),
      s"A non-aggregating expression escaped the GROUP BY: $agg")
  }

  /** `plan` through the whole optimizer, to see what a query really gets. */
  private def optimize(plan: LogicalPlan): LogicalPlan =
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true",
      // Otherwise the one-row local relation is folded away and there is no plan left to look at.
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
      SimpleTestOptimizer.execute(plan)
    }

  // ---- what gets a column ----

  test("pre-evaluates a repeated input in a Project below the operator") {
    val arg = Add(attrA, Literal(1L))
    val optimized = convert(select(tpudf(usedTwice(arg), LongType, arg)))
    val inputs = preEvaluated(optimized)
    assert(inputs.map(_.child) == Seq(arg), s"Expected one column for a + 1, got: $optimized")
    val input = inputs.head.toAttribute
    assert(optimized.asInstanceOf[Project].projectList.map(_.asInstanceOf[Alias].child) ==
      Seq(Multiply(input, input)), s"Uses did not read the column: $optimized")
    assert(countEvaluations(optimized)(_ == arg) == 1, s"Not a single evaluation: $optimized")
  }

  test("pre-evaluates an input used only once") {
    // The Python eval operator computes a column per argument whether the body uses it once or
    // twice, so a single use is pre-evaluated too rather than left inline in the body.
    val arg = Rand(Literal(1L))
    val option = Add(marker(arg, 0, newId), Literal(1.0))
    val optimized = convert(select(tpudf(option, DoubleType, arg)))
    assert(preEvaluated(optimized).map(_.child) == Seq(arg),
      s"Expected the single-use input pre-evaluated, got: $optimized")
  }

  test("keeps the pre-evaluation when the call sits in a conditional branch") {
    // A `With` expression would be inlined back into the branch here (RewriteWithExpression cannot
    // pull out of a branch that might not run), which is why this rewrite builds the Project
    // itself.
    // The input becomes eager, which is what the interpreted UDF does.
    val arg = Add(attrA, Literal(1L))
    val call = tpudf(usedTwice(arg), LongType, arg)
    val optimized = convert(select(If(GreaterThan(attrB, Literal(0L)), call, Literal(0L))))
    assert(preEvaluated(optimized).map(_.child) == Seq(arg),
      s"Expected the input pre-evaluated despite the branch, got: $optimized")
    assert(countEvaluations(optimized)(_ == arg) == 1, s"Not a single evaluation: $optimized")
  }

  // Which arguments are cheap enough to leave alone; isCheapInput has the reasoning, including why
  // this is not CollapseProject.isCheap.
  namedGridTest[(Expression, Boolean)](
    "decides whether an input is cheap enough to leave inline:")(Map(
    "a column" -> (attrA, false),
    "a foldable expression" -> (Add(Literal(2L), Literal(3L)), false),
    "a struct field" -> (GetStructField(attrStruct, 0), false),
    "a map probe" -> (GetMapValue(attrMap, Literal("k")), true),
    "a Python call" -> (PythonUDF("inner", null, LongType, Seq(attrA),
      PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true), true))) {
    case (arg, expectColumn) =>
      val optimized = convert(select(tpudf(usedTwice(arg), LongType, arg)))
      val columns = preEvaluated(optimized).map(_.child)
      if (expectColumn) {
        assert(columns == Seq(arg), s"Expected a column, got: $optimized")
        assert(countEvaluations(optimized)(_ == arg) == 1, s"Not a single evaluation: $optimized")
      } else {
        assert(columns.isEmpty, s"Expected the input left inline, got: $optimized")
      }
  }

  // ---- which copies share a column ----

  test("gives each parameter its own column for separately drawn arguments") {
    // f(rand(1), rand(1)) with `lambda a, b: a - b`: the spliced copies are indistinguishable, but
    // they are two parameters, so Python owes the body two draws. Their ids keep them apart.
    val arg = Rand(Literal(1L))
    val option = Subtract(marker(arg, 0, newId), marker(arg, 1, newId))
    val optimized = convert(select(tpudf(option, DoubleType, arg, arg)))
    assert(preEvaluated(optimized).map(_.child) == Seq(arg, arg),
      s"Expected a column per parameter, got: $optimized")
  }

  test("shares one column between copies of a parameter reseeded independently") {
    // An argument whose seed was unresolved at call time (`expr("rand()")`, or SQL) is reseeded
    // per copy by ResolveRandomSeed: substitution runs first and analysis then rewrites each
    // copy on its own. The id says both copies are one parameter, so they must share: otherwise
    // `lambda x: x == x` compares two independent draws.
    val id = newId
    val option = EqualTo(marker(Rand(Literal(11L)), 0, id), marker(Rand(Literal(22L)), 0, id))
    val optimized = convert(select(tpudf(option, BooleanType, Rand(Literal(11L)))))
    assert(preEvaluated(optimized).map(_.child) == Seq(Rand(Literal(11L))),
      s"Expected one column for the reseeded copies, got: $optimized")
    assert(countEvaluations(optimized)(_.isInstanceOf[Rand]) == 1,
      s"Expected one draw, got: $optimized")
  }

  test("shares one column between everything bound to the same deterministic argument") {
    // Two parameters of one call, and two separate calls: same values either way, so one column
    // does for all of them. The interpreted UDF shares the input column too
    // (EvalPythonEvaluatorFactory reuses a semanticEquals argument).
    val arg = Add(attrA, Literal(1L))
    val twoParams = tpudf(
      Multiply(marker(arg, 0, newId), marker(arg, 1, newId)), LongType, arg, arg)
    val secondCall = tpudf(Add(marker(arg, 0, newId), marker(arg, 0, newId)), LongType, arg)
    val optimized = convert(Project(
      Seq(Alias(twoParams, "x")(), Alias(secondCall, "y")()), relation))
    assert(preEvaluated(optimized).map(_.child) == Seq(arg),
      s"Expected one shared column, got: $optimized")
    assert(countEvaluations(optimized)(_ == arg) == 1, s"Not a single evaluation: $optimized")
  }

  test("re-evaluates an argument nested inside another") {
    // f(a + 1, (a + 1) + 2): parameter b embeds a copy of a's argument, and that copy is b's own
    // work -- Python evaluates b's column independently -- not a third use of a.
    val inner = Add(attrA, Literal(1L))
    val outer = Add(inner, Literal(2L))
    val option = Add(marker(inner, 0, newId), marker(outer, 1, newId))
    val optimized = convert(select(tpudf(option, LongType, inner, outer)))
    assert(preEvaluated(optimized).map(_.child) == Seq(inner, outer),
      s"Expected a column per parameter, got: $optimized")
  }

  test("gives copies analysis rewrote differently a column each") {
    // Analysis rewrites each spliced copy on its own, so one parameter's copies can end up as
    // different expressions -- here one use was cast to int. Deterministic copies key on the
    // argument rather than on the parameter, so each shape gets its own column.
    //
    // Pinned as a known gap rather than as what we want: `a + 1` sits inside both columns, so it is
    // computed twice per row where the Python eval operator computes one column for the parameter.
    val asLong = Add(attrA, Literal(1L))
    val asInt = Cast(asLong, IntegerType)
    val id = newId
    val option = Add(Cast(marker(asLong, 0, id), IntegerType), marker(asInt, 0, id))
    val optimized = convert(select(tpudf(option, IntegerType, asLong)))
    assert(preEvaluated(optimized).map(_.child) == Seq(asLong, asInt),
      s"Expected a column per rewritten copy, got: $optimized")
    assert(countEvaluations(optimized)(_ == asLong) == 2,
      s"Expected a + 1 inside both columns, got: $optimized")
  }

  test("does not share a nested call's parameters with the outer call's") {
    // udf1(udf2(a), udf2(a)) where udf1's body uses parameter 0 twice. The outer parameter's
    // argument is the whole inner body, so it gets its own column one level up from the inner
    // call's: `a + 1` is evaluated once, not once per use of the outer parameter times once per
    // use inside the inner body.
    val innerArg = Add(attrA, Literal(1L))
    val innerOption = usedTwice(innerArg)
    val inner = tpudf(innerOption, LongType, innerArg)
    val outerId = newId
    val outerOption = Add(marker(inner, 0, outerId), marker(inner, 0, outerId))
    val optimized = convert(select(tpudf(outerOption, LongType, inner, inner)))
    assert(preEvaluated(optimized).length == 2, s"Expected a column per level, got: $optimized")
    assert(countEvaluations(optimized)(_ == innerArg) == 1,
      s"Expected the inner input evaluated once, got: $optimized")
    // The outer body reads its column twice rather than holding two copies of the inner body. Match
    // on the *rewritten* inner body -- comparing against `innerOption` can never fail, since that
    // still holds the markers `convert` has already asserted are gone.
    val innerColumn = preEvaluated(optimized).map(_.toAttribute).last
    assert(countEvaluations(optimized)(_ == Multiply(innerColumn, innerColumn)) == 1,
      s"Expected one copy of the inner body, got: $optimized")
  }

  // ---- where the column can live ----

  test("keeps the operator's output schema when it inherits its child's") {
    // A Sort outputs its child's columns, so the pre-evaluating Project would widen the query's
    // schema if the rewrite did not project the extra column away again.
    val arg = Add(attrA, Literal(1L))
    val plan = Sort(
      Seq(SortOrder(tpudf(usedTwice(arg), LongType, arg), Ascending)), global = true, relation)
    val optimized = convert(plan)
    assert(preEvaluated(optimized).map(_.child) == Seq(arg), s"Not pre-evaluated: $optimized")
    assert(optimized.output == plan.output, s"Output schema changed: ${optimized.output}")
  }

  test("leaves an input reading a lambda variable inline") {
    // `transform(arr, x -> udf(x))`: the argument is bound inside the lambda, so no child of the
    // operator can produce it. NamedLambdaVariable.references is its own attribute, which is what
    // rules this out.
    val lambdaVar = NamedLambdaVariable("x", LongType, nullable = false)
    val body = tpudf(usedTwice(lambdaVar), LongType, lambdaVar)
    val plan = select(ArrayTransform(attrArr, LambdaFunction(body, Seq(lambdaVar))))
    val optimized = convert(plan)
    comparePlans(optimized,
      select(ArrayTransform(attrArr,
        LambdaFunction(Multiply(lambdaVar, lambdaVar), Seq(lambdaVar)))))
  }

  test("leaves an input inside a lambda inline even when it reads no lambda variable") {
    // `transform(arr, x -> udf(rand()))`. `rand()` references nothing, so the child search would
    // happily put it below the operator -- and then the draw is made once per ROW and shared by
    // every element, where the lambda body runs once per element. The deterministic case is worse
    // than merely wrong: `a / b` below the operator raises DIVIDE_BY_ZERO under ANSI on a row whose
    // array is empty, so the body never ran. Being inside a lambda is what rules both out.
    val lambdaVar = NamedLambdaVariable("x", LongType, nullable = false)
    Seq(Rand(Literal(1L)), Divide(attrA, attrB)).foreach { arg =>
      val body = tpudf(Add(usedTwice(arg), lambdaVar), LongType, arg)
      val optimized = convert(select(ArrayTransform(attrArr, LambdaFunction(body, Seq(lambdaVar)))))
      assert(preEvaluated(optimized).isEmpty,
        s"Expected no column for an argument inside a lambda, got: $optimized")
    }
  }

  test("leaves a Command alone") {
    // A Command keeps its query in a field, not a child, and has no output of its own, so the
    // schema guard cannot project a widened child back down. Worse, a Project between
    // DeleteFromTable and its relation hides the relation DataSourceV2Strategy matches on and the
    // query dies with an internal error. So no column here -- but the markers still come off, since
    // this rewrite is the only thing that takes them off, and `convert` checks that for us.
    val arg = Add(attrA, Literal(1L))
    val plan = DeleteFromTable(relation,
      GreaterThan(tpudf(usedTwice(arg), LongType, arg), Literal(0L)))
    val optimized = convert(plan).asInstanceOf[DeleteFromTable]
    assert(preEvaluated(optimized).isEmpty, s"Expected no column in a Command, got: $optimized")
    assert(optimized.table eq relation,
      s"Expected the relation untouched below the Command, got: $optimized")
    assert(optimized.condition == GreaterThan(Multiply(arg, arg), Literal(0L)),
      s"Expected the argument left at both use sites, got: $optimized")
  }

  test("puts a join condition's column below the side that can compute it, or nowhere") {
    // The column has to live below a single child, so an argument reading both sides has nowhere to
    // go, and one reading only the right side must not land below the left.
    val right = LocalRelation($"c".long, $"d".long)
    val rightC = right.output.head
    def joinOn(arg: Expression): LogicalPlan = {
      val option = GreaterThan(usedTwice(arg), Literal(0L))
      Join(relation, right, Inner, Some(tpudf(option, BooleanType, arg)), JoinHint.NONE)
    }

    val oneSidedPlan = joinOn(Add(rightC, Literal(1L)))
    val oneSided = convert(oneSidedPlan)
    assert(preEvaluated(oneSided).map(_.child) == Seq(Add(rightC, Literal(1L))),
      s"Not pre-evaluated: $oneSided")
    val join = oneSided.collectFirst { case j: Join => j }.get
    assert(join.right.isInstanceOf[Project] && !join.left.isInstanceOf[Project],
      s"Expected the column below the right side only, got: $oneSided")
    assert(oneSided.output == oneSidedPlan.output, s"Output schema changed: ${oneSided.output}")

    val twoSidedPlan = joinOn(Add(attrA, rightC))
    val twoSided = convert(twoSidedPlan)
    assert(preEvaluated(twoSided).isEmpty,
      s"Expected no column for a two-sided input, got: $twoSided")
    assert(twoSided.output == twoSidedPlan.output, s"Output schema changed: ${twoSided.output}")
  }

  test("leaves a nondeterministic input inline below a multi-child operator") {
    // Put below one side of a join, a draw is made once per row of that side and reused for every
    // row it is paired with -- correlated across output rows. See preservesRowCount.
    val arg = Rand(Literal(1L))
    val id = newId
    val option = GreaterThan(Add(marker(arg, 0, id), marker(arg, 0, id)), Literal(0.5))
    val plan = Join(relation, LocalRelation($"c".long), Inner,
      Some(tpudf(option, BooleanType, arg)), JoinHint.NONE)
    assert(preEvaluated(convert(plan)).isEmpty,
      s"Expected no column for a draw below a join, got: ${convert(plan)}")
  }

  test("leaves an input carrying an outer reference inline") {
    // It belongs to the enclosing query; a Project inside a correlated subquery is not the place to
    // evaluate it.
    val arg = Add(OuterReference(attrB), Literal(1L))
    val optimized = convert(select(tpudf(usedTwice(arg), LongType, arg)))
    assert(preEvaluated(optimized).isEmpty,
      s"Expected no column for an outer reference, got: $optimized")
  }

  test("pre-evaluates a marker inside a subquery plan without tripping on the outer node") {
    // A subquery's plan is not an expression child, so the rewrite reaches its markers when the
    // transform descends into the subquery. The outer node used to see the subquery's pattern bits
    // and assert on a marker it was never going to touch.
    val arg = Add(attrA, Literal(1L))
    val inner = Aggregate(Nil,
      Seq(Alias(Max(tpudf(usedTwice(arg), LongType, arg)).toAggregateExpression(), "m")()),
      relation)
    val optimized = convert(Filter(LessThan(attrA, ScalarSubquery(inner)), relation))
    val subqueryPlans = optimized.expressions.flatMap(_.collect {
      case s: ScalarSubquery => s.plan
    })
    assert(subqueryPlans.nonEmpty, s"Expected the subquery to survive: $optimized")
    assert(subqueryPlans.flatMap(preEvaluated).map(_.child) == Seq(arg),
      s"Expected the input pre-evaluated inside the subquery: $optimized")
    // `convert` cannot check this for us: Expression.exists does not walk into a subquery's plan,
    // so its assertion never sees these. Same for countEvaluations, hence both by hand here.
    assert(!subqueryPlans.exists(_.exists(_.expressions.exists(
      _.exists(_.isInstanceOf[TranspiledUDFParameter])))),
      s"A marker survived inside the subquery: $optimized")
    assert(subqueryPlans.map(countEvaluations(_)(_ == arg)).sum == 1,
      s"Expected one evaluation inside the subquery: $optimized")
  }

  // ---- Aggregate ----

  test("leaves an aggregate argument inline") {
    // `udf(sum(a))` binds the parameter to the aggregate itself. Nothing an aggregate function
    // wraps is here, so the Aggregate rule declines it before `childIndexFor` is ever asked -- the
    // "cannot live in a Project" guard is a backstop, not what fires. PhysicalAggregation shares
    // semantically equal aggregate expressions anyway.
    val arg = Sum(attrA).toAggregateExpression()
    val id = newId
    val option = Add(marker(arg, 0, id), marker(arg, 0, id))
    val optimized = convert(
      Aggregate(Nil, Seq(Alias(tpudf(option, LongType, arg), "v")()), relation))
    assert(preEvaluated(optimized).isEmpty, s"Expected no column for an aggregate: $optimized")
    assert(optimized.asInstanceOf[Aggregate].aggregateExpressions
      .map(_.asInstanceOf[Alias].child) == Seq(Add(arg, arg)),
      s"Expected the aggregate inline and unmarked: $optimized")
  }

  test("pre-evaluates below an Aggregate when the use sits inside an aggregate function") {
    // The input is an ordinary per-row expression that the aggregate only consumes, so the column
    // goes in a Project below the Aggregate. `With` cannot express this: it forbids a common
    // expression reference inside a same-scope aggregate.
    val arg = Add(attrA, Literal(1L))
    val id = newId
    val body = Count(Seq(Add(marker(arg, 0, id), marker(arg, 0, id)))).toAggregateExpression()
    val optimized = convert(Aggregate(
      Seq(attrB), Seq(attrB, Alias(tpudf(body, LongType, arg), "c")()), relation))
    assert(preEvaluated(optimized).map(_.child) == Seq(arg),
      s"Expected the input pre-evaluated: $optimized")
    assert(optimized.asInstanceOf[Aggregate].child.isInstanceOf[Project],
      s"Expected a Project below the Aggregate: $optimized")
    assert(countEvaluations(optimized)(_ == arg) == 1, s"Not a single evaluation: $optimized")
  }

  // Nothing an aggregate function wraps may read a column, or the Aggregate ends up reading a
  // column that is not a grouping expression. The rule's scaladoc has the full reasoning.
  test("leaves an input inline in an Aggregate outside an aggregate function") {
    val arg = Add(attrA, Literal(1L))
    val call = tpudf(usedTwice(arg), LongType, arg)
    // `SELECT a + 1, f(a + 1) FROM t GROUP BY a + 1`, then the same call *as* the grouping
    // expression -- declined either way, though the second happens to be rewritable.
    Seq(
      Aggregate(Seq(arg), Seq(Alias(arg, "g")(), Alias(call, "v")()), relation),
      Aggregate(Seq(call), Seq(Alias(Count(Seq(attrB)).toAggregateExpression(), "c")()), relation)
    ).foreach { plan =>
      val agg = convert(plan).asInstanceOf[Aggregate]
      assert(preEvaluated(agg).isEmpty,
        s"Expected no column outside an aggregate function, got: $agg")
      assertGroupByIntact(agg)
    }
  }

  test("does not share a column between an enclosed use and a bare one in an Aggregate") {
    // `SELECT a + 1, count(f(a + 1)), g(a + 1) FROM t GROUP BY a + 1`. Both calls bind the same
    // deterministic argument, so they key to one column -- but only the use an aggregate function
    // wraps gets to read it. We ask per use instead of once per key, so the wrapped use keeps its
    // column and the bare one stays put as a grouping expression.
    val arg = Add(attrA, Literal(1L))
    val enclosed = Count(Seq(usedTwice(arg, id = newId))).toAggregateExpression()
    val bare = tpudf(usedTwice(arg, id = newId), LongType, arg)
    val agg = convert(Aggregate(
      Seq(arg),
      Seq(Alias(arg, "g")(), Alias(tpudf(enclosed, LongType, arg), "c")(), Alias(bare, "v")()),
      relation)).asInstanceOf[Aggregate]
    assert(preEvaluated(agg).map(_.child) == Seq(arg),
      s"Expected one column, for the enclosed use only, got: $agg")
    val column = preEvaluated(agg).head.toAttribute
    assert(agg.aggregateExpressions.last.asInstanceOf[Alias].child == Multiply(arg, arg),
      s"Expected the bare use left inline, got: $agg")
    assert(agg.aggregateExpressions(1).exists(_ == column),
      s"Expected the enclosed use reading the column, got: $agg")
    assertGroupByIntact(agg)
  }

  // ---- end to end ----

  test("keeps a nondeterministic input's column through predicate pushdown") {
    // What makes a column safe in a predicate: PushPredicateThroughNonJoin.canPushThrough refuses a
    // Project whose projectList is nondeterministic, so `_udf_input_0 = rand(1)` is not
    // inlined back into the predicate it pushes below. Without that, `lambda x: x + x > 0.5`
    // would draw twice per row -- so this is load-bearing rather than incidental.
    val arg = Rand(Literal(1L))
    val id = newId
    val option = GreaterThan(Add(marker(arg, 0, id), marker(arg, 0, id)), Literal(0.5))
    val rows = LocalRelation.fromExternalRows(Seq(attrA, attrB), Seq(Row(1L, 2L)))
    val optimized = optimize(Filter(tpudf(option, BooleanType, arg), rows))
    assert(preEvaluated(optimized).map(_.child) == Seq(arg),
      s"The column was inlined back into the predicate: $optimized")
    assert(countEvaluations(optimized)(_.isInstanceOf[Rand]) == 1,
      s"Expected one draw per row, got: $optimized")
    assert(optimized.output == rows.output, s"Extra columns leaked out: ${optimized.output}")
  }
}
