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
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Max, Sum}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LocalRelation, LogicalPlan, Project, Sort}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, LongType, MapType, StringType}

/**
 * Tests that every input a transpiled UDF option uses is evaluated once per row (SPARK-58626).
 *
 * The rewrite under test is [[PreEvaluateTranspiledUDFInputs]], which `ConvertToCatalyst` runs on
 * each plan node once that node's options have been substituted, so these tests drive the whole
 * thing through `ConvertToCatalyst` -- the shape of the plan it produces is the contract. A
 * predicate is the exception: `ConvertToCatalyst` keeps the interpreted UDF in a `Filter` or join
 * condition (pushdown would inline the columns straight back), so those shapes reach the rewrite
 * only through `preEvaluateOnly`.
 *
 * `UserDefinedPythonFunction.builder` puts a [[TranspiledUDFParameter]] on every copy of every
 * non-foldable argument an option uses, with one id per parameter per call; `marker` below stands
 * in for that. Which copies share a column is the interesting part: all copies of one parameter
 * do, and so do two parameters bound to the same deterministic argument, but two parameters
 * bound to separately-drawn `rand()`s do not -- Python would compute two columns there.
 */
class PreEvaluateTranspiledUDFInputsSuite extends PlanTest {

  private val attrA = $"a".long
  private val attrB = $"b".long
  private val attrArr = $"arr".array(LongType)
  private val relation = LocalRelation(attrA, attrB, attrArr)

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
   * The rewrite on its own, for shapes `ConvertToCatalyst` no longer routes to it: a predicate
   * keeps the interpreted UDF, so a marker in a join condition can only be built by hand.
   */
  private def preEvaluateOnly(plan: LogicalPlan): LogicalPlan =
    PreEvaluateTranspiledUDFInputs(plan)

  /** `plan` through the whole optimizer, to see what a query really gets. */
  private def optimize(plan: LogicalPlan): LogicalPlan =
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true",
      // Otherwise the one-row local relation is folded away and there is no plan left to look at.
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
      SimpleTestOptimizer.execute(plan)
    }

  // ---- tests ----

  test("pre-evaluates a repeated input in a Project below the operator") {
    // `lambda x: x * x` over f(a + 1): one column below, both uses read it back.
    val arg = Add(attrA, Literal(1L))
    val id = newId
    val option = Multiply(marker(arg, 0, id), marker(arg, 0, id))
    val optimized = convert(select(tpudf(option, LongType, arg)))
    val inputs = preEvaluated(optimized)
    assert(inputs.map(_.child) == Seq(arg), s"Expected one column for a + 1, got: $optimized")
    val input = inputs.head.toAttribute
    assert(optimized.asInstanceOf[Project].projectList.map(_.asInstanceOf[Alias].child) ==
      Seq(Multiply(input, input)), s"Uses did not read the column: $optimized")
    assert(countEvaluations(optimized)(_ == arg) == 1, s"Not a single evaluation: $optimized")
  }

  test("keeps the pre-evaluation when the call sits in a conditional branch") {
    // `when(b > 0, udf(a + 1))`. A `With` expression would be inlined back into the branch here
    // (RewriteWithExpression cannot hoist out of a branch that may not run), which is why this
    // rewrite builds the Project itself. The input becomes eager, which is what the interpreted
    // UDF does -- ExtractPythonUDFs evaluates its argument columns for every row.
    val arg = Add(attrA, Literal(1L))
    val id = newId
    val option = Multiply(marker(arg, 0, id), marker(arg, 0, id))
    val call = tpudf(option, LongType, arg)
    val optimized = convert(select(If(GreaterThan(attrB, Literal(0L)), call, Literal(0L))))
    assert(preEvaluated(optimized).map(_.child) == Seq(arg),
      s"Expected the input pre-evaluated despite the branch, got: $optimized")
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

  test("leaves a cheap input inline") {
    // A column read or a literal costs nothing at each use site, so the plan must not grow.
    val id = newId
    val columnOption = Multiply(marker(attrA, 0, id), marker(attrA, 0, id))
    val optimizedColumn = convert(select(tpudf(columnOption, LongType, attrA)))
    comparePlans(optimizedColumn, select(Multiply(attrA, attrA)))

    // A foldable argument is not even marked by the builder, but mark it here to pin that the
    // rewrite would decline it anyway.
    val foldable = Add(Literal(2L), Literal(3L))
    val foldableOption = Multiply(marker(foldable, 0, id), marker(foldable, 0, id))
    val optimizedFoldable = convert(select(tpudf(foldableOption, LongType, foldable)))
    comparePlans(optimizedFoldable, select(Multiply(foldable, foldable)))

    // A field read from a column is a cheap chain too.
    val struct = $"s".struct($"f".long)
    val field = GetStructField(struct, 0)
    val fieldOption = Multiply(marker(field, 0, id), marker(field, 0, id))
    val plan = Project(Seq(Alias(tpudf(fieldOption, LongType, field), "v")()),
      LocalRelation(struct))
    assert(preEvaluated(convert(plan)).isEmpty, s"Expected no column for a field read: $plan")
  }

  test("pre-evaluates a Python UDF input rather than leaving the call inline") {
    // Unlike CollapseProject.isCheap, a Python call is not cheap here: left inline at two use sites
    // it is two round trips into the worker per row.
    val arg = PythonUDF("inner", null, LongType, Seq(attrA),
      PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)
    val id = newId
    val option = Multiply(marker(arg, 0, id), marker(arg, 0, id))
    val optimized = convert(select(tpudf(option, LongType, arg)))
    assert(preEvaluated(optimized).map(_.child) == Seq(arg),
      s"Expected the Python UDF input pre-evaluated, got: $optimized")
    assert(countEvaluations(optimized)(_.isInstanceOf[PythonUDF]) == 1,
      s"Expected one Python call, got: $optimized")
  }

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
    // An argument whose seed was unresolved at call time (`expr(\"rand()\")`, or SQL text) is
    // reseeded per copy by ResolveRandomSeed, because substitution runs first and analysis then
    // rewrites each copy on its own. The id says both copies are one parameter, so they must share:
    // otherwise `lambda x: x == x` compares two independent draws.
    val id = newId
    val option = EqualTo(marker(Rand(Literal(11L)), 0, id), marker(Rand(Literal(22L)), 0, id))
    val optimized = convert(select(tpudf(option, BooleanType, Rand(Literal(11L)))))
    assert(preEvaluated(optimized).map(_.child) == Seq(Rand(Literal(11L))),
      s"Expected one column for the reseeded copies, got: $optimized")
    assert(countEvaluations(optimized)(_.isInstanceOf[Rand]) == 1,
      s"Expected one draw, got: $optimized")
  }

  test("shares one column between parameters bound to the same deterministic argument") {
    // f(a + 1, a + 1) with `lambda a, b: a * b`: two columns of identical values is two parameters
    // to Python, but one column here computes the same result with less work.
    val arg = Add(attrA, Literal(1L))
    val option = Multiply(marker(arg, 0, newId), marker(arg, 1, newId))
    val optimized = convert(select(tpudf(option, LongType, arg, arg)))
    assert(preEvaluated(optimized).map(_.child) == Seq(arg),
      s"Expected one shared column, got: $optimized")
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
    // different expressions -- here one use was cast to int. They key on the argument rather than
    // on the parameter, so each gets its own column, and each is still evaluated once.
    val asLong = Add(attrA, Literal(1L))
    val asInt = Cast(asLong, IntegerType)
    val id = newId
    val option = Add(Cast(marker(asLong, 0, id), IntegerType), marker(asInt, 0, id))
    val optimized = convert(select(tpudf(option, IntegerType, asLong)))
    assert(preEvaluated(optimized).map(_.child) == Seq(asLong, asInt),
      s"Expected a column per rewritten copy, got: $optimized")
  }

  test("leaves a nondeterministic parameter inline when its copies disagree on type") {
    // Copies of a nondeterministic parameter key on the marker id rather than on the argument, so a
    // single column would have to stand in for both -- and no attribute can carry both types.
    val id = newId
    val option = Add(
      Cast(marker(Rand(Literal(11L)), 0, id), IntegerType),
      marker(Cast(Rand(Literal(22L)), IntegerType), 0, id))
    val optimized = convert(select(tpudf(option, IntegerType, Rand(Literal(11L)))))
    assert(preEvaluated(optimized).isEmpty,
      s"Expected no column for copies of different types, got: $optimized")
    assert(countEvaluations(optimized)(_.isInstanceOf[Rand]) == 2,
      s"Expected both copies left inline, got: $optimized")
  }

  test("leaves an aggregate argument inline") {
    // `udf(sum(a))` binds the parameter to the aggregate itself, and an aggregate cannot live in a
    // Project. PhysicalAggregation shares semantically equal aggregate expressions anyway.
    val arg = Sum(attrA).toAggregateExpression()
    val id = newId
    val option = Add(marker(arg, 0, id), marker(arg, 0, id))
    val plan = Aggregate(Nil, Seq(Alias(tpudf(option, LongType, arg), "v")()), relation)
    val optimized = convert(plan)
    assert(preEvaluated(optimized).isEmpty, s"Expected no column for an aggregate: $optimized")
    val aggExprs = optimized.asInstanceOf[Aggregate].aggregateExpressions
    assert(aggExprs.map(_.asInstanceOf[Alias].child) == Seq(Add(arg, arg)),
      s"Expected the aggregate inline and unmarked: $optimized")
  }

  test("pre-evaluates below an Aggregate when the use sits inside an aggregate function") {
    // The input is an ordinary per-row expression, the aggregate only consumes it, so the column
    // goes in a Project below the Aggregate. `With` cannot express this: it forbids a common
    // expression reference inside a same-scope aggregate.
    val arg = Add(attrA, Literal(1L))
    val id = newId
    val body = Count(Seq(Add(marker(arg, 0, id), marker(arg, 0, id)))).toAggregateExpression()
    val plan = Aggregate(
      Seq(attrB), Seq(attrB, Alias(tpudf(body, LongType, arg), "c")()), relation)
    val optimized = convert(plan)
    val inputs = preEvaluated(optimized)
    assert(inputs.map(_.child) == Seq(arg), s"Expected the input pre-evaluated: $optimized")
    assert(optimized.isInstanceOf[Aggregate], s"Expected the Aggregate on top: $optimized")
    assert(optimized.asInstanceOf[Aggregate].child.isInstanceOf[Project],
      s"Expected a Project below the Aggregate: $optimized")
    assert(countEvaluations(optimized)(_ == arg) == 1, s"Not a single evaluation: $optimized")
  }

  test("keeps the operator's output schema when it inherits its child's") {
    // A Sort outputs its child's columns, so the pre-evaluating Project would widen the query's
    // schema if the rewrite did not project the extra column away again.
    val arg = Add(attrA, Literal(1L))
    val id = newId
    val option = Multiply(marker(arg, 0, id), marker(arg, 0, id))
    val plan = Sort(
      Seq(SortOrder(tpudf(option, LongType, arg), Ascending)), global = true, relation)
    val optimized = convert(plan)
    assert(preEvaluated(optimized).map(_.child) == Seq(arg), s"Not pre-evaluated: $optimized")
    assert(optimized.output == plan.output, s"Output schema changed: ${optimized.output}")
  }

  test("leaves an input reading a lambda variable inline") {
    // `transform(arr, x -> udf(x))`: the argument is bound inside the lambda, so no child of the
    // operator can produce it. NamedLambdaVariable.references is its own attribute, which is what
    // rules this out.
    val lambdaVar = NamedLambdaVariable("x", LongType, nullable = false)
    val id = newId
    val option = Multiply(marker(lambdaVar, 0, id), marker(lambdaVar, 0, id))
    val body = tpudf(option, LongType, lambdaVar)
    val plan = select(ArrayTransform(attrArr, LambdaFunction(body, Seq(lambdaVar))))
    val optimized = convert(plan)
    assert(preEvaluated(optimized).isEmpty,
      s"Expected no column for a lambda variable, got: $optimized")
    comparePlans(optimized,
      select(ArrayTransform(attrArr,
        LambdaFunction(Multiply(lambdaVar, lambdaVar), Seq(lambdaVar)))))
  }

  test("leaves an input reading both sides of a join inline") {
    // The column would have to live below one side, and neither side can compute it.
    val right = LocalRelation($"c".long)
    val rightAttr = right.output.head
    val arg = Add(attrA, rightAttr)
    val id = newId
    val option = GreaterThan(Multiply(marker(arg, 0, id), marker(arg, 0, id)), Literal(0L))
    val plan = Join(relation, right, Inner, Some(option), JoinHint.NONE)
    val optimized = preEvaluateOnly(plan)
    assert(preEvaluated(optimized).isEmpty,
      s"Expected no column for a two-sided input, got: $optimized")
    assert(optimized.output == plan.output, s"Output schema changed: ${optimized.output}")
  }

  test("does not reuse a column name freed by the type check") {
    // A parameter dropped by the disagree-on-type check has already taken a column name, and the
    // next parameter must not take it again: two live columns of the same name in one Project are
    // legal but a trap to read. Here the vetoed parameter takes _udf_input_0, so the deterministic
    // ones must be _udf_input_1 and _udf_input_2.
    val vetoedId = newId
    val option = Add(
      Add(
        Cast(marker(Rand(Literal(11L)), 0, vetoedId), IntegerType),
        Cast(marker(Add(attrA, Literal(1L)), 1, newId), IntegerType)),
      Add(
        marker(Cast(Rand(Literal(22L)), IntegerType), 0, vetoedId),
        Cast(marker(Add(attrB, Literal(1L)), 2, newId), IntegerType)))
    val optimized = convert(select(tpudf(option, IntegerType, Rand(Literal(11L)))))
    val names = preEvaluated(optimized).map(_.name)
    assert(names.distinct == names, s"Two columns share a name: $optimized")
    assert(names.length == 2, s"Expected the vetoed parameter to have no column: $optimized")
  }

  test("does not share a nested call's parameters with the outer call's") {
    // udf1(udf2(a), udf2(a)) where udf1's body uses parameter 0 twice: both calls mark a parameter
    // 0, so the ids are what keep the inner call's input out of the outer call's column.
    val innerArg = Add(attrA, Literal(1L))
    val innerId = newId
    val innerOption = Multiply(marker(innerArg, 0, innerId), marker(innerArg, 0, innerId))
    val inner = tpudf(innerOption, LongType, innerArg)
    val outerId = newId
    val outerOption = Add(marker(inner, 0, outerId), marker(inner, 0, outerId))
    val optimized = convert(select(tpudf(outerOption, LongType, inner, inner)))
    // Two columns, one per level: the outer parameter's holds the whole inner call, and the inner
    // call's own input is pre-evaluated below that -- so `a + 1` is evaluated once, not once per
    // use of the outer parameter times once per use inside the inner body.
    val inputs = preEvaluated(optimized)
    assert(inputs.length == 2, s"Expected a column per level, got: $optimized")
    assert(countEvaluations(optimized)(_ == innerArg) == 1,
      s"Expected the inner input evaluated once, got: $optimized")
    assert(countEvaluations(optimized)(_ == innerOption) == 0,
      s"Expected the inner call's body evaluated through its column, got: $optimized")
  }

  test("pre-evaluates below the side of a join that can compute the input") {
    // The column has to land below the child that produces its references, not below the first one.
    val right = LocalRelation($"c".long, $"d".long)
    val rightC = right.output.head
    val arg = Add(rightC, Literal(1L))
    val id = newId
    val option = GreaterThan(Multiply(marker(arg, 0, id), marker(arg, 0, id)), Literal(0L))
    val plan = Join(relation, right, Inner, Some(option), JoinHint.NONE)
    val optimized = preEvaluateOnly(plan)
    assert(preEvaluated(optimized).map(_.child) == Seq(arg), s"Not pre-evaluated: $optimized")
    val join = optimized.collectFirst { case j: Join => j }.get
    assert(join.right.isInstanceOf[Project] && !join.left.isInstanceOf[Project],
      s"Expected the column below the right side only, got: $optimized")
    assert(optimized.output == plan.output, s"Output schema changed: ${optimized.output}")
  }

  test("shares one column between two separate calls bound to the same argument") {
    // Two calls, two parameters, one deterministic argument: the interpreted UDF would build one
    // input column for both (EvalPythonEvaluatorFactory reuses a semanticEquals argument), so one
    // column here too.
    val arg = Add(attrA, Literal(1L))
    val first = tpudf(Multiply(marker(arg, 0, newId), marker(arg, 0, newId)), LongType, arg)
    val second = tpudf(Add(marker(arg, 0, newId), marker(arg, 0, newId)), LongType, arg)
    val plan = Project(Seq(Alias(first, "x")(), Alias(second, "y")()), relation)
    val optimized = convert(plan)
    assert(preEvaluated(optimized).map(_.child) == Seq(arg),
      s"Expected one column for both calls, got: $optimized")
    assert(countEvaluations(optimized)(_ == arg) == 1, s"Not a single evaluation: $optimized")
  }

  test("leaves an input inline in an Aggregate unless an aggregate function encloses it") {
    // `SELECT a + 1, f(a + 1) FROM t GROUP BY a + 1`: the grouping expression is the user's own
    // `a + 1` and carries no marker, so pre-evaluating the argument would rewrite only the
    // aggregate side and leave it reading a column that is not a grouping expression -- an invalid
    // Aggregate. Everything outside an aggregate function in an Aggregate is declined for that
    // reason; a grouping key is evaluated once per row anyway.
    val arg = Add(attrA, Literal(1L))
    val id = newId
    val option = Multiply(marker(arg, 0, id), marker(arg, 0, id))
    val call = tpudf(option, LongType, arg)
    val plan = Aggregate(
      Seq(arg), Seq(Alias(arg, "g")(), Alias(call, "v")()), relation)
    val optimized = convert(plan)
    assert(preEvaluated(optimized).isEmpty,
      s"Expected no column outside an aggregate function, got: $optimized")
    val agg = optimized.asInstanceOf[Aggregate]
    assert(agg.aggregateExpressions.last.asInstanceOf[Alias].child == Multiply(arg, arg),
      s"Expected the option inline, got: $optimized")
    // Every non-aggregating expression still reads only grouping expressions, which is what makes
    // the Aggregate valid.
    assert(agg.aggregateExpressions.forall(_.references.subsetOf(
      AttributeSet(agg.groupingExpressions.flatMap(_.references)))),
      s"An aggregate expression escaped the GROUP BY: $optimized")
  }

  test("leaves an input inline when the use sits in a grouping expression only") {
    // No aggregate function encloses it here either, so it is declined for the same reason -- the
    // safe answer even though this shape happens to be rewritable.
    val arg = Add(attrA, Literal(1L))
    val id = newId
    val option = Multiply(marker(arg, 0, id), marker(arg, 0, id))
    val call = tpudf(option, LongType, arg)
    val plan = Aggregate(
      Seq(call), Seq(Alias(Count(Seq(attrB)).toAggregateExpression(), "c")()), relation)
    val optimized = convert(plan)
    assert(preEvaluated(optimized).isEmpty, s"Expected no column, got: $optimized")
  }

  test("pre-evaluates a marker inside a subquery plan without tripping on the outer node") {
    // A marker inside a subquery is not the outer node's business: a subquery's plan is not an
    // expression child, so the rewrite reaches it when the transform descends into the subquery.
    // The outer node used to see the subquery's pattern bits and assert on a marker it was never
    // going to touch.
    val arg = Add(attrA, Literal(1L))
    val id = newId
    val option = Multiply(marker(arg, 0, id), marker(arg, 0, id))
    val inner = Aggregate(Nil,
      Seq(Alias(Max(tpudf(option, LongType, arg)).toAggregateExpression(), "m")()), relation)
    val plan = Filter(LessThan(attrA, ScalarSubquery(inner)), relation)
    val optimized = convert(plan)
    val subqueryPlans = optimized.expressions.flatMap(_.collect {
      case s: ScalarSubquery => s.plan
    })
    assert(subqueryPlans.nonEmpty, s"Expected the subquery to survive: $optimized")
    assert(!subqueryPlans.exists(_.exists(_.expressions.exists(
      _.exists(_.isInstanceOf[TranspiledUDFParameter])))),
      s"A marker survived inside the subquery: $optimized")
    assert(subqueryPlans.flatMap(preEvaluated).map(_.child) == Seq(arg),
      s"Expected the input pre-evaluated inside the subquery: $optimized")
  }

  test("leaves a nondeterministic input inline below a multi-child operator") {
    // Hoisted below one side of a join, a draw would be made once per row of that side and reused
    // for every row it is paired with -- correlated across output rows rather than one per row.
    val arg = Rand(Literal(1L))
    val id = newId
    val option = GreaterThan(Add(marker(arg, 0, id), marker(arg, 0, id)), Literal(0.5))
    val right = LocalRelation($"c".long)
    val plan = Join(relation, right, Inner, Some(option), JoinHint.NONE)
    val optimized = preEvaluateOnly(plan)
    assert(preEvaluated(optimized).isEmpty,
      s"Expected no column for a draw below a join, got: $optimized")
  }

  test("leaves an input carrying an outer reference inline") {
    // It belongs to the enclosing query; a Project inside a correlated subquery is not the place to
    // evaluate it.
    val arg = Add(OuterReference(attrB), Literal(1L))
    val id = newId
    val option = Multiply(marker(arg, 0, id), marker(arg, 0, id))
    val optimized = convert(select(tpudf(option, LongType, arg)))
    assert(preEvaluated(optimized).isEmpty,
      s"Expected no column for an outer reference, got: $optimized")
  }

  test("keeps the interpreted UDF when the call is a predicate") {
    // Pre-evaluating in a predicate is pointless: pushdown inlines the column back into the
    // predicate it pushes down. ConvertToCatalyst does not transpile there at all, so the Filter
    // keeps its PythonUDF and the input reaches the eval operator once per row.
    val arg = Add(attrA, Literal(1L))
    val id = newId
    val option = GreaterThan(Multiply(marker(arg, 0, id), marker(arg, 0, id)), Literal(0L))
    val call = tpudf(option, BooleanType, arg)
    val rows = LocalRelation.fromExternalRows(Seq(attrA, attrB), Seq(Row(1L, 2L)))
    val optimized = optimize(Filter(call, rows))
    assert(preEvaluated(optimized).isEmpty, s"Expected no column in a predicate: $optimized")
    assert(countEvaluations(optimized)(_.isInstanceOf[PythonUDF]) == 1,
      s"Expected the interpreted UDF in the predicate, got: $optimized")
    assert(countEvaluations(optimized)(_ == arg) == 1,
      s"Expected the input evaluated once, got: $optimized")
    // The same body in a Project is transpiled and keeps its column, which is the contrast.
    val inProject = optimize(Project(Seq(Alias(tpudf(option, BooleanType, arg), "v")()), rows))
    assert(countEvaluations(inProject)(_.isInstanceOf[PythonUDF]) == 0,
      s"Expected transpilation in a Project, got: $inProject")
    assert(countEvaluations(inProject)(_ == arg) == 1,
      s"Expected one evaluation in a Project, got: $inProject")
  }

  test("re-checks the Aggregate guard before reusing a column for a bare use") {
    // `SELECT a + 1, count(f(a + 1)), g(a + 1) FROM t GROUP BY a + 1`. Both calls bind the same
    // deterministic argument, so they key to one column -- but only the one an aggregate function
    // encloses may have it. Reusing the column for the bare use would leave it reading an attribute
    // that is not a grouping expression, which is an invalid Aggregate, so the guard is re-checked
    // per copy and the whole key is declined. Order matters: the enclosed copy registers first
    // here, which is the case a first-registration-only check misses.
    val arg = Add(attrA, Literal(1L))
    val enclosedId = newId
    val bareId = newId
    val enclosed = Count(Seq(Multiply(marker(arg, 0, enclosedId), marker(arg, 0, enclosedId))))
      .toAggregateExpression()
    val bare = tpudf(Add(marker(arg, 0, bareId), marker(arg, 0, bareId)), LongType, arg)
    val plan = Aggregate(
      Seq(arg),
      Seq(Alias(arg, "g")(), Alias(tpudf(enclosed, LongType, arg), "c")(), Alias(bare, "v")()),
      relation)
    val optimized = convert(plan)
    assert(preEvaluated(optimized).isEmpty,
      s"Expected the key declined for both copies, got: $optimized")
    val agg = optimized.asInstanceOf[Aggregate]
    assert(agg.aggregateExpressions.forall(_.references.subsetOf(
      AttributeSet(agg.groupingExpressions.flatMap(_.references)))),
      s"An aggregate expression escaped the GROUP BY: $optimized")
  }

  test("keeps a nested input inline in a predicate even when the outer call has no options") {
    // The outer call cannot transpile at all (no options), but that must not let the inner one
    // pre-evaluate inside the predicate: `inPredicate` has to survive the fallback, or the pushdown
    // would inline the column straight back.
    val arg = Add(attrA, Literal(1L))
    val id = newId
    val inner = tpudf(Multiply(marker(arg, 0, id), marker(arg, 0, id)), LongType, arg)
    val outer = TranspiledPythonUDF(
      "outer",
      PythonUDF("outer", null, BooleanType, Seq(inner),
        PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true),
      List())
    val optimized = convert(Filter(outer, relation))
    assert(preEvaluated(optimized).isEmpty,
      s"Expected no column inside the predicate, got: $optimized")
  }

  test("does not treat a map probe as a cheap input") {
    // GetMapValue walks the key array comparing keys, so leaving `m['k']` inline pays that scan at
    // every use site. A struct field read is an offset and stays inline.
    val attrMap = AttributeReference("m", MapType(StringType, LongType))()
    val attrStruct = $"s".struct($"f".long)
    val mapRelation = LocalRelation(attrMap, attrStruct)
    val probe = GetMapValue(attrMap, Literal("k"))
    val id = newId
    val mapPlan = Project(
      Seq(Alias(tpudf(Add(marker(probe, 0, id), marker(probe, 0, id)), LongType, probe), "v")()),
      mapRelation)
    assert(preEvaluated(convert(mapPlan)).map(_.child) == Seq(probe),
      s"Expected a column for a map probe: ${convert(mapPlan)}")

    val field = GetStructField(attrStruct, 0)
    val fieldPlan = Project(
      Seq(Alias(tpudf(Add(marker(field, 0, id), marker(field, 0, id)), LongType, field), "v")()),
      mapRelation)
    assert(preEvaluated(convert(fieldPlan)).isEmpty,
      s"Expected a struct field read to stay inline: ${convert(fieldPlan)}")
  }

  test("leaves a parameter inline when its copies read different columns") {
    // A nondeterministic key compares copies by id, not by shape, and the column is built from the
    // first copy -- so copies reading different columns must not share one, or the second copy's
    // reference would silently vanish. Reachable from a custom transpiler.
    val id = newId
    val option = Add(
      marker(Add(Rand(Literal(1L)), attrA), 0, id),
      marker(Add(Rand(Literal(1L)), attrB), 0, id))
    val optimized = convert(select(tpudf(option, DoubleType, Add(Rand(Literal(1L)), attrA))))
    assert(preEvaluated(optimized).isEmpty,
      s"Expected no shared column for copies reading different columns, got: $optimized")
    assert(countEvaluations(optimized)(_.isInstanceOf[Rand]) == 2,
      s"Expected both copies left inline, got: $optimized")
  }

  test("prints the marker's id as a number rather than a raw ExprId") {
    // Markers live in the plan from call construction until this rewrite runs, so they show up in
    // analyzed-plan strings and analysis errors. An ExprId's per-JVM UUID would make that text
    // different on every run.
    val marked = marker(Add(attrA, Literal(1L)), 0, newId).toString
    assert(!marked.contains("ExprId("), s"Marker printed a raw ExprId: $marked")
    assert(marked.contains("transpiledudfparameter"), s"Unexpected marker rendering: $marked")
  }
}
