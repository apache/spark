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

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectList, CollectSet, DeclarativeAggregate, ListAgg, PandasProduct, Percentile, PivotFirst, StddevSamp, TypedImperativeAggregate, V2Aggregator}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, CTERelationRef, DeleteAction, DeleteFromTable, DomainJoin, InsertAction, LateralJoin, LocalRelation, LogicalPlan, MergeIntoTable, Sample, UpdateTable}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.trees.TreePattern.{TreePattern, USER_DEFINED_AGGREGATION}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.connector.catalog.functions.{AggregateFunction => V2AggregateFunction}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, MapType, StringType}
import org.apache.spark.util.ThreadUtils

// A DeclarativeAggregate whose evaluate expression does not have its data type.
private case class MistypedAggregate(child: Expression)
  extends DeclarativeAggregate with UnaryLike[Expression] {
  override def dataType: DataType = LongType
  override def nullable: Boolean = true
  private lazy val buffer = AttributeReference("buffer", IntegerType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = buffer :: Nil
  override lazy val initialValues: Seq[Expression] = Seq(Literal(0))
  override lazy val updateExpressions: Seq[Expression] = Seq(buffer)
  override lazy val mergeExpressions: Seq[Expression] = Seq(buffer.left)
  override lazy val evaluateExpression: Expression = buffer
  override protected def withNewChildInternal(newChild: Expression): MistypedAggregate =
    copy(child = newChild)
}

// A DeclarativeAggregate that is declared not nullable but returns NULL on empty input.
private case class NullOnEmptyAggregate(child: Expression)
  extends DeclarativeAggregate with UnaryLike[Expression] {
  override def dataType: DataType = LongType
  override def nullable: Boolean = false
  private lazy val buffer = AttributeReference("buffer", LongType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = buffer :: Nil
  override lazy val initialValues: Seq[Expression] = Seq(Literal(null, LongType))
  override lazy val updateExpressions: Seq[Expression] = Seq(buffer)
  override lazy val mergeExpressions: Seq[Expression] = Seq(buffer.left)
  override lazy val evaluateExpression: Expression = buffer
  override protected def withNewChildInternal(newChild: Expression): NullOnEmptyAggregate =
    copy(child = newChild)
}

// A DeclarativeAggregate marked as user-defined with the USER_DEFINED_AGGREGATION tree pattern,
// like SimpleTypedAggregateExpression, which hooks a typed Aggregator into a Dataset and whose
// update and evaluate expressions run user code. It sums its input, ignoring NULLs, from 0.
private case class UserDefinedSumAggregate(child: Expression)
  extends DeclarativeAggregate with UnaryLike[Expression] {
  final override val nodePatterns: Seq[TreePattern] = Seq(USER_DEFINED_AGGREGATION)
  override def dataType: DataType = LongType
  override def nullable: Boolean = false
  private lazy val buffer = AttributeReference("buffer", LongType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = buffer :: Nil
  override lazy val initialValues: Seq[Expression] = Seq(Literal(0L))
  override lazy val updateExpressions: Seq[Expression] =
    Seq(Coalesce(Seq(Add(buffer, Cast(child, LongType)), buffer)))
  override lazy val mergeExpressions: Seq[Expression] = Seq(Add(buffer.left, buffer.right))
  override lazy val evaluateExpression: Expression = buffer
  override protected def withNewChildInternal(newChild: Expression): UserDefinedSumAggregate =
    copy(child = newChild)
}

// The same as UserDefinedSumAggregate, but marked as user-defined through UserDefinedExpression
// instead of the tree pattern.
private case class UserDefinedSumFunction(child: Expression)
  extends DeclarativeAggregate with UnaryLike[Expression] with UserDefinedExpression {
  override def name: String = "user_defined_sum"
  override def dataType: DataType = LongType
  override def nullable: Boolean = false
  private lazy val buffer = AttributeReference("buffer", LongType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = buffer :: Nil
  override lazy val initialValues: Seq[Expression] = Seq(Literal(0L))
  override lazy val updateExpressions: Seq[Expression] =
    Seq(Coalesce(Seq(Add(buffer, Cast(child, LongType)), buffer)))
  override lazy val mergeExpressions: Seq[Expression] = Seq(Add(buffer.left, buffer.right))
  override lazy val evaluateExpression: Expression = buffer
  override protected def withNewChildInternal(newChild: Expression): UserDefinedSumFunction =
    copy(child = newChild)
}

// An ImperativeAggregate whose result over a fresh buffer fails, as for a function that requires
// some input.
private case class FailingOnEmptyAggregate(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Array[Long]] with UnaryLike[Expression] {
  override def dataType: DataType = LongType
  override def nullable: Boolean = true
  override def createAggregationBuffer(): Array[Long] = Array(0L)
  override def update(buffer: Array[Long], input: InternalRow): Array[Long] = buffer
  override def merge(buffer: Array[Long], input: Array[Long]): Array[Long] = buffer
  override def eval(buffer: Array[Long]): Any = throw new IllegalStateException("no input")
  override def serialize(buffer: Array[Long]): Array[Byte] = Array.emptyByteArray
  override def deserialize(bytes: Array[Byte]): Array[Long] = Array(0L)
  override def withNewMutableAggBufferOffset(offset: Int): FailingOnEmptyAggregate =
    copy(mutableAggBufferOffset = offset)
  override def withNewInputAggBufferOffset(offset: Int): FailingOnEmptyAggregate =
    copy(inputAggBufferOffset = offset)
  override protected def withNewChildInternal(newChild: Expression): FailingOnEmptyAggregate =
    copy(child = newChild)
}

class PullupCorrelatedPredicatesSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    override protected val excludedOnceBatches = Set("PullupCorrelatedPredicates")

    val batches =
      Batch("PullupCorrelatedPredicates", Once,
        PullupCorrelatedPredicates) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".double)
  val testRelation2 = LocalRelation($"c".int, $"d".double)

  test("PullupCorrelatedPredicates should not produce unresolved plan") {
    val subPlan =
      testRelation2
        .where($"b" < $"d")
        .select($"c")
    val inSubquery =
      testRelation
        .where(InSubquery(Seq($"a"), ListQuery(subPlan)))
        .select($"a").analyze
    assert(inSubquery.resolved)

    val optimized = Optimize.execute(inSubquery)
    assert(optimized.resolved)
  }

  test("PullupCorrelatedPredicates in correlated subquery idempotency check") {
    val subPlan =
      testRelation2
      .where($"b" < $"d")
      .select($"c")
    val inSubquery =
      testRelation
      .where(InSubquery(Seq($"a"), ListQuery(subPlan)))
      .select($"a").analyze
    assert(inSubquery.resolved)

    val optimized = Optimize.execute(inSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized)
  }

  test("PullupCorrelatedPredicates exists correlated subquery idempotency check") {
    val subPlan =
      testRelation2
        .where($"b" === $"d" && $"d" === 1)
        .select(Literal(1))
    val existsSubquery =
      testRelation
        .where(Exists(subPlan))
        .select($"a").analyze
    assert(existsSubquery.resolved)

    val optimized = Optimize.execute(existsSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized)
  }

  test("PullupCorrelatedPredicates scalar correlated subquery idempotency check") {
    val subPlan =
      testRelation2
        .where($"b" === $"d" && $"d" === 1)
        .select(max($"d"))
    val scalarSubquery =
      testRelation
        .where(ScalarSubquery(subPlan) === 1)
        .select($"a").analyze

    val optimized = Optimize.execute(scalarSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized, false)
  }

  test("PullupCorrelatedPredicates lateral join idempotency check") {
    val right =
      testRelation2
        .where($"b" === $"d" && $"d" === 1)
        .select($"c")
    val left = testRelation
    val lateralJoin = LateralJoin(left, LateralSubquery(right), Inner, Some($"a" === $"c")).analyze
    val optimized = Optimize.execute(lateralJoin)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized)
  }

  // Returns ScalarSubquery.mayHaveCountBug, and whether DecorrelateInnerQuery handled the count
  // bug, which it does with a left outer domain join, for the aggregates `top` over `lower` (in
  // a struct if there are several, since a scalar subquery returns one column), in a Project
  // over `outer`.
  private def countBugHandling(
      lower: LogicalPlan,
      top: Seq[Expression] = Seq(count(Literal(1))),
      grouping: Seq[Expression] = Nil,
      outer: LogicalPlan = testRelation): (Option[Boolean], Boolean) = {
    val result = if (top.size == 1) top.head else CreateStruct(top)
    val subPlan = lower.groupBy(grouping: _*)(result.as("r"))
    countBugHandlingIn(outer.select(ScalarSubquery(subPlan).as("sq")))
  }

  // The correlated scalar subquery with the output `r` in `query` after the rule, which also
  // checks that running the rule again does not change the plan.
  private def subqueryWithOutputR(query: LogicalPlan): ScalarSubquery = {
    val optimized = Optimize.execute(query.analyze)
    comparePlans(Optimize.execute(optimized), optimized, false)
    val subqueries = optimized.flatMap(_.expressions).flatMap(_.collect {
      case s: ScalarSubquery if s.children.nonEmpty && s.plan.output.head.name == "r" => s
    })
    assert(subqueries.size == 1)
    subqueries.head
  }

  // countBugHandling of the correlated scalar subquery with the output `r` in `query`.
  private def countBugHandlingIn(query: LogicalPlan): (Option[Boolean], Boolean) = {
    val subquery = subqueryWithOutputR(query)
    val countBugDomainJoin = subquery.plan.exists {
      case d: DomainJoin => d.joinType == LeftOuter
      case _ => false
    }
    (subquery.mayHaveCountBug, countBugDomainJoin)
  }

  // ScalarSubquery.countBugResult of the subquery of countBugHandling, as a string.
  private def countBugResult(
      lower: LogicalPlan,
      top: Seq[Expression] = Seq(count(Literal(1))),
      grouping: Seq[Expression] = Nil,
      outer: LogicalPlan = testRelation,
      above: LogicalPlan => LogicalPlan = identity): Option[String] = {
    val result = if (top.size == 1) top.head else CreateStruct(top)
    val subPlan = lower.groupBy(grouping: _*)(result.as("r"))
    subqueryWithOutputR(above(outer.select(ScalarSubquery(subPlan).as("sq"))))
      .countBugResult.map(_.toString)
  }

  private def correlated(agg: Expression): LogicalPlan =
    testRelation2.where($"a" === $"c").groupBy()(agg.as("s"))

  private def uncorrelated(agg: Expression): LogicalPlan =
    testRelation2.groupBy()(agg.as("s")).where($"s" > $"b")

  // Evaluates `f` with the handling of the count bug before SPARK-60501.
  private def asBefore[T](f: => T): T =
    withSQLConf(SQLConf.LEGACY_SCALAR_SUBQUERY_NESTED_AGGREGATE_COUNT_BUG_BEHAVIOR.key -> "true") {
      f
    }

  test("SPARK-60501: the result for an outer row without matching rows of a correlated nested " +
      "ungrouped aggregate, and where it is not used") {
    val inConstructLeftJoins = (Some(true), false)
    // The lower aggregate returns one row on empty input whatever its function is. Without a
    // function with a default result there, the result for an outer row without matching rows
    // is computed before decorrelation, and constructLeftJoins uses it, without a domain join.
    Seq(max($"d"), sum($"d"), coalesce(sum($"d"), Literal(10.0))).foreach { agg =>
      assert(countBugHandling(correlated(agg)) == inConstructLeftJoins, agg)
      assert(countBugResult(correlated(agg)) == Some("1"), agg)
    }
    // Also when the correlated predicate is not right below the lower aggregate.
    val deeper = testRelation2.where($"a" === $"c").select($"d").groupBy()(max($"d").as("s"))
    assert(countBugResult(deeper) == Some("1"))
    // When the computed result is NULL, constructLeftJoins adds no count-bug branch (it maps a
    // NULL countBugResult to the plan without the branch), which is correct and unlike before,
    // where the result over no rows was not NULL. E.g. IF(COUNT(*) = 1, NULL, COUNT(*)) over a MAX
    // is NULL for an outer row without matching rows (COUNT over the one row of the lower MAX is
    // 1), where the result over no rows was 0. The golden file checks the result.
    val nullIfOne =
      If(EqualTo(count(Literal(1)), Literal(1L)), Literal(null, LongType), count(Literal(1)))
    assert(countBugResult(correlated(max($"d")), Seq(nullIfOne)) == Some("null"))
    // Where it cannot be computed, e.g. with a filter between the aggregates, the subquery keeps
    // the handling of before, as does one over a lower COUNT, which DecorrelateInnerQuery
    // handles, a grouped lower aggregate, which returns no row on empty input, and an
    // uncorrelated one, which keeps its row.
    val filtered = correlated(max($"d")).where($"s" > 0)
    val grouped = testRelation2.where($"a" === $"c").groupBy($"c")(max($"d").as("s"))
    Seq(filtered, deeper.where($"s" > 0), correlated(count($"d")), grouped,
        uncorrelated(max($"d")), uncorrelated(count($"d"))).foreach { lower =>
      assert(countBugResult(lower).isEmpty, lower)
      assert(countBugHandling(lower) == asBefore(countBugHandling(lower)), lower)
    }
    assert(countBugHandling(correlated(count($"d"))) == (Some(false), true))
    // The result needs no domain join for an equality correlation, so it applies whatever the
    // outer input is and whatever is above the subquery.
    val streamingOuter = testRelation.copy(isStreaming = true)
    val nonDeterministicOuter = testRelation.where(Rand(0) > 0.5)
    val cteOuter = CTERelationRef(0, _resolved = true, testRelation.output, isStreaming = false)
    val mapOuter = LocalRelation(AttributeReference("m", MapType(IntegerType, IntegerType))())
    Seq(streamingOuter, nonDeterministicOuter, cteOuter, testRelation.limit(10),
        Sample(0.0, 0.5, withReplacement = false, 1L, testRelation)).foreach { outer =>
      assert(countBugHandling(correlated(max($"d")), outer = outer) == inConstructLeftJoins,
        outer)
      assert(countBugResult(correlated(max($"d")), outer = outer) == Some("1"), outer)
    }
    assert(countBugResult(correlated(max($"d")), above = _.where(Rand(0) > 0.5)) == Some("1"))
    assert(countBugResult(correlated(max($"d")), above = _.limit(10)) == Some("1"))
    val overMap = testRelation2.where($"c" === Size($"m")).groupBy()(max($"d").as("s"))
    assert(countBugResult(overMap, outer = mapOuter) == Some("1"))
    // For a non-equality correlation or one below a UNION ALL, decorrelation adds the domain
    // join, as before SPARK-60501, also in a single aggregate. It computes the outer input
    // again, and may miss outer rows with matching rows where that gives other rows, and it
    // keeps one of the outer values that compare equal, e.g. 0.0 for -0.0 and 0.0. That does
    // not matter for a result of every outer row, such as COUNT(*) over the row of the MAX, but
    // other results are not used there.
    def countOver(lower: LogicalPlan): LogicalPlan = lower.groupBy()(count(Literal(1)).as("r"))
    val nonEquality = testRelation2.where($"a" > $"c").groupBy()(max($"d").as("s"))
    val isNullCount = count(Literal(1), Some($"s".isNull))
    val coalescedSum = coalesce(sum($"s"), Literal(5.0))
    val belowUnion = testRelation2.where($"a" === $"c").select($"d")
      .union(testRelation2.where($"a" === $"c").select($"d")).groupBy()(max($"d").as("s"))
    val overDouble = testRelation2.where($"b" > $"d").groupBy()(max($"d").as("s"))
    Seq(nonEquality, belowUnion, overDouble).foreach { lower =>
      assert(countBugResult(lower) == Some("1"), lower)
      assert(countBugResult(lower, outer = nonDeterministicOuter) == Some("1"), lower)
      // Also for a grouped top aggregate, which sees the one group of the row of the MAX.
      assert(countBugResult(lower, grouping = Seq($"s")) == Some("1"), lower)
      assert(countBugResult(lower, Seq(max($"s")), Seq($"s")).isEmpty, lower)
      assert(countBugResult(lower, Seq(isNullCount), Seq($"s")).isEmpty, lower)
      assert(countBugResult(lower, Seq(coalescedSum)).isEmpty, lower)
      assert(countBugResult(lower, Seq(isNullCount)).isEmpty, lower)
      assert(countBugHandling(lower, Seq(isNullCount)) ==
        asBefore(countBugHandling(lower, Seq(isNullCount))), lower)
    }
    assert(subqueryWithOutputR(testRelation.select(ScalarSubquery(countOver(nonEquality))
      .as("sq"))).plan.exists(_.isInstanceOf[DomainJoin]))
    // A user-defined aggregate function in a lower aggregate without GROUP BY can return a value
    // other than NULL on empty input, which cannot be computed without running user code: a
    // Python UDAF and a DSv2 function here, a Scala Aggregator in SubquerySuite, and test functions
    // marked either way further below. The aggregate still returns one row, so a top aggregate
    // that does not need the value, such as COUNT(*), gets its result, and one that needs it
    // keeps the handling of before. One that is not
    // correlated, in the input below the correlated aggregate, is not evaluated, and one in a
    // grouped aggregate below returns no row on empty input, which does not matter either.
    val v2Sum = new V2AggregateFunction[java.lang.Long, java.lang.Long] {
      override def newAggregationState(): java.lang.Long = 0L
      override def update(state: java.lang.Long, input: InternalRow): java.lang.Long =
        state + input.getLong(0)
      override def merge(l: java.lang.Long, r: java.lang.Long): java.lang.Long = l + r
      override def produceResult(state: java.lang.Long): java.lang.Long = state
      override def name(): String = "v2_sum"
      override def inputTypes(): Array[DataType] = Array(LongType)
      override def resultType(): DataType = LongType
    }
    Seq(PythonUDAF("udaf", null, LongType, Seq($"c"), udfDeterministic = true),
        V2Aggregator(v2Sum, Seq(Cast($"c", LongType)))).foreach { f =>
      val udaf = f.toAggregateExpression()
      assert(countBugResult(correlated(udaf)) == Some("1"), f)
      assert(countBugResult(correlated(udaf), Seq(isNullCount)).isEmpty, f)
      assert(countBugHandling(correlated(udaf), Seq(isNullCount)) ==
        asBefore(countBugHandling(correlated(udaf), Seq(isNullCount))), f)
      val inInput = testRelation2.where($"a" === $"c")
        .join(LocalRelation($"c".int).groupBy()(udaf.as("u"))).groupBy()(max($"d").as("s"))
      assert(countBugResult(inInput) == Some("1"), f)
      val overGrouped = testRelation2.where($"a" === $"c").groupBy($"c")(udaf.as("u"))
        .groupBy()(max($"u").as("s"))
      assert(countBugResult(overGrouped) == Some("1"), f)
    }
    // A user-defined function in a top aggregate is not run over the row of the lower one either:
    // the subquery keeps the handling of before.
    Seq(UserDefinedSumAggregate($"s"), UserDefinedSumFunction($"s")).foreach { f =>
      val udTop = f.toAggregateExpression()
      assert(countBugResult(correlated(max($"d")), Seq(udTop)).isEmpty, f)
    }
    // Nor is one in a lower aggregate, marked either way, run on empty input: a top that needs its
    // value keeps the handling of before, and COUNT(*) gets its result.
    Seq(UserDefinedSumAggregate($"d"), UserDefinedSumFunction($"d")).foreach { f =>
      val lower = correlated(f.toAggregateExpression())
      assert(countBugResult(lower, Seq(isNullCount)).isEmpty, f)
      assert(countBugResult(lower) == Some("1"), f)
    }
    // Nor over the one row of a lower aggregate in an aggregate between: a function that cannot be
    // computed over that row, here PERCENTILE or a user-defined one, gives a value that is not
    // known, so a top that needs it keeps the handling of before, and COUNT(*) gets its result.
    val isNullCountP = count(Literal(1), Some($"p".isNull))
    Seq(new Percentile($"s", Literal(0.5)) -> Literal(0.0),
        UserDefinedSumAggregate($"s") -> Literal(0L),
        UserDefinedSumFunction($"s") -> Literal(0L)).foreach { case (f, zero) =>
      val between = correlated(max($"d")).groupBy()(f.toAggregateExpression().as("p"))
      assert(countBugResult(between) == Some("1"), f)
      assert(countBugResult(between, Seq(sum($"p"))).isEmpty, f)
      assert(countBugResult(between, Seq(isNullCountP)).isEmpty, f)
      assert(countBugHandling(between, Seq(sum($"p"))) ==
        asBefore(countBugHandling(between, Seq(sum($"p")))), f)
      // The other values of that row are still taken.
      val twoValues = correlated(max($"d")).groupBy()(f.toAggregateExpression().as("p"),
        coalesce(max($"s"), Literal(7.0)).as("q"))
      assert(countBugResult(twoValues, Seq(sum($"q"))) == Some("7.0"), f)
      // A Project above passes on that the value is not known, and whether it can be NULL.
      val isNullP = between.select($"p".isNull.as("q"))
      assert(countBugResult(isNullP, Seq(count(Literal(1), Some($"q")))).isEmpty, f)
      val coalescedP = between.select(coalesce($"p", zero).as("q"))
      assert(countBugResult(coalescedP, Seq(count($"q"))) == Some("1"), f)
    }
    // Nor with the legacy conf, or where the count bug is not handled at all.
    asBefore {
      assert(countBugResult(correlated(max($"d"))).isEmpty)
      assert(countBugHandling(correlated(max($"d"))) == inConstructLeftJoins)
    }
    Seq(SQLConf.LEGACY_SCALAR_SUBQUERY_COUNT_BUG_HANDLING.key -> "true",
        SQLConf.DECORRELATE_INNER_QUERY_ENABLED.key -> "false").foreach { kv =>
      withSQLConf(kv) {
        assert(countBugResult(correlated(max($"d"))).isEmpty, kv)
      }
    }
  }

  test("SPARK-60501: compute the result for an outer row without matching rows before " +
      "decorrelation") {
    val inConstructLeftJoins = (Some(true), false)
    val lowerMax = correlated(max($"d"))
    // The top aggregate over the row of NULLs that the lower MAX returns on empty input, which
    // differs from its result over no rows for some functions, such as COUNT(*): with FILTER and
    // DISTINCT, functions that collect the NULL, and expressions over the functions.
    Seq(count(Literal(1)) -> "1", count($"s") -> "0", sum($"s") -> "null", max($"s") -> "null",
        avg($"s") -> "null", first($"s") -> "null", countDistinct($"s") -> "0",
        count(Literal(1), Some($"s".isNull)) -> "1", count(Literal(1), Some($"s" > 1)) -> "0",
        sum($"s", Some($"s" > 1)) -> "null", collectList($"s") -> "[]",
        CollectList($"s", ignoreNulls = false).toAggregateExpression() -> "[null]",
        CollectSet($"s", ignoreNulls = false).toAggregateExpression() -> "[null]",
        StddevSamp($"s").toAggregateExpression() -> "null",
        coalesce(sum($"s"), Literal(10.0)) -> "10.0", sum($"s") + 1 -> "null",
        Literal(1) -> "1").foreach { case (top, result) =>
      assert(countBugHandling(lowerMax, Seq(top)) == inConstructLeftJoins, top)
      assert(countBugResult(lowerMax, Seq(top)) == Some(result), top)
    }
    assert(countBugResult(lowerMax, Seq(count(Literal(1)), collectList($"s"))) == Some("[1,[]]"))
    // Through Projects, over another function and a literal of the lower aggregate, over three
    // levels and a grouped aggregate between, and for a grouped top aggregate, which returns a row
    // for the group of the row of the lower aggregate.
    assert(countBugResult(lowerMax.select(($"s" + 1).as("t")), Seq(count($"t"))) == Some("0"))
    assert(countBugResult(lowerMax.select(coalesce($"s", Literal(2.0)).as("t")), Seq(sum($"t")))
      == Some("2.0"))
    val lowerMaxAndMin =
      testRelation2.where($"a" === $"c").groupBy()(max($"d").as("s"), min($"d").as("n"))
    assert(countBugResult(lowerMaxAndMin, Seq(sum($"s"), count($"n"))) == Some("[null,0]"))
    val lowerWithLiteral =
      testRelation2.where($"a" === $"c").groupBy()(Literal(10).as("s"), max($"d").as("m"))
    assert(countBugResult(lowerWithLiteral, Seq(sum($"s"))) == Some("10"))
    assert(countBugResult(lowerMax.groupBy()(max($"s").as("s"))) == Some("1"))
    assert(countBugResult(lowerMax.groupBy($"s")($"s")) == Some("1"))
    // A grouped aggregate below returns no row on empty input, so the MAX over it is NULL, not
    // the MAX over a row with its COUNT of 0.
    val groupedBelow = testRelation2.where($"a" === $"c").groupBy($"d")(count(Literal(1)).as("n"))
      .groupBy()(max($"n").as("s"))
    assert(countBugResult(groupedBelow, Seq(count($"s"))) == Some("0"))
    assert(countBugHandling(lowerMax, grouping = Seq($"s")) == inConstructLeftJoins)
    assert(countBugResult(lowerMax, grouping = Seq($"s")) == Some("1"))
    // A NULL result of a grouped top aggregate is not used: the handling without it gives NULL
    // too, as the subquery returns no row there.
    assert(countBugResult(lowerMax, Seq(max($"s")), Seq($"s")).isEmpty)
    assert(countBugHandling(lowerMax, Seq(max($"s")), Seq($"s")) ==
      asBefore(countBugHandling(lowerMax, Seq(max($"s")), Seq($"s"))))
    // COLLECT_LIST with RESPECT NULLS over the row of NULLs, with a FILTER that does not hold.
    assert(countBugResult(lowerMax,
      Seq(CollectList($"s", ignoreNulls = false).toAggregateExpression(false, Some($"s" > 1))))
      == Some("[]"))
    // And with a FILTER that holds, which keeps the value: the NULL of the row of the MAX, and a
    // literal of the lower aggregate.
    assert(countBugResult(lowerMax,
      Seq(CollectList($"s", ignoreNulls = false).toAggregateExpression(false, Some($"s".isNull))))
      == Some("[null]"))
    assert(countBugResult(lowerWithLiteral,
      Seq(collectList($"s", Some($"s" > 1)))) == Some("[10]"))
    // And over arguments computed over the row, which skip the NULL or collect a struct of it.
    assert(countBugResult(lowerMax, Seq(collectList($"s" + 1))) == Some("[]"))
    assert(countBugResult(lowerMax, Seq(collectList(CreateStruct(Seq($"s"))))) ==
      Some("[[null]]"))
    // An outer reference between the aggregates, here in a Project, does not change the rows,
    // only values, which are not computed (see below), so it does not matter for a COUNT(*).
    assert(countBugResult(lowerMax.select(($"s" + $"a").as("s"))) == Some("1"))
    // The string of the subquery shows the result.
    assert(subqueryWithOutputR(testRelation.select(
      ScalarSubquery(lowerMax.groupBy()(count(Literal(1)).as("r"))).as("sq")))
      .toString.endsWith(" countBugResult=1"))
    // An expression that fails on the row of the lower aggregate does not matter when the top
    // aggregate does not use it.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val failing = lowerMax.select((Literal(10.0) / coalesce($"s", Literal(0.0))).as("t"))
      assert(countBugResult(failing) == Some("1"))
      // Otherwise the result is not computed, and the subquery keeps the handling of before,
      // also for a grouping key.
      assert(countBugResult(failing, Seq(sum($"t"))).isEmpty)
      assert(countBugResult(failing, grouping = Seq($"t")).isEmpty)
      assert(countBugResult(failing, Seq(collectList($"t"))).isEmpty)
      // Nor for one that evaluation could get without the failing value, e.g. COALESCE(1, t).
      assert(countBugResult(failing, Seq(sum(coalesce(Literal(1.0), $"t")))).isEmpty)
      assert(countBugHandling(failing, Seq(sum($"t"))) ==
        asBefore(countBugHandling(failing, Seq(sum($"t")))))
    }
    // Neither is it for a value or a grouping key over an outer reference, a function that is not
    // computed over one row, such as PERCENTILE, a FILTER with a subquery, a Python UDF in the
    // result, and a function whose evaluate expression does not have its data type.
    val subqueryRelation = LocalRelation($"e".double)
    val uncorrelatedFilter = ScalarSubquery(subqueryRelation.groupBy()(max($"e").as("x"))) > 0
    val correlatedFilter = ScalarSubquery(
      subqueryRelation.where($"e" === $"s").groupBy()(max($"e").as("x"))).isNotNull
    val pyUdf = PythonUDF("udf", null, LongType, Seq($"s"), PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true)
    assert(countBugResult(lowerMax.select(pyUdf.as("g")), grouping = Seq($"g")).isEmpty)
    assert(countBugResult(lowerMax.select(($"s" + $"a").as("s")), grouping = Seq($"s")).isEmpty)
    Seq(lowerMax.select(($"s" + $"a").as("s")) -> sum($"s"),
        lowerMax -> new Percentile($"s", Literal(0.5)).toAggregateExpression(),
        lowerMax -> sum($"s", Some(uncorrelatedFilter)),
        lowerMax -> sum($"s", Some(correlatedFilter)),
        lowerMax -> sum(pyUdf),
        lowerMax -> MistypedAggregate($"s").toAggregateExpression()).foreach { case (lower, top) =>
      assert(countBugResult(lower, Seq(top)).isEmpty, (lower, top))
      assert(countBugHandling(lower, Seq(top)) == asBefore(countBugHandling(lower, Seq(top))),
        (lower, top))
    }
    // A Python UDF over the lower aggregate does not matter when the top aggregate does not use
    // its result.
    val udfOverMax = PythonUDF("udf", null, LongType, Seq(max($"d")),
      PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)
    assert(countBugResult(correlated(udfOverMax)) == Some("1"))
    assert(countBugResult(correlated(udfOverMax), Seq(sum($"s"))).isEmpty)
    // It is not tried for a lower aggregate with a function with a default result, which is
    // handled in decorrelation as before, even when the top aggregate does not use it or its
    // default result is NULL.
    val lowerMaxAndCount =
      testRelation2.where($"a" === $"c").groupBy()(max($"d").as("s"), count($"d").as("n"))
    val lowerListAgg = correlated(ListAgg(Cast($"d", StringType)).toAggregateExpression())
    val countBelowProject = correlated(count($"d")).select($"s").groupBy()(max($"s").as("s"))
    Seq(lowerMaxAndCount -> sum($"s"), lowerMaxAndCount -> sum($"n"), lowerListAgg -> count($"s"),
        countBelowProject -> sum($"s")).foreach { case (lower, top) =>
      assert(countBugResult(lower, Seq(top)).isEmpty, (lower, top))
      assert(countBugHandling(lower, Seq(top)) == (Some(false), true), (lower, top))
    }
    // A lower function without a default result is taken with its own result over no rows, which
    // is not NULL for some: pandas_product gives 1, also where it is nullable, and PivotFirst an
    // array of NULLs. So the NULL-sensitive top over them counts no NULL, as on empty input.
    val isNullCount = count(Literal(1), Some($"s".isNull))
    val pivot = PivotFirst(testRelation2.output(0), testRelation2.output(1), Seq(1, 2))
    Seq(PandasProduct($"d", ignoreNA = true), PandasProduct($"d", ignoreNA = false), pivot)
        .foreach { f =>
      val lower = correlated(f.toAggregateExpression())
      assert(countBugResult(lower, Seq(isNullCount)) == Some("0"), f)
      assert(countBugResult(lower) == Some("1"), f)
    }
    // PERCENTILE, through a fresh buffer, gives NULL over no rows.
    val lowerPercentile = correlated(new Percentile($"d", Literal(0.5)).toAggregateExpression())
    assert(countBugResult(lowerPercentile, Seq(isNullCount)) == Some("1"))
    // Where the result of a lower function over no rows cannot be computed, here because its
    // evaluate expression does not have its data type, it is NULL for a function that is
    // declared not nullable, or its result over a fresh buffer fails, a top aggregate that needs
    // it keeps the handling of before, and COUNT(*) still gets its result.
    Seq(MistypedAggregate($"d"), NullOnEmptyAggregate($"d"), FailingOnEmptyAggregate($"d"))
        .foreach { f =>
      val lower = correlated(f.toAggregateExpression())
      assert(countBugResult(lower, Seq(isNullCount)).isEmpty, f)
      assert(countBugHandling(lower, Seq(isNullCount)) ==
        asBefore(countBugHandling(lower, Seq(isNullCount))), f)
      assert(countBugResult(lower) == Some("1"), f)
    }
  }

  test("SPARK-60501: the work to compute the result before decorrelation is linear in the levels") {
    // A value that is not a literal, here one that fails in ANSI mode or one over an outer
    // reference, is used twice at each level. Copying it into each use would double the work with
    // each level, so that 22 levels would take more than 30 seconds.
    val executor = ThreadUtils.newDaemonSingleThreadExecutor("spark-60501-levels")
    try {
      val done = Future {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
          Seq(Literal(10.0) / coalesce($"s", Literal(0.0)), $"s" + $"a").foreach { value =>
            var lower = correlated(max($"d")).select(value.as("t"))
            (1 to 22).foreach { _ => lower = lower.select(($"t" + $"t").as("t")) }
            assert(countBugResult(lower) == Some("1"), value)
            assert(countBugResult(lower, Seq(sum($"t"))).isEmpty, value)
          }
        }
      }(ExecutionContext.fromExecutor(executor))
      ThreadUtils.awaitResult(done, 30.seconds)
    } finally {
      executor.shutdownNow()
    }
  }

  test("SPARK-60501: canonicalized keeps countBugResult so two subqueries with the same plan " +
      "but a different result are not treated as equal") {
    // Two subqueries with the same plan, one with a computed count-bug result and one without,
    // must not be treated as semantically equal, which would let RewriteCorrelatedScalarSubquery
    // replace one with the other. canonicalized keeps countBugResult for that.
    val plan = testRelation2.groupBy()(max($"d").as("s"))
      .groupBy()(count(Literal(1)).as("r")).analyze
    val withResult = ScalarSubquery(plan, countBugResult = Some(Literal(1L)))
    val withoutResult = ScalarSubquery(plan)
    assert(withResult.plan.canonicalized == withoutResult.plan.canonicalized)
    assert(!withResult.semanticEquals(withoutResult))
    // Two with the same computed result are still equal.
    assert(withResult.semanticEquals(ScalarSubquery(plan, countBugResult = Some(Literal(1L)))))
  }

  test("PullupCorrelatedPredicates should handle deletes") {
    val subPlan = testRelation2.where($"a" === $"c").select($"c")
    val cond = InSubquery(Seq($"a"), ListQuery(subPlan))
    val deletePlan = DeleteFromTable(testRelation, cond).analyze
    assert(deletePlan.resolved)

    val optimized = Optimize.execute(deletePlan)
    assert(optimized.resolved)

    optimized match {
      case DeleteFromTable(_, s: InSubquery) =>
        val outerRefs = SubExprUtils.getOuterReferences(s.query.plan)
        assert(outerRefs.isEmpty, "should be no outer refs")
      case other =>
        fail(s"unexpected logical plan: $other")
    }
  }

  test("PullupCorrelatedPredicates should handle updates") {
    val subPlan = testRelation2.where($"a" === $"c").select($"c")
    val cond = InSubquery(Seq($"a"), ListQuery(subPlan))
    val updatePlan = UpdateTable(testRelation, Seq.empty, Some(cond)).analyze
    assert(updatePlan.resolved)

    val optimized = Optimize.execute(updatePlan)
    assert(optimized.resolved)

    optimized match {
      case UpdateTable(_, _, Some(s: InSubquery)) =>
        val outerRefs = SubExprUtils.getOuterReferences(s.query.plan)
        assert(outerRefs.isEmpty, "should be no outer refs")
      case other =>
        fail(s"unexpected logical plan: $other")
    }
  }

  test("PullupCorrelatedPredicates should handle merge") {
    val testRelation3 = LocalRelation($"e".int, $"f".double)
    val subPlan = testRelation3.where($"a" === $"e").select($"e")
    val cond = InSubquery(Seq($"a"), ListQuery(subPlan))

    val mergePlan = MergeIntoTable(
      testRelation,
      testRelation2,
      cond,
      Seq(DeleteAction(None)),
      Seq(InsertAction(None, Seq(Assignment($"a", $"c"), Assignment($"b", $"d")))),
      Seq(DeleteAction(None)),
      withSchemaEvolution = false)
    val analyzedMergePlan = mergePlan.analyze
    assert(analyzedMergePlan.resolved)

    val optimized = Optimize.execute(analyzedMergePlan)
    assert(optimized.resolved)

    optimized match {
      case MergeIntoTable(_, _, s: InSubquery, _, _, _, _) =>
        val outerRefs = SubExprUtils.getOuterReferences(s.query.plan)
        assert(outerRefs.isEmpty, "should be no outer refs")
      case other =>
        fail(s"unexpected logical plan: $other")
    }
  }
}
