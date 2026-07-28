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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateArray, Expression, GetArrayItem, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, ApproximatePercentile}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

class CombineApproximatePercentilesSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    override val batches: Seq[Batch] =
      Batch("Combine Approximate Percentiles", Once, CombineApproximatePercentiles) :: Nil
  }

  private val relation = LocalRelation($"value".int, $"other".int, $"group".int)
  private val value = relation.output(0)
  private val other = relation.output(1)
  private val group = relation.output(2)

  private def percentile(
      child: Expression,
      percentage: Double,
      accuracy: Int = 10000,
      isDistinct: Boolean = false,
      filter: Option[Expression] = None): AggregateExpression = {
    new ApproximatePercentile(child, Literal(percentage), Literal(accuracy))
      .toAggregateExpression(isDistinct = isDistinct, filter = filter)
  }

  private def optimizedAggregate(expressions: Alias*): Aggregate = {
    val plan = Aggregate(Seq.empty, expressions, relation)
    Optimize.execute(plan.analyze).asInstanceOf[Aggregate]
  }

  private def percentileAggregates(aggregate: Aggregate): Seq[AggregateExpression] = {
    aggregate.aggregateExpressions.flatMap(_.collect {
      case expression @ AggregateExpression(_: ApproximatePercentile, _, _, _, _) => expression
    })
  }

  test("combine scalar percentiles into one shared array-valued digest") {
    val result = optimizedAggregate(
      Alias(percentile(value, 0.5), "p50")(),
      Alias(percentile(value, 0.9), "p90")(),
      Alias(percentile(value, 0.95), "p95")())

    val aggregateExpressions = percentileAggregates(result)
    assert(aggregateExpressions.size == 3)
    assert(aggregateExpressions.map(_.resultId).distinct.size == 1)

    val combined = aggregateExpressions.head.aggregateFunction
      .asInstanceOf[ApproximatePercentile]
    assert(combined.percentageExpression ==
      CreateArray(Seq(Literal(0.5), Literal(0.9), Literal(0.95))))

    val ordinals = result.aggregateExpressions.map(_.collectFirst {
      case GetArrayItem(_, Literal(index: Int, _), false) => index
    })
    assert(ordinals == Seq(Some(0), Some(1), Some(2)))
    assert(result.output.map(_.name) == Seq("p50", "p90", "p95"))
  }

  test("preserve duplicate and non-monotonic percentile order") {
    val result = optimizedAggregate(
      Alias(percentile(value, 0.9), "first")(),
      Alias(percentile(value, 0.5), "second")(),
      Alias(percentile(value, 0.9), "third")())

    val combined = percentileAggregates(result).head.aggregateFunction
      .asInstanceOf[ApproximatePercentile]
    assert(combined.percentageExpression ==
      CreateArray(Seq(Literal(0.9), Literal(0.5), Literal(0.9))))
    assert(percentileAggregates(result).map(_.resultId).distinct.size == 1)
  }

  test("combine scalar percentiles nested in a result expression") {
    val result = optimizedAggregate(
      Alias(percentile(value, 0.5) + percentile(value, 0.9), "percentile_sum")())

    val expressions = percentileAggregates(result)
    assert(expressions.size == 2)
    assert(expressions.map(_.resultId).distinct.size == 1)
    assert(result.output.head.name == "percentile_sum")
  }

  test("combine structurally equivalent input expressions") {
    val result = optimizedAggregate(
      Alias(percentile(value + Literal(1), 0.5), "p50")(),
      Alias(percentile(value + Literal(1), 0.9), "p90")())

    assert(percentileAggregates(result).map(_.resultId).distinct.size == 1)
  }

  test("do not combine differently ordered input expressions") {
    val result = optimizedAggregate(
      Alias(percentile(value + Literal(1), 0.5), "p50")(),
      Alias(percentile(Literal(1) + value, 0.9), "p90")())

    assert(percentileAggregates(result).map(_.resultId).distinct.size == 2)
    assert(!result.aggregateExpressions.exists(_.exists(_.isInstanceOf[GetArrayItem])))
  }

  test("do not combine differently associated input expressions") {
    val result = optimizedAggregate(
      Alias(percentile((value + other) + group, 0.5), "p50")(),
      Alias(percentile(value + (other + group), 0.9), "p90")())

    assert(percentileAggregates(result).map(_.resultId).distinct.size == 2)
    assert(!result.aggregateExpressions.exists(_.exists(_.isInstanceOf[GetArrayItem])))
  }

  test("do not fuse canonically equivalent but structurally different input groups") {
    val firstInput = value + (other + group)
    val secondInput = (value + other) + group
    val result = optimizedAggregate(
      Alias(percentile(firstInput, 0.5), "first_p50")(),
      Alias(percentile(secondInput, 0.5), "second_p50")(),
      Alias(percentile(secondInput, 0.9), "second_p90")(),
      Alias(percentile(firstInput, 0.9), "first_p90")())

    assert(firstInput != secondInput)
    assert(firstInput.canonicalized == secondInput.canonicalized)
    assert(percentileAggregates(result).map(_.resultId).distinct.size == 4)
    assert(!result.aggregateExpressions.exists(_.exists(_.isInstanceOf[GetArrayItem])))
  }

  test("do not fuse canonically equivalent but differently evaluated accuracies") {
    val firstAccuracy =
      ((Literal(1.0e16) + Literal(-1.0e16)) + Literal(3.0)).cast(IntegerType)
    val secondAccuracy =
      (Literal(1.0e16) + (Literal(-1.0e16) + Literal(3.0))).cast(IntegerType)

    def percentileWithAccuracy(
        percentage: Double,
        accuracy: Expression): AggregateExpression = {
      new ApproximatePercentile(value, Literal(percentage), accuracy).toAggregateExpression()
    }

    val result = optimizedAggregate(
      Alias(percentileWithAccuracy(0.5, firstAccuracy), "first_p50")(),
      Alias(percentileWithAccuracy(0.5, secondAccuracy), "second_p50")(),
      Alias(percentileWithAccuracy(0.9, secondAccuracy), "second_p90")(),
      Alias(percentileWithAccuracy(0.9, firstAccuracy), "first_p90")())

    assert(firstAccuracy.eval() == 3)
    assert(secondAccuracy.eval() == 4)
    assert(firstAccuracy.canonicalized == secondAccuracy.canonicalized)
    assert(percentileAggregates(result).map(_.resultId).distinct.size == 4)
    assert(!result.aggregateExpressions.exists(_.exists(_.isInstanceOf[GetArrayItem])))
  }

  test("do not combine percentiles with different input columns") {
    val result = optimizedAggregate(
      Alias(percentile(value, 0.5), "value_p50")(),
      Alias(percentile(other, 0.9), "other_p90")())

    assert(percentileAggregates(result).map(_.resultId).distinct.size == 2)
    assert(!result.aggregateExpressions.exists(_.exists(_.isInstanceOf[GetArrayItem])))
  }

  test("do not combine percentiles with different accuracies") {
    val result = optimizedAggregate(
      Alias(percentile(value, 0.5, accuracy = 1000), "p50")(),
      Alias(percentile(value, 0.9, accuracy = 10000), "p90")())

    assert(percentileAggregates(result).map(_.resultId).distinct.size == 2)
    assert(!result.aggregateExpressions.exists(_.exists(_.isInstanceOf[GetArrayItem])))
  }

  test("combine only percentiles with structurally equivalent filters") {
    val firstFilter = value > Literal(0)
    val equivalentFilter = value > Literal(0)
    val secondFilter = value > Literal(1)
    val result = optimizedAggregate(
      Alias(percentile(value, 0.5, filter = Some(firstFilter)), "p50")(),
      Alias(percentile(value, 0.9, filter = Some(equivalentFilter)), "p90")(),
      Alias(percentile(value, 0.95, filter = Some(secondFilter)), "p95")())

    val expressions = percentileAggregates(result)
    assert(expressions.take(2).map(_.resultId).distinct.size == 1)
    assert(expressions.map(_.resultId).distinct.size == 2)
  }

  test("do not combine percentiles with differently associated filters") {
    val firstFilter = ((value + other) + group) > Literal(0)
    val secondFilter = (value + (other + group)) > Literal(0)
    val result = optimizedAggregate(
      Alias(percentile(value, 0.5, filter = Some(firstFilter)), "p50")(),
      Alias(percentile(value, 0.9, filter = Some(secondFilter)), "p90")())

    assert(percentileAggregates(result).map(_.resultId).distinct.size == 2)
    assert(!result.aggregateExpressions.exists(_.exists(_.isInstanceOf[GetArrayItem])))
  }

  test("do not combine filtered and unfiltered percentiles") {
    val result = optimizedAggregate(
      Alias(percentile(value, 0.5), "p50")(),
      Alias(percentile(value, 0.9, filter = Some(value > Literal(0))), "p90")())

    assert(percentileAggregates(result).map(_.resultId).distinct.size == 2)
    assert(!result.aggregateExpressions.exists(_.exists(_.isInstanceOf[GetArrayItem])))
  }

  test("do not combine distinct and non-distinct percentiles") {
    val result = optimizedAggregate(
      Alias(percentile(value, 0.5), "p50")(),
      Alias(percentile(value, 0.9, isDistinct = true), "p90")())

    assert(percentileAggregates(result).map(_.resultId).distinct.size == 2)
    assert(!result.aggregateExpressions.exists(_.exists(_.isInstanceOf[GetArrayItem])))
  }

  test("combine distinct percentiles with the same input") {
    val result = optimizedAggregate(
      Alias(percentile(value, 0.5, isDistinct = true), "p50")(),
      Alias(percentile(value, 0.9, isDistinct = true), "p90")())

    assert(percentileAggregates(result).map(_.resultId).distinct.size == 1)
  }

  test("leave existing array percentiles unchanged") {
    val arrayPercentile = new ApproximatePercentile(
      value,
      CreateArray(Seq(Literal(0.5), Literal(0.9))),
      Literal(10000)).toAggregateExpression()
    val result = optimizedAggregate(
      Alias(arrayPercentile, "percentiles")(),
      Alias(percentile(value, 0.95), "p95")())

    assert(percentileAggregates(result).map(_.resultId).distinct.size == 2)
    assert(!result.aggregateExpressions.exists(_.exists(_.isInstanceOf[GetArrayItem])))
  }

  test("do not fuse percentages that collide with an existing array percentile") {
    val firstPercentage =
      (Literal(1.0e16) + Literal(-1.0e16)) + Literal(0.5)
    val secondPercentage =
      Literal(1.0e16) + (Literal(-1.0e16) + Literal(0.5))
    val arrayPercentile = new ApproximatePercentile(
      value,
      CreateArray(Seq(firstPercentage, Literal(0.9))),
      Literal(10000)).toAggregateExpression()
    val scalarPercentile = new ApproximatePercentile(
      value, secondPercentage, Literal(10000)).toAggregateExpression()
    val result = optimizedAggregate(
      Alias(arrayPercentile, "percentiles")(),
      Alias(scalarPercentile, "p0")(),
      Alias(percentile(value, 0.9), "p90")())

    assert(firstPercentage.eval() == 0.5d)
    assert(secondPercentage.eval() == 0.0d)
    assert(firstPercentage.canonicalized == secondPercentage.canonicalized)
    assert(percentileAggregates(result).map(_.resultId).distinct.size == 3)
    assert(!result.aggregateExpressions.exists(_.exists(_.isInstanceOf[GetArrayItem])))
  }

  test("preserve grouping expressions and output expression identifiers") {
    val p50 = Alias(percentile(value, 0.5), "p50")()
    val p90 = Alias(percentile(value, 0.9), "p90")()
    val original = Aggregate(Seq(group), Seq(group, p50, p90), relation).analyze
      .asInstanceOf[Aggregate]
    val result = Optimize.execute(original).asInstanceOf[Aggregate]

    assert(result.groupingExpressions == original.groupingExpressions)
    assert(result.output.map(_.exprId) == original.output.map(_.exprId))
    assert(percentileAggregates(result).map(_.resultId).distinct.size == 1)
  }

  test("do not rewrite streaming aggregates") {
    val streamingRelation = relation.copy(isStreaming = true)
    val streamingValue = streamingRelation.output.head
    val original = Aggregate(
      Seq.empty,
      Seq(
        Alias(percentile(streamingValue, 0.5), "p50")(),
        Alias(percentile(streamingValue, 0.9), "p90")()),
      streamingRelation).analyze.asInstanceOf[Aggregate]

    assert(original.isStreaming)
    assert(percentileAggregates(original).map(_.resultId).distinct.size == 2)
    assert(Optimize.execute(original).fastEquals(original))
  }

  test("do not rewrite a single scalar percentile") {
    val original = Aggregate(
      Seq.empty,
      Seq(Alias(percentile(value, 0.5), "p50")()),
      relation).analyze

    assert(Optimize.execute(original).fastEquals(original))
  }
}
