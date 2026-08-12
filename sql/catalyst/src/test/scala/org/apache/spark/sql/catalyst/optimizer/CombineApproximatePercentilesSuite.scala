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

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateArray, Expression, GetArrayItem, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, ApproximatePercentile}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
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
    withSQLConf(SQLConf.COMBINE_APPROXIMATE_PERCENTILES_ENABLED.key -> "true") {
      Optimize.execute(plan.analyze).asInstanceOf[Aggregate]
    }
  }

  private def percentileAggregates(aggregate: Aggregate): Seq[AggregateExpression] = {
    aggregate.aggregateExpressions.flatMap(_.collect {
      case expression @ AggregateExpression(_: ApproximatePercentile, _, _, _, _) => expression
    })
  }

  private def percentageValues(percentile: ApproximatePercentile): Seq[Double] = {
    percentile.percentageExpression.eval().asInstanceOf[ArrayData].toDoubleArray().toSeq
  }

  private def ordinals(aggregate: Aggregate): Seq[Option[Int]] = {
    aggregate.aggregateExpressions.map(_.collectFirst {
      case GetArrayItem(_, Literal(index: Int, _), false) => index
    })
  }

  private def assertNotCombined(aggregate: Aggregate): Unit = {
    val expressions = percentileAggregates(aggregate)
    assert(expressions.map(_.resultId).distinct.size == expressions.size)
    assert(!aggregate.aggregateExpressions.exists(_.exists(_.isInstanceOf[GetArrayItem])))
  }

  test("do not combine approximate percentiles when disabled") {
    assert(!new SQLConf().getConf(SQLConf.COMBINE_APPROXIMATE_PERCENTILES_ENABLED))
    val original = Aggregate(
      Seq.empty,
      Seq(
        Alias(percentile(value, 0.5), "p50")(),
        Alias(percentile(value, 0.9), "p90")()),
      relation).analyze.asInstanceOf[Aggregate]

    withSQLConf(SQLConf.COMBINE_APPROXIMATE_PERCENTILES_ENABLED.key -> "false") {
      assertNotCombined(Optimize.execute(original).asInstanceOf[Aggregate])
    }
  }

  test("combine compatible percentiles and preserve output shape") {
    val firstPercentile = percentile(value, 0.9)
    firstPercentile.aggregateFunction.setTagValue(FunctionRegistry.FUNC_ALIAS, "approx_percentile")
    val aliases = Seq(
      Alias(firstPercentile, "first")(),
      Alias(percentile(value, 0.5), "second")(),
      Alias(percentile(value, 0.9), "third")())
    val original = Aggregate(Seq(group), group +: aliases, relation).analyze
      .asInstanceOf[Aggregate]
    val result = withSQLConf(SQLConf.COMBINE_APPROXIMATE_PERCENTILES_ENABLED.key -> "true") {
      Optimize.execute(original).asInstanceOf[Aggregate]
    }
    val aggregates = percentileAggregates(result)
    val combined = aggregates.head.aggregateFunction.asInstanceOf[ApproximatePercentile]

    assert(aggregates.map(_.resultId).distinct.size == 1)
    assert(aggregates.head.resultId != firstPercentile.resultId)
    assert(percentageValues(combined) == Seq(0.9, 0.5, 0.9))
    assert(combined.percentageExpression.toString == "[0.9,0.5,0.9]")
    assert(combined.percentageExpression.sql == "ARRAY(0.9D, 0.5D, 0.9D)")
    assert(combined.prettyName == "approx_percentile")
    assert(ordinals(result) == Seq(None, Some(0), Some(1), Some(2)))
    assert(result.groupingExpressions == original.groupingExpressions)
    assert(result.output.map(_.exprId) == original.output.map(_.exprId))
  }

  test("preserve fusion identity through later constant folding") {
    val result = optimizedAggregate(
      Alias(percentile(value, 0.5), "p50")(),
      Alias(percentile(value, 0.9), "p90")())
    val folded = ConstantFolding(result).asInstanceOf[Aggregate]
    val combined = percentileAggregates(folded).head.aggregateFunction
      .asInstanceOf[ApproximatePercentile]

    assert(combined.percentageExpression.isInstanceOf[PercentileFusionArray])
    assert(!combined.percentageExpression.contextIndependentFoldable)
  }

  test("avoid redundant entries for duplicate physical aggregates") {
    val duplicateOnly = optimizedAggregate(
      Alias(percentile(value, 0.5), "first")(),
      Alias(percentile(value, 0.5), "second")())
    assertNotCombined(duplicateOnly)

    val p50 = percentile(value, 0.5)
    val mixed = optimizedAggregate(
      Alias(p50, "first")(),
      Alias(p50, "second")(),
      Alias(percentile(value, 0.9), "third")())
    val combined = percentileAggregates(mixed).head.aggregateFunction
      .asInstanceOf[ApproximatePercentile]
    assert(percentageValues(combined) == Seq(0.5, 0.9))
    assert(ordinals(mixed) == Seq(Some(0), Some(0), Some(1)))
  }

  test("respect basic compatibility boundaries") {
    val incompatible = Seq(
      "input" -> (
        percentile(value, 0.5),
        percentile(other, 0.9)),
      "accuracy" -> (
        percentile(value, 0.5, accuracy = 1000),
        percentile(value, 0.9, accuracy = 10000)),
      "filter" -> (
        percentile(value, 0.5),
        percentile(value, 0.9, filter = Some(value > Literal(0)))),
      "distinct" -> (
        percentile(value, 0.5),
        percentile(value, 0.9, isDistinct = true)))

    incompatible.foreach { case (name, (first, second)) =>
      withClue(name) {
        assertNotCombined(optimizedAggregate(
          Alias(first, "first")(),
          Alias(second, "second")()))
      }
    }

    val filter = value > Literal(0)
    val compatible = optimizedAggregate(
      Alias(percentile(value, 0.5, filter = Some(filter)), "filtered_p50")(),
      Alias(percentile(value, 0.9, filter = Some(filter)), "filtered_p90")(),
      Alias(percentile(value, 0.5, isDistinct = true, filter = Some(filter)), "distinct_p50")(),
      Alias(percentile(value, 0.9, isDistinct = true, filter = Some(filter)), "distinct_p90")())
    assert(percentileAggregates(compatible).map(_.resultId).distinct.size == 2)
  }

  test("require structural equality for inputs and filters") {
    val firstInput = value + (other + group)
    val secondInput = (value + other) + group
    val inputResult = optimizedAggregate(
      Alias(percentile(firstInput, 0.5), "first_p50")(),
      Alias(percentile(secondInput, 0.5), "second_p50")(),
      Alias(percentile(secondInput, 0.9), "second_p90")(),
      Alias(percentile(firstInput, 0.9), "first_p90")())

    assert(firstInput != secondInput)
    assert(firstInput.canonicalized == secondInput.canonicalized)
    assertNotCombined(inputResult)

    val disjointInputPercentages = optimizedAggregate(
      Alias(percentile(firstInput, 0.5), "first_p50")(),
      Alias(percentile(firstInput, 0.9), "first_p90")(),
      Alias(percentile(secondInput, 0.25), "second_p25")(),
      Alias(percentile(secondInput, 0.75), "second_p75")())
    assert(percentileAggregates(disjointInputPercentages).map(_.resultId).distinct.size == 2)
    assert(ordinals(disjointInputPercentages) ==
      Seq(Some(0), Some(1), Some(0), Some(1)))

    val crossDistinctInput = optimizedAggregate(
      Alias(percentile(firstInput, 0.5), "first_p50")(),
      Alias(percentile(firstInput, 0.9), "first_p90")(),
      Alias(percentile(secondInput, 0.5, isDistinct = true), "distinct_p50")(),
      Alias(percentile(secondInput, 0.9, isDistinct = true), "distinct_p90")())
    assertNotCombined(crossDistinctInput)

    val firstFilter = firstInput > Literal(0)
    val secondFilter = secondInput > Literal(0)
    val filterResult = optimizedAggregate(
      Alias(percentile(value, 0.5, filter = Some(firstFilter)), "first_p50")(),
      Alias(percentile(value, 0.5, filter = Some(secondFilter)), "second_p50")(),
      Alias(percentile(value, 0.9, filter = Some(secondFilter)), "second_p90")(),
      Alias(percentile(value, 0.9, filter = Some(firstFilter)), "first_p90")())
    assertNotCombined(filterResult)

    val crossDistinctFilter = optimizedAggregate(
      Alias(percentile(value, 0.5, filter = Some(firstFilter)), "first_p50")(),
      Alias(percentile(value, 0.9, filter = Some(firstFilter)), "first_p90")(),
      Alias(percentile(
        value, 0.5, isDistinct = true, filter = Some(secondFilter)), "distinct_p50")(),
      Alias(percentile(
        value, 0.9, isDistinct = true, filter = Some(secondFilter)), "distinct_p90")())
    assertNotCombined(crossDistinctFilter)
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
    assertNotCombined(result)
  }

  test("do not fuse canonically equal percentages that evaluate differently") {
    val firstPercentage =
      (Literal(1.0e16) + Literal(-1.0e16)) + Literal(0.5)
    val secondPercentage =
      Literal(1.0e16) + (Literal(-1.0e16) + Literal(0.5))
    def percentileWithPercentage(percentage: Expression): AggregateExpression = {
      new ApproximatePercentile(value, percentage, Literal(10000)).toAggregateExpression()
    }

    val scalarResult = optimizedAggregate(
      Alias(percentileWithPercentage(firstPercentage), "p50")(),
      Alias(percentileWithPercentage(secondPercentage), "p0")(),
      Alias(percentile(value, 0.9), "p90")())
    assert(firstPercentage.eval() == 0.5d)
    assert(secondPercentage.eval() == 0.0d)
    assert(firstPercentage.canonicalized == secondPercentage.canonicalized)
    assertNotCombined(scalarResult)

    val arrayPercentile = new ApproximatePercentile(
      value,
      CreateArray(Seq(firstPercentage, Literal(0.9))),
      Literal(10000)).toAggregateExpression()
    val arrayResult = optimizedAggregate(
      Alias(arrayPercentile, "percentiles")(),
      Alias(percentileWithPercentage(secondPercentage), "p0")(),
      Alias(percentile(value, 0.9), "p90")())
    val arrayAggregates = percentileAggregates(arrayResult)
    assert(arrayAggregates.map(_.resultId).distinct.size == 2)
    assert(percentageValues(arrayAggregates.head.aggregateFunction
      .asInstanceOf[ApproximatePercentile]) == Seq(0.5, 0.9))
    assert(percentageValues(arrayAggregates(1).aggregateFunction
      .asInstanceOf[ApproximatePercentile]) == Seq(0.0, 0.9))
    assert(ordinals(arrayResult) == Seq(None, Some(0), Some(1)))
  }

  test("preserve existing arrays while combining compatible scalars") {
    val arrayPercentile = new ApproximatePercentile(
      value,
      CreateArray(Seq(Literal(0.25), Literal(0.75))),
      Literal(10000)).toAggregateExpression()
    val result = optimizedAggregate(
      Alias(arrayPercentile, "percentiles")(),
      Alias(percentile(value, 0.5), "p50")(),
      Alias(percentile(value, 0.9), "p90")())

    assert(percentileAggregates(result).map(_.resultId).distinct.size == 2)
    assert(result.aggregateExpressions.drop(1).forall(_.exists(_.isInstanceOf[GetArrayItem])))
  }
}
