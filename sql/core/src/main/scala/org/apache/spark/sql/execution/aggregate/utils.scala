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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.SparkPlan

/**
 * Utility functions used by the query planner to convert our plan to new aggregation code path.
 */
object Utils {

  def planAggregateWithoutPartial(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateFunctionToAttribute: Map[(AggregateFunction, Boolean), Attribute],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {

    val completeAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
    val completeAggregateAttributes = completeAggregateExpressions.map {
      expr => aggregateFunctionToAttribute(expr.aggregateFunction, expr.isDistinct)
    }

    SortBasedAggregate(
      requiredChildDistributionExpressions = Some(groupingExpressions),
      groupingExpressions = groupingExpressions,
      nonCompleteAggregateExpressions = Nil,
      nonCompleteAggregateAttributes = Nil,
      completeAggregateExpressions = completeAggregateExpressions,
      completeAggregateAttributes = completeAggregateAttributes,
      initialInputBufferOffset = 0,
      resultExpressions = resultExpressions,
      child = child
    ) :: Nil
  }

  def planAggregateWithoutDistinct(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateFunctionToAttribute: Map[(AggregateFunction, Boolean), Attribute],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {
    // Check if we can use TungstenAggregate.
    val usesTungstenAggregate = TungstenAggregate.supportsAggregate(
        groupingExpressions,
        aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))

    // 1. Create an Aggregate Operator for partial aggregations.

    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val partialAggregateExpressions = aggregateExpressions.map(_.copy(mode = Partial))
    val partialAggregateAttributes =
      partialAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    val partialResultExpressions =
      groupingAttributes ++
        partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

    val partialAggregate = if (usesTungstenAggregate) {
      TungstenAggregate(
        requiredChildDistributionExpressions = None: Option[Seq[Expression]],
        groupingExpressions = groupingExpressions,
        nonCompleteAggregateExpressions = partialAggregateExpressions,
        nonCompleteAggregateAttributes = partialAggregateAttributes,
        completeAggregateExpressions = Nil,
        completeAggregateAttributes = Nil,
        initialInputBufferOffset = 0,
        resultExpressions = partialResultExpressions,
        child = child)
    } else {
      SortBasedAggregate(
        requiredChildDistributionExpressions = None: Option[Seq[Expression]],
        groupingExpressions = groupingExpressions,
        nonCompleteAggregateExpressions = partialAggregateExpressions,
        nonCompleteAggregateAttributes = partialAggregateAttributes,
        completeAggregateExpressions = Nil,
        completeAggregateAttributes = Nil,
        initialInputBufferOffset = 0,
        resultExpressions = partialResultExpressions,
        child = child)
    }

    // 2. Create an Aggregate Operator for final aggregations.
    val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))
    // The attributes of the final aggregation buffer, which is presented as input to the result
    // projection:
    val finalAggregateAttributes = finalAggregateExpressions.map {
      expr => aggregateFunctionToAttribute(expr.aggregateFunction, expr.isDistinct)
    }

    val finalAggregate = if (usesTungstenAggregate) {
      TungstenAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        nonCompleteAggregateExpressions = finalAggregateExpressions,
        nonCompleteAggregateAttributes = finalAggregateAttributes,
        completeAggregateExpressions = Nil,
        completeAggregateAttributes = Nil,
        initialInputBufferOffset = groupingExpressions.length,
        resultExpressions = resultExpressions,
        child = partialAggregate)
    } else {
      SortBasedAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        nonCompleteAggregateExpressions = finalAggregateExpressions,
        nonCompleteAggregateAttributes = finalAggregateAttributes,
        completeAggregateExpressions = Nil,
        completeAggregateAttributes = Nil,
        initialInputBufferOffset = groupingExpressions.length,
        resultExpressions = resultExpressions,
        child = partialAggregate)
    }

    finalAggregate :: Nil
  }

  def planAggregateWithOneDistinct(
      groupingExpressions: Seq[NamedExpression],
      functionsWithDistinct: Seq[AggregateExpression],
      functionsWithoutDistinct: Seq[AggregateExpression],
      aggregateFunctionToAttribute: Map[(AggregateFunction, Boolean), Attribute],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {

    val aggregateExpressions = functionsWithDistinct ++ functionsWithoutDistinct
    val usesTungstenAggregate = TungstenAggregate.supportsAggregate(
      groupingExpressions,
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))

    // functionsWithDistinct is guaranteed to be non-empty. Even though it may contain more than one
    // DISTINCT aggregate function, all of those functions will have the same column expressions.
    // For example, it would be valid for functionsWithDistinct to be
    // [COUNT(DISTINCT foo), MAX(DISTINCT foo)], but [COUNT(DISTINCT bar), COUNT(DISTINCT foo)] is
    // disallowed because those two distinct aggregates have different column expressions.
    val distinctColumnExpressions = functionsWithDistinct.head.aggregateFunction.children
    val namedDistinctColumnExpressions = distinctColumnExpressions.map {
      case ne: NamedExpression => ne
      case other => Alias(other, other.toString)()
    }
    val distinctColumnAttributes = namedDistinctColumnExpressions.map(_.toAttribute)
    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    // 1. Create an Aggregate Operator for partial aggregations.
    val partialAggregate: SparkPlan = {
      val partialAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Partial))
      val partialAggregateAttributes =
        partialAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
      // We will group by the original grouping expression, plus an additional expression for the
      // DISTINCT column. For example, for AVG(DISTINCT value) GROUP BY key, the grouping
      // expressions will be [key, value].
      val partialAggregateGroupingExpressions =
        groupingExpressions ++ namedDistinctColumnExpressions
      val partialAggregateResult =
        groupingAttributes ++
          distinctColumnAttributes ++
          partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)
      if (usesTungstenAggregate) {
        TungstenAggregate(
          requiredChildDistributionExpressions = None,
          groupingExpressions = partialAggregateGroupingExpressions,
          nonCompleteAggregateExpressions = partialAggregateExpressions,
          nonCompleteAggregateAttributes = partialAggregateAttributes,
          completeAggregateExpressions = Nil,
          completeAggregateAttributes = Nil,
          initialInputBufferOffset = 0,
          resultExpressions = partialAggregateResult,
          child = child)
      } else {
        SortBasedAggregate(
          requiredChildDistributionExpressions = None,
          groupingExpressions = partialAggregateGroupingExpressions,
          nonCompleteAggregateExpressions = partialAggregateExpressions,
          nonCompleteAggregateAttributes = partialAggregateAttributes,
          completeAggregateExpressions = Nil,
          completeAggregateAttributes = Nil,
          initialInputBufferOffset = 0,
          resultExpressions = partialAggregateResult,
          child = child)
      }
    }

    // 2. Create an Aggregate Operator for partial merge aggregations.
    val partialMergeAggregate: SparkPlan = {
      val partialMergeAggregateExpressions =
        functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
      val partialMergeAggregateAttributes =
        partialMergeAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
      val partialMergeAggregateResult =
        groupingAttributes ++
          distinctColumnAttributes ++
          partialMergeAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)
      if (usesTungstenAggregate) {
        TungstenAggregate(
          requiredChildDistributionExpressions = Some(groupingAttributes),
          groupingExpressions = groupingAttributes ++ distinctColumnAttributes,
          nonCompleteAggregateExpressions = partialMergeAggregateExpressions,
          nonCompleteAggregateAttributes = partialMergeAggregateAttributes,
          completeAggregateExpressions = Nil,
          completeAggregateAttributes = Nil,
          initialInputBufferOffset = (groupingAttributes ++ distinctColumnAttributes).length,
          resultExpressions = partialMergeAggregateResult,
          child = partialAggregate)
      } else {
        SortBasedAggregate(
          requiredChildDistributionExpressions = Some(groupingAttributes),
          groupingExpressions = groupingAttributes ++ distinctColumnAttributes,
          nonCompleteAggregateExpressions = partialMergeAggregateExpressions,
          nonCompleteAggregateAttributes = partialMergeAggregateAttributes,
          completeAggregateExpressions = Nil,
          completeAggregateAttributes = Nil,
          initialInputBufferOffset = (groupingAttributes ++ distinctColumnAttributes).length,
          resultExpressions = partialMergeAggregateResult,
          child = partialAggregate)
      }
    }

    // 3. Create an Aggregate Operator for the final aggregation.
    val finalAndCompleteAggregate: SparkPlan = {
      val finalAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Final))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val finalAggregateAttributes = finalAggregateExpressions.map {
        expr => aggregateFunctionToAttribute(expr.aggregateFunction, expr.isDistinct)
      }

      val distinctColumnAttributeLookup =
        distinctColumnExpressions.zip(distinctColumnAttributes).toMap
      val (completeAggregateExpressions, completeAggregateAttributes) = functionsWithDistinct.map {
        // Children of an AggregateFunction with DISTINCT keyword has already
        // been evaluated. At here, we need to replace original children
        // to AttributeReferences.
        case agg @ AggregateExpression(aggregateFunction, mode, true) =>
          val rewrittenAggregateFunction = aggregateFunction
            .transformDown(distinctColumnAttributeLookup)
            .asInstanceOf[AggregateFunction]
          // We rewrite the aggregate function to a non-distinct aggregation because
          // its input will have distinct arguments.
          // We just keep the isDistinct setting to true, so when users look at the query plan,
          // they still can see distinct aggregations.
          val rewrittenAggregateExpression =
            AggregateExpression(rewrittenAggregateFunction, Complete, isDistinct = true)

          val aggregateFunctionAttribute = aggregateFunctionToAttribute(agg.aggregateFunction, true)
          (rewrittenAggregateExpression, aggregateFunctionAttribute)
      }.unzip
      if (usesTungstenAggregate) {
        TungstenAggregate(
          requiredChildDistributionExpressions = Some(groupingAttributes),
          groupingExpressions = groupingAttributes,
          nonCompleteAggregateExpressions = finalAggregateExpressions,
          nonCompleteAggregateAttributes = finalAggregateAttributes,
          completeAggregateExpressions = completeAggregateExpressions,
          completeAggregateAttributes = completeAggregateAttributes,
          initialInputBufferOffset = (groupingAttributes ++ distinctColumnAttributes).length,
          resultExpressions = resultExpressions,
          child = partialMergeAggregate)
      } else {
        SortBasedAggregate(
          requiredChildDistributionExpressions = Some(groupingAttributes),
          groupingExpressions = groupingAttributes,
          nonCompleteAggregateExpressions = finalAggregateExpressions,
          nonCompleteAggregateAttributes = finalAggregateAttributes,
          completeAggregateExpressions = completeAggregateExpressions,
          completeAggregateAttributes = completeAggregateAttributes,
          initialInputBufferOffset = (groupingAttributes ++ distinctColumnAttributes).length,
          resultExpressions = resultExpressions,
          child = partialMergeAggregate)
      }
    }

    finalAndCompleteAggregate :: Nil
  }
}
