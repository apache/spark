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
import org.apache.spark.sql.execution.{UnsafeFixedWidthAggregationMap, SparkPlan}
import org.apache.spark.sql.types.StructType

/**
 * Utility functions used by the query planner to convert our plan to new aggregation code path.
 */
object Utils {
  def supportsTungstenAggregate(
      groupingExpressions: Seq[Expression],
      aggregateBufferAttributes: Seq[Attribute]): Boolean = {
    val aggregationBufferSchema = StructType.fromAttributes(aggregateBufferAttributes)

    UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema) &&
      UnsafeProjection.canSupport(groupingExpressions)
  }

  def planAggregateWithoutPartial(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression2],
      aggregateFunctionToAttribute: Map[(AggregateFunction2, Boolean), Attribute],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {

    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val completeAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
    val completeAggregateAttributes = completeAggregateExpressions.map {
      expr => aggregateFunctionToAttribute(expr.aggregateFunction, expr.isDistinct)
    }

    SortBasedAggregate(
      requiredChildDistributionExpressions = Some(groupingAttributes),
      groupingExpressions = groupingAttributes,
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
      aggregateExpressions: Seq[AggregateExpression2],
      aggregateFunctionToAttribute: Map[(AggregateFunction2, Boolean), Attribute],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {
    // Check if we can use TungstenAggregate.
    val usesTungstenAggregate =
      child.sqlContext.conf.unsafeEnabled &&
      aggregateExpressions.forall(_.aggregateFunction.isInstanceOf[DeclarativeAggregate]) &&
      supportsTungstenAggregate(
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
      functionsWithDistinct: Seq[AggregateExpression2],
      functionsWithoutDistinct: Seq[AggregateExpression2],
      aggregateFunctionToAttribute: Map[(AggregateFunction2, Boolean), Attribute],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {

    val aggregateExpressions = functionsWithDistinct ++ functionsWithoutDistinct
    val usesTungstenAggregate =
      child.sqlContext.conf.unsafeEnabled &&
        aggregateExpressions.forall(
          _.aggregateFunction.isInstanceOf[DeclarativeAggregate]) &&
        supportsTungstenAggregate(
          groupingExpressions,
          aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))

    // 1. Create an Aggregate Operator for partial aggregations.
    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    // It is safe to call head at here since functionsWithDistinct has at least one
    // AggregateExpression2.
    val distinctColumnExpressions =
      functionsWithDistinct.head.aggregateFunction.children
    val namedDistinctColumnExpressions = distinctColumnExpressions.map {
      case ne: NamedExpression => ne -> ne
      case other =>
        val withAlias = Alias(other, other.toString)()
        other -> withAlias
    }
    val distinctColumnExpressionMap = namedDistinctColumnExpressions.toMap
    val distinctColumnAttributes = namedDistinctColumnExpressions.map(_._2.toAttribute)

    val partialAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Partial))
    val partialAggregateAttributes =
      partialAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    val partialAggregateGroupingExpressions =
      groupingExpressions ++ namedDistinctColumnExpressions.map(_._2)
    val partialAggregateResult =
        groupingAttributes ++
        distinctColumnAttributes ++
        partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)
    val partialAggregate = if (usesTungstenAggregate) {
      TungstenAggregate(
        requiredChildDistributionExpressions = None,
        // The grouping expressions are original groupingExpressions and
        // distinct columns. For example, for avg(distinct value) ... group by key
        // the grouping expressions of this Aggregate Operator will be [key, value].
        groupingExpressions = partialAggregateGroupingExpressions,
        nonCompleteAggregateExpressions = partialAggregateExpressions,
        nonCompleteAggregateAttributes = partialAggregateAttributes,
        completeAggregateExpressions = Nil,
        completeAggregateAttributes = Nil,
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

    // 2. Create an Aggregate Operator for partial merge aggregations.
    val partialMergeAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
    val partialMergeAggregateAttributes =
      partialMergeAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    val partialMergeAggregateResult =
        groupingAttributes ++
        distinctColumnAttributes ++
        partialMergeAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)
    val partialMergeAggregate = if (usesTungstenAggregate) {
      TungstenAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes ++ distinctColumnAttributes,
        nonCompleteAggregateExpressions = partialMergeAggregateExpressions,
        nonCompleteAggregateAttributes = partialMergeAggregateAttributes,
        completeAggregateExpressions = Nil,
        completeAggregateAttributes = Nil,
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

    // 3. Create an Aggregate Operator for partial merge aggregations.
    val finalAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Final))
    // The attributes of the final aggregation buffer, which is presented as input to the result
    // projection:
    val finalAggregateAttributes = finalAggregateExpressions.map {
      expr => aggregateFunctionToAttribute(expr.aggregateFunction, expr.isDistinct)
    }

    val (completeAggregateExpressions, completeAggregateAttributes) = functionsWithDistinct.map {
      // Children of an AggregateFunction with DISTINCT keyword has already
      // been evaluated. At here, we need to replace original children
      // to AttributeReferences.
      case agg @ AggregateExpression2(aggregateFunction, mode, true) =>
        val rewrittenAggregateFunction = aggregateFunction.transformDown {
          case expr if distinctColumnExpressionMap.contains(expr) =>
            distinctColumnExpressionMap(expr).toAttribute
        }.asInstanceOf[AggregateFunction2]
        // We rewrite the aggregate function to a non-distinct aggregation because
        // its input will have distinct arguments.
        // We just keep the isDistinct setting to true, so when users look at the query plan,
        // they still can see distinct aggregations.
        val rewrittenAggregateExpression =
          AggregateExpression2(rewrittenAggregateFunction, Complete, true)

        val aggregateFunctionAttribute = aggregateFunctionToAttribute(agg.aggregateFunction, true)
        (rewrittenAggregateExpression, aggregateFunctionAttribute)
    }.unzip

    val finalAndCompleteAggregate = if (usesTungstenAggregate) {
      TungstenAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        nonCompleteAggregateExpressions = finalAggregateExpressions,
        nonCompleteAggregateAttributes = finalAggregateAttributes,
        completeAggregateExpressions = completeAggregateExpressions,
        completeAggregateAttributes = completeAggregateAttributes,
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

    finalAndCompleteAggregate :: Nil
  }
}
