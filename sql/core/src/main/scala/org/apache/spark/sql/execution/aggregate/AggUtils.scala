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
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.{StateStoreRestoreExec, StateStoreSaveExec}

/**
 * A pattern that finds aggregate operators to support partial aggregations.
 */
object PartialAggregate {

  def unapply(plan: SparkPlan): Option[Distribution] = plan match {
    case agg: AggregateExec if AggUtils.supportPartialAggregate(agg.aggregateExpressions) =>
      Some(agg.requiredChildDistribution.head)
    case _ =>
      None
  }
}

/**
 * Utility functions used by the query planner to convert our plan to new aggregation code path.
 */
object AggUtils {

  def supportPartialAggregate(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    aggregateExpressions.map(_.aggregateFunction).forall(_.supportsPartial)
  }

  private def createPartialAggregateExec(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      child: SparkPlan): SparkPlan = {
    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val functionsWithDistinct = aggregateExpressions.filter(_.isDistinct)
    val partialAggregateExpressions = aggregateExpressions.map {
      case agg @ AggregateExpression(_, _, false, _) if functionsWithDistinct.length > 0 =>
        agg.copy(mode = PartialMerge)
      case agg =>
        agg.copy(mode = Partial)
    }
    val partialAggregateAttributes =
      partialAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    val partialResultExpressions =
      groupingAttributes ++
        partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

    createAggregateExec(
      requiredChildDistributionExpressions = None,
      groupingExpressions = groupingExpressions,
      aggregateExpressions = partialAggregateExpressions,
      aggregateAttributes = partialAggregateAttributes,
      initialInputBufferOffset = if (functionsWithDistinct.length > 0) {
        groupingExpressions.length + functionsWithDistinct.head.aggregateFunction.children.length
      } else {
        0
      },
      resultExpressions = partialResultExpressions,
      child = child)
  }

  private def updateMergeAggregateMode(aggregateExpressions: Seq[AggregateExpression]) = {
    def updateMode(mode: AggregateMode) = mode match {
      case Partial => PartialMerge
      case Complete => Final
      case mode => mode
    }
    aggregateExpressions.map(e => e.copy(mode = updateMode(e.mode)))
  }

  /**
   * Builds new merge and map-side [[AggregateExec]]s from an input aggregate operator.
   * If an aggregation needs a shuffle for satisfying its own distribution and supports partial
   * aggregations, a map-side aggregation is appended before the shuffle in
   * [[org.apache.spark.sql.execution.exchange.EnsureRequirements]].
   */
  def createMapMergeAggregatePair(operator: SparkPlan): (SparkPlan, SparkPlan) = operator match {
    case agg: AggregateExec =>
      val mapSideAgg = createPartialAggregateExec(
        agg.groupingExpressions, agg.aggregateExpressions, agg.child)
      val mergeAgg = createAggregateExec(
        requiredChildDistributionExpressions = agg.requiredChildDistributionExpressions,
        groupingExpressions = agg.groupingExpressions.map(_.toAttribute),
        aggregateExpressions = updateMergeAggregateMode(agg.aggregateExpressions),
        aggregateAttributes = agg.aggregateAttributes,
        initialInputBufferOffset = agg.groupingExpressions.length,
        resultExpressions = agg.resultExpressions,
        child = mapSideAgg
      )

      (mergeAgg, mapSideAgg)
  }

  private def createAggregateExec(
      requiredChildDistributionExpressions: Option[Seq[Expression]] = None,
      groupingExpressions: Seq[NamedExpression] = Nil,
      aggregateExpressions: Seq[AggregateExpression] = Nil,
      aggregateAttributes: Seq[Attribute] = Nil,
      initialInputBufferOffset: Int = 0,
      resultExpressions: Seq[NamedExpression] = Nil,
      child: SparkPlan): SparkPlan = {
    val useHash = HashAggregateExec.supportsAggregate(
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)) &&
      supportPartialAggregate(aggregateExpressions)
    if (useHash) {
      HashAggregateExec(
        requiredChildDistributionExpressions = requiredChildDistributionExpressions,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = initialInputBufferOffset,
        resultExpressions = resultExpressions,
        child = child)
    } else {
      SortAggregateExec(
        requiredChildDistributionExpressions = requiredChildDistributionExpressions,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = initialInputBufferOffset,
        resultExpressions = resultExpressions,
        child = child)
    }
  }

  def planAggregateWithoutDistinct(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {
    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val completeAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
    val completeAggregateAttributes = completeAggregateExpressions.map(_.resultAttribute)
    val supportPartial = supportPartialAggregate(aggregateExpressions)

    createAggregateExec(
      requiredChildDistributionExpressions =
        Some(if (supportPartial) groupingAttributes else groupingExpressions),
      groupingExpressions = groupingExpressions,
      aggregateExpressions = completeAggregateExpressions,
      aggregateAttributes = completeAggregateAttributes,
      initialInputBufferOffset = 0,
      resultExpressions = resultExpressions,
      child = child
    ) :: Nil
  }

  def planAggregateWithOneDistinct(
      groupingExpressions: Seq[NamedExpression],
      functionsWithDistinct: Seq[AggregateExpression],
      functionsWithoutDistinct: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {

    // functionsWithDistinct is guaranteed to be non-empty. Even though it may contain more than one
    // DISTINCT aggregate function, all of those functions will have the same column expressions.
    // For example, it would be valid for functionsWithDistinct to be
    // [COUNT(DISTINCT foo), MAX(DISTINCT foo)], but [COUNT(DISTINCT bar), COUNT(DISTINCT foo)] is
    // disallowed because those two distinct aggregates have different column expressions.
    val distinctExpressions = functionsWithDistinct.head.aggregateFunction.children
    val namedDistinctExpressions = distinctExpressions.map {
      case ne: NamedExpression => ne
      case other => Alias(other, other.toString)()
    }
    val distinctAttributes = namedDistinctExpressions.map(_.toAttribute)
    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    // 1. Create an Aggregate Operator for non-distinct aggregations.
    val partialAggregate: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Partial))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      createAggregateExec(
        requiredChildDistributionExpressions =
          Some(groupingAttributes ++ distinctAttributes),
        groupingExpressions = groupingExpressions ++ namedDistinctExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = (groupingAttributes ++ distinctAttributes).length,
        resultExpressions = groupingAttributes ++ distinctAttributes ++
          aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = child)
    }

    // 2. Create an Aggregate Operator for the final aggregation.
    val distinctColumnAttributeLookup = distinctExpressions.zip(distinctAttributes).toMap
    val rewrittenDistinctFunctions = functionsWithDistinct.map {
      // Children of an AggregateFunction with DISTINCT keyword has already
      // been evaluated. At here, we need to replace original children
      // to AttributeReferences.
      case agg @ AggregateExpression(aggregateFunction, mode, true, _) =>
        aggregateFunction.transformDown(distinctColumnAttributeLookup)
          .asInstanceOf[AggregateFunction]
    }
    val finalAndCompleteAggregate: SparkPlan = {
      val finalAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Final))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

      val (distinctAggregateExpressions, distinctAggregateAttributes) =
        rewrittenDistinctFunctions.zipWithIndex.map { case (func, i) =>
          // We rewrite the aggregate function to a non-distinct aggregation because
          // its input will have distinct arguments.
          // We keep the isDistinct setting to true because this flag is used to generate partial
          // aggregations and it is easy to see aggregation types in the query plan.
          val expr = AggregateExpression(func, Complete, isDistinct = true)
          // Use original AggregationFunction to lookup attributes, which is used to build
          // aggregateFunctionToAttribute
          val attr = functionsWithDistinct(i).resultAttribute
          (expr, attr)
        }.unzip

      createAggregateExec(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = finalAggregateExpressions ++ distinctAggregateExpressions,
        aggregateAttributes = finalAggregateAttributes ++ distinctAggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = resultExpressions,
        child = partialAggregate)
    }

    finalAndCompleteAggregate :: Nil
  }

  /**
   * Plans a streaming aggregation using the following progression:
   *  - Partial Aggregation (now there is at most 1 tuple per group)
   *  - StateStoreRestore (now there is 1 tuple from this batch + optionally one from the previous)
   *  - PartialMerge (now there is at most 1 tuple per group)
   *  - StateStoreSave (saves the tuple for the next batch)
   *  - Complete (output the current result of the aggregation)
   *
   *  If the first aggregation needs a shuffle to satisfy its distribution, a map-side partial
   *  an aggregation and a shuffle are added in `EnsureRequirements`.
   */
  def planStreamingAggregation(
      groupingExpressions: Seq[NamedExpression],
      functionsWithoutDistinct: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {

    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    val partialAggregate: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Partial))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      createAggregateExec(
        requiredChildDistributionExpressions =
            Some(groupingAttributes),
        groupingExpressions = groupingExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = groupingAttributes ++
            aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = child)
    }

    val restored = StateStoreRestoreExec(groupingAttributes, None, partialAggregate)

    val partialMerged: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      createAggregateExec(
        requiredChildDistributionExpressions =
            Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = groupingAttributes ++
            aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = restored)
    }
    // Note: stateId and returnAllStates are filled in later with preparation rules
    // in IncrementalExecution.
    val saved = StateStoreSaveExec(
      groupingAttributes, stateId = None, returnAllStates = None, partialMerged)

    val finalAndCompleteAggregate: SparkPlan = {
      val finalAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Final))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

      createAggregateExec(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = finalAggregateExpressions,
        aggregateAttributes = finalAggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = resultExpressions,
        child = saved)
    }

    finalAndCompleteAggregate :: Nil
  }
}
