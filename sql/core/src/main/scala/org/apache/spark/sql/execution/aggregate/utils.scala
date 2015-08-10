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

import scala.collection.mutable

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

  def planAggregateWithoutDistinct(
      groupingExpressions: Seq[Expression],
      aggregateExpressions: Seq[AggregateExpression2],
      aggregateFunctionMap: Map[(AggregateFunction2, Boolean), (AggregateFunction2, Attribute)],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {
    // Check if we can use TungstenAggregate.
    val usesTungstenAggregate =
      child.sqlContext.conf.unsafeEnabled &&
      aggregateExpressions.forall(_.aggregateFunction.isInstanceOf[AlgebraicAggregate]) &&
      supportsTungstenAggregate(
        groupingExpressions,
        aggregateExpressions.flatMap(_.aggregateFunction.bufferAttributes))


    // 1. Create an Aggregate Operator for partial aggregations.
    val namedGroupingExpressions = groupingExpressions.map {
      case ne: NamedExpression => ne -> ne
      // If the expression is not a NamedExpressions, we add an alias.
      // So, when we generate the result of the operator, the Aggregate Operator
      // can directly get the Seq of attributes representing the grouping expressions.
      case other =>
        val withAlias = Alias(other, other.toString)()
        other -> withAlias
    }
    val groupExpressionMap = namedGroupingExpressions.toMap
    val namedGroupingAttributes = namedGroupingExpressions.map(_._2.toAttribute)
    val partialAggregateExpressions = aggregateExpressions.map(_.copy(mode = Partial))
    val partialAggregateAttributes =
      partialAggregateExpressions.flatMap(_.aggregateFunction.bufferAttributes)
    val partialResultExpressions =
      namedGroupingAttributes ++
        partialAggregateExpressions.flatMap(_.aggregateFunction.cloneBufferAttributes)

    val partialAggregate = if (usesTungstenAggregate) {
      TungstenAggregate(
        requiredChildDistributionExpressions = None: Option[Seq[Expression]],
        groupingExpressions = namedGroupingExpressions.map(_._2),
        nonCompleteAggregateExpressions = partialAggregateExpressions,
        completeAggregateExpressions = Nil,
        initialInputBufferOffset = 0,
        resultExpressions = partialResultExpressions,
        child = child)
    } else {
      SortBasedAggregate(
        requiredChildDistributionExpressions = None: Option[Seq[Expression]],
        groupingExpressions = namedGroupingExpressions.map(_._2),
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
    val finalAggregateAttributes =
      finalAggregateExpressions.map {
        expr => aggregateFunctionMap(expr.aggregateFunction, expr.isDistinct)._2
      }

    val finalAggregate = if (usesTungstenAggregate) {
      val rewrittenResultExpressions = resultExpressions.map { expr =>
        expr.transformDown {
          case agg: AggregateExpression2 =>
            // aggregateFunctionMap contains unique aggregate functions.
            val aggregateFunction =
              aggregateFunctionMap(agg.aggregateFunction, agg.isDistinct)._1
            aggregateFunction.asInstanceOf[AlgebraicAggregate].evaluateExpression
          case expression =>
            // We do not rely on the equality check at here since attributes may
            // different cosmetically. Instead, we use semanticEquals.
            groupExpressionMap.collectFirst {
              case (expr, ne) if expr semanticEquals expression => ne.toAttribute
            }.getOrElse(expression)
        }.asInstanceOf[NamedExpression]
      }

      TungstenAggregate(
        requiredChildDistributionExpressions = Some(namedGroupingAttributes),
        groupingExpressions = namedGroupingAttributes,
        nonCompleteAggregateExpressions = finalAggregateExpressions,
        completeAggregateExpressions = Nil,
        initialInputBufferOffset = namedGroupingAttributes.length,
        resultExpressions = rewrittenResultExpressions,
        child = partialAggregate)
    } else {
      val rewrittenResultExpressions = resultExpressions.map { expr =>
        expr.transformDown {
          case agg: AggregateExpression2 =>
            aggregateFunctionMap(agg.aggregateFunction, agg.isDistinct)._2
          case expression =>
            // We do not rely on the equality check at here since attributes may
            // different cosmetically. Instead, we use semanticEquals.
            groupExpressionMap.collectFirst {
              case (expr, ne) if expr semanticEquals expression => ne.toAttribute
            }.getOrElse(expression)
        }.asInstanceOf[NamedExpression]
      }

      SortBasedAggregate(
        requiredChildDistributionExpressions = Some(namedGroupingAttributes),
        groupingExpressions = namedGroupingAttributes,
        nonCompleteAggregateExpressions = finalAggregateExpressions,
        nonCompleteAggregateAttributes = finalAggregateAttributes,
        completeAggregateExpressions = Nil,
        completeAggregateAttributes = Nil,
        initialInputBufferOffset = namedGroupingAttributes.length,
        resultExpressions = rewrittenResultExpressions,
        child = partialAggregate)
    }

    finalAggregate :: Nil
  }

  def planAggregateWithOneDistinct(
      groupingExpressions: Seq[Expression],
      functionsWithDistinct: Seq[AggregateExpression2],
      functionsWithoutDistinct: Seq[AggregateExpression2],
      aggregateFunctionMap: Map[(AggregateFunction2, Boolean), (AggregateFunction2, Attribute)],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {

    val aggregateExpressions = functionsWithDistinct ++ functionsWithoutDistinct
    val usesTungstenAggregate =
      child.sqlContext.conf.unsafeEnabled &&
        aggregateExpressions.forall(_.aggregateFunction.isInstanceOf[AlgebraicAggregate]) &&
        supportsTungstenAggregate(
          groupingExpressions,
          aggregateExpressions.flatMap(_.aggregateFunction.bufferAttributes))

    // 1. Create an Aggregate Operator for partial aggregations.
    // The grouping expressions are original groupingExpressions and
    // distinct columns. For example, for avg(distinct value) ... group by key
    // the grouping expressions of this Aggregate Operator will be [key, value].
    val namedGroupingExpressions = groupingExpressions.map {
      case ne: NamedExpression => ne -> ne
      // If the expression is not a NamedExpressions, we add an alias.
      // So, when we generate the result of the operator, the Aggregate Operator
      // can directly get the Seq of attributes representing the grouping expressions.
      case other =>
        val withAlias = Alias(other, other.toString)()
        other -> withAlias
    }
    val groupExpressionMap = namedGroupingExpressions.toMap
    val namedGroupingAttributes = namedGroupingExpressions.map(_._2.toAttribute)

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
      partialAggregateExpressions.flatMap(_.aggregateFunction.bufferAttributes)
    val partialAggregateGroupingExpressions =
      (namedGroupingExpressions ++ namedDistinctColumnExpressions).map(_._2)
    val partialAggregateResult =
      namedGroupingAttributes ++
        distinctColumnAttributes ++
        partialAggregateExpressions.flatMap(_.aggregateFunction.cloneBufferAttributes)
    val partialAggregate = if (usesTungstenAggregate) {
      TungstenAggregate(
        requiredChildDistributionExpressions = None: Option[Seq[Expression]],
        groupingExpressions = partialAggregateGroupingExpressions,
        nonCompleteAggregateExpressions = partialAggregateExpressions,
        completeAggregateExpressions = Nil,
        initialInputBufferOffset = 0,
        resultExpressions = partialAggregateResult,
        child = child)
    } else {
      SortBasedAggregate(
        requiredChildDistributionExpressions = None: Option[Seq[Expression]],
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
      partialMergeAggregateExpressions.flatMap(_.aggregateFunction.bufferAttributes)
    val partialMergeAggregateResult =
      namedGroupingAttributes ++
        distinctColumnAttributes ++
        partialMergeAggregateExpressions.flatMap(_.aggregateFunction.cloneBufferAttributes)
    val partialMergeAggregate = if (usesTungstenAggregate) {
      TungstenAggregate(
        requiredChildDistributionExpressions = Some(namedGroupingAttributes),
        groupingExpressions = namedGroupingAttributes ++ distinctColumnAttributes,
        nonCompleteAggregateExpressions = partialMergeAggregateExpressions,
        completeAggregateExpressions = Nil,
        initialInputBufferOffset = (namedGroupingAttributes ++ distinctColumnAttributes).length,
        resultExpressions = partialMergeAggregateResult,
        child = partialAggregate)
    } else {
      SortBasedAggregate(
        requiredChildDistributionExpressions = Some(namedGroupingAttributes),
        groupingExpressions = namedGroupingAttributes ++ distinctColumnAttributes,
        nonCompleteAggregateExpressions = partialMergeAggregateExpressions,
        nonCompleteAggregateAttributes = partialMergeAggregateAttributes,
        completeAggregateExpressions = Nil,
        completeAggregateAttributes = Nil,
        initialInputBufferOffset = (namedGroupingAttributes ++ distinctColumnAttributes).length,
        resultExpressions = partialMergeAggregateResult,
        child = partialAggregate)
    }

    // 3. Create an Aggregate Operator for partial merge aggregations.
    val finalAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Final))
    val finalAggregateAttributes =
      finalAggregateExpressions.map {
        expr => aggregateFunctionMap(expr.aggregateFunction, expr.isDistinct)._2
      }
    // Create a map to store those rewritten aggregate functions. We always need to use
    // both function and its corresponding isDistinct flag as the key because function itself
    // does not knows if it is has distinct keyword or now.
    val rewrittenAggregateFunctions =
      mutable.Map.empty[(AggregateFunction2, Boolean), AggregateFunction2]
    val (completeAggregateExpressions, completeAggregateAttributes) = functionsWithDistinct.map {
      // Children of an AggregateFunction with DISTINCT keyword has already
      // been evaluated. At here, we need to replace original children
      // to AttributeReferences.
      case agg @ AggregateExpression2(aggregateFunction, mode, true) =>
        val rewrittenAggregateFunction = aggregateFunction.transformDown {
          case expr if distinctColumnExpressionMap.contains(expr) =>
            distinctColumnExpressionMap(expr).toAttribute
        }.asInstanceOf[AggregateFunction2]
        // Because we have rewritten the aggregate function, we use rewrittenAggregateFunctions
        // to track the old version and the new version of this function.
        rewrittenAggregateFunctions += (aggregateFunction, true) -> rewrittenAggregateFunction
        // We rewrite the aggregate function to a non-distinct aggregation because
        // its input will have distinct arguments.
        // We just keep the isDistinct setting to true, so when users look at the query plan,
        // they still can see distinct aggregations.
        val rewrittenAggregateExpression =
          AggregateExpression2(rewrittenAggregateFunction, Complete, true)

        val aggregateFunctionAttribute =
          aggregateFunctionMap(agg.aggregateFunction, true)._2
        (rewrittenAggregateExpression -> aggregateFunctionAttribute)
    }.unzip

    val finalAndCompleteAggregate = if (usesTungstenAggregate) {
      val rewrittenResultExpressions = resultExpressions.map { expr =>
        expr.transform {
          case agg: AggregateExpression2 =>
            val function = agg.aggregateFunction
            val isDistinct = agg.isDistinct
            val aggregateFunction =
              if (rewrittenAggregateFunctions.contains(function, isDistinct)) {
                // If this function has been rewritten, we get the rewritten version from
                // rewrittenAggregateFunctions.
                rewrittenAggregateFunctions(function, isDistinct)
              } else {
                // Oterwise, we get it from aggregateFunctionMap, which contains unique
                // aggregate functions that have not been rewritten.
                aggregateFunctionMap(function, isDistinct)._1
              }
            aggregateFunction.asInstanceOf[AlgebraicAggregate].evaluateExpression
          case expression =>
            // We do not rely on the equality check at here since attributes may
            // different cosmetically. Instead, we use semanticEquals.
            groupExpressionMap.collectFirst {
              case (expr, ne) if expr semanticEquals expression => ne.toAttribute
            }.getOrElse(expression)
        }.asInstanceOf[NamedExpression]
      }

      TungstenAggregate(
        requiredChildDistributionExpressions = Some(namedGroupingAttributes),
        groupingExpressions = namedGroupingAttributes,
        nonCompleteAggregateExpressions = finalAggregateExpressions,
        completeAggregateExpressions = completeAggregateExpressions,
        initialInputBufferOffset = (namedGroupingAttributes ++ distinctColumnAttributes).length,
        resultExpressions = rewrittenResultExpressions,
        child = partialMergeAggregate)
    } else {
      val rewrittenResultExpressions = resultExpressions.map { expr =>
        expr.transform {
          case agg: AggregateExpression2 =>
            aggregateFunctionMap(agg.aggregateFunction, agg.isDistinct)._2
          case expression =>
            // We do not rely on the equality check at here since attributes may
            // different cosmetically. Instead, we use semanticEquals.
            groupExpressionMap.collectFirst {
              case (expr, ne) if expr semanticEquals expression => ne.toAttribute
            }.getOrElse(expression)
        }.asInstanceOf[NamedExpression]
      }
      SortBasedAggregate(
        requiredChildDistributionExpressions = Some(namedGroupingAttributes),
        groupingExpressions = namedGroupingAttributes,
        nonCompleteAggregateExpressions = finalAggregateExpressions,
        nonCompleteAggregateAttributes = finalAggregateAttributes,
        completeAggregateExpressions = completeAggregateExpressions,
        completeAggregateAttributes = completeAggregateAttributes,
        initialInputBufferOffset = (namedGroupingAttributes ++ distinctColumnAttributes).length,
        resultExpressions = rewrittenResultExpressions,
        child = partialMergeAggregate)
    }

    finalAndCompleteAggregate :: Nil
  }
}
