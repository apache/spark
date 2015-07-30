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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{StructType, MapType, ArrayType}

/**
 * Utility functions used by the query planner to convert our plan to new aggregation code path.
 */
object Utils {
  def planAggregateWithoutDistinct(
      groupingExpressions: Seq[Expression],
      aggregateExpressions: Seq[AggregateExpression2],
      aggregateFunctionMap: Map[(AggregateFunction2, Boolean), Attribute],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {
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
    val partialAggregateAttributes = partialAggregateExpressions.flatMap { agg =>
      agg.aggregateFunction.bufferAttributes
    }
    val partialAggregate =
      Aggregate2Sort(
        None: Option[Seq[Expression]],
        namedGroupingExpressions.map(_._2),
        partialAggregateExpressions,
        partialAggregateAttributes,
        namedGroupingAttributes ++ partialAggregateAttributes,
        child)

    // 2. Create an Aggregate Operator for final aggregations.
    val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))
    val finalAggregateAttributes =
      finalAggregateExpressions.map {
        expr => aggregateFunctionMap(expr.aggregateFunction, expr.isDistinct)
      }
    val rewrittenResultExpressions = resultExpressions.map { expr =>
      expr.transformDown {
        case agg: AggregateExpression2 =>
          aggregateFunctionMap(agg.aggregateFunction, agg.isDistinct).toAttribute
        case expression =>
          // We do not rely on the equality check at here since attributes may
          // different cosmetically. Instead, we use semanticEquals.
          groupExpressionMap.collectFirst {
            case (expr, ne) if expr semanticEquals expression => ne.toAttribute
          }.getOrElse(expression)
      }.asInstanceOf[NamedExpression]
    }
    val finalAggregate = Aggregate2Sort(
      Some(namedGroupingAttributes),
      namedGroupingAttributes,
      finalAggregateExpressions,
      finalAggregateAttributes,
      rewrittenResultExpressions,
      partialAggregate)

    finalAggregate :: Nil
  }

  def planAggregateWithOneDistinct(
      groupingExpressions: Seq[Expression],
      functionsWithDistinct: Seq[AggregateExpression2],
      functionsWithoutDistinct: Seq[AggregateExpression2],
      aggregateFunctionMap: Map[(AggregateFunction2, Boolean), Attribute],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {

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

    val partialAggregateExpressions = functionsWithoutDistinct.map {
      case AggregateExpression2(aggregateFunction, mode, _) =>
        AggregateExpression2(aggregateFunction, Partial, false)
    }
    val partialAggregateAttributes = partialAggregateExpressions.flatMap { agg =>
      agg.aggregateFunction.bufferAttributes
    }
    val partialAggregate =
      Aggregate2Sort(
        None: Option[Seq[Expression]],
        (namedGroupingExpressions ++ namedDistinctColumnExpressions).map(_._2),
        partialAggregateExpressions,
        partialAggregateAttributes,
        namedGroupingAttributes ++ distinctColumnAttributes ++ partialAggregateAttributes,
        child)

    // 2. Create an Aggregate Operator for partial merge aggregations.
    val partialMergeAggregateExpressions = functionsWithoutDistinct.map {
      case AggregateExpression2(aggregateFunction, mode, _) =>
        AggregateExpression2(aggregateFunction, PartialMerge, false)
    }
    val partialMergeAggregateAttributes =
      partialMergeAggregateExpressions.flatMap { agg =>
        agg.aggregateFunction.bufferAttributes
      }
    val partialMergeAggregate =
      Aggregate2Sort(
        Some(namedGroupingAttributes),
        namedGroupingAttributes ++ distinctColumnAttributes,
        partialMergeAggregateExpressions,
        partialMergeAggregateAttributes,
        namedGroupingAttributes ++ distinctColumnAttributes ++ partialMergeAggregateAttributes,
        partialAggregate)

    // 3. Create an Aggregate Operator for partial merge aggregations.
    val finalAggregateExpressions = functionsWithoutDistinct.map {
      case AggregateExpression2(aggregateFunction, mode, _) =>
        AggregateExpression2(aggregateFunction, Final, false)
    }
    val finalAggregateAttributes =
      finalAggregateExpressions.map {
        expr => aggregateFunctionMap(expr.aggregateFunction, expr.isDistinct)
      }
    val (completeAggregateExpressions, completeAggregateAttributes) = functionsWithDistinct.map {
      // Children of an AggregateFunction with DISTINCT keyword has already
      // been evaluated. At here, we need to replace original children
      // to AttributeReferences.
      case agg @ AggregateExpression2(aggregateFunction, mode, isDistinct) =>
        val rewrittenAggregateFunction = aggregateFunction.transformDown {
          case expr if distinctColumnExpressionMap.contains(expr) =>
            distinctColumnExpressionMap(expr).toAttribute
        }.asInstanceOf[AggregateFunction2]
        // We rewrite the aggregate function to a non-distinct aggregation because
        // its input will have distinct arguments.
        val rewrittenAggregateExpression =
          AggregateExpression2(rewrittenAggregateFunction, Complete, false)

        val aggregateFunctionAttribute = aggregateFunctionMap(agg.aggregateFunction, isDistinct)
        (rewrittenAggregateExpression -> aggregateFunctionAttribute)
    }.unzip

    val rewrittenResultExpressions = resultExpressions.map { expr =>
      expr.transform {
        case agg: AggregateExpression2 =>
          aggregateFunctionMap(agg.aggregateFunction, agg.isDistinct).toAttribute
        case expression =>
          // We do not rely on the equality check at here since attributes may
          // different cosmetically. Instead, we use semanticEquals.
          groupExpressionMap.collectFirst {
            case (expr, ne) if expr semanticEquals expression => ne.toAttribute
          }.getOrElse(expression)
      }.asInstanceOf[NamedExpression]
    }
    val finalAndCompleteAggregate = FinalAndCompleteAggregate2Sort(
      namedGroupingAttributes ++ distinctColumnAttributes,
      namedGroupingAttributes,
      finalAggregateExpressions,
      finalAggregateAttributes,
      completeAggregateExpressions,
      completeAggregateAttributes,
      rewrittenResultExpressions,
      partialMergeAggregate)

    finalAndCompleteAggregate :: Nil
  }
}
