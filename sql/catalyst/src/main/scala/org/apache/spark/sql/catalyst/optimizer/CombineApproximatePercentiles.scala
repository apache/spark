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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, ExprId, GetArrayItem, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateMode, ApproximatePercentile}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.types.DoubleType

/**
 * Combines scalar approximate percentiles that can share the same percentile digest.
 *
 * An approximate percentile digest depends on its input, accuracy, filter, distinctness, and
 * aggregate mode, but not on the percentile requested from the completed digest. Consequently,
 * compatible scalar percentiles can be calculated by one array-valued aggregate and projected
 * back to their original scalar outputs.
 *
 * Inputs and filters must retain their original expression structure so that floating-point
 * evaluation and ANSI overflow behavior are preserved. Streaming aggregates are left unchanged
 * to preserve the value schemas of existing checkpoints.
 */
object CombineApproximatePercentiles extends Rule[LogicalPlan] {

  private case class CompatibilityKey(
      child: Expression,
      accuracy: Long,
      mode: AggregateMode,
      isDistinct: Boolean,
      filter: Option[Expression])

  private case class PhysicalCompatibilityKey(
      child: Expression,
      accuracy: Expression,
      mode: AggregateMode,
      isDistinct: Boolean,
      filter: Option[Expression])

  private def physicalCompatibilityKey(
      key: CompatibilityKey,
      accuracy: Expression): PhysicalCompatibilityKey = PhysicalCompatibilityKey(
    key.child.canonicalized,
    accuracy.canonicalized,
    key.mode,
    key.isDistinct,
    key.filter.map(_.canonicalized))

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(AGGREGATE), ruleId) {
    case aggregate: Aggregate if aggregate.resolved && !aggregate.isStreaming =>
      combine(aggregate)
  }

  private def combine(aggregate: Aggregate): Aggregate = {
    val compatible = mutable.LinkedHashMap.empty[
      CompatibilityKey, mutable.ArrayBuffer[AggregateExpression]]
    val physicalCompatibilityKeys = mutable.HashMap.empty[
      PhysicalCompatibilityKey, mutable.HashSet[CompatibilityKey]]
    val arrayPercentiles = mutable.ArrayBuffer.empty[AggregateExpression]

    aggregate.aggregateExpressions.foreach(_.foreach {
      case expression @ AggregateExpression(
          percentile: ApproximatePercentile, mode, isDistinct, filter, _)
          if percentile.child.deterministic &&
            filter.forall(_.deterministic) =>
        val key = CompatibilityKey(
          percentile.child,
          percentile.accuracyExpression.eval().asInstanceOf[Number].longValue,
          mode,
          isDistinct,
          filter)
        physicalCompatibilityKeys.getOrElseUpdate(
          physicalCompatibilityKey(key, percentile.accuracyExpression),
          mutable.HashSet.empty) += key
        if (percentile.percentageExpression.dataType == DoubleType) {
          compatible.getOrElseUpdate(key, mutable.ArrayBuffer.empty) += expression
        } else {
          arrayPercentiles += expression
        }
      case _ =>
    })

    val replacements = mutable.HashMap.empty[ExprId, (AggregateExpression, Int)]
    compatible.iterator.filter { case (key, expressions) =>
      expressions.sizeCompare(1) > 0 && expressions.forall { expression =>
        val percentile = expression.aggregateFunction.asInstanceOf[ApproximatePercentile]
        physicalCompatibilityKeys(
          physicalCompatibilityKey(key, percentile.accuracyExpression)).sizeCompare(1) == 0
      }
    }.foreach { case (_, expressions) =>
      val first = expressions.head
      val percentile = first.aggregateFunction.asInstanceOf[ApproximatePercentile]
      val percentages = expressions.map { expression =>
        expression.aggregateFunction
          .asInstanceOf[ApproximatePercentile]
          .percentageExpression
      }
      val combined = first.copy(
        aggregateFunction = percentile.copy(
          percentageExpression = CreateArray(percentages.toSeq)))
      if (!arrayPercentiles.exists(_.semanticEquals(combined))) {
        expressions.zipWithIndex.foreach { case (expression, index) =>
          replacements.put(expression.resultId, (combined, index))
        }
      }
    }

    if (replacements.isEmpty) {
      aggregate
    } else {
      val rewrittenExpressions = aggregate.aggregateExpressions.map { expression =>
        expression.transformUp {
          case original: AggregateExpression if replacements.contains(original.resultId) =>
            val (combined, index) = replacements(original.resultId)
            GetArrayItem(combined, Literal(index), failOnError = false)
        }.asInstanceOf[NamedExpression]
      }
      aggregate.copy(aggregateExpressions = rewrittenExpressions)
    }
  }
}
