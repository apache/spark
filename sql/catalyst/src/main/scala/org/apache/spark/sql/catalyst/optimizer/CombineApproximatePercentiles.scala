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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExprId, GetArrayItem, LeafExpression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateMode, ApproximatePercentile}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DoubleType}

private[optimizer] case class PercentileFusionIdentity(
    aggregateFunctions: Seq[Expression],
    mode: AggregateMode,
    isDistinct: Boolean,
    filter: Option[Expression],
    percentageBits: Seq[Long])

/**
 * Foldable percentage array that retains the original scalar aggregate structures in equality.
 *
 * Fusion removes those structures from the physical aggregate. Keeping them here prevents
 * subquery or exchange reuse from equating plans that were distinct before fusion.
 */
private[optimizer] case class PercentileFusionArray(identity: PercentileFusionIdentity)
    extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false
  override def dataType: ArrayType = ArrayType(DoubleType, containsNull = false)

  private lazy val value = new GenericArrayData(
    identity.percentageBits.map(java.lang.Double.longBitsToDouble))
  private lazy val literal = Literal(value, dataType)

  override def eval(input: InternalRow): Any = value
  override def toString: String = literal.toString
  override def sql: String = literal.sql
}

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
      percentage: Expression,
      accuracy: Expression,
      mode: AggregateMode,
      isDistinct: Boolean,
      filter: Option[Expression])

  private def structurallyNormalize(
      expression: Expression,
      inputOrdinals: scala.collection.Map[ExprId, Int]): Expression = expression.transformUp {
    case attribute: AttributeReference =>
      inputOrdinals.get(attribute.exprId) match {
        case Some(ordinal) => AttributeReference("none", attribute.dataType)(ExprId(ordinal))
        case None => attribute
      }
  }

  private def physicalCompatibilityKey(
      key: CompatibilityKey,
      percentile: ApproximatePercentile): PhysicalCompatibilityKey = PhysicalCompatibilityKey(
    key.child.canonicalized,
    percentile.percentageExpression.canonicalized,
    percentile.accuracyExpression.canonicalized,
    key.mode,
    key.isDistinct,
    key.filter.map(_.canonicalized))

  private def hasSafePhysicalFusion(
      expressions: scala.collection.Iterable[AggregateExpression]): Boolean = {
    val physicalGroups = expressions.groupBy(_.canonicalized)
    // PhysicalAggregation already shares a digest within each canonical group. Fusion must both
    // remove a digest and preserve cases where canonical percentages evaluate differently.
    physicalGroups.sizeCompare(1) > 0 && physicalGroups.values.forall { group =>
      val percentages = group.iterator.map { expression =>
        expression.aggregateFunction
          .asInstanceOf[ApproximatePercentile]
          .percentageExpression
          .eval()
      }
      val first = percentages.next()
      percentages.forall(_ == first)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.COMBINE_APPROXIMATE_PERCENTILES_ENABLED)) return plan

    plan.transformUpWithPruning(_.containsPattern(AGGREGATE), ruleId) {
      case aggregate: Aggregate if aggregate.resolved && !aggregate.isStreaming =>
        combine(aggregate)
    }
  }

  private def combine(aggregate: Aggregate): Aggregate = {
    val compatible = mutable.LinkedHashMap.empty[
      CompatibilityKey, mutable.ArrayBuffer[AggregateExpression]]
    // PhysicalAggregation deduplicates semantically equivalent aggregates. Track every logical
    // key that shares a physical key so fusion does not change that existing deduplication.
    val physicalCompatibilityKeys = mutable.HashMap.empty[
      PhysicalCompatibilityKey, mutable.HashSet[CompatibilityKey]]

    aggregate.aggregateExpressions.foreach(_.foreach {
      case expression @ AggregateExpression(
          percentile: ApproximatePercentile, mode, isDistinct, filter, _)
          if percentile.child.deterministic &&
            filter.forall(_.deterministic) =>
        val key = CompatibilityKey(
          percentile.child,
          // Analysis already validates that accuracy is foldable, non-null, and in range.
          percentile.accuracyExpression.eval().asInstanceOf[Number].longValue,
          mode,
          isDistinct,
          filter)
        physicalCompatibilityKeys.getOrElseUpdate(
          physicalCompatibilityKey(key, percentile),
          mutable.HashSet.empty) += key
        if (percentile.percentageExpression.dataType == DoubleType) {
          compatible.getOrElseUpdate(key, mutable.ArrayBuffer.empty) += expression
        }
      case _ =>
    })

    val replacements = mutable.HashMap.empty[ExprId, (AggregateExpression, Int)]
    lazy val inputOrdinals = {
      val ordinals = mutable.HashMap.empty[ExprId, Int]
      aggregate.child.output.zipWithIndex.foreach { case (attribute, ordinal) =>
        ordinals.getOrElseUpdate(attribute.exprId, ordinal)
      }
      ordinals
    }
    compatible.iterator.map { case (key, expressions) =>
      key -> expressions.distinctBy(_.resultId)
    }.filter { case (key, expressions) =>
      hasSafePhysicalFusion(expressions) && expressions.forall { expression =>
        val percentile = expression.aggregateFunction.asInstanceOf[ApproximatePercentile]
        val physicalKey = physicalCompatibilityKey(key, percentile)
        // OptimizeOneRowPlan can erase DISTINCT after fusion. Across distinctness boundaries,
        // canonical matches are safe only when their original inputs and filters also match.
        physicalCompatibilityKeys(physicalKey).sizeCompare(1) == 0 &&
          physicalCompatibilityKeys
            .get(physicalKey.copy(isDistinct = !physicalKey.isDistinct))
            .forall(_.forall(other => other.child == key.child && other.filter == key.filter))
      }
    }.foreach { case (key, expressions) =>
      val first = expressions.head
      val percentile = first.aggregateFunction.asInstanceOf[ApproximatePercentile]
      val percentages = expressions.map { expression =>
        expression.aggregateFunction
          .asInstanceOf[ApproximatePercentile]
          .percentageExpression
      }
      val percentageValues = percentages.map(_.eval().asInstanceOf[Double]).toSeq
      val identity = PercentileFusionIdentity(
        expressions.map { expression =>
          structurallyNormalize(expression.aggregateFunction, inputOrdinals)
        }.toSeq,
        key.mode,
        key.isDistinct,
        key.filter.map(structurallyNormalize(_, inputOrdinals)),
        percentageValues.map(java.lang.Double.doubleToRawLongBits))
      val combinedFunction = percentile.copy(percentageExpression = PercentileFusionArray(identity))
      combinedFunction.copyTagsFrom(percentile)
      val combined = first.copy(
        aggregateFunction = combinedFunction, resultId = NamedExpression.newExprId)
      expressions.zipWithIndex.foreach { case (expression, index) =>
        replacements.put(expression.resultId, (combined, index))
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
