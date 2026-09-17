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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule combines adjacent aggregation with `Partial` and `Final` to `Complete` mode. For
 * [[HashAggregateExec]], it also combines `PartialMerge` and `Final` to `Final` mode. The latter
 * can be produced by physical plan extensions that add an extra aggregation stage.
 * Example for hash aggregate:
 *    HashAggregate (Final)         HashAggregate (Complete)
 *          |                             |
 *    HashAggregate (Partial)    =>    Exchange
 *          |
 *       Exchange
 *
 * Example for sort aggregate:
 *    SortAggregateExec (Final)       SortAggregateExec (Complete)
 *          |                               |
 *    SortAggregateExec (Partial)    =>    Sort
 *          |                               |
 *         Sort                          Exchange
 *          |
 *       Exchange
 *
 * It supports [[HashAggregateExec]], [[SortAggregateExec]] and [[ObjectHashAggregateExec]].
 *
 * A [[GroupPartitionsExec]] and the local sorts `EnsureRequirements` put between the two aggregates
 * are looked through, so the pair is combined even when the final aggregate's distribution was
 * satisfied without a shuffle. See `detachAggregate`.
 */
object CombineAdjacentAggregation extends Rule[SparkPlan] {
  private case class CombinedAggregate(
      aggregateExpressions: Seq[AggregateExpression],
      initialInputBufferOffset: Int)

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED)) {
      return plan
    }

    plan.transformDown {
      case finalAgg: HashAggregateExec =>
        detachAggregate(finalAgg.child, hasUpperSort = false) match {
          case Some((partialAgg: HashAggregateExec, child)) =>
            combinedAggregate(partialAgg, finalAgg)
              .map(combineHashAggregates(partialAgg, finalAgg, _, child))
              .getOrElse(finalAgg)
          case _ => finalAgg
        }

      case finalAgg: SortAggregateExec =>
        detachAggregate(finalAgg.child, hasUpperSort = false) match {
          case Some((partialAgg: SortAggregateExec, child)) if isPartialAgg(partialAgg, finalAgg) =>
            finalAgg.copy(
              groupingExpressions = partialAgg.groupingExpressions,
              aggregateExpressions = partialAgg.aggregateExpressions.map(_.copy(mode = Complete)),
              initialInputBufferOffset = 0,
              child = child)
          case _ => finalAgg
        }

      case finalAgg: ObjectHashAggregateExec =>
        detachAggregate(finalAgg.child, hasUpperSort = false) match {
          case Some((partialAgg: ObjectHashAggregateExec, child))
              if isPartialAgg(partialAgg, finalAgg) =>
            finalAgg.copy(
              groupingExpressions = partialAgg.groupingExpressions,
              aggregateExpressions = partialAgg.aggregateExpressions.map(_.copy(mode = Complete)),
              initialInputBufferOffset = 0,
              child = child)
          case _ => finalAgg
        }
    }
  }

  /**
   * Detaches the aggregate at the bottom of the chain `plan` starts and hands it back together with
   * the subtree to leave where it was, or `None` when the chain bottoms out at no aggregate, or at
   * one that cannot leave. The chain's `GroupPartitionsExec` and local sorts are crossed in place,
   * so the combined aggregate reads whatever ends up on top of them.
   *
   * A sort crossed above the aggregate orders the rows the combined aggregate reads, by the
   * grouping the two aggregates share, so the sort the aggregate reads goes with it: that crossed
   * sort is what orders those rows. Where the aggregate holds no sort of its own, it stays, being
   * the only cardinality reducer before that sort. With no sort crossed at all, the sort below the
   * aggregate stays below it: the aggregate reads what it read, and the ordering it claims is the
   * one it had.
   *
   * @param hasUpperSort whether a local sort has been crossed above `plan`, which is what makes the
   *                     sort below the aggregate dead.
   */
  private def detachAggregate(
      plan: SparkPlan,
      hasUpperSort: Boolean): Option[(BaseAggregateExec, SparkPlan)] = plan match {
    case aggregate: BaseAggregateExec =>
      if (!hasUpperSort) {
        Some((aggregate, aggregate.child))
      } else {
        aggregate.child match {
          case sort: SortExec if !sort.global => Some((aggregate, sort.child))
          case _ => None
        }
      }

    case group: GroupPartitionsExec =>
      detachAggregate(group.child, hasUpperSort) match {
        case Some((aggregate, child)) =>
          group.withKeyPositionsFor(child).map(regrouped => (aggregate, regrouped))
        case _ => None
      }

    case sort: SortExec if !sort.global =>
      detachAggregate(sort.child, hasUpperSort = true) match {
        case Some((aggregate, child)) =>
          Some((aggregate, sort.withNewChildren(Seq(child))))
        case _ => None
      }

    case _ => None
  }

  private def combineHashAggregates(
      partialAgg: HashAggregateExec,
      finalAgg: HashAggregateExec,
      combined: CombinedAggregate,
      child: SparkPlan): HashAggregateExec = {
    // Keep the final aggregate's distribution requirement because the rule runs after
    // EnsureRequirements. The other child-facing metadata comes from the removed aggregate.
    finalAgg.copy(
      isStreaming = partialAgg.isStreaming,
      numShufflePartitions = partialAgg.numShufflePartitions,
      groupingExpressions = partialAgg.groupingExpressions,
      aggregateExpressions = combined.aggregateExpressions,
      initialInputBufferOffset = combined.initialInputBufferOffset,
      child = child)
  }

  private def combinedAggregate(
      partialAgg: HashAggregateExec,
      finalAgg: HashAggregateExec): Option[CombinedAggregate] = {
    if (!isCompatibleAggregates(partialAgg, finalAgg)) {
      None
    } else if (partialAgg.aggregateExpressions.forall(_.mode == Partial)) {
      Some(CombinedAggregate(
        partialAgg.aggregateExpressions.map(_.copy(mode = Complete)),
        initialInputBufferOffset = 0))
    } else if (partialAgg.aggregateExpressions.forall(_.mode == PartialMerge) &&
        partialAgg.aggregateExpressions.forall(_.filter.isEmpty) &&
        finalAgg.aggregateExpressions.forall(_.filter.isEmpty)) {
      Some(CombinedAggregate(
        finalAgg.aggregateExpressions,
        partialAgg.initialInputBufferOffset))
    } else {
      None
    }
  }

  /**
   * Check if `partialAgg` is the partial aggregate of `finalAgg`.
   */
  private def isPartialAgg(
      partialAgg: BaseAggregateExec,
      finalAgg: BaseAggregateExec): Boolean = {
    partialAgg.aggregateExpressions.forall(_.mode == Partial) &&
      isCompatibleAggregates(partialAgg, finalAgg)
  }

  private def isCompatibleAggregates(
      partialAgg: BaseAggregateExec,
      finalAgg: BaseAggregateExec): Boolean = {
    finalAgg.aggregateExpressions.forall(_.mode == Final) &&
      partialAgg.groupingExpressions.map(_.canonicalized) ==
        finalAgg.groupingExpressions.map(_.canonicalized) &&
      finalAgg.logicalLink.isDefined &&
      partialAgg.logicalLink.isDefined &&
      finalAgg.logicalLink.get.sameResult(partialAgg.logicalLink.get)
  }
}
