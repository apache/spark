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

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateMode, Complete, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule combines adjacent aggregation with `Partial` and `Final` to `Complete` mode.
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
 */
object CombineAdjacentAggregation extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED)) {
      return plan
    }

    plan.transformDown {
      case finalAgg @ HashAggregateExec(_, _, _, _, _, _, _, _, partialAgg: HashAggregateExec) =>
        combinedMode(partialAgg, finalAgg)
          .map(combineHashAggregates(partialAgg, finalAgg, _))
          .getOrElse(finalAgg)

      case finalAgg @ SortAggregateExec(_, _, _, _, _, _, _, _, partialAgg: SortAggregateExec)
          if isPartialAgg(partialAgg, finalAgg) =>
        finalAgg.copy(
          groupingExpressions = partialAgg.groupingExpressions,
          aggregateExpressions = partialAgg.aggregateExpressions.map(_.copy(mode = Complete)),
          initialInputBufferOffset = 0,
          child = partialAgg.child)

      case finalAgg @ ObjectHashAggregateExec(_, _, _, _, _, _, _, _,
        partialAgg: ObjectHashAggregateExec)
          if isPartialAgg(partialAgg, finalAgg) =>
        finalAgg.copy(
          groupingExpressions = partialAgg.groupingExpressions,
          aggregateExpressions = partialAgg.aggregateExpressions.map(_.copy(mode = Complete)),
          initialInputBufferOffset = 0,
          child = partialAgg.child)
    }
  }

  private def combineHashAggregates(
      partialAgg: HashAggregateExec,
      finalAgg: HashAggregateExec,
      mode: AggregateMode): HashAggregateExec = {
    val aggregateExpressions = mode match {
      case Complete => partialAgg.aggregateExpressions.map(_.copy(mode = Complete))
      case Final => finalAgg.aggregateExpressions
      case other => throw new IllegalArgumentException(s"Unexpected aggregate mode: $other")
    }
    finalAgg.copy(
      requiredChildDistributionExpressions = partialAgg.requiredChildDistributionExpressions,
      isStreaming = partialAgg.isStreaming,
      numShufflePartitions = partialAgg.numShufflePartitions,
      groupingExpressions = partialAgg.groupingExpressions,
      aggregateExpressions = aggregateExpressions,
      initialInputBufferOffset = if (mode == Complete) 0 else finalAgg.initialInputBufferOffset,
      child = partialAgg.child)
  }

  private def combinedMode(
      partialAgg: HashAggregateExec,
      finalAgg: HashAggregateExec): Option[AggregateMode] = {
    if (!isCompatibleAggregates(partialAgg, finalAgg)) {
      None
    } else if (partialAgg.outputSet != finalAgg.usedInputs) {
      None
    } else if (partialAgg.aggregateExpressions.forall(_.mode == Partial)) {
      Some(Complete)
    } else if (partialAgg.aggregateExpressions.forall(_.mode == PartialMerge) &&
        partialAgg.aggregateExpressions.forall(_.filter.isEmpty) &&
        finalAgg.aggregateExpressions.forall(_.filter.isEmpty)) {
      Some(Final)
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
