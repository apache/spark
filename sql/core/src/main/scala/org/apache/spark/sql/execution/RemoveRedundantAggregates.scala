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

import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Final, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec,
  ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Remove redundant two-phase aggregate nodes (Partial + Final) when no shuffle exists
 * between them.
 *
 * In a normal two-phase aggregation, `AggUtils.planAggregateWithoutDistinct` creates a
 * partial aggregate (mode=Partial) and a final aggregate (mode=Final) with an implicit
 * expectation that `EnsureRequirements` will insert an Exchange between them. However,
 * when the child's `outputPartitioning` satisfies `ClusteredDistribution(groupingKeys)`,
 * `EnsureRequirements` skips the shuffle. Without the shuffle, the final aggregate
 * receives data that is already aggregated within each partition by the partial aggregate,
 * making it completely redundant.
 *
 * This rule detects such patterns and replaces both operators with a single node of the
 * same concrete type in Complete mode, which evaluates the aggregation in one pass
 * directly on the raw input.
 *
 * Handles HashAggregateExec, ObjectHashAggregateExec, and SortAggregateExec.
 * The final and partial node must be the same concrete type.
 */
object RemoveRedundantAggregates extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.REMOVE_REDUNDANT_AGGREGATES_ENABLED)) {
      plan
    } else {
      plan.transformUp {
        case finalAgg: BaseAggregateExec =>
          finalAgg.child match {
            case partialAgg: BaseAggregateExec if isRedundant(finalAgg, partialAgg) =>
              // Copy aggregateExpressions from the partial agg: Complete mode uses the same
              // updateExpressions path as Partial, operating on raw input rows.
              val completeAggExpressions =
                partialAgg.aggregateExpressions.map(_.copy(mode = Complete))
              // Preserve the same concrete type to keep codegen paths and ordering contracts.
              finalAgg match {
                case h: HashAggregateExec =>
                  h.copy(
                    requiredChildDistributionExpressions =
                      Some(partialAgg.groupingExpressions),
                    groupingExpressions = partialAgg.groupingExpressions,
                    aggregateExpressions = completeAggExpressions,
                    initialInputBufferOffset = 0,
                    child = partialAgg.child)
                case o: ObjectHashAggregateExec =>
                  o.copy(
                    requiredChildDistributionExpressions =
                      Some(partialAgg.groupingExpressions),
                    groupingExpressions = partialAgg.groupingExpressions,
                    aggregateExpressions = completeAggExpressions,
                    initialInputBufferOffset = 0,
                    child = partialAgg.child)
                case s: SortAggregateExec =>
                  s.copy(
                    requiredChildDistributionExpressions =
                      Some(partialAgg.groupingExpressions),
                    groupingExpressions = partialAgg.groupingExpressions,
                    aggregateExpressions = completeAggExpressions,
                    initialInputBufferOffset = 0,
                    child = partialAgg.child)
                case _ => finalAgg // unknown subclass: skip optimization
              }
            case _ => finalAgg
          }
      }
    }
  }

  private def isRedundant(
      finalAgg: BaseAggregateExec,
      partialAgg: BaseAggregateExec): Boolean = {
    val sameOperatorType = finalAgg.getClass == partialAgg.getClass
    // Two cases: grouping-only (both empty) or standard two-phase (Final over Partial).
    // An emptiness check is needed because forall() is vacuously true on an empty collection,
    // which would incorrectly match a PartialMerge node in the grouping-only dedup stage
    // of a COUNT(DISTINCT) plan.
    val correctModes =
      (finalAgg.aggregateExpressions.isEmpty && partialAgg.aggregateExpressions.isEmpty) ||
      (finalAgg.aggregateExpressions.forall(_.mode == Final) &&
       partialAgg.aggregateExpressions.forall(_.mode == Partial))
    // Use toAttribute.canonicalized so float/double keys compare equal: the partial agg wraps
    // them in Alias(KnownFloatingPointNormalized(...)) while the final agg has plain
    // AttributeReferences; both reduce to the same AttributeReference after toAttribute.
    val sameGroupingKeys =
      finalAgg.groupingExpressions.map(_.toAttribute.canonicalized) ==
        partialAgg.groupingExpressions.map(_.toAttribute.canonicalized)
    sameOperatorType && correctModes && sameGroupingKeys
  }
}
