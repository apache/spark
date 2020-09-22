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

package org.apache.spark.sql.execution.bucketing

import org.apache.spark.sql.catalyst.expressions.aggregate.{Partial, PartialMerge}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashClusteredDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf

/**
 * Disable unnecessary bucketed table scan based on actual physical query plan.
 * NOTE: this rule is designed to be applied right after [[EnsureRequirements]],
 * where all [[ShuffleExchangeExec]] and [[SortExec]] have been added to plan properly.
 *
 * When BUCKETING_ENABLED and DYNAMIC_DECIDE_BUCKETING_ENABLED are set to true, go through
 * query plan to check where bucketed table scan is unnecessary, and disable bucketed table
 * scan if needed.
 *
 * For all operators which [[hasInterestingPartition]] (i.e., require [[ClusteredDistribution]]
 * or [[HashClusteredDistribution]]), check if the sub-plan for operator has [[Exchange]] and
 * bucketed table scan. If yes, disable the bucketed table scan in the sub-plan.
 * Only allow certain operators in sub-plan, which guarantees each sub-plan is single lineage
 * (i.e., each operator has only one child). See details in
 * [[disableBucketWithInterestingPartition]]).
 *
 * Examples:
 * (1).join:
 *         SortMergeJoin(t1.i = t2.j)
 *            /            \
 *        Sort(i)        Sort(j)
 *          /               \
 *      Shuffle(i)       Scan(t2: i, j)
 *        /         (bucketed on column j, enable bucketed scan)
 *   Scan(t1: i, j)
 * (bucketed on column j, DISABLE bucketed scan)
 *
 * (2).aggregate:
 *         HashAggregate(i, ..., Final)
 *                      |
 *                  Shuffle(i)
 *                      |
 *         HashAggregate(i, ..., Partial)
 *                      |
 *                    Filter
 *                      |
 *                  Scan(t1: i, j)
 *  (bucketed on column j, DISABLE bucketed scan)
 *
 * The idea of [[hasInterestingPartition]] is inspired from "interesting order" in
 * the paper "Access Path Selection in a Relational Database Management System"
 * (http://www.inf.ed.ac.uk/teaching/courses/adbs/AccessPath.pdf).
 */
case class DisableUnnecessaryBucketedScan(conf: SQLConf) extends Rule[SparkPlan] {

  /**
   * Disable bucketed table scan with pre-order traversal of plan.
   *
   * @param withInterestingPartition The traversed plan has operator with interesting partition.
   * @param withExchange The traversed plan has [[Exchange]] operator.
   */
  private def disableBucketWithInterestingPartition(
      plan: SparkPlan,
      withInterestingPartition: Boolean,
      withExchange: Boolean): SparkPlan = {
    plan match {
      case p if hasInterestingPartition(p) =>
        // Operators with interesting partition, propagates `withInterestingPartition` as true
        // to its children.
        p.mapChildren(disableBucketWithInterestingPartition(_, true, false))
      case exchange: Exchange if withInterestingPartition =>
        // Exchange operator propagates `withExchange` as true to its child
        // if the plan has interesting partition.
        exchange.mapChildren(disableBucketWithInterestingPartition(
          _, withInterestingPartition, true))
      case scan: FileSourceScanExec
          if withInterestingPartition && withExchange && isBucketedScanWithoutFilter(scan) =>
        // Disable bucketed table scan if the plan has interesting partition,
        // and [[Exchange]] in the plan.
        scan.copy(disableBucketedScan = true)
      case o =>
        if (isAllowedUnaryExecNode(o)) {
          // Propagates `withInterestingPartition` and `withExchange` from parent
          // for only allowed single-child nodes.
          o.mapChildren(disableBucketWithInterestingPartition(
            _, withInterestingPartition, withExchange))
        } else {
          o.mapChildren(disableBucketWithInterestingPartition(_, false, false))
        }
    }
  }

  private def hasInterestingPartition(plan: SparkPlan): Boolean = {
    plan.requiredChildDistribution.exists {
      case _: ClusteredDistribution | _: HashClusteredDistribution => true
      case _ => false
    }
  }

  private def isAllowedUnaryExecNode(plan: SparkPlan): Boolean = {
    plan match {
      case _: SortExec | _: Exchange | _: ProjectExec | _: FilterExec |
           _: FileSourceScanExec => true
      case partialAgg: BaseAggregateExec =>
        val modes = partialAgg.aggregateExpressions.map(_.mode)
        modes.nonEmpty && modes.forall(mode => mode == Partial || mode == PartialMerge)
      case _ => false
    }
  }

  private def isBucketedScanWithoutFilter(scan: FileSourceScanExec): Boolean = {
    // Do not disable bucketed table scan if it has filter pruning,
    // because bucketed table scan is still useful here to save CPU/IO cost with
    // only reading selected bucket files.
    scan.bucketedScan && scan.optionalBucketSet.isEmpty
  }

  private def disableAllBucketedScan(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case scan: FileSourceScanExec if isBucketedScanWithoutFilter(scan) =>
        scan.copy(disableBucketedScan = true)
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.bucketingEnabled || !conf.autoBucketedScanEnabled) {
      plan
    } else {
      if (plan.find(hasInterestingPartition).isEmpty) {
        // Disable all bucketed scans if there's no operator with interesting partition
        // found in query plan.
        disableAllBucketedScan(plan)
      } else {
        disableBucketWithInterestingPartition(plan, false, false)
      }
    }
  }
}
