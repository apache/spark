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
 * Plans bucketing dynamically based on actual physical query plan.
 * NOTE: this rule is designed to be applied right after [[EnsureRequirements]],
 * where all [[ShuffleExchangeExec]] and [[SortExec]] have been added to plan properly.
 *
 * When BUCKETING_ENABLED and DYNAMIC_DECIDE_BUCKETING_ENABLED are set to true, go through
 * query plan to check where bucketed table scan is unnecessary, and disable bucketed table
 * scan if needed.
 *
 * For all operators which [[hasInterestingPartition]] (i.e., require [[ClusteredDistribution]]
 * or [[HashClusteredDistribution]]), check if the sub-plan for operator has [[Exchange]] and
 * bucketed table scan (and only allow certain operators in plan, see details in
 * [[canDisableBucketedScan]]). If yes, disable the bucketed table scan in the sub-plan.
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
case class PlanBucketing(conf: SQLConf) extends Rule[SparkPlan] {
  private def disableBucketWithInterestingPartition(plan: SparkPlan): SparkPlan = {
    var hasPlanWithInterestingPartition = false

    val newPlan = plan.transformUp {
      case p if hasInterestingPartition(p) =>
        hasPlanWithInterestingPartition = true

        val newChildren = p.children.map(child => {
          if (canDisableBucketedScan(child)) {
            disableBucketedScan(child)
          } else {
            child
          }
        })
        p.withNewChildren(newChildren)
      case other => other
    }

    if (hasPlanWithInterestingPartition) {
      newPlan
    } else {
      // Disable all bucketed scans if there's no operator with interesting partition
      // found in query plan.
      disableBucketedScan(newPlan)
    }
  }

  private def hasInterestingPartition(plan: SparkPlan): Boolean = {
    plan.requiredChildDistribution.exists {
      case _: ClusteredDistribution | _: HashClusteredDistribution => true
      case _ => false
    }
  }

  private def canDisableBucketedScan(plan: SparkPlan): Boolean = {
    val hasExchange = plan.find(_.isInstanceOf[Exchange]).isDefined
    val hasBucketedScan = plan.find {
      case scan: FileSourceScanExec => isBucketedScanWithoutFilter(scan)
      case _ => false
    }.isDefined

    def isAllowedPlan(plan: SparkPlan): Boolean = plan match {
      case _: SortExec | _: Exchange | _: ProjectExec | _: FilterExec |
           _: FileSourceScanExec => true
      case partialAgg: BaseAggregateExec =>
        val modes = partialAgg.aggregateExpressions.map(_.mode)
        modes.nonEmpty && modes.forall(mode => mode == Partial || mode == PartialMerge)
      case _ => false
    }
    val onlyHasAllowedPlans = plan.find(!isAllowedPlan(_)).isEmpty
    hasExchange && hasBucketedScan && onlyHasAllowedPlans
  }

  private def disableBucketedScan(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case scan: FileSourceScanExec if isBucketedScanWithoutFilter(scan) =>
        scan.copy(optionalDynamicDecideBucketing = Some(false))
    }
  }

  private def isBucketedScanWithoutFilter(scan: FileSourceScanExec): Boolean = {
    // Do not disable bucketing for the scan if it has filter pruning,
    // because bucketing is still useful here to save CPU/IO cost with
    // only reading selected bucket files.
    scan.bucketedScan && scan.optionalBucketSet.isEmpty
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.bucketingEnabled || !conf.dynamicDecideBucketingEnabled) {
      plan
    } else {
      disableBucketWithInterestingPartition(plan)
    }
  }
}
