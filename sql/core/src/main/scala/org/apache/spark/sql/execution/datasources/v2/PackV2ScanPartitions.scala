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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

/**
 * Packs the partitions of a V2 batch scan that a shuffle reads, with `PackScanPartitionsExec`,
 * when the shuffle's child reads one scan through operators that compute each of their partitions
 * from one scan partition: projects, filters, sorts, partial aggregates and the streamed side of a
 * broadcast hash join. The list starts from the operators that `DisableUnnecessaryBucketedScan`
 * allows; others that also map partitions one to one, such as expand and generate, are not packed
 * yet. Scans of file-based V2 sources are left to `spark.sql.files.*`, as V1 file scans are.
 *
 * The node reports no partitioning and no ordering, so it goes at the lowest point on that path
 * where the plan still meets every operator's requirements, checked with `ValidateRequirements`.
 * That is directly above the scan unless an operator on the path relies on the scan's ordering,
 * such as a partial sort aggregate with no sort between it and the scan. The node then goes above
 * that operator. Directly below the shuffle is the last candidate, and is kept even if the check
 * fails there, since a shuffle requires nothing of its child. The operators above the node run
 * once per packed partition, and those below it once per input.
 *
 * Above the shuffle, only an AQE local read with one mapper per task reports the layout from
 * before the shuffle. Over a packed shuffle that read reports none, and AQE keeps the full shuffle
 * read where an operator needs the scan's layout.
 *
 * The rule runs after `ReplaceHashWithSortAgg` and `RemoveRedundantSorts`, so the check sees which
 * operators rely on the scan's ordering once those rules have decided.
 */
object PackV2ScanPartitions extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.v2ScanPartitionPackingEnabled) {
      return plan
    }
    lazy val packing = V2ScanPartitionPacking(
      conf.v2ScanPartitionPackingMaxPartitionBytes,
      conf.v2ScanPartitionPackingOpenCostInBytes,
      conf.v2ScanPartitionPackingMinPartitionNum,
      conf.v2ScanPartitionPackingMaxPartitionNum)
    plan.transformUp {
      case shuffle: ShuffleExchangeExec if readsOneScan(shuffle.child) =>
        shuffle.withNewChildren(Seq(packAtLowestSafePoint(shuffle.child, packing)))
    }
  }

  // The node itself is not listed, so a child that is already packed is not packed again.
  private def readsOneScan(plan: SparkPlan): Boolean = plan match {
    case scan: BatchScanExec => !scan.scan.isInstanceOf[FileScan]
    case _: ProjectExec | _: FilterExec | _: SortExec => readsOneScan(plan.children.head)
    case agg: BaseAggregateExec if agg.requiredChildDistributionExpressions.isEmpty =>
      readsOneScan(agg.child)
    case join: BroadcastHashJoinExec => readsOneScan(streamedSide(join))
    case _ => false
  }

  private def streamedSide(join: BroadcastHashJoinExec): SparkPlan = join.buildSide match {
    case BuildLeft => join.right
    case BuildRight => join.left
  }

  /**
   * Tries the node above each operator below `top`, the shuffle's child, on its path from the scan
   * up, and keeps the first plan that passes `ValidateRequirements`. Otherwise the node goes
   * directly above `top`.
   */
  private def packAtLowestSafePoint(top: SparkPlan, packing: V2ScanPartitionPacking): SparkPlan = {
    pathToScan(top).tail.reverseIterator
      .map(packAbove(top, _, packing))
      .find(ValidateRequirements.validate)
      .getOrElse(PackScanPartitionsExec(top, packing))
  }

  // The operators from `plan` down to the scan, in that order. `readsOneScan` held for `plan`.
  private def pathToScan(plan: SparkPlan): List[SparkPlan] = plan match {
    case scan: BatchScanExec => scan :: Nil
    case join: BroadcastHashJoinExec => join :: pathToScan(streamedSide(join))
    case other => other :: pathToScan(other.children.head)
  }

  // `plan` with the node above `target`, an operator on its path to the scan.
  private def packAbove(
      plan: SparkPlan,
      target: SparkPlan,
      packing: V2ScanPartitionPacking): SparkPlan = {
    if (plan eq target) {
      PackScanPartitionsExec(plan, packing)
    } else {
      plan match {
        case join: BroadcastHashJoinExec => join.buildSide match {
          case BuildLeft =>
            join.withNewChildren(Seq(join.left, packAbove(join.right, target, packing)))
          case BuildRight =>
            join.withNewChildren(Seq(packAbove(join.left, target, packing), join.right))
        }
        case other => other.withNewChildren(Seq(packAbove(other.children.head, target, packing)))
      }
    }
  }
}
