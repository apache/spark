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
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

/**
 * Packs the partitions of a V2 batch scan below a shuffle, with `PackScanPartitionsExec`, when the
 * shuffle's child reads one scan through operators that compute each of their partitions from one
 * scan partition: projects, filters, sorts, partial aggregates and the streamed side of a broadcast
 * hash join. A shuffle reads only its child's rows, so the node gives up the scan's layout, and the
 * operators below it keep it. Above the shuffle, only an AQE local read with one mapper per task
 * reports the layout from before the shuffle. Over a packed shuffle that read reports none, and
 * AQE keeps the full shuffle read where an operator needs the scan's layout. The list starts from
 * the operators that `DisableUnnecessaryBucketedScan` allows; others that also map partitions one
 * to one, such as expand and generate, are not packed yet. Scans of file-based V2 sources are left
 * to `spark.sql.files.*`, as V1 file scans are.
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
        shuffle.withNewChildren(Seq(PackScanPartitionsExec(shuffle.child, packing)))
    }
  }

  // The node itself is not listed, so a child that is already packed is not packed again.
  private def readsOneScan(plan: SparkPlan): Boolean = plan match {
    case scan: BatchScanExec => !scan.scan.isInstanceOf[FileScan]
    case _: ProjectExec | _: FilterExec | _: SortExec => readsOneScan(plan.children.head)
    case agg: BaseAggregateExec if agg.requiredChildDistributionExpressions.isEmpty =>
      readsOneScan(agg.child)
    case join: BroadcastHashJoinExec => join.buildSide match {
      case BuildLeft => readsOneScan(join.right)
      case BuildRight => readsOneScan(join.left)
    }
    case _ => false
  }
}
