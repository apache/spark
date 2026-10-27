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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, V2ScanPartitionPacking, V2TableWriteExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Pack V2 scan inputs after distribution and ordering requirements have been planned.
 * A shuffle or broadcast exchange consumes neither input property. Otherwise, propagate
 * consumers' requirements conservatively through known operators, including a broadcast
 * join's streamed side. Keep unknown operators and explicit coalesces unchanged.
 *
 * Packing happens inside BatchScanExec, before partial aggregation and UnsafeRow conversion.
 * No keyed or ordered output is claimed, and scans used by GroupPartitionsExec stay unchanged.
 */
object PackV2ScanPartitions extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = apply(plan, None)

  def apply(plan: SparkPlan, requiredDistribution: Option[Distribution]): SparkPlan = {
    if (!conf.getConf(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED)) {
      return plan
    }
    lazy val options = V2ScanPartitionPacking(
      conf.getConf(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES),
      conf.getConf(SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES),
      conf.getConf(SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM).getOrElse(
        conf.getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM).getOrElse(
          SparkSession.active.sparkContext.defaultParallelism)),
      conf.getConf(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_NUM))

    def rewrite(node: SparkPlan, needsPartitioning: Boolean, needsOrdering: Boolean): SparkPlan = {
      node match {
        case scan: BatchScanExec if !needsPartitioning && !needsOrdering &&
            scan.partitionPacking.isEmpty &&
            options.pack(scan.inputPartitions.map(Some(_))).size < scan.inputPartitions.size =>
          scan.withPartitionPacking(options)
        case _: BatchScanExec => node
        case _: ShuffleExchangeLike | _: BroadcastExchangeLike =>
          node.withNewChildren(node.children.zipWithIndex.map { case (child, i) =>
            rewrite(child,
              node.requiredChildDistribution(i) != UnspecifiedDistribution,
              node.requiredChildOrdering(i).nonEmpty)
          })
        case sort: SortExec =>
          // Keep a scan ordering that makes this sort redundant. Otherwise the existing sort
          // establishes ordering itself, so packing cannot add a new sort or lose its output order.
          val redundant = SortOrder.orderingSatisfies(sort.child.outputOrdering, sort.sortOrder)
          sort.withNewChildren(Seq(rewrite(sort.child,
            needsPartitioning || sort.requiredChildDistribution.head != UnspecifiedDistribution,
            redundant)))
        case _: ProjectExec | _: FilterExec | _: BaseAggregateExec |
            _: BroadcastHashJoinExec | _: BroadcastNestedLoopJoinExec |
            _: V2TableWriteExec | _: DataWritingCommandExec | _: WriteFilesExec =>
          val children = node.children.zipWithIndex.map { case (child, i) =>
            rewrite(child,
              needsPartitioning || node.requiredChildDistribution(i) != UnspecifiedDistribution,
              needsOrdering || node.requiredChildOrdering(i).nonEmpty)
          }
          node.withNewChildren(children)
        case _ =>
          // Includes GroupPartitionsExec, UnionExec, windows, explicit coalesces and extensions.
          // An exchange further down can still isolate a scan from these consumers.
          node.mapChildren(rewrite(_, needsPartitioning = true, needsOrdering = true))
      }
    }

    rewrite(plan, requiredDistribution.exists(_ != UnspecifiedDistribution), needsOrdering = false)
  }
}
