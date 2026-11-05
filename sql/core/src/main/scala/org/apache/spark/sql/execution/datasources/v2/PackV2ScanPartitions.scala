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

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}

/**
 * Marks V2 scans for packing after distribution and ordering requirements have been planned.
 * A shuffle or broadcast exchange consumes neither input property. Otherwise, propagate
 * consumers' requirements conservatively through known operators, including a broadcast
 * join's streamed side. Keep unknown operators and explicit coalesces unchanged. Under AQE, a
 * local shuffle read with one mapper per task reports the layout below its shuffle, which a
 * packed scan no longer claims.
 *
 * Packing happens inside BatchScanExec, before partial aggregation and UnsafeRow conversion.
 * No keyed or ordered output is claimed, and scans used by GroupPartitionsExec stay unchanged.
 * The rule only marks a scan and does not plan its inputs: `BatchScanExec.inputRDD` decides the
 * groups at execution, from the inputs that runtime filtering leaves.
 *
 * Like `DisableUnnecessaryBucketedScan`, this is a separate rule that runs after
 * `EnsureRequirements` has planned every exchange. Under AQE, `requiredDistribution` is the
 * distribution the final plan must satisfy, the same one `EnsureRequirements` is given.
 * `keepRootLayout` keeps every claim that reaches the plan's root, for a plan whose output
 * partitioning and ordering reach a consumer, such as a streaming micro-batch's sink.
 */
case class PackV2ScanPartitions(
    requiredDistribution: Option[Distribution] = None,
    keepRootLayout: Boolean = false) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.v2ScanPartitionPackingEnabled) {
      return plan
    }
    lazy val options = V2ScanPartitionPacking(
      conf.v2ScanPartitionPackingMaxPartitionBytes,
      conf.v2ScanPartitionPackingOpenCostInBytes,
      conf.v2ScanPartitionPackingMinPartitionNum,
      conf.v2ScanPartitionPackingMaxPartitionNum)

    def rewrite(node: SparkPlan, needsPartitioning: Boolean, needsOrdering: Boolean): SparkPlan = {
      node match {
        case scan: BatchScanExec if !needsPartitioning && !needsOrdering &&
            scan.partitionPacking.isEmpty && !scan.scan.isInstanceOf[FileScan] =>
          // A V2 file scan is already sized by spark.sql.files.*, as a V1 file scan is.
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
          // ReplaceHashWithSortAgg runs later and uses the ordering of an aggregate's child. A V1
          // file write reads its child's ordering only with planned writes off or concurrent
          // output writers on, both internal settings, and then sorts the scan's rows itself.
          val sortAggregate = node match {
            case agg: BaseAggregateExec => ReplaceHashWithSortAgg.canReplace(agg)
            case _ => false
          }
          val children = node.children.zipWithIndex.map { case (child, i) =>
            rewrite(child,
              needsPartitioning || node.requiredChildDistribution(i) != UnspecifiedDistribution,
              needsOrdering || sortAggregate || node.requiredChildOrdering(i).nonEmpty)
          }
          node.withNewChildren(children)
        case _ =>
          // Includes GroupPartitionsExec, UnionExec, windows, explicit coalesces and extensions.
          // An exchange further down can still isolate a scan from these consumers.
          node.mapChildren(rewrite(_, needsPartitioning = true, needsOrdering = true))
      }
    }

    rewrite(plan, keepRootLayout || requiredDistribution.exists(_ != UnspecifiedDistribution),
      needsOrdering = keepRootLayout)
  }
}
