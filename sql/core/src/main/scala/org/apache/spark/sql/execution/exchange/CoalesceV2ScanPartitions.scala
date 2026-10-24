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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.read.HasPartitionKey
import org.apache.spark.sql.execution.{CoalesceExec, FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Optional task sizing for a keyed V2 scan below a required shuffle. Run after distribution
 * planning, so a scan used by a storage-partitioned consumer keeps the grouping that consumer
 * needs. Coalesce the whole scan pipeline immediately below the exchange: operators inside it
 * retain their input partitioning and ordering requirements, including partial aggregates and
 * the project that converts a V2 reader's rows to UnsafeRow.
 *
 * The number of distinct source keys is a compatibility target, not a distribution claim. The
 * existing narrow coalesce can put splits from different keys in the same task. It can still
 * reduce useful parallelism and does not impose a byte-size limit.
 */
object CoalesceV2ScanPartitions extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.v2BucketingEnabled ||
        !conf.getConf(SQLConf.V2_BUCKETING_COALESCE_SCAN_PARTITIONS_ENABLED)) {
      return plan
    }

    plan.transformUp {
      case exchange: ShuffleExchangeExec if exchange.shuffleOrigin == ENSURE_REQUIREMENTS =>
        findScan(exchange.child).map { scan =>
          // Use full source keys, even when scan column pruning collapses the projected keys.
          // Coalescing makes no keyed claim and must not change the subset-key permission.
          val factory = InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(
            scan.keyGroupedPartitioning.get.map(_.dataType))
          val numKeys = scan.inputPartitions.iterator.map { partition =>
            factory(partition.asInstanceOf[HasPartitionKey].partitionKey())
          }.toSet.size
          if (numKeys > 0 && numKeys < scan.inputPartitions.size) {
            exchange.copy(child = CoalesceExec(numKeys, exchange.child))
          } else {
            exchange
          }
        }.getOrElse(exchange)
    }
  }

  // Only follow a scan pipeline whose operators retain its number of partitions. Stop at
  // joins, unions, exchanges, GroupPartitionsExec and an existing CoalesceExec. Besides avoiding
  // changes to keyed consumers and explicit task sizing, the last case makes reapplication safe.
  @tailrec
  private def findScan(plan: SparkPlan): Option[BatchScanExec] = plan match {
    case scan: BatchScanExec if scan.outputPartitioning.isInstanceOf[KeyedPartitioning] =>
      Some(scan)
    case project: ProjectExec => findScan(project.child)
    case filter: FilterExec => findScan(filter.child)
    case aggregate: BaseAggregateExec if !aggregate.isStreaming => findScan(aggregate.child)
    case sort: SortExec if !sort.global => findScan(sort.child)
    case _ => None
  }
}
