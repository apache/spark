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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.execution.{CoalescedPartitionSpec, ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{REBALANCE_PARTITIONS_BY_COL, REBALANCE_PARTITIONS_BY_NONE, ShuffleOrigin}
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule to optimize the skewed shuffle partitions in [[RebalancePartitions]] based on the map
 * output statistics, which can avoid data skew that hurt performance.
 *
 * We use ADVISORY_PARTITION_SIZE_IN_BYTES size to decide if a partition should be optimized.
 * Let's say we have 3 maps with 3 shuffle partitions, and assuming r1 has data skew issue.
 * the map side looks like:
 *   m0:[b0, b1, b2], m1:[b0, b1, b2], m2:[b0, b1, b2]
 * and the reduce side looks like:
 *                            (without this rule) r1[m0-b1, m1-b1, m2-b1]
 *                              /                                     \
 *   r0:[m0-b0, m1-b0, m2-b0], r1-0:[m0-b1], r1-1:[m1-b1], r1-2:[m2-b1], r2[m0-b2, m1-b2, m2-b2]
 */
object OptimizeSkewInRebalancePartitions extends AQEShuffleReadRule {

  override val supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(REBALANCE_PARTITIONS_BY_NONE, REBALANCE_PARTITIONS_BY_COL)

  /**
   * Splits the skewed partition based on the map size and the target partition size
   * after split. Create a list of `PartialReducerPartitionSpec` for skewed partition and
   * create `CoalescedPartition` for normal partition.
   */
  private def optimizeSkewedPartitions(
      shuffleId: Int,
      bytesByPartitionId: Array[Long],
      targetSize: Long): Seq[ShufflePartitionSpec] = {
    val smallPartitionFactor =
      conf.getConf(SQLConf.ADAPTIVE_REBALANCE_PARTITIONS_SMALL_PARTITION_FACTOR)
    bytesByPartitionId.indices.flatMap { reduceIndex =>
      val bytes = bytesByPartitionId(reduceIndex)
      if (bytes > targetSize) {
        val newPartitionSpec = ShufflePartitionsUtil.createSkewPartitionSpecs(
          shuffleId, reduceIndex, targetSize, smallPartitionFactor)
        if (newPartitionSpec.isEmpty) {
          CoalescedPartitionSpec(reduceIndex, reduceIndex + 1, bytes) :: Nil
        } else {
          logDebug(s"For shuffle $shuffleId, partition $reduceIndex is skew, " +
            s"split it into ${newPartitionSpec.get.size} parts.")
          newPartitionSpec.get
        }
      } else {
        CoalescedPartitionSpec(reduceIndex, reduceIndex + 1, bytes) :: Nil
      }
    }
  }

  private def tryOptimizeSkewedPartitions(shuffle: ShuffleQueryStageExec): SparkPlan = {
    val advisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    val mapStats = shuffle.mapStats
    if (mapStats.isEmpty ||
      mapStats.get.bytesByPartitionId.forall(_ <= advisorySize)) {
      return shuffle
    }

    val newPartitionsSpec = optimizeSkewedPartitions(
      mapStats.get.shuffleId, mapStats.get.bytesByPartitionId, advisorySize)
    // return origin plan if we can not optimize partitions
    if (newPartitionsSpec.length == mapStats.get.bytesByPartitionId.length) {
      shuffle
    } else {
      AQEShuffleReadExec(shuffle, newPartitionsSpec)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED)) {
      return plan
    }

    plan transformUp {
      case stage: ShuffleQueryStageExec if isSupported(stage.shuffle) =>
        tryOptimizeSkewedPartitions(stage)
    }
  }
}
