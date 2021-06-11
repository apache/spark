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

import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, REPARTITION_BY_COL, REPARTITION_BY_NONE, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule to expand the shuffle partitions based on the map output statistics, which can
 * avoid data skew that hurt performance.
 *
 * We use ADVISORY_PARTITION_SIZE_IN_BYTES size to decide if a partition should be expanded.
 * Let's say we have 3 maps with 3 shuffle partitions, and assuming r1 has data skew issue.
 * the map side looks like:
 *   m0:[b0, b1, b2], m1:[b0, b1, b2], m2:[b0, b1, b2]
 * and the reduce side looks like:
 *                          (without this rule) r1[m0-b1, m1-b1, m2-b1]
 *                              /                              \
 *   r0:[m0-b0, m1-b0, m2-b0], r1:[m0-b1], r2:[m1-b1], r3:[m2-b1], r4[m0-b2, m1-b2, m2-b2]
 */
object ExpandShufflePartitions extends CustomShuffleReaderRule {
  override def supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(REPARTITION_BY_COL, REPARTITION_BY_NONE)

  private def expandPartitions(plan: SparkPlan): SparkPlan = {
    def collectShuffleStageInfos(plan: SparkPlan): Seq[ShuffleStageInfo] = plan match {
      case ShuffleStageInfo(stage, specs) => Seq(new ShuffleStageInfo(stage, specs))
      case _ => plan.children.flatMap(collectShuffleStageInfos)
    }
    val shuffleStageInfos = collectShuffleStageInfos(plan)
    assert(shuffleStageInfos.size == 1)
    val shuffleStageInfo = shuffleStageInfos.head
    if (!supportCoalesce(shuffleStageInfo.shuffleStage.shuffle)) {
      return plan
    }

    val advisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    val mapStats = shuffleStageInfo.shuffleStage.mapStats
    if (mapStats.isEmpty ||
      mapStats.get.bytesByPartitionId.forall(_ <= advisorySize)) {
      return plan
    }

    val newPartitionsSpec = ShufflePartitionsUtil.expandPartitions(
      mapStats.get, shuffleStageInfo.partitionSpecs, advisorySize)
    def updateShuffleReaders(p: SparkPlan): SparkPlan = p match {
      case ShuffleStageInfo(stage, _) =>
        CustomShuffleReaderExec(stage, newPartitionsSpec)
      case _ => p.mapChildren(updateShuffleReaders)
    }
    updateShuffleReaders(plan)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.ADAPTIVE_EXPAND_PARTITIONS_ENABLED)) {
      return plan
    }

    val leaves = plan.collectLeaves()
    // We only handle the shuffle which is introduced by REPARTITION
    if (leaves.size != 1 || !leaves.head.isInstanceOf[ShuffleQueryStageExec]) {
      return plan
    }

    val newPlan = expandPartitions(plan)
    val extraShuffle = EnsureRequirements.apply(newPlan).find {
      case _: ShuffleExchangeLike => true
      case _ => false
    }
    // For save, we don't expand partition if introduce extra shuffle.
    if (extraShuffle.isDefined) {
      plan
    } else {
      newPlan
    }
  }

  private def supportCoalesce(s: ShuffleExchangeLike): Boolean = {
    s.outputPartitioning != SinglePartition && supportedShuffleOrigins.contains(s.shuffleOrigin)
  }
}
