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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, REBALANCE_PARTITIONS_BY_COL, REBALANCE_PARTITIONS_BY_NONE, REPARTITION_BY_COL, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * A rule to coalesce the shuffle partitions based on the map output statistics, which can
 * avoid many small reduce tasks that hurt performance.
 */
case class CoalesceShufflePartitions(session: SparkSession) extends AQEShuffleReadRule {

  override val supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(ENSURE_REQUIREMENTS, REPARTITION_BY_COL, REBALANCE_PARTITIONS_BY_NONE,
      REBALANCE_PARTITIONS_BY_COL)

  override def isSupported(shuffle: ShuffleExchangeLike): Boolean = {
    shuffle.outputPartitioning != SinglePartition && super.isSupported(shuffle)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.coalesceShufflePartitionsEnabled) {
      return plan
    }
    if (!plan.collectLeaves().forall(_.isInstanceOf[QueryStageExec])) {
      // If not all leaf nodes are query stages, it's not safe to reduce the number of
      // shuffle partitions, because we may break the assumption that all children of a spark plan
      // have same number of output partitions.
      return plan
    }

    def collectShuffleStageInfos(plan: SparkPlan): Seq[ShuffleStageInfo] = plan match {
      case ShuffleStageInfo(stage, specs) => Seq(new ShuffleStageInfo(stage, specs))
      case _ => plan.children.flatMap(collectShuffleStageInfos)
    }

    val shuffleStageInfos = collectShuffleStageInfos(plan)
    // ShuffleExchanges introduced by repartition do not support changing the number of partitions.
    // We change the number of partitions in the stage only if all the ShuffleExchanges support it.
    if (!shuffleStageInfos.forall(s => isSupported(s.shuffleStage.shuffle))) {
      plan
    } else {
      // Ideally, this rule should simply coalesce partitions w.r.t. the target size specified by
      // ADVISORY_PARTITION_SIZE_IN_BYTES (default 64MB). To avoid perf regression in AQE, this
      // rule by default tries to maximize the parallelism and set the target size to
      // `total shuffle size / Spark default parallelism`. In case the `Spark default parallelism`
      // is too big, this rule also respect the minimum partition size specified by
      // COALESCE_PARTITIONS_MIN_PARTITION_SIZE (default 1MB).
      // For history reason, this rule also need to support the config
      // COALESCE_PARTITIONS_MIN_PARTITION_NUM. We should remove this config in the future.
      val minNumPartitions = conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM).getOrElse {
        if (conf.getConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST)) {
          // We fall back to Spark default parallelism if the minimum number of coalesced partitions
          // is not set, so to avoid perf regressions compared to no coalescing.
          session.sparkContext.defaultParallelism
        } else {
          // If we don't need to maximize the parallelism, we set `minPartitionNum` to 1, so that
          // the specified advisory partition size will be respected.
          1
        }
      }
      val advisoryTargetSize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
      val minPartitionSize = if (Utils.isTesting) {
        // In the tests, we usually set the target size to a very small value that is even smaller
        // than the default value of the min partition size. Here we also adjust the min partition
        // size to be not larger than 20% of the target size, so that the tests don't need to set
        // both configs all the time to check the coalescing behavior.
        conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE).min(advisoryTargetSize / 5)
      } else {
        conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE)
      }
      val newPartitionSpecs = ShufflePartitionsUtil.coalescePartitions(
        shuffleStageInfos.map(_.shuffleStage.mapStats),
        shuffleStageInfos.map(_.partitionSpecs),
        advisoryTargetSize = advisoryTargetSize,
        minNumPartitions = minNumPartitions,
        minPartitionSize = minPartitionSize)

      if (newPartitionSpecs.nonEmpty) {
        val specsMap = shuffleStageInfos.zip(newPartitionSpecs).map { case (stageInfo, partSpecs) =>
          (stageInfo.shuffleStage.id, partSpecs)
        }.toMap
        updateShuffleReads(plan, specsMap)
      } else {
        plan
      }
    }
  }

  private def updateShuffleReads(
      plan: SparkPlan, specsMap: Map[Int, Seq[ShufflePartitionSpec]]): SparkPlan = plan match {
    // Even for shuffle exchange whose input RDD has 0 partition, we should still update its
    // `partitionStartIndices`, so that all the leaf shuffles in a stage have the same
    // number of output partitions.
    case ShuffleStageInfo(stage, _) =>
      specsMap.get(stage.id).map { specs =>
        AQEShuffleReadExec(stage, specs)
      }.getOrElse(plan)
    case other => other.mapChildren(updateShuffleReads(_, specsMap))
  }
}

private class ShuffleStageInfo(
    val shuffleStage: ShuffleQueryStageExec,
    val partitionSpecs: Option[Seq[ShufflePartitionSpec]])

private object ShuffleStageInfo {
  def unapply(plan: SparkPlan)
  : Option[(ShuffleQueryStageExec, Option[Seq[ShufflePartitionSpec]])] = plan match {
    case stage: ShuffleQueryStageExec =>
      Some((stage, None))
    case AQEShuffleReadExec(s: ShuffleQueryStageExec, partitionSpecs) =>
      Some((s, Some(partitionSpecs)))
    case _ => None
  }
}
