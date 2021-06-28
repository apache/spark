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

/**
 * A rule to coalesce the shuffle partitions based on the map output statistics, which can
 * avoid many small reduce tasks that hurt performance.
 */
case class CoalesceShufflePartitions(session: SparkSession) extends CustomShuffleReaderRule {

  override val supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(ENSURE_REQUIREMENTS, REPARTITION_BY_COL, REBALANCE_PARTITIONS_BY_NONE,
      REBALANCE_PARTITIONS_BY_COL)

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
    if (!shuffleStageInfos.forall(s => supportCoalesce(s.shuffleStage.shuffle))) {
      plan
    } else {
      // We fall back to Spark default parallelism if the minimum number of coalesced partitions
      // is not set, so to avoid perf regressions compared to no coalescing.
      val minPartitionNum = conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM)
        .getOrElse(session.sparkContext.defaultParallelism)
      val newPartitionSpecs = ShufflePartitionsUtil.coalescePartitions(
        shuffleStageInfos.map(_.shuffleStage.mapStats),
        shuffleStageInfos.map(_.partitionSpecs),
        advisoryTargetSize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES),
        minNumPartitions = minPartitionNum)

      if (newPartitionSpecs.nonEmpty) {
        val specsMap = shuffleStageInfos.zip(newPartitionSpecs).map { case (stageInfo, partSpecs) =>
          (stageInfo.shuffleStage.id, partSpecs)
        }.toMap
        updateShuffleReaders(plan, specsMap)
      } else {
        plan
      }
    }
  }

  private def updateShuffleReaders(
      plan: SparkPlan, specsMap: Map[Int, Seq[ShufflePartitionSpec]]): SparkPlan = plan match {
    // Even for shuffle exchange whose input RDD has 0 partition, we should still update its
    // `partitionStartIndices`, so that all the leaf shuffles in a stage have the same
    // number of output partitions.
    case ShuffleStageInfo(stage, _) =>
      specsMap.get(stage.id).map { specs =>
        CustomShuffleReaderExec(stage, specs)
      }.getOrElse(plan)
    case other => other.mapChildren(updateShuffleReaders(_, specsMap))
  }

  private def supportCoalesce(s: ShuffleExchangeLike): Boolean = {
    s.outputPartitioning != SinglePartition && supportedShuffleOrigins.contains(s.shuffleOrigin)
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
    case CustomShuffleReaderExec(s: ShuffleQueryStageExec, partitionSpecs) =>
      Some((s, Some(partitionSpecs)))
    case _ => None
  }
}
