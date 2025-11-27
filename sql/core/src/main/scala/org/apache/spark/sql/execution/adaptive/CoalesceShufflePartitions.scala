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

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan, UnaryExecNode, UnionExec}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, REBALANCE_PARTITIONS_BY_COL, REBALANCE_PARTITIONS_BY_NONE, REPARTITION_BY_COL, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * A rule to coalesce the shuffle partitions based on the map output statistics, which can
 * avoid many small reduce tasks that hurt performance.
 */
case class CoalesceShufflePartitions(session: SparkSession) extends AQEShuffleReadRule {

  override def conf: SQLConf = session.sessionState.conf

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

    // Sub-plans under the Union/CartesianProduct/BroadcastHashJoin/BroadcastNestedLoopJoin
    // operator can be coalesced independently, so we can divide them into independent
    // "coalesce groups", and all shuffle stages within each group have to be coalesced together.
    val coalesceGroups = collectCoalesceGroups(plan)

    // Divide minimum task parallelism among coalesce groups according to their data sizes.
    val minNumPartitionsByGroup = if (coalesceGroups.length == 1) {
      Seq(math.max(minNumPartitions, 1))
    } else {
      val sizes = coalesceGroups.map(
        _.shuffleStages.flatMap(_.shuffleStage.mapStats.map(_.bytesByPartitionId.sum)).sum)
      val totalSize = sizes.sum
      sizes.map { size =>
        val num = if (totalSize > 0) {
          math.round(minNumPartitions * 1.0 * size / totalSize)
        } else {
          minNumPartitions
        }
        math.max(num.toInt, 1)
      }
    }

    val specsMap = mutable.HashMap.empty[Int, Seq[ShufflePartitionSpec]]
    // Coalesce partitions for each coalesce group independently.
    coalesceGroups.zip(minNumPartitionsByGroup).foreach { case (coalesceGroup, minNumPartitions) =>
      val advisoryTargetSize = advisoryPartitionSize(coalesceGroup)
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
        coalesceGroup.shuffleStages.map(_.shuffleStage.mapStats),
        coalesceGroup.shuffleStages.map(_.partitionSpecs),
        advisoryTargetSize = advisoryTargetSize,
        minNumPartitions = minNumPartitions,
        minPartitionSize = minPartitionSize)

      if (newPartitionSpecs.nonEmpty) {
        coalesceGroup.shuffleStages.zip(newPartitionSpecs).map { case (stageInfo, partSpecs) =>
          specsMap.put(stageInfo.shuffleStage.id, partSpecs)
        }
      }
    }

    if (specsMap.nonEmpty) {
      updateShuffleReads(plan, specsMap.toMap)
    } else {
      plan
    }
  }

  // data sources may request a particular advisory partition size for the final write stage
  // if it happens, the advisory partition size will be set in ShuffleQueryStageExec
  // only one shuffle stage is expected in such cases
  private def advisoryPartitionSize(coalesceGroup: CoalesceGroup): Long = {
    if (coalesceGroup.hasExplodingJoin) {
      return conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE)
    }
    val defaultAdvisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    coalesceGroup.shuffleStages match {
      case Seq(stage) =>
        stage.shuffleStage.advisoryPartitionSize.getOrElse(defaultAdvisorySize)
      case _ =>
        defaultAdvisorySize
    }
  }

  /**
   * Gather all coalesce-able groups such that the shuffle stages in each child of a
   * Union/CartesianProduct/BroadcastHashJoin/BroadcastNestedLoopJoin operator are in their
   * independent groups if:
   * 1) all leaf nodes of this child are exchange stages; and
   * 2) all these shuffle stages support coalescing.
   */
  private def collectCoalesceGroups(
      plan: SparkPlan,
      hasExplodingJoin: Boolean = false): Seq[CoalesceGroup] = plan match {
    case r @ AQEShuffleReadExec(q: ShuffleQueryStageExec, _) if isSupported(q.shuffle) =>
      Seq(CoalesceGroup(collectShuffleStageInfos(r), hasExplodingJoin))
    case unary: UnaryExecNode => collectCoalesceGroups(unary.child, hasExplodingJoin)
    // If a plan node does not need compatible data partitioning for its children, then each of its
    // child can be an individual coalesce group and Spark will apply shuffle partitions coalescing
    // for them independently,
    case p if !childrenNeedCompatiblePartitioning(p) =>
      val hasExplodingJoinSoFar = hasExplodingJoin || isExplodingJoin(p)
      p.children.flatMap(collectCoalesceGroups(_, hasExplodingJoinSoFar))
    // If not all leaf nodes are exchange query stages, it's not safe to reduce the number of
    // shuffle partitions, because we may break the assumption that all children of a spark plan
    // have same number of output partitions.
    case p if p.collectLeaves().forall(_.isInstanceOf[ExchangeQueryStageExec]) =>
      val shuffleStages = collectShuffleStageInfos(p)
      // ShuffleExchanges introduced by repartition do not support partition number change.
      // We change the number of partitions only if all the ShuffleExchanges support it.
      if (shuffleStages.forall(s => isSupported(s.shuffleStage.shuffle))) {
        // The recursion stops here, we need to call `p.exists(isExplodingJoin)` and find out if
        // there is any exploding join in this sub-plan-tree.
        Seq(CoalesceGroup(shuffleStages, hasExplodingJoin || p.exists(isExplodingJoin)))
      } else {
        Seq.empty
      }
    case _ => Seq.empty
  }

  private def childrenNeedCompatiblePartitioning(p: SparkPlan): Boolean = p match {
    // TODO: match more plan nodes here.
    case _: UnionExec => false
    case _: CartesianProductExec => false
    case _: BroadcastHashJoinExec => false
    case _: BroadcastNestedLoopJoinExec => false
    case _ => true
  }

  private def isExplodingJoin(p: SparkPlan): Boolean = p match {
    case _: BroadcastNestedLoopJoinExec => true
    case _: CartesianProductExec => true
    case _ => false
  }

  private def collectShuffleStageInfos(plan: SparkPlan): Seq[ShuffleStageInfo] = plan match {
    case ShuffleStageInfo(stage, specs) => Seq(new ShuffleStageInfo(stage, specs))
    case _ => plan.children.flatMap(collectShuffleStageInfos)
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

private case class CoalesceGroup(
  shuffleStages: Seq[ShuffleStageInfo],
  hasExplodingJoin: Boolean)
