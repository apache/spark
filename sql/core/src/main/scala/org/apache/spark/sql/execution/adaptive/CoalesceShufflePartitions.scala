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
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan, UnaryExecNode, UnionExec}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, REBALANCE_PARTITIONS_BY_COL, REBALANCE_PARTITIONS_BY_NONE, REPARTITION_BY_COL, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.internal.SQLConf

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

    // Sub-plans under the Union operator can be coalesced independently, so we can divide them
    // into independent "coalesce groups", and all shuffle stages within each group have to be
    // coalesced together.
    val coalesceGroups = collectCoalesceGroups(plan)
    val groups = coalesceGroups.map { shuffleStages =>
      val shuffleIds = shuffleStages.flatMap(_.shuffleStage.mapStats.map(_.shuffleId))
      (shuffleStages.map(_.shuffleStage.id),
        advisoryPartitionSize(shuffleStages),
        shuffleStages.map(_.shuffleStage.mapStats.map(_.bytesByPartitionId)),
        shuffleStages.map(_.partitionSpecs),
        s"For shuffle(${shuffleIds.mkString(", ")})")
    }
    val specsMap = ShufflePartitionsUtil.coalescePartitionsByGroup(
      groups, session.sparkContext.defaultParallelism)
    if (specsMap.nonEmpty) {
      updateShuffleReads(plan, specsMap)
    } else {
      plan
    }
  }

  // data sources may request a particular advisory partition size for the final write stage
  // if it happens, the advisory partition size will be set in ShuffleQueryStageExec
  // only one shuffle stage is expected in such cases
  private def advisoryPartitionSize(shuffleStages: Seq[ShuffleStageInfo]): Long = {
    val defaultAdvisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    shuffleStages match {
      case Seq(stage) =>
        stage.shuffleStage.advisoryPartitionSize.getOrElse(defaultAdvisorySize)
      case _ =>
        defaultAdvisorySize
    }
  }

  /**
   * Gather all coalesce-able groups such that the shuffle stages in each child of a Union operator
   * are in their independent groups if:
   * 1) all leaf nodes of this child are exchange stages; and
   * 2) all these shuffle stages support coalescing.
   */
  private def collectCoalesceGroups(plan: SparkPlan): Seq[Seq[ShuffleStageInfo]] = plan match {
    case r @ AQEShuffleReadExec(q: ShuffleQueryStageExec, _) if isSupported(q.shuffle) =>
      Seq(collectShuffleStageInfos(r))
    case unary: UnaryExecNode => collectCoalesceGroups(unary.child)
    case union: UnionExec => union.children.flatMap(collectCoalesceGroups)
    // If not all leaf nodes are exchange query stages, it's not safe to reduce the number of
    // shuffle partitions, because we may break the assumption that all children of a spark plan
    // have same number of output partitions.
    // Note that, `BroadcastQueryStageExec` is a valid case:
    // If a join has been optimized from shuffled join to broadcast join, then the one side is
    // `BroadcastQueryStageExec` and other side is `ShuffleQueryStageExec`. It can coalesce the
    // shuffle side as we do not expect broadcast exchange has same partition number.
    case p if p.collectLeaves().forall(_.isInstanceOf[ExchangeQueryStageExec]) =>
      val shuffleStages = collectShuffleStageInfos(p)
      // ShuffleExchanges introduced by repartition do not support partition number change.
      // We change the number of partitions only if all the ShuffleExchanges support it.
      if (shuffleStages.forall(s => isSupported(s.shuffleStage.shuffle))) {
        Seq(shuffleStages)
      } else {
        Seq.empty
      }
    case _ => Seq.empty
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
