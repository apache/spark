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
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, ExplainUtils, PartialReducerPartitionSpec, ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, REPARTITION, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule to coalesce the shuffle partitions based on the map output statistics, which can
 * avoid many small reduce tasks that hurt performance.
 */
case class CoalesceShufflePartitions(session: SparkSession) extends CustomShuffleReaderRule {

  override val supportedShuffleOrigins: Seq[ShuffleOrigin] = Seq(ENSURE_REQUIREMENTS, REPARTITION)

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.coalesceShufflePartitionsEnabled) {
      return plan
    }
    /* This is running before new QueryStageExec creation so either all leaves are
     QueryStageExec nodes or all leaves are CustomShuffleReaderExec if OptimizeSkewJoin
     mitigated something in the new stage. */
    if (!plan.collectLeaves().forall(_.isInstanceOf[QueryStageExec])) {
      // If not all leaf nodes are query stages, it's not safe to reduce the number of
      // shuffle partitions, because we may break the assumption that all children of a spark plan
      // have same number of output partitions.
      // If OptimizeSkewedJoin mitigated the stage, it would have wrapped all QueryStageExec
      // nodes with CustomShuffleReaderExec
      logInfo(s"CoalesceShufflePartitions: " +
        s"Cannot coalesce due to not all leaves are SQE.")
      return plan
    }

    def collectShuffleStages(plan: SparkPlan): Seq[ShuffleQueryStageExec] = plan match {
      case stage: ShuffleQueryStageExec => Seq(stage)
      case _ => plan.children.flatMap(collectShuffleStages)
    }

    val shuffleStages = collectShuffleStages(plan)
    val s = ExplainUtils.getAQELogPrefix(shuffleStages)
    // ShuffleExchanges introduced by repartition do not support changing the number of partitions.
    // We change the number of partitions in the stage only if all the ShuffleExchanges support it.
    if (!shuffleStages.forall(s => supportCoalesce(s.shuffle))) {
      logInfo(s"CoalesceShufflePartitions: Cannot coalesce due to explicit Repartition; $s")
      plan
    } else {
      // todo: perhaps add a check the set of children of nodes in csreList
      //  are exactly the shuffleStages
      val csreList = plan.collect {
        case csre: CustomShuffleReaderExec => csre
      }
      // `ShuffleQueryStageExec#mapStats` returns None when the input RDD has 0 partitions,
      // we should skip it when calculating the `partitionStartIndices`.
      val validMetrics = shuffleStages.flatMap(_.mapStats)

      // We may have different pre-shuffle partition numbers, don't reduce shuffle partition number
      // in that case. For example when we union fully aggregated data (data is arranged to a single
      // partition) and a result of a SortMergeJoin (multiple partitions).
      val distinctNumPreShufflePartitions =
        validMetrics.map(stats => stats.bytesByPartitionId.length).distinct
      if (validMetrics.nonEmpty && distinctNumPreShufflePartitions.length == 1) {
        // We fall back to Spark default parallelism if the minimum number of coalesced partitions
        // is not set, so to avoid perf regressions compared to no coalescing.
        val minPartitionNum = conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM)
          .getOrElse(session.sparkContext.defaultParallelism)
        val partitionSpecs = ShufflePartitionsUtil.coalescePartitions(
          validMetrics.toArray,
          advisoryTargetSize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES),
          minNumPartitions = minPartitionNum)
        // This transformation adds new nodes, so we must use `transformUp` here.
        val stageIds = shuffleStages.map(_.id).toSet

        if(csreList.isEmpty) {
          plan.transformUp {
            // even for shuffle exchange whose input RDD has 0 partition, we should still update its
            // `partitionStartIndices`, so that all the leaf shuffles in a stage have the same
            // number of output partitions.
            case stage: ShuffleQueryStageExec if stageIds.contains(stage.id) =>
              CustomShuffleReaderExec(stage, partitionSpecs)
          }
        } else {
          plan.transformUp {
                // why check stageIds.contains(stage.id) above?
            case customReader: CustomShuffleReaderExec =>
              CustomShuffleReaderExec(customReader.child,
                newPartitionSpec(partitionSpecs.asInstanceOf[Seq[CoalescedPartitionSpec]],
                  customReader))
          }
        }
      } else {
        logInfo(s"CoalesceShufflePartitions: Cannot coalesce due to distinct partition counts:" +
          s" $distinctNumPreShufflePartitions: $s")
        plan
      }
    }
  }

  /**
   * This assumes that any partition is either skewed and needs to be split or it's
   * too small and needs to be coalesced but not both.  (It can be neither and taken as is).
   * For example,
   * coalescedPartitionSpecs: CoalescedPartitionSpec(0,1), CoalescedPartitionSpec(1,4)
   *
   * and balancedReader.partitionSpecs (0th partition is replicated):
   * CoalescedPartitionSpec(0,1)
   * CoalescedPartitionSpec(0,1)
   * CoalescedPartitionSpec(0,1)
   * CoalescedPartitionSpec(1,2)
   * CoalescedPartitionSpec(2,3)
   * CoalescedPartitionSpec(3,4)
   *
   * desired output:
   * CoalescedPartitionSpec(0,1)
   * CoalescedPartitionSpec(0,1)
   * CoalescedPartitionSpec(0,1)
   * CoalescedPartitionSpec(1,4)
   *
   *
   * and balancedReader.partitionSpecs (0th partition is split):
   * (different invocation of newPartitionSpec())
   * PartialReducerPartitionSpec(0,0,1)
   * PartialReducerPartitionSpec(0,1,5)
   * PartialReducerPartitionSpec(0,5,10)
   * CoalescedPartitionSpec(1,2)
   * CoalescedPartitionSpec(2,3)
   * CoalescedPartitionSpec(3,4)
   *
   * desired output:
   * PartialReducerPartitionSpec(0,0,1)
   * PartialReducerPartitionSpec(0,1,5)
   * PartialReducerPartitionSpec(0,5,10)
   * CoalescedPartitionSpec(1,4)
   *
   * @param coalescedPartitionSpecs - as produced by [[ShufflePartitionsUtil.coalescePartitions]]
   * @param balancedReader          - created by [[OptimizeSkewedJoin]]
   * @return updated partition spec that combines results of skew mitigation and coalescing
   */
  private def newPartitionSpec(coalescedPartitionSpecs: Seq[CoalescedPartitionSpec],
                               balancedReader: CustomShuffleReaderExec) = {
    var balancedReaderPartIdx = 0;
    val newPartSpecs = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    coalescedPartitionSpecs.foreach(cps => {
      if (cps.endReducerIndex - cps.startReducerIndex > 1) {
        // cps is really coalesced
        newPartSpecs += cps
        balancedReaderPartIdx += (cps.endReducerIndex - cps.startReducerIndex)
      } else {
        // the matching reducer partition may have been split/replicated due to skew
        var iterate = true
        while (iterate && balancedReaderPartIdx < balancedReader.partitionSpecs.length) {
          val balancedReaderSpec = balancedReader.partitionSpecs(balancedReaderPartIdx)
          val startReducerIndex: Int = balancedReaderSpec match {
            case c: CoalescedPartitionSpec => c.startReducerIndex
            case p: PartialReducerPartitionSpec => p.reducerIndex
            case m =>
              throw new IllegalArgumentException(m.toString)
          }
          if (startReducerIndex == cps.startReducerIndex) {
            newPartSpecs += balancedReaderSpec
            balancedReaderPartIdx += 1
          } else {
            iterate = false
          }
        }
      }
    })
    newPartSpecs
  }
  private def supportCoalesce(s: ShuffleExchangeLike): Boolean = {
    s.outputPartitioning != SinglePartition && supportedShuffleOrigins.contains(s.shuffleOrigin)
  }
}
