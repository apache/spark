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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.MapOutputStatistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ShuffledRowRDD, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule to adjust the post shuffle partitions based on the map output statistics.
 *
 * The strategy used to determine the number of post-shuffle partitions is described as follows.
 * To determine the number of post-shuffle partitions, we have a target input size for a
 * post-shuffle partition. Once we have size statistics of all pre-shuffle partitions, we will do
 * a pass of those statistics and pack pre-shuffle partitions with continuous indices to a single
 * post-shuffle partition until adding another pre-shuffle partition would cause the size of a
 * post-shuffle partition to be greater than the target size.
 *
 * For example, we have two stages with the following pre-shuffle partition size statistics:
 * stage 1: [100 MiB, 20 MiB, 100 MiB, 10MiB, 30 MiB]
 * stage 2: [10 MiB,  10 MiB, 70 MiB,  5 MiB, 5 MiB]
 * assuming the target input size is 128 MiB, we will have four post-shuffle partitions,
 * which are:
 *  - post-shuffle partition 0: pre-shuffle partition 0 (size 110 MiB)
 *  - post-shuffle partition 1: pre-shuffle partition 1 (size 30 MiB)
 *  - post-shuffle partition 2: pre-shuffle partition 2 (size 170 MiB)
 *  - post-shuffle partition 3: pre-shuffle partition 3 and 4 (size 50 MiB)
 */
case class ReduceNumShufflePartitions(conf: SQLConf) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.reducePostShufflePartitionsEnabled) {
      return plan
    }
    if (!plan.collectLeaves().forall(_.isInstanceOf[QueryStageExec])) {
      // If not all leaf nodes are query stages, it's not safe to reduce the number of
      // shuffle partitions, because we may break the assumption that all children of a spark plan
      // have same number of output partitions.
      return plan
    }

    def collectShuffleStages(plan: SparkPlan): Seq[ShuffleQueryStageExec] = plan match {
      case _: LocalShuffleReaderExec => Nil
      case stage: ShuffleQueryStageExec => Seq(stage)
      case _ => plan.children.flatMap(collectShuffleStages)
    }

    val shuffleStages = collectShuffleStages(plan)
    // ShuffleExchanges introduced by repartition do not support changing the number of partitions.
    // We change the number of partitions in the stage only if all the ShuffleExchanges support it.
    if (!shuffleStages.forall(_.shuffle.canChangeNumPartitions)) {
      plan
    } else {
      val shuffleMetrics = shuffleStages.map { stage =>
        assert(stage.resultOption.isDefined, "ShuffleQueryStageExec should already be ready")
        stage.resultOption.get.asInstanceOf[MapOutputStatistics]
      }

      // `ShuffleQueryStageExec` gives null mapOutputStatistics when the input RDD has 0 partitions,
      // we should skip it when calculating the `partitionStartIndices`.
      val validMetrics = shuffleMetrics.filter(_ != null)
      // We may have different pre-shuffle partition numbers, don't reduce shuffle partition number
      // in that case. For example when we union fully aggregated data (data is arranged to a single
      // partition) and a result of a SortMergeJoin (multiple partitions).
      val distinctNumPreShufflePartitions =
        validMetrics.map(stats => stats.bytesByPartitionId.length).distinct
      if (validMetrics.nonEmpty && distinctNumPreShufflePartitions.length == 1) {
        val partitionStartIndices = estimatePartitionStartIndices(validMetrics.toArray)
        // This transformation adds new nodes, so we must use `transformUp` here.
        plan.transformUp {
          // even for shuffle exchange whose input RDD has 0 partition, we should still update its
          // `partitionStartIndices`, so that all the leaf shuffles in a stage have the same
          // number of output partitions.
          case stage: ShuffleQueryStageExec =>
            CoalescedShuffleReaderExec(stage, partitionStartIndices)
        }
      } else {
        plan
      }
    }
  }

  /**
   * Estimates partition start indices for post-shuffle partitions based on
   * mapOutputStatistics provided by all pre-shuffle stages.
   */
  // visible for testing.
  private[sql] def estimatePartitionStartIndices(
      mapOutputStatistics: Array[MapOutputStatistics]): Array[Int] = {
    val minNumPostShufflePartitions = conf.minNumPostShufflePartitions
    val advisoryTargetPostShuffleInputSize = conf.targetPostShuffleInputSize
    // If minNumPostShufflePartitions is defined, it is possible that we need to use a
    // value less than advisoryTargetPostShuffleInputSize as the target input size of
    // a post shuffle task.
    val totalPostShuffleInputSize = mapOutputStatistics.map(_.bytesByPartitionId.sum).sum
    // The max at here is to make sure that when we have an empty table, we
    // only have a single post-shuffle partition.
    // There is no particular reason that we pick 16. We just need a number to
    // prevent maxPostShuffleInputSize from being set to 0.
    val maxPostShuffleInputSize = math.max(
      math.ceil(totalPostShuffleInputSize / minNumPostShufflePartitions.toDouble).toLong, 16)
    val targetPostShuffleInputSize =
      math.min(maxPostShuffleInputSize, advisoryTargetPostShuffleInputSize)

    logInfo(
      s"advisoryTargetPostShuffleInputSize: $advisoryTargetPostShuffleInputSize, " +
        s"targetPostShuffleInputSize $targetPostShuffleInputSize.")

    // Make sure we do get the same number of pre-shuffle partitions for those stages.
    val distinctNumPreShufflePartitions =
      mapOutputStatistics.map(stats => stats.bytesByPartitionId.length).distinct
    // The reason that we are expecting a single value of the number of pre-shuffle partitions
    // is that when we add Exchanges, we set the number of pre-shuffle partitions
    // (i.e. map output partitions) using a static setting, which is the value of
    // spark.sql.shuffle.partitions. Even if two input RDDs are having different
    // number of partitions, they will have the same number of pre-shuffle partitions
    // (i.e. map output partitions).
    assert(
      distinctNumPreShufflePartitions.length == 1,
      "There should be only one distinct value of the number pre-shuffle partitions " +
        "among registered Exchange operator.")
    val numPreShufflePartitions = distinctNumPreShufflePartitions.head

    val partitionStartIndices = ArrayBuffer[Int]()
    // The first element of partitionStartIndices is always 0.
    partitionStartIndices += 0

    var postShuffleInputSize = 0L

    var i = 0
    while (i < numPreShufflePartitions) {
      // We calculate the total size of ith pre-shuffle partitions from all pre-shuffle stages.
      // Then, we add the total size to postShuffleInputSize.
      var nextShuffleInputSize = 0L
      var j = 0
      while (j < mapOutputStatistics.length) {
        nextShuffleInputSize += mapOutputStatistics(j).bytesByPartitionId(i)
        j += 1
      }

      // If including the nextShuffleInputSize would exceed the target partition size, then start a
      // new partition.
      if (i > 0 && postShuffleInputSize + nextShuffleInputSize > targetPostShuffleInputSize) {
        partitionStartIndices += i
        // reset postShuffleInputSize.
        postShuffleInputSize = nextShuffleInputSize
      } else {
        postShuffleInputSize += nextShuffleInputSize
      }

      i += 1
    }

    partitionStartIndices.toArray
  }
}

/**
 * A wrapper of shuffle query stage, which submits fewer reduce task as one reduce task may read
 * multiple shuffle partitions. This can avoid many small reduce tasks that hurt performance.
 *
 * @param child It's usually `ShuffleQueryStageExec`, but can be the shuffle exchange node during
 *              canonicalization.
 */
case class CoalescedShuffleReaderExec(
    child: SparkPlan,
    partitionStartIndices: Array[Int]) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    UnknownPartitioning(partitionStartIndices.length)
  }

  private var cachedShuffleRDD: ShuffledRowRDD = null

  override protected def doExecute(): RDD[InternalRow] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = child match {
        case stage: ShuffleQueryStageExec =>
          stage.shuffle.createShuffledRDD(Some(partitionStartIndices))
        case _ =>
          throw new IllegalStateException("operating on canonicalization plan")
      }
    }
    cachedShuffleRDD
  }
}
