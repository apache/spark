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
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.io.FileUtils

import org.apache.spark.{MapOutputStatistics, MapOutputTrackerMaster, SparkEnv}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule to optimize skewed joins to avoid straggler tasks whose share of data are significantly
 * larger than those of the rest of the tasks.
 *
 * The general idea is to divide each skew partition into smaller partitions and replicate its
 * matching partition on the other side of the join so that they can run in parallel tasks.
 * Note that when matching partitions from the left side and the right side both have skew,
 * it will become a cartesian product of splits from left and right joining together.
 *
 * For example, assume the Sort-Merge join has 4 partitions:
 * left:  [L1, L2, L3, L4]
 * right: [R1, R2, R3, R4]
 *
 * Let's say L2, L4 and R3, R4 are skewed, and each of them get split into 2 sub-partitions. This
 * is scheduled to run 4 tasks at the beginning: (L1, R1), (L2, R2), (L3, R3), (L4, R4).
 * This rule expands it to 9 tasks to increase parallelism:
 * (L1, R1),
 * (L2-1, R2), (L2-2, R2),
 * (L3, R3-1), (L3, R3-2),
 * (L4-1, R4-1), (L4-2, R4-1), (L4-1, R4-2), (L4-2, R4-2)
 *
 * Note that, when this rule is enabled, it also coalesces non-skewed partitions like
 * `ReduceNumShufflePartitions` does.
 */
case class OptimizeSkewedJoin(conf: SQLConf) extends Rule[SparkPlan] {

  private val ensureRequirements = EnsureRequirements(conf)

  private val supportedJoinTypes =
    Inner :: Cross :: LeftSemi :: LeftAnti :: LeftOuter :: RightOuter :: Nil

  /**
   * A partition is considered as a skewed partition if its size is larger than the median
   * partition size * ADAPTIVE_EXECUTION_SKEWED_PARTITION_FACTOR and also larger than
   * SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.
   */
  private def isSkewed(size: Long, medianSize: Long): Boolean = {
    size > medianSize * conf.getConf(SQLConf.ADAPTIVE_EXECUTION_SKEWED_PARTITION_FACTOR) &&
      size > conf.getConf(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE)
  }

  private def medianSize(stats: MapOutputStatistics): Long = {
    val numPartitions = stats.bytesByPartitionId.length
    val bytes = stats.bytesByPartitionId.sorted
    numPartitions match {
      case _ if (numPartitions % 2 == 0) =>
        math.max((bytes(numPartitions / 2) + bytes(numPartitions / 2 - 1)) / 2, 1)
      case _ => math.max(bytes(numPartitions / 2), 1)
    }
  }

  /**
   * The goal of skew join optimization is to make the data distribution more even. The target size
   * to split skewed partitions is the average size of non-skewed partition, or the
   * target post-shuffle partition size if avg size is smaller than it.
   */
  private def targetSize(stats: MapOutputStatistics, medianSize: Long): Long = {
    val targetPostShuffleSize = conf.getConf(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE)
    val nonSkewSizes = stats.bytesByPartitionId.filterNot(isSkewed(_, medianSize))
    // It's impossible that all the partitions are skewed, as we use median size to define skew.
    assert(nonSkewSizes.nonEmpty)
    math.max(targetPostShuffleSize, nonSkewSizes.sum / nonSkewSizes.length)
  }

  /**
   * Get the map size of the specific reduce shuffle Id.
   */
  private def getMapSizesForReduceId(shuffleId: Int, partitionId: Int): Array[Long] = {
    val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTracker.shuffleStatuses(shuffleId).mapStatuses.map{_.getSizeForBlock(partitionId)}
  }

  /**
   * Split the skewed partition based on the map size and the max split number.
   */
  private def getMapStartIndices(
      stage: ShuffleQueryStageExec,
      partitionId: Int,
      targetSize: Long): Array[Int] = {
    val shuffleId = stage.shuffle.shuffleDependency.shuffleHandle.shuffleId
    val mapPartitionSizes = getMapSizesForReduceId(shuffleId, partitionId)
    val partitionStartIndices = ArrayBuffer[Int]()
    partitionStartIndices += 0
    var i = 0
    var postMapPartitionSize = 0L
    while (i < mapPartitionSizes.length) {
      val nextMapPartitionSize = mapPartitionSizes(i)
      if (i > 0 && postMapPartitionSize + nextMapPartitionSize > targetSize) {
        partitionStartIndices += i
        postMapPartitionSize = nextMapPartitionSize
      } else {
        postMapPartitionSize += nextMapPartitionSize
      }
      i += 1
    }

    partitionStartIndices.toArray
  }

  private def getStatistics(stage: ShuffleQueryStageExec): MapOutputStatistics = {
    assert(stage.resultOption.isDefined, "ShuffleQueryStageExec should" +
      " already be ready when executing OptimizeSkewedPartitions rule")
    stage.resultOption.get.asInstanceOf[MapOutputStatistics]
  }

  private def canSplitLeftSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
      joinType == LeftAnti || joinType == LeftOuter
  }

  private def canSplitRightSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }

  private def getNumMappers(stage: ShuffleQueryStageExec): Int = {
    stage.shuffle.shuffleDependency.rdd.partitions.length
  }

  private def getSizeInfo(medianSize: Long, maxSize: Long): String = {
    s"median size: $medianSize, max size: ${maxSize}"
  }

  /*
   * This method aim to optimize the skewed join with the following steps:
   * 1. Check whether the shuffle partition is skewed based on the median size
   *    and the skewed partition threshold in origin smj.
   * 2. Assuming partition0 is skewed in left side, and it has 5 mappers (Map0, Map1...Map4).
   *    And we may split the 5 Mappers into 3 mapper ranges [(Map0, Map1), (Map2, Map3), (Map4)]
   *    based on the map size and the max split number.
   * 3. Wrap the join left child with a special shuffle reader that reads each mapper range with one
   *    task, so total 3 tasks.
   * 4. Wrap the join right child with a special shuffle reader that reads partition0 3 times by
   *    3 tasks separately.
   */
  def optimizeSkewJoin(plan: SparkPlan): SparkPlan = plan.transformUp {
    case smj @ SortMergeJoinExec(_, _, joinType, _,
        s1 @ SortExec(_, _, left: ShuffleQueryStageExec, _),
        s2 @ SortExec(_, _, right: ShuffleQueryStageExec, _), _)
        if supportedJoinTypes.contains(joinType) =>
      val leftStats = getStatistics(left)
      val rightStats = getStatistics(right)
      val numPartitions = leftStats.bytesByPartitionId.length

      val leftMedSize = medianSize(leftStats)
      val rightMedSize = medianSize(rightStats)
      logDebug(
        s"""
          |Try to optimize skewed join.
          |Left side partition size:
          |${getSizeInfo(leftMedSize, leftStats.bytesByPartitionId.max)}
          |Right side partition size:
          |${getSizeInfo(rightMedSize, rightStats.bytesByPartitionId.max)}
        """.stripMargin)
      val canSplitLeft = canSplitLeftSide(joinType)
      val canSplitRight = canSplitRightSide(joinType)
      val leftTargetSize = targetSize(leftStats, leftMedSize)
      val rightTargetSize = targetSize(rightStats, rightMedSize)

      val leftSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
      val rightSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
      // This is used to delay the creation of non-skew partitions so that we can potentially
      // coalesce them like `ReduceNumShufflePartitions` does.
      val nonSkewPartitionIndices = mutable.ArrayBuffer.empty[Int]
      val leftSkewDesc = new SkewDesc
      val rightSkewDesc = new SkewDesc
      for (partitionIndex <- 0 until numPartitions) {
        val leftSize = leftStats.bytesByPartitionId(partitionIndex)
        val isLeftSkew = isSkewed(leftSize, leftMedSize) && canSplitLeft
        val rightSize = rightStats.bytesByPartitionId(partitionIndex)
        val isRightSkew = isSkewed(rightSize, rightMedSize) && canSplitRight
        if (isLeftSkew || isRightSkew) {
          if (nonSkewPartitionIndices.nonEmpty) {
            // As soon as we see a skew, we'll "flush" out unhandled non-skew partitions.
            createNonSkewPartitions(leftStats, rightStats, nonSkewPartitionIndices).foreach { p =>
              leftSidePartitions += p
              rightSidePartitions += p
            }
            nonSkewPartitionIndices.clear()
          }

          val leftParts = if (isLeftSkew) {
            leftSkewDesc.addPartitionSize(leftSize)
            createSkewPartitions(
              partitionIndex,
              getMapStartIndices(left, partitionIndex, leftTargetSize),
              getNumMappers(left))
          } else {
            Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1))
          }

          val rightParts = if (isRightSkew) {
            rightSkewDesc.addPartitionSize(rightSize)
            createSkewPartitions(
              partitionIndex,
              getMapStartIndices(right, partitionIndex, rightTargetSize),
              getNumMappers(right))
          } else {
            Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1))
          }

          for {
            leftSidePartition <- leftParts
            rightSidePartition <- rightParts
          } {
            leftSidePartitions += leftSidePartition
            rightSidePartitions += rightSidePartition
          }
        } else {
          // Add to `nonSkewPartitionIndices` first, and add real partitions later, in case we can
          // coalesce the non-skew partitions.
          nonSkewPartitionIndices += partitionIndex
          // If this is the last partition, add real partition immediately.
          if (partitionIndex == numPartitions - 1) {
            createNonSkewPartitions(leftStats, rightStats, nonSkewPartitionIndices).foreach { p =>
              leftSidePartitions += p
              rightSidePartitions += p
            }
            nonSkewPartitionIndices.clear()
          }
        }
      }

      logDebug("number of skewed partitions: " +
        s"left ${leftSkewDesc.numPartitions}, right ${rightSkewDesc.numPartitions}")
      if (leftSkewDesc.numPartitions > 0 || rightSkewDesc.numPartitions > 0) {
        val newLeft = CustomShuffleReaderExec(
          left, leftSidePartitions.toArray, leftSkewDesc.toString)
        val newRight = CustomShuffleReaderExec(
          right, rightSidePartitions.toArray, rightSkewDesc.toString)
        smj.copy(
          left = s1.copy(child = newLeft), right = s2.copy(child = newRight), isSkewJoin = true)
      } else {
        smj
      }
  }

  private def createNonSkewPartitions(
      leftStats: MapOutputStatistics,
      rightStats: MapOutputStatistics,
      nonSkewPartitionIndices: Seq[Int]): Seq[ShufflePartitionSpec] = {
    assert(nonSkewPartitionIndices.nonEmpty)
    val shouldCoalesce = conf.getConf(SQLConf.REDUCE_POST_SHUFFLE_PARTITIONS_ENABLED)
    if (!shouldCoalesce || nonSkewPartitionIndices.length == 1) {
      nonSkewPartitionIndices.map(i => CoalescedPartitionSpec(i, i + 1))
    } else {
      ShufflePartitionsCoalescer.coalescePartitions(
        Array(leftStats, rightStats),
        firstPartitionIndex = nonSkewPartitionIndices.head,
        // `lastPartitionIndex` is exclusive.
        lastPartitionIndex = nonSkewPartitionIndices.last + 1,
        advisoryTargetSize = conf.targetPostShuffleInputSize)
    }
  }

  private def createSkewPartitions(
      reducerIndex: Int,
      mapStartIndices: Array[Int],
      numMappers: Int): Seq[PartialReducerPartitionSpec] = {
    mapStartIndices.indices.map { i =>
      val startMapIndex = mapStartIndices(i)
      val endMapIndex = if (i == mapStartIndices.length - 1) {
        numMappers
      } else {
        mapStartIndices(i + 1)
      }
      PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.ADAPTIVE_EXECUTION_SKEWED_JOIN_ENABLED)) {
      return plan
    }

    def collectShuffleStages(plan: SparkPlan): Seq[ShuffleQueryStageExec] = plan match {
      case stage: ShuffleQueryStageExec => Seq(stage)
      case _ => plan.children.flatMap(collectShuffleStages)
    }

    val shuffleStages = collectShuffleStages(plan)

    if (shuffleStages.length == 2) {
      // When multi table join, there will be too many complex combination to consider.
      // Currently we only handle 2 table join like following two use cases.
      // SMJ
      //   Sort
      //     Shuffle
      //   Sort
      //     Shuffle
      val optimizePlan = optimizeSkewJoin(plan)
      val numShuffles = ensureRequirements.apply(optimizePlan).collect {
        case e: ShuffleExchangeExec => e
      }.length

      if (numShuffles > 0) {
        logDebug("OptimizeSkewedJoin rule is not applied due" +
          " to additional shuffles will be introduced.")
        plan
      } else {
        optimizePlan
      }
    } else {
      plan
    }
  }
}

private class SkewDesc {
  private[this] var numSkewedPartitions: Int = 0
  private[this] var totalSize: Long = 0
  private[this] var maxSize: Long = 0
  private[this] var minSize: Long = 0

  def numPartitions: Int = numSkewedPartitions

  def addPartitionSize(size: Long): Unit = {
    if (numSkewedPartitions == 0) {
      maxSize = size
      minSize = size
    }
    numSkewedPartitions += 1
    totalSize += size
    if (size > maxSize) maxSize = size
    if (size < minSize) minSize = size
  }

  override def toString: String = {
    if (numSkewedPartitions == 0) {
      "no skewed partition"
    } else {
      val maxSizeStr = FileUtils.byteCountToDisplaySize(maxSize)
      val minSizeStr = FileUtils.byteCountToDisplaySize(minSize)
      val avgSizeStr = FileUtils.byteCountToDisplaySize(totalSize / numSkewedPartitions)
      s"$numSkewedPartitions skewed partitions with " +
        s"size(max=$maxSizeStr, min=$minSizeStr, avg=$avgSizeStr)"
    }
  }
}
