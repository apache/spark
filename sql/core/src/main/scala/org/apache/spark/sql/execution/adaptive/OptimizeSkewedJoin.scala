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

import org.apache.commons.io.FileUtils

import org.apache.spark.{MapOutputStatistics, MapOutputTrackerMaster, SparkEnv}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, ShuffleExchangeExec, ShuffleOrigin}
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
 * `CoalesceShufflePartitions` does.
 */
object OptimizeSkewedJoin extends CustomShuffleReaderRule {

  override val supportedShuffleOrigins: Seq[ShuffleOrigin] = Seq(ENSURE_REQUIREMENTS)

  private val ensureRequirements = EnsureRequirements

  private val supportedJoinTypes =
    Inner :: Cross :: LeftSemi :: LeftAnti :: LeftOuter :: RightOuter :: Nil

  /**
   * A partition is considered as a skewed partition if its size is larger than the median
   * partition size * SKEW_JOIN_SKEWED_PARTITION_FACTOR and also larger than
   * SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.
   */
  private def isSkewed(size: Long, medianSize: Long): Boolean = {
    size > medianSize * conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR) &&
      size > conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD)
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
   * advisory partition size if avg size is smaller than it.
   */
  private def targetSize(sizes: Seq[Long], medianSize: Long): Long = {
    val advisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    val nonSkewSizes = sizes.filterNot(isSkewed(_, medianSize))
    if (nonSkewSizes.isEmpty) {
      advisorySize
    } else {
      math.max(advisorySize, nonSkewSizes.sum / nonSkewSizes.length)
    }
  }

  /**
   * Get the map size of the specific reduce shuffle Id.
   */
  private def getMapSizesForReduceId(shuffleId: Int, partitionId: Int): Array[Long] = {
    val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTracker.shuffleStatuses(shuffleId).mapStatuses.map{_.getSizeForBlock(partitionId)}
  }

  /**
   * Splits the skewed partition based on the map size and the target partition size
   * after split, and create a list of `PartialMapperPartitionSpec`. Returns None if can't split.
   */
  private def createSkewPartitionSpecs(
      shuffleId: Int,
      reducerId: Int,
      targetSize: Long): Option[Seq[PartialReducerPartitionSpec]] = {
    val mapPartitionSizes = getMapSizesForReduceId(shuffleId, reducerId)
    val mapStartIndices = ShufflePartitionsUtil.splitSizeListByTargetSize(
      mapPartitionSizes, targetSize)
    if (mapStartIndices.length > 1) {
      Some(mapStartIndices.indices.map { i =>
        val startMapIndex = mapStartIndices(i)
        val endMapIndex = if (i == mapStartIndices.length - 1) {
          mapPartitionSizes.length
        } else {
          mapStartIndices(i + 1)
        }
        val dataSize = startMapIndex.until(endMapIndex).map(mapPartitionSizes(_)).sum
        PartialReducerPartitionSpec(reducerId, startMapIndex, endMapIndex, dataSize)
      })
    } else {
      None
    }
  }

  private def canSplitLeftSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
      joinType == LeftAnti || joinType == LeftOuter
  }

  private def canSplitRightSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }

  private def getSizeInfo(medianSize: Long, sizes: Seq[Long]): String = {
    s"median size: $medianSize, max size: ${sizes.max}, min size: ${sizes.min}, avg size: " +
      sizes.sum / sizes.length
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
        s1 @ SortExec(_, _, ShuffleStage(left: ShuffleStageInfo), _),
        s2 @ SortExec(_, _, ShuffleStage(right: ShuffleStageInfo), _), _)
        if supportedJoinTypes.contains(joinType) =>
      assert(left.partitionsWithSizes.length == right.partitionsWithSizes.length)
      val numPartitions = left.partitionsWithSizes.length
      // We use the median size of the original shuffle partitions to detect skewed partitions.
      val leftMedSize = medianSize(left.mapStats)
      val rightMedSize = medianSize(right.mapStats)
      logDebug(
        s"""
          |Optimizing skewed join.
          |Left side partitions size info:
          |${getSizeInfo(leftMedSize, left.mapStats.bytesByPartitionId)}
          |Right side partitions size info:
          |${getSizeInfo(rightMedSize, right.mapStats.bytesByPartitionId)}
        """.stripMargin)
      val canSplitLeft = canSplitLeftSide(joinType)
      val canSplitRight = canSplitRightSide(joinType)
      // We use the actual partition sizes (may be coalesced) to calculate target size, so that
      // the final data distribution is even (coalesced partitions + split partitions).
      val leftActualSizes = left.partitionsWithSizes.map(_._2)
      val rightActualSizes = right.partitionsWithSizes.map(_._2)
      val leftTargetSize = targetSize(leftActualSizes, leftMedSize)
      val rightTargetSize = targetSize(rightActualSizes, rightMedSize)

      val leftSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
      val rightSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
      var numSkewedLeft = 0
      var numSkewedRight = 0
      for (partitionIndex <- 0 until numPartitions) {
        val leftActualSize = leftActualSizes(partitionIndex)
        val isLeftSkew = isSkewed(leftActualSize, leftMedSize) && canSplitLeft
        val leftPartSpec = left.partitionsWithSizes(partitionIndex)._1
        val isLeftCoalesced = leftPartSpec.startReducerIndex + 1 < leftPartSpec.endReducerIndex

        val rightActualSize = rightActualSizes(partitionIndex)
        val isRightSkew = isSkewed(rightActualSize, rightMedSize) && canSplitRight
        val rightPartSpec = right.partitionsWithSizes(partitionIndex)._1
        val isRightCoalesced = rightPartSpec.startReducerIndex + 1 < rightPartSpec.endReducerIndex

        // A skewed partition should never be coalesced, but skip it here just to be safe.
        val leftParts = if (isLeftSkew && !isLeftCoalesced) {
          val reducerId = leftPartSpec.startReducerIndex
          val skewSpecs = createSkewPartitionSpecs(
            left.mapStats.shuffleId, reducerId, leftTargetSize)
          if (skewSpecs.isDefined) {
            logDebug(s"Left side partition $partitionIndex " +
              s"(${FileUtils.byteCountToDisplaySize(leftActualSize)}) is skewed, " +
              s"split it into ${skewSpecs.get.length} parts.")
            numSkewedLeft += 1
          }
          skewSpecs.getOrElse(Seq(leftPartSpec))
        } else {
          Seq(leftPartSpec)
        }

        // A skewed partition should never be coalesced, but skip it here just to be safe.
        val rightParts = if (isRightSkew && !isRightCoalesced) {
          val reducerId = rightPartSpec.startReducerIndex
          val skewSpecs = createSkewPartitionSpecs(
            right.mapStats.shuffleId, reducerId, rightTargetSize)
          if (skewSpecs.isDefined) {
            logDebug(s"Right side partition $partitionIndex " +
              s"(${FileUtils.byteCountToDisplaySize(rightActualSize)}) is skewed, " +
              s"split it into ${skewSpecs.get.length} parts.")
            numSkewedRight += 1
          }
          skewSpecs.getOrElse(Seq(rightPartSpec))
        } else {
          Seq(rightPartSpec)
        }

        for {
          leftSidePartition <- leftParts
          rightSidePartition <- rightParts
        } {
          leftSidePartitions += leftSidePartition
          rightSidePartitions += rightSidePartition
        }
      }

      logDebug(s"number of skewed partitions: left $numSkewedLeft, right $numSkewedRight")
      if (numSkewedLeft > 0 || numSkewedRight > 0) {
        val newLeft = CustomShuffleReaderExec(left.shuffleStage, leftSidePartitions.toSeq)
        val newRight = CustomShuffleReaderExec(right.shuffleStage, rightSidePartitions.toSeq)
        smj.copy(
          left = s1.copy(child = newLeft), right = s2.copy(child = newRight), isSkewJoin = true)
      } else {
        smj
      }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.SKEW_JOIN_ENABLED)) {
      return plan
    }

    def collectShuffleStages(plan: SparkPlan): Seq[ShuffleQueryStageExec] = plan match {
      case stage: ShuffleQueryStageExec => Seq(stage)
      case _ => plan.children.flatMap(collectShuffleStages)
    }

    val shuffleStages = collectShuffleStages(plan)

    if (shuffleStages.length == 2) {
      // When multi table join, there will be too many complex combination to consider.
      // Currently we only handle 2 table join like following use case.
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

private object ShuffleStage {
  def unapply(plan: SparkPlan): Option[ShuffleStageInfo] = plan match {
    case s: ShuffleQueryStageExec
        if s.mapStats.isDefined &&
          OptimizeSkewedJoin.supportedShuffleOrigins.contains(s.shuffle.shuffleOrigin) =>
      val mapStats = s.mapStats.get
      val sizes = mapStats.bytesByPartitionId
      val partitions = sizes.zipWithIndex.map {
        case (size, i) => CoalescedPartitionSpec(i, i + 1) -> size
      }
      Some(ShuffleStageInfo(s, mapStats, partitions))

    case CustomShuffleReaderExec(s: ShuffleQueryStageExec, partitionSpecs)
        if s.mapStats.isDefined && partitionSpecs.nonEmpty &&
          OptimizeSkewedJoin.supportedShuffleOrigins.contains(s.shuffle.shuffleOrigin) =>
      val mapStats = s.mapStats.get
      val sizes = mapStats.bytesByPartitionId
      val partitions = partitionSpecs.map {
        case spec @ CoalescedPartitionSpec(start, end) =>
          var sum = 0L
          var i = start
          while (i < end) {
            sum += sizes(i)
            i += 1
          }
          spec -> sum
        case other => throw new IllegalArgumentException(
          s"Expect CoalescedPartitionSpec but got $other")
      }
      Some(ShuffleStageInfo(s, mapStats, partitions))

    case _ => None
  }
}

private case class ShuffleStageInfo(
    shuffleStage: ShuffleQueryStageExec,
    mapStats: MapOutputStatistics,
    partitionsWithSizes: Seq[(CoalescedPartitionSpec, Long)])
