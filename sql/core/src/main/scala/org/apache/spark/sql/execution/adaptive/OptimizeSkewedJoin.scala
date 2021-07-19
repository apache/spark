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

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleOrigin}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
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
 */
object OptimizeSkewedJoin extends CustomShuffleReaderRule {

  override val supportedShuffleOrigins: Seq[ShuffleOrigin] = Seq(ENSURE_REQUIREMENTS)

  override def mayAddExtraShuffles: Boolean = true

  /**
   * A partition is considered as a skewed partition if its size is larger than the median
   * partition size * SKEW_JOIN_SKEWED_PARTITION_FACTOR and also larger than
   * SKEW_JOIN_SKEWED_PARTITION_THRESHOLD. Thus we pick the larger one as the skew threshold.
   */
  def getSkewThreshold(medianSize: Long): Long = {
    conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD).max(
      medianSize * conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR))
  }

  private def medianSize(sizes: Array[Long]): Long = {
    val numPartitions = sizes.length
    val bytes = sizes.sorted
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
  private def targetSize(sizes: Array[Long], skewThreshold: Long): Long = {
    val advisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    val nonSkewSizes = sizes.filter(_ <= skewThreshold)
    if (nonSkewSizes.isEmpty) {
      advisorySize
    } else {
      math.max(advisorySize, nonSkewSizes.sum / nonSkewSizes.length)
    }
  }

  private def canSplitLeftSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
      joinType == LeftAnti || joinType == LeftOuter
  }

  private def canSplitRightSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }

  private def getSizeInfo(medianSize: Long, sizes: Array[Long]): String = {
    s"median size: $medianSize, max size: ${sizes.max}, min size: ${sizes.min}, avg size: " +
      sizes.sum / sizes.length
  }

  /*
   * This method aim to optimize the skewed join with the following steps:
   * 1. Check whether the shuffle partition is skewed based on the median size
   *    and the skewed partition threshold in origin shuffled join (smj and shj).
   * 2. Assuming partition0 is skewed in left side, and it has 5 mappers (Map0, Map1...Map4).
   *    And we may split the 5 Mappers into 3 mapper ranges [(Map0, Map1), (Map2, Map3), (Map4)]
   *    based on the map size and the max split number.
   * 3. Wrap the join left child with a special shuffle reader that reads each mapper range with one
   *    task, so total 3 tasks.
   * 4. Wrap the join right child with a special shuffle reader that reads partition0 3 times by
   *    3 tasks separately.
   */
  private def tryOptimizeJoinChildren(
      left: ShuffleQueryStageExec,
      right: ShuffleQueryStageExec,
      joinType: JoinType): Option[(SparkPlan, SparkPlan)] = {
    val canSplitLeft = canSplitLeftSide(joinType)
    val canSplitRight = canSplitRightSide(joinType)
    if (!canSplitLeft && !canSplitRight) return None

    val leftSizes = left.mapStats.get.bytesByPartitionId
    val rightSizes = right.mapStats.get.bytesByPartitionId
    assert(leftSizes.length == rightSizes.length)
    val numPartitions = leftSizes.length
    // We use the median size of the original shuffle partitions to detect skewed partitions.
    val leftMedSize = medianSize(leftSizes)
    val rightMedSize = medianSize(rightSizes)
    logDebug(
      s"""
         |Optimizing skewed join.
         |Left side partitions size info:
         |${getSizeInfo(leftMedSize, leftSizes)}
         |Right side partitions size info:
         |${getSizeInfo(rightMedSize, rightSizes)}
      """.stripMargin)

    val leftSkewThreshold = getSkewThreshold(leftMedSize)
    val rightSkewThreshold = getSkewThreshold(rightMedSize)
    val leftTargetSize = targetSize(leftSizes, leftSkewThreshold)
    val rightTargetSize = targetSize(rightSizes, rightSkewThreshold)

    val leftSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    val rightSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    var numSkewedLeft = 0
    var numSkewedRight = 0
    for (partitionIndex <- 0 until numPartitions) {
      val leftSize = leftSizes(partitionIndex)
      val isLeftSkew = canSplitLeft && leftSize > leftSkewThreshold
      val rightSize = rightSizes(partitionIndex)
      val isRightSkew = canSplitRight && rightSize > rightSkewThreshold
      val leftNoSkewPartitionSpec =
        Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, leftSize))
      val rightNoSkewPartitionSpec =
        Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, rightSize))

      val leftParts = if (isLeftSkew) {
        val skewSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
          left.mapStats.get.shuffleId, partitionIndex, leftTargetSize)
        if (skewSpecs.isDefined) {
          logDebug(s"Left side partition $partitionIndex " +
            s"(${FileUtils.byteCountToDisplaySize(leftSize)}) is skewed, " +
            s"split it into ${skewSpecs.get.length} parts.")
          numSkewedLeft += 1
        }
        skewSpecs.getOrElse(leftNoSkewPartitionSpec)
      } else {
        leftNoSkewPartitionSpec
      }

      val rightParts = if (isRightSkew) {
        val skewSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
          right.mapStats.get.shuffleId, partitionIndex, rightTargetSize)
        if (skewSpecs.isDefined) {
          logDebug(s"Right side partition $partitionIndex " +
            s"(${FileUtils.byteCountToDisplaySize(rightSize)}) is skewed, " +
            s"split it into ${skewSpecs.get.length} parts.")
          numSkewedRight += 1
        }
        skewSpecs.getOrElse(rightNoSkewPartitionSpec)
      } else {
        rightNoSkewPartitionSpec
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
      Some((CustomShuffleReaderExec(left, leftSidePartitions.toSeq),
        CustomShuffleReaderExec(right, rightSidePartitions.toSeq)))
    } else {
      None
    }
  }

  def optimizeSkewJoin(plan: SparkPlan): SparkPlan = plan.transformUp {
    case smj @ SortMergeJoinExec(_, _, joinType, _,
        s1 @ SortExec(_, _, ShuffleStage(left: ShuffleQueryStageExec), _),
        s2 @ SortExec(_, _, ShuffleStage(right: ShuffleQueryStageExec), _), false) =>
      val newChildren = tryOptimizeJoinChildren(left, right, joinType)
      if (newChildren.isDefined) {
        val (newLeft, newRight) = newChildren.get
        smj.copy(
          left = s1.copy(child = newLeft), right = s2.copy(child = newRight), isSkewJoin = true)
      } else {
        smj
      }

    case shj @ ShuffledHashJoinExec(_, _, joinType, _, _,
        ShuffleStage(left: ShuffleQueryStageExec),
        ShuffleStage(right: ShuffleQueryStageExec), false) =>
      val newChildren = tryOptimizeJoinChildren(left, right, joinType)
      if (newChildren.isDefined) {
        val (newLeft, newRight) = newChildren.get
        shj.copy(left = newLeft, right = newRight, isSkewJoin = true)
      } else {
        shj
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
      // Or
      // SHJ
      //   Shuffle
      //   Shuffle
      optimizeSkewJoin(plan)
    } else {
      plan
    }
  }
}

private object ShuffleStage {
  def unapply(plan: SparkPlan): Option[ShuffleQueryStageExec] = plan match {
    case s: ShuffleQueryStageExec if s.mapStats.isDefined &&
        OptimizeSkewedJoin.supportedShuffleOrigins.contains(s.shuffle.shuffleOrigin) =>
      Some(s)
    case _ => None
  }
}
