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

import org.apache.spark.{MapOutputStatistics, MapOutputTrackerMaster, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

case class OptimizeSkewedPartitions(conf: SQLConf) extends Rule[SparkPlan] {

  private val supportedJoinTypes =
    Inner :: Cross :: LeftSemi :: LeftAnti :: LeftOuter :: RightOuter :: Nil

  /**
   * A partition is considered as a skewed partition if its size is larger than the median
   * partition size * spark.sql.adaptive.skewedPartitionFactor and also larger than
   * spark.sql.adaptive.skewedPartitionSizeThreshold.
   */
  private def isSkewed(
      stats: MapOutputStatistics,
      partitionId: Int,
      medianSize: Long): Boolean = {
    val size = stats.bytesByPartitionId(partitionId)
    size > medianSize * conf.getConf(SQLConf.ADAPTIVE_EXECUTION_SKEWED_PARTITION_FACTOR) &&
      size > conf.getConf(SQLConf.ADAPTIVE_EXECUTION_SKEWED_PARTITION_SIZE_THRESHOLD)
  }

  private def medianSize(stats: MapOutputStatistics): Long = {
    val numPartitions = stats.bytesByPartitionId.length
    val bytes = stats.bytesByPartitionId.sorted
    if (bytes(numPartitions / 2) > 0) bytes(numPartitions / 2) else 1
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
  private def getMapStartIndices(stage: ShuffleQueryStageExec, partitionId: Int): Array[Int] = {
    val shuffleId = stage.shuffle.shuffleDependency.shuffleHandle.shuffleId
    val mapPartitionSizes = getMapSizesForReduceId(shuffleId, partitionId)
    val maxSplits = math.min(conf.getConf(
      SQLConf.ADAPTIVE_EXECUTION_SKEWED_PARTITION_MAX_SPLITS), mapPartitionSizes.length)
    val avgPartitionSize = mapPartitionSizes.sum / maxSplits
    val advisoryPartitionSize = math.max(avgPartitionSize,
      conf.getConf(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE))
    val partitionIndices = mapPartitionSizes.indices
    val partitionStartIndices = ArrayBuffer[Int]()
    var postMapPartitionSize = mapPartitionSizes(0)
    partitionStartIndices += 0
    partitionIndices.drop(1).foreach { nextPartitionIndex =>
      val nextMapPartitionSize = mapPartitionSizes(nextPartitionIndex)
      if (postMapPartitionSize + nextMapPartitionSize > advisoryPartitionSize) {
        partitionStartIndices += nextPartitionIndex
        postMapPartitionSize = nextMapPartitionSize
      } else {
        postMapPartitionSize += nextMapPartitionSize
      }
    }

    if (partitionStartIndices.size > maxSplits) {
      partitionStartIndices.take(maxSplits).toArray
    } else partitionStartIndices.toArray
  }

  private def getStatistics(stage: ShuffleQueryStageExec): MapOutputStatistics = {
    assert(stage.resultOption.isDefined, "ShuffleQueryStageExec should" +
      " already be ready when executing OptimizeSkewedPartitions rule")
    stage.resultOption.get.asInstanceOf[MapOutputStatistics]
  }

  private def supportSplitOnLeftPartition(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
      joinType == LeftAnti || joinType == LeftOuter
  }

  private def supportSplitOnRightPartition(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }

  private def getNumMappers(stage: ShuffleQueryStageExec): Int = {
    stage.shuffle.shuffleDependency.rdd.partitions.length
  }

  def handleSkewJoin(plan: SparkPlan): SparkPlan = plan.transformUp {
    case smj @ SortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
      SortExec(_, _, left: ShuffleQueryStageExec, _),
      SortExec(_, _, right: ShuffleQueryStageExec, _))
      if supportedJoinTypes.contains(joinType) =>
      val leftStats = getStatistics(left)
      val rightStats = getStatistics(right)
      val numPartitions = leftStats.bytesByPartitionId.length

      val leftMedSize = medianSize(leftStats)
      val rightMedSize = medianSize(rightStats)
      val leftSizeInfo = s"median size: $leftMedSize, max size: ${leftStats.bytesByPartitionId.max}"
      val rightSizeInfo = s"median size: $rightMedSize," +
        s" max size: ${rightStats.bytesByPartitionId.max}"
      logDebug(
        s"""
          |Try to optimize skewed join.
          |Left side partition size: $leftSizeInfo
          |Right side partition size: $rightSizeInfo
        """.stripMargin)

      val skewedPartitions = mutable.HashSet[Int]()
      val subJoins = mutable.ArrayBuffer[SparkPlan]()
      for (partitionId <- 0 until numPartitions) {
        val isLeftSkew = isSkewed(leftStats, partitionId, leftMedSize)
        val isRightSkew = isSkewed(rightStats, partitionId, rightMedSize)
        val leftMapIdStartIndices = if (isLeftSkew && supportSplitOnLeftPartition(joinType)) {
          getMapStartIndices(left, partitionId)
        } else {
          Array(0)
        }
        val rightMapIdStartIndices = if (isRightSkew && supportSplitOnRightPartition(joinType)) {
          getMapStartIndices(right, partitionId)
        } else {
          Array(0)
        }

        if (leftMapIdStartIndices.length > 1 || rightMapIdStartIndices.length > 1) {
          skewedPartitions += partitionId
          for (i <- 0 until leftMapIdStartIndices.length;
               j <- 0 until rightMapIdStartIndices.length) {
            val leftEndMapId = if (i == leftMapIdStartIndices.length - 1) {
              getNumMappers(left)
            } else {
              leftMapIdStartIndices(i + 1)
            }
            val rightEndMapId = if (j == rightMapIdStartIndices.length - 1) {
              getNumMappers(right)
            } else {
              rightMapIdStartIndices(j + 1)
            }
            // TODO: we may can optimize the sort merge join to broad cast join after
            //       obtaining the raw data size of per partition,
            val leftSkewedReader = SkewedShufflePartitionReader(
              left, partitionId, leftMapIdStartIndices(i), leftEndMapId)

            val rightSkewedReader = SkewedShufflePartitionReader(right, partitionId,
                rightMapIdStartIndices(j), rightEndMapId)
            subJoins += SortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
              leftSkewedReader, rightSkewedReader)
          }
        }
      }
      logDebug(s"number of skewed partitions is ${skewedPartitions.size}")
      if (skewedPartitions.nonEmpty) {
        val optimizedSmj = smj.transformDown {
          case sort @ SortExec(_, _, shuffleStage: ShuffleQueryStageExec, _) =>
            val newStage = shuffleStage.copy(
              excludedPartitions = skewedPartitions.toSet)
            newStage.resultOption = shuffleStage.resultOption
            sort.copy(child = newStage)
        }
        subJoins += optimizedSmj
        UnionExec(subJoins)
      } else {
        smj
      }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.ADAPTIVE_EXECUTION_SKEWED_JOIN_ENABLED)) {
      return plan
    }

    def collectShuffleStages(plan: SparkPlan): Seq[ShuffleQueryStageExec] = plan match {
      case _: LocalShuffleReaderExec => Nil
      case _: CoalescedShuffleReaderExec => Nil
      case stage: ShuffleQueryStageExec => Seq(stage)
      case _ => plan.children.flatMap(collectShuffleStages)
    }

    val shuffleStages = collectShuffleStages(plan)

    if (shuffleStages.length == 2) {
      // Currently we only support handling skewed join for 2 table join.
      handleSkewJoin(plan)
    } else {
      plan

    }
  }
}

/**
 * A wrapper of shuffle query stage, which submits one reduce task to read a single
 * shuffle partition 'partitionIndex' produced by the mappers in range [startMapIndex, endMapIndex).
 * This is used to handle the skewed partitions.
 *
 * @param child It's usually `ShuffleQueryStageExec`, but can be the shuffle exchange
 *              node during canonicalization.
 * @param partitionIndex The pre shuffle partition index.
 * @param startMapIndex The start map index.
 * @param endMapIndex The end map index.
 */
case class SkewedShufflePartitionReader(
    child: QueryStageExec,
    partitionIndex: Int,
    startMapIndex: Int,
    endMapIndex: Int) extends LeafExecNode {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    UnknownPartitioning(1)
  }
  private var cachedSkewedShuffleRDD: SkewedShuffledRowRDD = null

  override def doExecute(): RDD[InternalRow] = {
    if (cachedSkewedShuffleRDD == null) {
      cachedSkewedShuffleRDD = child match {
        case stage: ShuffleQueryStageExec =>
          stage.shuffle.createSkewedShuffleRDD(partitionIndex, startMapIndex, endMapIndex)
        case _ =>
          throw new IllegalStateException("operating on canonicalization plan")
      }
    }
    cachedSkewedShuffleRDD
  }
}
