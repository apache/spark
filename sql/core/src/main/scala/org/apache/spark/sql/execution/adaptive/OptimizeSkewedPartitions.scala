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
import scala.concurrent.duration.Duration

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
import org.apache.spark.util.ThreadUtils

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
   * Get all the map data size for specific reduce partitionId.
   */
  def getMapSizeForSpecificPartition(partitionId: Int, shuffleId: Int): Array[Long] = {
    val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTracker.shuffleStatuses.get(shuffleId).
      get.mapStatuses.map{_.getSizeForBlock(partitionId)}
  }

  /**
   * Split the partition into the number of mappers. Each split read data from each mapper.
   */
  private def estimateMapStartIndices(
      stage: QueryStageExec,
      partitionId: Int,
      medianSize: Long): Array[Int] = {
    val dependency = ShuffleQueryStageExec.getShuffleStage(stage).plan.shuffleDependency
    val numMappers = dependency.rdd.partitions.length
    // TODO: split the partition based on the size
    (0 until numMappers).toArray
  }

  private def getStatistics(queryStage: QueryStageExec): MapOutputStatistics = {
    val shuffleStage = ShuffleQueryStageExec.getShuffleStage(queryStage)
    val metrics = shuffleStage.plan.mapOutputStatisticsFuture
    assert(metrics.isCompleted,
      "ShuffleQueryStageExec should already be ready when executing OptimizeSkewedPartitions rule")
    ThreadUtils.awaitResult(metrics, Duration.Zero)
  }

  /**
   * Base optimization support check: the join type is supported.
   * Note that for some join types(like left outer), whether a certain partition can be optimized
   * also depends on the filed isSkewAndSupportsSplit.
   */
  private def supportOptimization(
      joinType: JoinType,
      leftStage: QueryStageExec,
      rightStage: QueryStageExec): Boolean = {
    val joinTypeSupported = supportedJoinTypes.contains(joinType)
    val shuffleStageCheck = ShuffleQueryStageExec.isShuffleQueryStageExec(leftStage) &&
      ShuffleQueryStageExec.isShuffleQueryStageExec(rightStage)
    joinTypeSupported && shuffleStageCheck
  }

  private def supportSplitOnLeftPartition(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
      joinType == LeftAnti || joinType == LeftOuter
  }

  private def supportSplitOnRightPartition(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }

  private def getMappersNum(stage: QueryStageExec): Int = {
    ShuffleQueryStageExec.getShuffleStage(stage).plan.shuffleDependency.rdd.partitions.length
  }

  def handleSkewJoin(plan: SparkPlan): SparkPlan = plan.transformUp {
    case smj @ SortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
    SortExec(_, _, left: QueryStageExec, _),
    SortExec(_, _, right: QueryStageExec, _))
      if supportOptimization(joinType, left, right) =>
      val leftStats = getStatistics(left)
      val rightStats = getStatistics(right)
      val numPartitions = leftStats.bytesByPartitionId.length

      val leftMedSize = medianSize(leftStats)
      val rightMedSize = medianSize(rightStats)
      logDebug(
        s"""
          |Try to optimize skewed join.
          |Left side partition size: median size: $leftMedSize,
          | max size: ${leftStats.bytesByPartitionId.max}
          |Right side partition size: median size: $rightMedSize,
          | max size: ${rightStats.bytesByPartitionId.max}
        """.stripMargin)

      val skewedPartitions = mutable.HashSet[Int]()
      val subJoins = mutable.ArrayBuffer[SparkPlan]()
      for (partitionId <- 0 until numPartitions) {
        val isLeftSkew = isSkewed(leftStats, partitionId, leftMedSize)
        val isRightSkew = isSkewed(rightStats, partitionId, rightMedSize)
        val leftMapIdStartIndices = if (isLeftSkew && supportSplitOnLeftPartition(joinType)) {
          estimateMapStartIndices(left, partitionId, leftMedSize)
        } else {
          Array(0)
        }
        val rightMapIdStartIndices = if (isRightSkew && supportSplitOnRightPartition(joinType)) {
          estimateMapStartIndices(right, partitionId, rightMedSize)
        } else {
          Array(0)
        }

        if (leftMapIdStartIndices.length > 1 || rightMapIdStartIndices.length > 1) {
          skewedPartitions += partitionId
          for (i <- 0 until leftMapIdStartIndices.length;
               j <- 0 until rightMapIdStartIndices.length) {
            val leftEndMapId = if (i == leftMapIdStartIndices.length - 1) {
              getMappersNum(left)
            } else {
              leftMapIdStartIndices(i + 1)
            }
            val rightEndMapId = if (j == rightMapIdStartIndices.length - 1) {
              getMappersNum(right)
            } else {
              rightMapIdStartIndices(j + 1)
            }
            // TODO: we may can optimize the sort merge join to broad cast join after
            //       obtaining the raw data size of per partition,
            val leftSkewedReader =
            SkewedShufflePartitionReader(
              left, partitionId, leftMapIdStartIndices(i), leftEndMapId)

            val rightSkewedReader =
              SkewedShufflePartitionReader(right, partitionId,
                rightMapIdStartIndices(j), rightEndMapId)
            subJoins += SortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
              leftSkewedReader, rightSkewedReader)
          }
        }
      }
      logDebug(s"number of skewed partitions is ${skewedPartitions.size}")
      if (skewedPartitions.size > 0) {
        val optimizedSmj = smj.transformDown {
          case sort: SortExec if (
            ShuffleQueryStageExec.isShuffleQueryStageExec(sort.child)) => {
            val newStage = ShuffleQueryStageExec.getShuffleStage(
              sort.child.asInstanceOf[QueryStageExec]).copy(
              excludedPartitions = skewedPartitions.toSet)
            val partialReader = PartialShuffleReader(
              newStage, skewedPartitions.toSet)
            sort.copy(child = partialReader)
          }
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
      case _: SkewedShufflePartitionReader => Nil
      case _: PartialShuffleReader => Nil
      case _: CoalescedShuffleReaderExec => Nil
      case stage: ShuffleQueryStageExec => Seq(stage)
      case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) => Seq(stage)
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
 * A wrapper of shuffle query stage, which submits one reduce task to read one shuffle partition.
 * This is used to handle the non-skewed partitions.
 *
 * @param child It's usually `ShuffleQueryStageExec` or `ReusedQueryStageExec`, but can be the
 *              shuffle exchange node during canonicalization.
 * @param excludedPartitions The excluded partitions.
 */
case class PartialShuffleReader(
    child: QueryStageExec, excludedPartitions: Set[Int]) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def doCanonicalize(): SparkPlan = child.canonicalized

  override def outputPartitioning: Partitioning = {
    UnknownPartitioning(getPartitionIndexRanges(excludedPartitions).length)
  }

  private def getPartitionIndexRanges(omittedPartitions: Set[Int]): Array[(Int, Int)] = {
    val length = ShuffleQueryStageExec.getShuffleStage(child)
      .plan.shuffleDependency.partitioner.numPartitions
    val partitionStartIndices = ArrayBuffer[Int]()
    val partitionEndIndices = ArrayBuffer[Int]()
    (0 until length).map { i =>
      if (!omittedPartitions.contains(i)) {
        partitionStartIndices += i
        partitionEndIndices += i + 1
      }
    }
    partitionStartIndices.zip(partitionEndIndices).toArray
  }

  private var cachedShuffleRDD: ShuffledRowRDD = null

  override protected def doExecute(): RDD[InternalRow] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = child match {
        case stage: ShuffleQueryStageExec =>
          stage.plan.createShuffledRDD(Some(getPartitionIndexRanges(excludedPartitions)))
        case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) =>
          stage.plan.createShuffledRDD(Some(getPartitionIndexRanges(excludedPartitions)))
      }
    }
    cachedShuffleRDD
  }
}

/**
 * A wrapper of shuffle query stage, which submits one reduce task to read the partition produced
 * by the mappers in range [startMapId, endMapId]. This is used to handle the skewed partitions.
 *
 * @param child It's usually `ShuffleQueryStageExec` or `ReusedQueryStageExec`, but can be the
 *              shuffle exchange node during canonicalization.
 * @param partitionIndex The pre shuffle partition index.
 * @param startMapId The start map id.
 * @param endMapId The end map id.
 */
case class SkewedShufflePartitionReader(
    child: QueryStageExec,
    partitionIndex: Int,
    startMapId: Int,
    endMapId: Int) extends LeafExecNode {

  override def output: Seq[Attribute] = child.output

  override def doCanonicalize(): SparkPlan = child.canonicalized

  override def outputPartitioning: Partitioning = {
    UnknownPartitioning(1)
  }
  private var cachedSkewedShuffleRDD: SkewedShuffledRowRDD = null

  override def doExecute(): RDD[InternalRow] = {
    if (cachedSkewedShuffleRDD == null) {
      cachedSkewedShuffleRDD = child match {
        case stage: ShuffleQueryStageExec =>
          stage.plan.createSkewedShuffleRDD(partitionIndex, startMapId, endMapId)
        case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) =>
          stage.plan.createSkewedShuffleRDD(partitionIndex, startMapId, endMapId)
      }
    }
    cachedSkewedShuffleRDD
  }
}
