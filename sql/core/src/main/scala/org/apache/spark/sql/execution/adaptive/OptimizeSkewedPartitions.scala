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
    size > medianSize * conf.adaptiveSkewedFactor &&
      size > conf.adaptiveSkewedSizeThreshold
  }

  private def medianSize(stats: MapOutputStatistics): Long = {
    val bytesLen = stats.bytesByPartitionId.length
    val bytes = stats.bytesByPartitionId.sorted
    if (bytes(bytesLen / 2) > 0) bytes(bytesLen / 2) else 1
  }

  /*
  * Get all the map data size for specific reduce partitionId.
  */
  def getMapSizeForSpecificPartition(partitionId: Int, shuffleId: Int): Array[Long] = {
    val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTracker.shuffleStatuses.get(shuffleId).
      get.mapStatuses.map{_.getSizeForBlock(partitionId)}
  }

  /*
  * Split the mappers based on the map size of specific skewed reduce partitionId.
  */
  def splitMappersBasedDataSize(mapPartitionSize: Array[Long], numMappers: Int): Array[Int] = {
    val advisoryTargetPostShuffleInputSize = conf.targetPostShuffleInputSize
    val partitionStartIndices = ArrayBuffer[Int]()
    var i = 0
    var postMapPartitionSize: Long = mapPartitionSize(i)
    partitionStartIndices += i
    while (i < numMappers && i + 1 < numMappers) {
      val nextIndex = if (i + 1 < numMappers) {
        i + 1
      } else numMappers -1

      if (postMapPartitionSize + mapPartitionSize(nextIndex) > advisoryTargetPostShuffleInputSize) {
        postMapPartitionSize = mapPartitionSize(nextIndex)
        partitionStartIndices += nextIndex
      } else {
        postMapPartitionSize += mapPartitionSize(nextIndex)
      }
      i += 1
    }
    partitionStartIndices.toArray
  }

  /**
   * We split the partition into several splits. Each split reads the data from several map outputs
   * ranging from startMapId to endMapId(exclusive). This method calculates the split number and
   * the startMapId for all splits.
   */
  private def estimateMapIdStartIndices(
    stage: QueryStageExec,
    partitionId: Int,
    medianSize: Long): Array[Int] = {
    val dependency = getShuffleStage(stage).plan.shuffleDependency
    val shuffleId = dependency.shuffleHandle.shuffleId
    val mapSize = getMapSizeForSpecificPartition(partitionId, shuffleId)
    val numMappers = dependency.rdd.partitions.length
    splitMappersBasedDataSize(mapSize, numMappers)
  }

  private def getShuffleStage(queryStage: QueryStageExec): ShuffleQueryStageExec = {
    queryStage match {
      case stage: ShuffleQueryStageExec => stage
      case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) => stage
    }
  }

  private def getStatistics(queryStage: QueryStageExec): MapOutputStatistics = {
    val shuffleStage = queryStage match {
      case stage: ShuffleQueryStageExec => stage
      case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) => stage
    }
    val metrics = shuffleStage.mapOutputStatisticsFuture
    assert(metrics.isCompleted, "ShuffleQueryStageExec should already be ready")
    ThreadUtils.awaitResult(metrics, Duration.Zero)
  }

  /**
   * Base optimization support check: the join type is supported and plan statistics is available.
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
    val statisticsReady: Boolean = if (shuffleStageCheck) {
      getStatistics(leftStage) != null && getStatistics(rightStage) != null
    } else false

    joinTypeSupported && statisticsReady
  }

  private def supportSplitOnLeftPartition(joinType: JoinType) = joinType != RightOuter

  private def supportSplitOnRightPartition(joinType: JoinType) = {
    joinType != LeftOuter && joinType != LeftSemi && joinType != LeftAnti
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
      logInfo(s"HandlingSkewedJoin left medSize: ($leftMedSize)" +
        s" right medSize ($rightMedSize)")
      logInfo(s"left bytes Max : ${leftStats.bytesByPartitionId.max}")
      logInfo(s"right bytes Max : ${rightStats.bytesByPartitionId.max}")

      val skewedPartitions = mutable.HashSet[Int]()
      val subJoins = mutable.ArrayBuffer[SparkPlan](smj)
      for (partitionId <- 0 until numPartitions) {
        val isLeftSkew = isSkewed(leftStats, partitionId, leftMedSize)
        val isRightSkew = isSkewed(rightStats, partitionId, rightMedSize)
        val isSkewAndSupportsSplit =
          (isLeftSkew && supportSplitOnLeftPartition(joinType)) ||
            (isRightSkew && supportSplitOnRightPartition(joinType))

        if (isSkewAndSupportsSplit) {
          skewedPartitions += partitionId
          val leftMapIdStartIndices = if (isLeftSkew && supportSplitOnLeftPartition(joinType)) {
            estimateMapIdStartIndices(left, partitionId, leftMedSize)
          } else {
            Array(0)
          }
          val rightMapIdStartIndices = if (isRightSkew && supportSplitOnRightPartition(joinType)) {
            estimateMapIdStartIndices(right, partitionId, rightMedSize)
          } else {
            Array(0)
          }

          for (i <- 0 until leftMapIdStartIndices.length;
               j <- 0 until rightMapIdStartIndices.length) {
            val leftEndMapId = if (i == leftMapIdStartIndices.length - 1) {
              getShuffleStage(left).plan.shuffleDependency.rdd.partitions.length
            } else {
              leftMapIdStartIndices(i + 1)
            }
            val rightEndMapId = if (j == rightMapIdStartIndices.length - 1) {
              getShuffleStage(right).
                plan.shuffleDependency.rdd.partitions.length
            } else {
              rightMapIdStartIndices(j + 1)
            }
            // For the skewed partition, we set the id of shuffle query stage to -1.
            // And skip this shuffle query stage optimization in 'ReduceNumShufflePartitions' rule.
            val leftSkewedReader =
              PostShufflePartitionReader(getShuffleStage(left).copy(id = -1),
                partitionId, leftMapIdStartIndices(i), leftEndMapId)

            val rightSkewedReader =
              PostShufflePartitionReader(getShuffleStage(right).copy(id = -1),
                partitionId, rightMapIdStartIndices(j), rightEndMapId)

            subJoins +=
              SortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
                leftSkewedReader, rightSkewedReader)
          }
        }
      }
      logInfo(s"skewed partition number is ${skewedPartitions.size}")
      if (skewedPartitions.size > 0) {
        getShuffleStage(left).skewedPartitions = skewedPartitions
        getShuffleStage(right).skewedPartitions = skewedPartitions
        UnionExec(subJoins.toList)
      } else {
        smj
      }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveSkewedJoinEnabled) {
      return  plan
    }

    def collectShuffleStages(plan: SparkPlan): Seq[ShuffleQueryStageExec] = plan match {
      case _: LocalShuffleReaderExec => Nil
      case _: PostShufflePartitionReader => Nil
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

case class PostShufflePartitionReader(
    child: QueryStageExec,
    partitionIndex: Int,
    startMapId: Int,
    endMapId: Int) extends UnaryExecNode {

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
