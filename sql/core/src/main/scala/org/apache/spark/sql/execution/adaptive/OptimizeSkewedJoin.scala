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
import scala.collection.mutable.{ArrayBuffer, HashSet}

import org.apache.spark.{MapOutputStatistics, MapOutputTrackerMaster, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

case class OptimizeSkewedJoin(conf: SQLConf) extends Rule[SparkPlan] {

  private val ensureRequirements = EnsureRequirements(conf)

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
    numPartitions match {
      case _ if (numPartitions % 2 == 0) =>
        math.max((bytes(numPartitions / 2) + bytes(numPartitions / 2 - 1)) / 2, 1)
      case _ => math.max(bytes(numPartitions / 2), 1)
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
   * Split the skewed partition based on the map size and the max split number.
   */
  private def getMapStartIndices(stage: ShuffleQueryStageExec, partitionId: Int): Array[Int] = {
    val shuffleId = stage.shuffle.shuffleDependency.shuffleHandle.shuffleId
    val mapPartitionSizes = getMapSizesForReduceId(shuffleId, partitionId)
    val maxSplits = math.min(conf.getConf(
      SQLConf.ADAPTIVE_EXECUTION_SKEWED_PARTITION_MAX_SPLITS), mapPartitionSizes.length)
    val avgPartitionSize = mapPartitionSizes.sum / maxSplits
    val advisoryPartitionSize = math.max(avgPartitionSize,
      conf.getConf(SQLConf.ADAPTIVE_EXECUTION_SKEWED_PARTITION_SIZE_THRESHOLD))
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

  private def containShuffleQueryStage(plan : SparkPlan): (Boolean, ShuffleQueryStageExec) =
    plan match {
      case stage: ShuffleQueryStageExec => (true, stage)
      case sort: SortExec if (sort.child.isInstanceOf[ShuffleQueryStageExec]) =>
        (true, sort.child.asInstanceOf[ShuffleQueryStageExec])
      case _ => (false, null)
  }

  private def reOptimizeChild(
      skewedReader: SkewedPartitionReaderExec,
      child: SparkPlan): SparkPlan = child match {
    case sort: SortExec if (sort.child.isInstanceOf[ShuffleQueryStageExec]) =>
      sort.copy(child = skewedReader)
    case _ => child
  }

  private def getSizeInfo(medianSize: Long, maxSize: Long): String = {
    s"median size: $medianSize, max size: ${maxSize}"
  }

  /*
   * This method aim to optimize the skewed join with the following steps:
   * 1. Check whether the shuffle partition is skewed based on the median size
   *    and the skewed partition threshold in origin smj.
   * 2. Assuming partition0 is skewed in left side, and it has 5 mappers (Map0, Map1...Map4).
   *    And we will split the 5 Mappers into 3 mapper ranges [(Map0, Map1), (Map2, Map3), (Map4)]
   *    based on the map size and the max split number.
   * 3. Create the 3 smjs with separately reading the above mapper ranges and then join with
   *    the Partition0 in right side.
   * 4. Finally union the above 3 split smjs and the origin smj.
   */
  def handleSkewJoin(plan: SparkPlan): SparkPlan = {
    val optimizePlan = plan.transformUp {
      case smj @ SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, leftPlan, rightPlan)
        if (containShuffleQueryStage(leftPlan)._1 && containShuffleQueryStage(rightPlan)._1) &&
          supportedJoinTypes.contains(joinType) =>
        val left = containShuffleQueryStage(leftPlan)._2
        val right = containShuffleQueryStage(rightPlan)._2
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
              val leftSkewedReader = SkewedPartitionReaderExec(
                left, partitionId, leftMapIdStartIndices(i), leftEndMapId)
              val rightSkewedReader = SkewedPartitionReaderExec(right, partitionId,
                rightMapIdStartIndices(j), rightEndMapId)
              val skewedLeft = reOptimizeChild(leftSkewedReader, leftPlan)
              val skewedRight = reOptimizeChild(rightSkewedReader, rightPlan)
              subJoins += SortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
                skewedLeft, skewedRight)
            }
          }
        }
        logDebug(s"number of skewed partitions is ${skewedPartitions.size}")
        if (skewedPartitions.nonEmpty) {
          val visitedStages = HashSet.empty[Int]
          val optimizedSmj = smj.transformDown {
            case shuffleStage: ShuffleQueryStageExec if !visitedStages.contains(shuffleStage.id) =>
              visitedStages.add(shuffleStage.id)
              PartialShuffleReaderExec(shuffleStage, skewedPartitions.toSet)
          }
          subJoins += optimizedSmj
          UnionExec(subJoins)
        } else {
          smj
        }
    }
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
      // When multi table join, there will be too many complex combination to consider.
      // Currently we only handle 2 table join like following two use cases.
      // SMJ                    SMJ
      //   Sort                   Shuffle
      //     Shuffle      or      Shuffle
      //   Sort
      //     Shuffle
      handleSkewJoin(plan)
    } else {
      plan
    }
  }
}

/**
 * A wrapper of shuffle query stage, which submits one reduce task to read a single
 * shuffle partition 'partitionIndex' produced by the mappers in range [startMapIndex, endMapIndex).
 * This is used to increase the parallelism when reading skewed partitions.
 *
 * @param child It's usually `ShuffleQueryStageExec`, but can be the shuffle exchange
 *              node during canonicalization.
 * @param partitionIndex The pre shuffle partition index.
 * @param startMapIndex The start map index.
 * @param endMapIndex The end map index.
 */
case class SkewedPartitionReaderExec(
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

/**
 * A wrapper of shuffle query stage, which skips some partitions when reading the shuffle blocks.
 *
 * @param child It's usually `ShuffleQueryStageExec`, but can be the shuffle exchange node during
 *              canonicalization.
 * @param excludedPartitions The partitions to skip when reading.
 */
case class PartialShuffleReaderExec(
    child: QueryStageExec,
    excludedPartitions: Set[Int]) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    UnknownPartitioning(1)
  }

  private def shuffleExchange(): ShuffleExchangeExec = child match {
    case stage: ShuffleQueryStageExec => stage.shuffle
    case _ =>
      throw new IllegalStateException("operating on canonicalization plan")
  }

  private def getPartitionIndexRanges(): Array[(Int, Int)] = {
    val length = shuffleExchange().shuffleDependency.partitioner.numPartitions
    (0 until length).filterNot(excludedPartitions.contains).map(i => (i, i + 1)).toArray
  }

  private var cachedShuffleRDD: RDD[InternalRow] = null

  override def doExecute(): RDD[InternalRow] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = if (excludedPartitions.isEmpty) {
        child.execute()
      } else {
        shuffleExchange().createShuffledRDD(Some(getPartitionIndexRanges()))
      }
    }
    cachedShuffleRDD
  }
}
