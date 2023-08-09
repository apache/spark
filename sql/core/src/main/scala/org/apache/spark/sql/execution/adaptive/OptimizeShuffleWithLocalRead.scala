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

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, REBALANCE_PARTITIONS_BY_NONE, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule to optimize the shuffle read to local read iff no additional shuffles
 * will be introduced:
 * 1. if the input plan is a shuffle, add local read directly as we can never introduce
 * extra shuffles in this case.
 * 2. otherwise, add local read to the probe side of broadcast hash join and
 * then run `EnsureRequirements` to check whether additional shuffle introduced.
 * If introduced, we will revert all the local reads.
 */
object OptimizeShuffleWithLocalRead extends AQEShuffleReadRule {

  override val supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(ENSURE_REQUIREMENTS, REBALANCE_PARTITIONS_BY_NONE)

  override protected def isSupported(shuffle: ShuffleExchangeLike): Boolean = {
    shuffle.outputPartitioning != SinglePartition && super.isSupported(shuffle)
  }

  // The build side is a broadcast query stage which should have been optimized using local read
  // already. So we only need to deal with probe side here.
  private def createProbeSideLocalRead(plan: SparkPlan): SparkPlan = {
    plan.transformDown {
      case join @ BroadcastJoinWithShuffleLeft(shuffleStage, BuildRight) =>
        val localRead = createLocalRead(shuffleStage)
        join.asInstanceOf[BroadcastHashJoinExec].copy(left = localRead)
      case join @ BroadcastJoinWithShuffleRight(shuffleStage, BuildLeft) =>
        val localRead = createLocalRead(shuffleStage)
        join.asInstanceOf[BroadcastHashJoinExec].copy(right = localRead)
    }
  }

  private def createLocalRead(plan: SparkPlan): AQEShuffleReadExec = {
    plan match {
      case c @ AQEShuffleReadExec(s: ShuffleQueryStageExec, _) =>
        AQEShuffleReadExec(s, getPartitionSpecs(s, Some(c.partitionSpecs.length)))
      case s: ShuffleQueryStageExec =>
        AQEShuffleReadExec(s, getPartitionSpecs(s, None))
    }
  }

  // TODO: this method assumes all shuffle blocks are the same data size. We should calculate the
  //       partition start indices based on block size to avoid data skew.
  private def getPartitionSpecs(
      shuffleStage: ShuffleQueryStageExec,
      advisoryParallelism: Option[Int]): Seq[ShufflePartitionSpec] = {
    val numMappers = shuffleStage.shuffle.numMappers
    // ShuffleQueryStageExec.mapStats.isDefined promise numMappers > 0
    assert(numMappers > 0)
    val numReducers = shuffleStage.shuffle.numPartitions
    val expectedParallelism = advisoryParallelism.getOrElse(numReducers)
    val splitPoints = if (expectedParallelism >= numMappers) {
      equallyDivide(numReducers, expectedParallelism / numMappers)
    } else {
      equallyDivide(numMappers, expectedParallelism)
    }
    if (expectedParallelism >= numMappers) {
      (0 until numMappers).flatMap { mapIndex =>
        (splitPoints :+ numReducers).sliding(2).map {
          case Seq(start, end) => PartialMapperPartitionSpec(mapIndex, start, end)
        }
      }
    } else {
      (0 until 1).flatMap { _ =>
        (splitPoints :+ numMappers).sliding(2).map {
          case Seq(start, end) => CoalescedMapperPartitionSpec(start, end, numReducers)
        }
      }
    }
  }

  /**
   * To equally divide n elements into m buckets, basically each bucket should have n/m elements,
   * for the remaining n%m elements, add one more element to the first n%m buckets each. Returns
   * a sequence with length numBuckets and each value represents the start index of each bucket.
   */
  private def equallyDivide(numElements: Int, numBuckets: Int): Seq[Int] = {
    val elementsPerBucket = numElements / numBuckets
    val remaining = numElements % numBuckets
    val splitPoint = (elementsPerBucket + 1) * remaining
    (0 until remaining).map(_ * (elementsPerBucket + 1)) ++
      (remaining until numBuckets).map(i => splitPoint + (i - remaining) * elementsPerBucket)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.LOCAL_SHUFFLE_READER_ENABLED)) {
      return plan
    }

    plan match {
      case s: SparkPlan if canUseLocalShuffleRead(s) =>
        createLocalRead(s)
      case s: SparkPlan =>
        createProbeSideLocalRead(s)
    }
  }

  object BroadcastJoinWithShuffleLeft {
    def unapply(plan: SparkPlan): Option[(SparkPlan, BuildSide)] = plan match {
      case join: BroadcastHashJoinExec if canUseLocalShuffleRead(join.left) =>
        Some((join.left, join.buildSide))
      case _ => None
    }
  }

  object BroadcastJoinWithShuffleRight {
    def unapply(plan: SparkPlan): Option[(SparkPlan, BuildSide)] = plan match {
      case join: BroadcastHashJoinExec if canUseLocalShuffleRead(join.right) =>
        Some((join.right, join.buildSide))
      case _ => None
    }
  }

  def canUseLocalShuffleRead(plan: SparkPlan): Boolean = plan match {
    case s: ShuffleQueryStageExec =>
      s.mapStats.isDefined && isSupported(s.shuffle)
    case AQEShuffleReadExec(s: ShuffleQueryStageExec, _) =>
      s.mapStats.isDefined && isSupported(s.shuffle) &&
        s.shuffle.shuffleOrigin == ENSURE_REQUIREMENTS
    case _ => false
  }
}
