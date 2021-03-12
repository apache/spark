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
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, ShuffleExchangeExec, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule to optimize the shuffle reader to local reader iff no additional shuffles
 * will be introduced:
 * 1. if the input plan is a shuffle, add local reader directly as we can never introduce
 * extra shuffles in this case.
 * 2. otherwise, add local reader to the probe side of broadcast hash join and
 * then run `EnsureRequirements` to check whether additional shuffle introduced.
 * If introduced, we will revert all the local readers.
 */
object OptimizeLocalShuffleReader extends CustomShuffleReaderRule {

  override val supportedShuffleOrigins: Seq[ShuffleOrigin] = Seq(ENSURE_REQUIREMENTS)

  private val ensureRequirements = EnsureRequirements

  // The build side is a broadcast query stage which should have been optimized using local reader
  // already. So we only need to deal with probe side here.
  private def createProbeSideLocalReader(plan: SparkPlan): SparkPlan = {
    val optimizedPlan = plan.transformDown {
      case join @ BroadcastJoinWithShuffleLeft(shuffleStage, BuildRight) =>
        val localReader = createLocalReader(shuffleStage)
        join.asInstanceOf[BroadcastHashJoinExec].copy(left = localReader)
      case join @ BroadcastJoinWithShuffleRight(shuffleStage, BuildLeft) =>
        val localReader = createLocalReader(shuffleStage)
        join.asInstanceOf[BroadcastHashJoinExec].copy(right = localReader)
    }

    val numShuffles = ensureRequirements.apply(optimizedPlan).collect {
      case e: ShuffleExchangeExec => e
    }.length

    // Check whether additional shuffle introduced. If introduced, revert the local reader.
    if (numShuffles > 0) {
      logDebug("OptimizeLocalShuffleReader rule is not applied due" +
        " to additional shuffles will be introduced.")
      plan
    } else {
      optimizedPlan
    }
  }

  private def createLocalReader(plan: SparkPlan): CustomShuffleReaderExec = {
    plan match {
      case c @ CustomShuffleReaderExec(s: ShuffleQueryStageExec, _) =>
        CustomShuffleReaderExec(s, getPartitionSpecs(s, Some(c.partitionSpecs.length)))
      case s: ShuffleQueryStageExec =>
        CustomShuffleReaderExec(s, getPartitionSpecs(s, None))
    }
  }

  // TODO: this method assumes all shuffle blocks are the same data size. We should calculate the
  //       partition start indices based on block size to avoid data skew.
  private def getPartitionSpecs(
      shuffleStage: ShuffleQueryStageExec,
      advisoryParallelism: Option[Int]): Seq[ShufflePartitionSpec] = {
    val numMappers = shuffleStage.shuffle.numMappers
    val numReducers = shuffleStage.shuffle.numPartitions
    val expectedParallelism = advisoryParallelism.getOrElse(numReducers)
    val splitPoints = if (numMappers == 0) {
      Seq.empty
    } else {
      equallyDivide(numReducers, math.max(1, expectedParallelism / numMappers))
    }
    (0 until numMappers).flatMap { mapIndex =>
      (splitPoints :+ numReducers).sliding(2).map {
        case Seq(start, end) => PartialMapperPartitionSpec(mapIndex, start, end)
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
      case s: SparkPlan if canUseLocalShuffleReader(s) =>
        createLocalReader(s)
      case s: SparkPlan =>
        createProbeSideLocalReader(s)
    }
  }

  object BroadcastJoinWithShuffleLeft {
    def unapply(plan: SparkPlan): Option[(SparkPlan, BuildSide)] = plan match {
      case join: BroadcastHashJoinExec if canUseLocalShuffleReader(join.left) =>
        Some((join.left, join.buildSide))
      case _ => None
    }
  }

  object BroadcastJoinWithShuffleRight {
    def unapply(plan: SparkPlan): Option[(SparkPlan, BuildSide)] = plan match {
      case join: BroadcastHashJoinExec if canUseLocalShuffleReader(join.right) =>
        Some((join.right, join.buildSide))
      case _ => None
    }
  }

  def canUseLocalShuffleReader(plan: SparkPlan): Boolean = plan match {
    case s: ShuffleQueryStageExec =>
      s.mapStats.isDefined && supportLocalReader(s.shuffle)
    case CustomShuffleReaderExec(s: ShuffleQueryStageExec, partitionSpecs) =>
      s.mapStats.isDefined && partitionSpecs.nonEmpty && supportLocalReader(s.shuffle)
    case _ => false
  }

  private def supportLocalReader(s: ShuffleExchangeLike): Boolean = {
    s.outputPartitioning != SinglePartition && supportedShuffleOrigins.contains(s.shuffleOrigin)
  }
}
