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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildLeft, BuildRight, BuildSide}
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
case class OptimizeLocalShuffleReader(conf: SQLConf) extends Rule[SparkPlan] {
  import OptimizeLocalShuffleReader._

  private val ensureRequirements = EnsureRequirements(conf)

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

  private def createLocalReader(plan: SparkPlan): LocalShuffleReaderExec = {
    plan match {
      case c @ CoalescedShuffleReaderExec(s: ShuffleQueryStageExec, _) =>
        LocalShuffleReaderExec(
          s, getPartitionStartIndices(s, Some(c.partitionStartIndices.length)))
      case s: ShuffleQueryStageExec =>
        LocalShuffleReaderExec(s, getPartitionStartIndices(s, None))
    }
  }

  // TODO: this method assumes all shuffle blocks are the same data size. We should calculate the
  //       partition start indices based on block size to avoid data skew.
  private def getPartitionStartIndices(
      shuffleStage: ShuffleQueryStageExec,
      advisoryParallelism: Option[Int]): Array[Array[Int]] = {
    val shuffleDep = shuffleStage.shuffle.shuffleDependency
    val numReducers = shuffleDep.partitioner.numPartitions
    val expectedParallelism = advisoryParallelism.getOrElse(numReducers)
    val numMappers = shuffleDep.rdd.getNumPartitions
    Array.fill(numMappers) {
      equallyDivide(numReducers, math.max(1, expectedParallelism / numMappers)).toArray
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
}

object OptimizeLocalShuffleReader {

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

  def canUseLocalShuffleReader(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[ShuffleQueryStageExec] ||
      plan.isInstanceOf[CoalescedShuffleReaderExec]
  }
}

/**
 * A wrapper of shuffle query stage, which submits one or more reduce tasks per mapper to read the
 * shuffle files written by one mapper. By doing this, it's very likely to read the shuffle files
 * locally, as the shuffle files that a reduce task needs to read are in one node.
 *
 * @param child It's usually `ShuffleQueryStageExec`, but can be the shuffle exchange node during
 *              canonicalization.
 * @param partitionStartIndicesPerMapper A mapper usually writes many shuffle blocks, and it's
 *                                       better to launch multiple tasks to read shuffle blocks of
 *                                       one mapper. This array contains the partition start
 *                                       indices for each mapper.
 */
case class LocalShuffleReaderExec(
    child: SparkPlan,
    partitionStartIndicesPerMapper: Array[Array[Int]]) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override lazy val outputPartitioning: Partitioning = {
    // when we read one mapper per task, then the output partitioning is the same as the plan
    // before shuffle.
    if (partitionStartIndicesPerMapper.forall(_.length == 1)) {
      child match {
        case ShuffleQueryStageExec(_, s: ShuffleExchangeExec) =>
          s.child.outputPartitioning
        case ShuffleQueryStageExec(_, r @ ReusedExchangeExec(_, s: ShuffleExchangeExec)) =>
          s.child.outputPartitioning match {
            case e: Expression => r.updateAttr(e).asInstanceOf[Partitioning]
            case other => other
          }
        case _ =>
          throw new IllegalStateException("operating on canonicalization plan")
      }
    } else {
      UnknownPartitioning(partitionStartIndicesPerMapper.map(_.length).sum)
    }
  }

  private var cachedShuffleRDD: RDD[InternalRow] = null

  override protected def doExecute(): RDD[InternalRow] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = child match {
        case stage: ShuffleQueryStageExec =>
          stage.shuffle.createLocalShuffleRDD(partitionStartIndicesPerMapper)
        case _ =>
          throw new IllegalStateException("operating on canonicalization plan")
      }
    }
    cachedShuffleRDD
  }
}
