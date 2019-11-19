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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchangeExec}
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

  def withProbeSideLocalReader(plan: SparkPlan): SparkPlan = {
    plan.transformDown {
      case join @ OptimizeLocalShuffleReader.BroadcastJoinWithShuffleLeft(
      shuffleStage, BuildRight) =>
        val localReader = createLocalReader(shuffleStage)
        join.asInstanceOf[BroadcastHashJoinExec].copy(left = localReader)
      case join @ OptimizeLocalShuffleReader.BroadcastJoinWithShuffleRight(
      shuffleStage, BuildLeft) =>
        val localReader = createLocalReader(shuffleStage)
        join.asInstanceOf[BroadcastHashJoinExec].copy(right = localReader)
    }
  }

  def createLocalReader(plan: SparkPlan): LocalShuffleReaderExec = {
    plan match {
      case c: CoalescedShuffleReaderExec =>
        LocalShuffleReaderExec(c.child, Some(c.partitionStartIndices.length))
      case q: QueryStageExec => LocalShuffleReaderExec(q)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.OPTIMIZE_LOCAL_SHUFFLE_READER_ENABLED)) {
      return plan
    }

    val optimizedPlan = plan match {
      case s: SparkPlan if OptimizeLocalShuffleReader.canUseLocalShuffleReader(s) =>
        createLocalReader(s)
      case s: SparkPlan => withProbeSideLocalReader(s)
    }

    def numExchanges(plan: SparkPlan): Int = {
      plan.collect {
        case e: ShuffleExchangeExec => e
      }.length
    }
    // Check whether additional shuffle introduced. If introduced, revert the local reader.
    if (numExchanges(EnsureRequirements(conf).apply(optimizedPlan)) > 0) {
      logDebug("OptimizeLocalShuffleReader rule is not applied due" +
        " to additional shuffles will be introduced.")
      plan
    } else {
      optimizedPlan
    }
  }
}

object OptimizeLocalShuffleReader {

  object BroadcastJoinWithShuffleLeft {
    def unapply(plan: SparkPlan): Option[(SparkPlan, BuildSide)] = plan match {
      case join: BroadcastHashJoinExec if OptimizeLocalShuffleReader.
        canUseLocalShuffleReader(join.left) =>
        Some((join.left, join.buildSide))
      case _ => None
    }
  }

  object BroadcastJoinWithShuffleRight {
    def unapply(plan: SparkPlan): Option[(SparkPlan, BuildSide)] = plan match {
      case join: BroadcastHashJoinExec if OptimizeLocalShuffleReader.
        canUseLocalShuffleReader(join.right) =>
        Some((join.right, join.buildSide))
      case _ => None
    }
  }

  def canUseLocalShuffleReader(plan: SparkPlan): Boolean = {
    ShuffleQueryStageExec.isShuffleQueryStageExec(plan) ||
      plan.isInstanceOf[CoalescedShuffleReaderExec]
  }
}

/**
 * A wrapper of shuffle query stage, which submits one reduce task per mapper to read the shuffle
 * files written by one mapper. By doing this, it's very likely to read the shuffle files locally,
 * as the shuffle files that a reduce task needs to read are in one node.
 *
 * @param child It's usually `ShuffleQueryStageExec` or `ReusedQueryStageExec`, but can be the
 *              shuffle exchange node during canonicalization.
 */
case class LocalShuffleReaderExec(
    child: SparkPlan,
    advisoryParallelism: Option[Int] = None) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child match {
    case stage: ShuffleQueryStageExec =>
      stage.plan.child.outputPartitioning
    case r @ ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) =>
      r.updatePartitioning(stage.plan.child.outputPartitioning)
  }

  private var cachedShuffleRDD: RDD[InternalRow] = null

  override protected def doExecute(): RDD[InternalRow] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = child match {
        case stage: ShuffleQueryStageExec =>
          stage.plan.createLocalShuffleRDD(advisoryParallelism)
        case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) =>
          stage.plan.createLocalShuffleRDD(advisoryParallelism)
      }
    }
    cachedShuffleRDD
  }
}
