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

object BroadcastJoinWithShuffleLeft {
  def unapply(plan: SparkPlan): Option[(QueryStageExec, BuildSide)] = plan match {
    case join: BroadcastHashJoinExec if ShuffleQueryStageExec.isShuffleQueryStageExec(join.left) =>
      Some((join.left.asInstanceOf[QueryStageExec], join.buildSide))
    case _ => None
  }
}

object BroadcastJoinWithShuffleRight {
  def unapply(plan: SparkPlan): Option[(QueryStageExec, BuildSide)] = plan match {
    case join: BroadcastHashJoinExec if ShuffleQueryStageExec.isShuffleQueryStageExec(join.right) =>
      Some((join.right.asInstanceOf[QueryStageExec], join.buildSide))
    case _ => None
  }
}

/**
 * A rule to optimize the shuffle reader to local reader as far as possible
 * when converting the 'SortMergeJoinExec' to 'BroadcastHashJoinExec' in runtime.
 *
 * This rule can be divided into two steps:
 * Step1: Add the local reader in probe side and then check whether additional
 *       shuffle introduced. If introduced, we will revert all the local
 *       reader in probe side.
 * Step2: Add the local reader in build side and will not check whether
 *        additional shuffle introduced. Because the build side will not introduce
 *        additional shuffle.
 */
case class OptimizeLocalShuffleReader(conf: SQLConf) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.OPTIMIZE_LOCAL_SHUFFLE_READER_ENABLED)) {
      return plan
    }
    // Add local reader in probe side.
    val withProbeSideLocalReader = plan.transformDown {
      case join @ BroadcastJoinWithShuffleLeft(shuffleStage, BuildRight) =>
        val localReader = LocalShuffleReaderExec(shuffleStage)
        join.asInstanceOf[BroadcastHashJoinExec].copy(left = localReader)
      case join @ BroadcastJoinWithShuffleRight(shuffleStage, BuildLeft) =>
        val localReader = LocalShuffleReaderExec(shuffleStage)
        join.asInstanceOf[BroadcastHashJoinExec].copy(right = localReader)
    }

    def numExchanges(plan: SparkPlan): Int = {
      plan.collect {
        case e: ShuffleExchangeExec => e
      }.length
    }
    // Check whether additional shuffle introduced. If introduced, revert the local reader.
    val numExchangeBefore = numExchanges(EnsureRequirements(conf).apply(plan))
    val numExchangeAfter = numExchanges(EnsureRequirements(conf).apply(withProbeSideLocalReader))
    val optimizedPlan = if (numExchangeAfter > numExchangeBefore) {
      logDebug("OptimizeLocalShuffleReader rule is not applied in the probe side due" +
        " to additional shuffles will be introduced.")
      plan
    } else {
      withProbeSideLocalReader
    }
    // Add the local reader in build side and and do not need to check whether
    // additional shuffle introduced.
    optimizedPlan.transformDown {
      case join @ BroadcastJoinWithShuffleLeft(shuffleStage, BuildLeft) =>
        val localReader = LocalShuffleReaderExec(shuffleStage)
        join.asInstanceOf[BroadcastHashJoinExec].copy(left = localReader)
      case join @ BroadcastJoinWithShuffleRight(shuffleStage, BuildRight) =>
        val localReader = LocalShuffleReaderExec(shuffleStage)
        join.asInstanceOf[BroadcastHashJoinExec].copy(right = localReader)
    }
  }
}

case class LocalShuffleReaderExec(child: QueryStageExec) extends UnaryExecNode {

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
          stage.plan.createLocalShuffleRDD()
        case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) =>
          stage.plan.createLocalShuffleRDD()
      }
    }
    cachedShuffleRDD
  }
}
