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
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildLeft, BuildRight}
import org.apache.spark.sql.internal.SQLConf

case class OptimizeLocalShuffleReader(conf: SQLConf) extends Rule[SparkPlan] {

  def canUseLocalShuffleReaderLeft(join: BroadcastHashJoinExec): Boolean = {
    join.buildSide == BuildRight &&  ShuffleQueryStageExec.isShuffleQueryStageExec(join.left)
  }

  def canUseLocalShuffleReaderRight(join: BroadcastHashJoinExec): Boolean = {
    join.buildSide == BuildLeft &&  ShuffleQueryStageExec.isShuffleQueryStageExec(join.right)
  }

  def setIsLocalShuffleToTrue(stage: QueryStageExec): Unit = {
    stage match {
      case stage: ShuffleQueryStageExec =>
        stage.isLocalShuffle = true
      case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) =>
        stage.isLocalShuffle = true
    }

  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.OPTIMIZE_LOCAL_SHUFFLE_READER_ENABLED)) {
      return plan
    }

    val optimizedPlan = plan.transformDown {
      case join: BroadcastHashJoinExec if canUseLocalShuffleReaderRight(join) =>
        val localReader = LocalShuffleReaderExec(join.right.asInstanceOf[QueryStageExec])
        setIsLocalShuffleToTrue(join.right.asInstanceOf[QueryStageExec])
        join.copy(right = localReader)
      case join: BroadcastHashJoinExec if canUseLocalShuffleReaderLeft(join) =>
        val localReader = LocalShuffleReaderExec(join.left.asInstanceOf[QueryStageExec])
        setIsLocalShuffleToTrue(join.left.asInstanceOf[QueryStageExec])
        join.copy(left = localReader)
    }

    def numExchanges(plan: SparkPlan): Int = {
      plan.collect {
        case e: ShuffleExchangeExec => e
      }.length
    }

    val numExchangeBefore = numExchanges(EnsureRequirements(conf).apply(plan))
    val numExchangeAfter = numExchanges(EnsureRequirements(conf).apply(optimizedPlan))

    if (numExchangeAfter > numExchangeBefore) {
      logDebug("OptimizeLocalShuffleReader rule is not applied due" +
        " to additional shuffles will be introduced.")
      plan
    } else {
      optimizedPlan
    }
  }
}

case class LocalShuffleReaderExec(child: QueryStageExec) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def doCanonicalize(): SparkPlan = child.canonicalized

  override def outputPartitioning: Partitioning = {

    def tryReserveChildPartitioning(stage: ShuffleQueryStageExec): Partitioning = {
      val initialPartitioning = stage.plan.child.outputPartitioning
      if (initialPartitioning.isInstanceOf[UnknownPartitioning]) {
        UnknownPartitioning(stage.plan.shuffleDependency.rdd.partitions.length)
      } else {
        initialPartitioning
      }
    }

    child match {
      case stage: ShuffleQueryStageExec =>
        tryReserveChildPartitioning(stage)
      case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) =>
        tryReserveChildPartitioning(stage)
    }
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

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean): Unit = {
    super.generateTreeString(depth,
      lastChildren,
      append,
      verbose,
      prefix,
      addSuffix,
      maxFields,
      printNodeId)
    child.generateTreeString(
      depth + 1, lastChildren :+ true, append, verbose, "", false, maxFields, printNodeId)
  }
}
