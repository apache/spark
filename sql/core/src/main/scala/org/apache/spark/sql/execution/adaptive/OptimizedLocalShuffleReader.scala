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
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec}
import org.apache.spark.sql.internal.SQLConf

case class OptimizedLocalShuffleReader(conf: SQLConf) extends Rule[SparkPlan] {

  private def setIsLocalToFalse(shuffleStage: QueryStageExec): QueryStageExec = {
    shuffleStage match {
      case stage: ShuffleQueryStageExec =>
        stage.isLocalShuffle = false
      case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) =>
        stage.isLocalShuffle = false
    }
    shuffleStage
  }

  private def revertLocalShuffleReader(newPlan: SparkPlan): SparkPlan = {
    val revertPlan = newPlan.transformUp {
      case localReader: LocalShuffleReaderExec
        if (ShuffleQueryStageExec.isShuffleQueryStageExec(localReader.child)) =>
        setIsLocalToFalse(localReader.child)
    }
    revertPlan
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    // Collect the `BroadcastHashJoinExec` nodes and if isEmpty directly return.
    val bhjs = plan.collect {
      case bhj: BroadcastHashJoinExec => bhj
    }

    if (!conf.optimizedLocalShuffleReaderEnabled || bhjs.isEmpty) {
      return plan
    }

    // If the streamedPlan is `ShuffleQueryStageExec`, set the value of `isLocalShuffle` to true
    bhjs.map {
      case bhj: BroadcastHashJoinExec =>
        bhj.children map {
          case stage: ShuffleQueryStageExec => stage.isLocalShuffle = true
          case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) =>
            stage.isLocalShuffle = true
          case plan: SparkPlan => plan
        }
    }

    // Add the new `LocalShuffleReaderExec` node if the value of `isLocalShuffle` is true
    val newPlan = plan.transformUp {
      case stage: ShuffleQueryStageExec if (stage.isLocalShuffle) =>
        LocalShuffleReaderExec(stage)
      case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) if (stage.isLocalShuffle) =>
        LocalShuffleReaderExec(stage)
    }

    val afterEnsureRequirements = EnsureRequirements(conf).apply(newPlan)
    val numExchanges = afterEnsureRequirements.collect {
      case e: ShuffleExchangeExec => e
    }.length
    if (numExchanges > 0) {
      logWarning("Local shuffle reader optimization is not applied due" +
        " to additional shuffles will be introduced.")
      revertLocalShuffleReader(newPlan)
    } else {
      newPlan
    }
  }
}

case class LocalShuffleReaderExec(
    child: QueryStageExec) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def doCanonicalize(): SparkPlan = child.canonicalized

  override def outputPartitioning: Partitioning = {
    val numPartitions = child match {
      case stage: ShuffleQueryStageExec =>
        stage.plan.shuffleDependency.rdd.partitions.length
      case ReusedQueryStageExec(_, stage: ShuffleQueryStageExec, _) =>
        stage.plan.shuffleDependency.rdd.partitions.length
    }
    UnknownPartitioning(numPartitions)
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
