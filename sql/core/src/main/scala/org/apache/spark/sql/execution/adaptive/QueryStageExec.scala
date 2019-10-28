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
import scala.concurrent.Future

import org.apache.spark.{FutureAction, MapOutputStatistics}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange._


/**
 * A query stage is an independent subgraph of the query plan. Query stage materializes its output
 * before proceeding with further operators of the query plan. The data statistics of the
 * materialized output can be used to optimize subsequent query stages.
 *
 * There are 2 kinds of query stages:
 *   1. Shuffle query stage. This stage materializes its output to shuffle files, and Spark launches
 *      another job to execute the further operators.
 *   2. Broadcast query stage. This stage materializes its output to an array in driver JVM. Spark
 *      broadcasts the array before executing the further operators.
 */
abstract class QueryStageExec extends LeafExecNode {

  /**
   * An id of this query stage which is unique in the entire query plan.
   */
  val id: Int

  /**
   * The sub-tree of the query plan that belongs to this query stage.
   */
  val plan: SparkPlan

  /**
   * Materialize this query stage, to prepare for the execution, like submitting map stages,
   * broadcasting data, etc. The caller side can use the returned [[Future]] to wait until this
   * stage is ready.
   */
  def doMaterialize(): Future[Any]

  /**
   * Cancel the stage materialization if in progress; otherwise do nothing.
   */
  def cancel(): Unit

  /**
   * Materialize this query stage, to prepare for the execution, like submitting map stages,
   * broadcasting data, etc. The caller side can use the returned [[Future]] to wait until this
   * stage is ready.
   */
  final def materialize(): Future[Any] = executeQuery {
    doMaterialize()
  }

  /**
   * Compute the statistics of the query stage if executed, otherwise None.
   */
  def computeStats(): Option[Statistics] = resultOption.map { _ =>
    // Metrics `dataSize` are available in both `ShuffleExchangeExec` and `BroadcastExchangeExec`.
    Statistics(sizeInBytes = plan.metrics("dataSize").value)
  }

  @transient
  @volatile
  private[adaptive] var resultOption: Option[Any] = None

  override def output: Seq[Attribute] = plan.output
  override def outputPartitioning: Partitioning = plan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = plan.outputOrdering
  override def executeCollect(): Array[InternalRow] = plan.executeCollect()
  override def executeTake(n: Int): Array[InternalRow] = plan.executeTake(n)
  override def executeToIterator(): Iterator[InternalRow] = plan.executeToIterator()

  override def doPrepare(): Unit = plan.prepare()
  override def doExecute(): RDD[InternalRow] = plan.execute()
  override def doExecuteBroadcast[T](): Broadcast[T] = plan.executeBroadcast()
  override def doCanonicalize(): SparkPlan = plan.canonicalized

  protected override def stringArgs: Iterator[Any] = Iterator.single(id)

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
    plan.generateTreeString(
      depth + 1, lastChildren :+ true, append, verbose, "", false, maxFields, printNodeId)
  }
}

/**
 * A shuffle query stage whose child is a [[ShuffleExchangeExec]].
 */
case class ShuffleQueryStageExec(
    override val id: Int,
    override val plan: ShuffleExchangeExec) extends QueryStageExec {

  @transient lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (plan.inputRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(plan.shuffleDependency)
    }
  }

  override def doMaterialize(): Future[Any] = {
    mapOutputStatisticsFuture
  }

  override def cancel(): Unit = {
    mapOutputStatisticsFuture match {
      case action: FutureAction[MapOutputStatistics] if !mapOutputStatisticsFuture.isCompleted =>
        action.cancel()
      case _ =>
    }
  }
}

/**
 * A broadcast query stage whose child is a [[BroadcastExchangeExec]].
 */
case class BroadcastQueryStageExec(
    override val id: Int,
    override val plan: BroadcastExchangeExec) extends QueryStageExec {

  override def doMaterialize(): Future[Any] = {
    plan.completionFuture
  }

  override def cancel(): Unit = {
    if (!plan.relationFuture.isDone) {
      sparkContext.cancelJobGroup(plan.runId.toString)
      plan.relationFuture.cancel(true)
    }
  }
}

object ShuffleQueryStageExec {
  /**
   * Returns true if the plan is a [[ShuffleQueryStageExec]] or a reused [[ShuffleQueryStageExec]].
   */
  def isShuffleQueryStageExec(plan: SparkPlan): Boolean = plan match {
    case r: ReusedQueryStageExec => isShuffleQueryStageExec(r.plan)
    case _: ShuffleQueryStageExec => true
    case _ => false
  }
}

object BroadcastQueryStageExec {
  /**
   * Returns true if the plan is a [[BroadcastQueryStageExec]] or a reused
   * [[BroadcastQueryStageExec]].
   */
  def isBroadcastQueryStageExec(plan: SparkPlan): Boolean = plan match {
    case r: ReusedQueryStageExec => isBroadcastQueryStageExec(r.plan)
    case _: BroadcastQueryStageExec => true
    case _ => false
  }
}

/**
 * A wrapper for reused query stage to have different output.
 */
case class ReusedQueryStageExec(
    override val id: Int,
    override val plan: QueryStageExec,
    override val output: Seq[Attribute]) extends QueryStageExec {

  override def doMaterialize(): Future[Any] = {
    plan.materialize()
  }

  override def cancel(): Unit = {
    plan.cancel()
  }

  // `ReusedQueryStageExec` can have distinct set of output attribute ids from its child, we need
  // to update the attribute ids in `outputPartitioning` and `outputOrdering`.
  private lazy val updateAttr: Expression => Expression = {
    val originalAttrToNewAttr = AttributeMap(plan.output.zip(output))
    e => e.transform {
      case attr: Attribute => originalAttrToNewAttr.getOrElse(attr, attr)
    }
  }

  override def outputPartitioning: Partitioning = plan.outputPartitioning match {
    case e: Expression => updateAttr(e).asInstanceOf[Partitioning]
    case other => other
  }

  override def outputOrdering: Seq[SortOrder] = {
    plan.outputOrdering.map(updateAttr(_).asInstanceOf[SortOrder])
  }

  override def computeStats(): Option[Statistics] = plan.computeStats()
}
