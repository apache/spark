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

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Future

import org.apache.spark.{FutureAction, MapOutputStatistics}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.vectorized.ColumnarBatch

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
   * The canonicalized plan before applying query stage optimizer rules.
   */
  val _canonicalized: SparkPlan

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
  final def materialize(): Future[Any] = {
    logDebug(s"Materialize query stage ${this.getClass.getSimpleName}: $id")
    doMaterialize()
  }

  def newReuseInstance(newStageId: Int, newOutput: Seq[Attribute]): QueryStageExec

  /**
   * Returns the runtime statistics after stage materialization.
   */
  def getRuntimeStatistics: Statistics

  /**
   * Compute the statistics of the query stage if executed, otherwise None.
   */
  def computeStats(): Option[Statistics] = if (isMaterialized) {
    val runtimeStats = getRuntimeStatistics
    val dataSize = runtimeStats.sizeInBytes.max(0)
    val numOutputRows = runtimeStats.rowCount.map(_.max(0))
    val attributeStats = runtimeStats.attributeStats
    Some(Statistics(dataSize, numOutputRows, attributeStats, isRuntime = true))
  } else {
    None
  }

  @transient
  @volatile
  protected var _resultOption = new AtomicReference[Option[Any]](None)

  private[adaptive] def resultOption: AtomicReference[Option[Any]] = _resultOption
  def isMaterialized: Boolean = resultOption.get().isDefined

  override def output: Seq[Attribute] = plan.output
  override def outputPartitioning: Partitioning = plan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = plan.outputOrdering
  override def executeCollect(): Array[InternalRow] = plan.executeCollect()
  override def executeTake(n: Int): Array[InternalRow] = plan.executeTake(n)
  override def executeTail(n: Int): Array[InternalRow] = plan.executeTail(n)
  override def executeToIterator(): Iterator[InternalRow] = plan.executeToIterator()

  protected override def doExecute(): RDD[InternalRow] = plan.execute()
  override def supportsColumnar: Boolean = plan.supportsColumnar
  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = plan.executeColumnar()
  override def doExecuteBroadcast[T](): Broadcast[T] = plan.executeBroadcast()
  override def doCanonicalize(): SparkPlan = _canonicalized

  protected override def stringArgs: Iterator[Any] = Iterator.single(id)

  override def simpleStringWithNodeId(): String = {
    super.simpleStringWithNodeId() + computeStats().map(", " + _.toString).getOrElse("")
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {
    super.generateTreeString(depth,
      lastChildren,
      append,
      verbose,
      prefix,
      addSuffix,
      maxFields,
      printNodeId,
      indent)
    plan.generateTreeString(
      depth + 1, lastChildren :+ true, append, verbose, "", false, maxFields, printNodeId, indent)
  }

  override protected[sql] def cleanupResources(): Unit = {
    plan.cleanupResources()
    super.cleanupResources()
  }
}

/**
 * A shuffle query stage whose child is a [[ShuffleExchangeLike]] or [[ReusedExchangeExec]].
 *
 * @param id the query stage id.
 * @param plan the underlying plan.
 * @param _canonicalized the canonicalized plan before applying query stage optimizer rules.
 */
case class ShuffleQueryStageExec(
    override val id: Int,
    override val plan: SparkPlan,
    override val _canonicalized: SparkPlan) extends QueryStageExec {

  @transient val shuffle = plan match {
    case s: ShuffleExchangeLike => s
    case ReusedExchangeExec(_, s: ShuffleExchangeLike) => s
    case _ =>
      throw new IllegalStateException(s"wrong plan for shuffle stage:\n ${plan.treeString}")
  }

  @transient private lazy val shuffleFuture = shuffle.submitShuffleJob

  override def doMaterialize(): Future[Any] = shuffleFuture

  override def newReuseInstance(newStageId: Int, newOutput: Seq[Attribute]): QueryStageExec = {
    val reuse = ShuffleQueryStageExec(
      newStageId,
      ReusedExchangeExec(newOutput, shuffle),
      _canonicalized)
    reuse._resultOption = this._resultOption
    reuse
  }

  override def cancel(): Unit = shuffleFuture match {
    case action: FutureAction[MapOutputStatistics] if !action.isCompleted =>
      action.cancel()
    case _ =>
  }

  /**
   * Returns the Option[MapOutputStatistics]. If the shuffle map stage has no partition,
   * this method returns None, as there is no map statistics.
   */
  def mapStats: Option[MapOutputStatistics] = {
    assert(resultOption.get().isDefined, s"${getClass.getSimpleName} should already be ready")
    val stats = resultOption.get().get.asInstanceOf[MapOutputStatistics]
    Option(stats)
  }

  override def getRuntimeStatistics: Statistics = shuffle.runtimeStatistics
}

/**
 * A broadcast query stage whose child is a [[BroadcastExchangeLike]] or [[ReusedExchangeExec]].
 *
 * @param id the query stage id.
 * @param plan the underlying plan.
 * @param _canonicalized the canonicalized plan before applying query stage optimizer rules.
 */
case class BroadcastQueryStageExec(
    override val id: Int,
    override val plan: SparkPlan,
    override val _canonicalized: SparkPlan) extends QueryStageExec {

  @transient val broadcast = plan match {
    case b: BroadcastExchangeLike => b
    case ReusedExchangeExec(_, b: BroadcastExchangeLike) => b
    case _ =>
      throw new IllegalStateException(s"wrong plan for broadcast stage:\n ${plan.treeString}")
  }

  override def doMaterialize(): Future[Any] = {
    broadcast.submitBroadcastJob
  }

  override def newReuseInstance(newStageId: Int, newOutput: Seq[Attribute]): QueryStageExec = {
    val reuse = BroadcastQueryStageExec(
      newStageId,
      ReusedExchangeExec(newOutput, broadcast),
      _canonicalized)
    reuse._resultOption = this._resultOption
    reuse
  }

  override def cancel(): Unit = {
    if (!broadcast.relationFuture.isDone) {
      sparkContext.cancelJobGroup(broadcast.runId.toString)
      broadcast.relationFuture.cancel(true)
    }
  }

  override def getRuntimeStatistics: Statistics = broadcast.runtimeStatistics
}
