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

import scala.concurrent.Future

import org.apache.spark.MapOutputStatistics
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
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
 *   2. Broadcast stage. This stage materializes its output to an array in driver JVM. Spark
 *      broadcasts the array before executing the further operators.
 */
abstract class QueryStageExec extends LeafExecNode {

  /**
   * An id of this query stage which is unique in the entire query plan.
   */
  def id: Int

  /**
   * The sub-tree of the query plan that belongs to this query stage.
   */
  def plan: SparkPlan

  /**
   * Returns a new query stage with a new plan, which is optimized based on accurate runtime data
   * statistics.
   */
  def withNewPlan(newPlan: SparkPlan): QueryStageExec

  /**
   * Materialize this query stage, to prepare for the execution, like submitting map stages,
   * broadcasting data, etc. The caller side can use the returned [[Future]] to wait until this
   * stage is ready.
   */
  def materialize(): Future[Any]

  override def output: Seq[Attribute] = plan.output
  override def outputPartitioning: Partitioning = plan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = plan.outputOrdering
  override def executeCollect(): Array[InternalRow] = plan.executeCollect()
  override def executeTake(n: Int): Array[InternalRow] = plan.executeTake(n)
  override def executeToIterator(): Iterator[InternalRow] = plan.executeToIterator()
  override def doExecute(): RDD[InternalRow] = plan.execute()
  override def doExecuteBroadcast[T](): Broadcast[T] = plan.executeBroadcast()
  override def doCanonicalize(): SparkPlan = plan.canonicalized

  // TODO: maybe we should not hide query stage entirely from explain result.
  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int): Unit = {
    plan.generateTreeString(
      depth, lastChildren, append, verbose, "", false, maxFields)
  }
}

/**
 * A shuffle query stage whose child is a [[ShuffleExchangeExec]].
 */
case class ShuffleQueryStageExec(id: Int, plan: ShuffleExchangeExec) extends QueryStageExec {

  override def withNewPlan(newPlan: SparkPlan): QueryStageExec = {
    copy(plan = newPlan.asInstanceOf[ShuffleExchangeExec])
  }

  @transient lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (plan.inputRDD.getNumPartitions == 0) {
      // `submitMapStage` does not accept RDD with 0 partition. Here we return null and the caller
      // side should take care of it.
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(plan.shuffleDependency)
    }
  }

  override def materialize(): Future[Any] = {
    mapOutputStatisticsFuture
  }
}

/**
 * A broadcast query stage whose child is a [[BroadcastExchangeExec]].
 */
case class BroadcastQueryStageExec(id: Int, plan: BroadcastExchangeExec) extends QueryStageExec {

  override def withNewPlan(newPlan: SparkPlan): QueryStageExec = {
    copy(plan = newPlan.asInstanceOf[BroadcastExchangeExec])
  }

  override def materialize(): Future[Any] = {
    plan.relationFuture
  }
}

/**
 * A wrapper of query stage to indicate that it's reused. Note that itself is not a query stage.
 */
case class ReusedQueryStageExec(child: SparkPlan, output: Seq[Attribute])
  extends UnaryExecNode {

  // Ignore this wrapper for canonicalizing.
  override def doCanonicalize(): SparkPlan = child.canonicalized

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    child.executeBroadcast()
  }

  // `ReusedQueryStageExec` can have distinct set of output attribute ids from its child, we need
  // to update the attribute ids in `outputPartitioning` and `outputOrdering`.
  private lazy val updateAttr: Expression => Expression = {
    val originalAttrToNewAttr = AttributeMap(child.output.zip(output))
    e => e.transform {
      case attr: Attribute => originalAttrToNewAttr.getOrElse(attr, attr)
    }
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning match {
    case e: Expression => updateAttr(e).asInstanceOf[Partitioning]
    case other => other
  }

  override def outputOrdering: Seq[SortOrder] = {
    child.outputOrdering.map(updateAttr(_).asInstanceOf[SortOrder])
  }
}
