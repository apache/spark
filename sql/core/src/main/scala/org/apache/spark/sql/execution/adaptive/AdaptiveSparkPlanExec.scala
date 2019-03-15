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

import java.util.concurrent.CountDownLatch

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate

/**
 * A root node to execute the query plan adaptively. It splits the query plan into independent
 * stages and executes them in order according to their dependencies. The query stage
 * materializes its output at the end. When one stage completes, the data statistics of its
 * materialized output will be used to optimize the subsequent stages.
 * This is called mid-query re-optimization in database literature.
 */
case class AdaptiveSparkPlanExec(initialPlan: SparkPlan, session: SparkSession)
  extends LeafExecNode{

  override def output: Seq[Attribute] = initialPlan.output

  @volatile private var currentPlan: SparkPlan = initialPlan
  @volatile private var error: Throwable = null

  // We will release the lock when all the query stages are completed, or we fail to
  // optimize/execute query stages. Getting `finalPlan` will be blocked until the lock is release.
  // This is better than wait()/notify(), as we can easily check if the computation has completed,
  // by calling `readyLock.getCount()`.
  private val readyLock = new CountDownLatch(1)

  private def createCallback(executionId: Option[Long]) = new QueryStageManagerCallback {
    override def onPlanUpdate(updatedPlan: SparkPlan): Unit = {
      updateCurrentPlan(updatedPlan, executionId)
    }

    override def onFinalPlan(finalPlan: SparkPlan): Unit = {
      updateCurrentPlan(finalPlan, executionId)
      readyLock.countDown()
    }

    override def onStageMaterializationFailed(stage: QueryStageExec, e: Throwable): Unit = {
      error = new SparkException(
        s"""
           |Fail to materialize query stage ${stage.id}:
           |${stage.plan.treeString}
           """.stripMargin, e)
      readyLock.countDown()
    }

    override def onError(e: Throwable): Unit = {
      error = e
      readyLock.countDown()
    }
  }

  private def updateCurrentPlan(newPlan: SparkPlan, executionId: Option[Long]): Unit = {
    currentPlan = newPlan
    executionId.foreach { id =>
      session.sparkContext.listenerBus.post(SparkListenerSQLAdaptiveExecutionUpdate(
        id,
        SQLExecution.getQueryExecution(id).toString,
        SparkPlanInfo.fromSparkPlan(currentPlan)))
    }
  }

  def finalPlan: SparkPlan = {
    if (readyLock.getCount > 0) {
      val sc = session.sparkContext
      val executionId = Option(sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).map(_.toLong)
      val stageManager = new QueryStageManager(initialPlan, session, createCallback(executionId))
      stageManager.start()
      readyLock.await()
      stageManager.stop()
    }

    if (error != null) throw error
    currentPlan
  }

  override def executeCollect(): Array[InternalRow] = finalPlan.executeCollect()
  override def executeTake(n: Int): Array[InternalRow] = finalPlan.executeTake(n)
  override def executeToIterator(): Iterator[InternalRow] = finalPlan.executeToIterator()
  override def doExecute(): RDD[InternalRow] = finalPlan.execute()
  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int): Unit = {
    currentPlan.generateTreeString(
      depth, lastChildren, append, verbose, "", false, maxFields)
  }
}
