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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate

/**
 * A root node to trigger query stages and execute the query plan adaptively. It incrementally
 * updates the query plan when a query stage is materialized and provides accurate runtime
 * statistics.
 */
case class AdaptiveSparkPlan(initialPlan: ResultQueryStage, session: SparkSession)
  extends LeafExecNode{

  override def output: Seq[Attribute] = initialPlan.output

  @volatile private var currentQueryStage: QueryStage = initialPlan
  @volatile private var error: Throwable = null
  private val readyLock = new CountDownLatch(1)

  private def replaceStage(oldStage: QueryStage, newStage: QueryStage): QueryStage = {
    if (oldStage.id == newStage.id) {
      newStage
    } else {
      val newPlanForOldStage = oldStage.plan.transform {
        case q: QueryStage => replaceStage(q, newStage)
      }
      oldStage.withNewPlan(newPlanForOldStage)
    }
  }

  private def createCallback(executionId: Option[Long]): QueryStageTriggerCallback = {
    new QueryStageTriggerCallback {
      override def onStageUpdated(stage: QueryStage): Unit = {
        updateCurrentQueryStage(stage, executionId)
        if (stage.isInstanceOf[ResultQueryStage]) readyLock.countDown()
      }

      override def onStagePlanningFailed(stage: QueryStage, e: Throwable): Unit = {
        error = new RuntimeException(
          s"""
             |Fail to plan stage ${stage.id}:
             |${stage.plan.treeString}
           """.stripMargin, e)
        readyLock.countDown()
      }

      override def onStageMaterializingFailed(stage: QueryStage, e: Throwable): Unit = {
        error = new RuntimeException(
          s"""
             |Fail to materialize stage ${stage.id}:
             |${stage.plan.treeString}
           """.stripMargin, e)
        readyLock.countDown()
      }

      override def onError(e: Throwable): Unit = {
        error = e
        readyLock.countDown()
      }
    }
  }

  private def updateCurrentQueryStage(newStage: QueryStage, executionId: Option[Long]): Unit = {
    currentQueryStage = replaceStage(currentQueryStage, newStage)
    executionId.foreach { id =>
      session.sparkContext.listenerBus.post(SparkListenerSQLAdaptiveExecutionUpdate(
        id,
        SQLExecution.getQueryExecution(id).toString,
        SparkPlanInfo.fromSparkPlan(currentQueryStage)))
    }
  }

  def resultStage: ResultQueryStage = {
    if (readyLock.getCount > 0) {
      val sc = session.sparkContext
      val executionId = Option(sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).map(_.toLong)
      val trigger = new QueryStageTrigger(session, createCallback(executionId))
      trigger.start()
      trigger.trigger(initialPlan)
      readyLock.await()
      trigger.stop()
    }

    if (error != null) throw error
    currentQueryStage.asInstanceOf[ResultQueryStage]
  }

  override def executeCollect(): Array[InternalRow] = resultStage.executeCollect()
  override def executeTake(n: Int): Array[InternalRow] = resultStage.executeTake(n)
  override def executeToIterator(): Iterator[InternalRow] = resultStage.executeToIterator()
  override def doExecute(): RDD[InternalRow] = resultStage.execute()
  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int): Unit = {
    currentQueryStage.generateTreeString(
      depth, lastChildren, append, verbose, "", false, maxFields)
  }
}
