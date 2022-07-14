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

package org.apache.spark.sql.diagnostic

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.internal.StaticSQLConf.UI_RETAINED_EXECUTIONS
import org.apache.spark.status.{ElementTrackingStore, KVUtils}

/**
 * A Spark listener that writes diagnostic information to a data store. The information can be
 * accessed by the public REST API.
 *
 * @param kvStore used to store the diagnostic information
 */
private[spark] class DiagnosticListener(
    conf: SparkConf,
    kvStore: ElementTrackingStore) extends SparkListener {

  kvStore.addTrigger(
    classOf[ExecutionDiagnosticData],
    conf.get(UI_RETAINED_EXECUTIONS)) { count =>
    cleanupExecutions(count)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart => onExecutionStart(e)
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    case e: SparkListenerSQLAdaptiveExecutionUpdate => onAdaptiveExecutionUpdate(e)
    case _ => // Ignore
  }

  private def onAdaptiveExecutionUpdate(event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    val data = new AdaptiveExecutionUpdate(
      event.executionId,
      System.currentTimeMillis(),
      event.physicalPlanDescription
    )
    kvStore.write(data)
  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    val sqlConf = event.qe.sparkSession.sessionState.conf
    val planDescriptionMode = ExplainMode.fromString(sqlConf.uiExplainMode)
    val physicalPlan = event.qe.explainString(
      planDescriptionMode, sqlConf.maxToStringFieldsForDiagnostic)
    val data = new ExecutionDiagnosticData(
      event.executionId,
      physicalPlan,
      event.time,
      None,
      None
    )
    // Check triggers since it's adding new netries
    kvStore.write(data, checkTriggers = true)
  }

  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    try {
      val existing = kvStore.read(classOf[ExecutionDiagnosticData], event.executionId)
      val sqlConf = event.qe.sparkSession.sessionState.conf
      val planDescriptionMode = ExplainMode.fromString(sqlConf.uiExplainMode)
      val physicalPlan = event.qe.explainString(
        planDescriptionMode, sqlConf.maxToStringFieldsForDiagnostic)
      val data = new ExecutionDiagnosticData(
        event.executionId,
        physicalPlan,
        existing.submissionTime,
        Some(event.time),
        event.executionFailure.map(
          e => s"${e.getClass.getCanonicalName}: ${e.getMessage}").orElse(Some(""))
      )
      kvStore.write(data)
    } catch {
      case _: NoSuchElementException =>
      // this is possibly caused by the query failed before execution.
    }
  }

  private def cleanupExecutions(count: Long): Unit = {
    val countToDelete = count - conf.get(UI_RETAINED_EXECUTIONS)
    if (countToDelete <= 0) {
      return
    }
    val view = kvStore.view(classOf[ExecutionDiagnosticData]).index("completionTime").first(0L)
    val toDelete = KVUtils.viewToSeq(view, countToDelete.toInt)(_.completionTime.isDefined)
    toDelete.foreach(e => kvStore.delete(classOf[ExecutionDiagnosticData], e.executionId))
    kvStore.removeAllByIndexValues(
      classOf[AdaptiveExecutionUpdate], "id", toDelete.map(_.executionId))
  }
}

private[spark] object DiagnosticListener {
  val QUEUE_NAME = "diagnostics"
}
