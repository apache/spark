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

package org.apache.spark.sql.execution.history

import scala.collection.mutable

import org.apache.spark.deploy.history.{EventFilter, EventFilterBuilder}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui._
import org.apache.spark.sql.streaming.StreamingQueryListener

// FIXME: UTs
class SQLEventFilterBuilder extends SparkListener with EventFilterBuilder {
  val liveExecutionToJobs = new mutable.HashMap[Long, mutable.Set[Int]]
  val jobToStages = new mutable.HashMap[Int, Seq[Int]]
  val stageToTasks = new mutable.HashMap[Int, mutable.Set[Long]]

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val executionIdString = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString == null) {
      // This is not a job created by SQL
      return
    }

    val executionId = executionIdString.toLong
    val jobId = jobStart.jobId

    val jobsForExecution = liveExecutionToJobs.getOrElseUpdate(executionId,
      mutable.HashSet[Int]())
    jobsForExecution += jobId

    jobToStages += jobStart.jobId -> jobStart.stageIds
    jobStart.stageIds.foreach { stageId => stageToTasks += stageId -> mutable.HashSet[Long]() }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    if (stageToTasks.contains(taskStart.stageId)) {
      val curTasks = stageToTasks(taskStart.stageId)
      curTasks += taskStart.taskInfo.taskId
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart => onExecutionStart(e)
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    case _ => // Ignore
  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    liveExecutionToJobs += event.executionId -> mutable.HashSet[Int]()
  }

  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val jobs = liveExecutionToJobs.getOrElse(event.executionId, mutable.HashSet[Int]())
    liveExecutionToJobs -= event.executionId

    val stages = jobToStages.filter(kv => jobs.contains(kv._1)).values.flatten
    jobToStages --= jobs
    stageToTasks --= stages
  }

  override def createFilter(): EventFilter = {
    new SQLLiveEntitiesEventFilter(this)
  }
}

// FIXME: UTs
class SQLLiveEntitiesEventFilter(trackListener: SQLEventFilterBuilder)
  extends EventFilter with Logging {

  private val liveTasks: Set[Long] = trackListener.stageToTasks.values match {
    case xs if xs.isEmpty => Set.empty[Long]
    case xs => xs.reduce(_ ++ _).toSet
  }

  if (log.isDebugEnabled) {
    logDebug(s"live executions : ${trackListener.liveExecutionToJobs.keySet}")
    logDebug(s"jobs in live executions : ${trackListener.liveExecutionToJobs.values.flatten}")
    logDebug(s"jobs : ${trackListener.jobToStages.keySet}")
    logDebug(s"stages in jobs : ${trackListener.jobToStages.values.flatten}")
    logDebug(s"stages : ${trackListener.stageToTasks.keySet}")
    logDebug(s"tasks in stages : ${trackListener.stageToTasks.values.flatten}")
  }

  override def filterStageCompleted(event: SparkListenerStageCompleted): Option[Boolean] = {
    Some(trackListener.stageToTasks.contains(event.stageInfo.stageId))
  }

  override def filterStageSubmitted(event: SparkListenerStageSubmitted): Option[Boolean] = {
    Some(trackListener.stageToTasks.contains(event.stageInfo.stageId))
  }

  override def filterTaskStart(event: SparkListenerTaskStart): Option[Boolean] = {
    Some(liveTasks.contains(event.taskInfo.taskId))
  }

  override def filterTaskGettingResult(event: SparkListenerTaskGettingResult): Option[Boolean] = {
    Some(liveTasks.contains(event.taskInfo.taskId))
  }

  override def filterTaskEnd(event: SparkListenerTaskEnd): Option[Boolean] = {
    Some(liveTasks.contains(event.taskInfo.taskId))
  }

  override def filterJobStart(event: SparkListenerJobStart): Option[Boolean] = {
    Some(trackListener.jobToStages.contains(event.jobId))
  }

  override def filterJobEnd(event: SparkListenerJobEnd): Option[Boolean] = {
    Some(trackListener.jobToStages.contains(event.jobId))
  }

  override def filterExecutorMetricsUpdate(
      event: SparkListenerExecutorMetricsUpdate): Option[Boolean] = {
    Some(event.accumUpdates.exists { case (_, stageId, _, _) =>
      trackListener.stageToTasks.contains(stageId)
    })
  }

  override def filterOtherEvent(event: SparkListenerEvent): Option[Boolean] = event match {
    case e: SparkListenerSQLExecutionStart => filterExecutionStart(e)
    case e: SparkListenerSQLAdaptiveExecutionUpdate => filterAdaptiveExecutionUpdate(e)
    case e: SparkListenerSQLExecutionEnd => filterExecutionEnd(e)
    case e: SparkListenerDriverAccumUpdates => filterDriverAccumUpdates(e)

      // these events are for finished batches so safer to ignore
    case _: StreamingQueryListener.QueryProgressEvent => Some(false)
    case _ => None
  }

  def filterExecutionStart(event: SparkListenerSQLExecutionStart): Option[Boolean] = {
    Some(trackListener.liveExecutionToJobs.contains(event.executionId))
  }

  def filterAdaptiveExecutionUpdate(
      event: SparkListenerSQLAdaptiveExecutionUpdate): Option[Boolean] = {
    Some(trackListener.liveExecutionToJobs.contains(event.executionId))
  }

  def filterExecutionEnd(event: SparkListenerSQLExecutionEnd): Option[Boolean] = {
    Some(trackListener.liveExecutionToJobs.contains(event.executionId))
  }

  def filterDriverAccumUpdates(event: SparkListenerDriverAccumUpdates): Option[Boolean] = {
    Some(trackListener.liveExecutionToJobs.contains(event.executionId))
  }
}
