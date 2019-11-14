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

/**
 * This class tracks live SQL executions, and pass the list to the [[SQLLiveEntitiesEventFilter]]
 * to help SQLLiveEntitiesEventFilter to filter out finished SQL executions as well as relevant
 * jobs (+ stages/tasks/RDDs). Unlike BasicEventFilterBuilder, it doesn't concern about the status
 * of individual job - it only concerns whether SQL execution is finished or not.
 */
private[spark] class SQLEventFilterBuilder extends SparkListener with EventFilterBuilder {
  private val _liveExecutionToJobs = new mutable.HashMap[Long, mutable.Set[Int]]
  private val _jobToStages = new mutable.HashMap[Int, Seq[Int]]
  private val _stageToTasks = new mutable.HashMap[Int, mutable.Set[Long]]
  private val _stageToRDDs = new mutable.HashMap[Int, Seq[Int]]
  private val stages = new mutable.HashSet[Int]

  def liveExecutionToJobs: Map[Long, Set[Int]] = _liveExecutionToJobs.mapValues(_.toSet).toMap
  def jobToStages: Map[Int, Seq[Int]] = _jobToStages.toMap
  def stageToTasks: Map[Int, Set[Long]] = _stageToTasks.mapValues(_.toSet).toMap
  def stageToRDDs: Map[Int, Seq[Int]] = _stageToRDDs.toMap

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val executionIdString = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString == null) {
      // This is not a job created by SQL
      return
    }

    val executionId = executionIdString.toLong
    val jobId = jobStart.jobId

    val jobsForExecution = _liveExecutionToJobs.getOrElseUpdate(executionId,
      mutable.HashSet[Int]())
    jobsForExecution += jobId

    _jobToStages += jobStart.jobId -> jobStart.stageIds
    stages ++= jobStart.stageIds
    jobStart.stageIds.foreach { stageId => _stageToTasks += stageId -> mutable.HashSet[Long]() }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageId = stageSubmitted.stageInfo.stageId
    if (stages.contains(stageId)) {
      val rddInfos = stageSubmitted.stageInfo.rddInfos
      _stageToRDDs += stageId -> rddInfos.map(_.id)
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    if (_stageToTasks.contains(taskStart.stageId)) {
      val curTasks = _stageToTasks(taskStart.stageId)
      curTasks += taskStart.taskInfo.taskId
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart => onExecutionStart(e)
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    case _ => // Ignore
  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    _liveExecutionToJobs += event.executionId -> mutable.HashSet[Int]()
  }

  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val jobs = _liveExecutionToJobs.getOrElse(event.executionId, mutable.HashSet[Int]())
    _liveExecutionToJobs -= event.executionId

    val stagesToDrop = _jobToStages.filter(kv => jobs.contains(kv._1)).values.flatten
    _jobToStages --= jobs
    stages --= stagesToDrop
    _stageToTasks --= stagesToDrop
    _stageToRDDs --= stagesToDrop
  }

  override def createFilter(): EventFilter = {
    SQLLiveEntitiesEventFilter(this)
  }
}

/**
 * This class filters out events which are related to the finished SQL executions based on the
 * given information.
 *
 * Note that filterXXX methods will return None instead of Some(false) if the event is related to
 * job but not coupled with live SQL executions, because the instance has the information about
 * jobs for live SQL executions which should be filtered in, but don't know whether the job is
 * related to the finished SQL executions, or job is NOT related to the SQL executions. For this
 * case, it just gives up the decision and let other filters decide it.
 *
 * The events which are not related to the SQL execution will be considered as "Don't mind".
 */
private[spark] class SQLLiveEntitiesEventFilter(
    liveExecutionToJobs: Map[Long, Set[Int]],
    jobToStages: Map[Int, Seq[Int]],
    stageToTasks: Map[Int, Set[Long]],
    stageToRDDs: Map[Int, Seq[Int]]) extends EventFilter with Logging {

  private val liveTasks: Set[Long] = stageToTasks.values match {
    case xs if xs.isEmpty => Set.empty[Long]
    case xs => xs.reduce(_ ++ _).toSet
  }

  private val liveRDDs: Set[Int] = stageToRDDs.values match {
    case xs if xs.isEmpty => Set.empty[Int]
    case xs => xs.reduce(_ ++ _).toSet
  }

  if (log.isDebugEnabled) {
    logDebug(s"live executions : ${liveExecutionToJobs.keySet}")
    logDebug(s"jobs in live executions : ${liveExecutionToJobs.values.flatten}")
    logDebug(s"jobs : ${jobToStages.keySet}")
    logDebug(s"stages in jobs : ${jobToStages.values.flatten}")
    logDebug(s"stages : ${stageToTasks.keySet}")
    logDebug(s"tasks in stages : ${stageToTasks.values.flatten}")
    logDebug(s"RDDs in stages : ${stageToRDDs.values.flatten}")
  }

  override def filterStageCompleted(event: SparkListenerStageCompleted): Option[Boolean] = {
    trueOrNone(stageToTasks.contains(event.stageInfo.stageId))
  }

  override def filterStageSubmitted(event: SparkListenerStageSubmitted): Option[Boolean] = {
    trueOrNone(stageToTasks.contains(event.stageInfo.stageId))
  }

  override def filterTaskStart(event: SparkListenerTaskStart): Option[Boolean] = {
    trueOrNone(liveTasks.contains(event.taskInfo.taskId))
  }

  override def filterTaskGettingResult(event: SparkListenerTaskGettingResult): Option[Boolean] = {
    trueOrNone(liveTasks.contains(event.taskInfo.taskId))
  }

  override def filterTaskEnd(event: SparkListenerTaskEnd): Option[Boolean] = {
    trueOrNone(liveTasks.contains(event.taskInfo.taskId))
  }

  override def filterJobStart(event: SparkListenerJobStart): Option[Boolean] = {
    trueOrNone(jobToStages.contains(event.jobId))
  }

  override def filterJobEnd(event: SparkListenerJobEnd): Option[Boolean] = {
    trueOrNone(jobToStages.contains(event.jobId))
  }

  override def filterUnpersistRDD(event: SparkListenerUnpersistRDD): Option[Boolean] = {
    trueOrNone(liveRDDs.contains(event.rddId))
  }

  override def filterExecutorMetricsUpdate(
      event: SparkListenerExecutorMetricsUpdate): Option[Boolean] = {
    trueOrNone(event.accumUpdates.exists { case (_, stageId, _, _) =>
      stageToTasks.contains(stageId)
    })
  }

  override def filterSpeculativeTaskSubmitted(
      event: SparkListenerSpeculativeTaskSubmitted): Option[Boolean] = {
    trueOrNone(stageToTasks.contains(event.stageId))
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
    Some(liveExecutionToJobs.contains(event.executionId))
  }

  def filterAdaptiveExecutionUpdate(
      event: SparkListenerSQLAdaptiveExecutionUpdate): Option[Boolean] = {
    Some(liveExecutionToJobs.contains(event.executionId))
  }

  def filterExecutionEnd(event: SparkListenerSQLExecutionEnd): Option[Boolean] = {
    Some(liveExecutionToJobs.contains(event.executionId))
  }

  def filterDriverAccumUpdates(event: SparkListenerDriverAccumUpdates): Option[Boolean] = {
    Some(liveExecutionToJobs.contains(event.executionId))
  }

  private def trueOrNone(booleanValue: Boolean): Option[Boolean] = {
    if (booleanValue) {
      Some(booleanValue)
    } else {
      None
    }
  }
}

private[spark] object SQLLiveEntitiesEventFilter {
  def apply(builder: SQLEventFilterBuilder): SQLLiveEntitiesEventFilter = {
    new SQLLiveEntitiesEventFilter(
      builder.liveExecutionToJobs,
      builder.jobToStages,
      builder.stageToTasks,
      builder.stageToRDDs)
  }
}
