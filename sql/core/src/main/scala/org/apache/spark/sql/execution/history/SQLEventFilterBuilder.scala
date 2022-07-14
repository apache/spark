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

import org.apache.spark.deploy.history.{EventFilter, EventFilterBuilder, JobEventFilter}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui._
import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * This class tracks live SQL executions, and pass the list to the [[SQLLiveEntitiesEventFilter]]
 * to help SQLLiveEntitiesEventFilter to accept live SQL executions as well as relevant
 * jobs (+ stages/tasks/RDDs).
 *
 * Note that this class only tracks the jobs which are relevant to SQL executions - cannot classify
 * between finished job and live job without relation of SQL execution.
 */
private[spark] class SQLEventFilterBuilder extends SparkListener with EventFilterBuilder {
  private val liveExecutionToJobs = new mutable.HashMap[Long, mutable.Set[Int]]
  private val jobToStages = new mutable.HashMap[Int, Set[Int]]
  private val stageToTasks = new mutable.HashMap[Int, mutable.Set[Long]]
  private val stageToRDDs = new mutable.HashMap[Int, Set[Int]]
  private val stages = new mutable.HashSet[Int]

  private[history] def liveSQLExecutions: Set[Long] = liveExecutionToJobs.keySet.toSet
  private[history] def liveJobs: Set[Int] = liveExecutionToJobs.values.flatten.toSet
  private[history] def liveStages: Set[Int] = stageToRDDs.keySet.toSet
  private[history] def liveTasks: Set[Long] = stageToTasks.values.flatten.toSet
  private[history] def liveRDDs: Set[Int] = stageToRDDs.values.flatten.toSet

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

    jobToStages += jobStart.jobId -> jobStart.stageIds.toSet
    stages ++= jobStart.stageIds
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageId = stageSubmitted.stageInfo.stageId
    if (stages.contains(stageId)) {
      stageToRDDs.put(stageId, stageSubmitted.stageInfo.rddInfos.map(_.id).toSet)
      stageToTasks.getOrElseUpdate(stageId, new mutable.HashSet[Long]())
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    stageToTasks.get(taskStart.stageId).foreach { tasks =>
      tasks += taskStart.taskInfo.taskId
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
    liveExecutionToJobs.remove(event.executionId).foreach { jobs =>
      val stagesToDrop = jobToStages.filter(kv => jobs.contains(kv._1)).values.flatten
      jobToStages --= jobs
      stages --= stagesToDrop
      stageToTasks --= stagesToDrop
      stageToRDDs --= stagesToDrop
    }
  }

  override def createFilter(): EventFilter = {
    new SQLLiveEntitiesEventFilter(liveSQLExecutions, liveJobs, liveStages, liveTasks, liveRDDs)
  }
}

/**
 * This class accepts events which are related to the live SQL executions based on the given
 * information.
 *
 * Note that acceptFn will not match the event ("Don't mind") instead of returning false on
 * job related events, because it cannot determine whether the job is related to the finished
 * SQL executions, or job is NOT related to the SQL executions. For this case, it just gives up
 * the decision and let other filters decide it.
 */
private[spark] class SQLLiveEntitiesEventFilter(
    liveSQLExecutions: Set[Long],
    liveJobs: Set[Int],
    liveStages: Set[Int],
    liveTasks: Set[Long],
    liveRDDs: Set[Int])
  extends JobEventFilter(None, liveJobs, liveStages, liveTasks, liveRDDs) with Logging {

  logDebug(s"live SQL executions : $liveSQLExecutions")

  private val _acceptFn: PartialFunction[SparkListenerEvent, Boolean] = {
    case e: SparkListenerSQLExecutionStart =>
      liveSQLExecutions.contains(e.executionId)
    case e: SparkListenerSQLAdaptiveExecutionUpdate =>
      liveSQLExecutions.contains(e.executionId)
    case e: SparkListenerSQLExecutionEnd =>
      liveSQLExecutions.contains(e.executionId)
    case e: SparkListenerDriverAccumUpdates =>
      liveSQLExecutions.contains(e.executionId)

    case e if acceptFnForJobEvents.lift(e).contains(true) =>
      // NOTE: if acceptFnForJobEvents(e) returns false, we should leave it to "unmatched"
      // because we don't know whether the job has relevant SQL execution which is finished,
      // or the job is not related to the SQL execution.
      true

    // these events are for finished batches so safer to ignore
    case _: StreamingQueryListener.QueryProgressEvent => false
  }

  override def acceptFn(): PartialFunction[SparkListenerEvent, Boolean] = _acceptFn
}
