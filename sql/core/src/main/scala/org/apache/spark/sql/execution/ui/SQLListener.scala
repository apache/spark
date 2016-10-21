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

package org.apache.spark.sql.execution.ui

import scala.collection.mutable

import org.apache.spark.{JobExecutionStatus, SparkConf}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.{SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.metric._
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.AccumulatorContext

@DeveloperApi
case class SparkListenerSQLExecutionStart(
    executionId: Long,
    description: String,
    details: String,
    physicalPlanDescription: String,
    sparkPlanInfo: SparkPlanInfo,
    time: Long)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerSQLExecutionEnd(executionId: Long, time: Long)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerDriverAccumUpdates(executionId: Long, accumUpdates: Seq[(Long, Long)])
  extends SparkListenerEvent

class SQLHistoryListenerFactory extends SparkHistoryListenerFactory {

  override def createListeners(conf: SparkConf, sparkUI: SparkUI): Seq[SparkListener] = {
    List(new SQLHistoryListener(conf, sparkUI))
  }
}

class SQLListener(conf: SparkConf) extends SparkListener with Logging {

  private val retainedExecutions = conf.getInt("spark.sql.ui.retainedExecutions", 1000)

  private val activeExecutions = mutable.HashMap[Long, SQLExecutionUIData]()

  // Old data in the following fields must be removed in "trimExecutionsIfNecessary".
  // If adding new fields, make sure "trimExecutionsIfNecessary" can clean up old data
  private val _executionIdToData = mutable.HashMap[Long, SQLExecutionUIData]()

  /**
   * Maintain the relation between job id and execution id so that we can get the execution id in
   * the "onJobEnd" method.
   */
  private val _jobIdToExecutionId = mutable.HashMap[Long, Long]()

  private val _stageIdToStageMetrics = mutable.HashMap[Long, SQLStageMetrics]()

  private val failedExecutions = mutable.ListBuffer[SQLExecutionUIData]()

  private val completedExecutions = mutable.ListBuffer[SQLExecutionUIData]()

  def executionIdToData: Map[Long, SQLExecutionUIData] = synchronized {
    _executionIdToData.toMap
  }

  def jobIdToExecutionId: Map[Long, Long] = synchronized {
    _jobIdToExecutionId.toMap
  }

  def stageIdToStageMetrics: Map[Long, SQLStageMetrics] = synchronized {
    _stageIdToStageMetrics.toMap
  }

  private def trimExecutionsIfNecessary(
      executions: mutable.ListBuffer[SQLExecutionUIData]): Unit = {
    if (executions.size > retainedExecutions) {
      val toRemove = math.max(retainedExecutions / 10, 1)
      executions.take(toRemove).foreach { execution =>
        for (executionUIData <- _executionIdToData.remove(execution.executionId)) {
          for (jobId <- executionUIData.jobs.keys) {
            _jobIdToExecutionId.remove(jobId)
          }
          for (stageId <- executionUIData.stages) {
            _stageIdToStageMetrics.remove(stageId)
          }
        }
      }
      executions.trimStart(toRemove)
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val executionIdString = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString == null) {
      // This is not a job created by SQL
      return
    }
    val executionId = executionIdString.toLong
    val jobId = jobStart.jobId
    val stageIds = jobStart.stageIds

    synchronized {
      activeExecutions.get(executionId).foreach { executionUIData =>
        executionUIData.jobs(jobId) = JobExecutionStatus.RUNNING
        executionUIData.stages ++= stageIds
        stageIds.foreach(stageId =>
          _stageIdToStageMetrics(stageId) = new SQLStageMetrics(stageAttemptId = 0))
        _jobIdToExecutionId(jobId) = executionId
      }
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    val jobId = jobEnd.jobId
    for (executionId <- _jobIdToExecutionId.get(jobId);
         executionUIData <- _executionIdToData.get(executionId)) {
      jobEnd.jobResult match {
        case JobSucceeded => executionUIData.jobs(jobId) = JobExecutionStatus.SUCCEEDED
        case JobFailed(_) => executionUIData.jobs(jobId) = JobExecutionStatus.FAILED
      }
      if (executionUIData.completionTime.nonEmpty && !executionUIData.hasRunningJobs) {
        // We are the last job of this execution, so mark the execution as finished. Note that
        // `onExecutionEnd` also does this, but currently that can be called before `onJobEnd`
        // since these are called on different threads.
        markExecutionFinished(executionId)
      }
    }
  }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = synchronized {
    for ((taskId, stageId, stageAttemptID, accumUpdates) <- executorMetricsUpdate.accumUpdates) {
      updateTaskAccumulatorValues(taskId, stageId, stageAttemptID, accumUpdates, finishTask = false)
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = synchronized {
    val stageId = stageSubmitted.stageInfo.stageId
    val stageAttemptId = stageSubmitted.stageInfo.attemptId
    // Always override metrics for old stage attempt
    if (_stageIdToStageMetrics.contains(stageId)) {
      _stageIdToStageMetrics(stageId) = new SQLStageMetrics(stageAttemptId)
    } else {
      // If a stage belongs to some SQL execution, its stageId will be put in "onJobStart".
      // Since "_stageIdToStageMetrics" doesn't contain it, it must not belong to any SQL execution.
      // So we can ignore it. Otherwise, this may lead to memory leaks (SPARK-11126).
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    if (taskEnd.taskMetrics != null) {
      updateTaskAccumulatorValues(
        taskEnd.taskInfo.taskId,
        taskEnd.stageId,
        taskEnd.stageAttemptId,
        taskEnd.taskMetrics.externalAccums.map(a => a.toInfo(Some(a.value), None)),
        finishTask = true)
    }
  }

  /**
   * Update the accumulator values of a task with the latest metrics for this task. This is called
   * every time we receive an executor heartbeat or when a task finishes.
   */
  protected def updateTaskAccumulatorValues(
      taskId: Long,
      stageId: Int,
      stageAttemptID: Int,
      _accumulatorUpdates: Seq[AccumulableInfo],
      finishTask: Boolean): Unit = {
    val accumulatorUpdates =
      _accumulatorUpdates.filter(_.update.isDefined).map(accum => (accum.id, accum.update.get))

    _stageIdToStageMetrics.get(stageId) match {
      case Some(stageMetrics) =>
        if (stageAttemptID < stageMetrics.stageAttemptId) {
          // A task of an old stage attempt. Because a new stage is submitted, we can ignore it.
        } else if (stageAttemptID > stageMetrics.stageAttemptId) {
          logWarning(s"A task should not have a higher stageAttemptID ($stageAttemptID) then " +
            s"what we have seen (${stageMetrics.stageAttemptId})")
        } else {
          // TODO We don't know the attemptId. Currently, what we can do is overriding the
          // accumulator updates. However, if there are two same task are running, such as
          // speculation, the accumulator updates will be overriding by different task attempts,
          // the results will be weird.
          stageMetrics.taskIdToMetricUpdates.get(taskId) match {
            case Some(taskMetrics) =>
              if (finishTask) {
                taskMetrics.finished = true
                taskMetrics.accumulatorUpdates = accumulatorUpdates
              } else if (!taskMetrics.finished) {
                taskMetrics.accumulatorUpdates = accumulatorUpdates
              } else {
                // If a task is finished, we should not override with accumulator updates from
                // heartbeat reports
              }
            case None =>
              // TODO Now just set attemptId to 0. Should fix here when we can get the attempt
              // id from SparkListenerExecutorMetricsUpdate
              stageMetrics.taskIdToMetricUpdates(taskId) = new SQLTaskMetrics(
                  attemptId = 0, finished = finishTask, accumulatorUpdates)
          }
        }
      case None =>
      // This execution and its stage have been dropped
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case SparkListenerSQLExecutionStart(executionId, description, details,
      physicalPlanDescription, sparkPlanInfo, time) =>
      val physicalPlanGraph = SparkPlanGraph(sparkPlanInfo)
      val sqlPlanMetrics = physicalPlanGraph.allNodes.flatMap { node =>
        node.metrics.map(metric => metric.accumulatorId -> metric)
      }
      val executionUIData = new SQLExecutionUIData(
        executionId,
        description,
        details,
        physicalPlanDescription,
        physicalPlanGraph,
        sqlPlanMetrics.toMap,
        time)
      synchronized {
        activeExecutions(executionId) = executionUIData
        _executionIdToData(executionId) = executionUIData
      }
    case SparkListenerSQLExecutionEnd(executionId, time) => synchronized {
      _executionIdToData.get(executionId).foreach { executionUIData =>
        executionUIData.completionTime = Some(time)
        if (!executionUIData.hasRunningJobs) {
          // onExecutionEnd happens after all "onJobEnd"s
          // So we should update the execution lists.
          markExecutionFinished(executionId)
        } else {
          // There are some running jobs, onExecutionEnd happens before some "onJobEnd"s.
          // Then we don't if the execution is successful, so let the last onJobEnd updates the
          // execution lists.
        }
      }
    }
    case SparkListenerDriverAccumUpdates(executionId, accumUpdates) => synchronized {
      _executionIdToData.get(executionId).foreach { executionUIData =>
        for ((accId, accValue) <- accumUpdates) {
          executionUIData.driverAccumUpdates(accId) = accValue
        }
      }
    }
    case _ => // Ignore
  }

  private def markExecutionFinished(executionId: Long): Unit = {
    activeExecutions.remove(executionId).foreach { executionUIData =>
      if (executionUIData.isFailed) {
        failedExecutions += executionUIData
        trimExecutionsIfNecessary(failedExecutions)
      } else {
        completedExecutions += executionUIData
        trimExecutionsIfNecessary(completedExecutions)
      }
    }
  }

  def getRunningExecutions: Seq[SQLExecutionUIData] = synchronized {
    activeExecutions.values.toSeq
  }

  def getFailedExecutions: Seq[SQLExecutionUIData] = synchronized {
    failedExecutions
  }

  def getCompletedExecutions: Seq[SQLExecutionUIData] = synchronized {
    completedExecutions
  }

  def getExecution(executionId: Long): Option[SQLExecutionUIData] = synchronized {
    _executionIdToData.get(executionId)
  }

  /**
   * Get all accumulator updates from all tasks which belong to this execution and merge them.
   */
  def getExecutionMetrics(executionId: Long): Map[Long, String] = synchronized {
    _executionIdToData.get(executionId) match {
      case Some(executionUIData) =>
        val accumulatorUpdates = {
          for (stageId <- executionUIData.stages;
               stageMetrics <- _stageIdToStageMetrics.get(stageId).toIterable;
               taskMetrics <- stageMetrics.taskIdToMetricUpdates.values;
               accumulatorUpdate <- taskMetrics.accumulatorUpdates) yield {
            (accumulatorUpdate._1, accumulatorUpdate._2)
          }
        }.filter { case (id, _) => executionUIData.accumulatorMetrics.contains(id) }

        val driverUpdates = executionUIData.driverAccumUpdates.toSeq
        mergeAccumulatorUpdates(accumulatorUpdates ++ driverUpdates, accumulatorId =>
          executionUIData.accumulatorMetrics(accumulatorId).metricType)
      case None =>
        // This execution has been dropped
        Map.empty
    }
  }

  private def mergeAccumulatorUpdates(
      accumulatorUpdates: Seq[(Long, Any)],
      metricTypeFunc: Long => String): Map[Long, String] = {
    accumulatorUpdates.groupBy(_._1).map { case (accumulatorId, values) =>
      val metricType = metricTypeFunc(accumulatorId)
      accumulatorId ->
        SQLMetrics.stringValue(metricType, values.map(_._2.asInstanceOf[Long]))
    }
  }

}


/**
 * A [[SQLListener]] for rendering the SQL UI in the history server.
 */
class SQLHistoryListener(conf: SparkConf, sparkUI: SparkUI)
  extends SQLListener(conf) {

  private var sqlTabAttached = false

  override def onExecutorMetricsUpdate(u: SparkListenerExecutorMetricsUpdate): Unit = {
    // Do nothing; these events are not logged
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    updateTaskAccumulatorValues(
      taskEnd.taskInfo.taskId,
      taskEnd.stageId,
      taskEnd.stageAttemptId,
      taskEnd.taskInfo.accumulables.flatMap { a =>
        // Filter out accumulators that are not SQL metrics
        // For now we assume all SQL metrics are Long's that have been JSON serialized as String's
        if (a.metadata == Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER)) {
          val newValue = a.update.map(_.toString.toLong).getOrElse(0L)
          Some(a.copy(update = Some(newValue)))
        } else {
          None
        }
      },
      finishTask = true)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case _: SparkListenerSQLExecutionStart =>
      if (!sqlTabAttached) {
        new SQLTab(this, sparkUI)
        sqlTabAttached = true
      }
      super.onOtherEvent(event)
    case _ => super.onOtherEvent(event)
  }
}

/**
 * Represent all necessary data for an execution that will be used in Web UI.
 */
private[ui] class SQLExecutionUIData(
    val executionId: Long,
    val description: String,
    val details: String,
    val physicalPlanDescription: String,
    val physicalPlanGraph: SparkPlanGraph,
    val accumulatorMetrics: Map[Long, SQLPlanMetric],
    val submissionTime: Long) {

  var completionTime: Option[Long] = None

  val jobs: mutable.HashMap[Long, JobExecutionStatus] = mutable.HashMap.empty

  val stages: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer()

  val driverAccumUpdates: mutable.HashMap[Long, Long] = mutable.HashMap.empty

  /**
   * Return whether there are running jobs in this execution.
   */
  def hasRunningJobs: Boolean = jobs.values.exists(_ == JobExecutionStatus.RUNNING)

  /**
   * Return whether there are any failed jobs in this execution.
   */
  def isFailed: Boolean = jobs.values.exists(_ == JobExecutionStatus.FAILED)

  def runningJobs: Seq[Long] =
    jobs.filter { case (_, status) => status == JobExecutionStatus.RUNNING }.keys.toSeq

  def succeededJobs: Seq[Long] =
    jobs.filter { case (_, status) => status == JobExecutionStatus.SUCCEEDED }.keys.toSeq

  def failedJobs: Seq[Long] =
    jobs.filter { case (_, status) => status == JobExecutionStatus.FAILED }.keys.toSeq
}

/**
 * Represent a metric in a SQLPlan.
 *
 * Because we cannot revert our changes for an "Accumulator", we need to maintain accumulator
 * updates for each task. So that if a task is retried, we can simply override the old updates with
 * the new updates of the new attempt task. Since we cannot add them to accumulator, we need to use
 * "AccumulatorParam" to get the aggregation value.
 */
private[ui] case class SQLPlanMetric(
    name: String,
    accumulatorId: Long,
    metricType: String)

/**
 * Store all accumulatorUpdates for all tasks in a Spark stage.
 */
private[ui] class SQLStageMetrics(
    val stageAttemptId: Long,
    val taskIdToMetricUpdates: mutable.HashMap[Long, SQLTaskMetrics] = mutable.HashMap.empty)

/**
 * Store all accumulatorUpdates for a Spark task.
 */
private[ui] class SQLTaskMetrics(
    val attemptId: Long, // TODO not used yet
    var finished: Boolean,
    var accumulatorUpdates: Seq[(Long, Any)])
