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

package org.apache.spark.sql.ui

import scala.collection.mutable

import org.apache.spark.{AccumulatorParam, JobExecutionStatus}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.SQLExecution

private[sql] class SQLListener(sqlContext: SQLContext) extends SparkListener {

  private val retainedExecutions =
    sqlContext.sparkContext.conf.getInt("spark.sql.ui.retainedExecutions", 1000)

  private val activeExecutions = mutable.HashMap[Long, SQLExecutionUIData]()

  // Old data in the following fields must be removed in "trimExecutionsIfNecessary".
  // If adding new fields, make sure "trimExecutionsIfNecessary" can clean up old data

  // VisibleForTesting
  val executionIdToData = mutable.HashMap[Long, SQLExecutionUIData]()

  /**
   * Maintain the relation between job id and execution id so that we can get the execution id in
   * the "onJobEnd" method.
   */
  private val jobIdToExecutionId = mutable.HashMap[Long, Long]()

  private val stageIdToStageMetrics = mutable.HashMap[Long, SQLStageMetrics]()

  private val failedExecutions = mutable.ListBuffer[SQLExecutionUIData]()

  private val completedExecutions = mutable.ListBuffer[SQLExecutionUIData]()

  // VisibleForTesting
  def executionIdToDataSize: Int = synchronized {
    executionIdToData.size
  }

  // VisibleForTesting
  def jobIdToExecutionIdSize: Int = synchronized {
    jobIdToExecutionId.size
  }

  // VisibleForTesting
  def stageIdToStageMetricsSize: Int = synchronized {
    stageIdToStageMetrics.size
  }

  private def trimExecutionsIfNecessary(
      executions: mutable.ListBuffer[SQLExecutionUIData]): Unit = {
    if (executions.size > retainedExecutions) {
      val toRemove = math.max(retainedExecutions / 10, 1)
      executions.take(toRemove).foreach { execution =>
        for (executionUIData <- executionIdToData.remove(execution.executionId)) {
          for (jobId <- executionUIData.jobs.keys) {
            jobIdToExecutionId.remove(jobId)
          }
          for (stageId <- executionUIData.stages) {
            stageIdToStageMetrics.remove(stageId)
          }
        }
      }
      executions.trimStart(toRemove)
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val executionId = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionId == null) {
      // This is not a job created by SQL
      return
    }
    val jobId = jobStart.jobId
    val stageIds = jobStart.stageIds

    synchronized {
      activeExecutions.get(executionId.toLong).foreach { executionUIData =>
        executionUIData.jobs(jobId) = JobExecutionStatus.RUNNING
        executionUIData.stages ++= stageIds
        // attemptId must be 0. Right?
        stageIds.foreach(stageId =>
          stageIdToStageMetrics(stageId) = SQLStageMetrics(stageAttemptId = 0))
        jobIdToExecutionId(jobId) = executionUIData.executionId
      }
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    val jobId = jobEnd.jobId
    for (executionId <- jobIdToExecutionId.get(jobId);
         executionUIData <- executionIdToData.get(executionId)) {
      jobEnd.jobResult match {
        case JobSucceeded => executionUIData.jobs(jobId) = JobExecutionStatus.SUCCEEDED
        case JobFailed(_) => executionUIData.jobs(jobId) = JobExecutionStatus.FAILED
      }
      tryFinishExecution(executionUIData, jobEnd.time)
    }
  }

  /**
   * Should handle two cases:
   * 1. onJobEnd happens before onExecutionEnd
   * 2. onExecutionEnd happens before onJobEnd
   */
  private def tryFinishExecution(
      executionUIData: SQLExecutionUIData, time: Long): Unit = synchronized {
    if (executionUIData.isFinished) {
      if (executionUIData.completionTime.isEmpty) {
        // This is called from the last onJobEnd. We just show the job end time to UI. If will be
        // updated by onExecutionEnd later.
        executionUIData.completionTime = Some(time)
      }
      // Note: this execution may have been already removed by other method. If so, just do nothing.
      activeExecutions.remove(executionUIData.executionId).foreach { _ =>
        if (executionUIData.isFailed) {
          failedExecutions += executionUIData
          trimExecutionsIfNecessary(failedExecutions)
        } else {
          completedExecutions += executionUIData
          trimExecutionsIfNecessary(completedExecutions)
        }
      }
    }
  }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = synchronized {
    for ((taskId, stageId, stageAttemptID, metrics) <- executorMetricsUpdate.taskMetrics) {
      updateTaskMetrics(taskId, stageId, stageAttemptID, metrics, false)
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = synchronized {
    val stageId = stageSubmitted.stageInfo.stageId
    val stageAttemptId = stageSubmitted.stageInfo.attemptId
    // Always override metrics for old stage attempt
    stageIdToStageMetrics(stageId) = SQLStageMetrics(stageAttemptId)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    updateTaskMetrics(
      taskEnd.taskInfo.taskId, taskEnd.stageId, taskEnd.stageAttemptId, taskEnd.taskMetrics, true)
  }

  private def updateTaskMetrics(
      taskId: Long,
      stageId: Int,
      stageAttemptID: Int,
      metrics: TaskMetrics,
      finishTask: Boolean): Unit = {
    if (metrics == null) {
      return
    }

    stageIdToStageMetrics.get(stageId) match {
      case Some(stageMetrics) =>
        if (stageAttemptID < stageMetrics.stageAttemptId) {
          // A task of an old stage attempt. Because a new stage is submitted, we can ignore it.
        } else if (stageAttemptID > stageMetrics.stageAttemptId) {
          // TODO A running task with a higher stageAttemptID??
        } else {
          // TODO We don't know the attemptId. Currently, what we can do is overriding the
          // accumulator updates. However, if there are two same task are running, such as
          // speculation, the accumulator updates will be overriding by different task attempts,
          // the results will be weird.
          stageMetrics.taskIdToMetricUpdates.get(taskId) match {
            case Some(taskMetrics) =>
              if (finishTask) {
                taskMetrics.finished = true
                taskMetrics.accumulatorUpdates = metrics.accumulatorUpdates()
              } else if (!taskMetrics.finished){
                // If a task is finished, we should not override with accumulator updates from
                // heartbeat reports
                taskMetrics.accumulatorUpdates = metrics.accumulatorUpdates()
              }
            case None =>
              // TODO Now just set attemptId to 0. Should fix here when we can get the attempt
              // id from SparkListenerExecutorMetricsUpdate
              stageMetrics.taskIdToMetricUpdates(taskId) =
                SQLTaskMetrics(attemptId = 0, finished = finishTask, metrics.accumulatorUpdates())
          }
        }
      case None =>
      // This execution and its stage have been dropped
    }
  }

  def onExecutionStart(
      executionId: Long,
      description: String,
      details: String,
      df: DataFrame,
      time: Long): Unit = {
    val physicalPlanDescription = df.queryExecution.toString
    val physicalPlanGraph = SparkPlanGraph(df.queryExecution.executedPlan)
    val metrics = physicalPlanGraph.nodes.flatMap { node =>
      node.metrics.map(metric => metric.accumulatorId -> metric)
    }

    val executionUIData = SQLExecutionUIData(executionId, description, details,
      physicalPlanDescription, physicalPlanGraph, metrics.toMap, time)

    synchronized {
      activeExecutions(executionId) = executionUIData
      executionIdToData(executionId) = executionUIData
    }
  }

  def onExecutionEnd(executionId: Long, time: Long): Unit = synchronized {
    executionIdToData.get(executionId).foreach { executionUIData =>
      executionUIData.completionTime = Some(time)
      tryFinishExecution(executionUIData, time)
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
    executionIdToData.get(executionId)
  }

  def getExecutionMetrics(executionId: Long): Map[Long, Any] = synchronized {
    executionIdToData.get(executionId) match {
      case Some(executionUIData) =>
        // Get all accumulator updates from all tasks which belong to this execution and merge them
        val accumulatorUpdates = {
          for (stageId <- executionUIData.stages;
               stageMetrics <- stageIdToStageMetrics.get(stageId).toIterable;
               taskMetrics <- stageMetrics.taskIdToMetricUpdates.values;
               accumulatorUpdate <- taskMetrics.accumulatorUpdates.toSeq)
            yield accumulatorUpdate
        }
        mergeAccumulatorUpdates(accumulatorUpdates, accumulatorId =>
          executionUIData.accumulatorMetrics(accumulatorId).accumulatorParam)
      case None =>
        // This execution has been dropped
        Map.empty
    }
  }

  private def mergeAccumulatorUpdates(
     accumulatorUpdates: Seq[(Long, Any)],
     paramFunc: Long => AccumulatorParam[Any]): Map[Long, Any] = {
    accumulatorUpdates.groupBy(_._1).map { case (accumulatorId, values) =>
      val param = paramFunc(accumulatorId)
      (accumulatorId, values.map(_._2).reduceLeft(param.addInPlace))
    }
  }

}

/**
 * Represent all necessary data for an execution that will be used in Web UI.
 */
private[ui] case class SQLExecutionUIData(
    executionId: Long,
    description: String,
    details: String,
    physicalPlanDescription: String,
    physicalPlanGraph: SparkPlanGraph,
    accumulatorMetrics: Map[Long, SQLPlanMetric],
    submissionTime: Long,
    var completionTime: Option[Long] = None,
    jobs: mutable.HashMap[Long, JobExecutionStatus] = mutable.HashMap.empty,
    stages: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer()) {

  /**
   * Return if there is no running job.
   */
  def isFinished: Boolean = jobs.values.forall(_ != JobExecutionStatus.RUNNING)

  /**
   * Return if there is any failed job.
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
    accumulatorParam: AccumulatorParam[Any])

/**
 * Store all accumulatorUpdates for all tasks in a Spark stage.
 */
private[ui] case class SQLStageMetrics(
    stageAttemptId: Long,
    taskIdToMetricUpdates: mutable.HashMap[Long, SQLTaskMetrics] = mutable.HashMap.empty)

/**
 * Store all accumulatorUpdates for a Spark task.
 */
private[ui] case class SQLTaskMetrics(
    attemptId: Long, // TODO not used yet
    var finished: Boolean,
    var accumulatorUpdates: Map[Long, Any])
