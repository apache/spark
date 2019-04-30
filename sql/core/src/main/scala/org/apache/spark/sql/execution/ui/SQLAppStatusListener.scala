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

import java.util.{Date, NoSuchElementException}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.{JobExecutionStatus, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Status._
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.status.{ElementTrackingStore, KVUtils, LiveEntity}

class SQLAppStatusListener(
    conf: SparkConf,
    kvstore: ElementTrackingStore,
    live: Boolean) extends SparkListener with Logging {

  // How often to flush intermediate state of a live execution to the store. When replaying logs,
  // never flush (only do the very last write).
  private val liveUpdatePeriodNs = if (live) conf.get(LIVE_ENTITY_UPDATE_PERIOD) else -1L

  // Live tracked data is needed by the SQL status store to calculate metrics for in-flight
  // executions; that means arbitrary threads may be querying these maps, so they need to be
  // thread-safe.
  private val liveExecutions = new ConcurrentHashMap[Long, LiveExecutionData]()
  private val stageMetrics = new ConcurrentHashMap[Int, LiveStageMetrics]()

  // Returns true if this listener has no live data. Exposed for tests only.
  private[sql] def noLiveData(): Boolean = {
    liveExecutions.isEmpty && stageMetrics.isEmpty
  }

  kvstore.addTrigger(classOf[SQLExecutionUIData], conf.get(UI_RETAINED_EXECUTIONS)) { count =>
    cleanupExecutions(count)
  }

  kvstore.onFlush {
    if (!live) {
      val now = System.nanoTime()
      liveExecutions.values.asScala.foreach { exec =>
        // This saves the partial aggregated metrics to the store; this works currently because
        // when the SHS sees an updated event log, all old data for the application is thrown
        // away.
        exec.metricsValues = aggregateMetrics(exec)
        exec.write(kvstore, now)
      }
    }
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val executionIdString = event.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString == null) {
      // This is not a job created by SQL
      return
    }

    val executionId = executionIdString.toLong
    val jobId = event.jobId
    val exec = Option(liveExecutions.get(executionId))
      .orElse {
        try {
          // Should not overwrite the kvstore with new entry, if it already has the SQLExecution
          // data corresponding to the execId.
          val sqlStoreData = kvstore.read(classOf[SQLExecutionUIData], executionId)
          val executionData = new LiveExecutionData(executionId)
          executionData.description = sqlStoreData.description
          executionData.details = sqlStoreData.details
          executionData.physicalPlanDescription = sqlStoreData.physicalPlanDescription
          executionData.metrics = sqlStoreData.metrics
          executionData.submissionTime = sqlStoreData.submissionTime
          executionData.completionTime = sqlStoreData.completionTime
          executionData.jobs = sqlStoreData.jobs
          executionData.stages = sqlStoreData.stages
          executionData.metricsValues = sqlStoreData.metricValues
          executionData.endEvents = sqlStoreData.jobs.size + 1
          liveExecutions.put(executionId, executionData)
          Some(executionData)
        } catch {
          case _: NoSuchElementException => None
        }
      }.getOrElse(getOrCreateExecution(executionId))

    // Record the accumulator IDs for the stages of this job, so that the code that keeps
    // track of the metrics knows which accumulators to look at.
    val accumIds = exec.metrics.map(_.accumulatorId).toSet
    event.stageIds.foreach { id =>
      stageMetrics.put(id, new LiveStageMetrics(id, 0, accumIds, new ConcurrentHashMap()))
    }

    exec.jobs = exec.jobs + (jobId -> JobExecutionStatus.RUNNING)
    exec.stages ++= event.stageIds.toSet
    update(exec, force = true)
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    if (!isSQLStage(event.stageInfo.stageId)) {
      return
    }

    // Reset the metrics tracking object for the new attempt.
    Option(stageMetrics.get(event.stageInfo.stageId)).foreach { metrics =>
      metrics.taskMetrics.clear()
      metrics.attemptId = event.stageInfo.attemptNumber
    }
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    liveExecutions.values().asScala.foreach { exec =>
      if (exec.jobs.contains(event.jobId)) {
        val result = event.jobResult match {
          case JobSucceeded => JobExecutionStatus.SUCCEEDED
          case _ => JobExecutionStatus.FAILED
        }
        exec.jobs = exec.jobs + (event.jobId -> result)
        exec.endEvents += 1
        update(exec)
      }
    }
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    event.accumUpdates.foreach { case (taskId, stageId, attemptId, accumUpdates) =>
      updateStageMetrics(stageId, attemptId, taskId, accumUpdates, false)
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    if (!isSQLStage(event.stageId)) {
      return
    }

    val info = event.taskInfo
    // SPARK-20342. If processing events from a live application, use the task metrics info to
    // work around a race in the DAGScheduler. The metrics info does not contain accumulator info
    // when reading event logs in the SHS, so we have to rely on the accumulator in that case.
    val accums = if (live && event.taskMetrics != null) {
      event.taskMetrics.externalAccums.flatMap { a =>
        // This call may fail if the accumulator is gc'ed, so account for that.
        try {
          Some(a.toInfo(Some(a.value), None))
        } catch {
          case _: IllegalAccessError => None
        }
      }
    } else {
      info.accumulables
    }
    updateStageMetrics(event.stageId, event.stageAttemptId, info.taskId, accums,
      info.successful)
  }

  def liveExecutionMetrics(executionId: Long): Option[Map[Long, String]] = {
    Option(liveExecutions.get(executionId)).map { exec =>
      if (exec.metricsValues != null) {
        exec.metricsValues
      } else {
        aggregateMetrics(exec)
      }
    }
  }

  private def aggregateMetrics(exec: LiveExecutionData): Map[Long, String] = {
    val metricTypes = exec.metrics.map { m => (m.accumulatorId, m.metricType) }.toMap
    val metrics = exec.stages.toSeq
      .flatMap { stageId => Option(stageMetrics.get(stageId)) }
      .flatMap(_.taskMetrics.values().asScala)
      .flatMap { metrics => metrics.ids.zip(metrics.values) }

    val aggregatedMetrics = (metrics ++ exec.driverAccumUpdates.toSeq)
      .filter { case (id, _) => metricTypes.contains(id) }
      .groupBy(_._1)
      .map { case (id, values) =>
        id -> SQLMetrics.stringValue(metricTypes(id), values.map(_._2))
      }

    // Check the execution again for whether the aggregated metrics data has been calculated.
    // This can happen if the UI is requesting this data, and the onExecutionEnd handler is
    // running at the same time. The metrics calculated for the UI can be inaccurate in that
    // case, since the onExecutionEnd handler will clean up tracked stage metrics.
    if (exec.metricsValues != null) {
      exec.metricsValues
    } else {
      aggregatedMetrics
    }
  }

  private def updateStageMetrics(
      stageId: Int,
      attemptId: Int,
      taskId: Long,
      accumUpdates: Seq[AccumulableInfo],
      succeeded: Boolean): Unit = {
    Option(stageMetrics.get(stageId)).foreach { metrics =>
      if (metrics.attemptId != attemptId || metrics.accumulatorIds.isEmpty) {
        return
      }

      val oldTaskMetrics = metrics.taskMetrics.get(taskId)
      if (oldTaskMetrics != null && oldTaskMetrics.succeeded) {
        return
      }

      val updates = accumUpdates
        .filter { acc => acc.update.isDefined && metrics.accumulatorIds.contains(acc.id) }
        .sortBy(_.id)

      if (updates.isEmpty) {
        return
      }

      val ids = new Array[Long](updates.size)
      val values = new Array[Long](updates.size)
      updates.zipWithIndex.foreach { case (acc, idx) =>
        ids(idx) = acc.id
        // In a live application, accumulators have Long values, but when reading from event
        // logs, they have String values. For now, assume all accumulators are Long and covert
        // accordingly.
        values(idx) = acc.update.get match {
          case s: String => s.toLong
          case l: Long => l
          case o => throw new IllegalArgumentException(s"Unexpected: $o")
        }
      }

      // TODO: storing metrics by task ID can cause metrics for the same task index to be
      // counted multiple times, for example due to speculation or re-attempts.
      metrics.taskMetrics.put(taskId, new LiveTaskMetrics(ids, values, succeeded))
    }
  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    val SparkListenerSQLExecutionStart(executionId, description, details,
      physicalPlanDescription, sparkPlanInfo, time) = event

    def toStoredNodes(nodes: Seq[SparkPlanGraphNode]): Seq[SparkPlanGraphNodeWrapper] = {
      nodes.map {
        case cluster: SparkPlanGraphCluster =>
          val storedCluster = new SparkPlanGraphClusterWrapper(
            cluster.id,
            cluster.name,
            cluster.desc,
            toStoredNodes(cluster.nodes),
            cluster.metrics)
          new SparkPlanGraphNodeWrapper(null, storedCluster)

        case node =>
          new SparkPlanGraphNodeWrapper(node, null)
      }
    }

    val planGraph = SparkPlanGraph(sparkPlanInfo)
    val sqlPlanMetrics = planGraph.allNodes.flatMap { node =>
      node.metrics.map { metric => (metric.accumulatorId, metric) }
    }.toMap.values.toList

    val graphToStore = new SparkPlanGraphWrapper(
      executionId,
      toStoredNodes(planGraph.nodes),
      planGraph.edges)
    kvstore.write(graphToStore)

    val exec = getOrCreateExecution(executionId)
    exec.description = description
    exec.details = details
    exec.physicalPlanDescription = physicalPlanDescription
    exec.metrics = sqlPlanMetrics
    exec.submissionTime = time
    update(exec)
  }

  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val SparkListenerSQLExecutionEnd(executionId, time) = event
    Option(liveExecutions.get(executionId)).foreach { exec =>
      exec.metricsValues = aggregateMetrics(exec)
      exec.completionTime = Some(new Date(time))
      exec.endEvents += 1
      update(exec)

      removeStaleMetricsData(exec)
    }
  }

  private def removeStaleMetricsData(exec: LiveExecutionData): Unit = {
    // Remove stale LiveStageMetrics objects for stages that are not active anymore.
    val activeStages = liveExecutions.values().asScala.flatMap { other =>
      if (other != exec) other.stages else Nil
    }.toSet
    stageMetrics.keySet().asScala
      .filter(!activeStages.contains(_))
      .foreach(stageMetrics.remove)
  }

  private def onDriverAccumUpdates(event: SparkListenerDriverAccumUpdates): Unit = {
    val SparkListenerDriverAccumUpdates(executionId, accumUpdates) = event
    Option(liveExecutions.get(executionId)).foreach { exec =>
      exec.driverAccumUpdates = exec.driverAccumUpdates ++ accumUpdates
      update(exec)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart => onExecutionStart(e)
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    case e: SparkListenerDriverAccumUpdates => onDriverAccumUpdates(e)
    case _ => // Ignore
  }

  private def getOrCreateExecution(executionId: Long): LiveExecutionData = {
    liveExecutions.computeIfAbsent(executionId,
      (_: Long) => new LiveExecutionData(executionId))
  }

  private def update(exec: LiveExecutionData, force: Boolean = false): Unit = {
    val now = System.nanoTime()
    if (exec.endEvents >= exec.jobs.size + 1) {
      exec.write(kvstore, now)
      removeStaleMetricsData(exec)
      liveExecutions.remove(exec.executionId)
    } else if (force) {
      exec.write(kvstore, now)
    } else if (liveUpdatePeriodNs >= 0) {
      if (now - exec.lastWriteTime > liveUpdatePeriodNs) {
        exec.write(kvstore, now)
      }
    }
  }

  private def isSQLStage(stageId: Int): Boolean = {
    liveExecutions.values().asScala.exists { exec =>
      exec.stages.contains(stageId)
    }
  }

  private def cleanupExecutions(count: Long): Unit = {
    val countToDelete = count - conf.get(UI_RETAINED_EXECUTIONS)
    if (countToDelete <= 0) {
      return
    }

    val view = kvstore.view(classOf[SQLExecutionUIData]).index("completionTime").first(0L)
    val toDelete = KVUtils.viewToSeq(view, countToDelete.toInt)(_.completionTime.isDefined)
    toDelete.foreach { e =>
      kvstore.delete(e.getClass(), e.executionId)
      kvstore.delete(classOf[SparkPlanGraphWrapper], e.executionId)
    }
  }

}

private class LiveExecutionData(val executionId: Long) extends LiveEntity {

  var description: String = null
  var details: String = null
  var physicalPlanDescription: String = null
  var metrics = Seq[SQLPlanMetric]()
  var submissionTime = -1L
  var completionTime: Option[Date] = None

  var jobs = Map[Int, JobExecutionStatus]()
  var stages = Set[Int]()
  var driverAccumUpdates = Map[Long, Long]()

  @volatile var metricsValues: Map[Long, String] = null

  // Just in case job end and execution end arrive out of order, keep track of how many
  // end events arrived so that the listener can stop tracking the execution.
  var endEvents = 0

  override protected def doUpdate(): Any = {
    new SQLExecutionUIData(
      executionId,
      description,
      details,
      physicalPlanDescription,
      metrics,
      submissionTime,
      completionTime,
      jobs,
      stages,
      metricsValues)
  }

}

private class LiveStageMetrics(
    val stageId: Int,
    var attemptId: Int,
    val accumulatorIds: Set[Long],
    val taskMetrics: ConcurrentHashMap[Long, LiveTaskMetrics])

private class LiveTaskMetrics(
    val ids: Array[Long],
    val values: Array[Long],
    val succeeded: Boolean)
