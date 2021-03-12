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

import java.util.{Arrays, Date, NoSuchElementException}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{JobExecutionStatus, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Status._
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.status.{ElementTrackingStore, KVUtils, LiveEntity}
import org.apache.spark.util.collection.OpenHashMap

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

  kvstore.addTrigger(classOf[SQLExecutionUIData], conf.get[Int](UI_RETAINED_EXECUTIONS)) { count =>
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
          executionData.endEvents.set(sqlStoreData.jobs.size + 1)
          liveExecutions.put(executionId, executionData)
          Some(executionData)
        } catch {
          case _: NoSuchElementException => None
        }
      }.getOrElse(getOrCreateExecution(executionId))

    // Record the accumulator IDs and metric types for the stages of this job, so that the code
    // that keeps track of the metrics knows which accumulators to look at.
    val accumIdsAndType = exec.metrics.map { m => (m.accumulatorId, m.metricType) }.toMap
    if (accumIdsAndType.nonEmpty) {
      event.stageInfos.foreach { stage =>
        stageMetrics.put(stage.stageId, new LiveStageMetrics(stage.stageId, 0,
          stage.numTasks, accumIdsAndType))
      }
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
    Option(stageMetrics.get(event.stageInfo.stageId)).foreach { stage =>
      if (stage.attemptId != event.stageInfo.attemptNumber) {
        stageMetrics.put(event.stageInfo.stageId,
          new LiveStageMetrics(event.stageInfo.stageId, event.stageInfo.attemptNumber,
            stage.numTasks, stage.accumIdsToMetricType))
      }
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
        exec.endEvents.incrementAndGet()
        update(exec)
      }
    }
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    event.accumUpdates.foreach { case (taskId, stageId, attemptId, accumUpdates) =>
      updateStageMetrics(stageId, attemptId, taskId, SQLAppStatusListener.UNKNOWN_INDEX,
        accumUpdates, false)
    }
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    Option(stageMetrics.get(event.stageId)).foreach { stage =>
      if (stage.attemptId == event.stageAttemptId) {
        stage.registerTask(event.taskInfo.taskId, event.taskInfo.index)
      }
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
    updateStageMetrics(event.stageId, event.stageAttemptId, info.taskId, info.index, accums.toSeq,
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

    val liveStageMetrics = exec.stages.toSeq
      .flatMap { stageId => Option(stageMetrics.get(stageId)) }

    val taskMetrics = liveStageMetrics.flatMap(_.metricValues())

    val maxMetrics = liveStageMetrics.flatMap(_.maxMetricValues())

    val allMetrics = new mutable.HashMap[Long, Array[Long]]()

    val maxMetricsFromAllStages = new mutable.HashMap[Long, Array[Long]]()

    taskMetrics.filter(m => metricTypes.contains(m._1)).foreach { case (id, values) =>
      val prev = allMetrics.getOrElse(id, null)
      val updated = if (prev != null) {
        prev ++ values
      } else {
        values
      }
      allMetrics(id) = updated
    }

    // Find the max for each metric id between all stages.
    val validMaxMetrics = maxMetrics.filter(m => metricTypes.contains(m._1))
    validMaxMetrics.foreach { case (id, value, taskId, stageId, attemptId) =>
      val updated = maxMetricsFromAllStages.getOrElse(id, Array(value, stageId, attemptId, taskId))
      if (value > updated(0)) {
        updated(0) = value
        updated(1) = stageId
        updated(2) = attemptId
        updated(3) = taskId
      }
      maxMetricsFromAllStages(id) = updated
    }

    exec.driverAccumUpdates.foreach { case (id, value) =>
      if (metricTypes.contains(id)) {
        val prev = allMetrics.getOrElse(id, null)
        val updated = if (prev != null) {
          // If the driver updates same metrics as tasks and has higher value then remove
          // that entry from maxMetricsFromAllStage. This would make stringValue function default
          // to "driver" that would be displayed on UI.
          if (maxMetricsFromAllStages.contains(id) && value > maxMetricsFromAllStages(id)(0)) {
            maxMetricsFromAllStages.remove(id)
          }
          val _copy = Arrays.copyOf(prev, prev.length + 1)
          _copy(prev.length) = value
          _copy
        } else {
          Array(value)
        }
        allMetrics(id) = updated
      }
    }

    val aggregatedMetrics = allMetrics.map { case (id, values) =>
      id -> SQLMetrics.stringValue(metricTypes(id), values, maxMetricsFromAllStages.getOrElse(id,
        Array.empty[Long]))
    }.toMap

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
      taskIdx: Int,
      accumUpdates: Seq[AccumulableInfo],
      succeeded: Boolean): Unit = {
    Option(stageMetrics.get(stageId)).foreach { metrics =>
      if (metrics.attemptId == attemptId) {
        metrics.updateTaskMetrics(taskId, taskIdx, succeeded, accumUpdates)
      }
    }
  }

  private def toStoredNodes(nodes: Seq[SparkPlanGraphNode]): Seq[SparkPlanGraphNodeWrapper] = {
    nodes.map {
      case cluster: SparkPlanGraphCluster =>
        val storedCluster = new SparkPlanGraphClusterWrapper(
          cluster.id,
          cluster.name,
          cluster.desc,
          toStoredNodes(cluster.nodes.toSeq),
          cluster.metrics)
        new SparkPlanGraphNodeWrapper(null, storedCluster)

      case node =>
        new SparkPlanGraphNodeWrapper(node, null)
    }
  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    val SparkListenerSQLExecutionStart(executionId, description, details,
      physicalPlanDescription, sparkPlanInfo, time) = event

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

  private def onAdaptiveExecutionUpdate(event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    val SparkListenerSQLAdaptiveExecutionUpdate(
      executionId, physicalPlanDescription, sparkPlanInfo) = event

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
    exec.physicalPlanDescription = physicalPlanDescription
    exec.metrics ++= sqlPlanMetrics
    update(exec)
  }

  private def onAdaptiveSQLMetricUpdate(event: SparkListenerSQLAdaptiveSQLMetricUpdates): Unit = {
    val SparkListenerSQLAdaptiveSQLMetricUpdates(executionId, sqlPlanMetrics) = event

    val exec = getOrCreateExecution(executionId)
    exec.metrics ++= sqlPlanMetrics
    update(exec)
  }

  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val SparkListenerSQLExecutionEnd(executionId, time) = event
    Option(liveExecutions.get(executionId)).foreach { exec =>
      exec.completionTime = Some(new Date(time))
      update(exec)

      // Aggregating metrics can be expensive for large queries, so do it asynchronously. The end
      // event count is updated after the metrics have been aggregated, to prevent a job end event
      // arriving during aggregation from cleaning up the metrics data.
      kvstore.doAsync {
        exec.metricsValues = aggregateMetrics(exec)
        removeStaleMetricsData(exec)
        exec.endEvents.incrementAndGet()
        update(exec, force = true)
      }
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
    case e: SparkListenerSQLAdaptiveExecutionUpdate => onAdaptiveExecutionUpdate(e)
    case e: SparkListenerSQLAdaptiveSQLMetricUpdates => onAdaptiveSQLMetricUpdate(e)
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
    if (exec.endEvents.get() >= exec.jobs.size + 1) {
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
  var driverAccumUpdates = Seq[(Long, Long)]()

  @volatile var metricsValues: Map[Long, String] = null

  // Just in case job end and execution end arrive out of order, keep track of how many
  // end events arrived so that the listener can stop tracking the execution.
  val endEvents = new AtomicInteger()

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
    val attemptId: Int,
    val numTasks: Int,
    val accumIdsToMetricType: Map[Long, String]) {

  /**
   * Mapping of task IDs to their respective index. Note this may contain more elements than the
   * stage's number of tasks, if speculative execution is on.
   */
  private val taskIndices = new OpenHashMap[Long, Int]()

  /** Bit set tracking which indices have been successfully computed. */
  private val completedIndices = new mutable.BitSet()

  /**
   * Task metrics values for the stage. Maps the metric ID to the metric values for each
   * index. For each metric ID, there will be the same number of values as the number
   * of indices. This relies on `SQLMetrics.stringValue` treating 0 as a neutral value,
   * independent of the actual metric type.
   */
  private val taskMetrics = new ConcurrentHashMap[Long, Array[Long]]()

  private val  metricsIdToMaxTaskValue = new ConcurrentHashMap[Long, Array[Long]]()

  def registerTask(taskId: Long, taskIdx: Int): Unit = {
    taskIndices.update(taskId, taskIdx)
  }

  def updateTaskMetrics(
      taskId: Long,
      eventIdx: Int,
      finished: Boolean,
      accumUpdates: Seq[AccumulableInfo]): Unit = {
    val taskIdx = if (eventIdx == SQLAppStatusListener.UNKNOWN_INDEX) {
      if (!taskIndices.contains(taskId)) {
        // We probably missed the start event for the task, just ignore it.
        return
      }
      taskIndices(taskId)
    } else {
      // Here we can recover from a missing task start event. Just register the task again.
      registerTask(taskId, eventIdx)
      eventIdx
    }

    if (completedIndices.contains(taskIdx)) {
      return
    }

    accumUpdates
      .filter { acc => acc.update.isDefined && accumIdsToMetricType.contains(acc.id) }
      .foreach { acc =>
        // In a live application, accumulators have Long values, but when reading from event
        // logs, they have String values. For now, assume all accumulators are Long and convert
        // accordingly.
        val value = acc.update.get match {
          case s: String => s.toLong
          case l: Long => l
          case o => throw new IllegalArgumentException(s"Unexpected: $o")
        }

        val metricValues = taskMetrics.computeIfAbsent(acc.id, _ => new Array(numTasks))
        metricValues(taskIdx) = value

        if (SQLMetrics.metricNeedsMax(accumIdsToMetricType(acc.id))) {
          val maxMetricsTaskId = metricsIdToMaxTaskValue.computeIfAbsent(acc.id, _ => Array(value,
            taskId))

          if (value > maxMetricsTaskId.head) {
            maxMetricsTaskId(0) = value
            maxMetricsTaskId(1) = taskId
          }
        }
      }
    if (finished) {
      completedIndices += taskIdx
    }
  }

  def metricValues(): Seq[(Long, Array[Long])] = taskMetrics.asScala.toSeq

  // Return Seq of metric id, value, taskId, stageId, attemptId for this stage
  def maxMetricValues(): Seq[(Long, Long, Long, Int, Int)] = {
    metricsIdToMaxTaskValue.asScala.toSeq.map { case (id, maxMetrics) => (id, maxMetrics(0),
      maxMetrics(1), stageId, attemptId)
    }
  }
}

private object SQLAppStatusListener {
  val UNKNOWN_INDEX = -1
}
