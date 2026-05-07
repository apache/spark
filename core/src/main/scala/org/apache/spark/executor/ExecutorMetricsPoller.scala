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
package org.apache.spark.executor

import java.lang.Long.{MAX_VALUE => LONG_MAX_VALUE}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLongArray

import scala.collection.mutable.HashMap

import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryManager
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * A class that polls executor metrics, and tracks their peaks per task and per stage.
 * Each executor keeps an instance of this class.
 * The poll method polls the executor metrics, and is either run in its own thread or
 * called by the executor's heartbeater thread, depending on configuration.
 * The class keeps two ConcurrentHashMaps that are accessed (via its methods) by the
 * executor's task runner threads concurrently with the polling thread. One thread may
 * update one of these maps while another reads it, so the reading thread may not get
 * the latest metrics, but this is ok.
 * We track executor metric peaks per stage, as well as per task. The per-stage peaks
 * are sent in executor heartbeats. That way, we get incremental updates of the metrics
 * as the tasks are running, and if the executor dies we still have some metrics. The
 * per-task peaks are sent in the task result at task end. These are useful for short
 * tasks. If there are no heartbeats during the task, we still get the metrics polled
 * for the task.
 *
 * @param memoryManager the memory manager used by the executor.
 * @param pollingInterval the polling interval in milliseconds.
 */
private[spark] class ExecutorMetricsPoller(
    memoryManager: MemoryManager,
    pollingInterval: Long,
    executorMetricsSource: Option[ExecutorMetricsSource]) extends Logging {

  type StageKey = (Int, Int)
  // Task Count and Metric Peaks
  private[executor] case class TCMP(count: Long, peaks: AtomicLongArray)

  // Map of (stageId, stageAttemptId) to (count of running tasks, executor metric peaks)
  private[executor] val stageTCMP = new ConcurrentHashMap[StageKey, TCMP]

  // Map of taskId to executor metric peaks
  private val taskMetricPeaks = new ConcurrentHashMap[Long, AtomicLongArray]

  private val poller =
    if (pollingInterval > 0) {
      Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-metrics-poller"))
    } else {
      None
    }

  /**
   * Function to poll executor metrics.
   * On start, if pollingInterval is positive, this is scheduled to run at that interval.
   * Otherwise, this is called by the reportHeartBeat function defined in Executor and passed
   * to its Heartbeater.
   */
  def poll(): Unit = {
    // Note: Task runner threads may update stageTCMP or read from taskMetricPeaks concurrently
    // with this function via calls to methods of this class.

    // get the latest values for the metrics
    val latestMetrics = ExecutorMetrics.getCurrentMetrics(memoryManager)
    executorMetricsSource.foreach(_.updateMetricsSnapshot(latestMetrics))

    def updatePeaks(metrics: AtomicLongArray): Unit = {
      (0 until metrics.length).foreach { i =>
        metrics.getAndAccumulate(i, latestMetrics(i), math.max)
      }
    }

    // for each active stage, update the peaks
    stageTCMP.forEachValue(LONG_MAX_VALUE, v => updatePeaks(v.peaks))

    // for each running task, update the peaks
    taskMetricPeaks.forEachValue(LONG_MAX_VALUE, updatePeaks)
  }

  /** Starts the polling thread. */
  def start(): Unit = {
    poller.foreach { exec =>
      val pollingTask: Runnable = () => Utils.logUncaughtExceptions(poll())
      exec.scheduleAtFixedRate(pollingTask, 0L, pollingInterval, TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Called by TaskRunner#run.
   */
  def onTaskStart(taskId: Long, stageId: Int, stageAttemptId: Int): Unit = {
    // Put an entry in taskMetricPeaks for the task.
    taskMetricPeaks.put(taskId, new AtomicLongArray(ExecutorMetricType.numMetrics))

    // Put a new entry in stageTCMP for the stage if there isn't one already.
    // Increment the task count.
    val countAndPeaks = stageTCMP.compute((stageId, stageAttemptId), (k: StageKey, v: TCMP) =>
      if (v == null) {
        TCMP(1L, new AtomicLongArray(ExecutorMetricType.numMetrics))
      } else {
        TCMP(v.count + 1, v.peaks)
      })
    logDebug(s"stageTCMP: ($stageId, $stageAttemptId) -> ${countAndPeaks.count}")
  }

  /**
   * Called by TaskRunner#run. It should only be called if onTaskStart has been called with
   * the same arguments.
   */
  def onTaskCompletion(taskId: Long, stageId: Int, stageAttemptId: Int): Unit = {
    // Decrement the task count.

    def decrementCount(stage: StageKey, countAndPeaks: TCMP): TCMP = {
      val countValue = countAndPeaks.count - 1
      assert(countValue >= 0, "task count shouldn't below 0")
      logDebug(s"stageTCMP: (${stage._1}, ${stage._2}) -> " + countValue)
      TCMP(countValue, countAndPeaks.peaks)
    }

    stageTCMP.computeIfPresent((stageId, stageAttemptId), decrementCount)

    // Remove the entry from taskMetricPeaks for the task.
    taskMetricPeaks.remove(taskId)
  }

  /**
   * Called by TaskRunner#run.
   */
  def getTaskMetricPeaks(taskId: Long): Array[Long] = {
    // If this is called with an invalid taskId or a valid taskId but the task was killed and
    // onTaskStart was therefore not called, then we return an array of zeros.
    val currentPeaks = taskMetricPeaks.get(taskId) // may be null
    val metricPeaks = new Array[Long](ExecutorMetricType.numMetrics) // initialized to zeros
    if (currentPeaks != null) {
      ExecutorMetricType.metricToOffset.foreach { case (_, i) =>
        metricPeaks(i) = currentPeaks.get(i)
      }
    }
    metricPeaks
  }


  /**
   * Called by the reportHeartBeat function defined in Executor and passed to its Heartbeater.
   * It resets the metric peaks in stageTCMP before returning the executor updates.
   * Thus, the executor updates contains the per-stage metric peaks since the last heartbeat
   * (the last time this method was called).
   */
  def getExecutorUpdates(): HashMap[StageKey, ExecutorMetrics] = {
    val executorUpdates = new HashMap[StageKey, ExecutorMetrics]

    def getUpdateAndResetPeaks(k: StageKey, v: TCMP): TCMP = {
      executorUpdates.put(k, new ExecutorMetrics(v.peaks))
      TCMP(v.count, new AtomicLongArray(ExecutorMetricType.numMetrics))
    }

    stageTCMP.replaceAll(getUpdateAndResetPeaks)

    def removeIfInactive(k: StageKey, v: TCMP): TCMP = {
      if (v.count == 0) {
        logDebug(s"removing (${k._1}, ${k._2}) from stageTCMP")
        null
      } else {
        v
      }
    }

    // Remove the entry from stageTCMP if the task count reaches zero.
    executorUpdates.foreach { case (k, _) =>
      stageTCMP.computeIfPresent(k, removeIfInactive)
    }

    executorUpdates
  }

  /** Stops the polling thread. */
  def stop(): Unit = {
    poller.foreach { exec =>
      exec.shutdown()
      exec.awaitTermination(10, TimeUnit.SECONDS)
    }
  }
}
