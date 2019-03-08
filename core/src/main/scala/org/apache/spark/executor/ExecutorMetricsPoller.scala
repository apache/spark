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
import java.util.concurrent.atomic.{AtomicLong, AtomicLongArray}

import scala.collection.mutable.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryManager
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * :: DeveloperApi ::
 * A class that polls executor metrics, and tracks their peaks per task and per stage.
 * Each executor keeps an instance of this class.
 *
 * @param memoryManager the memory manager used by the executor.
 * @param pollingInterval the polling interval in milliseconds.
 */
@DeveloperApi
private[spark] class ExecutorMetricsPoller(
    memoryManager: MemoryManager,
    pollingInterval: Long)
  extends Logging {

  type StageKey = (Int, Int)
  // tuple for Task Count and Metric Peaks
  type TCMP = (AtomicLong, AtomicLongArray)

  // Map of (stageId, stageAttemptId) to (count of running tasks, executor metric peaks)
  private val stageTCMP = new ConcurrentHashMap[StageKey, TCMP]

  // Map of taskId to executor metric peaks
  private val taskMetricPeaks = new ConcurrentHashMap[Long, AtomicLongArray]

  private val poller =
    if (pollingInterval > 0) {
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-poller")
    } else {
      null
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

    def compareAndUpdate(current: Long, latest: Long): Long =
      if (latest > current) latest else current

    def updatePeaks(metrics: AtomicLongArray): Unit = {
      (0 until metrics.length).foreach { i =>
        metrics.getAndAccumulate(i, latestMetrics(i), compareAndUpdate)
      }
    }

    // for each active stage, update the peaks
    stageTCMP.forEachValue(LONG_MAX_VALUE, v => updatePeaks(v._2))

    // for each running task, update the peaks
    taskMetricPeaks.forEachValue(LONG_MAX_VALUE, updatePeaks)
  }

  /** Starts the polling thread. */
  def start(): Unit = {
    if (poller != null) {
      val pollingTask = new Runnable() {
        override def run(): Unit = Utils.logUncaughtExceptions(poll())
      }
      poller.scheduleAtFixedRate(pollingTask, 0L, pollingInterval, TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Called by Executor#launchTask.
   *
   * @param taskId the id of the task being launched.
   */
  def onTaskLaunch(taskId: Long): Unit = {
    taskMetricPeaks.put(taskId, new AtomicLongArray(ExecutorMetricType.numMetrics))
  }

  /**
   * Called by TaskRunner#run.
   *
   * @param stageId the id of the stage the task belongs to.
   * @param stageAttemptId the attempt number of the stage the task belongs to.
   */
  def onTaskStart(stageId: Int, stageAttemptId: Int): Unit = {
    // Put a new entry in stageTCMP for the stage if there isn't one already.
    // Increment the task count.
    val (count, _) = stageTCMP.computeIfAbsent((stageId, stageAttemptId),
      _ => (new AtomicLong(0), new AtomicLongArray(ExecutorMetricType.numMetrics)))
    val stageCount = count.incrementAndGet()
    logDebug(s"stageTCMP: ($stageId, $stageAttemptId) -> $stageCount")
  }

  /**
   * Called by TaskRunner#run.
   *
   * @param stageId the id of the stage the task belongs to.
   * @param stageAttemptId the attempt number of the stage the task belongs to.
   */
  def onTaskCompletion(stageId: Int, stageAttemptId: Int): Unit = {
    // Decrement the task count.
    // Remove the entry from stageTCMP if the task count reaches zero

    def decrementCount(stage: StageKey, countAndPeaks: TCMP): TCMP = {
      val count = countAndPeaks._1
      val countValue = count.decrementAndGet()
      if (countValue == 0L) {
        logDebug(s"removing (${stage._1}, ${stage._2}) from stageTCMP")
        null
      } else {
        logDebug(s"stageTCMP: (${stage._1}, ${stage._2}) -> " + countValue)
        countAndPeaks
      }
    }

    stageTCMP.computeIfPresent((stageId, stageAttemptId), decrementCount)
  }

  /**
   * Called by TaskRunner#run.
   *
   * @param taskId the id of the task that was run.
   */
  def onTaskCleanup(taskId: Long): Unit = {
    taskMetricPeaks.remove(taskId)
  }

  /**
   * Called by TaskRunner#run.
   *
   * @param taskId the id of the task that was run.
   */
  def getTaskMetricPeaks(taskId: Long): Array[Long] = {
    val currentPeaks = taskMetricPeaks.get(taskId)
    val metricPeaks = new Array[Long](ExecutorMetricType.numMetrics)
    ExecutorMetricType.metricToOffset.foreach { case (_, i) =>
      metricPeaks(i) = currentPeaks.get(i)
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
    // build the executor level memory metrics
    val executorUpdates = new HashMap[StageKey, ExecutorMetrics]

    def peaksForStage(k: StageKey, v: TCMP): (StageKey, AtomicLongArray) =
      if (v._1.get() > 0) (k, v._2) else null

    def addPeaks(nested: (StageKey, AtomicLongArray)): Unit = {
      val (k, v) = nested
      executorUpdates.put(k, new ExecutorMetrics(v))
    }

    stageTCMP.forEach[(StageKey, AtomicLongArray)](LONG_MAX_VALUE, peaksForStage, addPeaks)

    // reset the peaks
    def resetPeaks(k: StageKey, v: TCMP): TCMP =
      (v._1, new AtomicLongArray(ExecutorMetricType.numMetrics))

    stageTCMP.replaceAll(resetPeaks)

    executorUpdates
  }

  /** Stops the polling thread. */
  def stop(): Unit = {
    if (poller != null) {
      poller.shutdown()
      poller.awaitTermination(10, TimeUnit.SECONDS)
    }
  }
}
