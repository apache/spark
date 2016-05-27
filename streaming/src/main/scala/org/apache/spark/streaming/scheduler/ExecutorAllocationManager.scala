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


package org.apache.spark.streaming.scheduler

import scala.util.Random

import org.apache.spark.{ExecutorAllocationClient, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.util.RecurringTimer
import org.apache.spark.util.{Clock, Utils}

/**
 * Class that manages executor allocated to a StreamingContext, and dynamically request or kill
 * executors based on the statistics of the streaming computation. This is different from the core
 * dynamic allocation policy; the core policy relies on executors being idle for a while, but the
 * micro-batch model of streaming prevents any particular executors from being idle for a long
 * time. Instead, the measure of "idle-ness" needs to be based on the time taken to process
 * each batch.
 *
 * At a high level, the policy implemented by this class is as follows:
 * - Use StreamingListener interface get batch processing times of completed batches
 * - Periodically take the average batch completion times and compare with the batch interval
 * - If (avg. proc. time / batch interval) >= scaling up ratio, then request more executors.
 *   The number of executors requested is based on the ratio = (avg. proc. time / batch interval).
 * - If (avg. proc. time / batch interval) <= scaling down ratio, then try to kill an executor that
 *   is not running a receiver.
 *
 * This features should ideally be used in conjunction with backpressure, as backpressure ensures
 * system stability, while executors are being readjusted.
 */
private[streaming] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    receiverTracker: ReceiverTracker,
    conf: SparkConf,
    batchDurationMs: Long,
    clock: Clock) extends StreamingListener with Logging {

  import ExecutorAllocationManager._

  private val scalingIntervalSecs = conf.getTimeAsSeconds(
    SCALING_INTERVAL_KEY,
    s"${SCALING_INTERVAL_DEFAULT_SECS}s")
  private val scalingUpRatio = conf.getDouble(SCALING_UP_RATIO_KEY, SCALING_UP_RATIO_DEFAULT)
  private val scalingDownRatio = conf.getDouble(SCALING_DOWN_RATIO_KEY, SCALING_DOWN_RATIO_DEFAULT)
  private val minNumExecutors = conf.getInt(
    MIN_EXECUTORS_KEY,
    math.max(1, receiverTracker.numReceivers))
  private val maxNumExecutors = conf.getInt(MAX_EXECUTORS_KEY, Integer.MAX_VALUE)
  private val timer = new RecurringTimer(clock, scalingIntervalSecs * 1000,
    _ => manageAllocation(), "streaming-executor-allocation-manager")

  @volatile private var batchProcTimeSum = 0L
  @volatile private var batchProcTimeCount = 0

  validateSettings()

  def start(): Unit = {
    timer.start()
    logInfo(s"ExecutorAllocationManager started with " +
      s"ratios = [$scalingUpRatio, $scalingDownRatio] and interval = $scalingIntervalSecs sec")
  }

  def stop(): Unit = {
    timer.stop(interruptTimer = true)
    logInfo("ExecutorAllocationManager stopped")
  }

  /**
   * Manage executor allocation by requesting or killing executors based on the collected
   * batch statistics.
   */
  private def manageAllocation(): Unit = synchronized {
    logInfo(s"Managing executor allocation with ratios = [$scalingUpRatio, $scalingDownRatio]")
    if (batchProcTimeCount > 0) {
      val averageBatchProcTime = batchProcTimeSum / batchProcTimeCount
      val ratio = averageBatchProcTime.toDouble / batchDurationMs
      logInfo(s"Average: $averageBatchProcTime, ratio = $ratio" )
      if (ratio >= scalingUpRatio) {
        logDebug("Requesting executors")
        val numNewExecutors = math.max(math.round(ratio).toInt, 1)
        requestExecutors(numNewExecutors)
      } else if (ratio <= scalingDownRatio) {
        logDebug("Killing executors")
        killExecutor()
      }
    }
    batchProcTimeSum = 0
    batchProcTimeCount = 0
  }

  /** Request the specified number of executors over the currently active one */
  private def requestExecutors(numNewExecutors: Int): Unit = {
    require(numNewExecutors >= 1)
    val allExecIds = client.getExecutorIds()
    logDebug(s"Executors (${allExecIds.size}) = ${allExecIds}")
    val targetTotalExecutors =
      math.max(math.min(maxNumExecutors, allExecIds.size + numNewExecutors), minNumExecutors)
    client.requestTotalExecutors(targetTotalExecutors, 0, Map.empty)
    logInfo(s"Requested total $targetTotalExecutors executors")
  }

  /** Kill an executor that is not running any receiver, if possible */
  private def killExecutor(): Unit = {
    val allExecIds = client.getExecutorIds()
    logDebug(s"Executors (${allExecIds.size}) = ${allExecIds}")

    if (allExecIds.nonEmpty && allExecIds.size > minNumExecutors) {
      val execIdsWithReceivers = receiverTracker.allocatedExecutors.values.flatten.toSeq
      logInfo(s"Executors with receivers (${execIdsWithReceivers.size}): ${execIdsWithReceivers}")

      val removableExecIds = allExecIds.diff(execIdsWithReceivers)
      logDebug(s"Removable executors (${removableExecIds.size}): ${removableExecIds}")
      if (removableExecIds.nonEmpty) {
        val execIdToRemove = removableExecIds(Random.nextInt(removableExecIds.size))
        client.killExecutor(execIdToRemove)
        logInfo(s"Requested to kill executor $execIdToRemove")
      } else {
        logInfo(s"No non-receiver executors to kill")
      }
    } else {
      logInfo("No available executor to kill")
    }
  }

  private def addBatchProcTime(timeMs: Long): Unit = synchronized {
    batchProcTimeSum += timeMs
    batchProcTimeCount += 1
    logDebug(
      s"Added batch processing time $timeMs, sum = $batchProcTimeSum, count = $batchProcTimeCount")
  }

  private def validateSettings(): Unit = {
    require(
      scalingIntervalSecs > 0,
      s"Config $SCALING_INTERVAL_KEY must be more than 0")

    require(
      scalingUpRatio > 0,
      s"Config $SCALING_UP_RATIO_KEY must be more than 0")

    require(
      scalingDownRatio > 0,
      s"Config $SCALING_DOWN_RATIO_KEY must be more than 0")

    require(
      minNumExecutors > 0,
      s"Config $MIN_EXECUTORS_KEY must be more than 0")

    require(
      maxNumExecutors > 0,
      s"$MAX_EXECUTORS_KEY must be more than 0")

    require(
      scalingUpRatio > scalingDownRatio,
      s"Config $SCALING_UP_RATIO_KEY must be more than config $SCALING_DOWN_RATIO_KEY")

    if (conf.contains(MIN_EXECUTORS_KEY) && conf.contains(MAX_EXECUTORS_KEY)) {
      require(
        maxNumExecutors >= minNumExecutors,
        s"Config $MAX_EXECUTORS_KEY must be more than config $MIN_EXECUTORS_KEY")
    }
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    logDebug("onBatchCompleted called: " + batchCompleted)
    if (!batchCompleted.batchInfo.outputOperationInfos.values.exists(_.failureReason.nonEmpty)) {
      batchCompleted.batchInfo.processingDelay.foreach(addBatchProcTime)
    }
  }
}

private[streaming] object ExecutorAllocationManager extends Logging {
  val ENABLED_KEY = "spark.streaming.dynamicAllocation.enabled"

  val SCALING_INTERVAL_KEY = "spark.streaming.dynamicAllocation.scalingInterval"
  val SCALING_INTERVAL_DEFAULT_SECS = 60

  val SCALING_UP_RATIO_KEY = "spark.streaming.dynamicAllocation.scalingUpRatio"
  val SCALING_UP_RATIO_DEFAULT = 0.9

  val SCALING_DOWN_RATIO_KEY = "spark.streaming.dynamicAllocation.scalingDownRatio"
  val SCALING_DOWN_RATIO_DEFAULT = 0.3

  val MIN_EXECUTORS_KEY = "spark.streaming.dynamicAllocation.minExecutors"

  val MAX_EXECUTORS_KEY = "spark.streaming.dynamicAllocation.maxExecutors"

  def isDynamicAllocationEnabled(conf: SparkConf): Boolean = {
    val numExecutor = conf.getInt("spark.executor.instances", 0)
    val streamingDynamicAllocationEnabled = conf.getBoolean(ENABLED_KEY, false)
    if (numExecutor != 0 && streamingDynamicAllocationEnabled) {
      throw new IllegalArgumentException(
        "Dynamic Allocation for streaming cannot be enabled while spark.executor.instances is set.")
    }
    if (Utils.isDynamicAllocationEnabled(conf) && streamingDynamicAllocationEnabled) {
      throw new IllegalArgumentException(
        """
          |Dynamic Allocation cannot be enabled for both streaming and core at the same time.
          |Please disable core Dynamic Allocation by setting spark.dynamicAllocation.enabled to
          |false to use Dynamic Allocation in streaming.
        """.stripMargin)
    }
    val testing = conf.getBoolean("spark.streaming.dynamicAllocation.testing", false)
    numExecutor == 0 && streamingDynamicAllocationEnabled && (!Utils.isLocalMaster(conf) || testing)
  }

  def createIfEnabled(
      client: ExecutorAllocationClient,
      receiverTracker: ReceiverTracker,
      conf: SparkConf,
      batchDurationMs: Long,
      clock: Clock): Option[ExecutorAllocationManager] = {
    if (isDynamicAllocationEnabled(conf)) {
      Some(new ExecutorAllocationManager(client, receiverTracker, conf, batchDurationMs, clock))
    } else None
  }
}
