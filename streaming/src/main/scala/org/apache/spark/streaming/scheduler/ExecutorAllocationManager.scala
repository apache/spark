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
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.internal.config.Streaming._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.ExecutorDecommissionInfo
import org.apache.spark.streaming.util.RecurringTimer
import org.apache.spark.util.{Clock, Utils}

/**
 * Class that manages executors allocated to a StreamingContext, and dynamically requests or kills
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
 *
 * Note that an initial set of executors (spark.executor.instances) was allocated when the
 * SparkContext was created. This class scales executors up/down after the StreamingContext
 * has started.
 */
private[streaming] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    receiverTracker: ReceiverTracker,
    conf: SparkConf,
    batchDurationMs: Long,
    clock: Clock) extends StreamingListener with Logging {

  private val scalingIntervalSecs = conf.get(STREAMING_DYN_ALLOCATION_SCALING_INTERVAL)
  private val scalingUpRatio = conf.get(STREAMING_DYN_ALLOCATION_SCALING_UP_RATIO)
  private val scalingDownRatio = conf.get(STREAMING_DYN_ALLOCATION_SCALING_DOWN_RATIO)
  private val minNumExecutors = conf.get(STREAMING_DYN_ALLOCATION_MIN_EXECUTORS)
    .getOrElse(math.max(1, receiverTracker.numReceivers()))
  private val maxNumExecutors = conf.get(STREAMING_DYN_ALLOCATION_MAX_EXECUTORS)
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
    // Just map the targetTotalExecutors to the default ResourceProfile
    client.requestTotalExecutors(
      Map(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID -> targetTotalExecutors),
      Map(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID -> 0),
      Map.empty)
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
        if (conf.get(DECOMMISSION_ENABLED)) {
          client.decommissionExecutor(execIdToRemove,
            ExecutorDecommissionInfo("spark scale down", None),
            adjustTargetNumExecutors = true)
        } else {
          client.killExecutor(execIdToRemove)
        }
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
      scalingUpRatio > scalingDownRatio,
      s"Config ${STREAMING_DYN_ALLOCATION_SCALING_UP_RATIO.key} must be more than config " +
        s"${STREAMING_DYN_ALLOCATION_SCALING_DOWN_RATIO.key}")

    if (conf.contains(STREAMING_DYN_ALLOCATION_MIN_EXECUTORS.key) &&
      conf.contains(STREAMING_DYN_ALLOCATION_MAX_EXECUTORS.key)) {
      require(
        maxNumExecutors >= minNumExecutors,
        s"Config ${STREAMING_DYN_ALLOCATION_MAX_EXECUTORS.key} must be more than config " +
          s"${STREAMING_DYN_ALLOCATION_MIN_EXECUTORS.key}")
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

  def isDynamicAllocationEnabled(conf: SparkConf): Boolean = {
    val streamingDynamicAllocationEnabled = Utils.isStreamingDynamicAllocationEnabled(conf)
    if (Utils.isDynamicAllocationEnabled(conf) && streamingDynamicAllocationEnabled) {
      throw new IllegalArgumentException(
        """
          |Dynamic Allocation cannot be enabled for both streaming and core at the same time.
          |Please disable core Dynamic Allocation by setting spark.dynamicAllocation.enabled to
          |false to use Dynamic Allocation in streaming.
        """.stripMargin)
    }
    streamingDynamicAllocationEnabled
  }

  def createIfEnabled(
      client: ExecutorAllocationClient,
      receiverTracker: ReceiverTracker,
      conf: SparkConf,
      batchDurationMs: Long,
      clock: Clock): Option[ExecutorAllocationManager] = {
    if (isDynamicAllocationEnabled(conf) && client != null) {
      Some(new ExecutorAllocationManager(client, receiverTracker, conf, batchDurationMs, clock))
    } else None
  }
}
