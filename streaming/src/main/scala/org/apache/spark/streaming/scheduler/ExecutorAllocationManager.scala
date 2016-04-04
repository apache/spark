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
import org.apache.spark.util.Clock

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

  @volatile private var batchProcTimeSum = 0L
  @volatile private var batchProcTimeCount = 0

  private val timer = new RecurringTimer(clock, scalingIntervalSecs * 1000,
    _ => manageAllocation(), "streaming-executor-allocation-manager")

  def start(): Unit = {
    timer.start()
    logInfo(s"ExecutorAllocationManager started with " +
      s"ratios = [$scalingUpRatio, $scalingDownRatio] and interval = $scalingIntervalSecs sec")
  }

  def stop(): Unit = {
    timer.stop(interruptTimer = true)
    logInfo("ExecutorAllocationManager stopped")
  }

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

  private def requestExecutors(numNewExecutors: Int): Unit = {
    require(numNewExecutors >= 1)
    val allExecIds = client.getExecutorIds()
    logDebug(s"Executors (${allExecIds.size}) = ${allExecIds}")
    val targetTotalExecutors =
      math.max(math.min(maxNumExecutors, allExecIds.size + numNewExecutors), minNumExecutors)
    client.requestTotalExecutors(targetTotalExecutors, 0, Map.empty)
    logInfo(s"Requested total $targetTotalExecutors executors")
  }

  private def killExecutor(): Unit = {
    val allExecIds = client.getExecutorIds()
    logDebug(s"Executors (${allExecIds.size}) = ${allExecIds}")

    if (allExecIds.nonEmpty && allExecIds.size > minNumExecutors) {
      val execIdsWithReceivers = receiverTracker.getAllocatedExecutors.values.flatten.toSeq
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

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    logDebug("onBatchCompleted called: " + batchCompleted)
    if (!batchCompleted.batchInfo.outputOperationInfos.values.exists(_.failureReason.nonEmpty)) {
      batchCompleted.batchInfo.processingDelay.foreach(addBatchProcTime)
    }
  }
}

private[streaming] object ExecutorAllocationManager {
  val SCALING_INTERVAL_KEY = "spark.streaming.dynamicAllocation.scalingInterval"
  val SCALING_INTERVAL_DEFAULT_SECS = 60

  val SCALING_UP_RATIO_KEY = "spark.streaming.dynamicAllocation.scalingUpRatio"
  val SCALING_UP_RATIO_DEFAULT = 0.9

  val SCALING_DOWN_RATIO_KEY = "spark.streaming.dynamicAllocation.scalingDownRatio"
  val SCALING_DOWN_RATIO_DEFAULT = 0.3

  val MIN_EXECUTORS_KEY = "spark.streaming.dynamicAllocation.minExecutors"

  val MAX_EXECUTORS_KEY = "spark.streaming.dynamicAllocation.maxExecutors"
}
