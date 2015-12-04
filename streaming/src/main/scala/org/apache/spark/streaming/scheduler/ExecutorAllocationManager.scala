package org.apache.spark.streaming.scheduler

import scala.util.Random

import org.apache.spark.streaming.util.RecurringTimer
import org.apache.spark.util.Clock
import org.apache.spark.{ExecutorAllocationClient, Logging, SparkConf}

private[streaming] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    receiverTracker: ReceiverTracker,
    conf: SparkConf,
    batchDurationMs: Long,
    clock: Clock)
  extends StreamingListener with Logging {

  import ExecutorAllocationManager._

  private val scalingIntervalSecs = conf.getTimeAsSeconds(
    "spark.streaming.dynamicAllocation.scalingInterval", s"${DEFAULT_SCALING_INTERVAL_SECS}s")
  private val scalingUpRatio = conf.getDouble(
    "spark.streaming.dynamicAllocation.scalingUpRatio", DEFAULT_SCALE_UP_RATIO)
  private val scalingDownRatio = conf.getDouble(
    "spark.streaming.dynamicAllocation.scalingDownRatio", DEFAULT_SCALE_DOWN_RATIO)

  @volatile private var batchProcTimeSum = 0L
  @volatile private var batchProcTimeCount = 0

  private val timer =  new RecurringTimer(clock, scalingIntervalSecs * 1000,
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
      if (ratio > scalingUpRatio) {
        requestExecutors(ratio)
        logInfo("Requesting executors")
      } else if (ratio < scalingDownRatio) {
        logInfo("Killing executors")
        killExecutor()
      }
    }
    batchProcTimeSum = 0
    batchProcTimeCount = 0
  }

  private def requestExecutors(ratio: Double): Unit = {

    val allExecIds = client.getExecutorIds()
    logInfo(s"Executors (${allExecIds.size}) = ${allExecIds}")
    val numExecsToRequest = allExecIds.size + math.max(math.round(ratio).toInt, 1)
    client.requestTotalExecutors(numExecsToRequest, 0, Map.empty)
    logInfo(s"Requested total $numExecsToRequest")
  }

  private def killExecutor(): Unit = {
    val allExecIds = client.getExecutorIds()
    logInfo(s"Executors (${allExecIds.size}) = ${allExecIds}")

    if (allExecIds.nonEmpty) {
      val execIdsWithReceivers = receiverTracker.getAllocatedExecutors.values.flatten.toSeq
      logInfo(s"Executors with receivers (${execIdsWithReceivers.size}): ${execIdsWithReceivers}")

      val removableExecIds = allExecIds.diff(execIdsWithReceivers)
      logInfo(s"Removable executors (${removableExecIds.size}): ${removableExecIds}")
      if (removableExecIds.nonEmpty) {
        val execIdToRemove = removableExecIds(Random.nextInt(removableExecIds.size))
        client.killExecutor(execIdToRemove)
        logInfo(s"Requested to kill executor $execIdToRemove")
      } else {
        logInfo(s"No non-receiver executors to kill")
      }
    } else {
      logInfo("No executors to kill")
    }
  }

  private def addBatchProcTime(timeMs: Long): Unit = synchronized {
    batchProcTimeSum += timeMs
    batchProcTimeCount += 1
    logInfo(
      s"Added batch processing time $timeMs, sum = $batchProcTimeSum, count = $batchProcTimeCount")
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    logInfo("onBatchCompleted called: " + batchCompleted)
    batchCompleted.batchInfo.processingDelay.foreach(addBatchProcTime)
  }
}

private[streaming] object ExecutorAllocationManager {
  val DEFAULT_SCALING_INTERVAL_SECS = 60
  val DEFAULT_SCALE_UP_RATIO = 0.9
  val DEFAULT_SCALE_DOWN_RATIO = 0.5
}
