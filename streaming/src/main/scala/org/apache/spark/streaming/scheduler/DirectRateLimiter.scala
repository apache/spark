package org.apache.spark.streaming.scheduler

import java.util.concurrent.TimeUnit._

import scala.collection.mutable

import org.apache.spark.streaming.{Time, StreamingContext}

private[streaming]
trait DirectRateLimiter {
  self: RateLimiter =>

  val defaultRate = streamingContext.conf.getInt("spark.streaming.maxRatePerPartition", 0).toDouble

  private val processedRecordsMap = new mutable.HashMap[Time, Long]()
  private val slideDurationInMs = streamingContext.graph.batchDuration.milliseconds

  private var unProcessedRecords: Long = 0L
  private var desiredProcessRate: Double = Int.MaxValue.toDouble

  private val dynamicRateUpdater = new StreamingListener {
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      val processedRecords = processedRecordsMap.getOrElse(batchCompleted.batchInfo.batchTime, 0L)
      val processedTimeInMs = batchCompleted.batchInfo.processingDelay.getOrElse(
        throw new IllegalStateException("Illegal status: cannot get the processed delay"))

      unProcessedRecords -= processedRecords
      processedRecordsMap --= processedRecordsMap.keys.filter(
        _ < batchCompleted.batchInfo.batchTime)
      desiredProcessRate = processedRecords / slideDurationInMs * 1000
      computeEffectiveRate(processedRecords, processedTimeInMs)
    }
  }

  streamingContext.addStreamingListener(dynamicRateUpdater)

  def streamingContext: StreamingContext

  def updateProcessedRecords(batchTime: Time, processedRecords: Long): Unit = {
    unProcessedRecords += processedRecords
    processedRecordsMap += ((batchTime, processedRecords))
  }

  def maxMessagesPerPartition: Option[Long] = {
    if (effectiveRate == Int.MaxValue.toDouble) {
      return None
    }

    if (effectiveRate == Int.MaxValue.toDouble)
    if (unProcessedRecords > 0) {
      val timeForUnProcessedRecords = (unProcessedRecords / effectiveRate).toLong * 1000
      if (timeForUnProcessedRecords > slideDurationInMs) {
        Some(0L)
      } else {
        Some(((slideDurationInMs - timeForUnProcessedRecords) / 1000 * effectiveRate).toLong)
      }
    }
  }

  def getEffectiveRate = self.effectiveRate
}

private[streaming]
class FixedDirectRateLimiter(
    @transient val streamingContext: StreamingContext)
  extends FixedRateLimiter with DirectRateLimiter {
  final def isDriver: Boolean = true
}

private[streaming]
class DynamicDirectRateLimiter(
    @transient val streamingContext: StreamingContext)
  extends DynamicRateLimiter with DirectRateLimiter {

  final def isDriver: Boolean = true

  def effectiveRate: Double = dynamicRate
}

