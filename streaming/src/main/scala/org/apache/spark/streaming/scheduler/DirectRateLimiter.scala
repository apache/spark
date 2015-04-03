package org.apache.spark.streaming.scheduler

import scala.collection.mutable

import org.apache.spark.streaming.{Time, StreamingContext}

private[streaming] trait DirectRateLimiter extends Serializable {
  self: RateLimiter =>

  private val processedRecordsMap = new mutable.HashMap[Time, Long]()
  private val slideDurationInMs = streamingContext.graph.batchDuration.milliseconds
  private var unProcessedRecords: Long = 0L

  private val dynamicRateUpdater = new StreamingListener {
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      val processedRecords = processedRecordsMap.getOrElse(batchCompleted.batchInfo.batchTime, 0L)
      val processedTimeInMs = batchCompleted.batchInfo.processingDelay.getOrElse(
        throw new IllegalStateException("Illegal status: cannot get the processed delay"))

      computeEffectiveRate(processedRecords, processedTimeInMs)

      unProcessedRecords -= processedRecords
      processedRecordsMap --= processedRecordsMap.keys.filter(
        _ < batchCompleted.batchInfo.batchTime)
    }
  }

  streamingContext.addStreamingListener(dynamicRateUpdater)

  def streamingContext: StreamingContext

  def updateProcessedRecords(batchTime: Time, processedRecords: Long): Unit = {
    unProcessedRecords += processedRecords
    processedRecordsMap += ((batchTime, processedRecords))
  }

  def maxMessages: Option[Long] = {
    if (effectiveRate == Int.MaxValue.toDouble) {
      return None
    }

    if (unProcessedRecords > 0) {
      val timeForUnProcessedRecords = (unProcessedRecords / effectiveRate).toLong * 1000
      if (timeForUnProcessedRecords > slideDurationInMs) {
        Some(0L)
      } else {
        Some(((slideDurationInMs - timeForUnProcessedRecords) / 1000 * effectiveRate).toLong)
      }
    } else {
      Some((effectiveRate * slideDurationInMs / 1000).toLong)
    }
  }
}

private[streaming]
class FixedDirectRateLimiter(
    val defaultRate: Double,
    @transient val streamingContext: StreamingContext)
  extends FixedRateLimiter with DirectRateLimiter {
  final def isDriver: Boolean = true
}

private[streaming]
class DynamicDirectRateLimiter(
    val defaultRate: Double,
    val slowStartInitialRate: Double,
    @transient val streamingContext: StreamingContext)
  extends DynamicRateLimiter with DirectRateLimiter {

  final def isDriver: Boolean = true
}

