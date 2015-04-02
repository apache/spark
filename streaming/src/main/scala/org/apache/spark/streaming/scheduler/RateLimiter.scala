package org.apache.spark.streaming.scheduler

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext

private[streaming]
trait RateLimiter {
  def isDriver: Boolean

  def defaultRate: Double

  def computeEffectiveRate(processedRecords: Long, processTimeInMs: Long): Unit

  def effectiveRate: Double
}

private[streaming]
abstract class FixedRateLimiter extends RateLimiter {
  override def effectiveRate: Double = {
    if (defaultRate == 0.0) Int.MaxValue.toDouble else defaultRate
  }

  override def computeEffectiveRate(processedRecords: Long, processTimeInMs: Long): Unit = { }
}

private[streaming]
abstract class DynamicRateLimiter extends RateLimiter {
  // Need to implement as a slow start
  protected var dynamicRate = if (defaultRate == 0.0) Int.MaxValue.toDouble else defaultRate

  override def computeEffectiveRate(processedRecords: Long, processTimeInMs: Long): Unit = {
    assert(isDriver)
    val processRate = if (processedRecords == 0L) {
      Int.MaxValue.toDouble
    } else {
      processedRecords.toDouble / processTimeInMs * 1000
    }

    if (defaultRate <= 0) {
      dynamicRate = processRate
    } else {
      dynamicRate = if (processRate > defaultRate) defaultRate else processRate
    }
  }
}

private[streaming]
object RateLimiter {
  def createReceiverRateLimiter(conf: SparkConf,
      isDriver: Boolean,
      streamId: Int,
      streamingContext: StreamingContext): ReceiverRateLimiter = {
    conf.get("spark.streaming.rateLimiter", "fixed") match {
      case "fixed" => new FixedReceiverRateLimiter(isDriver, streamId, streamingContext)
      case "dynamic" => new DynamicReceiverRateLimiter(isDriver, streamId, streamingContext)
    }
  }

  def createDirectRateLimiter(conf: SparkConf, streamingContext: StreamingContext)
      : DirectRateLimiter = {
    conf.get("spark.streaming.rateLimiter", "fixed") match {
      case "fixed" => new FixedDirectRateLimiter(streamingContext)
      case "dynamic" => new DynamicDirectRateLimiter(streamingContext)
    }
  }
}
