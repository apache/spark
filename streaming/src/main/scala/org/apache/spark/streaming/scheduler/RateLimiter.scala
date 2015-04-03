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
trait FixedRateLimiter extends RateLimiter {
  def effectiveRate: Double = {
    if (defaultRate == 0.0) Int.MaxValue.toDouble else defaultRate
  }

  def computeEffectiveRate(processedRecords: Long, processTimeInMs: Long): Unit = { }
}

private[streaming]
trait DynamicRateLimiter extends RateLimiter {
  def slowStartInitialRate: Double

  protected var dynamicRate = {
    if (defaultRate == 0.0 || slowStartInitialRate > defaultRate)
      slowStartInitialRate
    else
      defaultRate
  }

  def computeEffectiveRate(processedRecords: Long, processTimeInMs: Long): Unit = {
    assert(isDriver)
    val processRate = if (processedRecords == 0L) {
      dynamicRate * 2
    } else {
      processedRecords.toDouble / processTimeInMs * 1000
    }

    if (defaultRate <= 0) {
      dynamicRate = processRate
    } else {
      dynamicRate = if (processRate > defaultRate) defaultRate else processRate
    }
  }

  def effectiveRate: Double = dynamicRate
}

private[streaming]
object RateLimiter {
  def createReceiverRateLimiter(conf: SparkConf,
      isDriver: Boolean,
      streamId: Int,
      streamingContext: StreamingContext): ReceiverRateLimiter = {
    val defaultRate = conf.getInt("spark.streaming.receiver.maxRate", 0).toDouble
    conf.get("spark.streaming.rateLimiter", "fixed") match {
      case "fixed" =>
        new FixedReceiverRateLimiter(isDriver, streamId, defaultRate, streamingContext)
      case "dynamic" =>
        val initialRate = conf.getInt("spark.streaming.rateLimiter.slowStartRate", 128).toDouble
        new DynamicReceiverRateLimiter(isDriver, streamId, defaultRate, initialRate,
          streamingContext)
      case _ =>
        throw new IllegalArgumentException("Unknown name for RateLimiter")
    }
  }

  def createDirectRateLimiter(conf: SparkConf, streamingContext: StreamingContext)
      : DirectRateLimiter = {
    val defaultRate = conf.getInt("spark.streaming.receiver.maxRate", 0).toDouble
    conf.get("spark.streaming.rateLimiter", "fixed") match {
      case "fixed" =>
        new FixedDirectRateLimiter(defaultRate, streamingContext)
      case "dynamic" =>
        val initialRate = conf.getInt("spark.streaming.rateLimiter.slowStartRate", 128).toDouble
        new DynamicDirectRateLimiter(defaultRate, initialRate, streamingContext)
      case _ =>
        throw new IllegalArgumentException("Unknown name for RateLimiter")
    }
  }
}
