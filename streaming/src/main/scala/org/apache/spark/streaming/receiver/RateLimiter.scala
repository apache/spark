package org.apache.spark.streaming.receiver

import org.apache.spark.{Logging, SparkConf}
import java.util.concurrent.TimeUnit._

abstract class RateLimiter(conf: SparkConf) extends Logging {

  private var lastSyncTime = System.nanoTime
  private var messagesWrittenSinceSync = 0L
  private val desiredRate = conf.getInt("spark.streaming.receiver.maxRate",0)
  private val SYNC_INTERVAL = NANOSECONDS.convert(10, SECONDS)

  def waitToPush() {
    if( desiredRate <= 0 ) {
      return
    }
    val now = System.nanoTime
    val elapsedNanosecs = math.max(now - lastSyncTime, 1)
    val rate = messagesWrittenSinceSync.toDouble * 1000000000 / elapsedNanosecs
    if (rate < desiredRate) {
      // It's okay to write; just update some variables and return
      messagesWrittenSinceSync += 1
      if (now > lastSyncTime + SYNC_INTERVAL) {
        // Sync interval has passed; let's resync
        lastSyncTime = now
        messagesWrittenSinceSync = 1
      }
    } else {
      // Calculate how much time we should sleep to bring ourselves to the desired rate.
      val targetTimeInMillis = messagesWrittenSinceSync * 1000 / desiredRate
      val elapsedTimeInMillis = elapsedNanosecs / 1000000
      val sleepTimeInMillis = targetTimeInMillis - elapsedTimeInMillis
      if (sleepTimeInMillis > 0) {
        logTrace("Natural rate is " + rate + " per second but desired rate is " +
          desiredRate + ", sleeping for " + sleepTimeInMillis + " ms to compensate.")
        Thread.sleep(sleepTimeInMillis)
      }
      waitToPush()
    }
  }
}
