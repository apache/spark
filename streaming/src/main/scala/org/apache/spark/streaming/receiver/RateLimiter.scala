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

package org.apache.spark.streaming.receiver

import scala.collection.mutable.ArrayBuffer

import com.google.common.util.concurrent.{RateLimiter => GuavaRateLimiter}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.Clock


/** Provides waitToPush() method to limit the rate at which receivers consume data.
  *
  * waitToPush method will block the thread if too many messages have been pushed too quickly,
  * and only return when a new message has been pushed. It assumes that only one message is
  * pushed at a time.
  *
  * The spark configuration spark.streaming.receiver.maxRate gives the maximum number of messages
  * per second that each receiver will accept.
  *
  * @param conf spark configuration
  */
private[receiver] abstract class RateLimiter(conf: SparkConf, clock: Clock) extends Logging {

  // treated as an upper limit
  private val maxRateLimit = conf.getLong("spark.streaming.receiver.maxRate", Long.MaxValue)
  private lazy val rateLimiter = GuavaRateLimiter.create(getInitialRateLimit().toDouble)

  def waitToPush() {
    rateLimiter.acquire()
  }

  /**
   * Return the current rate limit. If no limit has been set so far, it returns {{{Long.MaxValue}}}.
   */
  def getCurrentLimit: Long = rateLimiter.getRate.toLong

  /**
   * Set the rate limit to `newRate`. The new rate will not exceed the maximum rate configured by
   * {{{spark.streaming.receiver.maxRate}}}, even if `newRate` is higher than that.
   *
   * @param newRate A new rate in events per second. It has no effect if it's 0 or negative.
   */
  private[receiver] def updateRate(newRate: Long): Unit =
    if (newRate > 0) {
      if (maxRateLimit > 0) {
        rateLimiter.setRate(newRate.min(maxRateLimit))
        appendLimitToHistory(newRate.min(maxRateLimit))
      } else {
        rateLimiter.setRate(newRate)
        appendLimitToHistory(newRate)
      }
    }

  /**
   * Get the initial rateLimit to initial rateLimiter
   */
  private def getInitialRateLimit(): Long = {
    math.min(conf.getLong("spark.streaming.backpressure.initialRate", maxRateLimit), maxRateLimit)
  }

  private[receiver] case class RateLimitSnapshot(limit: Double, ts: Long)

  private[receiver] val rateLimitHistory: ArrayBuffer[RateLimitSnapshot] =
    ArrayBuffer(RateLimitSnapshot(getInitialRateLimit().toDouble, -1L))

  /**
   * Logs the rateLimit change history, so that we can do a sum later.
   *
   * @param rate the new rate
   * @param ts at which time the rate changed
   */
  private[receiver] def appendLimitToHistory(rate: Double, ts: Long = clock.getTimeMillis()) {
    rateLimitHistory.synchronized {
      rateLimitHistory += RateLimitSnapshot(rate, ts)
    }
  }

  private val blockIntervalMs = conf.getTimeAsMs("spark.streaming.blockInterval", "200ms")
  require(blockIntervalMs > 0, s"'spark.streaming.blockInterval' should be a positive value")

  /**
   * Calculate the upper bound of how many events can be received in a block interval.
   * Note this should be called for each block interval once and only once.
   *
   * @param ts the ending timestamp of a block interval
   * @return the upper bound of how many events can be received in a block interval
   */
  private[receiver]  def sumHistoryThenTrim(ts: Long = clock.getTimeMillis()): Long = {
    var sum: Double = 0
    rateLimitHistory.synchronized {
      // first add a RateLimitSnapshot
      // this RateLimitSnapshot will be used as the ending of this block interval and the beginning
      // of the next block interval
      rateLimitHistory += RateLimitSnapshot(rateLimitHistory.last.limit, ts)

      // then do a sum
      for (idx <- 0 until rateLimitHistory.length - 1) {
        val duration = rateLimitHistory(idx + 1).ts - (if (rateLimitHistory(idx).ts < 0) {
          rateLimitHistory.last.ts - blockIntervalMs
        }
        else {
          rateLimitHistory(idx).ts
        })
        sum += rateLimitHistory(idx).limit * duration
      }

      // trim the history to the last one
      rateLimitHistory.trimStart(rateLimitHistory.length - 1)
    }

    (sum / 1000).ceil.toLong
  }
}

private[streaming] object RateLimiterHelper {

  def sumRateLimits(rateLimits: Seq[Option[Long]]): Option[Long] = {
    if (rateLimits.length == 0 || rateLimits.count(_.isEmpty) > 0) {
      None
    }
    else {
      val sum = rateLimits.map(_.get).foldLeft(0L) { (x, y) =>
        val z = x + y
        // deals with overflow carefully
        if (z < 0) {
          Long.MaxValue
        } else {
          z
        }
      }
      Some(sum)
    }
  }
}
