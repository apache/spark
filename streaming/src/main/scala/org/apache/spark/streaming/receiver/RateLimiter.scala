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

import com.google.common.util.concurrent.{RateLimiter => GuavaRateLimiter}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingConf.{BACKPRESSURE_INITIAL_RATE, RECEIVER_MAX_RATE}

/**
 * Provides waitToPush() method to limit the rate at which receivers consume data.
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
private[receiver] abstract class RateLimiter(conf: SparkConf) extends Logging {

  // treated as an upper limit
  private val maxRateLimit = conf.get(RECEIVER_MAX_RATE)
  private lazy val rateLimiter = GuavaRateLimiter.create(getInitialRateLimit().toDouble)

  def waitToPush(): Unit = {
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
   * @param newRate A new rate in records per second. It has no effect if it's 0 or negative.
   */
  private[receiver] def updateRate(newRate: Long): Unit =
    if (newRate > 0) {
      if (maxRateLimit > 0) {
        rateLimiter.setRate(newRate.min(maxRateLimit))
      } else {
        rateLimiter.setRate(newRate)
      }
    }

  /**
   * Get the initial rateLimit to initial rateLimiter
   */
  private def getInitialRateLimit(): Long = {
    math.min(conf.get(BACKPRESSURE_INITIAL_RATE), maxRateLimit)
  }
}
