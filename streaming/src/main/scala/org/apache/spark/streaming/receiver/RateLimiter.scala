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

import java.util.concurrent.atomic.AtomicInteger

import com.google.common.util.concurrent.{RateLimiter => GuavaRateLimiter}

import org.apache.spark.{Logging, SparkConf}

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
private[receiver] abstract class RateLimiter(conf: SparkConf) extends Logging {

  // treated as an upper limit
  private val maxRateLimit = conf.getInt("spark.streaming.receiver.maxRate", 0)
  private[receiver] var currentRateLimit = new AtomicInteger(maxRateLimit)
  private lazy val rateLimiter = GuavaRateLimiter.create(currentRateLimit.get())

  def waitToPush() {
    if (currentRateLimit.get() > 0) {
      rateLimiter.acquire()
    }
  }

  private[receiver] def updateRate(newRate: Int): Unit =
    if (newRate > 0) {
      try {
        if (maxRateLimit > 0) {
          currentRateLimit.set(newRate.min(maxRateLimit))
        }
        else {
          currentRateLimit.set(newRate)
        }
      } finally {
        rateLimiter.setRate(currentRateLimit.get())
      }
    }
}
