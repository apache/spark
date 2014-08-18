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

import org.apache.spark.{Logging, SparkConf}
import java.util.concurrent.TimeUnit._

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

  private var lastSyncTime = System.nanoTime
  private var messagesWrittenSinceSync = 0L
  private val desiredRate = conf.getInt("spark.streaming.receiver.maxRate", 0)
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
