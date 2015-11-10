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

package org.apache.spark.util

/**
 * An interface to represent clocks, so that they can be mocked out in unit tests.
 */
private[spark] trait Clock {

  /** @return the time in milliseconds */
  def getTimeMillis(): Long

  /**
   * @param targetTime block until the current time is at least this value
   * @return current system time when wait has completed
   */
  def waitTillTime(targetTime: Long): Long
}

/**
 * A clock backed by the actual time from the OS as reported by
 * `System.currentTimeMillis()`
 */
private[spark] class SystemClock extends Clock {

  val minPollTime = 25L

  /**
   * @return the same time (milliseconds since the epoch)
   *         as is reported by `System.currentTimeMillis()`
   */
  def getTimeMillis(): Long = System.currentTimeMillis()

  /**
   * @param targetTime block until the current time is at least this value
   * @return current system time when wait has completed
   */
  def waitTillTime(targetTime: Long): Long = {
    var currentTime = 0L
    currentTime = getTimeMillis()

    var waitTime = targetTime - currentTime
    if (waitTime <= 0) {
      return currentTime
    }

    val pollTime = math.max(waitTime / 10.0, minPollTime).toLong

    while (true) {
      currentTime = getTimeMillis()
      waitTime = targetTime - currentTime
      if (waitTime <= 0) {
        return currentTime
      }
      val sleepTime = math.min(waitTime, pollTime)
      Thread.sleep(sleepTime)
    }
    throw new Exception("waitTillTime reached code that should not be reachable")
  }
}

/**
 * A clock whose time monotonically increases, irrespective of
 * changes in the system clock (VM host clock shift, operations or NTP
 * initiated time shift)
 */
private[spark] class MonotonicClock extends SystemClock {

  /** @return the same time in milliseconds as is reported by `System.nanoTime()` */
  override def getTimeMillis(): Long = {
    System.nanoTime() / (1000 * 1000)
  }
}
