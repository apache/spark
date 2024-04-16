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
  /** @return Current system time, in ms. */
  def getTimeMillis(): Long

  // scalastyle:off line.size.limit
  /**
   * Current value of high resolution time source, in ns.
   *
   * This method abstracts the call to the JRE's `System.nanoTime()` call. As with that method, the
   * value here is not guaranteed to be monotonically increasing, but rather a higher resolution
   * time source for use in the calculation of time intervals. The characteristics of the values
   * returned may very from JVM to JVM (or even the same JVM running on different OSes or CPUs), but
   * in general it should be preferred over [[getTimeMillis()]] when calculating time differences.
   *
   * Specifically for Linux on x64 architecture, the following links provide useful information
   * about the characteristics of the value returned:
   *
   *  http://btorpey.github.io/blog/2014/02/18/clock-sources-in-linux/
   *  https://stackoverflow.com/questions/10921210/cpu-tsc-fetch-operation-especially-in-multicore-multi-processor-environment
   *
   * TL;DR: on modern (2.6.32+) Linux kernels with modern (AMD K8+) CPUs, the values returned by
   * `System.nanoTime()` are consistent across CPU cores *and* packages, and provide always
   * increasing values (although it may not be completely monotonic when the system clock is
   * adjusted by NTP daemons using time slew).
   */
  // scalastyle:on line.size.limit
  def nanoTime(): Long

  /**
   * Wait until the wall clock reaches at least the given time. Note this may not actually wait for
   * the actual difference between the current and target times, since the wall clock may drift.
   */
  def waitTillTime(targetTime: Long): Long
}

/**
 * A clock backed by the actual time from the OS as reported by the `System` API.
 */
private[spark] class SystemClock extends Clock {

  val minPollTime = 25L

  /**
   * @return the same time (milliseconds since the epoch)
   *         as is reported by `System.currentTimeMillis()`
   */
  override def getTimeMillis(): Long = System.currentTimeMillis()

  /**
   * @return value reported by `System.nanoTime()`.
   */
  override def nanoTime(): Long = System.nanoTime()

  /**
   * @param targetTime block until the current time is at least this value
   * @return current system time when wait has completed
   */
  override def waitTillTime(targetTime: Long): Long = {
    var currentTime = System.currentTimeMillis()

    var waitTime = targetTime - currentTime
    if (waitTime <= 0) {
      return currentTime
    }

    val pollTime = math.max(waitTime / 10.0, minPollTime.toDouble).toLong

    while (true) {
      currentTime = System.currentTimeMillis()
      waitTime = targetTime - currentTime
      if (waitTime <= 0) {
        return currentTime
      }
      val sleepTime = math.min(waitTime, pollTime)
      Thread.sleep(sleepTime)
    }
    -1
  }
}
