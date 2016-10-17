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
 * A `Clock` whose time can be manually set and modified. Its reported time does not change
 * as time elapses, but only as its time is modified by callers. This is mainly useful for
 * testing.
 *
 * @param time initial time (in milliseconds since the epoch)
 */
private[spark] class ManualClock(private var time: Long) extends Clock {

  private var _isWaiting = false
  private var _readyForFirstPeek = false

  /**
   * @return `ManualClock` with initial time 0
   */
  def this() = this(0L)

  def getTimeMillis(): Long =
    synchronized {
      time
    }

  /**
   * @param timeToSet new time (in milliseconds) that the clock should represent
   */
  def setTime(timeToSet: Long): Unit = synchronized {
    time = timeToSet
    _readyForFirstPeek = false
    notifyAll()
  }

  /**
   * Note: if we don't want to advance too early, we should place a `isWaitingAndReadyForFirstPeek`
   * guard before we call this. See SPARK-16002. See also `[sql] StreamTest#AdvanceManualClock`.
   *
   * @param timeToAdd time (in milliseconds) to add to the clock's time
   */
  def advance(timeToAdd: Long): Unit = synchronized {
    time += timeToAdd
    _readyForFirstPeek = false
    notifyAll()
  }

  /**
   * @param targetTime block until the clock time is set or advanced to at least this time
   * @return current time reported by the clock when waiting finishes
   */
  def waitTillTime(targetTime: Long): Long = synchronized {
    _isWaiting = true
    _readyForFirstPeek = true
    try {
      while (time < targetTime) {
        wait(10)
      }
      getTimeMillis()
    } finally {
      _isWaiting = false
      _readyForFirstPeek = false
    }
  }

  /**
   * Returns whether there is any thread being blocked in `waitTillTime`, and this will be the first
   * time it's been peeked in the blocking state.
   */
  def isWaiting: Boolean = synchronized { _isWaiting && _readyForFirstPeek }
}
