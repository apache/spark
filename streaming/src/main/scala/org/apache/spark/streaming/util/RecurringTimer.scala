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

package org.apache.spark.streaming.util

import org.apache.spark.Logging

private[streaming]
class RecurringTimer(clock: Clock, period: Long, callback: (Long) => Unit, name: String)
  extends Logging {
  
  private val thread = new Thread("RecurringTimer - " + name) {
    setDaemon(true)
    override def run() { loop }    
  }

  private var prevTime = -1L
  private var nextTime = -1L
  private var stopped = false

  /**
   * Get the earliest time when this timer can be started. The time must be a
   * multiple of this timer's period and more than current time.
   */
  def getStartTime(): Long = {
    (math.floor(clock.currentTime.toDouble / period) + 1).toLong * period
  }

  /**
   * Get the earliest time when this timer can be restarted, based on the earlier start time.
   * The time must be a multiple of this timer's period and more than current time.
   */
  def getRestartTime(originalStartTime: Long): Long = {
    val gap = clock.currentTime - originalStartTime
    (math.floor(gap.toDouble / period).toLong + 1) * period + originalStartTime
  }

  /**
   * Start at the given start time.
   */
  def start(startTime: Long): Long = synchronized {
    nextTime = startTime
    thread.start()
    logInfo("Started timer for " + name + " at time " + nextTime)
    nextTime
  }

  /**
   * Start at the earliest time it can start based on the period.
   */
  def start(): Long = {
    start(getStartTime())
  }

  /**
   * Stop the timer. stopAfterNextCallback = true make it wait for next callback to be completed.
   * Returns the last time when it had called back.
   */
  def stop(stopAfterNextCallback: Boolean = false): Long = synchronized {
    if (!stopped) {
      stopped = true
      if (!stopAfterNextCallback) thread.interrupt()
      thread.join()
    }
    logInfo("Stopped timer for " + name + " after time " + prevTime)
    prevTime
  }

  /**
   * Repeatedly call the callback every interval.
   */
  private def loop() {
    try {
      while (!stopped) {
        clock.waitTillTime(nextTime)
        callback(nextTime)
        prevTime = nextTime
        nextTime += period
        logDebug("Callback for " + name + " called at time " + prevTime)
      }
    } catch {
      case e: InterruptedException =>
    }
  }
}

private[streaming]
object RecurringTimer {
  
  def main(args: Array[String]) {
    var lastRecurTime = 0L
    val period = 1000
    
    def onRecur(time: Long) {
      val currentTime = System.currentTimeMillis()
      println("" + currentTime + ": " + (currentTime - lastRecurTime))
      lastRecurTime = currentTime
    }
    val timer = new  RecurringTimer(new SystemClock(), period, onRecur, "Test")
    timer.start()
    Thread.sleep(30 * 1000)
    timer.stop()
  }
}