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

private[streaming]
class RecurringTimer(val clock: Clock, val period: Long, val callback: (Long) => Unit) {
  
  private val minPollTime = 25L
  
  private val pollTime = {
    if (period / 10.0 > minPollTime) {
      (period / 10.0).toLong
    } else {
      minPollTime
    }  
  }
  
  private val thread = new Thread() {
    override def run() { loop }    
  }
  
  private var nextTime = 0L

  def getStartTime(): Long = {
    (math.floor(clock.currentTime.toDouble / period) + 1).toLong * period
  }

  def getRestartTime(originalStartTime: Long): Long = {
    val gap = clock.currentTime - originalStartTime
    (math.floor(gap.toDouble / period).toLong + 1) * period + originalStartTime
  }

  def start(startTime: Long): Long = {
    nextTime = startTime
    thread.start()
    nextTime
  }

  def start(): Long = {
    start(getStartTime())
  }

  def stop() {
    thread.interrupt() 
  }
  
  private def loop() {
    try {
      while (true) {
        clock.waitTillTime(nextTime)
        callback(nextTime)
        nextTime += period
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
    val timer = new  RecurringTimer(new SystemClock(), period, onRecur)
    timer.start()
    Thread.sleep(30 * 1000)
    timer.stop()
  }
}

