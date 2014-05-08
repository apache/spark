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
trait Clock {
  def currentTime(): Long
  def waitTillTime(targetTime: Long): Long
}

private[streaming]
class SystemClock(val startTimeMs: Option[Long] = None) extends Clock {

  val minPollTime = 25L
  val deltaTimeMs = startTimeMs.map(_ - System.currentTimeMillis()).getOrElse(0L)

  def currentTime(): Long = {
    System.currentTimeMillis() + deltaTimeMs
  }

  def waitTillTime(targetTime: Long): Long = {
    var currTime = 0L
    currTime = currentTime()

    var waitTime = targetTime - currTime
    if (waitTime <= 0) {
      return currTime
    }

    val pollTime = {
      if (waitTime / 10.0 > minPollTime) {
        (waitTime / 10.0).toLong
      } else {
        minPollTime
      }
    }

    while (true) {
      currTime = currentTime()
      waitTime = targetTime - currTime
      if (waitTime <= 0) {
        return currTime
      }
      val sleepTime =
        if (waitTime < pollTime) {
          waitTime
        } else {
          pollTime
        }
      Thread.sleep(sleepTime)
    }
    -1
  }
}

private[streaming]
class ManualClock() extends Clock {

  var time = 0L

  def currentTime() = time

  def setTime(timeToSet: Long) = {
    this.synchronized {
      time = timeToSet
      this.notifyAll()
    }
  }

  def addToTime(timeToAdd: Long) = {
    this.synchronized {
      time += timeToAdd
      this.notifyAll()
    }
  }
  def waitTillTime(targetTime: Long): Long = {
    this.synchronized {
      while (time < targetTime) {
        this.wait(100)
      }
    }
    currentTime()
  }
}
