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

package org.apache.spark.mllib.tree.impl

import scala.collection.mutable.{HashMap => MutableHashMap}

/**
 * Time tracker implementation which holds labeled timers.
 */
private[spark] class TimeTracker extends Serializable {

  private val starts: MutableHashMap[String, Long] = new MutableHashMap[String, Long]()

  private val totals: MutableHashMap[String, Long] = new MutableHashMap[String, Long]()

  /**
   * Starts a new timer, or re-starts a stopped timer.
   */
  def start(timerLabel: String): Unit = {
    val currentTime = System.nanoTime()
    if (starts.contains(timerLabel)) {
      throw new RuntimeException(s"TimeTracker.start(timerLabel) called again on" +
        s" timerLabel = $timerLabel before that timer was stopped.")
    }
    starts(timerLabel) = currentTime
  }

  /**
   * Stops a timer and returns the elapsed time in seconds.
   */
  def stop(timerLabel: String): Double = {
    val currentTime = System.nanoTime()
    if (!starts.contains(timerLabel)) {
      throw new RuntimeException(s"TimeTracker.stop(timerLabel) called on" +
        s" timerLabel = $timerLabel, but that timer was not started.")
    }
    val elapsed = currentTime - starts(timerLabel)
    starts.remove(timerLabel)
    if (totals.contains(timerLabel)) {
      totals(timerLabel) += elapsed
    } else {
      totals(timerLabel) = elapsed
    }
    elapsed / 1e9
  }

  /**
   * Print all timing results in seconds.
   */
  override def toString: String = {
    totals.map { case (label, elapsed) =>
        s"  $label: ${elapsed / 1e9}"
      }.mkString("\n")
  }
}
