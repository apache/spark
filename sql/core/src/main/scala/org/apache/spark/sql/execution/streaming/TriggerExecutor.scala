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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.ProcessingTime
import org.apache.spark.util.{Clock, SystemClock}

trait TriggerExecutor {

  /**
   * Execute batches using `batchRunner`. If `batchRunner` runs `false`, terminate the execution.
   */
  def execute(batchRunner: () => Boolean): Unit
}

/**
 * A trigger executor that runs a batch every `intervalMs` milliseconds.
 */
case class ProcessingTimeExecutor(processingTime: ProcessingTime, clock: Clock = new SystemClock())
  extends TriggerExecutor with Logging {

  private val intervalMs = processingTime.intervalMs

  override def execute(batchRunner: () => Boolean): Unit = {
    while (true) {
      val batchStartTimeMs = clock.getTimeMillis()
      val terminated = !batchRunner()
      if (intervalMs > 0) {
        val batchEndTimeMs = clock.getTimeMillis()
        val batchElapsedTimeMs = batchEndTimeMs - batchStartTimeMs
        if (batchElapsedTimeMs > intervalMs) {
          notifyBatchFallingBehind(batchElapsedTimeMs)
        }
        if (terminated) {
          return
        }
        clock.waitTillTime(nextBatchTime(batchEndTimeMs))
      } else {
        if (terminated) {
          return
        }
      }
    }
  }

  /** Called when a batch falls behind. Expose for test only */
  def notifyBatchFallingBehind(realElapsedTimeMs: Long): Unit = {
    logWarning("Current batch is falling behind. The trigger interval is " +
      s"${intervalMs} milliseconds, but spent ${realElapsedTimeMs} milliseconds")
  }

  /**
   * Returns the start time in milliseconds for the next batch interval, given the current time.
   * Note that a batch interval is inclusive with respect to its start time, and thus calling
   * `nextBatchTime` with the result of a previous call should return the next interval. (i.e. given
   * an interval of `100 ms`, `nextBatchTime(nextBatchTime(0)) = 200` rather than `0`).
   */
  def nextBatchTime(now: Long): Long = {
    now / intervalMs * intervalMs + intervalMs
  }
}
