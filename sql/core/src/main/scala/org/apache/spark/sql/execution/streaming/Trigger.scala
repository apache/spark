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

/**
 * A interface that indicates how to run a batch.
 */
trait Trigger {

  /**
   * Execute batches using `batchRunner`. If `batchRunner` runs `false`, terminate the execution.
   */
  def execute(batchRunner: () => Boolean): Unit
}

/**
 * A trigger that runs a batch every `intervalMs` milliseconds.
 */
case class ProcessingTime(intervalMs: Long) extends Trigger with Logging {

  require(intervalMs >= 0, "the interval of trigger should not be negative")

  override def execute(batchRunner: () => Boolean): Unit = {
    while (true) {
      val batchStartTimeMs = System.currentTimeMillis()
      if (!batchRunner()) {
        return
      }
      if (intervalMs > 0) {
        val batchEndTimeMs = System.currentTimeMillis()
        val batchElapsedTimeMs = batchEndTimeMs - batchStartTimeMs
        if (batchElapsedTimeMs > intervalMs) {
          logWarning("Current batch is falling behind. The trigger interval is " +
            s"${intervalMs} milliseconds, but spent ${batchElapsedTimeMs} milliseconds")
        }
        waitUntil(nextBatchTime(batchEndTimeMs))
      }
    }
  }

  private def waitUntil(time: Long): Unit = {
    var now = System.currentTimeMillis()
    while (now < time) {
      Thread.sleep(time - now)
      now = System.currentTimeMillis()
    }
  }

  /** Return the next multiple of intervalMs */
  def nextBatchTime(now: Long): Long = {
    (now - 1) / intervalMs * intervalMs + intervalMs
  }
}
