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

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{ELAPSED_TIME, TRIGGER_INTERVAL}
import org.apache.spark.util.{Clock, SystemClock}

trait TriggerExecutor {

  private var execCtx: MicroBatchExecutionContext = _

  /**
   * Execute batches using `batchRunner`. If `batchRunner` runs `false`, terminate the execution.
   */
  def execute(batchRunner: (MicroBatchExecutionContext) => Boolean): Unit

  def setNextBatch(execContext: MicroBatchExecutionContext): Unit = {
    execCtx = execContext
  }

  protected def runOneBatch(batchRunner: (MicroBatchExecutionContext)
    => Boolean): Boolean = {
    batchRunner(execCtx)
  }
}

/**
 * A trigger executor that runs a single batch only, then terminates.
 */
case class SingleBatchExecutor() extends TriggerExecutor {

  /**
   * Execute a single batch using `batchRunner`.
   */
  override def execute(batchRunner: (MicroBatchExecutionContext) => Boolean): Unit = {
    runOneBatch(batchRunner)
  }
}

/**
 * A trigger executor that runs multiple batches then terminates.
 */
case class MultiBatchExecutor() extends TriggerExecutor {
  /**
   * Execute multiple batches using `batchRunner`
   */
  override def execute(batchRunner: (MicroBatchExecutionContext) => Boolean): Unit
    = while (runOneBatch(batchRunner)) {}
}

/**
 * A trigger executor that runs a batch every `intervalMs` milliseconds.
 */
case class ProcessingTimeExecutor(
    processingTimeTrigger: ProcessingTimeTrigger,
    clock: Clock = new SystemClock())
  extends TriggerExecutor with Logging {

  private val intervalMs = processingTimeTrigger.intervalMs
  require(intervalMs >= 0)

  override def execute(triggerHandler: (MicroBatchExecutionContext) => Boolean): Unit = {
    while (true) {
      val triggerTimeMs = clock.getTimeMillis()
      val nextTriggerTimeMs = nextBatchTime(triggerTimeMs)
      val terminated = !runOneBatch(triggerHandler)
      if (intervalMs > 0) {
        val batchElapsedTimeMs = clock.getTimeMillis() - triggerTimeMs
        if (batchElapsedTimeMs > intervalMs) {
          notifyBatchFallingBehind(batchElapsedTimeMs)
        }
        if (terminated) {
          return
        }
        clock.waitTillTime(nextTriggerTimeMs)
      } else {
        if (terminated) {
          return
        }
      }
    }
  }

  /** Called when a batch falls behind */
  def notifyBatchFallingBehind(realElapsedTimeMs: Long): Unit = {
    logWarning(log"Current batch is falling behind. The trigger interval is " +
      log"${MDC(TRIGGER_INTERVAL, intervalMs)}} milliseconds, but spent " +
      log"${MDC(ELAPSED_TIME, realElapsedTimeMs)} milliseconds")
  }

  /**
   * Returns the start time in milliseconds for the next batch interval, given the current time.
   * Note that a batch interval is inclusive with respect to its start time, and thus calling
   * `nextBatchTime` with the result of a previous call should return the next interval. (i.e. given
   * an interval of `100 ms`, `nextBatchTime(nextBatchTime(0)) = 200` rather than `0`).
   */
  def nextBatchTime(now: Long): Long = {
    if (intervalMs == 0) now else now / intervalMs * intervalMs + intervalMs
  }
}
