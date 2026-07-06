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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.internal.Logging

/**
 * Captures an error raised out of band (e.g. on a Netty event-loop or other background thread) so
 * the owning task / driver thread can surface it at a safe point.
 *
 * A background thread records the first error via [[markError]] rather than failing the task
 * directly; the owning thread polls [[throwErrorIfExists]] (or [[getError]]) at a safe point and
 * re-throws on its own thread. This avoids a race where calling `TaskContext.markTaskFailed` from
 * a background thread can run concurrently with the task thread's `markTaskCompleted` and silently
 * skip completion listeners (including cleanup), leading to thread leaks.
 *
 * Shared by the streaming shuffle and the streaming-query async checkpointing paths.
 */
private[spark] class ErrorNotifier extends Logging {

  private val error = new AtomicReference[Throwable]

  /**
   * Record a fatal error. Only the first error is retained - subsequent calls
   * are no-ops so cascading failures cannot mask the original cause.
   */
  def markError(th: Throwable): Unit = {
    if (error.compareAndSet(null, th)) {
      logError(log"A fatal error has occurred.", th)
    } else {
      // Attach subsequent errors as suppressed so they're not silently lost.
      val existing = error.get()
      if (existing != null && existing != th) {
        existing.addSuppressed(th)
      }
    }
  }

  /** Get any errors that have occurred */
  def getError(): Option[Throwable] = {
    Option(error.get())
  }

  /** Throw errors that have occurred */
  def throwErrorIfExists(): Unit = {
    getError().foreach({th => throw th})
  }
}
