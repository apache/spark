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

import org.apache.spark.Logging

/**
 * The default uncaught exception handler for Executors terminates the whole process, to avoid
 * getting into a bad state indefinitely. Since Executors are relatively lightweight, it's better
 * to fail fast when things go wrong.
 */
private[spark] object SparkUncaughtExceptionHandler
  extends Thread.UncaughtExceptionHandler with Logging {

  override def uncaughtException(thread: Thread, exception: Throwable) {
    try {
      logError("Uncaught exception in thread " + thread, exception)

      // We may have been called from a shutdown hook. If so, we must not call System.exit().
      // (If we do, we will deadlock.)
      if (!ShutdownHookManager.inShutdown()) {
        if (exception.isInstanceOf[OutOfMemoryError]) {
          System.exit(SparkExitCode.OOM)
        } else {
          System.exit(SparkExitCode.UNCAUGHT_EXCEPTION)
        }
      }
    } catch {
      case oom: OutOfMemoryError => Runtime.getRuntime.halt(SparkExitCode.OOM)
      case t: Throwable => Runtime.getRuntime.halt(SparkExitCode.UNCAUGHT_EXCEPTION_TWICE)
    }
  }

  def uncaughtException(exception: Throwable) {
    uncaughtException(Thread.currentThread(), exception)
  }
}
