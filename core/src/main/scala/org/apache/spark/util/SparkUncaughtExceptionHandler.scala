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

import org.apache.spark.internal.Logging

/**
 * The default uncaught exception handler for Executors terminates the whole process, to avoid
 * getting into a bad state indefinitely. Since Executors are relatively lightweight, it's better
 * to fail fast when things go wrong.
 */
private[spark] object SparkUncaughtExceptionHandler
  extends Thread.UncaughtExceptionHandler with Logging {
  private[this] var exitOnException = true

  def apply(exitOnException: Boolean): Thread.UncaughtExceptionHandler = {
    this.exitOnException = exitOnException
    this
  }

  override def uncaughtException(thread: Thread, exception: Throwable) {
    // Make it explicit that uncaught exceptions are thrown when process is shutting down.
    // It will help users when they analyze the executor logs
    val errMsg = "Uncaught exception in thread " + thread
    if (ShutdownHookManager.inShutdown()) {
      logError("[Process in shutdown] " + errMsg, exception)
    } else if (exception.isInstanceOf[Error] ||
      (!exception.isInstanceOf[Error] && exitOnException)) {
      try {
        logError(errMsg + ". Shutting down now..", exception)
        if (exception.isInstanceOf[OutOfMemoryError]) {
          System.exit(SparkExitCode.OOM)
        } else {
          System.exit(SparkExitCode.UNCAUGHT_EXCEPTION)
        }
      } catch {
        case oom: OutOfMemoryError => Runtime.getRuntime.halt(SparkExitCode.OOM)
        case t: Throwable => Runtime.getRuntime.halt(SparkExitCode.UNCAUGHT_EXCEPTION_TWICE)
      }
    } else {
      logError(errMsg, exception)
    }
  }

  def uncaughtException(exception: Throwable) {
    uncaughtException(Thread.currentThread(), exception)
  }
}
