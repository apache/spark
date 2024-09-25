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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.TaskContext
import org.apache.spark.internal.{LogEntry, Logging, LogKeys, MessageWithContext}

trait StateStoreThreadAwareLogging extends Logging {

  private def loadThreadInfo(): String = {
    val tc: TaskContext = TaskContext.get()
    val taskStr = if (tc != null) {
      val taskDetails =
        s"partition ${tc.partitionId()}.${tc.attemptNumber()} in stage " +
          s"${tc.stageId()}.${tc.stageAttemptNumber()}, TID ${tc.taskAttemptId()}"
      s", task: $taskDetails"
    } else ""

    s"[ThreadId: ${Thread.currentThread().getId()}$taskStr]"
  }

  private def appendThreadInfo(msg: => String): LogEntry = {
    val storeThreadCtx = new java.util.HashMap[String, String]()
    val threadInfo = loadThreadInfo()
    storeThreadCtx.put(LogKeys.THREAD_NAME.name, threadInfo)
    new LogEntry(MessageWithContext(s"${threadInfo} $msg", storeThreadCtx))
  }

  private def appendThreadInfo(logEntry: LogEntry): LogEntry = {
    if (logEntry.context.containsKey(LogKeys.THREAD_NAME.name)) {
      logEntry
    } else {
      val storeThreadCtx = new java.util.HashMap[String, String]()
      val threadInfo = loadThreadInfo()
      storeThreadCtx.put(LogKeys.THREAD_NAME.name, threadInfo)
      new LogEntry(MessageWithContext(s"${threadInfo} ${logEntry.message}", storeThreadCtx))
    }
  }

  override protected def logInfo(msg: => String): Unit =
    super.logInfo(appendThreadInfo(msg))

  override protected def logInfo(logEntry: LogEntry): Unit =
    super.logInfo(appendThreadInfo(logEntry))

  override protected def logInfo(logEntry: LogEntry, throwable: Throwable): Unit =
    super.logInfo(appendThreadInfo(logEntry), throwable)

  override protected def logWarning(msg: => String): Unit =
    super.logWarning(appendThreadInfo(msg))

  override protected def logWarning(logEntry: LogEntry): Unit =
    super.logWarning(appendThreadInfo(logEntry))

  override protected def logWarning(logEntry: LogEntry, throwable: Throwable): Unit =
    super.logWarning(appendThreadInfo(logEntry), throwable)

  override protected def logDebug(msg: => String): Unit =
    super.logDebug(appendThreadInfo(msg))

  override protected def logDebug(logEntry: LogEntry): Unit =
    super.logDebug(appendThreadInfo(logEntry))

  override protected def logDebug(logEntry: LogEntry, throwable: Throwable): Unit =
    super.logDebug(appendThreadInfo(logEntry), throwable)

  override protected def logError(msg: => String): Unit =
    super.logError(appendThreadInfo(msg))

  override protected def logError(logEntry: LogEntry): Unit =
    super.logError(appendThreadInfo(logEntry))

  override protected def logError(logEntry: LogEntry, throwable: Throwable): Unit =
    super.logError(appendThreadInfo(logEntry), throwable)

  override protected def logTrace(msg: => String): Unit =
    super.logTrace(appendThreadInfo(msg))

  override protected def logTrace(logEntry: LogEntry): Unit =
    super.logTrace(appendThreadInfo(logEntry))

  override protected def logTrace(logEntry: LogEntry, throwable: Throwable): Unit =
    super.logTrace(appendThreadInfo(logEntry), throwable)
}
