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

package org.apache.spark.scheduler

import java.util.{HashMap, Locale, Properties}

import org.apache.spark.internal.{LogEntry, Logging, LogKeys, MessageWithContext}

/**
 * A logging trait for scheduler components where log messages should include
 * structured streaming identifiers (query ID and batch ID).
 *
 * Streaming execution sets these identifiers via
 * [[org.apache.spark.SparkContext#setLocalProperty]], which is thread-local.
 * Scheduler code typically runs on a different thread (e.g. the
 * task-scheduler-event-loop-worker), so `getLocalProperty` would not have
 * the streaming context. This trait instead reads the identifiers from the
 * task's [[java.util.Properties]], which are propagated with the
 * [[org.apache.spark.scheduler.TaskSet]] across thread boundaries.
 *
 * Mix this trait into any scheduler component that has access to task
 * properties and needs streaming-aware log output.
 */
private[scheduler] trait StructuredStreamingIdAwareSchedulerLogging extends Logging {
  // we gather the query and batch Id from the properties of a given TaskSet
  protected def properties: Properties
  protected def streamingIdAwareLoggingEnabled: Boolean
  protected def streamingQueryIdLength: Int

  override protected def logInfo(msg: => String): Unit =
    super.logInfo(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, msg, streamingIdAwareLoggingEnabled, streamingQueryIdLength))

  override protected def logInfo(entry: LogEntry): Unit = {
    super.logInfo(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, entry, streamingIdAwareLoggingEnabled, streamingQueryIdLength))
  }

  override protected def logInfo(msg: => String, t: Throwable): Unit =
    super.logInfo(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, msg, streamingIdAwareLoggingEnabled, streamingQueryIdLength), t)

  override protected def logInfo(entry: LogEntry, t: Throwable): Unit = {
    super.logInfo(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, entry, streamingIdAwareLoggingEnabled, streamingQueryIdLength), t)
  }

  override protected def logWarning(msg: => String): Unit =
    super.logWarning(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, msg, streamingIdAwareLoggingEnabled, streamingQueryIdLength))

  override protected def logWarning(entry: LogEntry): Unit = {
    super.logWarning(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, entry, streamingIdAwareLoggingEnabled, streamingQueryIdLength))
  }

  override protected def logWarning(msg: => String, t: Throwable): Unit =
    super.logWarning(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, msg, streamingIdAwareLoggingEnabled, streamingQueryIdLength), t)

  override protected def logWarning(entry: LogEntry, t: Throwable): Unit = {
    super.logWarning(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, entry, streamingIdAwareLoggingEnabled, streamingQueryIdLength), t)
  }

  override protected def logDebug(msg: => String): Unit =
    super.logDebug(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, msg, streamingIdAwareLoggingEnabled, streamingQueryIdLength))

  override protected def logDebug(entry: LogEntry): Unit = {
    super.logDebug(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, entry, streamingIdAwareLoggingEnabled, streamingQueryIdLength))
  }

  override protected def logDebug(msg: => String, t: Throwable): Unit =
    super.logDebug(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, msg, streamingIdAwareLoggingEnabled, streamingQueryIdLength), t)

  override protected def logDebug(entry: LogEntry, t: Throwable): Unit = {
    super.logDebug(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, entry, streamingIdAwareLoggingEnabled, streamingQueryIdLength), t)
  }

  override protected def logError(msg: => String): Unit =
    super.logError(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, msg, streamingIdAwareLoggingEnabled, streamingQueryIdLength))

  override protected def logError(entry: LogEntry): Unit = {
    super.logError(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, entry, streamingIdAwareLoggingEnabled, streamingQueryIdLength))
  }

  override protected def logError(msg: => String, t: Throwable): Unit =
    super.logError(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, msg, streamingIdAwareLoggingEnabled, streamingQueryIdLength), t)

  override protected def logError(entry: LogEntry, t: Throwable): Unit = {
    super.logError(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, entry, streamingIdAwareLoggingEnabled, streamingQueryIdLength), t)
  }

  override protected def logTrace(msg: => String): Unit =
    super.logTrace(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, msg, streamingIdAwareLoggingEnabled, streamingQueryIdLength))

  override protected def logTrace(entry: LogEntry): Unit = {
    super.logTrace(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, entry, streamingIdAwareLoggingEnabled, streamingQueryIdLength))
  }

  override protected def logTrace(msg: => String, t: Throwable): Unit =
    super.logTrace(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, msg, streamingIdAwareLoggingEnabled, streamingQueryIdLength), t)

  override protected def logTrace(entry: LogEntry, t: Throwable): Unit = {
    super.logTrace(
      StructuredStreamingIdAwareSchedulerLogging
        .constructStreamingLogEntry(
          properties, entry, streamingIdAwareLoggingEnabled, streamingQueryIdLength), t)
  }
}

/**
 * Helpers for constructing log entries enriched with structured streaming
 * identifiers extracted from task properties.
 */
private[scheduler] object StructuredStreamingIdAwareSchedulerLogging extends Logging {
  val QUERY_ID_KEY = "sql.streaming.queryId"
  val BATCH_ID_KEY = "streaming.sql.batchId"

  private[scheduler] def constructStreamingLogEntry(
      properties: Properties,
      entry: LogEntry,
      enabled: Boolean,
      queryIdLength: Int): LogEntry = {
    if (!enabled || properties == null) {
      return entry
    }
    // wrap in log entry to defer until log is evaluated
    new LogEntry({
      val (queryId: Option[String], batchId: Option[String]) =
        getStreamingProperties(properties, queryIdLength)

      formatMessage(
        queryId,
        batchId,
        entry
      )
    })
  }

  private[scheduler] def constructStreamingLogEntry(
      properties: Properties,
      msg: => String,
      enabled: Boolean,
      queryIdLength: Int): LogEntry = {
    if (!enabled || properties == null) {
      return new LogEntry(
        MessageWithContext(msg, java.util.Collections.emptyMap())
      )
    }

    new LogEntry({
      val (queryId: Option[String], batchId: Option[String]) =
        getStreamingProperties(properties, queryIdLength)

      MessageWithContext(
        formatMessage(
          queryId,
          batchId,
          msg
        ),
        constructStreamingContext(queryId, batchId)
      )
    })
  }

  private def constructStreamingContext(
      queryId: Option[String],
      batchId: Option[String]): HashMap[String, String] = {
    val streamingContext = new HashMap[String, String]()
    // MDC places the log key in the context as all lowercase, so we do the same here
    queryId.foreach(streamingContext.put(LogKeys.QUERY_ID.name.toLowerCase(Locale.ROOT), _))
    batchId.foreach(streamingContext.put(LogKeys.BATCH_ID.name.toLowerCase(Locale.ROOT), _))
    streamingContext
  }

  private def formatMessage(
      queryId: Option[String],
      batchId: Option[String],
      msg: => String): String = {
    val msgWithBatchId = batchId.map(bid => s"[batchId = $bid] $msg").getOrElse(msg)
    queryId.map(qId => s"[queryId = $qId] $msgWithBatchId").getOrElse(msgWithBatchId)
  }

  private def formatMessage(
      queryId: Option[String],
      batchId: Option[String],
      msg: => LogEntry): MessageWithContext = {
    val msgWithBatchId: MessageWithContext = batchId.map(
      bId => log"[batchId = ${MDC(LogKeys.BATCH_ID, bId)}] " + toMessageWithContext(msg)
    ).getOrElse(toMessageWithContext(msg))
    queryId.map(
      qId => log"[queryId = ${MDC(LogKeys.QUERY_ID, qId)}] " + msgWithBatchId
    ).getOrElse(msgWithBatchId)
  }

  private def toMessageWithContext(entry: LogEntry): MessageWithContext = {
    MessageWithContext(entry.message, entry.context)
  }

  private def getStreamingProperties(
      properties: Properties,
      queryIdLength: Int): (Option[String], Option[String]) = {
    val queryId = Option(properties.getProperty(QUERY_ID_KEY)).filter(_.nonEmpty).map { id =>
      if (queryIdLength == -1) {
        id
      } else {
        id.take(queryIdLength)
      }
    }
    val batchId = Option(properties.getProperty(BATCH_ID_KEY)).filter(_.nonEmpty)
    (queryId, batchId)
  }
}
