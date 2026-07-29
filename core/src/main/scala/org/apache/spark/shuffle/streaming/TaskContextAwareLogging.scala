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

package org.apache.spark.shuffle.streaming

import scala.concurrent.duration.Duration

import org.apache.spark.TaskContext
import org.apache.spark.internal.{LogEntry, Logging, LogKeys, MessageWithContext}

trait TaskContextAwareLogging extends Logging {

  def context: TaskContext

  private val queryId: Option[String] = Option(context)
    .flatMap(ctx => Option(ctx.getLocalProperty("sql.streaming.queryId")).map(_.take(5)))
    .filter(_.nonEmpty)

  @volatile private var shuffleId: Option[Int] = None

  def setShuffleIdForLogging(shuffleId: Int): Unit = {
    this.shuffleId = Some(shuffleId)
  }

  private def loadTaskId: Option[String] = {
    Option(context)
      .flatMap(ctx => Option(ctx.partitionId()))
      .map(_.toString)
  }

  private def loadStageId: Option[String] = {
    Option(context)
      .flatMap(ctx => Option(ctx.stageId()))
      .map(_.toString)
  }

  protected def formatMessage(
      msg: => String,
      taskId: Option[String] = loadTaskId,
      stageId: Option[String] = loadStageId): String = {
    val taskIdMsg = taskId.map(tid => s"[taskId = $tid] ").getOrElse("")
    val stageIdMsg = stageId.map(sid => s"[stageId = $sid] ").getOrElse("")
    val shuffleIdMsg = shuffleId.map(shid => s"[shuffleId = $shid] ").getOrElse("")
    val queryIdMsg = queryId.map(qid => s"[queryId = $qid] ").getOrElse("")
    s"$queryIdMsg$shuffleIdMsg$stageIdMsg$taskIdMsg$msg"
  }

  override protected def logInfo(msg: => String): Unit =
    super.logInfo(formatMessage(msg))

  override protected def logInfo(entry: LogEntry): Unit =
    super.logInfo(log"${MDC(LogKeys.STREAMING_QUERY_ID, queryId.getOrElse(""))} " +
      log"${MDC(LogKeys.SHUFFLE_ID, shuffleId.getOrElse(-1))} " + entry)

  override protected def logWarning(msg: => String): Unit =
    super.logWarning(formatMessage(msg))

  override protected def logWarning(entry: LogEntry): Unit =
    super.logWarning(log"${MDC(LogKeys.STREAMING_QUERY_ID, queryId.getOrElse(""))} " +
      log"${MDC(LogKeys.SHUFFLE_ID, shuffleId.getOrElse(-1))} " + entry)

  override protected def logDebug(msg: => String): Unit =
    super.logDebug(formatMessage(msg))

  override protected def logError(msg: => String): Unit =
    super.logError(formatMessage(msg))

  override protected def logError(entry: LogEntry): Unit =
    super.logError(log"${MDC(LogKeys.STREAMING_QUERY_ID, queryId.getOrElse(""))} " +
      log"${MDC(LogKeys.SHUFFLE_ID, shuffleId.getOrElse(-1))} " + entry)

  override protected def logError(entry: LogEntry, throwable: Throwable): Unit =
    super.logError(log"${MDC(LogKeys.STREAMING_QUERY_ID, queryId.getOrElse(""))} " +
      log"${MDC(LogKeys.SHUFFLE_ID, shuffleId.getOrElse(-1))} " + entry, throwable)

  override protected def logError(msg: => String, throwable: Throwable): Unit =
    super.logError(formatMessage(msg), throwable)

  protected case class LogThrottler(logFn: LogEntry => Unit, interval: Duration) {
    private var nextLogNanos = Long.MinValue
    private var suppressed = 0

    def apply(msg: => MessageWithContext): Unit = {
      val now = System.nanoTime()
      if (now >= nextLogNanos) {
        logFn(if (suppressed > 0) {
          msg + log" (${MDC(LogKeys.COUNT, suppressed)} suppressed warnings)"
        } else {
          msg
        })
        nextLogNanos = now + interval.toNanos
        suppressed = 0
      } else {
        suppressed += 1
      }
    }
  }
}
