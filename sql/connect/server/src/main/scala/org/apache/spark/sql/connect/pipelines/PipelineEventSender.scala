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

package org.apache.spark.sql.connect.pipelines

import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import com.google.protobuf.{Timestamp => ProtoTimestamp}
import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.common.FlowStatus
import org.apache.spark.sql.pipelines.logging.{FlowProgress, PipelineEvent, RunProgress}
import org.apache.spark.util.ThreadUtils

/**
 * Handles sending pipeline events to the client in a background thread. This prevents pipeline
 * execution from blocking on streaming events.
 */
class PipelineEventSender(
    responseObserver: StreamObserver[ExecutePlanResponse],
    sessionHolder: SessionHolder)
    extends Logging
    with AutoCloseable {

  private final val queueCapacity: Int =
    sessionHolder.session.conf
      .get(SQLConf.PIPELINES_EVENT_QUEUE_CAPACITY.key)
      .toInt

  // ExecutorService for background event processing
  private val executor: ThreadPoolExecutor =
    ThreadUtils.newDaemonSingleThreadExecutor(threadName =
      s"PipelineEventSender-${sessionHolder.sessionId}")

  /*
   * Atomic flags to track the state of the sender
   * - `isShutdown`: Indicates if the sender has been shut down, if true, no new events
   *    can be accepted, and the executor will be shut down after processing all submitted events.
   */
  private val isShutdown = new AtomicBoolean(false)

  /**
   * Send an event async by submitting it to the executor, if the sender is not shut down.
   * Otherwise, throws an IllegalStateException, to raise awareness of the shutdown state.
   *
   * For RunProgress events, we ensure they are always queued even if the queue is full. For other
   * events, we may drop them if the queue is at capacity to prevent blocking.
   */
  def sendEvent(event: PipelineEvent): Unit = synchronized {
    if (!isShutdown.get()) {
      if (shouldEnqueueEvent(event)) {
        executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              sendEventToClient(event)
            } catch {
              case NonFatal(e) =>
                logError(
                  log"Failed to send pipeline event to client: " +
                    log"${MDC(LogKeys.ERROR, event.message)}",
                  e)
            }
          }
        })
      }
    } else {
      throw new IllegalStateException(
        s"Cannot send event after shutdown for session ${sessionHolder.sessionId}")
    }
  }

  private def shouldEnqueueEvent(event: PipelineEvent): Boolean = {
    event.details match {
      case _: RunProgress =>
        // For RunProgress events, always enqueue event
        true
      case flowProgress: FlowProgress if FlowStatus.isTerminal(flowProgress.status) =>
        // For FlowProgress events that are terminal, always enqueue event
        true
      case _ =>
        // For other events, check if we have capacity
        executor.getQueue.size() < queueCapacity
    }
  }

  // Implementing AutoCloseable to allow for try-with-resources usage
  // This will ensure that the sender is properly shut down and all resources are released
  // without requiring explicit shutdown calls in user code.
  override def close(): Unit = shutdown()

  /**
   * Shutdown the event sender, stop taking new events and wait for processing to complete. This
   * method blocks until all queued events have been processed. Idempotent operation: calling this
   * multiple times has no effect after the first call.
   */
  def shutdown(): Unit = {
    if (isShutdown.compareAndSet(false, true)) {
      // Request a shutdown of the executor which waits for all tasks to complete
      executor.shutdown()
      // Blocks until all tasks have completed execution after a shutdown request,
      // disregard the timeout since we want all events to be processed
      if (!executor.awaitTermination(Long.MaxValue, java.util.concurrent.TimeUnit.MILLISECONDS)) {
        logError(
          log"Pipeline event sender for session " +
            log"${MDC(LogKeys.SESSION_ID, sessionHolder.sessionId)} failed to terminate")
        executor.shutdownNow()
      }
      logInfo(
        log"Pipeline event sender shutdown completed for session " +
          log"${MDC(LogKeys.SESSION_ID, sessionHolder.sessionId)}")
    }
  }

  /**
   * Send a single event to the client
   */
  private[connect] def sendEventToClient(event: PipelineEvent): Unit = {
    try {
      val protoEvent = constructProtoEvent(event)
      responseObserver.onNext(
        proto.ExecutePlanResponse
          .newBuilder()
          .setSessionId(sessionHolder.sessionId)
          .setServerSideSessionId(sessionHolder.serverSessionId)
          .setPipelineEventResult(proto.PipelineEventResult.newBuilder
            .setEvent(protoEvent)
            .build())
          .build())
    } catch {
      case NonFatal(e) =>
        logError(
          log"Failed to send pipeline event to client: " +
            log"${MDC(LogKeys.ERROR, event.message)}",
          e)
    }
  }

  private def constructProtoEvent(event: PipelineEvent): proto.PipelineEvent = {
    val protoEventBuilder = proto.PipelineEvent
      .newBuilder()
      .setTimestamp(
        ProtoTimestamp
          .newBuilder()
          // java.sql.Timestamp normalizes its internal fields: getTime() returns
          // the full timestamp in milliseconds, while getNanos() returns the
          // fractional seconds (0-999,999,999 ns). This ensures no precision is
          // lost or double-counted.
          .setSeconds(event.timestamp.getTime / 1000)
          .setNanos(event.timestamp.getNanos)
          .build())
      .setMessage(event.messageWithError)
    protoEventBuilder.build()
  }
}
