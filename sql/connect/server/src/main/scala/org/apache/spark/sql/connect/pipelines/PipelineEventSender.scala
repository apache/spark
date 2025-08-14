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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.pipelines.logging.PipelineEvent
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

  // ExecutorService for background event processing
  private val executor: ThreadPoolExecutor = ThreadUtils
    .newDaemonSingleThreadExecutor(s"pipeline-event-sender-${sessionHolder.sessionId}")

  /*
   * Atomic flags to track the state of the sender
   * - `isShutdown`: Indicates if the sender has been shut down, if true, no new events
   *    can be accepted, and the executor will be shut down after processing all submitted events.
   */
  private val isShutdown = new AtomicBoolean(false)

  /**
   * Send an event async by submitting it to the executor, if the sender is not shut down.
   * Otherwise, throws an IllegalStateException, to raise awareness of the shutdown state.
   */
  def sendEvent(event: PipelineEvent): Unit = {
    if (!isShutdown.get()) {
      executor.submit(new Runnable {
        override def run(): Unit = {
          try {
            sendEventToClient(event)
          } catch {
            case NonFatal(e) =>
              logError(s"Failed to send pipeline event to client: ${event.message}", e)
          }
        }
      })
    } else {
      throw new IllegalStateException(
        s"Cannot send event after shutdown for session ${sessionHolder.sessionId}")
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
          s"Pipeline event sender for session ${sessionHolder.sessionId}" +
            s"failed to terminate")
        executor.shutdownNow()
      }
      logInfo(s"Pipeline event sender shutdown completed for session ${sessionHolder.sessionId}")
    }
  }

  /**
   * Send a single event to the client
   */
  private def sendEventToClient(event: PipelineEvent): Unit = {
    try {
      val message = if (event.error.nonEmpty) {
        // Returns the message associated with a Throwable and all its causes
        def getExceptionMessages(throwable: Throwable): Seq[String] = {
          throwable.getMessage +:
            Option(throwable.getCause).map(getExceptionMessages).getOrElse(Nil)
        }
        val errorMessages = getExceptionMessages(event.error.get)
        s"""${event.message}
           |Error: ${errorMessages.mkString("\n")}""".stripMargin
      } else {
        event.message
      }
      responseObserver.onNext(
        proto.ExecutePlanResponse
          .newBuilder()
          .setSessionId(sessionHolder.sessionId)
          .setServerSideSessionId(sessionHolder.serverSessionId)
          .setPipelineEventResult(
            proto.PipelineEventResult.newBuilder
              .setEvent(
                proto.PipelineEvent
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
                  .setMessage(message)
                  .build())
              .build())
          .build())
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to send pipeline event to client: ${event.message}", e)
    }
  }
}
