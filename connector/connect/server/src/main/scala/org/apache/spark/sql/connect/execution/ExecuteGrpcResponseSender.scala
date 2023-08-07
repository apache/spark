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

package org.apache.spark.sql.connect.execution

import com.google.protobuf.Message
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.common.ProtoUtils
import org.apache.spark.sql.connect.config.Connect.{CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_DURATION, CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_SIZE}
import org.apache.spark.sql.connect.service.ExecuteHolder

/**
 * ExecuteGrpcResponseSender sends responses to the GRPC stream. It runs on the RPC thread, and
 * gets notified by ExecuteResponseObserver about available responses. It notifies the
 * ExecuteResponseObserver back about cached responses that can be removed after being sent out.
 * @param executeHolder
 *   The execution this sender attaches to.
 * @param grpcObserver
 *   the GRPC request StreamObserver
 */
private[connect] class ExecuteGrpcResponseSender[T <: Message](
    val executeHolder: ExecuteHolder,
    grpcObserver: StreamObserver[T])
    extends Logging {

  private var executionObserver = executeHolder.responseObserver
    .asInstanceOf[ExecuteResponseObserver[T]]

  private var detached = false

  // Signal to wake up when grpcCallObserver.isReady()
  private val grpcCallObserverReadySignal = new Object

  val flowControl = true

  private var consumeSleep = 0L
  private var sendSleep = 0L

  /**
   * Detach this sender from executionObserver. Called only from executionObserver that this
   * sender is attached to. executionObserver holds lock, and needs to notify after this call.
   */
  def detach(): Unit = {
    if (detached == true) {
      throw new IllegalStateException("ExecuteGrpcResponseSender already detached!")
    }
    detached = true
  }

  def run(lastConsumedStreamIndex: Long): Unit = {

    // In reattachable execution, we check if grpcCallObserver is ready for sending.
    // See sendResponse
    if (executeHolder.reattachable && flowControl) {
      val grpcCallObserver = grpcObserver.asInstanceOf[ServerCallStreamObserver[T]]
      grpcCallObserver.setOnReadyHandler(() => {
        logTrace(s"Stream ready, notify grpcCallObserverReadySignal.")
        grpcCallObserverReadySignal.synchronized {
          grpcCallObserverReadySignal.notifyAll()
        }
      })
    }

    // We run in a separate daemon thread
    val t = new Thread {
      override def run(): Unit = {
        try {
          execute(lastConsumedStreamIndex)
        } finally {
          if (!executeHolder.reattachable) {
            // Non reattachable executions release here immediately.
            // (Reattachable executions close release with ReleaseExecute RPC.)
            executeHolder.close()
          }
        }
      }
    }
    t.setDaemon(true) // can I not set daemon and just don't join?
    t.start()
  }

  /**
   * Attach to the executionObserver, consume responses from it, and send them to grpcObserver.
   *
   * In non reattachable execution, it will keep sending responses until the query finishes. In
   * reattachable execution, it can finish earlier after reaching a time deadline or size limit.
   *
   * After this function finishes, the grpcObserver is closed with either onCompleted or onError.
   *
   * @param lastConsumedStreamIndex
   *   the last index that was already consumed and sent. This sender will start from index after
   *   that. 0 means start from beginning (since first response has index 1)
   */
  def execute(lastConsumedStreamIndex: Long): Unit = {
    logInfo(
      s"Starting for opId=${executeHolder.operationId}, " +
        s"reattachable=${executeHolder.reattachable}, " +
        s"lastConsumedStreamIndex=$lastConsumedStreamIndex")
    val startTime = System.nanoTime()

    // register to be notified about available responses.
    executionObserver.attachConsumer(this)

    var nextIndex = lastConsumedStreamIndex + 1
    var finished = false

    // Time at which this sender should finish if the response stream is not finished by then.
    val deadlineTimeMillis = if (!executeHolder.reattachable) {
      Long.MaxValue
    } else {
      val confSize =
        SparkEnv.get.conf.get(CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_DURATION).toLong
      if (confSize > 0) System.currentTimeMillis() + confSize else Long.MaxValue
    }

    // Maximum total size of responses. The response which tips over this threshold will be sent.
    val maximumResponseSize: Long = if (!executeHolder.reattachable) {
      Long.MaxValue
    } else {
      val confSize =
        SparkEnv.get.conf.get(CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_SIZE).toLong
      if (confSize > 0) confSize else Long.MaxValue
    }
    var sentResponsesSize: Long = 0

    while (!finished) {
      var response: Option[CachedStreamResponse[T]] = None

      // Conditions for exiting the inner loop:
      // 1. was detached from response observer
      def detachedFromObserver = detached
      // 2. has a response to send
      def gotResponse = response.nonEmpty
      // 3. sent everything from the stream and the stream is finished
      def streamFinished = executionObserver.getLastResponseIndex().exists(nextIndex > _)
      // 4. time deadline or size limit reached
      def deadlineLimitReached =
        sentResponsesSize > maximumResponseSize || deadlineTimeMillis < System.currentTimeMillis()

      // Get next available response.
      // Wait until either this sender got detached or next response is ready,
      // or the stream is complete and it had already sent all responses.
      logTrace(s"Trying to get next response with index=$nextIndex.")
      executionObserver.synchronized {
        logTrace(s"Acquired executionObserver lock.")
        val sleepStart = System.nanoTime()
        var sleepEnd = 0L
        while (!detachedFromObserver &&
          !gotResponse &&
          !streamFinished &&
          !deadlineLimitReached) {
          logTrace(s"Try to get response with index=$nextIndex from observer.")
          response = executionObserver.consumeResponse(nextIndex)
          logTrace(s"Response index=$nextIndex from observer: ${response.isDefined}")
          // If response is empty, release executionObserver lock and wait to get notified.
          // The state of detached, response and lastIndex are change under lock in
          // executionObserver, and will notify upon state change.
          if (response.isEmpty) {
            val timeout = Math.max(1, deadlineTimeMillis - System.currentTimeMillis())
            logTrace(s"Wait for response to become available with timeout=$timeout ms.")
            executionObserver.wait(timeout)
            logTrace(s"Reacquired executionObserver lock after waiting.")
            sleepEnd = System.nanoTime()
          }
        }
        logTrace(
          s"Exiting loop: detached=$detached, " +
            s"response=${response.map(r => ProtoUtils.abbreviate(r.response))}, " +
            s"lastIndex=${executionObserver.getLastResponseIndex()}, " +
            s"deadline=${deadlineLimitReached}")
        if (sleepEnd > 0) {
          consumeSleep += sleepEnd - sleepStart
          logTrace(s"Slept waiting for execution stream for ${sleepEnd - sleepStart}ns.")
        }
      }

      // Process the outcome of the inner loop.
      if (detachedFromObserver) {
        // This sender got detached by the observer.
        // This only happens if this RPC is actually dead, and the client already came back with
        // a ReattachExecute RPC. Kill this RPC.
        logWarning(
          s"Got detached from opId=${executeHolder.operationId} at index ${nextIndex - 1}." +
            s"totalTime=${System.nanoTime - startTime}ns " +
            s"waitingForResults=${consumeSleep}ns waitingForSend=${sendSleep}ns")
        throw new SparkSQLException(errorClass = "INVALID_CURSOR.DISCONNECTED", Map.empty)
      } else if (gotResponse) {
        // There is a response available to be sent.
        val sent = sendResponse(response.get, deadlineTimeMillis)
        if (sent) {
          sentResponsesSize += response.get.serializedByteSize
          nextIndex += 1
          assert(finished == false)
        } else {
          // If it wasn't sent, time deadline must have been reached before stream became available,
          // will exit in the enxt loop iterattion.
          assert(deadlineLimitReached)
        }
      } else if (streamFinished) {
        // Stream is finished and all responses have been sent
        logInfo(
          s"Stream finished for opId=${executeHolder.operationId}, " +
            s"sent all responses up to last index ${nextIndex - 1}. " +
            s"totalTime=${System.nanoTime - startTime}ns " +
            s"waitingForResults=${consumeSleep}ns waitingForSend=${sendSleep}ns")
        executionObserver.getError() match {
          case Some(t) => grpcObserver.onError(t)
          case None => grpcObserver.onCompleted()
        }
        finished = true
      } else if (deadlineLimitReached) {
        // The stream is not complete, but should be finished now.
        // The client needs to reattach with ReattachExecute.
        logInfo(
          s"Deadline reached, shutting down stream for opId=${executeHolder.operationId} " +
            s"after index ${nextIndex - 1}. + " +
            s"totalTime=${System.nanoTime - startTime}ns " +
            s"waitingForResults=${consumeSleep}ns waitingForSend=${sendSleep}ns")
        grpcObserver.onCompleted()
        finished = true
      }
    }
  }

  /**
   * Send the response to the grpcCallObserver. In reattachable execution, we control the flow,
   * and only pass the response to the grpcCallObserver when it's ready to send. Otherwise,
   * grpcCallObserver.onNext() would return in a non-blocking way, but could queue responses
   * without sending them if the client doesn't keep up receiving them. When pushing more
   * responses to onNext(), there is no insight how far behind the service is in actually sending
   * them out.
   * @param deadlineTimeMillis
   *   when reattachable, wait for ready stream until this deadline.
   * @return
   *   true if the response was sent, false otherwise (meaning deadline passed)
   */
  private def sendResponse(
      response: CachedStreamResponse[T],
      deadlineTimeMillis: Long): Boolean = {
    if (!executeHolder.reattachable || !flowControl) {
      // no flow control in non-reattachable execute
      logDebug(
        s"SEND opId=${executeHolder.operationId} responseId=${response.responseId} " +
          s"idx=${response.streamIndex} (no flow control)")
      grpcObserver.onNext(response.response)
      true
    } else {
      // In reattachable execution, we control the flow, and only pass the response to the
      // grpcCallObserver when it's ready to send.
      // Otherwise, grpcCallObserver.onNext() would return in a non-blocking way, but could queue
      // responses without sending them if the client doesn't keep up receiving them.
      // When pushing more responses to onNext(), there is no insight how far behind the service is
      // in actually sending them out.
      // By sending responses only when grpcCallObserver.isReady(), we control that the actual
      // sending doesn't fall behind what we push from here.
      // By using the deadline, we exit the RPC if the responses aren't picked up by the client.
      // A client that is still interested in continuing the query will need to reattach.

      val grpcCallObserver = grpcObserver.asInstanceOf[ServerCallStreamObserver[T]]

      grpcCallObserverReadySignal.synchronized {
        logTrace(s"Acquired grpcCallObserverReadySignal lock.")
        val sleepStart = System.nanoTime()
        var sleepEnd = 0L
        while (!grpcCallObserver.isReady() && deadlineTimeMillis >= System.currentTimeMillis()) {
          val timeout = Math.max(1, deadlineTimeMillis - System.currentTimeMillis())
          var sleepStart = System.nanoTime()
          logTrace(s"Wait for grpcCallObserver to become ready with timeout=$timeout ms.")
          grpcCallObserverReadySignal.wait(timeout)
          logTrace(s"Reacquired grpcCallObserverReadySignal lock after waiting.")
          sleepEnd = System.nanoTime()
        }
        if (grpcCallObserver.isReady()) {
          val sleepTime = if (sleepEnd > 0L) sleepEnd - sleepStart else 0L
          logDebug(
            s"SEND opId=${executeHolder.operationId} responseId=${response.responseId} " +
              s"idx=${response.streamIndex}" +
              s"(waiting ${sleepTime}ns for GRPC stream to be ready)")
          sendSleep += sleepTime
          grpcCallObserver.onNext(response.response)
          true
        } else {
          logTrace(s"grpcCallObserver is not ready, exiting.")
          false
        }
      }
    }
  }
}
