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

import com.google.protobuf.MessageLite
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.internal.Logging
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
private[connect] class ExecuteGrpcResponseSender[T <: MessageLite](
    val executeHolder: ExecuteHolder,
    grpcObserver: StreamObserver[T])
    extends Logging {

  private var executionObserver = executeHolder.responseObserver
    .asInstanceOf[ExecuteResponseObserver[T]]

  private var detached = false

  // Signal to wake up when grpcCallObserver.isReady()
  private val grpcCallObserverReadySignal = new Object

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
  def run(lastConsumedStreamIndex: Long): Unit = {
    logDebug(
      s"GrpcResponseSender run for $executeHolder, " +
        s"reattachable=${executeHolder.reattachable}, " +
        s"lastConsumedStreamIndex=$lastConsumedStreamIndex")

    // register to be notified about available responses.
    executionObserver.attachConsumer(this)

    // In reattachable execution, we check if grpcCallObserver is ready for sending.
    // See sendResponse
    if (executeHolder.reattachable) {
      val grpcCallObserver = grpcObserver.asInstanceOf[ServerCallStreamObserver[T]]
      grpcCallObserver.setOnReadyHandler(() => {
        grpcCallObserverReadySignal.synchronized {
          grpcCallObserverReadySignal.notifyAll()
        }
      })
    }

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
      logDebug(s"Trying to get next response with index=$nextIndex.")
      executionObserver.synchronized {
        logDebug(s"Acquired executionObserver lock.")
        while (!detachedFromObserver &&
          !gotResponse &&
          !streamFinished &&
          !deadlineLimitReached) {
          logDebug(s"Try to get response with index=$nextIndex from observer.")
          response = executionObserver.consumeResponse(nextIndex)
          logDebug(s"Response index=$nextIndex from observer: ${response.isDefined}")
          // If response is empty, release executionObserver lock and wait to get notified.
          // The state of detached, response and lastIndex are change under lock in
          // executionObserver, and will notify upon state change.
          if (response.isEmpty) {
            val timeout = Math.max(1, deadlineTimeMillis - System.currentTimeMillis())
            logDebug(s"Wait for response to become available with timeout=$timeout ms.")
            executionObserver.wait(timeout)
            logDebug(s"Reacquired executionObserver lock after waiting.")
          }
        }
        logDebug(
          s"Exiting loop: detached=$detached, response=$response, " +
            s"lastIndex=${executionObserver.getLastResponseIndex()}, " +
            s"deadline=${deadlineLimitReached}")
      }

      // Process the outcome of the inner loop.
      if (detachedFromObserver) {
        // This sender got detached by the observer.
        // This only happens if this RPC is actually dead, and the client already came back with
        // a ReattachExecute RPC. Kill this RPC.
        logDebug(s"Detached from observer at index ${nextIndex - 1}. Complete stream.")
        throw new SparkSQLException(errorClass = "INVALID_CURSOR.DISCONNECTED", Map.empty)
      } else if (gotResponse) {
        // There is a response available to be sent.
        val sent = sendResponse(response.get.response, deadlineTimeMillis)
        if (sent) {
          logDebug(s"Sent response index=$nextIndex.")
          sentResponsesSize += response.get.serializedByteSize
          nextIndex += 1
          assert(finished == false)
        } else {
          // If it wasn't sent, time deadline must have been reached before stream became available.
          assert(deadlineLimitReached)
          finished = true
        }
      } else if (streamFinished) {
        // Stream is finished and all responses have been sent
        logDebug(s"Stream finished and sent all responses up to index ${nextIndex - 1}.")
        executionObserver.getError() match {
          case Some(t) => grpcObserver.onError(t)
          case None => grpcObserver.onCompleted()
        }
        finished = true
      } else if (deadlineLimitReached) {
        // The stream is not complete, but should be finished now.
        // The client needs to reattach with ReattachExecute.
        logDebug(s"Deadline reached, finishing stream after index ${nextIndex - 1}.")
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
  private def sendResponse(response: T, deadlineTimeMillis: Long): Boolean = {
    if (!executeHolder.reattachable) {
      // no flow control in non-reattachable execute
      grpcObserver.onNext(response)
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
        logDebug(s"Acquired grpcCallObserverReadySignal lock.")
        while (!grpcCallObserver.isReady() && deadlineTimeMillis >= System.currentTimeMillis()) {
          val timeout = Math.max(1, deadlineTimeMillis - System.currentTimeMillis())
          logDebug(s"Wait for grpcCallObserver to become ready with timeout=$timeout ms.")
          grpcCallObserverReadySignal.wait(timeout)
          logDebug(s"Reacquired grpcCallObserverReadySignal lock after waiting.")
        }
        if (grpcCallObserver.isReady()) {
          logDebug(s"grpcCallObserver is ready, sending response.")
          grpcCallObserver.onNext(response)
          true
        } else {
          logDebug(s"grpcCallObserver is not ready, exiting.")
          false
        }
      }
    }
  }
}
