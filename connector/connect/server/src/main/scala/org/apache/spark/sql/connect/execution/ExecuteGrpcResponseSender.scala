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

import io.grpc.stub.StreamObserver

import org.apache.spark.internal.Logging

/**
 * ExecuteGrpcResponseSender sends responses to the GRPC stream. It runs on the RPC thread, and
 * gets notified by ExecuteResponseObserver about available responses. It notifies the
 * ExecuteResponseObserver back about cached responses that can be removed after being sent out.
 * @param responseObserver
 *   the GRPC request StreamObserver
 */
private[connect] class ExecuteGrpcResponseSender[T](grpcObserver: StreamObserver[T])
    extends Logging {

  private var detached = false

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
   * @param lastConsumedStreamIndex
   *   the last index that was already consumed and sent. This sender will start from index after
   *   that. 0 means start from beginning (since first response has index 1)
   *
   * @return
   *   true if the execution was detached before stream completed. The caller needs to finish the
   *   grpcObserver stream false if stream was finished. In this case, grpcObserver stream is
   *   already completed.
   */
  def run(
      executionObserver: ExecuteResponseObserver[T],
      lastConsumedStreamIndex: Long): Boolean = {
    // register to be notified about available responses.
    executionObserver.attachConsumer(this)

    var nextIndex = lastConsumedStreamIndex + 1
    var finished = false

    while (!finished) {
      var response: Option[CachedStreamResponse[T]] = None
      // Get next available response.
      // Wait until either this sender got detached or next response is ready,
      // or the stream is complete and it had already sent all responses.
      logDebug(s"Trying to get next response with index=$nextIndex.")
      executionObserver.synchronized {
        logDebug(s"Acquired lock.")
        while (!detached && response.isEmpty &&
          executionObserver.getLastIndex().forall(nextIndex <= _)) {
          logDebug(s"Try to get response with index=$nextIndex from observer.")
          response = executionObserver.getResponse(nextIndex)
          logDebug(s"Response index=$nextIndex from observer: ${response.isDefined}")
          // If response is empty, release executionObserver lock and wait to get notified.
          // The state of detached, response and lastIndex are change under lock in
          // executionObserver, and will notify upon state change.
          if (response.isEmpty) {
            logDebug(s"Wait for response to become available.")
            executionObserver.wait()
            logDebug(s"Reacquired lock after waiting.")
          }
        }
        logDebug(
          s"Exiting loop: detached=$detached, response=$response," +
            s"lastIndex=${executionObserver.getLastIndex()}")
      }

      // Send next available response.
      if (detached) {
        // This sender got detached by the observer.
        logDebug(s"Detached from observer at index ${nextIndex - 1}. Complete stream.")
        finished = true
      } else if (response.isDefined) {
        // There is a response available to be sent.
        grpcObserver.onNext(response.get.response)
        logDebug(s"Sent response index=$nextIndex.")
        nextIndex += 1
      } else if (executionObserver.getLastIndex().forall(nextIndex > _)) {
        // Stream is finished and all responses have been sent
        logDebug(s"Sent all responses up to index ${nextIndex - 1}.")
        executionObserver.getError() match {
          case Some(t) => grpcObserver.onError(t)
          case None => grpcObserver.onCompleted()
        }
        finished = true
      }
    }
    // Return true if stream finished, or false if was detached.
    detached
  }
}
