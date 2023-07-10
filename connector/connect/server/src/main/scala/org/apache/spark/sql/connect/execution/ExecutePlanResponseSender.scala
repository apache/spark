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

import org.apache.spark.connect.proto.ExecutePlanResponse

/**
 * ExecutePlanResponseSender sends responses to the GRPC stream.
 * It runs on the RPC thread, and gets notified by ExecutePlanResponseObserver about available
 * responses.
 * It notifies the ExecutePlanResponseObserver back about cached responses that can be removed
 * after being sent out.
 * @param responseObserver the GRPC request StreamObserver
 */
private[connect] class ExecutePlanResponseSender(
  executionObserver: ExecutePlanResponseObserver,
  grpcObserver: StreamObserver[ExecutePlanResponse]) {

  executionObserver.setExecutePlanResponseSender(this)

  private val signal = new Object
  private var detached = false

  def notifyResponse(): Unit = {
    signal.synchronized {
      signal.notify()
    }
  }

  def detach(): Unit = {
    signal.synchronized {
      this.detached = true
      signal.notify()
    }
  }

  /**
   * Receive responses from executionObserver and send them to grpcObserver.
   * @param lastSentIndex Start sending the stream from response after this.
   * @return true if finished because stream completed
   *         false if finished because stream was detached
   */
  def run(lastSentIndex: Long): Boolean = {
    // register to be notified about available responses.
    executionObserver.setExecutePlanResponseSender(this)

    var currentIndex = lastSentIndex + 1
    var finished = false

    while (!finished) {
      var response: Option[CachedExecutePlanResponse] = None
      // Get next available response.
      // Wait until either this sender got detached or next response is ready,
      // or the stream is complete and it had already sent all responses.
      signal.synchronized {
        while (!detached && response.isEmpty &&
            executionObserver.getLastIndex().forall(currentIndex <= _)) {
          response = executionObserver.getResponse(currentIndex)
          // If response is empty, wait to get notified.
          // We are cholding signal here, so:
          // - if detach() is waiting on signal, and will acquire it after wait()
          //   here releases it, and wait() will be waked up by notify.
          // - if getLastIndex() or getResponse() changed, executionObserver would call
          //   notify(), which would wait on signal, and will acquire it after wait() here releases
          //   it, and wait will be waked up by notify.
          if (response.isEmpty) {
            signal.wait()
          }
        }
      }

      // Send next available response.
      if (detached) {
        // This sender got detached by the observer.
        finished = true
      } else if (response.isDefined) {
        // There is a response available to be sent.
        grpcObserver.onNext(response.get.r)
        // Remove after sending.
        executionObserver.removeUntilIndex(currentIndex)
        currentIndex += 1
      } else if (executionObserver.getLastIndex().forall(currentIndex > _)) {
        // Stream is finished and all responses have been sent
        executionObserver.getError() match {
          case Some(t) => grpcObserver.onError(t)
          case None => grpcObserver.onCompleted()
        }
        finished = true
      }
    }
    // Return true if stream finished, or false if was detached.
    !detached
  }
}
