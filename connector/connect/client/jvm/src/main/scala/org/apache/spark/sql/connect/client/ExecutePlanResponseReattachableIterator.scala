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
package org.apache.spark.sql.connect.client

import java.util.UUID

import scala.util.control.NonFatal

import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging

/**
 * Retryable iterator of ExecutePlanResponses to an ExecutePlan call.
 *
 * It can handle situations when:
 *   - the ExecutePlanResponse stream was broken by retryable network error (governed by
 *     retryPolicy)
 *   - the ExecutePlanResponse was gracefully ended by the server without a ResultComplete
 *     message; this tells the client that there is more, and it should reattach to continue.
 *
 * Initial iterator is the result of an ExecutePlan on the request, but it can be reattached with
 * ReattachExecute request. ReattachExecute request is provided the responseId of last returned
 * ExecutePlanResponse on the iterator to return a new iterator from server that continues after
 * that.
 *
 * Since in reattachable execute the server does buffer some responses in case the client needs to
 * backtrack
 */
class ExecutePlanResponseReattachableIterator(
    request: proto.ExecutePlanRequest,
    channel: ManagedChannel,
    retryPolicy: GrpcRetryHandler.RetryPolicy)
    extends java.util.Iterator[proto.ExecutePlanResponse]
    with Logging {

  val operationId = if (request.hasOperationId) {
    request.getOperationId
  } else {
    // Add operation id, if not present.
    // with operationId set by the client, the client can use it to try to reattach on error
    // even before getting the first response. If the operation in fact didn't even reach the
    // server, that will end with INVALID_HANDLE.OPERATION_NOT_FOUND error.
    UUID.randomUUID.toString
  }

  // Need raw stubs, don't want retry handling or error conversion done by the custom stubs.
  // - this does it's own custom retry handling
  // - error conversion is wrapped around this in CustomSparkConnectBlockingStub,
  //   this needs raw GRPC errors for retries.
  private val rawBlockingStub = proto.SparkConnectServiceGrpc.newBlockingStub(channel)
  private val rawAsyncStub = proto.SparkConnectServiceGrpc.newStub(channel)

  private val initialRequest: proto.ExecutePlanRequest = request
    .toBuilder()
    .addRequestOptions(
      proto.ExecutePlanRequest.RequestOption
        .newBuilder()
        .setReattachOptions(proto.ReattachOptions.newBuilder().setReattachable(true).build())
        .build())
    .setOperationId(operationId)
    .build()

  // ResponseId of the last response returned by next()
  private var lastReturnedResponseId: Option[String] = None

  // True after ResponseComplete message was seen in the stream.
  // Server will always send this message at the end of the stream, if the underlying iterator
  // finishes without producing one, another iterator needs to be reattached.
  private var responseComplete: Boolean = false

  // Initial iterator comes from ExecutePlan request.
  private var iterator: java.util.Iterator[proto.ExecutePlanResponse] =
    rawBlockingStub.executePlan(initialRequest)

  override def next(): proto.ExecutePlanResponse = synchronized {
    // hasNext will trigger reattach in case the stream completed without responseComplete
    if (!hasNext()) {
      throw new java.util.NoSuchElementException()
    }

    // Get next response, possibly triggering reattach in case of stream error.
    var firstTry = true
    val ret = retry {
      if (firstTry) {
        // on first try, we use the existing iterator.
        firstTry = false
      } else {
        // on retry, the iterator is borked, so we need a new one
        iterator = rawBlockingStub.reattachExecute(createReattachExecuteRequest())
      }
      iterator.next()
    }

    // Record last returned response, to know where to restart in case of reattach.
    lastReturnedResponseId = Some(ret.getResponseId)
    if (ret.hasResultComplete) {
      responseComplete = true
      releaseExecute(None) // release all
    } else {
      releaseExecute(lastReturnedResponseId) // release until this response
    }
    ret
  }

  override def hasNext(): Boolean = synchronized {
    if (responseComplete) {
      // After response complete response
      return false
    }
    var firstTry = true
    retry {
      if (firstTry) {
        // on first try, we use the existing iterator.
        firstTry = false
      } else {
        // on retry, the iterator is borked, so we need a new one
        iterator = rawBlockingStub.reattachExecute(createReattachExecuteRequest())
      }
      var hasNext = iterator.hasNext()
      // Graceful reattach:
      // If iterator ended, but there was no ResultComplete, it means that there is more,
      // and we need to reattach.
      if (!hasNext && !responseComplete) {
        do {
          iterator = rawBlockingStub.reattachExecute(createReattachExecuteRequest())
          assert(!responseComplete) // shouldn't change...
          hasNext = iterator.hasNext()
          // It's possible that the new iterator will be empty, so we need to loop to get another.
          // Eventually, there will be a non empty iterator, because there's always a ResultComplete
          // at the end of the stream.
        } while (!hasNext)
      }
      hasNext
    }
  }

  /**
   * Inform the server to release the execution.
   *
   * This will send an asynchronous RPC which will not block this iterator, the iterator can
   * continue to be consumed.
   *
   * Release with untilResponseId informs the server that the iterator has been consumed until and
   * including response with that responseId, and these responses can be freed.
   *
   * Release with None means that the responses have been completely consumed and informs the
   * server that the completed execution can be completely freed.
   */
  private def releaseExecute(untilResponseId: Option[String]): Unit = {
    val request = createReleaseExecuteRequest(untilResponseId)
    rawAsyncStub.releaseExecute(request, createRetryingReleaseExecuteResponseObserer(request))
  }

  /**
   * Create result callback to the asynchronouse ReleaseExecute. The client does not block on
   * ReleaseExecute and continues with iteration, but if it fails with a retryable error, the
   * callback will retrigger the asynchronous ReleaseExecute.
   */
  private def createRetryingReleaseExecuteResponseObserer(
      requestForRetry: proto.ReleaseExecuteRequest,
      currentRetryNum: Int = 0): StreamObserver[proto.ReleaseExecuteResponse] = {
    new StreamObserver[proto.ReleaseExecuteResponse] {
      override def onNext(v: proto.ReleaseExecuteResponse): Unit = {}
      override def onCompleted(): Unit = {}
      override def onError(t: Throwable): Unit = t match {
        case NonFatal(e) if retryPolicy.canRetry(e) && currentRetryNum < retryPolicy.maxRetries =>
          Thread.sleep(
            (retryPolicy.maxBackoff min retryPolicy.initialBackoff * Math
              .pow(retryPolicy.backoffMultiplier, currentRetryNum)).toMillis)
          rawAsyncStub.releaseExecute(
            requestForRetry,
            createRetryingReleaseExecuteResponseObserer(requestForRetry, currentRetryNum + 1))
        case _ =>
          logWarning(s"ReleaseExecute failed with exception: $t.")
      }
    }
  }

  private def createReattachExecuteRequest() = {
    val reattach = proto.ReattachExecuteRequest
      .newBuilder()
      .setSessionId(initialRequest.getSessionId)
      .setUserContext(initialRequest.getUserContext)
      .setOperationId(initialRequest.getOperationId)

    if (initialRequest.hasClientType) {
      reattach.setClientType(initialRequest.getClientType)
    }

    if (lastReturnedResponseId.isDefined) {
      reattach.setLastResponseId(lastReturnedResponseId.get)
    }
    reattach.build()
  }

  private def createReleaseExecuteRequest(untilResponseId: Option[String]) = {
    val release = proto.ReleaseExecuteRequest
      .newBuilder()
      .setSessionId(initialRequest.getSessionId)
      .setUserContext(initialRequest.getUserContext)
      .setOperationId(initialRequest.getOperationId)

    if (initialRequest.hasClientType) {
      release.setClientType(initialRequest.getClientType)
    }

    untilResponseId match {
      case None =>
        release.setReleaseAll(proto.ReleaseExecuteRequest.ReleaseAll.newBuilder().build())
      case Some(responseId) =>
        release
          .setReleaseUntil(
            proto.ReleaseExecuteRequest.ReleaseUntil
              .newBuilder()
              .setResponseId(responseId)
              .build())
    }

    release.build()
  }

  /**
   * Retries the given function with exponential backoff according to the client's retryPolicy.
   */
  private def retry[T](fn: => T, currentRetryNum: Int = 0): T =
    GrpcRetryHandler.retry(retryPolicy)(fn, currentRetryNum)
}
