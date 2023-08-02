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

import io.grpc.{ManagedChannel, StatusRuntimeException}
import io.grpc.protobuf.StatusProto
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
  private var iterator: java.util.Iterator[proto.ExecutePlanResponse] = retry {
    execute()
  }

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
        iterator = reattach()
      }
      callIter(_.next())
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
        iterator = reattach()
      }
      var hasNext = callIter(_.hasNext())
      // Graceful reattach:
      // If iterator ended, but there was no ResultComplete, it means that there is more,
      // and we need to reattach.
      if (!hasNext && !responseComplete) {
        do {
          iterator = rawBlockingStub.reattachExecute(createReattachExecuteRequest())
          assert(!responseComplete) // shouldn't change...
          hasNext = callIter(_.hasNext())
          // It's possible that the new iterator will be empty, so we need to loop to get another.
          // Eventually, there will be a non empty iterator, because there's always a ResultComplete
          // at the end of the stream.
        } while (!hasNext)
      }
      hasNext
    }
  }

  /**
   * Get a new iterator to the execution by using ReattachExecute. However, if this fails with
   * this operationId not existing on the server, this means that the initial ExecutePlan request
   * didn't even reach the server. In that case, attempt to start again with ExecutePlan.
   *
   * Called inside retry block, so retryable failure will get handled upstream.
   *
   * Note: From empirical observation even if one would expect an immediate error from the GRPC,
   * but one is only thrown from the first iterator.next() or iterator.hasNext() call. However, in
   * case this is a GRPC quirk that cannot be relied upon, check the error here.
   */
  private def reattach(): java.util.Iterator[proto.ExecutePlanResponse] = {
    try {
      rawBlockingStub.reattachExecute(createReattachExecuteRequest())
    } catch {
      case ex: StatusRuntimeException
          if StatusProto
            .fromThrowable(ex)
            .getMessage
            .contains("INVALID_HANDLE.OPERATION_NOT_FOUND") =>
        if (lastReturnedResponseId.isDefined) {
          throw new IllegalStateException(
            "OPERATION_NOT_FOUND on the server but responses were already received from it.",
            ex)
        }
        // We use the helper that will check if OPERATION_ALREADY_EXISTS out of abundance in case a
        // situation in which some earlier lost ExecutePlan actually reached the server is possible.
        execute()
    }
  }

  /**
   * Start the execution by using ExecutePlan. However, if this fails with this operationId
   * already existing on the server, it means that a previous try has in fact reached the server.
   * In that case, try to reattach to the execution instead.
   *
   * Note: From empirical observation even if one would expect an immediate error from the GRPC,
   * but one is only thrown from the first iterator.next() or iterator.hasNext() call. However, in
   * case this is a GRPC quirk that cannot be relied upon, check the error here.
   */
  private def execute(): java.util.Iterator[proto.ExecutePlanResponse] = {
    try {
      rawBlockingStub.executePlan(initialRequest)
    } catch {
      case ex: StatusRuntimeException
          if StatusProto
            .fromThrowable(ex)
            .getMessage
            .contains("INVALID_HANDLE.OPERATION_ALREADY_EXISTS") =>
        // we just checked that OPERATION_ALREADY_EXISTS, so we don't need to use the helper that
        // would check if OPERATION_NOT_FOUND.
        rawBlockingStub.reattachExecute(createReattachExecuteRequest())
    }
  }

  /**
   * Call next() or hasNext() on the iterator. If this fails with this operationId not existing on
   * the server, this means that the initial ExecutePlan request didn't even reach the server. In
   * that case, attempt to start again with ExecutePlan.
   *
   * Called inside retry block, so retryable failure will get handled upstream.
   */
  private def callIter[V](iterFun: java.util.Iterator[proto.ExecutePlanResponse] => V) = {
    try {
      iterFun(iterator)
    } catch {
      case ex: StatusRuntimeException
          if StatusProto
            .fromThrowable(ex)
            .getMessage
            .contains("INVALID_HANDLE.OPERATION_NOT_FOUND") =>
        if (lastReturnedResponseId.isDefined) {
          throw new IllegalStateException(
            "OPERATION_NOT_FOUND on the server but responses were already received from it.",
            ex)
        }
        // Try a new ExecutePlan, and throw upstream for retry.
        iterator = execute()
        throw new GrpcRetryHandler.RetryException
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
