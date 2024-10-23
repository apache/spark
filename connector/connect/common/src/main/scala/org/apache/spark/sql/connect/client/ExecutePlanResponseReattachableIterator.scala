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

import scala.collection.JavaConverters._
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
 * that. If the initial ExecutePlan did not even reach the server, and hence reattach fails with
 * INVALID_HANDLE.OPERATION_NOT_FOUND, we attempt to retry ExecutePlan.
 *
 * In reattachable execute the server does buffer some responses in case the client needs to
 * backtrack. To let server release this buffer sooner, this iterator asynchronously sends
 * ReleaseExecute RPCs that instruct the server to release responses that it already processed.
 */
class ExecutePlanResponseReattachableIterator(
    request: proto.ExecutePlanRequest,
    channel: ManagedChannel,
    retryPolicy: GrpcRetryHandler.RetryPolicy)
    extends WrappedCloseableIterator[proto.ExecutePlanResponse]
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

  // True after ResultComplete message was seen in the stream.
  // Server will always send this message at the end of the stream, if the underlying iterator
  // finishes without producing one, another iterator needs to be reattached.
  // Visible for testing.
  private[connect] var resultComplete: Boolean = false

  // Initial iterator comes from ExecutePlan request.
  // Note: This is not retried, because no error would ever be thrown here, and GRPC will only
  // throw error on first iter.hasNext() or iter.next()
  // Visible for testing.
  private[connect] var iter: Option[java.util.Iterator[proto.ExecutePlanResponse]] =
    Some(rawBlockingStub.executePlan(initialRequest))

  override def innerIterator: Iterator[proto.ExecutePlanResponse] = iter match {
    case Some(it) => it.asScala
    case None =>
      // The iterator is only unset for short moments while retry exception is thrown.
      // It should only happen in the middle of internal processing. Since this iterator is not
      // thread safe, no-one should be accessing it at this moment.
      throw new IllegalStateException("innerIterator unset")
  }

  override def next(): proto.ExecutePlanResponse = synchronized {
    // hasNext will trigger reattach in case the stream completed without resultComplete
    if (!hasNext()) {
      throw new java.util.NoSuchElementException()
    }

    try {
      // Get next response, possibly triggering reattach in case of stream error.
      val ret = retry {
        callIter(_.next())
      }

      // Record last returned response, to know where to restart in case of reattach.
      lastReturnedResponseId = Some(ret.getResponseId)
      if (ret.hasResultComplete) {
        releaseAll()
      } else {
        releaseUntil(lastReturnedResponseId.get)
      }
      ret
    } catch {
      case NonFatal(ex) =>
        releaseAll() // ReleaseExecute on server after error.
        throw ex
    }
  }

  override def hasNext(): Boolean = synchronized {
    if (resultComplete) {
      // After response complete response
      return false
    }
    try {
      retry {
        var hasNext = callIter(_.hasNext())
        // Graceful reattach:
        // If iter ended, but there was no ResultComplete, it means that there is more,
        // and we need to reattach.
        if (!hasNext && !resultComplete) {
          do {
            iter = None // unset iterator for new ReattachExecute to be called in _call_iter
            assert(!resultComplete) // shouldn't change...
            hasNext = callIter(_.hasNext())
            // It's possible that the new iter will be empty, so we need to loop to get another.
            // Eventually, there will be a non empty iter, because there is always a
            // ResultComplete inserted by the server at the end of the stream.
          } while (!hasNext)
        }
        hasNext
      }
    } catch {
      case NonFatal(ex) =>
        releaseAll() // ReleaseExecute on server after error.
        throw ex
    }
  }

  override def close(): Unit = {
    releaseAll()
  }

  /**
   * Inform the server to release the buffered execution results until and including given result.
   *
   * This will send an asynchronous RPC which will not block this iterator, the iterator can
   * continue to be consumed.
   */
  private def releaseUntil(untilResponseId: String): Unit = {
    if (!resultComplete) {
      val request = createReleaseExecuteRequest(Some(untilResponseId))
      rawAsyncStub.releaseExecute(request, createRetryingReleaseExecuteResponseObserver(request))
    }
  }

  /**
   * Inform the server to release the execution, either because all results were consumed, or the
   * execution finished with error and the error was received.
   *
   * This will send an asynchronous RPC which will not block this. The client continues executing,
   * and if the release fails, server is equipped to deal with abandoned executions.
   */
  private def releaseAll(): Unit = {
    if (!resultComplete) {
      val request = createReleaseExecuteRequest(None)
      rawAsyncStub.releaseExecute(request, createRetryingReleaseExecuteResponseObserver(request))
      resultComplete = true
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
      if (iter.isEmpty) {
        iter = Some(rawBlockingStub.reattachExecute(createReattachExecuteRequest()))
      }
      iterFun(iter.get)
    } catch {
      case ex: StatusRuntimeException
          if Option(StatusProto.fromThrowable(ex))
            .exists(_.getMessage.contains("INVALID_HANDLE.OPERATION_NOT_FOUND")) =>
        if (lastReturnedResponseId.isDefined) {
          throw new IllegalStateException(
            "OPERATION_NOT_FOUND on the server but responses were already received from it.",
            ex)
        }
        // Try a new ExecutePlan, and throw upstream for retry.
        iter = Some(rawBlockingStub.executePlan(initialRequest))
        throw new GrpcRetryHandler.RetryException
      case NonFatal(e) =>
        // Remove the iterator, so that a new one will be created after retry.
        iter = None
        throw e
    }
  }

  /**
   * Create result callback to the asynchronouse ReleaseExecute. The client does not block on
   * ReleaseExecute and continues with iteration, but if it fails with a retryable error, the
   * callback will retrigger the asynchronous ReleaseExecute.
   */
  private def createRetryingReleaseExecuteResponseObserver(
      requestForRetry: proto.ReleaseExecuteRequest)
      : StreamObserver[proto.ReleaseExecuteResponse] = {
    new StreamObserver[proto.ReleaseExecuteResponse] {
      override def onNext(v: proto.ReleaseExecuteResponse): Unit = {}
      override def onCompleted(): Unit = {}
      override def onError(t: Throwable): Unit = {
        var firstTry = true
        try {
          retry {
            if (firstTry) {
              firstTry = false
              throw t // we already failed once, handle first retry
            } else {
              // we already are in async execution thread, can execute further retries sync
              rawBlockingStub.releaseExecute(requestForRetry)
            }
          }
        } catch {
          case NonFatal(e) =>
            logWarning(s"ReleaseExecute failed with exception: $e.")
        }
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
  private def retry[T](fn: => T): T =
    GrpcRetryHandler.retry(retryPolicy)(fn)
}
