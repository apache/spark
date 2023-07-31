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

class ExecutePlanResponseReattachableIterator(
    request: proto.ExecutePlanRequest,
    channel: ManagedChannel,
    retryPolicy: GrpcRetryHandler.RetryPolicy)
    extends java.util.Iterator[proto.ExecutePlanResponse]
    with Logging {

  val operationId = if (request.hasOperationId) {
    request.getOperationId
  } else {
    UUID.randomUUID.toString
  }

  // We don't want retry handling or error conversion done by the custom stubs.
  private val rawBlockingStub = proto.SparkConnectServiceGrpc.newBlockingStub(channel)
  private val rawAsyncStub = proto.SparkConnectServiceGrpc.newStub(channel)

  private val initialRequest: proto.ExecutePlanRequest = request
    .toBuilder()
    .addRequestOptions(
      proto.ExecutePlanRequest.RequestOption.newBuilder()
        .setReattachOptions(proto.ReattachOptions.newBuilder().setReattachable(true).build())
        .build())
    .setOperationId(operationId)
    .build()

  private var lastResponseId: Option[String] = None
  private var responseComplete: Boolean = false
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
    lastResponseId = Some(ret.getResponseId)
    if (ret.hasResultComplete) {
      responseComplete = true
      releaseExecute(None) // release all
    } else {
      releaseExecute(lastResponseId) // release until this response
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
      // It's possible that the next iterator will also close without any response, so we need
      // to keep iterating in a loop.
      // Eventually, there will be a non empty iterator, because there's a ResultComplete at
      // the end of the stream.
      if (!hasNext && !responseComplete) {
        while (!hasNext) {
          iterator = rawBlockingStub.reattachExecute(createReattachExecuteRequest())
          assert(!responseComplete) // shouldn't change...
          hasNext = iterator.hasNext()
        }
      }
      hasNext
    }
  }

  private def releaseExecute(untilResponseId: Option[String]) = {
    val request = createReleaseExecuteRequest(untilResponseId)
    rawAsyncStub.releaseExecute(
      request,
      createRetryingReleaseExecuteResponseObserer(request)
    )
  }

  private def createReattachExecuteRequest() = {
    val reattach = proto.ReattachExecuteRequest.newBuilder()
      .setSessionId(initialRequest.getSessionId)
      .setUserContext(initialRequest.getUserContext)
      .setOperationId(initialRequest.getOperationId)

    if (initialRequest.hasClientType) {
      reattach.setClientType(initialRequest.getClientType)
    }

    if (lastResponseId.isDefined) {
      reattach.setLastResponseId(lastResponseId.get)
    }
    reattach.build()
  }

  private def createReleaseExecuteRequest(untilResponseId: Option[String]) = {
    val release = proto.ReleaseExecuteRequest.newBuilder()
      .setSessionId(initialRequest.getSessionId)
      .setUserContext(initialRequest.getUserContext)
      .setOperationId(initialRequest.getOperationId)

    if (initialRequest.hasClientType) {
      release.setClientType(initialRequest.getClientType)
    }

    untilResponseId match {
      case None =>
        release.setReleaseType(proto.ReleaseExecuteRequest.ReleaseType.RELEASE_ALL)
      case Some(responseId) =>
        release.setReleaseType(proto.ReleaseExecuteRequest.ReleaseType.RELEASE_UNTIL_RESPONSE)
        release.setUntilResponseId(responseId)
    }

    release.build()
  }

  private def createRetryingReleaseExecuteResponseObserer(
    requestForRetry: proto.ReleaseExecuteRequest, currentRetryNum: Int = 0)
    : StreamObserver[proto.ReleaseExecuteResponse] = {
    new StreamObserver[proto.ReleaseExecuteResponse] {
      override def onNext(v: proto.ReleaseExecuteResponse): Unit = {}
      override def onCompleted(): Unit = {}
      override def onError(t: Throwable): Unit = t match {
        case NonFatal(e) if retryPolicy.canRetry(e) && currentRetryNum < retryPolicy.maxRetries =>
          Thread.sleep(
            (retryPolicy.maxBackoff min retryPolicy.initialBackoff * Math
              .pow(retryPolicy.backoffMultiplier, currentRetryNum)).toMillis)
          rawAsyncStub.releaseExecute(requestForRetry,
            createRetryingReleaseExecuteResponseObserer(requestForRetry, currentRetryNum + 1))
        case _ =>
          logWarning(s"ReleaseExecute failed with exception: $t.")
      }
    }
  }

  /**
   * Retries the given function with exponential backoff according to the client's retryPolicy.
   */
  private def retry[T](fn: => T, currentRetryNum: Int = 0): T =
    GrpcRetryHandler.retry(retryPolicy)(fn, currentRetryNum)
}
