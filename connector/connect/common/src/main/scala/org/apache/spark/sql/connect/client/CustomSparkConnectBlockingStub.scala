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

import scala.jdk.CollectionConverters._

import com.google.protobuf.GeneratedMessageV3
import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto._

// This is common logic shared between the blocking and non-blocking stubs.
//
// The common logic is responsible to verify the integrity of the response. The invariant is
// that the same stub instance is used for all requests from the same client.
private[client] class SparkConnectCommonStub {
  // Server side session ID, used to detect if the server side session changed. This is set upon
  // receiving the first response from the server. This value is used only for executions that
  // do not use server-side streaming.
  private var serverSideSessionId: Option[String] = None

  protected def verifyResponse[RespT <: GeneratedMessageV3](fn: => RespT): RespT = {
    val response = fn
    val field = response.getDescriptorForType.findFieldByName("server_side_session_id")
    val value = response.getField(field)
    serverSideSessionId match {
      case Some(id) if value != id =>
        throw new IllegalStateException(s"Server side session ID changed from $id to $value")
      case _ =>
        synchronized {
          serverSideSessionId = Some(value.toString)
        }
    }
    response
  }

  /**
   * Wraps an existing iterator with another closeable iterator that verifies the response. This
   * is needed for server-side streaming calls that are converted to iterators.
   */
  def wrapIterator[T <: GeneratedMessageV3](inner: CloseableIterator[T]): CloseableIterator[T] = {
    new WrappedCloseableIterator[T] {

      override def innerIterator: Iterator[T] = inner

      override def hasNext: Boolean = {
        innerIterator.hasNext
      }

      override def next(): T = {
        verifyResponse {
          innerIterator.next()
        }
      }

      override def close(): Unit = {
        innerIterator match {
          case it: CloseableIterator[T] => it.close()
          case _ => // nothing
        }
      }
    }
  }

  /**
   * Wraps an existing stream observer with another stream observer that verifies the response.
   * This is necessary for client-side streaming calls.
   */
  def wrapStreamObserver[T <: GeneratedMessageV3](inner: StreamObserver[T]): StreamObserver[T] = {
    new StreamObserver[T] {
      private val innerObserver = inner
      override def onNext(value: T): Unit = {
        innerObserver.onNext(verifyResponse(value))
      }
      override def onError(t: Throwable): Unit = {
        innerObserver.onError(t)
      }
      override def onCompleted(): Unit = {
        innerObserver.onCompleted()
      }
    }
  }
}

private[connect] class CustomSparkConnectBlockingStub(
    channel: ManagedChannel,
    retryPolicy: GrpcRetryHandler.RetryPolicy)
    extends SparkConnectCommonStub {

  private val stub = SparkConnectServiceGrpc.newBlockingStub(channel)

  private val retryHandler = new GrpcRetryHandler(retryPolicy)

  // GrpcExceptionConverter with a GRPC stub for fetching error details from server.
  private val grpcExceptionConverter = new GrpcExceptionConverter(stub)

  def executePlan(request: ExecutePlanRequest): CloseableIterator[ExecutePlanResponse] = {
    grpcExceptionConverter.convert(
      request.getSessionId,
      request.getUserContext,
      request.getClientType) {
      grpcExceptionConverter.convertIterator[ExecutePlanResponse](
        request.getSessionId,
        request.getUserContext,
        request.getClientType,
        retryHandler.RetryIterator[ExecutePlanRequest, ExecutePlanResponse](
          request,
          r => {
            wrapIterator(CloseableIterator(stub.executePlan(r).asScala))
          }))
    }
  }

  def executePlanReattachable(
      request: ExecutePlanRequest): CloseableIterator[ExecutePlanResponse] = {
    grpcExceptionConverter.convert(
      request.getSessionId,
      request.getUserContext,
      request.getClientType) {
      grpcExceptionConverter.convertIterator[ExecutePlanResponse](
        request.getSessionId,
        request.getUserContext,
        request.getClientType,
        // Don't use retryHandler - own retry handling is inside.
        wrapIterator(new ExecutePlanResponseReattachableIterator(request, channel, retryPolicy)))
    }
  }

  def analyzePlan(request: AnalyzePlanRequest): AnalyzePlanResponse = {
    grpcExceptionConverter.convert(
      request.getSessionId,
      request.getUserContext,
      request.getClientType) {
      retryHandler.retry {
        verifyResponse {
          stub.analyzePlan(request)
        }
      }
    }
  }

  def config(request: ConfigRequest): ConfigResponse = {
    grpcExceptionConverter.convert(
      request.getSessionId,
      request.getUserContext,
      request.getClientType) {
      retryHandler.retry {
        verifyResponse {
          stub.config(request)
        }
      }
    }
  }

  def interrupt(request: InterruptRequest): InterruptResponse = {
    grpcExceptionConverter.convert(
      request.getSessionId,
      request.getUserContext,
      request.getClientType) {
      retryHandler.retry {
        verifyResponse {
          stub.interrupt(request)
        }
      }
    }
  }

  def releaseSession(request: ReleaseSessionRequest): ReleaseSessionResponse = {
    grpcExceptionConverter.convert(
      request.getSessionId,
      request.getUserContext,
      request.getClientType) {
      retryHandler.retry {
        verifyResponse {
          stub.releaseSession(request)
        }
      }
    }
  }

  def artifactStatus(request: ArtifactStatusesRequest): ArtifactStatusesResponse = {
    grpcExceptionConverter.convert(
      request.getSessionId,
      request.getUserContext,
      request.getClientType) {
      retryHandler.retry {
        verifyResponse {
          stub.artifactStatus(request)
        }
      }
    }
  }
}
