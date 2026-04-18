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

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import io.grpc.{Deadline, ManagedChannel}

import org.apache.spark.connect.proto._
import org.apache.spark.sql.util.CloseableIterator

private[connect] class CustomSparkConnectBlockingStub(
    channel: ManagedChannel,
    stubState: SparkConnectStubState) {

  private val stub = SparkConnectServiceGrpc.newBlockingStub(channel)

  private def withDeadline(
      d: Option[FiniteDuration]): SparkConnectServiceGrpc.SparkConnectServiceBlockingStub =
    d.map(dur => stub.withDeadline(Deadline.after(dur.toMillis, TimeUnit.MILLISECONDS)))
      .getOrElse(stub)

  private val retryHandler = stubState.retryHandler

  // GrpcExceptionConverter with a GRPC stub for fetching error details from server.
  private val grpcExceptionConverter = stubState.exceptionConverter

  // Non-reattachable executePlan intentionally has no deadline: a timeout here would kill the
  // server-side execution with no way to recover (there is no ReattachExecute for this path).
  // Use reattachable execution for long-running queries that need deadline protection.
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
            stubState.responseValidator.wrapIterator(
              CloseableIterator(stub.executePlan(r).asScala))
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
        stubState.responseValidator.wrapIterator(
          // Reattachable iterator retries internally; omit RetryIterator wrapper here.
          new ExecutePlanResponseReattachableIterator(
            request,
            channel,
            stubState.retryHandler,
            stubState.rpcDeadlines.reattachableExecutePlan,
            stubState.rpcDeadlines.reattachExecute)))
    }
  }

  def analyzePlan(request: AnalyzePlanRequest): AnalyzePlanResponse = {
    grpcExceptionConverter.convert(
      request.getSessionId,
      request.getUserContext,
      request.getClientType) {
      retryHandler.retry {
        stubState.responseValidator.verifyResponse {
          withDeadline(stubState.rpcDeadlines.analyzePlan).analyzePlan(request)
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
        stubState.responseValidator.verifyResponse {
          withDeadline(stubState.rpcDeadlines.config).config(request)
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
        stubState.responseValidator.verifyResponse {
          withDeadline(stubState.rpcDeadlines.interrupt).interrupt(request)
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
        stubState.responseValidator.verifyResponse {
          withDeadline(stubState.rpcDeadlines.releaseSession).releaseSession(request)
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
        stubState.responseValidator.verifyResponse {
          withDeadline(stubState.rpcDeadlines.artifactStatus).artifactStatus(request)
        }
      }
    }
  }

  def cloneSession(request: CloneSessionRequest): CloneSessionResponse = {
    grpcExceptionConverter.convert(
      request.getSessionId,
      request.getUserContext,
      request.getClientType) {
      retryHandler.retry {
        stubState.responseValidator.verifyResponse {
          withDeadline(stubState.rpcDeadlines.cloneSession).cloneSession(request)
        }
      }
    }
  }

  def getStatus(request: GetStatusRequest): GetStatusResponse = {
    grpcExceptionConverter.convert(
      request.getSessionId,
      request.getUserContext,
      request.getClientType) {
      retryHandler.retry {
        stubState.responseValidator.verifyResponse {
          withDeadline(stubState.rpcDeadlines.getStatus).getStatus(request)
        }
      }
    }
  }
}
