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

import scala.collection.JavaConverters._

import io.grpc.ManagedChannel

import org.apache.spark.connect.proto._

private[client] class CustomSparkConnectBlockingStub(
    private[client] val channel: ManagedChannel,
    retryPolicy: GrpcRetryHandler.RetryPolicy,
    enableHeartbeat: Boolean = true) {

  private val stub = SparkConnectServiceGrpc.newBlockingStub(channel)
  private val retryHandler = new GrpcRetryHandler(retryPolicy)
  private[client] val heartbeat: Heartbeat = {
    if (enableHeartbeat) {
      new HeartbeatImpl(this)
    } else {
      new Heartbeat
    }
  }

  def executePlan(request: ExecutePlanRequest): CloseableIterator[ExecutePlanResponse] = {
    heartbeat.beatIterator {
      GrpcExceptionConverter.convert {
        GrpcExceptionConverter.convertIterator[ExecutePlanResponse](
          retryHandler.RetryIterator[ExecutePlanRequest, ExecutePlanResponse](
            request,
            r => CloseableIterator(stub.executePlan(r).asScala)))
      }
    }
  }

  def executePlanReattachable(
      request: ExecutePlanRequest): CloseableIterator[ExecutePlanResponse] = {
    heartbeat.beatIterator {
      GrpcExceptionConverter.convert {
        GrpcExceptionConverter.convertIterator[ExecutePlanResponse](
          // Don't use retryHandler - own retry handling is inside.
          new ExecutePlanResponseReattachableIterator(request, channel, retryPolicy))
      }
    }
  }

  def analyzePlan(request: AnalyzePlanRequest): AnalyzePlanResponse = {
    heartbeat.beat {
      GrpcExceptionConverter.convert {
        retryHandler.retry {
          stub.analyzePlan(request)
        }
      }
    }
  }

  def config(request: ConfigRequest): ConfigResponse = {
    heartbeat.beat {
      GrpcExceptionConverter.convert {
        retryHandler.retry {
          stub.config(request)
        }
      }
    }
  }

  def interrupt(request: InterruptRequest): InterruptResponse = {
    heartbeat.beat {
      GrpcExceptionConverter.convert {
        retryHandler.retry {
          stub.interrupt(request)
        }
      }
    }
  }

  def artifactStatus(request: ArtifactStatusesRequest): ArtifactStatusesResponse = {
    heartbeat.beat {
      GrpcExceptionConverter.convert {
        retryHandler.retry {
          stub.artifactStatus(request)
        }
      }
    }
  }

  private[client] def executePing(): Unit = {
    val request = AnalyzePlanRequest.newBuilder().setSparkVersion(
      AnalyzePlanRequest.SparkVersion.newBuilder().build()).build()

    retryHandler.retry {
      stub.analyzePlan(request)
    }
  }
}
