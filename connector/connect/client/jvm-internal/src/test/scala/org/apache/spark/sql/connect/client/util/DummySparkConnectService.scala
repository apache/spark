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
package org.apache.spark.sql.connect.client.util

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse, ArtifactStatusesRequest, ArtifactStatusesResponse, ExecutePlanRequest, ExecutePlanResponse, SparkConnectServiceGrpc}

class DummySparkConnectService() extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {

  private var inputPlan: proto.Plan = _
  private val inputArtifactRequests: mutable.ListBuffer[AddArtifactsRequest] =
    mutable.ListBuffer.empty

  private[sql] def getAndClearLatestInputPlan(): proto.Plan = {
    val plan = inputPlan
    inputPlan = null
    plan
  }

  private[sql] def getAndClearLatestAddArtifactRequests(): Seq[AddArtifactsRequest] = {
    val requests = inputArtifactRequests.toSeq
    inputArtifactRequests.clear()
    requests
  }

  override def executePlan(
      request: ExecutePlanRequest,
      responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
    // Reply with a dummy response using the same client ID
    val requestSessionId = request.getSessionId
    val operationId = if (request.hasOperationId) {
      request.getOperationId
    } else {
      UUID.randomUUID().toString
    }
    inputPlan = request.getPlan
    val response = ExecutePlanResponse
      .newBuilder()
      .setSessionId(requestSessionId)
      .setOperationId(operationId)
      .build()
    responseObserver.onNext(response)
    // Reattachable execute must end with ResultComplete
    if (request.getRequestOptionsList.asScala.exists { option =>
        option.hasReattachOptions && option.getReattachOptions.getReattachable == true
      }) {
      val resultComplete = ExecutePlanResponse
        .newBuilder()
        .setSessionId(requestSessionId)
        .setOperationId(operationId)
        .setResultComplete(proto.ExecutePlanResponse.ResultComplete.newBuilder().build())
        .build()
      responseObserver.onNext(resultComplete)
    }
    responseObserver.onCompleted()
  }

  override def analyzePlan(
      request: AnalyzePlanRequest,
      responseObserver: StreamObserver[AnalyzePlanResponse]): Unit = {
    // Reply with a dummy response using the same client ID
    val requestSessionId = request.getSessionId
    request.getAnalyzeCase match {
      case proto.AnalyzePlanRequest.AnalyzeCase.SCHEMA =>
        inputPlan = request.getSchema.getPlan
      case proto.AnalyzePlanRequest.AnalyzeCase.EXPLAIN =>
        inputPlan = request.getExplain.getPlan
      case proto.AnalyzePlanRequest.AnalyzeCase.TREE_STRING =>
        inputPlan = request.getTreeString.getPlan
      case proto.AnalyzePlanRequest.AnalyzeCase.IS_LOCAL =>
        inputPlan = request.getIsLocal.getPlan
      case proto.AnalyzePlanRequest.AnalyzeCase.IS_STREAMING =>
        inputPlan = request.getIsStreaming.getPlan
      case proto.AnalyzePlanRequest.AnalyzeCase.INPUT_FILES =>
        inputPlan = request.getInputFiles.getPlan
      case _ => inputPlan = null
    }
    val response = AnalyzePlanResponse
      .newBuilder()
      .setSessionId(requestSessionId)
      .build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }

  override def addArtifacts(responseObserver: StreamObserver[AddArtifactsResponse])
      : StreamObserver[AddArtifactsRequest] = new StreamObserver[AddArtifactsRequest] {
    override def onNext(v: AddArtifactsRequest): Unit = inputArtifactRequests.append(v)

    override def onError(throwable: Throwable): Unit = responseObserver.onError(throwable)

    override def onCompleted(): Unit = {
      responseObserver.onNext(proto.AddArtifactsResponse.newBuilder().build())
      responseObserver.onCompleted()
    }
  }

  override def artifactStatus(
      request: ArtifactStatusesRequest,
      responseObserver: StreamObserver[ArtifactStatusesResponse]): Unit = {
    val builder = proto.ArtifactStatusesResponse.newBuilder()
    request.getNamesList().iterator().asScala.foreach { name =>
      val status = proto.ArtifactStatusesResponse.ArtifactStatus.newBuilder()
      val exists = if (name.startsWith("cache/")) {
        inputArtifactRequests.exists { artifactReq =>
          if (artifactReq.hasBatch) {
            val batch = artifactReq.getBatch
            batch.getArtifactsList.asScala.exists { singleArtifact =>
              singleArtifact.getName == name
            }
          } else false
        }
      } else false
      builder.putStatuses(name, status.setExists(exists).build())
    }
    responseObserver.onNext(builder.build())
    responseObserver.onCompleted()
  }

  override def interrupt(
      request: proto.InterruptRequest,
      responseObserver: StreamObserver[proto.InterruptResponse]): Unit = {
    val response = proto.InterruptResponse.newBuilder().setSessionId(request.getSessionId).build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }

  override def reattachExecute(
      request: proto.ReattachExecuteRequest,
      responseObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    // Reply with a dummy response using the same client ID
    val requestSessionId = request.getSessionId
    val response = ExecutePlanResponse
      .newBuilder()
      .setSessionId(requestSessionId)
      .build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }

  override def releaseExecute(
      request: proto.ReleaseExecuteRequest,
      responseObserver: StreamObserver[proto.ReleaseExecuteResponse]): Unit = {
    val response = proto.ReleaseExecuteResponse
      .newBuilder()
      .setSessionId(request.getSessionId)
      .setOperationId(request.getOperationId)
      .build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}

object DummySparkConnectService {
  def withNettyDummySparkConnectService(port: Int)(
      f: (DummySparkConnectService, Server) => Unit) {
    val service = new DummySparkConnectService
    val server = NettyServerBuilder
      .forPort(port)
      .addService(service)
      .build()
    try {
      server.start()
      f(service, server)
    } finally {
      server.shutdownNow()
      assert(server.awaitTermination(5, TimeUnit.SECONDS), "server failed to shutdown")
    }
  }
}
