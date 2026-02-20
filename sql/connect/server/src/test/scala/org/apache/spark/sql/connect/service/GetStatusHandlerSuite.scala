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
package org.apache.spark.sql.connect.service

import java.util.UUID

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.GetStatusResponse
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

class GetStatusHandlerSuite extends SharedSparkSession {

  // Default userId matching SparkConnectTestUtils.createDummySessionHolder default
  private val defaultUserId = "testUser"

  protected override def afterEach(): Unit = {
    super.afterEach()
    SparkConnectService.sessionManager.invalidateAllSessions()
  }

  private def sendGetStatusRequest(
      sessionId: String,
      userId: String = defaultUserId,
      serverSideSessionId: Option[String] = None,
      customize: proto.GetStatusRequest.Builder => Unit = _ => ()): GetStatusResponse = {
    val userContext = proto.UserContext.newBuilder().setUserId(userId).build()
    val requestBuilder = proto.GetStatusRequest
      .newBuilder()
      .setUserContext(userContext)
      .setSessionId(sessionId)

    serverSideSessionId.foreach(requestBuilder.setClientObservedServerSideSessionId)
    customize(requestBuilder)

    val request = requestBuilder.build()
    val responseObserver = new GetStatusResponseObserver()
    val handler = new SparkConnectGetStatusHandler(responseObserver)
    handler.handle(request)

    ThreadUtils.awaitResult(responseObserver.promise.future, 10.seconds)
  }

  private def sendGetOperationStatusRequest(
      sessionId: String,
      operationIds: Seq[String] = Seq.empty,
      userId: String = defaultUserId,
      serverSideSessionId: Option[String] = None): GetStatusResponse = {
    sendGetStatusRequest(sessionId, userId, serverSideSessionId, { builder =>
      val operationStatusRequest = proto.GetStatusRequest.OperationStatusRequest.newBuilder()
      operationIds.foreach(operationStatusRequest.addOperationIds)
      builder.setOperationStatus(operationStatusRequest)
    })
  }

  test("GetStatus returns session info for new session") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val response = sendGetStatusRequest(
      sessionHolder.sessionId,
      userId = sessionHolder.userId
    )

    assert(response.getSessionId == sessionHolder.sessionId)
    assert(response.getServerSideSessionId.nonEmpty)
  }

  test("GetStatus without operation IDs returns all existing operations") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder1 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val executeHolder2 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val executeHolder3 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    val response = sendGetOperationStatusRequest(
      sessionHolder.sessionId,
      userId = sessionHolder.userId)

    val statuses = response.getOperationStatusesList.asScala
    assert(statuses.size == 3)
    val operationIds = statuses.map(_.getOperationId).toSet
    assert(operationIds.contains(executeHolder1.operationId))
    assert(operationIds.contains(executeHolder2.operationId))
    assert(operationIds.contains(executeHolder3.operationId))
  }

  test("GetStatus returns UNKNOWN status for non-existent operation ID") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val nonExistentOperationId = UUID.randomUUID().toString

    val response = sendGetOperationStatusRequest(
      sessionHolder.sessionId,
      Seq(nonExistentOperationId),
      userId = sessionHolder.userId)

    val statuses = response.getOperationStatusesList.asScala
    assert(statuses.size == 1)
    assert(statuses.head.getOperationId == nonExistentOperationId)
    assert(statuses.head.getState ==
      proto.GetStatusResponse.OperationStatus.OperationState.OPERATION_STATE_UNKNOWN)
  }

  test("GetStatus returns RUNNING status for active operation") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    // The execute holder is created with Started status by createDummyExecuteHolder
    val response = sendGetOperationStatusRequest(
      sessionHolder.sessionId,
      Seq(executeHolder.operationId),
      sessionHolder.userId)

    val statuses = response.getOperationStatusesList.asScala
    assert(statuses.size == 1)
    assert(statuses.head.getOperationId == executeHolder.operationId)
    assert(statuses.head.getState ==
      proto.GetStatusResponse.OperationStatus.OperationState.OPERATION_STATE_RUNNING)
  }
}

private class GetStatusResponseObserver extends StreamObserver[proto.GetStatusResponse] {
  val promise: Promise[GetStatusResponse] = Promise()
  override def onNext(value: proto.GetStatusResponse): Unit = promise.success(value)
  override def onError(t: Throwable): Unit = promise.failure(t)
  override def onCompleted(): Unit = {}
}
