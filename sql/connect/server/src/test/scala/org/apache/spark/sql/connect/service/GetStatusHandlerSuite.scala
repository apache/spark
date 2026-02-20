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

import java.util
import java.util.{Optional, UUID}

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import com.google.protobuf
import com.google.protobuf.StringValue
import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.GetStatusResponse
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.plugin.{GetStatusPlugin, SparkConnectPluginRegistry}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

/**
 * A test-only base class for GetStatusPlugins that echo extensions back with configurable
 * prefixes. For each input extension containing a StringValue, it produces a response extension
 * with the value prefixed by "{requestPrefix}" or "{opPrefix}{operationId}:".
 */
abstract class EchoGetStatusPluginBase(requestPrefix: String, opPrefix: String)
    extends GetStatusPlugin {

  override def processRequestExtensions(
      sessionHolder: SessionHolder,
      requestExtensions: util.List[protobuf.Any]): Optional[util.List[protobuf.Any]] =
    echoWithPrefix(requestExtensions, requestPrefix)

  override def processOperationExtensions(
      operationId: String,
      sessionHolder: SessionHolder,
      operationExtensions: util.List[protobuf.Any]): Optional[util.List[protobuf.Any]] =
    echoWithPrefix(operationExtensions, s"$opPrefix$operationId:")

  private def echoWithPrefix(
      extensions: util.List[protobuf.Any],
      prefix: String): Optional[util.List[protobuf.Any]] = {
    if (extensions.isEmpty) return Optional.empty()
    val result = new util.ArrayList[protobuf.Any]()
    extensions.forEach { ext =>
      if (ext.is(classOf[StringValue])) {
        val value = ext.unpack(classOf[StringValue]).getValue
        result.add(protobuf.Any.pack(StringValue.of(s"$prefix$value")))
      }
    }
    Optional.of(result)
  }
}

class EchoGetStatusPlugin
    extends EchoGetStatusPluginBase("request-echo:", "op-echo:")

class SecondEchoGetStatusPlugin
    extends EchoGetStatusPluginBase("second-request:", "second-op:")

/**
 * A no-op plugin that always returns Optional.empty() for both request and operation extensions.
 */
class NoOpGetStatusPlugin extends GetStatusPlugin {
  override def processRequestExtensions(
      sessionHolder: SessionHolder,
      requestExtensions: util.List[protobuf.Any]): Optional[util.List[protobuf.Any]] =
    Optional.empty()

  override def processOperationExtensions(
      operationId: String,
      sessionHolder: SessionHolder,
      operationExtensions: util.List[protobuf.Any]): Optional[util.List[protobuf.Any]] =
    Optional.empty()
}

/**
 * A plugin that always throws a RuntimeException.
 */
class FailingGetStatusPlugin extends GetStatusPlugin {
  override def processRequestExtensions(
      sessionHolder: SessionHolder,
      requestExtensions: util.List[protobuf.Any]): Optional[util.List[protobuf.Any]] =
    throw new RuntimeException("request plugin failure")

  override def processOperationExtensions(
      operationId: String,
      sessionHolder: SessionHolder,
      operationExtensions: util.List[protobuf.Any]): Optional[util.List[protobuf.Any]] =
    throw new RuntimeException("operation plugin failure")
}

class GetStatusHandlerSuite extends SharedSparkSession {

  // Default userId matching SparkConnectTestUtils.createDummySessionHolder default
  private val defaultUserId = "testUser"

  protected override def afterEach(): Unit = {
    super.afterEach()
    SparkConnectService.sessionManager.invalidateAllSessions()
    SparkConnectPluginRegistry.reset()
  }

  private def sendGetStatusRequest(
      sessionId: String,
      userId: String = defaultUserId,
      serverSideSessionId: Option[String] = None,
      requestExtensions: Seq[protobuf.Any] = Seq.empty,
      customize: proto.GetStatusRequest.Builder => Unit = _ => ()): GetStatusResponse = {
    val userContext = proto.UserContext.newBuilder().setUserId(userId).build()
    val requestBuilder = proto.GetStatusRequest
      .newBuilder()
      .setUserContext(userContext)
      .setSessionId(sessionId)

    requestExtensions.foreach(requestBuilder.addExtensions)
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
      serverSideSessionId: Option[String] = None,
      requestExtensions: Seq[protobuf.Any] = Seq.empty,
      operationExtensions: Seq[protobuf.Any] = Seq.empty): GetStatusResponse = {
    sendGetStatusRequest(sessionId, userId, serverSideSessionId,
      requestExtensions, { builder =>
        val operationStatusRequest = proto.GetStatusRequest.OperationStatusRequest.newBuilder()
        operationIds.foreach(operationStatusRequest.addOperationIds)
        operationExtensions.foreach(operationStatusRequest.addExtensions)
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

  test("GetStatus propagates both request and operation extensions via plugin") {
    SparkConnectPluginRegistry.setGetStatusPluginsForTesting(Seq(new EchoGetStatusPlugin()))
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder1 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val executeHolder2 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    val reqExt = protobuf.Any.pack(StringValue.of("req-data"))
    val opExt = protobuf.Any.pack(StringValue.of("op-data"))
    val response = sendGetOperationStatusRequest(
      sessionHolder.sessionId,
      operationIds = Seq(executeHolder1.operationId, executeHolder2.operationId),
      userId = sessionHolder.userId,
      requestExtensions = Seq(reqExt),
      operationExtensions = Seq(opExt))

    // Verify request-level extensions
    val responseExtensions = response.getExtensionsList.asScala
    assert(responseExtensions.size == 1)
    assert(
      responseExtensions.head.unpack(classOf[StringValue]).getValue == "request-echo:req-data")

    // Verify operation-level extensions for both operations
    val statuses = response.getOperationStatusesList.asScala
    assert(statuses.size == 2)

    val statusByOpId = statuses.map(s => s.getOperationId -> s).toMap
    Seq(executeHolder1, executeHolder2).foreach { holder =>
      val opStatus = statusByOpId(holder.operationId)
      assert(opStatus.getState ==
        proto.GetStatusResponse.OperationStatus.OperationState.OPERATION_STATE_RUNNING)

      val opExtensions = opStatus.getExtensionsList.asScala
      assert(opExtensions.size == 1)
      assert(opExtensions.head.unpack(classOf[StringValue]).getValue ==
        s"op-echo:${holder.operationId}:op-data")
    }
  }

  test("GetStatus with no plugin returns no extensions") {
    SparkConnectPluginRegistry.setGetStatusPluginsForTesting(Seq.empty)
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val operationId = UUID.randomUUID().toString

    val reqExt = protobuf.Any.pack(StringValue.of("ignored"))
    val opExt = protobuf.Any.pack(StringValue.of("also-ignored"))
    val response = sendGetOperationStatusRequest(
      sessionHolder.sessionId,
      operationIds = Seq(operationId),
      userId = sessionHolder.userId,
      requestExtensions = Seq(reqExt),
      operationExtensions = Seq(opExt))

    assert(response.getExtensionsList.isEmpty)
    val statuses = response.getOperationStatusesList.asScala
    assert(statuses.size == 1)
    assert(statuses.head.getExtensionsList.isEmpty)
  }

  test("GetStatus aggregates extensions from multiple plugins, skipping empty ones") {
    SparkConnectPluginRegistry.setGetStatusPluginsForTesting(
      Seq(new EchoGetStatusPlugin(), new NoOpGetStatusPlugin(), new SecondEchoGetStatusPlugin()))
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    val reqExt = protobuf.Any.pack(StringValue.of("hello"))
    val opExt = protobuf.Any.pack(StringValue.of("world"))
    val response = sendGetOperationStatusRequest(
      sessionHolder.sessionId,
      operationIds = Seq(executeHolder.operationId),
      userId = sessionHolder.userId,
      requestExtensions = Seq(reqExt),
      operationExtensions = Seq(opExt))

    val responseExtValues = response.getExtensionsList.asScala
      .map(_.unpack(classOf[StringValue]).getValue)
    assert(responseExtValues.size == 2)
    assert(responseExtValues.contains("request-echo:hello"))
    assert(responseExtValues.contains("second-request:hello"))

    val statuses = response.getOperationStatusesList.asScala
    assert(statuses.size == 1)
    val opExtValues = statuses.head.getExtensionsList.asScala
      .map(_.unpack(classOf[StringValue]).getValue)
    assert(opExtValues.size == 2)
    assert(opExtValues.contains(s"op-echo:${executeHolder.operationId}:world"))
    assert(opExtValues.contains(s"second-op:${executeHolder.operationId}:world"))
  }

  test("GetStatus chain isolates plugin failures and collects from healthy plugins") {
    SparkConnectPluginRegistry.setGetStatusPluginsForTesting(
      Seq(new EchoGetStatusPlugin(), new FailingGetStatusPlugin(),
        new SecondEchoGetStatusPlugin()))
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    val reqExt = protobuf.Any.pack(StringValue.of("safe"))
    val opExt = protobuf.Any.pack(StringValue.of("safe"))
    val response = sendGetOperationStatusRequest(
      sessionHolder.sessionId,
      operationIds = Seq(executeHolder.operationId),
      userId = sessionHolder.userId,
      requestExtensions = Seq(reqExt),
      operationExtensions = Seq(opExt))

    val responseExtValues = response.getExtensionsList.asScala
      .map(_.unpack(classOf[StringValue]).getValue)
    assert(responseExtValues.size == 2)
    assert(responseExtValues.contains("request-echo:safe"))
    assert(responseExtValues.contains("second-request:safe"))

    val opExtValues = response.getOperationStatusesList.asScala
      .head.getExtensionsList.asScala
      .map(_.unpack(classOf[StringValue]).getValue)
    assert(opExtValues.size == 2)
    assert(opExtValues.contains(s"op-echo:${executeHolder.operationId}:safe"))
    assert(opExtValues.contains(s"second-op:${executeHolder.operationId}:safe"))
  }
}

private class GetStatusResponseObserver extends StreamObserver[proto.GetStatusResponse] {
  val promise: Promise[GetStatusResponse] = Promise()
  override def onNext(value: proto.GetStatusResponse): Unit = promise.success(value)
  override def onError(t: Throwable): Unit = promise.failure(t)
  override def onCompleted(): Unit = {}
}
