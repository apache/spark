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

import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.connect.proto.{AnalyzePlanRequest, AnalyzePlanResponse, SparkConnectServiceGrpc}
import org.apache.spark.sql.connect.common.config.ConnectCommon

class SparkConnectClientSuite
    extends AnyFunSuite // scalastyle:ignore funsuite
    with BeforeAndAfterEach {

  private var client: SparkConnectClient = _
  private var server: Server = _

  private def startDummyServer(port: Int): Unit = {
    val sb = NettyServerBuilder
      .forPort(port)
      .addService(new DummySparkConnectService())

    server = sb.build
    server.start()
  }
  override def beforeEach(): Unit = {
    super.beforeEach()
    client = null
    server = null
  }

  override def afterEach(): Unit = {
    if (server != null) {
      server.shutdownNow()
      assert(server.awaitTermination(5, TimeUnit.SECONDS), "server failed to shutdown")
    }

    if (client != null) {
      client.shutdown()
    }
  }

  test("Placeholder test: Create SparkConnectClient") {
    client = SparkConnectClient.builder().userId("abc123").build()
    assert(client.userId == "abc123")
  }

  private def testClientConnection(
      client: SparkConnectClient,
      serverPort: Int = ConnectCommon.CONNECT_GRPC_BINDING_PORT): Unit = {
    startDummyServer(serverPort)
    val request = AnalyzePlanRequest
      .newBuilder()
      .setClientId("abc123")
      .build()

    val response = client.analyze(request)
    assert(response.getClientId === "abc123")
  }

  test("Test connection") {
    val testPort = 16000
    client = SparkConnectClient.builder().port(testPort).build()
    testClientConnection(client, testPort)
  }

  test("Test connection string") {
    val testPort = 16000
    client = SparkConnectClient.builder().connectionString("sc://localhost:16000").build()
    testClientConnection(client, testPort)
  }

  private case class TestPackURI(
      connectionString: String,
      isCorrect: Boolean,
      extraChecks: SparkConnectClient => Unit = _ => {})

  private val URIs = Seq[TestPackURI](
    TestPackURI("sc://host", isCorrect = true),
    TestPackURI("sc://localhost/", isCorrect = true, client => testClientConnection(client)),
    TestPackURI(
      "sc://localhost:123/",
      isCorrect = true,
      client => testClientConnection(client, 123)),
    TestPackURI("sc://localhost/;", isCorrect = true, client => testClientConnection(client)),
    TestPackURI("sc://host:123", isCorrect = true),
    TestPackURI(
      "sc://host:123/;user_id=a94",
      isCorrect = true,
      client => assert(client.userId == "a94")),
    TestPackURI("scc://host:12", isCorrect = false),
    TestPackURI("http://host", isCorrect = false),
    TestPackURI("sc:/host:1234/path", isCorrect = false),
    TestPackURI("sc://host/path", isCorrect = false),
    TestPackURI("sc://host/;parm1;param2", isCorrect = false),
    TestPackURI("sc://host:123;user_id=a94", isCorrect = false),
    TestPackURI("sc:///user_id=123", isCorrect = false),
    TestPackURI("sc://host:-4", isCorrect = false),
    TestPackURI("sc://:123/", isCorrect = false))

  private def checkTestPack(testPack: TestPackURI): Unit = {
    val client = SparkConnectClient.builder().connectionString(testPack.connectionString).build()
    testPack.extraChecks(client)
  }

  URIs.foreach { testPack =>
    test(s"Check URI: ${testPack.connectionString}, isCorrect: ${testPack.isCorrect}") {
      if (!testPack.isCorrect) {
        assertThrows[IllegalArgumentException](checkTestPack(testPack))
      } else {
        checkTestPack(testPack)
      }
    }
  }

  // TODO(SPARK-41917): Remove test once SSL and Auth tokens are supported.
  test("Non user-id parameters throw unsupported errors") {
    assertThrows[UnsupportedOperationException] {
      SparkConnectClient.builder().connectionString("sc://host/;use_ssl=true").build()
    }

    assertThrows[UnsupportedOperationException] {
      SparkConnectClient.builder().connectionString("sc://host/;token=abc").build()
    }

    assertThrows[UnsupportedOperationException] {
      SparkConnectClient.builder().connectionString("sc://host/;xyz=abc").build()

    }
  }
}

class DummySparkConnectService() extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {

  override def analyzePlan(
      request: AnalyzePlanRequest,
      responseObserver: StreamObserver[AnalyzePlanResponse]): Unit = {
    // Reply with a dummy response using the same client ID
    val requestClientId = request.getClientId
    val response = AnalyzePlanResponse
      .newBuilder()
      .setClientId(requestClientId)
      .build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}
