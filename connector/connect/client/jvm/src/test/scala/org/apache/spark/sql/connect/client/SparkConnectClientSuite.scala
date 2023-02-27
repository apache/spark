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

import io.grpc.{Server, StatusRuntimeException}
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{AnalyzePlanRequest, AnalyzePlanResponse, ExecutePlanRequest, ExecutePlanResponse, SparkConnectServiceGrpc}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.client.util.ConnectFunSuite
import org.apache.spark.sql.connect.common.config.ConnectCommon

class SparkConnectClientSuite extends ConnectFunSuite with BeforeAndAfterEach {

  private var client: SparkConnectClient = _
  private var service: DummySparkConnectService = _
  private var server: Server = _

  private def startDummyServer(port: Int): Unit = {
    service = new DummySparkConnectService
    server = NettyServerBuilder
      .forPort(port)
      .addService(service)
      .build()
    server.start()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    client = null
    server = null
    service = null
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

  // Use 0 to start the server at a random port
  private def testClientConnection(serverPort: Int = 0)(
      clientBuilder: Int => SparkConnectClient): Unit = {
    startDummyServer(serverPort)
    client = clientBuilder(server.getPort)
    val request = AnalyzePlanRequest
      .newBuilder()
      .setClientId("abc123")
      .build()

    val response = client.analyze(request)
    assert(response.getClientId === "abc123")
  }

  test("Test connection") {
    testClientConnection() { testPort => SparkConnectClient.builder().port(testPort).build() }
  }

  test("Test connection string") {
    testClientConnection() { testPort =>
      SparkConnectClient.builder().connectionString(s"sc://localhost:$testPort").build()
    }
  }

  test("Test encryption") {
    startDummyServer(0)
    client = SparkConnectClient
      .builder()
      .connectionString(s"sc://localhost:${server.getPort}/;use_ssl=true")
      .build()

    val request = AnalyzePlanRequest.newBuilder().setClientId("abc123").build()

    // Failed the ssl handshake as the dummy server does not have any server credentials installed.
    assertThrows[StatusRuntimeException] {
      client.analyze(request)
    }
  }

  test("SparkSession initialisation with connection string") {
    val testPort = 16002
    client = SparkConnectClient.builder().connectionString(s"sc://localhost:$testPort").build()
    startDummyServer(testPort)
    val session = SparkSession.builder().client(client).build()
    val df = session.range(10)
    df.analyze // Trigger RPC
    assert(df.plan === service.getAndClearLatestInputPlan())
  }

  private case class TestPackURI(
      connectionString: String,
      isCorrect: Boolean,
      extraChecks: SparkConnectClient => Unit = _ => {})

  private val URIs = Seq[TestPackURI](
    TestPackURI("sc://host", isCorrect = true),
    TestPackURI(
      "sc://localhost/",
      isCorrect = true,
      client => testClientConnection(ConnectCommon.CONNECT_GRPC_BINDING_PORT)(_ => client)),
    TestPackURI(
      "sc://localhost:1234/",
      isCorrect = true,
      client => testClientConnection(1234)(_ => client)),
    TestPackURI(
      "sc://localhost/;",
      isCorrect = true,
      client => testClientConnection(ConnectCommon.CONNECT_GRPC_BINDING_PORT)(_ => client)),
    TestPackURI("sc://host:123", isCorrect = true),
    TestPackURI(
      "sc://host:123/;user_id=a94",
      isCorrect = true,
      client => assert(client.userId == "a94")),
    TestPackURI(
      "sc://host:123/;user_agent=a945",
      isCorrect = true,
      client => assert(client.userAgent == "a945")),
    TestPackURI("scc://host:12", isCorrect = false),
    TestPackURI("http://host", isCorrect = false),
    TestPackURI("sc:/host:1234/path", isCorrect = false),
    TestPackURI("sc://host/path", isCorrect = false),
    TestPackURI("sc://host/;parm1;param2", isCorrect = false),
    TestPackURI("sc://host:123;user_id=a94", isCorrect = false),
    TestPackURI("sc:///user_id=123", isCorrect = false),
    TestPackURI("sc://host:-4", isCorrect = false),
    TestPackURI("sc://:123/", isCorrect = false),
    TestPackURI("sc://host:123/;use_ssl=true", isCorrect = true),
    TestPackURI("sc://host:123/;token=mySecretToken", isCorrect = true),
    TestPackURI("sc://host:123/;token=", isCorrect = false),
    TestPackURI("sc://host:123/;use_ssl=true;token=mySecretToken", isCorrect = true),
    TestPackURI("sc://host:123/;token=mySecretToken;use_ssl=true", isCorrect = true),
    TestPackURI("sc://host:123/;use_ssl=false;token=mySecretToken", isCorrect = false),
    TestPackURI("sc://host:123/;token=mySecretToken;use_ssl=false", isCorrect = false),
    TestPackURI("sc://host:123/;param1=value1;param2=value2", isCorrect = true))

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
}

class DummySparkConnectService() extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {

  private var inputPlan: proto.Plan = _

  private[sql] def getAndClearLatestInputPlan(): proto.Plan = {
    val plan = inputPlan
    inputPlan = null
    plan
  }

  override def executePlan(
      request: ExecutePlanRequest,
      responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
    // Reply with a dummy response using the same client ID
    val requestClientId = request.getClientId
    inputPlan = request.getPlan
    val response = ExecutePlanResponse
      .newBuilder()
      .setClientId(requestClientId)
      .build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }

  override def analyzePlan(
      request: AnalyzePlanRequest,
      responseObserver: StreamObserver[AnalyzePlanResponse]): Unit = {
    // Reply with a dummy response using the same client ID
    val requestClientId = request.getClientId
    inputPlan = request.getPlan
    val response = AnalyzePlanResponse
      .newBuilder()
      .setClientId(requestClientId)
      .build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}
