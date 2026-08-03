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
package org.apache.spark.udf.worker.grpc

import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, Server}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.scalatest.BeforeAndAfterEach
// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker.{Cancel, DataResponse, Finish, Heartbeat, Init, PayloadChunk,
  ShutdownRequest, UdfWorkerGrpc, WorkerRequest}
import org.apache.spark.udf.worker.grpc.testing.{SuccessfulExecution,
  UdfWorkerProtocolClient, UdfWorkerTestImplementation}

/**
 * Implementation-neutral contract for servers that implement the UDF worker protocol.
 *
 * Concrete suites provide deterministic requests and expected responses through
 * [[implementation]]. Protocol sequencing, chunking, cancellation, and management RPC
 * behavior stay here so another server implementation can run the same contract unchanged.
 */
private[grpc] abstract class UdfWorkerProtocolSuite
    extends AnyFunSuite with BeforeAndAfterEach {
// scalastyle:on funsuite

  protected def implementation: UdfWorkerTestImplementation

  private var server: Server = _
  private var channel: ManagedChannel = _
  private var stub: UdfWorkerGrpc.UdfWorkerStub = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    val serverName = InProcessServerBuilder.generateName()
    server = InProcessServerBuilder.forName(serverName)
      .addService(implementation.newService())
      .build()
      .start()
    channel = InProcessChannelBuilder.forName(serverName).build()
    stub = UdfWorkerGrpc.newStub(channel)
  }

  override def afterEach(): Unit = {
    if (channel != null) {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
      channel = null
    }
    if (server != null) {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
      server = null
    }
    super.afterEach()
  }

  test("single request completes with the expected response") {
    runSuccessful(implementation.singleRequest)
  }

  test("multiple requests preserve the implementation's response ordering") {
    runSuccessful(implementation.multipleRequests)
  }

  test("requests and responses can progress on separate threads") {
    val scenario = implementation.multipleRequests
    val client = new UdfWorkerProtocolClient(stub)
    client.sendInit(scenario.init)

    val producer = new Thread(() => {
      scenario.requests.foreach(client.sendData)
      client.sendFinish()
    }, "udf-protocol-producer")
    producer.start()

    assert(client.awaitDone(10000L), "stream did not complete")
    producer.join(10000L)
    assert(!producer.isAlive, "producer did not finish")
    assertSuccessfulResult(client, scenario.expectedResponses)
  }

  test("a UDF payload can be delivered in one chunk") {
    val scenario = implementation.singleRequest
    val client = new UdfWorkerProtocolClient(stub)
    val (init, chunks) = chunked(scenario.init, 1)
    client.sendInit(init)
    chunks.foreach(client.sendPayloadChunk)
    scenario.requests.foreach(client.sendData)
    client.sendFinish()

    assert(client.awaitDone())
    assertSuccessfulResult(client, scenario.expectedResponses)
  }

  test("finish immediately after initialization is supported") {
    runSuccessful(implementation.noInput)
  }

  test("cancel before finish terminates the stream cleanly") {
    val scenario = implementation.singleRequest
    val client = new UdfWorkerProtocolClient(stub)
    client.sendInit(scenario.init)
    scenario.requests.headOption.foreach(client.sendData)
    client.sendCancel(Cancel.newBuilder().setReason("task interrupted").build())

    assert(client.awaitDone())
    assert(client.streamError.isEmpty, s"unexpected stream error: ${client.streamError}")
  }

  test("cancel after finish accepts either terminal response") {
    val scenario = implementation.singleRequest
    val client = new UdfWorkerProtocolClient(stub)
    client.sendInit(scenario.init)
    scenario.requests.foreach(client.sendData)
    client.sendFinish()
    client.sendCancel(Cancel.newBuilder().setReason("interrupted after finish").build())

    assert(client.awaitDone())
    assert(client.streamError.isEmpty, s"unexpected stream error: ${client.streamError}")
  }

  test("a user-code error is returned as an execution error") {
    val scenario = implementation.userError
    val client = new UdfWorkerProtocolClient(stub)
    client.sendInit(scenario.init)
    client.sendData(scenario.request)

    assert(client.awaitDone())
    assert(client.streamError.isEmpty, s"unexpected stream error: ${client.streamError}")
    assert(client.executionError.contains(scenario.expectedError),
      s"expected ${scenario.expectedError}, got ${client.executionError}")
  }

  test("a second Init is rejected with a protocol error") {
    val scenario = implementation.singleRequest
    val client = new UdfWorkerProtocolClient(stub)
    client.sendInit(scenario.init)
    assert(client.awaitInitResponse(), "first InitResponse not received")
    client.sendInit(scenario.init)

    assert(client.awaitDone())
    assert(client.streamError.isEmpty)
    assert(client.executionError.exists(_.hasProtocol),
      s"expected ProtocolError, got ${client.executionError}")
  }

  test("an inline initialization failure is returned in InitResponse") {
    val scenario = implementation.initError
    val client = new UdfWorkerProtocolClient(stub)
    client.sendInit(scenario.init)

    assert(client.awaitDone())
    assert(client.streamError.isEmpty, s"unexpected stream error: ${client.streamError}")
    assert(client.executionError.contains(scenario.expectedError),
      s"expected ${scenario.expectedError}, got ${client.executionError}")
  }

  test("a chunked initialization failure is returned in InitResponse") {
    val scenario = implementation.initError
    val client = new UdfWorkerProtocolClient(stub)
    val (init, chunks) = chunked(scenario.init, 3)
    client.sendInit(init)
    chunks.foreach(client.sendPayloadChunk)

    assert(client.awaitDone())
    assert(client.streamError.isEmpty, s"unexpected stream error: ${client.streamError}")
    assert(client.executionError.contains(scenario.expectedError),
      s"expected ${scenario.expectedError}, got ${client.executionError}")
  }

  test("an unsupported protocol version fails initialization") {
    val valid = implementation.singleRequest.init
    val invalid = Init.newBuilder(valid)
      .setProtocolVersion(implementation.supportedProtocolVersion + 999)
      .build()
    val client = new UdfWorkerProtocolClient(stub)
    client.sendInit(invalid)

    assert(client.awaitDone())
    assert(client.streamError.isEmpty)
    assert(client.executionError.exists(_.hasProtocol),
      s"expected ProtocolError, got ${client.executionError}")
  }

  test("cancel before Init is accepted") {
    val client = new UdfWorkerProtocolClient(stub)
    client.sendCancel(Cancel.newBuilder().setReason("aborting before init").build())

    assert(client.awaitDone())
    assert(client.streamError.isEmpty)
    assert(client.executionError.isEmpty)
  }

  test("cancel during chunked initialization is accepted") {
    val client = new UdfWorkerProtocolClient(stub)
    val (init, chunks) = chunked(implementation.singleRequest.init, 3)
    client.sendInit(init)
    client.sendPayloadChunk(chunks.head.toBuilder.setLast(false).build())
    client.sendCancel(Cancel.newBuilder().setReason("aborting during chunking").build())

    assert(client.awaitDone())
    assert(client.streamError.isEmpty)
    assert(client.executionError.isEmpty)
  }

  test("a UDF payload can be assembled from multiple chunks") {
    val scenario = implementation.singleRequest
    val client = new UdfWorkerProtocolClient(stub)
    val (init, chunks) = chunked(scenario.init, 3)
    client.sendInit(init)
    chunks.foreach(client.sendPayloadChunk)
    scenario.requests.foreach(client.sendData)
    client.sendFinish()

    assert(client.awaitDone())
    assertSuccessfulResult(client, scenario.expectedResponses)
  }

  test("PayloadChunk in the data phase is rejected with a protocol error") {
    val client = new UdfWorkerProtocolClient(stub)
    client.sendInit(implementation.singleRequest.init)
    assert(client.awaitInitResponse(), "InitResponse not received")
    client.sendPayloadChunk(PayloadChunk.newBuilder()
      .setData(ByteString.copyFromUtf8("unexpected"))
      .setLast(true)
      .build())

    assert(client.awaitDone())
    assert(client.streamError.isEmpty)
    assert(client.executionError.exists(_.hasProtocol),
      s"expected ProtocolError, got ${client.executionError}")
  }

  test("Manage acknowledges heartbeat") {
    val response = UdfWorkerGrpc.newBlockingStub(channel).manage(WorkerRequest.newBuilder()
      .setHeartbeat(Heartbeat.getDefaultInstance)
      .build())
    assert(response.hasHeartbeat, "expected HeartbeatResponse")
  }

  test("Manage acknowledges shutdown") {
    val response = UdfWorkerGrpc.newBlockingStub(channel).manage(WorkerRequest.newBuilder()
      .setShutdown(ShutdownRequest.newBuilder().setReason("test complete").build())
      .build())
    assert(response.hasShutdown, "expected ShutdownResponse")
  }

  private def runSuccessful(scenario: SuccessfulExecution): Unit = {
    val client = new UdfWorkerProtocolClient(stub)
    client.sendInit(scenario.init)
    scenario.requests.foreach(client.sendData)
    client.sendFinish(Finish.getDefaultInstance)

    assert(client.awaitDone(), "stream did not complete")
    assertSuccessfulResult(client, scenario.expectedResponses)
  }

  private def assertSuccessfulResult(
      client: UdfWorkerProtocolClient,
      expected: Seq[DataResponse]): Unit = {
    assert(client.streamError.isEmpty, s"unexpected stream error: ${client.streamError}")
    assert(client.executionError.isEmpty,
      s"unexpected execution error: ${client.executionError}")
    assert(client.drainResults() == expected)
  }

  private def chunked(init: Init, parts: Int): (Init, Seq[PayloadChunk]) = {
    require(parts > 0, "parts must be positive")
    val payload = init.getUdf.getPayload
    val chunkedInit = Init.newBuilder(init)
      .setIsChunkingPayload(true)
      .setUdf(init.getUdf.toBuilder.setPayload(ByteString.EMPTY).build())
      .build()
    val chunks = (0 until parts).map { index =>
      val start = payload.size() * index / parts
      val end = payload.size() * (index + 1) / parts
      PayloadChunk.newBuilder()
        .setData(payload.substring(start, end))
        .setLast(index == parts - 1)
        .build()
    }
    (chunkedInit, chunks)
  }
}
