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

import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, Server}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.StreamObserver
import org.scalatest.BeforeAndAfterEach
// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker.{Cancel, DataRequest, ExecutionError, Finish, Heartbeat,
  Init, PayloadChunk, ShutdownRequest, UdfControlRequest, UdfControlResponse, UdfPayload,
  UdfRequest, UdfResponse, UDFWorkerDataFormat, UdfWorkerGrpc, WorkerRequest}
import org.apache.spark.udf.worker.grpc.testing.EchoWorkerService

/**
 * Protocol validation test for the UDF gRPC execution protocol.
 *
 * Implements a minimal echo worker (gRPC server) and engine client to verify
 * the full Execute stream lifecycle: init, data streaming, finish, cancel,
 * error handling, and the Manage RPC. The worker echoes each DataRequest
 * batch back as a DataResponse; error paths are triggered by a sentinel
 * payload value.
 */
class EchoProtocolSuite extends AnyFunSuite with BeforeAndAfterEach {
// scalastyle:on funsuite

  // Constants reused across protocol tests. Defined on the shared
  // [[EchoWorkerService]] so this suite and the cross-process integration
  // tests speak the same triggers.
  import EchoWorkerService.{SupportedVersion, ErrorTrigger, InitErrorTrigger}

  private var server: Server = _
  private var channel: ManagedChannel = _
  private var stub: UdfWorkerGrpc.UdfWorkerStub = _

  override def beforeEach(): Unit = {
    val serverName = InProcessServerBuilder.generateName()
    server = InProcessServerBuilder.forName(serverName)
      .directExecutor()
      .addService(new EchoWorkerService)
      .build()
      .start()
    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
    stub = UdfWorkerGrpc.newStub(channel)
  }

  override def afterEach(): Unit = {
    channel.shutdownNow()
    server.shutdownNow()
  }

  // ===========================================================================
  // ENGINE SIDE (gRPC client)
  // ===========================================================================

  /**
   * Minimal engine client that drives the Execute stream and collects results.
   *
   * The request stream is half-closed (onCompleted) only after the session
   * outcome is known from the server: on receiving FinishResponse,
   * CancelResponse, or a gRPC error. This keeps the stream open long enough
   * for Cancel to follow Finish when needed.
   */
  private class EngineClient(stub: UdfWorkerGrpc.UdfWorkerStub) {
    private val results = new LinkedBlockingQueue[Array[Byte]]()
    private val done = new CountDownLatch(1)
    @volatile var executionError: Option[ExecutionError] = None
    @volatile var streamError: Option[Throwable] = None
    private val requestCompleted = new AtomicBoolean(false)
    // Counted down on InitResponse (success or failure) or on terminal error.
    // The engine MUST wait for this before sending any DataRequest or Finish.
    private val initResponseLatch = new CountDownLatch(1)

    private val responseObserver = new StreamObserver[UdfResponse] {
      override def onNext(response: UdfResponse): Unit = {
        response.getResponseCase match {
          case UdfResponse.ResponseCase.DATA =>
            results.add(response.getData.getData.toByteArray)

          case UdfResponse.ResponseCase.CONTROL =>
            val ctrl = response.getControl
            ctrl.getControlCase match {
              case UdfControlResponse.ControlCase.INIT =>
                // InitResponse received. If error is set, init failed.
                val resp = ctrl.getInit
                if (resp.hasError) {
                  executionError = Some(resp.getError)
                  if (!requestCompleted.get()) sendCancel("aborting after init error")
                }
                initResponseLatch.countDown()
                // Data phase begins only on success (no error).

              case UdfControlResponse.ControlCase.ERROR =>
                // Data-phase error. Send Cancel so the worker can abort cleanly;
                // the error is surfaced after CancelResponse arrives.
                executionError = Some(ctrl.getError.getError)
                if (!requestCompleted.get()) {
                  sendCancel("aborting after ErrorResponse")
                }

              case UdfControlResponse.ControlCase.FINISH =>
                completeRequestStream()
                done.countDown()

              case UdfControlResponse.ControlCase.CANCEL =>
                completeRequestStream()
                done.countDown()

              case unexpected =>
                throw new IllegalStateException(
                  s"unexpected control response: $unexpected")
            }

          case unexpected =>
            throw new IllegalStateException(
              s"unexpected response type: $unexpected")
        }
      }

      override def onError(t: Throwable): Unit = {
        streamError = Some(t)
        completeRequestStream()
        initResponseLatch.countDown()
        done.countDown()
      }

      override def onCompleted(): Unit = {
        initResponseLatch.countDown()
        done.countDown()
      }
    }

    private val requestObserver: StreamObserver[UdfRequest] = stub.execute(responseObserver)

    def sendInit(
        payloadBytes: Array[Byte],
        sendChunked: Boolean = false,
        protocolVersion: Int = SupportedVersion): Unit = {
      if (sendChunked) {
        requestObserver.onNext(UdfRequest.newBuilder()
          .setControl(UdfControlRequest.newBuilder()
            .setInit(Init.newBuilder()
              .setProtocolVersion(protocolVersion)
              .setIsChunkingPayload(true)
              .setDataFormat(UDFWorkerDataFormat.ARROW)
              .setUdf(UdfPayload.newBuilder()
                .setPayload(ByteString.EMPTY)
                .setFormat("echo")
                .build())
              .build())
            .build())
          .build())
        requestObserver.onNext(UdfRequest.newBuilder()
          .setControl(UdfControlRequest.newBuilder()
            .setPayload(PayloadChunk.newBuilder()
              .setData(ByteString.copyFrom(payloadBytes))
              .setLast(true)
              .build())
            .build())
          .build())
      } else {
        requestObserver.onNext(UdfRequest.newBuilder()
          .setControl(UdfControlRequest.newBuilder()
            .setInit(Init.newBuilder()
              .setProtocolVersion(protocolVersion)
              .setDataFormat(UDFWorkerDataFormat.ARROW)
              .setUdf(UdfPayload.newBuilder()
                .setPayload(ByteString.copyFrom(payloadBytes))
                .setFormat("echo")
                .build())
              .build())
            .build())
          .build())
      }
    }

    def sendData(data: Array[Byte]): Unit = {
      awaitInitResponse()
      requestObserver.onNext(UdfRequest.newBuilder()
        .setData(DataRequest.newBuilder()
          .setData(ByteString.copyFrom(data))
          .build())
        .build())
    }

    // Sends Init with is_chunking_payload=true but no chunks. Tests then
    // drive the chunks themselves via sendPayloadChunk.
    def sendInitChunked(protocolVersion: Int = SupportedVersion): Unit = {
      requestObserver.onNext(UdfRequest.newBuilder()
        .setControl(UdfControlRequest.newBuilder()
          .setInit(Init.newBuilder()
            .setProtocolVersion(protocolVersion)
            .setIsChunkingPayload(true)
            .setDataFormat(UDFWorkerDataFormat.ARROW)
            .setUdf(UdfPayload.newBuilder()
              .setPayload(ByteString.EMPTY)
              .setFormat("echo")
              .build())
            .build())
          .build())
        .build())
    }

    // Sends a single PayloadChunk. Does not wait for InitResponse --
    // chunks are part of the init handshake itself.
    def sendPayloadChunk(data: Array[Byte], last: Boolean): Unit = {
      requestObserver.onNext(UdfRequest.newBuilder()
        .setControl(UdfControlRequest.newBuilder()
          .setPayload(PayloadChunk.newBuilder()
            .setData(ByteString.copyFrom(data))
            .setLast(last)
            .build())
          .build())
        .build())
    }

    def sendFinish(): Unit = {
      awaitInitResponse()
      if (requestCompleted.get()) return
      requestObserver.onNext(UdfRequest.newBuilder()
        .setControl(UdfControlRequest.newBuilder()
          .setFinish(Finish.getDefaultInstance)
          .build())
        .build())
      // Request stream stays open: Cancel may still follow Finish.
      // completeRequestStream() is called by the response observer.
    }

    // The engine MUST wait for InitResponse before sending any DataRequest
    // or Finish. Under directExecutor this returns immediately because
    // sendInit's InitResponse callback runs synchronously.
    private def awaitInitResponse(): Unit = {
      if (!initResponseLatch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("InitResponse not received within timeout")
      }
    }

    def sendCancel(reason: String = ""): Unit = {
      // If a terminator already arrived (FinishResponse / CancelResponse),
      // the request stream has been half-closed and Cancel arrives too
      // late -- silently ignore, matching the proto's Cancel-after-Finish
      // contract.
      if (requestCompleted.get()) return
      requestObserver.onNext(UdfRequest.newBuilder()
        .setControl(UdfControlRequest.newBuilder()
          .setCancel(Cancel.newBuilder().setReason(reason).build())
          .build())
        .build())
      // Request stream stays open until the response terminator arrives;
      // completeRequestStream() is called by the response observer.
    }

    def completeRequestStream(): Unit = {
      if (requestCompleted.compareAndSet(false, true)) {
        requestObserver.onCompleted()
      }
    }

    def awaitDone(timeoutMs: Long = 5000): Boolean =
      done.await(timeoutMs, TimeUnit.MILLISECONDS)

    def drainResults(): Seq[Array[Byte]] = {
      val buf = new java.util.ArrayList[Array[Byte]]()
      results.drainTo(buf)
      import scala.jdk.CollectionConverters._
      buf.asScala.toSeq
    }
  }

  // ===========================================================================
  // TESTS
  // ===========================================================================

  test("echo: single DataRequest round-trip") {
    val client = new EngineClient(stub)
    client.sendInit("dummy-payload".getBytes)
    client.sendData("hello".getBytes)
    client.sendFinish()

    assert(client.awaitDone(), "stream did not complete in time")
    assert(client.streamError.isEmpty, s"unexpected stream error: ${client.streamError}")
    assert(client.executionError.isEmpty, s"unexpected execution error: ${client.executionError}")
    val results = client.drainResults()
    assert(results.length == 1)
    assert(new String(results.head) == "hello")
  }

  test("echo: multiple DataRequest batches are all echoed") {
    val client = new EngineClient(stub)
    client.sendInit("dummy-payload".getBytes)
    Seq("batch1", "batch2", "batch3").foreach(b => client.sendData(b.getBytes))
    client.sendFinish()

    assert(client.awaitDone())
    assert(client.streamError.isEmpty)
    val results = client.drainResults().map(new String(_))
    assert(results == Seq("batch1", "batch2", "batch3"))
  }

  // The engine drives the request side from a producer thread while the
  // response observer fires on a gRPC-managed callback thread. gRPC's
  // bidirectional streaming and HTTP/2 flow control manage the interleaving;
  // no explicit coordination is needed beyond the protocol ordering invariants.
  test("echo: concurrent sending and receiving (producer/consumer pattern)") {
    val asyncStub = UdfWorkerGrpc.newStub(channel)

    val receivedCount = new java.util.concurrent.atomic.AtomicInteger(0)
    val doneLatch = new CountDownLatch(1)
    @volatile var streamErr: Option[Throwable] = None
    val requestCompleted = new AtomicBoolean(false)
    // reqObs is assigned after responseObs is created. AtomicReference
    // gives the response observer (which may run on a gRPC callback
    // thread) a safe view of the assignment made by the test thread.
    val reqObsRef = new java.util.concurrent.atomic.AtomicReference[StreamObserver[UdfRequest]]()

    val responseObs = new StreamObserver[UdfResponse] {
      private def completeRequestStream(): Unit =
        if (requestCompleted.compareAndSet(false, true)) reqObsRef.get().onCompleted()

      override def onNext(r: UdfResponse): Unit = r.getResponseCase match {
        case UdfResponse.ResponseCase.DATA => receivedCount.incrementAndGet()
        case UdfResponse.ResponseCase.CONTROL =>
          val c = r.getControl
          c.getControlCase match {
            case UdfControlResponse.ControlCase.INIT => // InitResponse: data phase can proceed
            case UdfControlResponse.ControlCase.FINISH =>
              completeRequestStream()
              doneLatch.countDown()
            case unexpected =>
              throw new IllegalStateException(
                s"unexpected control response: $unexpected")
          }
        case unexpected =>
          throw new IllegalStateException(
            s"unexpected response type: $unexpected")
      }
      override def onError(t: Throwable): Unit = {
        streamErr = Some(t)
        completeRequestStream()
        doneLatch.countDown()
      }
      override def onCompleted(): Unit = doneLatch.countDown()
    }
    reqObsRef.set(asyncStub.execute(responseObs))

    val producer = new Thread(() => {
      val reqObs = reqObsRef.get()
      reqObs.onNext(UdfRequest.newBuilder()
        .setControl(UdfControlRequest.newBuilder()
          .setInit(Init.newBuilder()
            .setProtocolVersion(SupportedVersion)
            .setDataFormat(UDFWorkerDataFormat.ARROW)
            .setUdf(UdfPayload.newBuilder()
              .setPayload(ByteString.copyFromUtf8("payload"))
              .setFormat("echo").build())
            .build())
          .build())
        .build())
      (1 to 5).foreach { i =>
        reqObs.onNext(UdfRequest.newBuilder()
          .setData(DataRequest.newBuilder()
            .setData(ByteString.copyFromUtf8(s"batch-$i")).build())
          .build())
      }
      reqObs.onNext(UdfRequest.newBuilder()
        .setControl(UdfControlRequest.newBuilder()
          .setFinish(Finish.getDefaultInstance).build())
        .build())
      // Request stream stays open; completeRequestStream() is called by
      // the response observer on FinishResponse or gRPC error.
    }, "producer")
    producer.start()

    assert(doneLatch.await(10, TimeUnit.SECONDS), "stream did not complete")
    assert(streamErr.isEmpty, s"unexpected error: $streamErr")
    assert(receivedCount.get() == 5, s"expected 5 echoes, got ${receivedCount.get()}")
  }

  test("echo: chunked payload delivery") {
    val client = new EngineClient(stub)
    client.sendInit("chunked-payload".getBytes, sendChunked = true)
    client.sendData("data".getBytes)
    client.sendFinish()

    assert(client.awaitDone())
    assert(client.streamError.isEmpty)
    assert(new String(client.drainResults().head) == "data")
  }

  test("echo: generator-style UDF (zero DataRequests, engine sends Finish after Init)") {
    val client = new EngineClient(stub)
    client.sendInit("generator-payload".getBytes)
    client.sendFinish()

    assert(client.awaitDone())
    assert(client.streamError.isEmpty)
    assert(client.drainResults().isEmpty)
  }

  test("cancel: engine cancels mid-stream before sending Finish") {
    val client = new EngineClient(stub)
    client.sendInit("dummy-payload".getBytes)
    client.sendData("batch1".getBytes)
    client.sendCancel("task interrupted")

    assert(client.awaitDone())
    assert(client.streamError.isEmpty)
  }

  // Cancel MAY follow Finish. The worker sends CancelResponse if Cancel arrives
  // before FinishResponse is sent, or FinishResponse if it arrived too late.
  // The engine must accept either outcome.
  test("cancel: engine sends Cancel after Finish -- accepts FinishResponse or CancelResponse") {
    val client = new EngineClient(stub)
    client.sendInit("dummy-payload".getBytes)
    client.sendData("data".getBytes)
    client.sendFinish()
    client.sendCancel("task interrupted after finish")

    assert(client.awaitDone(), "stream did not complete")
    assert(client.streamError.isEmpty,
      s"Cancel-after-Finish must not cause a gRPC error: ${client.streamError}")
  }

  test("ErrorResponse: worker signals UserError, engine sends Cancel and receives CancelResponse") {
    val client = new EngineClient(stub)
    client.sendInit("dummy-payload".getBytes)
    client.sendData(ErrorTrigger.toByteArray)

    assert(client.awaitDone())
    assert(client.streamError.isEmpty, s"expected no gRPC error, got ${client.streamError}")
    assert(client.executionError.isDefined, "expected an ExecutionError")
    assert(client.executionError.get.hasUser, "expected UserError kind")
    assert(client.executionError.get.getUser.getErrorClass == "SimulatedError")
  }

  test("protocol error: second Init is rejected with ProtocolError + CancelResponse") {
    val client = new EngineClient(stub)
    client.sendInit("payload".getBytes)
    client.sendInit("second-init".getBytes)

    assert(client.awaitDone())
    assert(client.streamError.isEmpty, "expected ProtocolError, not a gRPC stream error")
    assert(client.executionError.isDefined, "expected an ExecutionError")
    assert(client.executionError.get.hasProtocol, "expected ProtocolError kind")
  }

  test("init error: inline payload triggers init failure") {
    val client = new EngineClient(stub)
    client.sendInit(InitErrorTrigger.toByteArray)

    assert(client.awaitDone())
    assert(client.streamError.isEmpty, s"expected no gRPC error, got ${client.streamError}")
    assert(client.executionError.isDefined, "expected an init error")
    assert(client.executionError.get.hasWorker, "expected WorkerError kind")
    assert(client.executionError.get.getWorker.getMessage == "simulated init failure")
  }

  test("init error: chunked payload assembled across chunks triggers init failure") {
    val client = new EngineClient(stub)
    client.sendInit(InitErrorTrigger.toByteArray, sendChunked = true)

    assert(client.awaitDone())
    assert(client.streamError.isEmpty, s"expected no gRPC error, got ${client.streamError}")
    assert(client.executionError.isDefined, "expected an init error")
    assert(client.executionError.get.hasWorker, "expected WorkerError kind")
  }

  test("init error: unsupported protocol version triggers init failure") {
    val client = new EngineClient(stub)
    client.sendInit("payload".getBytes, protocolVersion = SupportedVersion + 999)

    assert(client.awaitDone())
    assert(client.streamError.isEmpty, s"expected no gRPC error, got ${client.streamError}")
    assert(client.executionError.isDefined, "expected an init error")
    assert(client.executionError.get.hasProtocol, "expected ProtocolError kind")
  }

  test("cancel: Cancel before Init is accepted (AwaitingInit state)") {
    val client = new EngineClient(stub)
    client.sendCancel("aborting before init")

    assert(client.awaitDone())
    assert(client.streamError.isEmpty, s"expected no gRPC error, got ${client.streamError}")
    assert(client.executionError.isEmpty, "Cancel before Init is normal abort, not an error")
  }

  test("cancel: Cancel during chunked payload delivery (AwaitingChunks state)") {
    val client = new EngineClient(stub)
    client.sendInitChunked()
    client.sendPayloadChunk("partial".getBytes, last = false)
    // No final chunk: worker is still accumulating when Cancel arrives.
    client.sendCancel("aborting mid-chunking")

    assert(client.awaitDone())
    assert(client.streamError.isEmpty)
    assert(client.executionError.isEmpty)
  }

  test("echo: chunked payload assembled from multiple non-final chunks") {
    val client = new EngineClient(stub)
    client.sendInitChunked()
    client.sendPayloadChunk("part1".getBytes, last = false)
    client.sendPayloadChunk("part2".getBytes, last = false)
    client.sendPayloadChunk("final".getBytes, last = true)
    // The accumulator state machine should have produced InitResponse after
    // the last=true chunk; the data-phase round-trip below verifies the
    // worker correctly transitioned to Data.
    client.sendData("after-chunks".getBytes)
    client.sendFinish()

    assert(client.awaitDone())
    assert(client.streamError.isEmpty)
    assert(new String(client.drainResults().head) == "after-chunks")
  }

  test("protocol error: PayloadChunk in Data state (no chunking flag on Init)") {
    val client = new EngineClient(stub)
    client.sendInit("payload".getBytes) // non-chunked init transitions to Data
    client.sendPayloadChunk("oops".getBytes, last = true)

    assert(client.awaitDone())
    assert(client.streamError.isEmpty)
    assert(client.executionError.isDefined, "expected a protocol error")
    assert(client.executionError.get.hasProtocol)
  }

  test("Manage: heartbeat is acknowledged") {
    val blockingStub = UdfWorkerGrpc.newBlockingStub(channel)
    val resp = blockingStub.manage(WorkerRequest.newBuilder()
      .setHeartbeat(Heartbeat.getDefaultInstance)
      .build())
    assert(resp.hasHeartbeat, "expected HeartbeatResponse")
  }

  test("Manage: ShutdownRequest is acknowledged") {
    val blockingStub = UdfWorkerGrpc.newBlockingStub(channel)
    val resp = blockingStub.manage(WorkerRequest.newBuilder()
      .setShutdown(ShutdownRequest.newBuilder().setReason("test done").build())
      .build())
    assert(resp.hasShutdown, "expected ShutdownResponse")
  }
}
