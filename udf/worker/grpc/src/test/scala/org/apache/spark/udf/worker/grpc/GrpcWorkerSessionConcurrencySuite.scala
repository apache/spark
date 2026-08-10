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

import java.util.Locale
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString
import io.grpc.{CallOptions, ClientCall, ConnectivityState, ForwardingClientCall, ManagedChannel,
  Metadata, MethodDescriptor, Server}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.StreamObserver
import org.scalatest.BeforeAndAfterEach
// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker.{Cancel, CancelResponse, DataRequest, DataResponse,
  ErrorResponse, ExecutionError, Finish, FinishResponse, Init, InitResponse, UdfControlResponse,
  UdfPayload, UdfRequest, UdfResponse, UDFWorkerDataFormat, UdfWorkerGrpc, UserError,
  WorkerRequest, WorkerResponse}
import org.apache.spark.udf.worker.core.{Termination, WorkerHandle, WorkerLogger}

/**
 * Concurrency tests for [[GrpcWorkerSession]] that pin the wire-ordering and
 * fast-fail invariants under concurrent and worker-misbehavior scenarios:
 *  - Cancel must never appear on the wire before Init.
 *  - A worker terminator (ERROR / FINISH / CANCEL / onCompleted) arriving
 *    before InitResponse must fail [[GrpcWorkerSession#init]] fast, not hang
 *    for `initResponseTimeoutMs`.
 *  - Repeated [[Iterator#hasNext]] after natural iterator exhaustion must
 *    return immediately, not block for `terminalTimeoutMs`.
 *  - Close racing the initial write must either prevent Init or send Cancel
 *    after Init without releasing the worker underneath an active stream.
 *  - An immediate ErrorResponse after InitResponse must fail init and send the
 *    protocol-required Cancel; a premature FinishResponse must not truncate input.
 *  - Cancel and close concurrent with an in-progress data phase terminate
 *    cleanly without leaks or unbounded hangs.
 *
 * Runs entirely in-process: no subprocess, no UDS. Server services are
 * custom-built per test so we can drive specific worker misbehavior.
 */
class GrpcWorkerSessionConcurrencySuite
    extends AnyFunSuite with BeforeAndAfterEach {
// scalastyle:on funsuite

  /** Used by tests to keep stale in-flight infra reachable for teardown. */
  private val openServers = new ConcurrentLinkedQueue[Server]()
  private val openChannels = new ConcurrentLinkedQueue[ManagedChannel]()
  private val openSessions = new ConcurrentLinkedQueue[GrpcWorkerSession]()

  override def afterEach(): Unit = {
    // Shut channels down first. This fires onError on any still-live stream,
    // which settles the session terminal and counts down the init/terminal
    // latches. That unblocks both the session.close() below and any worker
    // thread a failing test left parked on a (deliberately large) timeout, so
    // teardown never hangs even when a test asserts via assertFinishesWithin.
    openChannels.asScala.foreach { c =>
      try c.shutdownNow().awaitTermination(2, TimeUnit.SECONDS) catch { case _: Throwable => () }
    }
    openChannels.clear()
    openSessions.asScala.foreach { s => try s.close(emptyCancel) catch { case _: Throwable => () } }
    openSessions.clear()
    openServers.asScala.foreach { s =>
      try s.shutdownNow().awaitTermination(2, TimeUnit.SECONDS) catch { case _: Throwable => () }
    }
    openServers.clear()
    super.afterEach()
  }

  // A session timeout large enough that a correct test never reaches it; a
  // regression that fails to short-circuit blocks here for minutes and is caught
  // by assertFinishesWithin (below) instead of a flaky `elapsed < timeout` bound.
  private val NeverReachedTimeoutMs = TimeUnit.MINUTES.toMillis(10)

  /**
   * Runs `body` on a daemon thread and asserts it finishes within `withinMs`,
   * rethrowing whatever `body` threw (so an `intercept` inside `body` still
   * works). Pair with [[NeverReachedTimeoutMs]]: the correct fast path returns
   * in milliseconds, so `withinMs` (seconds) has an enormous safety margin and
   * does not flake, while a regression that parks on the timeout never returns
   * within `withinMs` and fails the assertion.
   */
  private def assertFinishesWithin(withinMs: Long, name: String)(body: => Unit): Unit = {
    val thrown = new AtomicReference[Throwable]()
    val done = new CountDownLatch(1)
    val worker = new Thread(() => {
      try body catch { case t: Throwable => thrown.set(t) } finally done.countDown()
    }, name)
    worker.setDaemon(true)
    worker.start()
    assert(done.await(withinMs, TimeUnit.MILLISECONDS),
      s"$name did not finish within ${withinMs}ms; it parked on a session timeout " +
        "that the fast path should have short-circuited")
    Option(thrown.get()).foreach(t => throw t)
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private class TestWorkerHandle extends WorkerHandle {
    val invalidated = new AtomicBoolean(false)
    val released = new AtomicBoolean(false)
    override def id: String = "test-worker"
    override def markInvalid(): Unit = invalidated.set(true)
    override def releaseSession(): Unit = released.set(true)
  }

  /**
   * Builds an in-process server/channel pair.
   *
   * @param directExecutor when `true` (default), responses are delivered
   *        reentrantly on the calling thread -- the worst case for the session's
   *        reentrancy handling. When `false`, both server and channel use their
   *        default executors, so responses arrive on a *separate* thread. That
   *        cross-thread delivery mirrors the production Netty transport (the gRPC
   *        callback runs on an event-loop thread, never reentrantly), which the
   *        directExecutor tests deliberately do not exercise.
   */
  private def startServer(
      service: UdfWorkerGrpc.UdfWorkerImplBase,
      directExecutor: Boolean = true): (Server, ManagedChannel) = {
    val name = InProcessServerBuilder.generateName()
    val serverBuilder = InProcessServerBuilder.forName(name).addService(service)
    val channelBuilder = InProcessChannelBuilder.forName(name)
    if (directExecutor) {
      serverBuilder.directExecutor()
      channelBuilder.directExecutor()
    }
    val server = serverBuilder.build().start()
    val channel = channelBuilder.build()
    openServers.add(server)
    openChannels.add(channel)
    (server, channel)
  }

  /** A [[ManagedChannel]] wrapper whose operations delegate to `underlying`. */
  private class DelegatingManagedChannel(underlying: ManagedChannel) extends ManagedChannel {
    override def shutdown(): ManagedChannel = {
      underlying.shutdown()
      this
    }

    override def isShutdown: Boolean = underlying.isShutdown

    override def isTerminated: Boolean = underlying.isTerminated

    override def shutdownNow(): ManagedChannel = {
      underlying.shutdownNow()
      this
    }

    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
      underlying.awaitTermination(timeout, unit)

    override def newCall[ReqT, RespT](
        methodDescriptor: MethodDescriptor[ReqT, RespT],
        callOptions: CallOptions): ClientCall[ReqT, RespT] =
      underlying.newCall(methodDescriptor, callOptions)

    override def authority(): String = underlying.authority()

    override def getState(requestConnection: Boolean): ConnectivityState =
      underlying.getState(requestConnection)

    override def notifyWhenStateChanged(
        source: ConnectivityState,
        callback: Runnable): Unit = underlying.notifyWhenStateChanged(source, callback)

    override def resetConnectBackoff(): Unit = underlying.resetConnectBackoff()

    override def enterIdle(): Unit = underlying.enterIdle()
  }

  private def newSession(
      channel: ManagedChannel,
      initResponseTimeoutMs: Long = 5000L,
      terminalTimeoutMs: Long = 5000L,
      interruptCancelTimeoutMs: Long = 5000L): GrpcWorkerSession = {
    val session = new GrpcWorkerSession(
      new TestWorkerHandle, channel, WorkerLogger.NoOp,
      initResponseTimeoutMs = initResponseTimeoutMs,
      terminalTimeoutMs = terminalTimeoutMs,
      interruptCancelTimeoutMs = interruptCancelTimeoutMs)
    openSessions.add(session)
    session
  }

  /**
   * The [[TestWorkerHandle]] backing a session, for asserting the handle
   * lifecycle. `workerHandle` is `private[worker]`, and this suite lives under
   * `org.apache.spark.udf.worker`, so the access is in scope.
   */
  private def handleOf(session: GrpcWorkerSession): TestWorkerHandle =
    session.workerHandle.asInstanceOf[TestWorkerHandle]

  // Protocol version carried on Init. The in-process fake workers in this suite
  // do not validate it; any sane value is fine.
  private val SupportedVersion = 1

  private def basicInit(payload: String = "echo"): Init = Init.newBuilder()
    .setProtocolVersion(SupportedVersion)
    .setDataFormat(UDFWorkerDataFormat.ARROW)
    .setUdf(UdfPayload.newBuilder()
      .setPayload(ByteString.copyFromUtf8(payload))
      .setFormat("echo")
      .build())
    .build()

  // Default lifecycle messages for the data phase and finalization.
  private val emptyFinish: () => Finish = () => Finish.getDefaultInstance
  private val emptyCancel: () => Cancel = () => Cancel.getDefaultInstance

  /** Wraps strings as input [[DataRequest]] batches. */
  private def echoIn(batches: String*): Iterator[DataRequest] =
    batches.iterator.map(s => DataRequest.newBuilder()
      .setData(ByteString.copyFromUtf8(s)).build())

  /**
   * Captures every incoming request in order. Replies follow a user-supplied
   * function, so tests can drive arbitrary worker misbehavior. The default
   * `onRequest` matches an Echo worker: InitResponse on Init, echo on Data,
   * FinishResponse on Finish, CancelResponse on Cancel.
   */
  private class CapturingService(
      val captured: ConcurrentLinkedQueue[UdfRequest] = new ConcurrentLinkedQueue(),
      onRequest: (UdfRequest, StreamObserver[UdfResponse]) => Unit = null)
    extends UdfWorkerGrpc.UdfWorkerImplBase {

    private val handler: (UdfRequest, StreamObserver[UdfResponse]) => Unit =
      if (onRequest != null) onRequest else defaultEcho

    override def execute(resp: StreamObserver[UdfResponse]): StreamObserver[UdfRequest] =
      new StreamObserver[UdfRequest] {
        // gRPC requires serialized writes to a request StreamObserver; the
        // capturing service may reply from multiple control paths so we
        // synchronize on `resp` rather than relying on directExecutor.
        override def onNext(req: UdfRequest): Unit = {
          captured.add(req)
          resp.synchronized { handler(req, resp) }
        }
        override def onError(t: Throwable): Unit = ()
        override def onCompleted(): Unit = resp.synchronized { resp.onCompleted() }
      }

    override def manage(
        request: WorkerRequest,
        responseObserver: StreamObserver[WorkerResponse]): Unit = ()

    private def defaultEcho(req: UdfRequest, resp: StreamObserver[UdfResponse]): Unit = {
      req.getRequestCase match {
        case UdfRequest.RequestCase.CONTROL =>
          val c = req.getControl
          c.getControlCase match {
            case _ if c.hasInit =>
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setInit(
                  InitResponse.getDefaultInstance).build()
              ).build())
            case _ if c.hasFinish =>
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setFinish(
                  FinishResponse.getDefaultInstance).build()
              ).build())
            case _ if c.hasCancel =>
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setCancel(
                  CancelResponse.getDefaultInstance).build()
              ).build())
            case _ => ()
          }
        case UdfRequest.RequestCase.DATA =>
          resp.onNext(UdfResponse.newBuilder()
            .setData(DataResponse.newBuilder().setData(req.getData.getData).build())
            .build())
        case _ => ()
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Cancel-never-precedes-Init invariant. Publication of requestObserver and the
  // Init write are serialized with close/cancel under requestLock: close either
  // wins first and prevents Init entirely, or waits until Init is on the wire
  // before writing Cancel.
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // Terminator-before-InitResponse: must fail init fast, not hang for
  // initResponseTimeoutMs (onTerminalSettled completes initValue without a value).
  // ---------------------------------------------------------------------------

  test("worker emits ErrorResponse before InitResponse: init fails fast") {
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setError(
              ErrorResponse.newBuilder().setError(
                ExecutionError.newBuilder().setUser(
                  UserError.newBuilder().setMessage("simulated pre-init error")
                    .setErrorClass("PreInitError").build()).build()).build()).build())
            .build())
        } else if (req.hasControl && req.getControl.hasCancel) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setCancel(
              CancelResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, channel) = startServer(service)
    // Huge init timeout: a regression that failed to complete initValue would
    // park init here until the timeout, so finishing quickly is proof the fast
    // path ran -- no wall-clock threshold needed.
    val session = newSession(channel, initResponseTimeoutMs = NeverReachedTimeoutMs)

    assertFinishesWithin(10000, "init") {
      val ex = intercept[GrpcWorkerSessionException] { session.init(basicInit()) }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("error") ||
        ex.getMessage.toLowerCase(Locale.ROOT).contains("init"),
        s"expected init/error in message, got: ${ex.getMessage}")
    }
    session.close(emptyCancel)
  }

  test("worker emits InitResponse with error: init fails fast (no terminal-timeout stall)") {
    // Regression for directExecutor reentrancy where InitResponse(error) is
    // delivered inside stream.onNext. requestObserver is already published, so
    // doInit can send the required Cancel after onNext returns and drain the
    // CancelResponse without stalling on terminalTimeoutMs.
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setInit(
              InitResponse.newBuilder().setError(
                ExecutionError.newBuilder().setUser(
                  UserError.newBuilder().setMessage("simulated init failure")
                    .setErrorClass("InitError").build()).build()).build()).build())
            .build())
        } else if (req.hasControl && req.getControl.hasCancel) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setCancel(
              CancelResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, channel) = startServer(service)
    // Huge terminalTimeoutMs: a regression (awaiting a CancelResponse that never
    // arrives) would park init in awaitTerminal for the full timeout; the fix
    // settles the terminal at once so init returns in milliseconds. Finishing
    // well within assertFinishesWithin is the signal the stall did not happen.
    val session = newSession(channel,
      initResponseTimeoutMs = NeverReachedTimeoutMs, terminalTimeoutMs = NeverReachedTimeoutMs)

    assertFinishesWithin(10000, "init") {
      val ex = intercept[GrpcWorkerSessionException] { session.init(basicInit()) }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("init"),
        s"expected an init-failure message, got: ${ex.getMessage}")
    }
    session.close(emptyCancel)
  }

  test("terminal signal wins over a late InitResponse") {
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          // DATA before InitResponse is a protocol failure and completes the
          // init one-shot without a value. A later InitResponse must not overwrite
          // that completion and make init() return successfully.
          resp.onNext(UdfResponse.newBuilder()
            .setData(DataResponse.newBuilder()
              .setData(ByteString.copyFromUtf8("premature")).build())
            .build())
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setInit(
              InitResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, channel) = startServer(service)
    val session = newSession(channel, initResponseTimeoutMs = NeverReachedTimeoutMs)

    val ex = intercept[GrpcWorkerSessionException] { session.init(basicInit()) }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("init") ||
      ex.getMessage.toLowerCase(Locale.ROOT).contains("stream"),
      s"expected the earlier protocol failure to win, got: ${ex.getMessage}")
    assert(session.close(emptyCancel).isInstanceOf[Termination.TransportFailed])
  }

  test("ErrorResponse immediately after InitResponse fails init and sends Cancel") {
    val cancelSeen = new CountDownLatch(1)
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          // Both responses are delivered reentrantly from inside the Init write.
          // The second response is a valid generator-style data-phase failure.
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setInit(
              InitResponse.getDefaultInstance).build()).build())
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setError(
              ErrorResponse.newBuilder().setError(
                ExecutionError.newBuilder().setUser(
                  UserError.newBuilder().setMessage("generator failed")
                    .setErrorClass("GeneratorError").build()).build()).build()).build())
            .build())
        } else if (req.hasControl && req.getControl.hasCancel) {
          cancelSeen.countDown()
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setCancel(
              CancelResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, channel) = startServer(service)
    val session = newSession(channel, terminalTimeoutMs = NeverReachedTimeoutMs)

    val ex = intercept[GrpcWorkerSessionException] { session.init(basicInit()) }
    assert(ex.getMessage.contains("generator failed"),
      s"the immediate ErrorResponse must fail init with the worker error, got: ${ex.getMessage}")
    assert(cancelSeen.await(5, TimeUnit.SECONDS),
      "an immediate post-init ErrorResponse must send the protocol-required Cancel")
    assert(session.close(emptyCancel).isInstanceOf[Termination.Cancelled])
  }

  test("FinishResponse before Finish is rejected instead of truncating input") {
    val replied = new AtomicBoolean(false)
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setInit(
              InitResponse.getDefaultInstance).build()).build())
        } else if (req.hasData && replied.compareAndSet(false, true)) {
          // A buggy worker claims success after the first batch even though the
          // engine has neither exhausted its input nor sent Finish.
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setFinish(
              FinishResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, channel) = startServer(service)
    val session = newSession(channel, terminalTimeoutMs = NeverReachedTimeoutMs)
    session.init(basicInit())

    val ex = intercept[GrpcWorkerSessionException] {
      session.process(echoIn("first", "must-not-be-consumed"), emptyFinish).foreach(_ => ())
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("stream failed"),
      s"a premature FinishResponse must be a protocol failure, got: ${ex.getMessage}")
    val dataRequests = service.captured.asScala.count(_.hasData)
    assert(dataRequests == 1,
      s"the premature terminator should stop input after one batch, got: $dataRequests")
    assert(session.close(emptyCancel).isInstanceOf[Termination.TransportFailed])
  }

  test("worker emits FinishResponse before InitResponse: init fails fast") {
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setFinish(
              FinishResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, channel) = startServer(service)
    val session = newSession(channel, initResponseTimeoutMs = NeverReachedTimeoutMs)

    assertFinishesWithin(10000, "init") {
      val ex = intercept[GrpcWorkerSessionException] { session.init(basicInit()) }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("finish") ||
        ex.getMessage.toLowerCase(Locale.ROOT).contains("init"),
        s"expected init/finish in message, got: ${ex.getMessage}")
    }
    session.close(emptyCancel)
  }

  test("worker emits CancelResponse before InitResponse: init fails fast") {
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setCancel(
              CancelResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, channel) = startServer(service)
    val session = newSession(channel, initResponseTimeoutMs = NeverReachedTimeoutMs)

    assertFinishesWithin(10000, "init") {
      val ex = intercept[GrpcWorkerSessionException] { session.init(basicInit()) }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("cancel") ||
        ex.getMessage.toLowerCase(Locale.ROOT).contains("init"),
        s"expected init/cancel in message, got: ${ex.getMessage}")
    }
    session.close(emptyCancel)
  }

  test("worker half-closes response stream before InitResponse: init fails fast") {
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onCompleted()
        }
      })
    val (_, channel) = startServer(service)
    val session = newSession(channel, initResponseTimeoutMs = NeverReachedTimeoutMs)

    assertFinishesWithin(10000, "init") {
      intercept[GrpcWorkerSessionException] { session.init(basicInit()) }
    }
    session.close(emptyCancel)
  }

  test("worker emits malformed UdfResponse before InitResponse: init fails fast") {
    // A UdfResponse with no oneof set (RESPONSE_NOT_SET) is malformed: the
    // session's responseObserver settles a transport-failure terminal in its
    // catch-all `case other` branch. Regression guard for failing to wake the
    // initValue waiter when that terminal settles: without it, init() blocks
    // until initResponseTimeoutMs and reports a misleading "timed out" error
    // instead of failing fast with the malformed-response cause.
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onNext(UdfResponse.getDefaultInstance)
        }
      })
    val (_, channel) = startServer(service)
    val session = newSession(channel, initResponseTimeoutMs = NeverReachedTimeoutMs)

    assertFinishesWithin(10000, "init") {
      val ex = intercept[GrpcWorkerSessionException] { session.init(basicInit()) }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("init"),
        s"expected init in message, got: ${ex.getMessage}")
    }
    session.close(emptyCancel)
  }

  // ---------------------------------------------------------------------------
  // Exhaustion guard: repeated hasNext after the iterator drains must not
  // block for terminalTimeoutMs (exhausted flag fix).
  // ---------------------------------------------------------------------------

  test("repeated hasNext after natural exhaustion returns immediately") {
    val service = new CapturingService()
    val (_, channel) = startServer(service)
    // Huge terminalTimeoutMs: without the exhausted-flag short-circuit, a second
    // hasNext() would re-enter the output-queue poll and park for the full
    // timeout. Probing within assertFinishesWithin therefore proves the probes
    // are non-blocking, with no dependence on a wall-clock threshold.
    val session = newSession(channel, terminalTimeoutMs = NeverReachedTimeoutMs)
    session.init(basicInit())
    val it = session.process(echoIn("hello"), emptyFinish)
    assert(new String(it.next().getData.toByteArray) == "hello")
    // Drain to terminator.
    assert(!it.hasNext, "iterator should be exhausted after the single echo batch")
    // Now probe many times; each call must return false without blocking.
    assertFinishesWithin(10000, "repeated-hasNext") {
      (1 to 5).foreach { _ =>
        assert(!it.hasNext, "exhausted iterator should keep returning false")
      }
    }
    session.close(emptyCancel)
  }

  // ---------------------------------------------------------------------------
  // close() concurrent with process(): clean termination.
  // ---------------------------------------------------------------------------

  test("close concurrent with process: iterator surfaces cancellation cleanly") {
    // Worker echoes data but never sends FinishResponse (only after Cancel
    // arrives, it sends CancelResponse). This pins the timing so the
    // engine-side iterator is genuinely waiting when close() intervenes.
    val readyToCancel = new CountDownLatch(1)
    val service = new CapturingService(
      onRequest = (req, resp) => {
        req.getRequestCase match {
          case UdfRequest.RequestCase.CONTROL =>
            val c = req.getControl
            if (c.hasInit) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setInit(
                  InitResponse.getDefaultInstance).build()).build())
            } else if (c.hasCancel) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setCancel(
                  CancelResponse.getDefaultInstance).build()).build())
            }
          // Finish ignored: the worker never replies, so the iterator is
          // blocked waiting on output until cancel intervenes.
          case UdfRequest.RequestCase.DATA =>
            resp.onNext(UdfResponse.newBuilder()
              .setData(DataResponse.newBuilder().setData(req.getData.getData).build())
              .build())
            readyToCancel.countDown()
          case _ => ()
        }
      })
    val (_, channel) = startServer(service)
    val session = newSession(channel, terminalTimeoutMs = 30000L)
    session.init(basicInit())

    val handle = new AtomicReference[Throwable]()
    val processThread = new Thread(() => {
      try session.process(echoIn("hello"), emptyFinish).foreach(_ => ())
      catch { case t: Throwable => handle.set(t) }
    }, "process-cancel")
    processThread.start()
    assert(readyToCancel.await(5, TimeUnit.SECONDS),
      "worker never received the data batch")
    // close() from another thread is the cancellation trigger: it sends Cancel,
    // the worker replies CancelResponse, and the in-flight iterator surfaces it.
    session.close(emptyCancel)
    processThread.join(10000)
    assert(!processThread.isAlive, "process thread should terminate after close")
    val t = handle.get()
    assert(t != null, "expected the iterator to surface a cancellation exception")
    assert(t.isInstanceOf[GrpcWorkerSessionException],
      s"expected GrpcWorkerSessionException, got ${t.getClass.getName}")
  }

  // ---------------------------------------------------------------------------
  // Close concurrent with in-progress process(): bounded, no hang.
  // ---------------------------------------------------------------------------

  test("close concurrent with process: bounded shutdown, no leak") {
    val readyToClose = new CountDownLatch(1)
    val service = new CapturingService(
      onRequest = (req, resp) => {
        req.getRequestCase match {
          case UdfRequest.RequestCase.CONTROL =>
            val c = req.getControl
            if (c.hasInit) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setInit(
                  InitResponse.getDefaultInstance).build()).build())
            } else if (c.hasCancel) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setCancel(
                  CancelResponse.getDefaultInstance).build()).build())
            }
          case UdfRequest.RequestCase.DATA =>
            resp.onNext(UdfResponse.newBuilder()
              .setData(DataResponse.newBuilder().setData(req.getData.getData).build())
              .build())
            readyToClose.countDown()
          case _ => ()
        }
      })
    val (_, channel) = startServer(service)
    // Huge terminalTimeoutMs: the worker replies CancelResponse, so a correct
    // close() returns on that terminator in milliseconds. A regression that
    // failed to terminate on the terminator would instead park on the timeout,
    // which assertFinishesWithin catches without a wall-clock threshold.
    val session = newSession(channel, terminalTimeoutMs = NeverReachedTimeoutMs)
    session.init(basicInit())

    val processThread = new Thread(() => {
      try session.process(echoIn("hello"), emptyFinish).foreach(_ => ())
      catch { case _: Throwable => () }
    }, "process-close")
    processThread.start()
    assert(readyToClose.await(5, TimeUnit.SECONDS),
      "worker never received the data batch")

    assertFinishesWithin(10000, "close")(session.close(emptyCancel))
    processThread.join(10000)
    assert(!processThread.isAlive, "process thread should terminate after close")
  }

  // ---------------------------------------------------------------------------
  // Cross-thread delivery (no directExecutor).
  //
  // The tests above deliver worker responses reentrantly on the caller's thread
  // (directExecutor), which is the harness for the reentrancy-hardening paths
  // but is NOT how the production Netty transport behaves -- there the gRPC
  // callback always runs on a separate event-loop thread. These tests re-run the
  // key fast-fail and data-phase paths with cross-thread delivery so the
  // session's correctness does not silently depend on reentrant delivery.
  // ---------------------------------------------------------------------------

  test("cross-thread delivery: ErrorResponse before InitResponse fails init fast") {
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setError(
              ErrorResponse.newBuilder().setError(
                ExecutionError.newBuilder().setUser(
                  UserError.newBuilder().setMessage("simulated pre-init error")
                    .setErrorClass("PreInitError").build()).build()).build()).build())
            .build())
        } else if (req.hasControl && req.getControl.hasCancel) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setCancel(
              CancelResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, channel) = startServer(service, directExecutor = false)
    // requestObserver is published before the cross-thread ErrorResponse can be
    // delivered, so unlike the directExecutor case a Cancel does reach the wire;
    // either way init must fail fast rather than park on a session timeout.
    val session = newSession(channel,
      initResponseTimeoutMs = NeverReachedTimeoutMs, terminalTimeoutMs = NeverReachedTimeoutMs)

    assertFinishesWithin(10000, "init") {
      val ex = intercept[GrpcWorkerSessionException] { session.init(basicInit()) }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("error") ||
        ex.getMessage.toLowerCase(Locale.ROOT).contains("init"),
        s"expected init/error in message, got: ${ex.getMessage}")
    }
    session.close(emptyCancel)
  }

  test("cross-thread delivery: InitResponse with error fails init fast") {
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setInit(
              InitResponse.newBuilder().setError(
                ExecutionError.newBuilder().setUser(
                  UserError.newBuilder().setMessage("simulated init failure")
                    .setErrorClass("InitError").build()).build()).build()).build())
            .build())
        } else if (req.hasControl && req.getControl.hasCancel) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setCancel(
              CancelResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, channel) = startServer(service, directExecutor = false)
    val session = newSession(channel,
      initResponseTimeoutMs = NeverReachedTimeoutMs, terminalTimeoutMs = NeverReachedTimeoutMs)

    assertFinishesWithin(10000, "init") {
      val ex = intercept[GrpcWorkerSessionException] { session.init(basicInit()) }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("init"),
        s"expected an init-failure message, got: ${ex.getMessage}")
    }
    session.close(emptyCancel)
  }

  test("cross-thread delivery: data-phase ErrorResponse surfaces through the iterator") {
    // Worker accepts init, then replies to the first DataRequest with an
    // ErrorResponse instead of an echo. Per the protocol the engine follows with
    // Cancel and the worker replies CancelResponse. Exercises the data-phase
    // race-recovery (sendOrEndOnRacedTerminal) under cross-thread delivery: the
    // error terminal can settle while the iterator is mid-send.
    val erroredOnce = new AtomicBoolean(false)
    val service = new CapturingService(
      onRequest = (req, resp) => {
        req.getRequestCase match {
          case UdfRequest.RequestCase.CONTROL =>
            val c = req.getControl
            if (c.hasInit) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setInit(
                  InitResponse.getDefaultInstance).build()).build())
            } else if (c.hasCancel) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setCancel(
                  CancelResponse.getDefaultInstance).build()).build())
            }
          case UdfRequest.RequestCase.DATA =>
            if (erroredOnce.compareAndSet(false, true)) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setError(
                  ErrorResponse.newBuilder().setError(
                    ExecutionError.newBuilder().setUser(
                      UserError.newBuilder().setMessage("boom in UDF")
                        .setErrorClass("UdfError").build()).build()).build()).build())
                .build())
            }
          case _ => ()
        }
      })
    val (_, channel) = startServer(service, directExecutor = false)
    val session = newSession(channel, terminalTimeoutMs = 5000L)
    session.init(basicInit())

    val it = session.process(echoIn("a", "b", "c"), emptyFinish)
    val ex = intercept[GrpcWorkerSessionException] { it.foreach(_ => ()) }
    assert(ex.getMessage.contains("boom in UDF"),
      s"expected the worker's UDF error to surface, got: ${ex.getMessage}")
    assert(ex.executionError != null, "structured executionError should be preserved")
    assert(ex.executionError.getUser.getErrorClass == "UdfError")
    session.close(emptyCancel)
  }

  test("cross-thread delivery: multi-batch echo round-trips in order then finishes") {
    val service = new CapturingService()
    val (_, channel) = startServer(service, directExecutor = false)
    val session = newSession(channel, terminalTimeoutMs = 5000L)
    session.init(basicInit())

    val it = session.process(echoIn("a", "b", "c"), emptyFinish)
    val out = it.map(r => new String(r.getData.toByteArray)).toList
    assert(out == List("a", "b", "c"),
      s"echo worker should return inputs in order over cross-thread delivery, got: $out")
    assert(!it.hasNext, "iterator should be exhausted after the FinishResponse terminator")
    val handle = handleOf(session)
    val termination = session.close(emptyCancel)
    // Handle lifecycle on a clean finish: the session is released back to the
    // dispatcher exactly once and, because a Finished terminal is salvageable,
    // the worker is NOT marked invalid (it stays eligible for reuse).
    assert(termination.isInstanceOf[Termination.Finished],
      s"a fully drained echo session should settle Finished, got: $termination")
    assert(handle.released.get(), "close() must release the worker handle")
    assert(!handle.invalidated.get(),
      "a clean Finished termination is salvageable; the worker must not be marked invalid")
    // Idempotent close(): still released exactly once, still not invalidated.
    session.close(emptyCancel)
    assert(handle.released.get() && !handle.invalidated.get(),
      "a repeat close() must not change the handle lifecycle outcome")
  }

  // ---------------------------------------------------------------------------
  // An init error surfaces through init()'s exception (carrying the structured
  // ExecutionError); per the protocol the engine then sends Cancel and the worker
  // replies CancelResponse, so the terminal -- and close()'s return -- is the
  // proto terminator Cancelled, not Failed. A genuine transport failure (worker
  // never replies to Cancel) still reports TransportFailed faithfully rather than
  // collapsing into a clean Cancelled.
  // ---------------------------------------------------------------------------

  test("pre-init error: init throws the structured error; close returns Cancelled") {
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setError(
              ErrorResponse.newBuilder().setError(
                ExecutionError.newBuilder().setUser(
                  UserError.newBuilder().setMessage("pre-init boom")
                    .setErrorClass("PreInitError").build()).build()).build()).build())
            .build())
        } else if (req.hasControl && req.getControl.hasCancel) {
          // Proto: the engine must Cancel after an init error and the worker
          // replies CancelResponse, settling the terminal as Cancelled.
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setCancel(
              CancelResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, channel) = startServer(service)
    val session = newSession(channel, terminalTimeoutMs = NeverReachedTimeoutMs)
    // The structured error is surfaced through init()'s exception, not the terminal.
    val ex = intercept[GrpcWorkerSessionException](session.init(basicInit()))
    assert(ex.executionError != null && ex.executionError.getUser.getErrorClass == "PreInitError",
      s"init() must throw the structured error, got: ${ex.executionError}")

    // close() returns the proto terminator: Cancelled, not a generic Failed outcome.
    val termination = session.close(emptyCancel)
    assert(termination.isInstanceOf[Termination.Cancelled],
      s"expected the proto terminator Termination.Cancelled, got: $termination")
  }

  test("close that times out without a terminator returns a TransportFailed termination") {
    // Worker accepts Init but never replies to Cancel, so close() must give up
    // after terminalTimeoutMs and report the failure rather than masquerade as a
    // clean Cancelled. This is the "error only during close" case: nothing is
    // draining the iterator, so the Termination is the only channel for it.
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setInit(
              InitResponse.getDefaultInstance).build()).build())
        }
        // Deliberately ignore Cancel: no terminator ever arrives.
      })
    val (_, channel) = startServer(service)
    val session = newSession(channel, terminalTimeoutMs = 1000L)
    session.init(basicInit())

    val handle = handleOf(session)
    val termination = session.close(emptyCancel)
    assert(termination.isInstanceOf[Termination.TransportFailed],
      s"a close that times out without a terminator must report TransportFailed, got: $termination")
    // Handle lifecycle on an unsalvageable termination: the session is released
    // AND the worker is marked invalid so the dispatcher will not recycle a
    // worker left in an unknown state by the timed-out stream.
    assert(handle.released.get(), "close() must release the worker handle")
    assert(handle.invalidated.get(),
      "a TransportFailed termination is unsalvageable; the worker must be marked invalid")
  }

  // ---------------------------------------------------------------------------
  // Worker sends a DataResponse before InitResponse: a protocol violation that
  // must fail init fast (fast-fail in responseObserver.onNext DATA branch),
  // not enqueue the stray batch and let init() hang for initResponseTimeoutMs.
  // ---------------------------------------------------------------------------

  test("worker emits DataResponse before InitResponse: init fails fast") {
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          // Reply with a DATA response instead of the required InitResponse.
          resp.onNext(UdfResponse.newBuilder()
            .setData(DataResponse.newBuilder()
              .setData(ByteString.copyFromUtf8("premature")).build())
            .build())
        }
      })
    val (_, channel) = startServer(service)
    // Huge init timeout: a regression that enqueued the stray batch instead of
    // fast-failing would park init here until the timeout, so finishing quickly
    // is the proof the fast path ran -- no wall-clock threshold needed.
    val session = newSession(channel, initResponseTimeoutMs = NeverReachedTimeoutMs)

    assertFinishesWithin(10000, "init") {
      val ex = intercept[GrpcWorkerSessionException] { session.init(basicInit()) }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("init"),
        s"expected an init-failure message, got: ${ex.getMessage}")
    }
    session.close(emptyCancel)
  }

  // ---------------------------------------------------------------------------
  // Proto compliance on an init error: the engine MUST send Cancel after an init
  // error and the worker replies CancelResponse (udf_message.proto). init()
  // sends the Cancel -- from where requestObserver is published, not the response
  // callback that may still see it null under reentrant delivery -- and drains
  // the CancelResponse, so the terminal is Cancelled. The structured error is
  // surfaced through init()'s exception, not the terminal.
  // ---------------------------------------------------------------------------

  test("cross-thread InitResponse error: engine sends Cancel, close returns Cancelled") {
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setInit(
              InitResponse.newBuilder().setError(
                ExecutionError.newBuilder().setUser(
                  UserError.newBuilder().setMessage("init failure")
                    .setErrorClass("InitError").build()).build()).build()).build())
            .build())
        } else if (req.hasControl && req.getControl.hasCancel) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setCancel(
              CancelResponse.getDefaultInstance).build()).build())
        }
      })
    // Cross-thread delivery: requestObserver is published before the InitResponse
    // error arrives, so the Cancel reaches the wire and its CancelResponse settles
    // the terminal Cancelled -- the proto terminator.
    val (_, channel) = startServer(service, directExecutor = false)
    val session = newSession(channel,
      initResponseTimeoutMs = NeverReachedTimeoutMs, terminalTimeoutMs = NeverReachedTimeoutMs)
    val ex = intercept[GrpcWorkerSessionException](session.init(basicInit()))
    assert(ex.executionError != null && ex.executionError.getUser.getErrorClass == "InitError",
      s"init() must throw the structured error, got: ${ex.executionError}")

    // Proto invariant: a Cancel was actually written to the worker on the init error.
    assert(service.captured.asScala.exists(r => r.hasControl && r.getControl.hasCancel),
      "the engine must send Cancel after an init error (udf_message.proto)")

    val termination = session.close(emptyCancel)
    assert(termination.isInstanceOf[Termination.Cancelled],
      s"expected the proto terminator Termination.Cancelled, got: $termination")
  }

  // ---------------------------------------------------------------------------
  // A throwing input iterator (hasNext or next) must Cancel the stream so the
  // worker is not stranded awaiting input, and rethrow to the engine.
  // ---------------------------------------------------------------------------

  test("input iterator throwing in hasNext: cancels the stream and surfaces the error") {
    val cancelSeen = new CountDownLatch(1)
    val service = new CapturingService(
      onRequest = (req, resp) => {
        req.getRequestCase match {
          case UdfRequest.RequestCase.CONTROL =>
            val c = req.getControl
            if (c.hasInit) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setInit(
                  InitResponse.getDefaultInstance).build()).build())
            } else if (c.hasCancel) {
              cancelSeen.countDown()
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setCancel(
                  CancelResponse.getDefaultInstance).build()).build())
            }
          case _ => ()
        }
      })
    val (_, channel) = startServer(service, directExecutor = false)
    val session = newSession(channel, terminalTimeoutMs = 5000L)
    session.init(basicInit())

    // An input iterator whose hasNext throws on first probe (mimics a Spark
    // upstream that computes the next element eagerly in hasNext).
    val boom = new Iterator[DataRequest] {
      override def hasNext: Boolean = throw new RuntimeException("hasNext boom")
      override def next(): DataRequest = throw new NoSuchElementException()
    }
    val it = session.process(boom, emptyFinish)
    val ex = intercept[RuntimeException] { it.foreach(_ => ()) }
    assert(ex.getMessage.contains("hasNext boom"),
      s"the upstream failure must propagate, got: ${ex.getMessage}")
    assert(cancelSeen.await(5, TimeUnit.SECONDS),
      "a throwing input iterator must Cancel the stream so the worker is not stranded")
    session.close(emptyCancel)
  }

  test("input iterator returning null: cancels the stream and surfaces the error") {
    val service = new CapturingService()
    val (_, channel) = startServer(service)
    val session = newSession(channel)
    session.init(basicInit())

    val input = Iterator.single(null.asInstanceOf[DataRequest])
    val it = session.process(input, emptyFinish)
    val ex = intercept[NullPointerException] { it.hasNext }
    assert(ex.getMessage.contains("input iterator returned null"),
      s"the invalid input must propagate, got: ${ex.getMessage}")
    val requests = service.captured.asScala.toSeq
    assert(requests.size == 2 && requests.head.getControl.hasInit &&
      requests(1).getControl.hasCancel,
      s"a null input must produce exactly Init then Cancel, got: $requests")
    assert(session.close(emptyCancel).isInstanceOf[Termination.Cancelled])
  }

  // ---------------------------------------------------------------------------
  // A throwing finish() thunk (caller-supplied, may run a finish callback) must
  // Cancel the stream before rethrowing the callback failure.
  // ---------------------------------------------------------------------------

  test("finish thunk throwing: cancels the stream and surfaces the error") {
    val cancelSeen = new CountDownLatch(1)
    val service = new CapturingService(
      onRequest = (req, resp) => {
        req.getRequestCase match {
          case UdfRequest.RequestCase.CONTROL =>
            val c = req.getControl
            if (c.hasInit) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setInit(
                  InitResponse.getDefaultInstance).build()).build())
            } else if (c.hasCancel) {
              cancelSeen.countDown()
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setCancel(
                  CancelResponse.getDefaultInstance).build()).build())
            }
          case UdfRequest.RequestCase.DATA =>
            resp.onNext(UdfResponse.newBuilder()
              .setData(DataResponse.newBuilder().setData(req.getData.getData).build())
              .build())
          case _ => ()
        }
      })
    val (_, channel) = startServer(service, directExecutor = false)
    val session = newSession(channel, terminalTimeoutMs = 5000L)
    session.init(basicInit())

    // finish() throws when the input is exhausted and the iterator tries to
    // build the Finish message. Drive the whole iterator inside intercept:
    // under cross-thread delivery input flows ahead of output, so finish() may
    // fire before the echo of "only" is read back -- the throw can surface on
    // any probe, so we must not read a batch outside the intercept.
    val throwingFinish: () => Finish = () => throw new RuntimeException("finish boom")
    val it = session.process(echoIn("only"), throwingFinish)
    val ex = intercept[RuntimeException] { it.foreach(_ => ()) }
    assert(ex.getMessage.contains("finish boom"),
      s"the finish-thunk failure must propagate, got: ${ex.getMessage}")
    assert(cancelSeen.await(5, TimeUnit.SECONDS),
      "a throwing finish thunk must Cancel the stream so the worker is not stranded")
    session.close(emptyCancel)
  }

  test("finish thunk returning null: cancels the stream and surfaces the error") {
    val service = new CapturingService()
    val (_, channel) = startServer(service)
    val session = newSession(channel)
    session.init(basicInit())

    val it = session.process(Iterator.empty, () => null)
    val ex = intercept[NullPointerException] { it.hasNext }
    assert(ex.getMessage.contains("finish callback returned null"),
      s"the invalid finish result must propagate, got: ${ex.getMessage}")
    val requests = service.captured.asScala.toSeq
    assert(requests.size == 2 && requests.head.getControl.hasInit &&
      requests(1).getControl.hasCancel,
      s"a null finish result must produce exactly Init then Cancel, got: $requests")
    assert(session.close(emptyCancel).isInstanceOf[Termination.Cancelled])
  }

  // ---------------------------------------------------------------------------
  // close() concurrent with init(): close must serialize with stream opening, and
  // a close-settled terminal must wake an init blocked on its response.
  //
  // Regression guard for onTerminalSettled waking initValue. The terminal here is
  // settled by close() ITSELF (the terminalTimeoutMs path in doClose, because the
  // worker ignores Cancel), NOT by the response callback -- so unlike every other
  // terminator this path does not run handleControl's signalWithoutValue(). Only
  // onTerminalSettled can wake the init() blocked on initValue; without it, init()
  // stays parked until initResponseTimeoutMs.
  // ---------------------------------------------------------------------------

  test("close concurrent with stream opening waits and sends Cancel only after Init") {
    val callStartEntered = new CountDownLatch(1)
    val releaseCallStart = new CountDownLatch(1)
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasCancel) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setCancel(
              CancelResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, underlying) = startServer(service)
    val channel = new DelegatingManagedChannel(underlying) {
      override def newCall[ReqT, RespT](
          methodDescriptor: MethodDescriptor[ReqT, RespT],
          callOptions: CallOptions): ClientCall[ReqT, RespT] =
        new ForwardingClientCall.SimpleForwardingClientCall[ReqT, RespT](
          super.newCall(methodDescriptor, callOptions)) {
          override def start(
              responseListener: ClientCall.Listener[RespT],
              headers: Metadata): Unit = {
            callStartEntered.countDown()
            assert(releaseCallStart.await(10, TimeUnit.SECONDS),
              "timed out waiting to release ClientCall.start")
            super.start(responseListener, headers)
          }
        }
    }
    val session = newSession(channel,
      initResponseTimeoutMs = NeverReachedTimeoutMs, terminalTimeoutMs = NeverReachedTimeoutMs)

    val initThrown = new AtomicReference[Throwable]()
    val initThread = new Thread(() => {
      try session.init(basicInit()) catch { case t: Throwable => initThrown.set(t) }
    }, "init-open-close-race")
    initThread.start()
    assert(callStartEntered.await(5, TimeUnit.SECONDS), "client call never started opening")

    val closeResult = new AtomicReference[Termination]()
    val closeThrown = new AtomicReference[Throwable]()
    val closeDone = new CountDownLatch(1)
    val closeThread = new Thread(() => {
      try closeResult.set(session.close(emptyCancel))
      catch { case t: Throwable => closeThrown.set(t) }
      finally closeDone.countDown()
    }, "close-during-stream-open")
    closeThread.start()

    // The close thread must block on requestLock while ClientCall.start is still
    // opening the RPC. Before the fix it returned through the requestObserver-null
    // path and released the worker while stream creation was still in progress.
    val blockedDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    while (closeThread.getState != Thread.State.BLOCKED && closeDone.getCount != 0 &&
        System.nanoTime() < blockedDeadlineNanos) {
      Thread.sleep(10L)
    }
    assert(closeThread.getState == Thread.State.BLOCKED,
      s"close must wait for stream creation, state=${closeThread.getState}")
    assert(!handleOf(session).released.get(),
      "close must not release the worker while the RPC is still opening")

    releaseCallStart.countDown()
    closeThread.join(10000)
    initThread.join(10000)
    assert(!closeThread.isAlive && !initThread.isAlive,
      "close and init must both finish after the worker receives Cancel")
    assert(closeThrown.get() == null, s"close failed: ${closeThrown.get()}")
    assert(initThrown.get().isInstanceOf[GrpcWorkerSessionException],
      s"init must fail after concurrent close, got: ${initThrown.get()}")
    assert(closeResult.get().isInstanceOf[Termination.Cancelled],
      s"worker acknowledgement should settle Cancelled, got: ${closeResult.get()}")
    val requests = service.captured.asScala.toSeq
    assert(requests.size == 2 && requests.head.getControl.hasInit &&
      requests(1).getControl.hasCancel,
      s"expected exactly Init then Cancel, got: $requests")
  }

  test("outgoing send failure aborts the request side and close does not half-close") {
    val cancelCalls = new AtomicInteger(0)
    val halfCloseCalls = new AtomicInteger(0)
    val failNextSend = new AtomicBoolean(true)
    val service = new CapturingService()
    val (_, underlying) = startServer(service)
    val channel = new DelegatingManagedChannel(underlying) {
      override def newCall[ReqT, RespT](
          methodDescriptor: MethodDescriptor[ReqT, RespT],
          callOptions: CallOptions): ClientCall[ReqT, RespT] =
        new ForwardingClientCall.SimpleForwardingClientCall[ReqT, RespT](
          super.newCall(methodDescriptor, callOptions)) {
          override def sendMessage(message: ReqT): Unit = {
            if (failNextSend.compareAndSet(true, false)) {
              throw new RuntimeException("send boom")
            }
            super.sendMessage(message)
          }

          override def cancel(message: String, cause: Throwable): Unit = {
            cancelCalls.incrementAndGet()
            super.cancel(message, cause)
          }

          override def halfClose(): Unit = {
            halfCloseCalls.incrementAndGet()
            super.halfClose()
          }
        }
    }
    val session = newSession(channel)

    val ex = intercept[GrpcWorkerSessionException] { session.init(basicInit()) }
    assert(ex.getCause != null && ex.getCause.getMessage.contains("send boom"),
      s"the outgoing failure must surface from init, got: $ex")
    assert(cancelCalls.get() == 1,
      "an outgoing onNext failure must terminate the request side with onError")

    val termination = session.close(emptyCancel)
    assert(termination.isInstanceOf[Termination.TransportFailed],
      s"the outgoing failure must settle TransportFailed, got: $termination")
    assert(halfCloseCalls.get() == 0,
      "close must not invoke onCompleted after the request side was aborted")
    assert(cancelCalls.get() == 1,
      "close must not terminate an already-aborted request side again")
  }

  test("close concurrent with blocked init: init is woken and fails fast") {
    val initReceived = new CountDownLatch(1)
    val service = new CapturingService(
      onRequest = (req, resp) => {
        // Accept the Init stream but never reply -- init() blocks awaiting
        // InitResponse. Ignore Cancel too, so the terminator never arrives from
        // the worker and close() must settle the terminal on its own timeout.
        if (req.hasControl && req.getControl.hasInit) {
          initReceived.countDown()
        }
      })
    val (_, channel) = startServer(service)
    // Huge initResponseTimeoutMs: a regression that fails to wake init() on the
    // close-settled terminal would park it here for 10 minutes, so finishing
    // within the join below is the proof it was woken -- no wall-clock threshold.
    // Small terminalTimeoutMs: close() gives up waiting for the (never-arriving)
    // CancelResponse quickly and settles TransportFailed itself.
    val session = newSession(channel,
      initResponseTimeoutMs = NeverReachedTimeoutMs, terminalTimeoutMs = 1000L)

    val thrown = new AtomicReference[Throwable]()
    val initThread = new Thread(() => {
      try session.init(basicInit()) catch { case t: Throwable => thrown.set(t) }
    }, "blocked-init")
    initThread.start()
    assert(initReceived.await(5, TimeUnit.SECONDS),
      "worker never received the Init request")

    // close() from the test thread settles the terminal (TransportFailed, after
    // terminalTimeoutMs of no CancelResponse), which must wake the parked init().
    session.close(emptyCancel)
    initThread.join(10000)
    assert(!initThread.isAlive,
      "init() parked on initResponseTimeoutMs; the close-settled terminal did not " +
        "wake it (onTerminalSettled must signal initValue)")
    val t = thrown.get()
    assert(t != null, "init() must surface an init-failure exception")
    assert(t.isInstanceOf[GrpcWorkerSessionException],
      s"expected GrpcWorkerSessionException, got ${if (t == null) "null" else t.getClass.getName}")
  }

  // ---------------------------------------------------------------------------
  // Terminator-carried callback errors: a FinishResponse / CancelResponse may
  // carry an error raised by the finish / cancel callback (udf_message.proto).
  // The result iterator must surface it, and a prior data-phase ErrorResponse
  // must take precedence over the terminator's own callback error
  // (throwIfTerminalError / responseError).
  // ---------------------------------------------------------------------------

  test("FinishResponse carrying a callback error: iterator throws it") {
    // Worker echoes the single batch, then finishes with a FinishResponse whose
    // error field is set (a finish-callback failure). No data-phase ErrorResponse
    // precedes it, so the terminator's own error is the one surfaced.
    val service = new CapturingService(
      onRequest = (req, resp) => {
        req.getRequestCase match {
          case UdfRequest.RequestCase.CONTROL =>
            val c = req.getControl
            if (c.hasInit) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setInit(
                  InitResponse.getDefaultInstance).build()).build())
            } else if (c.hasFinish) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setFinish(
                  FinishResponse.newBuilder().setError(
                    ExecutionError.newBuilder().setUser(
                      UserError.newBuilder().setMessage("finish callback boom")
                        .setErrorClass("FinishCallbackError").build()).build())
                    .build()).build()).build())
            }
          case UdfRequest.RequestCase.DATA =>
            resp.onNext(UdfResponse.newBuilder()
              .setData(DataResponse.newBuilder().setData(req.getData.getData).build())
              .build())
          case _ => ()
        }
      })
    val (_, channel) = startServer(service, directExecutor = false)
    val session = newSession(channel, terminalTimeoutMs = 5000L)
    session.init(basicInit())

    val it = session.process(echoIn("hello"), emptyFinish)
    val ex = intercept[GrpcWorkerSessionException] { it.foreach(_ => ()) }
    assert(ex.getMessage.contains("finish callback boom"),
      s"the FinishResponse callback error must surface, got: ${ex.getMessage}")
    assert(ex.executionError != null &&
      ex.executionError.getUser.getErrorClass == "FinishCallbackError",
      "the structured finish-callback error should be preserved")
    session.close(emptyCancel)
  }

  test("data-phase ErrorResponse takes precedence over a CancelResponse callback error") {
    // Worker replies to the first DataRequest with an ErrorResponse (data-phase
    // failure), then -- per the protocol, the engine sends Cancel -- answers with
    // a CancelResponse that ALSO carries a (cancel-callback) error. The iterator
    // must surface the original data-phase error, not the terminator's, per
    // responseError's precedence rule.
    val erroredOnce = new AtomicBoolean(false)
    val service = new CapturingService(
      onRequest = (req, resp) => {
        req.getRequestCase match {
          case UdfRequest.RequestCase.CONTROL =>
            val c = req.getControl
            if (c.hasInit) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setInit(
                  InitResponse.getDefaultInstance).build()).build())
            } else if (c.hasCancel) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setCancel(
                  CancelResponse.newBuilder().setError(
                    ExecutionError.newBuilder().setUser(
                      UserError.newBuilder().setMessage("cancel callback boom")
                        .setErrorClass("CancelCallbackError").build()).build())
                    .build()).build()).build())
            }
          case UdfRequest.RequestCase.DATA =>
            if (erroredOnce.compareAndSet(false, true)) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setError(
                  ErrorResponse.newBuilder().setError(
                    ExecutionError.newBuilder().setUser(
                      UserError.newBuilder().setMessage("data-phase boom")
                        .setErrorClass("DataPhaseError").build()).build()).build())
                  .build()).build())
            }
          case _ => ()
        }
      })
    val (_, channel) = startServer(service, directExecutor = false)
    val session = newSession(channel, terminalTimeoutMs = 5000L)
    session.init(basicInit())

    val it = session.process(echoIn("a", "b", "c"), emptyFinish)
    val ex = intercept[GrpcWorkerSessionException] { it.foreach(_ => ()) }
    assert(ex.getMessage.contains("data-phase boom"),
      s"the prior data-phase error must take precedence, got: ${ex.getMessage}")
    assert(ex.executionError != null &&
      ex.executionError.getUser.getErrorClass == "DataPhaseError",
      s"expected the data-phase error to be preserved, got: ${ex.executionError}")
    session.close(emptyCancel)
  }

  // ---------------------------------------------------------------------------
  // Data-phase output timeout: after Finish, if the worker goes silent without a
  // terminator, advance()'s per-poll wait (terminalTimeoutMs) must expire and the
  // iterator must surface a TransportFailed rather than block forever.
  // ---------------------------------------------------------------------------

  test("data-phase output timeout: silent worker after Finish surfaces a timeout") {
    // Worker accepts Init and echoes data, but never sends a terminator and
    // ignores Finish/Cancel -- so once input is exhausted the iterator's output
    // poll has nothing to drain and must time out on terminalTimeoutMs.
    val service = new CapturingService(
      onRequest = (req, resp) => {
        req.getRequestCase match {
          case UdfRequest.RequestCase.CONTROL =>
            val c = req.getControl
            if (c.hasInit) {
              resp.onNext(UdfResponse.newBuilder().setControl(
                UdfControlResponse.newBuilder().setInit(
                  InitResponse.getDefaultInstance).build()).build())
            }
          // Finish and Cancel ignored: no terminator ever arrives.
          case UdfRequest.RequestCase.DATA =>
            resp.onNext(UdfResponse.newBuilder()
              .setData(DataResponse.newBuilder().setData(req.getData.getData).build())
              .build())
          case _ => ()
        }
      })
    val (_, channel) = startServer(service, directExecutor = false)
    // Small terminalTimeoutMs: the poll gives up quickly; assertFinishesWithin
    // guards against a regression that blocks the iterator indefinitely.
    val session = newSession(channel, terminalTimeoutMs = 1000L)
    session.init(basicInit())

    val it = session.process(echoIn("hello"), emptyFinish)
    assertFinishesWithin(10000, "output-timeout") {
      val ex = intercept[GrpcWorkerSessionException] { it.foreach(_ => ()) }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("timed out") ||
        ex.getMessage.toLowerCase(Locale.ROOT).contains("stream failed"),
        s"expected a timeout/transport-failure message, got: ${ex.getMessage}")
    }
    session.close(emptyCancel)
  }

  // ---------------------------------------------------------------------------
  // Interrupt handling (cancelled query / killed task). An interrupt while
  // blocked on init or pulling input sends a best-effort Cancel and waits
  // interruptCancelTimeoutMs for the CancelResponse: a responsive worker settles
  // a clean, salvageable Cancelled; an unresponsive one falls back to Interrupted
  // and is invalidated because no acknowledgement or liveness proof makes it safe
  // to reuse.
  // ---------------------------------------------------------------------------

  test("interrupt during init: responsive worker settles Cancelled, worker salvageable") {
    // Worker never sends InitResponse (so init() blocks), but DOES reply to
    // Cancel -- so the interrupt's bounded drain observes the CancelResponse and
    // settles a clean Cancelled.
    val initReceived = new CountDownLatch(1)
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          initReceived.countDown() // block: no InitResponse
        } else if (req.hasControl && req.getControl.hasCancel) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setCancel(
              CancelResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, channel) = startServer(service, directExecutor = false)
    // Huge initResponseTimeoutMs so only the interrupt can end the init wait;
    // generous interruptCancelTimeoutMs so the ack is observed.
    val session = newSession(channel,
      initResponseTimeoutMs = NeverReachedTimeoutMs, interruptCancelTimeoutMs = 5000L)
    val handle = handleOf(session)

    val thrown = new AtomicReference[Throwable]()
    val initThread = new Thread(() => {
      try session.init(basicInit()) catch { case t: Throwable => thrown.set(t) }
    }, "interrupt-init-responsive")
    initThread.start()
    assert(initReceived.await(5, TimeUnit.SECONDS), "worker never received Init")
    initThread.interrupt()
    initThread.join(10000)
    assert(!initThread.isAlive, "init() must return after the interrupt")
    assert(thrown.get().isInstanceOf[InterruptedException],
      s"init() must rethrow InterruptedException, got: ${thrown.get()}")

    val termination = session.close(emptyCancel)
    assert(termination.isInstanceOf[Termination.Cancelled],
      s"a responsive worker's ack should settle Cancelled, got: $termination")
    assert(!handle.invalidated.get(),
      "an acknowledged Cancelled outcome is salvageable; the worker must not be invalidated")
    assert(handle.released.get(), "close() must release the worker handle")
  }

  test("interrupt during init: unresponsive worker settles Interrupted and is invalidated") {
    // Worker never sends InitResponse and ignores Cancel -- so the interrupt's
    // bounded drain times out and the session falls back to Interrupted.
    val initReceived = new CountDownLatch(1)
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          initReceived.countDown() // block: no InitResponse, and Cancel ignored
        }
      })
    val (_, channel) = startServer(service, directExecutor = false)
    // Short interruptCancelTimeoutMs so the fallback fires quickly; huge
    // terminalTimeoutMs so a regression that used the wrong timeout would stall.
    val session = newSession(channel,
      initResponseTimeoutMs = NeverReachedTimeoutMs,
      terminalTimeoutMs = NeverReachedTimeoutMs,
      interruptCancelTimeoutMs = 500L)
    val handle = handleOf(session)

    val thrown = new AtomicReference[Throwable]()
    val initThread = new Thread(() => {
      try session.init(basicInit()) catch { case t: Throwable => thrown.set(t) }
    }, "interrupt-init-unresponsive")
    initThread.start()
    assert(initReceived.await(5, TimeUnit.SECONDS), "worker never received Init")
    initThread.interrupt()
    // Bounded by interruptCancelTimeoutMs (500ms), not terminalTimeoutMs (10min):
    // if init parked on the wrong timeout this join would fail.
    initThread.join(10000)
    assert(!initThread.isAlive,
      "init() must return within interruptCancelTimeoutMs of the interrupt")
    assert(thrown.get().isInstanceOf[InterruptedException],
      s"init() must rethrow InterruptedException, got: ${thrown.get()}")

    val termination = session.close(emptyCancel)
    assert(termination.isInstanceOf[Termination.Interrupted],
      s"an unresponsive worker should settle Interrupted, got: $termination")
    assert(handle.invalidated.get(),
      "an unacknowledged Interrupted outcome must invalidate the worker")
    assert(handle.released.get(), "close() must release the worker handle")
  }

  test("interrupt while pulling input: bounded Cancel drain settles Interrupted") {
    // Worker accepts Init but ignores Cancel. The interrupted input pull must use
    // interruptCancelTimeoutMs rather than leaving close() to wait terminalTimeoutMs.
    val service = new CapturingService(
      onRequest = (req, resp) => {
        if (req.hasControl && req.getControl.hasInit) {
          resp.onNext(UdfResponse.newBuilder().setControl(
            UdfControlResponse.newBuilder().setInit(
              InitResponse.getDefaultInstance).build()).build())
        }
      })
    val (_, channel) = startServer(service)
    val session = newSession(channel,
      terminalTimeoutMs = NeverReachedTimeoutMs,
      interruptCancelTimeoutMs = 500L)
    val handle = handleOf(session)
    session.init(basicInit())

    val inputNextEntered = new CountDownLatch(1)
    val neverReleaseInput = new CountDownLatch(1)
    val input = new Iterator[DataRequest] {
      override def hasNext: Boolean = true
      override def next(): DataRequest = {
        inputNextEntered.countDown()
        neverReleaseInput.await()
        throw new IllegalStateException("blocked input unexpectedly resumed")
      }
    }
    val thrown = new AtomicReference[Throwable]()
    val termination = new AtomicReference[Termination]()
    val processThread = new Thread(() => {
      try {
        session.process(input, emptyFinish).hasNext
      } catch {
        case t: Throwable => thrown.set(t)
      } finally {
        termination.set(session.close(emptyCancel))
      }
    }, "interrupt-input-pull")
    processThread.setDaemon(true)
    processThread.start()
    assert(inputNextEntered.await(5, TimeUnit.SECONDS), "input.next() was never entered")

    processThread.interrupt()
    // Bounded by interruptCancelTimeoutMs (500ms), not terminalTimeoutMs (10min).
    processThread.join(10000)
    assert(!processThread.isAlive,
      "input interruption must unwind within interruptCancelTimeoutMs")
    assert(thrown.get().isInstanceOf[InterruptedException],
      s"processing must rethrow InterruptedException, got: ${thrown.get()}")
    assert(termination.get().isInstanceOf[Termination.Interrupted],
      s"an unresponsive worker should settle Interrupted, got: ${termination.get()}")
    assert(service.captured.asScala.exists(r => r.hasControl && r.getControl.hasCancel),
      "interrupting an input pull must send Cancel")
    assert(handle.invalidated.get(),
      "an unacknowledged Interrupted outcome must invalidate the worker")
    assert(handle.released.get(), "close() must release the worker handle")
  }
}
