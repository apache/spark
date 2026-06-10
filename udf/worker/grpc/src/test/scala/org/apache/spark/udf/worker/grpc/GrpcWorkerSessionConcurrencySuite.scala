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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, Server}
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
import org.apache.spark.udf.worker.grpc.testing.EchoWorkerService

/**
 * Concurrency tests for [[GrpcWorkerSession]] that pin the wire-ordering and
 * fast-fail invariants under concurrent and worker-misbehavior scenarios:
 *  - Cancel must never appear on the wire before Init.
 *  - A worker terminator (ERROR / FINISH / CANCEL / onCompleted) arriving
 *    before InitResponse must fail [[GrpcWorkerSession#init]] fast, not hang
 *    for `initResponseTimeoutMs`.
 *  - Repeated [[Iterator#hasNext]] after natural iterator exhaustion must
 *    return immediately, not block for `terminalTimeoutMs`.
 *  - Cancel issued before [[GrpcWorkerSession#init]] makes a subsequent
 *    [[GrpcWorkerSession#doProcess]] surface the cancellation immediately,
 *    instead of blocking for `terminalTimeoutMs`.
 *  - Cancel / close concurrent with an in-progress data phase terminate
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

  private def newSession(
      channel: ManagedChannel,
      initResponseTimeoutMs: Long = 5000L,
      terminalTimeoutMs: Long = 5000L): GrpcWorkerSession = {
    val session = new GrpcWorkerSession(
      new TestWorkerHandle, channel, WorkerLogger.NoOp,
      initResponseTimeoutMs = initResponseTimeoutMs,
      terminalTimeoutMs = terminalTimeoutMs)
    openSessions.add(session)
    session
  }

  private def basicInit(payload: String = "echo"): Init = Init.newBuilder()
    .setProtocolVersion(EchoWorkerService.SupportedVersion)
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
  // Cancel-never-precedes-Init invariant.
  //
  // The former cancel()-vs-init() race test was retired when cancel() folded
  // into close(): close() transitions the session to Closed and blocks init(),
  // so a Cancel-before-Init race is no longer reachable through the public API
  // (close() almost always wins and aborts init() before any control is sent,
  // making such a race test vacuous). The invariant is now structural --
  // sendCancelInternal early-returns while requestObserver is null, and
  // requestObserver is published only after Init is on the wire -- and is
  // exercised by "worker emits InitResponse with error: init fails fast" (the
  // requestObserver-null path: a Cancel that cannot be written) and by "close
  // concurrent with process" (a Cancel written only after Init + data).
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // Terminator-before-InitResponse: must fail init fast, not hang for
  // initResponseTimeoutMs (defensive initLatch.countDown() in handleControl).
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
    // Huge init timeout: a regression that dropped the defensive
    // initLatch.countDown() would park init here until the timeout, so finishing
    // quickly is proof the fast path ran -- no wall-clock threshold needed.
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
    // Regression for the directExecutor reentrancy where InitResponse(error) is
    // delivered inside stream.onNext, before doInit publishes requestObserver,
    // so the in-band Cancel cannot be written. The INIT-error path must settle a
    // terminal itself rather than block awaiting a CancelResponse that can never
    // arrive (which would stall for the full terminalTimeoutMs).
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
    session.close(emptyCancel)
  }

  // ---------------------------------------------------------------------------
  // close() reports failures faithfully (Termination.Failed / TransportFailed)
  // rather than collapsing them into a clean Cancelled, so a failure that occurs
  // during finish/close -- where nothing is consuming the result iterator -- is
  // not lost.
  // ---------------------------------------------------------------------------

  test("close after a pre-init error returns a Failed termination carrying the error") {
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
        }
      })
    val (_, channel) = startServer(service)
    val session = newSession(channel, terminalTimeoutMs = 3000L)
    intercept[GrpcWorkerSessionException](session.init(basicInit()))

    val termination = session.close(emptyCancel)
    termination match {
      case Termination.Failed(err) =>
        assert(err.getUser.getErrorClass == "PreInitError",
          s"the structured error must be carried on the Termination, got: $err")
      case other =>
        fail(s"expected Termination.Failed carrying the error, got: $other")
    }
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

    val termination = session.close(emptyCancel)
    assert(termination.isInstanceOf[Termination.TransportFailed],
      s"a close that times out without a terminator must report TransportFailed, got: $termination")
  }
}
