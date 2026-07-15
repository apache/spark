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

import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.util.control.NonFatal

import io.grpc.{ConnectivityState, ManagedChannel}
import io.grpc.stub.StreamObserver

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.{Cancel, CancelResponse, DataRequest, DataResponse,
  ExecutionError, Finish, FinishResponse, Init, InitResponse, UdfControlRequest,
  UdfControlResponse, UdfRequest, UdfResponse, UdfWorkerGrpc}
import org.apache.spark.udf.worker.core.{Termination, WorkerHandle, WorkerLogger, WorkerSession}
import org.apache.spark.udf.worker.core.WorkerSession.SessionState
import org.apache.spark.udf.worker.grpc.GrpcWorkerSession._

/**
 * :: Experimental ::
 * gRPC implementation of [[WorkerSession]] for the `UdfWorker.Execute`
 * bidirectional RPC.
 *
 * Drives one bidirectional `Execute` stream against the worker per the
 * ordering invariants documented in `udf_message.proto`:
 * {{{
 *   Engine -> Worker:  Init -> (DataRequest)* -> Finish (Cancel)?
 *                                              | Cancel
 *   Worker -> Engine:  InitResponse -> (DataResponse)* ->
 *                      (ErrorResponse)? -> (FinishResponse | CancelResponse)
 * }}}
 *
 * Knows nothing about how the worker was provisioned (locally spawned,
 * indirectly looked up, ...) -- the dispatcher constructs this with a
 * [[WorkerHandle]] and channel; the base [[WorkerSession]] handles
 * dispatcher-side cleanup on close.
 *
 * '''Driving model.''' Consumption-driven (Volcano / pull): the thread that
 * consumes the [[doProcess]] result iterator is the one that pulls input and
 * sends each `DataRequest`; the gRPC callback thread only receives output. It is
 * pull-driven but not one-input-per-output -- `advance` sends the next input
 * whenever the output queue is momentarily empty, so under async delivery it may
 * push several input batches before any output is read (bounded by HTTP/2 flow
 * control).
 *
 * '''State machine.''' This class does not keep its own state machine: it
 * drives the single [[WorkerSession.SessionState]] owned by the base. The base
 * advances `Created -> Initializing` (in `init`) and `Initialized -> Streaming`
 * (in `process`); this class advances the protocol-event edges through
 * [[compareAndSetState]] / [[completeTerminal]] as it exchanges messages:
 * {{{
 *   Initializing --(InitResponse ok)--> Initialized   [handleControl]
 *   Streaming ----(Finish written)----> Finishing      [ProcessIterator]
 *   <any non-terminal> --(Cancel written)--> Cancelling [sendCancelInternal]
 *   <any non-terminal> --(terminator/error)--> terminal [completeTerminal]
 * }}}
 * The two clean terminals carry the worker's `FinishResponse` / `CancelResponse`
 * (metrics + finish/cancel callback `data`/`error`) so [[close]] can return
 * them. The only flag kept outside the machine is [[cancelRequested]]: a
 * cancellation can be requested before the stream exists (so it cannot be a
 * state transition yet), and it must both fast-fail the result iterator and
 * suppress any in-flight Data/Finish.
 *
 * Threading:
 *  - [[doInit]] is synchronous: sends `Init` and blocks on `InitResponse`,
 *    returning it.
 *  - [[doProcess]] returns an iterator. Input batches are forwarded inline
 *    (the iterator's `next()` thread also sends `DataRequest`). Output
 *    batches arrive via the response observer (gRPC callback thread) and
 *    are consumed by the same iterator. A terminator (`FinishResponse`,
 *    `CancelResponse`, `ErrorResponse`, gRPC stream error) is published
 *    once.
 *  - [[doClose]] is thread-safe and idempotent: it settles + returns the
 *    terminator (cancelling in-flight work if the stream had not finished)
 *    and half-closes the request side.
 *
 * TODO [SPARK-55278]: this class does not yet implement payload chunking;
 * the entire [[Init.udf]] payload is sent inline. Chunking will be added
 * when a UDF payload large enough to exceed gRPC's default message size
 * limit is introduced.
 *
 * @param workerHandle dispatcher-side handle for releasing the worker on
 *                     [[close]] (see [[WorkerSession]]).
 * @param channel      a gRPC channel built and owned by the caller (the
 *                     dispatcher). Not closed here -- the dispatcher tears it
 *                     down via [[WorkerHandle]].
 * @param logger       diagnostics. Defaults to [[WorkerLogger.NoOp]].
 * @param initResponseTimeoutMs upper bound on the wait for `InitResponse`
 *                              after [[doInit]] sends `Init`.
 * @param terminalTimeoutMs     upper bound on the wait for a stream
 *                              terminator (`FinishResponse`,
 *                              `CancelResponse`, or `ErrorResponse`).
 *                              Each output-queue poll resets this wait;
 *                              see [[doProcess]] / `ProcessIterator`.
 */
@Experimental
class GrpcWorkerSession(
    workerHandle: WorkerHandle,
    channel: ManagedChannel,
    logger: WorkerLogger = WorkerLogger.NoOp,
    initResponseTimeoutMs: Long = DEFAULT_INIT_RESPONSE_TIMEOUT_MS,
    terminalTimeoutMs: Long = DEFAULT_TERMINAL_TIMEOUT_MS)
  extends WorkerSession(workerHandle, logger) {

  require(channel != null, "channel is required")

  private val asyncStub = UdfWorkerGrpc.newStub(channel)

  // Output batches from the worker, drained by the process() iterator.
  // Intentionally unbounded: a bounded queue would block the gRPC callback
  // (Netty event-loop) thread when full, stalling terminator/control delivery on
  // the whole channel. HTTP/2 flow control bounds the wire and the consumer
  // normally drains promptly; a stalled downstream can still grow it, but the fix
  // is protocol-level back-pressure (out of scope), not bounding the queue.
  //
  // TODO [SPARK-57324]: expose queue depth as a metric (early warning for a
  // stalled consumer).
  private val outputQueue = new LinkedBlockingQueue[QueueItem]()

  // The worker's `InitResponse` (success or error) coupled with the latch that
  // init() blocks on. Fires when the InitResponse arrives (`complete`) or when a
  // terminal settles first without one -- transport error, half-close, or a
  // premature terminator (`signalWithoutValue`). Until it fires we have no proof
  // the worker accepted the session.
  //
  // Settle-before-release rule (referenced from every callback that both settles
  // a terminal and fires this latch): settle the terminal FIRST, then complete /
  // signal. init() blocks on the latch, and only latch await/release establish a
  // happens-before edge, so a woken init() is guaranteed to observe the terminal
  // rather than a transient state. OneShotValue keeps that publish-then-release
  // in one place instead of every caller remembering to count down after setting
  // the reference.
  private val initValue = new OneShotValue[InitResponse]

  // Fired when the session reaches a terminal [[SessionState]]. doClose() and
  // the init-error path block on this to drain the terminator.
  private val terminalLatch = new CountDownLatch(1)

  // Captures an ErrorResponse encountered during the data phase so that
  // the CancelResponse terminator can attribute the failure to the original
  // user / worker / protocol error rather than reporting a bare "Cancelled".
  private val executionError = new AtomicReference[Option[ExecutionError]](None)

  // Cancellation INTENT -- distinct from the `Cancelling` state, which is reached
  // only once a Cancel is actually written to the wire ([[sendCancelInternal]]).
  // Intent is set first and can outrun (or never reach) that write, so
  // `cancelRequested` does NOT imply state `Cancelling`. Kept outside the
  // [[SessionState]] machine because a cancel can be requested before the stream
  // exists (pre-init), where there is no wire transition to make yet. Used to (a)
  // make cancel idempotent across all call sites, (b) fast-fail ProcessIterator
  // on a pre-init cancel, and (c) suppress any Data/Finish that would otherwise
  // race a Cancel onto the wire (re-read inside [[requestLock]]).
  private val cancelRequested = new AtomicBoolean(false)

  // gRPC requires serialized writes to a request StreamObserver.
  private val requestLock = new Object

  // Initialised in init() -- before that, close() is a no-op on the request
  // side, which is exactly the contract the wrapping WorkerSession expects.
  @volatile private var requestObserver: StreamObserver[UdfRequest] = _

  private val responseObserver: StreamObserver[UdfResponse] = new StreamObserver[UdfResponse] {
    override def onNext(response: UdfResponse): Unit = {
      response.getResponseCase match {
        case UdfResponse.ResponseCase.DATA =>
          // A DataResponse before InitResponse violates the protocol (InitResponse
          // must precede any DataResponse): fast-fail rather than enqueue it and let
          // init() block to its timeout. Settle-before-release (see initValue).
          if (!initResolved) {
            Transitions.transportFailed(new IllegalStateException(
              "worker sent a DataResponse before InitResponse"))
            initValue.signalWithoutValue()
          } else {
            outputQueue.put(QueueItem.Batch(response.getData))
          }

        case UdfResponse.ResponseCase.CONTROL =>
          handleControl(response.getControl)

        case other =>
          // A malformed response (empty / unknown oneof) is a terminal transport
          // failure; fast-fail init the same way. Settle-before-release (see initValue).
          Transitions.transportFailed(new IllegalStateException(
            s"unexpected response oneof: $other"))
          initValue.signalWithoutValue()
      }
    }

    override def onError(t: Throwable): Unit = {
      // Transport-level failure: the stream is dead, no further writes possible.
      // Settle-before-release (see initValue) so init() surfaces the transport
      // cause instead of the initResponseTimeoutMs "timed out" error.
      Transitions.transportFailed(t)
      initValue.signalWithoutValue()
    }

    override def onCompleted(): Unit = {
      // Worker half-closed its side without sending a terminator (FinishResponse
      // / CancelResponse). Treat as transport error so the engine sees a
      // failure, not a silent end-of-stream.
      if (!currentState.isTerminal) {
        Transitions.transportFailed(new IllegalStateException(
          "worker response stream closed without a terminator"))
      }
      // Defensive: if onCompleted reached us before InitResponse, doInit is
      // still blocked on initValue and would otherwise time out.
      initValue.signalWithoutValue()
    }
  }

  /**
   * Wakes the result iterator (blocked on [[outputQueue]]) and any thread
   * waiting on [[terminalLatch]] when the base settles a terminal. Invoked once,
   * by the caller that wins [[completeTerminal]].
   */
  override protected def onTerminalSettled(termination: Termination): Unit = {
    outputQueue.put(QueueItem.EndOfStream)
    terminalLatch.countDown()
  }

  private def handleControl(ctrl: UdfControlResponse): Unit = ctrl.getControlCase match {
    case UdfControlResponse.ControlCase.INIT =>
      val resp = ctrl.getInit
      if (resp.hasError) {
        // Attribute a subsequent close()'s terminator to the init error rather
        // than a bare Cancelled, mirroring the data-phase ERROR branch.
        executionError.compareAndSet(None, Some(resp.getError))
        // Settle Failed(err) before the Cancel below (not after its CancelResponse):
        // the sticky terminal makes close() report the init error, and the Cancel
        // becomes best-effort worker cleanup -- so if it can't be written (a
        // directExecutor worker delivered this reentrantly, before doInit published
        // requestObserver), doInit still doesn't wait terminalTimeoutMs for a
        // CancelResponse that can never arrive. Settle-before-release (see initValue).
        Transitions.failed(resp.getError)
        initValue.complete(resp)
        sendCancelInternal(() => cancelWithReason("init failed"))
      } else {
        // InitResponse OK. Only advance from Initializing so a terminal that raced
        // in (e.g. a transport error) still wins; process() then opens the data
        // phase. Settle-before-release (see initValue): publish after the CAS.
        Transitions.initAccepted()
        initValue.complete(resp)
      }

    case UdfControlResponse.ControlCase.ERROR =>
      val err = ctrl.getError.getError
      executionError.compareAndSet(None, Some(err))
      // Before init resolves, settle Failed here rather than rely on the Cancel
      // below: sendCancelInternal may find requestObserver still null (reentrant
      // delivery inside stream.onNext, before doInit published it), so no Cancel --
      // hence no CancelResponse -- would settle the terminal, and doInit would
      // return as if init succeeded. completeTerminal is idempotent, so the
      // data-phase ERROR -> Cancel -> CancelResponse path is unaffected.
      if (!initResolved) {
        Transitions.failed(err)
      }
      // Settle-before-release (see initValue). No value to publish -- ERROR is not
      // an InitResponse; on the data-phase path the latch already fired in init.
      initValue.signalWithoutValue()
      sendCancelInternal(() => cancelWithReason("aborting after ErrorResponse"))

    case UdfControlResponse.ControlCase.FINISH =>
      // The FinishResponse carries metrics + the finish-callback data/error.
      // Keep it on the terminal so close() can return it; the iterator inspects
      // its error field to decide whether to throw.
      Transitions.finished(ctrl.getFinish)
      // Defensive: FINISH before InitResponse is a worker protocol bug, but
      // we should fail init fast rather than hang the 30s init timeout.
      initValue.signalWithoutValue()

    case UdfControlResponse.ControlCase.CANCEL =>
      // The CancelResponse carries metrics + the cancel-callback error. Keep it
      // on the terminal so close() can return it; any prior ErrorResponse is
      // tracked in executionError and surfaced by the iterator.
      Transitions.cancelled(ctrl.getCancel)
      // Defensive: CANCEL before InitResponse unblocks doInit so it can
      // surface the cancellation instead of timing out.
      initValue.signalWithoutValue()

    case UdfControlResponse.ControlCase.CONTROL_NOT_SET =>
      Transitions.transportFailed(new IllegalStateException(
        "empty UdfControlResponse oneof"))
      initValue.signalWithoutValue()
  }

  /**
   * True once init is no longer pending -- i.e. the stream is past `Initializing`.
   * Not "init succeeded": a terminal (including a failure) also counts as resolved.
   */
  private def initResolved: Boolean = currentState match {
    case SessionState.Created | SessionState.Initializing => false
    case _ => true
  }

  private def cancelWithReason(reason: String): Cancel =
    Cancel.newBuilder().setReason(reason).build()

  /**
   * The protocol transition graph in one place: names for the edges, not a
   * second source of truth. Every edge acts on the single
   * [[WorkerSession.SessionState]] owned by the base via [[compareAndSetState]]
   * (non-terminal) or [[completeTerminal]] (terminal), so the base's CAS is the
   * only synchronization and a terminal that arrived first always wins (the
   * non-terminal CASes fail against it; [[completeTerminal]] is first-wins).
   *
   * Edges this class drives -- edge, method, then driver site(s) / thread (the
   * base drives the API-call edges: `Created -> Initializing` in `init`,
   * `Initialized -> Streaming` in `process`):
   * {{{
   *   Initializing -> Initialized     initAccepted    handleControl INIT-ok  [gRPC cb]
   *   Streaming -> Finishing          finishSent      advance branch 3       [engine]
   *   non-terminal -> Cancelling      cancelSentFrom  sendCancelInternal, iff a Cancel
   *                                     reaches the wire  [gRPC cb | engine | init | close]
   *   non-terminal -> Terminal        finished/cancelled/failed/transportFailed
   *                                     handleControl / doInit / doClose / advance / onError
   * }}}
   * `Cancelling` is reached only if a Cancel is actually written; a pre-stream or
   * raced cancel goes straight to a `Cancelled`/`TransportFailed` terminal (see
   * [[sendCancelInternal]], [[doClose]], `ProcessIterator`) or nowhere -- so
   * `cancelRequested` (intent) does not imply state `Cancelling`.
   */
  private object Transitions {
    /** `InitResponse` OK: `Initializing -> Initialized`. */
    def initAccepted(): Boolean =
      compareAndSetState(SessionState.Initializing, SessionState.Initialized)

    /** Input exhausted and `Finish` written: `Streaming -> Finishing` (once). */
    def finishSent(): Boolean =
      compareAndSetState(SessionState.Streaming, SessionState.Finishing)

    /** `Cancel` written: `cur -> Cancelling`, from any non-terminal `cur`. */
    def cancelSentFrom(cur: SessionState): Boolean =
      !cur.isTerminal && compareAndSetState(cur, SessionState.Cancelling)

    /** Clean terminal carrying the worker's `FinishResponse`. */
    def finished(response: FinishResponse): Boolean =
      completeTerminal(Termination.Finished(response))

    /** Clean terminal carrying the worker's `CancelResponse`. */
    def cancelled(response: CancelResponse): Boolean =
      completeTerminal(Termination.Cancelled(response))

    /** Failure terminal carrying a structured [[ExecutionError]]. */
    def failed(error: ExecutionError): Boolean =
      completeTerminal(Termination.Failed(error))

    /** Failure terminal carrying a transport-level cause. */
    def transportFailed(cause: Throwable): Boolean =
      completeTerminal(Termination.TransportFailed(cause))
  }

  // ---- WorkerSession hooks ------------------------------------------------

  override protected def doInit(message: Init): InitResponse = {
    // Fail fast if the channel is already shut down. Without this check,
    // asyncStub.execute(...) would still succeed and the failure would
    // surface ~initResponseTimeoutMs later as a misleading "InitResponse
    // timed out" error.
    if (channel.getState(false) == ConnectivityState.SHUTDOWN) {
      val ex = new IllegalStateException("gRPC channel is shut down")
      Transitions.transportFailed(ex)
      throw new GrpcWorkerSessionException("UDF worker channel is closed", ex)
    }
    // Open the stream as a local first; only publish `requestObserver`
    // AFTER the Init has been put on the wire. close()'s cancel reads
    // `requestObserver` outside [[requestLock]] and returns early when
    // it is null, so this ordering prevents a concurrent cancel from
    // sneaking a Cancel ahead of Init.
    val stream = asyncStub.execute(responseObserver)
    val initRequest = UdfRequest.newBuilder()
      .setControl(UdfControlRequest.newBuilder().setInit(message).build())
      .build()
    try {
      requestLock.synchronized {
        // Reentrant delivery: a directExecutor worker (one whose gRPC callbacks run
        // synchronously on the caller's thread) can deliver InitResponse -- and thus
        // run responseObserver/handleControl -- from *inside* this stream.onNext,
        // before requestObserver below is published. The base advanced
        // Created -> Initializing before calling doInit, so that reentrant
        // InitResponse still finds Initializing and advances to Initialized; the
        // handleControl error paths guard the still-null requestObserver. This is
        // the scenario the "before doInit published requestObserver" comments mean.
        stream.onNext(initRequest)
        requestObserver = stream
      }
    } catch {
      case NonFatal(e) =>
        // Expose the stream so a subsequent close() can still attempt a
        // best-effort half-close; the terminal already reflects the failure.
        requestObserver = stream
        Transitions.transportFailed(e)
        // Surface as GrpcWorkerSessionException so the engine integration layer
        // (which catches that type and wraps it) sees a uniform init-failure
        // exception rather than the raw transport error.
        throw new GrpcWorkerSessionException("UDF worker stream failed during init", e)
    }

    val responded = try {
      initValue.await(initResponseTimeoutMs)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        sendCancelInternal(() => cancelWithReason("interrupted during init"))
        // Make sure close() does not block waiting for a terminator that
        // will never arrive on this thread's behalf.
        Transitions.transportFailed(
          new InterruptedException("interrupted while waiting for InitResponse"))
        throw new InterruptedException("interrupted while waiting for InitResponse")
    }

    if (!responded) {
      sendCancelInternal(() => cancelWithReason("InitResponse timed out"))
      val timeout = new TimeoutException(
        s"timed out waiting for InitResponse after ${initResponseTimeoutMs}ms")
      // Settle the terminal so a subsequent close() does not stall for a
      // second `terminalTimeoutMs` waiting for a worker that already missed
      // its init deadline.
      Transitions.transportFailed(timeout)
      // Surface as GrpcWorkerSessionException (carrying the timeout cause) so
      // the engine integration layer that catches that type can wrap it.
      throw new GrpcWorkerSessionException(
        s"timed out waiting for InitResponse after ${initResponseTimeoutMs}ms", timeout)
    }

    initValue.get match {
      case Some(resp) if resp.hasError =>
        // The protocol requires the engine to send Cancel after an init
        // error and the worker to respond with CancelResponse. Drain it
        // before throwing so we don't leave a dangling stream.
        awaitTerminal()
        throw new GrpcWorkerSessionException(
          s"UDF worker init failed: ${describeError(resp.getError)}", resp.getError)
      case Some(resp) =>
        resp
      case None =>
        // No InitResponse arrived but the latch fired. The defensive
        // initValue.signalWithoutValue() in handleControl / onError /
        // onCompleted means we get here when the worker terminated the stream
        // before sending InitResponse. Surface that as an init failure rather
        // than letting the caller proceed as if init succeeded.
        currentState match {
          case SessionState.Terminal(Termination.TransportFailed(cause)) =>
            throw new GrpcWorkerSessionException(
              "UDF worker stream failed during init", cause)
          case SessionState.Terminal(Termination.Failed(err)) =>
            throw new GrpcWorkerSessionException(
              s"UDF worker reported an error before init completed: " +
                describeError(err), err)
          case SessionState.Terminal(Termination.Cancelled(_)) =>
            throw new GrpcWorkerSessionException(
              "UDF worker stream was cancelled before init completed")
          case SessionState.Terminal(Termination.Finished(_)) =>
            throw new GrpcWorkerSessionException(
              "UDF worker finished before init completed")
          case other =>
            throw new IllegalStateException(
              s"init latch fired without an InitResponse or terminal: $other")
        }
    }
  }

  override protected def doProcess(
      input: Iterator[DataRequest],
      finish: () => Finish): Iterator[DataResponse] = {
    // Init success is guaranteed by the base [[WorkerSession]] lifecycle: if
    // doInit had failed it would have thrown and process() would never run.
    new ProcessIterator(input, finish)
  }

  override protected def doClose(cancel: () => Cancel): Termination = {
    if (requestObserver == null) {
      // init() never put a stream on the wire (closed before/around init, or
      // init threw before publishing). There is no protocol terminator; treat
      // the session as cancelled-before-start. The base WorkerSession still
      // releases the worker handle, so the worker is torn down.
      Transitions.cancelled(CancelResponse.getDefaultInstance)
      return Termination.Cancelled(CancelResponse.getDefaultInstance)
    }
    // If the stream has not finished on its own, cancel anything in flight so
    // the worker can clean up, then wait for the terminator. sendCancelInternal
    // evaluates the cancel thunk only when it actually writes a Cancel, and at
    // most once across all callers.
    if (!currentState.isTerminal) {
      sendCancelInternal(cancel)
      try {
        terminalLatch.await(terminalTimeoutMs, TimeUnit.MILLISECONDS)
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }
    // If the worker still has not settled (timeout or interrupt above),
    // record a terminal so isWorkerSalvageable returns a definite answer
    // and any other thread reading the state sees a stable value.
    if (!currentState.isTerminal) {
      Transitions.transportFailed(new TimeoutException(
        s"timed out waiting for stream terminator after ${terminalTimeoutMs}ms"))
    }
    // Half-close the request side regardless of whether we sent Cancel or
    // Finish earlier -- the worker's onCompleted handler relies on this to
    // tear down the per-stream state cleanly. onCompleted on an
    // already-torn-down stream throws and is caught, so this stays idempotent
    // across repeat close() calls.
    try {
      requestLock.synchronized { requestObserver.onCompleted() }
    } catch {
      case NonFatal(e) =>
        logger.debug("Error half-closing UDF Execute stream", e)
    }
    // Derive the Termination from the settled terminal. close() is the
    // finalizer/cleanup path and must NOT re-throw: a UDF / data-phase error is
    // already surfaced while the result iterator is consumed, and an init error
    // is surfaced from init(). settledTermination returns the settled terminal
    // as-is: the response proto for the clean terminators, and the failure
    // terminals (Failed / TransportFailed) carrying their cause -- not a bare
    // Cancelled.
    settledTermination
  }

  // ---- Internal request helpers ---------------------------------------------

  /**
   * Sends a Data or Finish request to the worker. Two invariants are
   * checked inside [[requestLock]]:
   *  - terminal state: writes are unsafe (transport dead or terminal
   *    received). Throws, so the caller's terminal/exception path runs.
   *  - [[cancelRequested]]: a Cancel has been (or is about to be) sent.
   *    Silently no-ops so a Data/Finish never appears on the wire after
   *    Cancel; the caller's iterator falls through to the terminator wait.
   *
   * Init is NOT sent through this helper -- [[doInit]] writes Init
   * directly under [[requestLock]] before publishing
   * [[requestObserver]], so there is no path through which a concurrent
   * cancel could send Cancel before Init.
   */
  private def sendRequest(req: UdfRequest): Unit =
    requestLock.synchronized {
      if (currentState.isTerminal) {
        throw new IllegalStateException(
          "cannot send request: UDF Execute stream is already closed")
      }
      // Suppress the write when a cancel has been (or is about to be) flushed
      // through this lock; that preserves the proto ordering invariant (no
      // Data/Finish after Cancel). Test the flag rather than `return`-ing from
      // this by-name `synchronized` body: a non-local return compiles to a thrown
      // NonLocalReturnControl, which is brittle (also relied on in sendCancelInternal).
      if (!cancelRequested.get()) {
        requestObserver.onNext(req)
      }
    }

  /**
   * Sends a `Cancel` control message, returning `true` iff a `Cancel` was
   * actually written to the wire. Idempotent across ALL call sites (close()'s
   * cancel, in-band cancels from `handleControl`'s INIT error / ERROR paths,
   * and engine-internal cancels from the iterator) via [[cancelRequested]]:
   * only the first caller puts a `Cancel` on the wire, and the `cancel` thunk is
   * evaluated only by that caller and only when the message is actually sent --
   * so a side-effecting thunk (e.g. one carrying a client cancel callback) runs
   * at most once. Setting [[cancelRequested]] also blocks subsequent Data/Finish
   * writes from [[ProcessIterator]] (see [[sendRequest]]), preserving the proto
   * invariant that nothing follows `Cancel` on the engine-to-worker side.
   */
  private def sendCancelInternal(cancel: () => Cancel): Boolean = {
    if (!cancelRequested.compareAndSet(false, true)) return false
    if (requestObserver == null) return false
    if (currentState.isTerminal) return false
    try {
      requestLock.synchronized {
        // A terminal that arrived first must win; bail without writing. Compute
        // the block's value rather than `return` (see sendRequest on why non-local
        // return is avoided -- here it would also escape the NonFatal catch below).
        val cur = currentState
        if (cur.isTerminal) {
          false
        } else {
          // Record that a Cancel is on the wire by advancing to Cancelling.
          Transitions.cancelSentFrom(cur)
          requestObserver.onNext(UdfRequest.newBuilder()
            .setControl(UdfControlRequest.newBuilder().setCancel(cancel()).build())
            .build())
          true
        }
      }
    } catch {
      case NonFatal(e) =>
        logger.debug(s"Cancel send failed (stream may already be torn down): ${e.getMessage}")
        Transitions.transportFailed(e)
        false
    }
  }

  private def awaitTerminal(): Unit = {
    if (currentState.isTerminal) return
    try {
      if (!terminalLatch.await(terminalTimeoutMs, TimeUnit.MILLISECONDS)) {
        Transitions.transportFailed(new TimeoutException(
          s"timed out waiting for stream terminator after ${terminalTimeoutMs}ms"))
      }
    } catch {
      case _: InterruptedException =>
        // Attribute the terminal to the interrupt, not a timeout: the wait was
        // cut short, it did not exhaust terminalTimeoutMs. Reporting "timed
        // out" here would misdiagnose an interrupted close as a slow worker.
        Thread.currentThread().interrupt()
        Transitions.transportFailed(
          new InterruptedException("interrupted while waiting for stream terminator"))
    }
  }

  // ---- ProcessIterator ------------------------------------------------------

  /**
   * Iterator returned by [[doProcess]]. Drives the data phase of the
   * stream end-to-end: each call to `hasNext` / `next` may send a
   * `DataRequest` or `Finish` to the worker, and reads result batches
   * out of [[outputQueue]] as the worker emits them.
   *
   * The iterator is single-threaded with respect to the engine, but
   * coexists with the gRPC callback thread (which enqueues responses
   * and may settle the state) and with any thread that finalizes via
   * [[close]]. It drives the `Streaming -> Finishing` transition (sending
   * `Finish` once input is exhausted, built lazily from the `finish` thunk).
   */
  private class ProcessIterator(input: Iterator[DataRequest], finish: () => Finish)
      extends Iterator[DataResponse] {

    // Latched once the terminator sentinel has been observed. Without this,
    // a second hasNext() after the iterator naturally exhausts would re-enter
    // advance(), fall through all branches, and block branch 4 for the
    // full terminalTimeoutMs before returning. Callers that probe hasNext
    // an extra time (iterator.size, instrumentation wrappers) would hang.
    // Iterator-local and only touched by the single engine thread.
    private val exhausted = new AtomicBoolean(false)
    @volatile private var prefetched: DataResponse = _

    // Pre-init cancel fast-fail: if a cancel ran before doInit published the
    // requestObserver, no Cancel was sent on the wire and the worker may
    // never emit a terminator. Trip the terminal explicitly so the first
    // hasNext / next surfaces a cancellation instead of blocking for
    // terminalTimeoutMs in branch 4.
    if (cancelRequested.get() && !currentState.isTerminal) {
      Transitions.cancelled(CancelResponse.getDefaultInstance)
    }

    /**
     * Runs a caller-supplied thunk (`input.hasNext`, `input.next()`, or the
     * `finish` builder) that may throw, and on failure Cancels the in-flight
     * stream before rethrowing. Without the Cancel a failure would leave the
     * worker awaiting more input with no terminator owed on the wire; with it the
     * worker tears down and the exception still propagates to the engine. Setting
     * `cancelRequested` also suppresses any further Data/Finish this loop might
     * otherwise attempt (see [[sendRequest]]).
     */
    private def cancelOnThrow[T](reason: String)(op: => T): T =
      try op catch {
        case NonFatal(e) =>
          sendCancelInternal(() => cancelWithReason(reason))
          throw e
      }

    override def hasNext: Boolean = {
      if (prefetched ne null) return true
      advance()
      prefetched ne null
    }

    override def next(): DataResponse = {
      if (prefetched eq null) advance()
      val out = prefetched
      if (out eq null) {
        throw new NoSuchElementException("ProcessIterator exhausted")
      }
      prefetched = null
      out
    }

    /**
     * Fills [[prefetched]] with the next output batch, or leaves it null at the
     * terminator (which also sets [[exhausted]], so `hasNext` reads null as "done").
     * Each loop iteration tries in order: (1) drain queued output -- a batch (fill
     * and return) or the terminator sentinel (return); (2) while `Streaming`, send
     * the next input batch and loop; (3) once input is exhausted, send `Finish`
     * (CAS `Streaming -> Finishing`, once) and loop; (4) otherwise block for late
     * output or the terminator, then return. Branches 2-3 loop; 1 and 4 return.
     *
     * '''Per-poll timeout.''' Branch 4 waits up to [[terminalTimeoutMs]] per
     * poll, reset by every worker event -- the contract is "emit at least one
     * event every [[terminalTimeoutMs]] after Finish", not "finish the UDF within
     * [[terminalTimeoutMs]]". A worker expecting a long post-Finish silence MAY
     * emit an empty `DataResponse` as a heartbeat to reset the wait; it is
     * surfaced to the caller, so it should be a batch the caller recognises as
     * empty (e.g. a zero-row Arrow batch).
     */
    private def advance(): Unit = {
      if (exhausted.get()) return // terminator already seen; never re-block
      while (prefetched eq null) {
        // (1) Drain anything the worker has already produced.
        outputQueue.poll() match {
          case null => // queue empty, fall through to send/wait branches below
          case QueueItem.EndOfStream =>
            exhausted.set(true)
            throwIfTerminalError()
            return
          case QueueItem.Batch(b) =>
            prefetched = b
            return
        }

        // (2) Send next input batch while the stream is open for data.
        // `input.hasNext` may itself fetch/compute the next element (many Spark
        // iterators prefetch), so it can throw just like `input.next()`; both go
        // through cancelOnThrow so a failing upstream Cancels the stream instead
        // of stranding the worker waiting for input that will never arrive.
        if (!cancelRequested.get() && currentState == SessionState.Streaming &&
            cancelOnThrow("input iterator failed")(input.hasNext)) {
          val batch = cancelOnThrow("input iterator failed")(input.next())
          if (!sendOrEndOnRacedTerminal(UdfRequest.newBuilder().setData(batch).build())) return
        } else if (!cancelRequested.get() && Transitions.finishSent()) {
          // (3) No more input; send Finish exactly once (unless cancelled). The
          // `finish` thunk is caller-supplied (it may run a finish callback), so
          // it can throw; cancelOnThrow Cancels the stream before rethrowing. The
          // Streaming -> Finishing CAS has already run, but we have not written
          // Finish yet, so the Cancel is the only engine-to-worker message that
          // reaches the wire -- the proto "nothing after Cancel" invariant holds.
          val finishMsg = cancelOnThrow("finish callback failed")(finish())
          if (!sendOrEndOnRacedTerminal(UdfRequest.newBuilder()
              .setControl(UdfControlRequest.newBuilder().setFinish(finishMsg).build())
              .build())) {
            return
          }
        } else {
          // (4) Block for late output or the terminator. See class doc
          //     above for the per-poll-vs-total-session timeout semantics.
          val item = try {
            outputQueue.poll(terminalTimeoutMs, TimeUnit.MILLISECONDS)
          } catch {
            case _: InterruptedException =>
              Thread.currentThread().interrupt()
              sendCancelInternal(() => cancelWithReason("iterator interrupted"))
              exhausted.set(true)
              throw new InterruptedException("interrupted while reading UDF result")
          }
          item match {
            case null =>
              Transitions.transportFailed(new IllegalStateException(
                s"timed out waiting for UDF output after ${terminalTimeoutMs}ms"))
              exhausted.set(true)
              throwIfTerminalError()
              return
            case QueueItem.EndOfStream =>
              exhausted.set(true)
              throwIfTerminalError()
              return
            case QueueItem.Batch(b) =>
              prefetched = b
              return
          }
        }
      }
    }

    /**
     * Sends one data-phase request (`DataRequest` / `Finish`), recovering from a
     * terminator that raced the write. Returns `true` to keep looping (sent, or
     * suppressed by a pending cancel), `false` if a terminator settled and the
     * iterator should end (caller `return`s).
     *
     * On a write failure a terminator usually already settled (the worker
     * finished/failed early) and the write only failed because the stream is
     * closed: record a transport terminal if none is set ([[completeTerminal]] is
     * a no-op once settled), then [[throwIfTerminalError]] -- which throws for an
     * error/transport terminal, or returns for a clean `Finished` that raced the
     * send, in which case the benign "stream closed" error is dropped.
     */
    private def sendOrEndOnRacedTerminal(req: UdfRequest): Boolean = {
      try {
        sendRequest(req)
        true
      } catch {
        case NonFatal(e) =>
          if (!currentState.isTerminal) {
            Transitions.transportFailed(e)
          }
          exhausted.set(true)
          throwIfTerminalError()
          false
      }
    }

    /**
     * Surfaces a failed terminator as an exception when the result iterator is
     * drained. A data-phase [[ExecutionError]] (captured in [[executionError]])
     * takes precedence over the terminator's own error, then the finish/cancel
     * callback error carried on the terminator, then a bare cancellation.
     */
    private def throwIfTerminalError(): Unit = currentState match {
      case SessionState.Terminal(Termination.Finished(response)) =>
        responseError(response.hasError, response.getError, executionError.get())
          .foreach(err => throw new GrpcWorkerSessionException(
            s"UDF execution failed: ${describeError(err)}", err))
      case SessionState.Terminal(Termination.Cancelled(response)) =>
        responseError(response.hasError, response.getError, executionError.get()) match {
          case Some(err) =>
            throw new GrpcWorkerSessionException(
              s"UDF execution failed: ${describeError(err)}", err)
          case None =>
            throw new GrpcWorkerSessionException("UDF execution was cancelled")
        }
      case SessionState.Terminal(Termination.Failed(err)) =>
        throw new GrpcWorkerSessionException(
          s"UDF execution failed: ${describeError(err)}", err)
      case SessionState.Terminal(Termination.TransportFailed(t)) =>
        throw new GrpcWorkerSessionException("UDF worker stream failed", t)
      case other =>
        throw new IllegalStateException(s"terminator sentinel without terminal: $other")
    }

    /** Picks the error to surface: prior data-phase error, else the terminator's. */
    private def responseError(
        hasError: Boolean,
        error: ExecutionError,
        priorError: Option[ExecutionError]): Option[ExecutionError] =
      priorError.orElse(if (hasError) Some(error) else None)
  }
}

object GrpcWorkerSession {
  /** Upper bound on the wait for `InitResponse`. */
  val DEFAULT_INIT_RESPONSE_TIMEOUT_MS: Long = 30000L

  /** Upper bound on the wait for `FinishResponse` / `CancelResponse`. */
  val DEFAULT_TERMINAL_TIMEOUT_MS: Long = 30000L

  // Distinguishes a (possibly empty) data batch from end-of-stream, and makes
  // the iterator's match exhaustive.
  private sealed trait QueueItem
  private object QueueItem {
    final case class Batch(response: DataResponse) extends QueueItem
    case object EndOfStream extends QueueItem
  }

  /**
   * A write-once value paired with the latch a waiter blocks on, so the "publish
   * the value, then release the waiter" ordering lives in one place instead of
   * every call site having to remember to count the latch down after setting the
   * reference. The value is set at most once ([[complete]]); the latch can also
   * be released without a value ([[signalWithoutValue]]) when the waiter must be
   * woken by a terminal that arrived instead of the value. All reads/writes go
   * through the latch, so a waiter released by [[await]] has a happens-before
   * edge to the [[complete]] that set the value.
   */
  private final class OneShotValue[A] {
    private val latch = new CountDownLatch(1)
    private val ref = new AtomicReference[Option[A]](None)

    /** Publishes the value (first writer wins) and releases any waiter. */
    def complete(value: A): Unit = {
      ref.compareAndSet(None, Some(value))
      latch.countDown()
    }

    /** Releases the waiter without a value (a terminal arrived, not the value). */
    def signalWithoutValue(): Unit = latch.countDown()

    /** Blocks up to `timeoutMs` for a release; true iff released in time. */
    def await(timeoutMs: Long): Boolean = latch.await(timeoutMs, TimeUnit.MILLISECONDS)

    /** The published value, or None if none was ever set. */
    def get: Option[A] = ref.get()
  }

  private[grpc] def describeError(err: ExecutionError): String = err.getKindCase match {
    case ExecutionError.KindCase.USER =>
      val u = err.getUser
      val cls = if (u.hasErrorClass) s"[${u.getErrorClass}] " else ""
      s"$cls${u.getMessage}"
    case ExecutionError.KindCase.WORKER =>
      s"WorkerError: ${err.getWorker.getMessage}"
    case ExecutionError.KindCase.PROTOCOL =>
      s"ProtocolError: ${err.getProtocol.getMessage}"
    case ExecutionError.KindCase.KIND_NOT_SET =>
      "ExecutionError without kind"
  }
}

/**
 * :: Experimental ::
 * Exception thrown by [[GrpcWorkerSession]] when the UDF execution fails
 * at the engine-protocol layer: init failure, ErrorResponse from the
 * worker, a failure terminator with no response, transport failure, or
 * (from the result iterator) a cancellation.
 *
 * This extends plain [[RuntimeException]] rather than Spark's
 * `SparkRuntimeException` so the udf-worker modules stay free of a spark-core
 * dependency. The engine integration layer (which already depends on
 * spark-core) is expected to catch this and wrap it in a
 * `SparkRuntimeException` with an appropriate error class when surfacing UDF
 * failures to users. [[executionError]] is preserved to carry the structured
 * cause across that boundary.
 *
 * @param executionError the structured protocol error when the failure carries
 *                       one (a worker `ErrorResponse`, an init error, or a
 *                       failure terminator with an error). `null` when there is
 *                       no structured cause: a transport failure, a timeout, or
 *                       a cancellation without an error. Callers must null-check
 *                       before use.
 */
@Experimental
class GrpcWorkerSessionException(
    message: String,
    cause: Throwable = null,
    @javax.annotation.Nullable val executionError: ExecutionError = null)
  extends RuntimeException(message, cause) {

  def this(message: String, error: ExecutionError) =
    this(message, null, error)
}
