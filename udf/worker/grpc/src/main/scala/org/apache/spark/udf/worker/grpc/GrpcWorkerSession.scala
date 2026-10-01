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

import java.util.Objects
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
 * ordering invariants documented in `udf_message.proto` (`PayloadChunk*`
 * between Init and InitResponse omitted here; chunking is not yet
 * implemented -- see the TODO below):
 * {{{
 *   Engine -> Worker:  Init -> (DataRequest)* -> Finish (Cancel)?
 *                                              | Cancel
 *   Worker -> Engine:  InitResponse -> (DataResponse)* ->
 *                      (ErrorResponse)? -> (FinishResponse | CancelResponse)
 * }}}
 *
 * Knows nothing about how the worker was provisioned -- the dispatcher
 * constructs this with a [[WorkerHandle]] and channel; the base
 * [[WorkerSession]] handles dispatcher-side cleanup on close.
 *
 * '''Driving model.''' Consumption-driven (Volcano / pull): the thread that
 * consumes the [[doProcess]] result iterator is the one that pulls input and
 * sends each `DataRequest`; the gRPC callback thread only receives output. It is
 * pull-driven but not one-input-per-output -- `advance` sends the next input
 * whenever the output queue is momentarily empty, so under async delivery it may
 * push several input batches before any output is read. HTTP/2 flow control
 * bounds wire traffic, but application-level buffering is intentionally left to
 * a follow-up (see the TODO on [[outputQueue]]).
 *
 * '''State machine.''' This class does not keep its own state machine: it
 * drives the single [[WorkerSession.SessionState]] owned by the base. The base
 * advances `Created -> Initializing` (in `init`) and `Initialized -> Streaming`
 * (in `process`); this class advances the protocol-event edges through
 * [[compareAndSetState]] / [[completeTerminal]] as it exchanges messages:
 * {{{
 *   Initializing --(InitResponse ok)--> Initialized   [handleControl]
 *   Streaming ----(input exhausted)-------> Finishing      [ProcessIterator]
 *   <any non-terminal> --(Cancel send starts)--> Cancelling [sendCancelInternal]
 *   <any non-terminal> --(terminator/error)--> terminal [completeTerminal]
 * }}}
 * The two clean terminals carry the worker's `FinishResponse` / `CancelResponse`
 * (metrics + finish/cancel callback `data`/`error`) so [[close]] can return
 * them. Cancellation intent is deliberately tracked outside the machine in
 * [[cancelRequested]], which makes the wire write idempotent and suppresses any
 * in-flight Data/Finish once cancellation begins.
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
 *    and terminates the request side.
 *
 * TODO [SPARK-55278]: this class does not yet implement payload chunking;
 * the entire [[org.apache.spark.udf.worker.UdfPayload]] is sent inline. Chunking will be added
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
 * @param interruptCancelTimeoutMs upper bound on the wait for a `CancelResponse`
 *                              after an interrupt (cancelled query / killed
 *                              task) sends `Cancel`. Short by design; on expiry
 *                              the session settles `Interrupted`. See
 *                              `handleInterrupt`.
 */
@Experimental
class GrpcWorkerSession(
    workerHandle: WorkerHandle,
    channel: ManagedChannel,
    logger: WorkerLogger = WorkerLogger.NoOp,
    initResponseTimeoutMs: Long = DEFAULT_INIT_RESPONSE_TIMEOUT_MS,
    terminalTimeoutMs: Long = DEFAULT_TERMINAL_TIMEOUT_MS,
    interruptCancelTimeoutMs: Long = DEFAULT_INTERRUPT_CANCEL_TIMEOUT_MS)
  extends WorkerSession(workerHandle, logger) {

  require(channel != null, "channel is required")

  private val asyncStub = UdfWorkerGrpc.newStub(channel)

  // Output batches from the worker, drained by the process() iterator.
  // Intentionally unbounded in this first implementation: a bounded queue would
  // block the gRPC callback (Netty event-loop) thread when full, stalling
  // terminator/control delivery on the whole channel. The consumer normally
  // drains promptly, but HTTP/2 flow control alone does not bound this application
  // queue or gRPC's asynchronous send buffers.
  //
  // TODO [SPARK-55278]: add application-level gRPC flow control in a follow-up,
  // using ClientCallStreamObserver readiness for requests and manual inbound
  // demand for responses, before wiring this transport into a production path.
  // TODO [SPARK-57324]: expose queue depth as a metric (early warning for a
  // stalled consumer).
  private val outputQueue = new LinkedBlockingQueue[QueueItem]()

  // This value couples the worker's `InitResponse` (success or error) with the
  // latch on which init() blocks. The latch fires when the InitResponse arrives
  // (`complete`), when a pre-init ErrorResponse arrives, or when a terminal
  // settles first without an InitResponse (`signalWithoutValue`). Until it fires
  // we have no proof the worker accepted the session.
  //
  // Settle-before-release rule (referenced from every callback that both settles
  // a terminal and fires this latch): settle the terminal FIRST, then complete /
  // signal. init() blocks on the latch, and only a latch await/release pair
  // establishes a happens-before edge, so a woken init() is guaranteed to
  // observe the terminal rather than a transient state. OneShotValue keeps that
  // publish-then-release in one place instead of every caller remembering to
  // count down after setting the reference.
  private val initValue = new OneShotValue[InitResponse]

  // Fired when the session reaches a terminal [[SessionState]]. doClose() and
  // the init-error path block on this to drain the terminator.
  private val terminalLatch = new CountDownLatch(1)

  // Captures an ErrorResponse encountered during the data phase so that
  // the CancelResponse terminator can attribute the failure to the original
  // user / worker / protocol error rather than reporting a bare "Cancelled".
  private val executionError = new AtomicReference[Option[ExecutionError]](None)

  // True immediately before Finish is handed to the request observer. It is set
  // before onNext so a reentrant callback may deliver FinishResponse while the
  // Finish request is still being written.
  private val finishSendStarted = new AtomicBoolean(false)

  // Cancellation INTENT -- distinct from the `Cancelling` state, which is reached
  // once a Cancel send is attempted under [[requestLock]] ([[sendCancelInternal]]).
  // Intent is set first and can outrun (or never reach) that attempt, so
  // `cancelRequested` does NOT imply state `Cancelling`. It is kept outside the
  // [[SessionState]] machine to (a) make cancellation idempotent across all call
  // sites and (b) suppress any Data/Finish that would otherwise race a Cancel
  // onto the wire (re-read inside [[requestLock]]).
  private val cancelRequested = new AtomicBoolean(false)

  // gRPC requires serialized writes to a request StreamObserver.
  private val requestLock = new Object

  // Initialized in init() -- before that, close() is a no-op on the request
  // side, which is exactly the contract the wrapping WorkerSession expects.
  @volatile private var requestObserver: StreamObserver[UdfRequest] = _

  // True after the request side has been half-closed, aborted, or observed the
  // transport closing. It prevents this session from issuing duplicate terminal
  // calls, including following onError with onCompleted from close().
  private val requestSideTerminated = new AtomicBoolean(false)

  private val responseObserver: StreamObserver[UdfResponse] = new StreamObserver[UdfResponse] {
    override def onNext(response: UdfResponse): Unit = {
      response.getResponseCase match {
        case UdfResponse.ResponseCase.DATA =>
          // A DataResponse before InitResponse violates the protocol (InitResponse
          // must precede any DataResponse): fast-fail rather than enqueue it and let
          // init() block to its timeout. Settling the terminal wakes a blocked
          // init() via onTerminalSettled.
          if (!initResolved) {
            Transitions.transportFailed(new IllegalStateException(
              "worker sent a DataResponse before InitResponse"))
          } else {
            outputQueue.put(QueueItem.Batch(response.getData))
          }

        case UdfResponse.ResponseCase.CONTROL =>
          handleControl(response.getControl)

        case other =>
          // A malformed response (empty / unknown oneof) is a terminal transport
          // failure; fast-fail init the same way.
          Transitions.transportFailed(new IllegalStateException(
            s"unexpected response oneof: $other"))
      }
    }

    override def onError(t: Throwable): Unit = {
      // Transport-level failure: the stream is dead, no further writes possible.
      // Settling the terminal wakes a blocked init() (via onTerminalSettled) so it
      // surfaces the transport cause instead of the initResponseTimeoutMs error.
      requestSideTerminated.set(true)
      Transitions.transportFailed(t)
    }

    override def onCompleted(): Unit = {
      // Worker half-closed its side without sending a terminator (FinishResponse
      // / CancelResponse). Treat as transport error so the engine sees a
      // failure, not a silent end-of-stream. Settling the terminal wakes a blocked
      // init() via onTerminalSettled.
      requestSideTerminated.set(true)
      if (!currentState.isTerminal) {
        Transitions.transportFailed(new IllegalStateException(
          "worker response stream closed without a terminator"))
      }
    }
  }

  /**
   * Wakes everything that can be blocked when the base settles a terminal:
   * the result iterator (on [[outputQueue]]), a thread on [[terminalLatch]]
   * (close), and a thread still blocked in [[doInit]] on [[initValue]]. Invoked
   * once, by the caller that wins [[completeTerminal]], so this is the '''single'''
   * place a settled terminal wakes a blocked [[doInit]] -- callers that settle a
   * terminal (any response-callback failure/terminator branch, the timeout paths,
   * or close()) do NOT signal [[initValue]] themselves; settling is enough.
   * Settle-before-release (see [[initValue]]) holds because this runs after the
   * terminal CAS in [[completeTerminal]]. The only direct [[initValue]] signals
   * left are the non-terminal init paths ([[handleControl]]'s INIT-ok / INIT-error
   * / pre-init-ERROR branches), which must wake [[doInit]] '''without''' settling a
   * terminal. INIT-ok first advances the session to `Initialized`; the error
   * branches remain `Initializing` only until [[doInit]] sends Cancel and throws.
   */
  override protected def onTerminalSettled(termination: Termination): Unit = {
    outputQueue.put(QueueItem.EndOfStream)
    initValue.signalWithoutValue()
    terminalLatch.countDown()
  }

  private def handleControl(ctrl: UdfControlResponse): Unit = ctrl.getControlCase match {
    case UdfControlResponse.ControlCase.INIT =>
      val resp = ctrl.getInit
      if (resp.hasError) {
        // Record the error and publish the InitResponse; do NOT settle a terminal
        // or send Cancel here. The proto requires the engine to Cancel after an
        // init error (udf_message.proto); doInit owns that synchronous cleanup
        // after the initial onNext returns, then drains the CancelResponse before
        // throwing. Settle-before-release (see initValue): there is no terminal to
        // settle first here, just publish.
        executionError.compareAndSet(None, Some(resp.getError))
        initValue.complete(resp)
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
      if (!initResolved) {
        // Pre-init ErrorResponse. Leave the session in Initializing and just wake
        // init(): the recorded executionError tells doInit the worker failed, and
        // doInit sends the Cancel and drains the CancelResponse after the initial
        // onNext returns, just like the INIT-error branch.
        initValue.signalWithoutValue()
      } else {
        // Data-phase ErrorResponse: requestObserver is published, so cancel here.
        // Cancel -> CancelResponse settles the terminal; the iterator surfaces the
        // recorded executionError. The init latch already fired in init(), so no
        // signalWithoutValue is needed.
        sendCancelInternal(() => cancelWithReason("aborting after ErrorResponse"))
      }

    case UdfControlResponse.ControlCase.FINISH =>
      // The FinishResponse carries metrics + the finish-callback data/error.
      // Keep it on the terminal so close() can return it; the iterator inspects
      // its error field to decide whether to throw. A FinishResponse is valid only
      // after the engine has started sending Finish; accepting one earlier could
      // silently truncate input and report a clean result.
      if (finishSendStarted.get()) {
        Transitions.finished(ctrl.getFinish)
      } else {
        Transitions.transportFailed(new IllegalStateException(
          "worker sent FinishResponse before the engine sent Finish"))
      }

    case UdfControlResponse.ControlCase.CANCEL =>
      // The CancelResponse carries metrics + the cancel-callback error. Keep it
      // on the terminal so close() can return it; any prior ErrorResponse is
      // tracked in executionError and surfaced by the iterator. Settling the
      // terminal wakes a blocked init() (a CANCEL before InitResponse) via
      // onTerminalSettled.
      Transitions.cancelled(ctrl.getCancel)

    case UdfControlResponse.ControlCase.CONTROL_NOT_SET =>
      // Settling the terminal wakes a blocked init() via onTerminalSettled.
      Transitions.transportFailed(new IllegalStateException(
        "empty UdfControlResponse oneof"))
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
   *   Streaming -> Finishing          beginFinish      advance branch 3       [engine]
   *   non-terminal -> Cancelling      beginCancelFrom  sendCancelInternal, immediately
   *                                      before the Cancel send  [gRPC cb | engine | init | close]
   *   non-terminal -> Terminal        finished/cancelled/transportFailed/interrupted
   *                                     handleControl / doInit / doClose / advance / onError
   * }}}
   * `Cancelling` is reached only when a Cancel send starts; a pre-stream or
   * raced cancel goes straight to a `Cancelled`/`TransportFailed` terminal (see
   * [[sendCancelInternal]], [[doClose]], `ProcessIterator`) or nowhere -- so
   * `cancelRequested` (intent) does not imply state `Cancelling`.
   */
  private object Transitions {
    /** `InitResponse` OK: `Initializing -> Initialized`. */
    def initAccepted(): Boolean =
      compareAndSetState(SessionState.Initializing, SessionState.Initialized)

    /** Input exhausted: `Streaming -> Finishing` (once). */
    def beginFinish(): Boolean =
      compareAndSetState(SessionState.Streaming, SessionState.Finishing)

    /** `Cancel` send starts: `cur -> Cancelling`, from any non-terminal `cur`. */
    def beginCancelFrom(cur: SessionState): Boolean =
      !cur.isTerminal && compareAndSetState(cur, SessionState.Cancelling)

    /** Clean terminal carrying the worker's `FinishResponse`. */
    def finished(response: FinishResponse): Boolean =
      completeTerminal(Termination.Finished(response))

    /** Clean terminal carrying the worker's `CancelResponse`. */
    def cancelled(response: CancelResponse): Boolean =
      completeTerminal(Termination.Cancelled(response))

    /** Failure terminal carrying a transport-level cause. */
    def transportFailed(cause: Throwable): Boolean =
      completeTerminal(Termination.TransportFailed(cause))

    /**
     * Terminal for an engine-thread interrupt (e.g. a Spark task kill). Distinct
     * from [[transportFailed]] because the cause is an engine-side interrupt, but
     * still unsalvageable without a worker acknowledgement. Settled by
     * [[handleInterrupt]] only when the brief post-Cancel drain expires without a
     * `CancelResponse`; if the worker acks in time the session settles a
     * cooperative [[cancelled]] instead.
     */
    def interrupted(cause: Throwable): Boolean =
      completeTerminal(Termination.Interrupted(cause))
  }

  // ---- WorkerSession hooks ------------------------------------------------

  override protected def doInit(message: Init): InitResponse = {
    // Construct the request before opening the RPC. A malformed Init must not
    // leave an otherwise-unused Execute stream waiting for its first request.
    val initRequest = UdfRequest.newBuilder()
      .setControl(UdfControlRequest.newBuilder().setInit(message).build())
      .build()

    // Fail fast if the channel is already shut down. Without this check,
    // asyncStub.execute(...) would still succeed and the failure would
    // surface ~initResponseTimeoutMs later as a misleading "InitResponse
    // timed out" error.
    if (channel.getState(false) == ConnectivityState.SHUTDOWN) {
      val ex = new IllegalStateException("gRPC channel is shut down")
      Transitions.transportFailed(ex)
      throw new GrpcWorkerSessionException("UDF worker channel is closed", ex)
    }
    try {
      requestLock.synchronized {
        // Serialize stream creation, publication, and Init with close/cancel. A
        // close that wins the lock first prevents the RPC from opening; one that
        // loses cannot release the worker until the initial write returns.
        if (!currentState.isTerminal) {
          val stream = asyncStub.execute(responseObserver)
          requestObserver = stream
          // Stream creation may invoke a response callback synchronously. Do not
          // send Init if that callback has already settled a terminal.
          if (!currentState.isTerminal) {
            // With directExecutor, gRPC callbacks run synchronously on the caller's
            // thread, so InitResponse can reach responseObserver/handleControl from
            // *inside* this stream.onNext.
            // requestObserver is already published, so an immediate ErrorResponse
            // can write its required Cancel reentrantly without losing the request.
            sendOnNext(initRequest)
          }
        }
      }
    } catch {
      case NonFatal(e) =>
        Transitions.transportFailed(e)
        // Surface as GrpcWorkerSessionException so the engine integration layer
        // (which catches that type and wraps it) sees a uniform init-failure
        // exception rather than the raw transport error.
        throw new GrpcWorkerSessionException("UDF worker stream failed during init", e)
    }

    try {
      initValue.await(initResponseTimeoutMs)
    } catch {
      case _: InterruptedException =>
        // Interrupt (cancelled query / killed task) while awaiting InitResponse:
        // cooperatively Cancel with a bounded drain, settling Cancelled if the
        // worker acks in time, else Interrupted. See handleInterrupt.
        handleInterrupt("waiting for InitResponse")
        throw new InterruptedException("interrupted while waiting for InitResponse")
      case e: TimeoutException =>
        sendCancelInternal(() => cancelWithReason("InitResponse timed out"))
        // Settle the terminal so a subsequent close() does not stall for a
        // second `terminalTimeoutMs` waiting for a worker that already missed
        // its init deadline.
        Transitions.transportFailed(e)
        // Surface as GrpcWorkerSessionException (carrying the timeout cause) so
        // the engine integration layer that catches that type can wrap it.
        throw new GrpcWorkerSessionException(
          s"timed out waiting for InitResponse after ${initResponseTimeoutMs}ms", e)
    }

    initValue.get match {
      case Some(resp) if resp.hasError =>
        failInitWithError(resp.getError,
          s"UDF worker init failed: ${describeError(resp.getError)}")
      case Some(resp) =>
        executionError.get() match {
          case Some(err) =>
            // A reentrant callback can report an ErrorResponse immediately after
            // InitResponse and before the initial onNext returns. The callback
            // has already sent Cancel; drain its response
            // and surface the original error instead of returning init success
            // for a session that is already cancelling or terminal.
            failInitWithError(err,
              s"UDF worker reported an error as init completed: ${describeError(err)}")
          case None if currentState != SessionState.Initialized =>
            // Likewise, an immediate protocol failure or concurrent close may
            // have moved the session out of Initialized after publishing the
            // InitResponse. A normal return would violate init()'s contract and
            // leave process() to fail later with only an ordering error.
            failInitFromCurrentState()
          case None =>
            resp
        }
      case None =>
        // No InitResponse arrived but the latch fired.
        //
        // A pre-init ErrorResponse leaves the session in Initializing with the
        // error recorded in executionError (see handleControl ERROR branch): the
        // engine must now Cancel and drain the CancelResponse, which failInit does.
        executionError.get() match {
          case Some(err) if !currentState.isTerminal =>
            failInitWithError(err,
              s"UDF worker reported an error before init completed: ${describeError(err)}")
          case _ =>
            // Otherwise a terminal already settled -- the worker terminated the
            // stream before sending InitResponse (transport error, half-close, or
            // a premature FinishResponse/CancelResponse). Surface it as an init
            // failure rather than letting the caller proceed as if init succeeded.
            failInitFromCurrentState()
        }
    }
  }

  /** Surfaces the terminal that prevented init from completing normally. */
  private def failInitFromCurrentState(): Nothing = {
    executionError.get() match {
      case Some(err) =>
        failInitWithError(err,
          s"UDF worker reported an error as init completed: ${describeError(err)}")
      case None => ()
    }
    // Cancelling can be observed after a concurrent close or a reentrant error
    // before its CancelResponse arrives. Drain it so the exception reflects the
    // stable terminal rather than a transient state.
    if (currentState == SessionState.Cancelling) {
      awaitTerminal()
    }
    currentState match {
      case SessionState.Terminal(Termination.TransportFailed(cause)) =>
        throw new GrpcWorkerSessionException("UDF worker stream failed during init", cause)
      case SessionState.Terminal(Termination.Failed(err)) =>
        throw new GrpcWorkerSessionException(
          s"UDF worker reported an error before init completed: ${describeError(err)}", err)
      case SessionState.Terminal(Termination.Cancelled(_)) =>
        throw new GrpcWorkerSessionException(
          "UDF worker stream was cancelled before init completed")
      case SessionState.Terminal(Termination.Interrupted(cause)) =>
        throw new GrpcWorkerSessionException("UDF worker init was interrupted", cause)
      case SessionState.Terminal(Termination.Finished(_)) =>
        throw new GrpcWorkerSessionException("UDF worker finished before init completed")
      case other =>
        throw new IllegalStateException(
          s"init completed without an accepted session or terminal: $other")
    }
  }

  /**
   * Fails init when the worker reports an error before init returns: an
   * `InitResponse` carrying an error, a pre-init `ErrorResponse`, or an immediate
   * post-init `ErrorResponse`. The protocol requires the engine to send `Cancel`
   * and the worker to reply with `CancelResponse` (udf_message.proto); we send it,
   * drain the terminator so no stream is left dangling, and throw the structured
   * error. A responsive worker settles `Cancelled`; a failed drain settles
   * `TransportFailed`.
   */
  private def failInitWithError(err: ExecutionError, message: String): Nothing = {
    sendCancelInternal(() => cancelWithReason("init failed"))
    awaitTerminal()
    throw new GrpcWorkerSessionException(message, err)
  }

  override protected def doProcess(
      input: Iterator[DataRequest],
      finish: () => Finish): Iterator[DataResponse] = {
    // Init success is guaranteed by the base [[WorkerSession]] lifecycle: if
    // doInit had failed it would have thrown and process() would never run.
    new ProcessIterator(input, finish)
  }

  override protected def doClose(cancel: () => Cancel): Termination = {
    // Coordinate the no-stream decision with doInit's publication + Init write.
    // If close wins this lock, doInit observes the terminal and never sends Init;
    // if doInit wins, close observes the published observer and sends Cancel only
    // after the Init send has returned.
    val noPublishedStream = requestLock.synchronized {
      if (requestObserver == null) {
        // init() never put a stream on the wire (closed before/around init, or
        // init threw before publishing). There is no protocol terminator; if no
        // terminal has settled yet, treat the session as cancelled-before-start.
        // A terminal may already be settled here (e.g. the channel-shutdown
        // TransportFailed in doInit also leaves requestObserver null); return the
        // settled terminal as-is rather than a bare Cancelled that disagrees with
        // the state. The base WorkerSession still releases the worker handle.
        if (!currentState.isTerminal) {
          Transitions.cancelled(CancelResponse.getDefaultInstance)
        }
        true
      } else {
        false
      }
    }
    if (noPublishedStream) {
      return settledTermination
    }
    // If the stream has not finished on its own, cancel anything in flight so
    // the worker can clean up, then wait for the terminator. sendCancelInternal
    // evaluates the cancel thunk only when it attempts a Cancel, and at
    // most once across all callers.
    if (!currentState.isTerminal) {
      sendCancelInternal(cancel)
      try {
        terminalLatch.await(terminalTimeoutMs, TimeUnit.MILLISECONDS)
      } catch {
        case _: InterruptedException =>
          // Interrupted mid-close: settle Interrupted via the
          // shared handler rather than falling through to the TransportFailed
          // guard below. The Cancel was already attempted, so handleInterrupt's
          // sendCancelInternal is a no-op and it just runs the bounded drain.
          //
          // TODO [SPARK-57640]: close() is a finalizer often already on the task
          // kill path, yet this makes an interrupted close block up to another
          // interruptCancelTimeoutMs (chasing a clean, salvageable Cancelled
          // rather than immediately unwinding to Interrupted). Current choice:
          // spend the bounded wait to obtain proof the worker is recyclable; it
          // is small next to terminalTimeoutMs. Revisit if killed-task teardown
          // latency (especially many sessions torn down at once) makes an
          // immediate unwind preferable -- e.g. skip this second drain in the
          // close() path, since close already gave the worker its terminalTimeoutMs window.
          handleInterrupt("closing the session")
      }
    }
    // If the worker still has not settled (timeout above, or an interrupt whose
    // bounded drain did not settle a terminal), record a terminal so
    // isWorkerSalvageable returns a definite answer and any other thread reading
    // the state sees a stable value.
    if (!currentState.isTerminal) {
      Transitions.transportFailed(new TimeoutException(
        s"timed out waiting for stream terminator after ${terminalTimeoutMs}ms"))
    }
    // Close the request side according to the settled outcome. Clean protocol
    // terminators are half-closed; failures abort the RPC so close() never follows
    // an onError with onCompleted.
    currentState match {
      case SessionState.Terminal(Termination.Finished(_)) |
          SessionState.Terminal(Termination.Cancelled(_)) =>
        completeRequestSide()
      case SessionState.Terminal(Termination.Failed(error)) =>
        abortRequestSide(new GrpcWorkerSessionException(
          s"UDF execution failed: ${describeError(error)}", error))
      case SessionState.Terminal(Termination.TransportFailed(cause)) =>
        abortRequestSide(cause)
      case SessionState.Terminal(Termination.Interrupted(cause)) =>
        abortRequestSide(cause)
      case other =>
        logger.debug(s"UDF Execute stream closed without a terminal outcome: $other")
    }
    // Derive the Termination from the settled terminal. close() is the
    // finalizer/cleanup path and must NOT re-throw: a UDF / data-phase error is
    // already surfaced while the result iterator is consumed, and an init error
    // is surfaced from init(). settledTermination returns the settled terminal
    // as-is: the response proto for the clean terminators, and the failure
    // terminals (Failed / TransportFailed / Interrupted) carrying their cause --
    // not a bare Cancelled.
    settledTermination
  }

  // ---- Internal request helpers ---------------------------------------------

  /**
   * Writes one request and aborts the request side if the observer rejects it.
   * Callers hold [[requestLock]], so this also serializes the compensating
   * `onError` with every other request-side operation.
   */
  private def sendOnNext(req: UdfRequest): Unit = {
    try {
      requestObserver.onNext(req)
    } catch {
      case NonFatal(e) =>
        abortRequestSide(e)
        throw e
    }
  }

  /** Aborts the request side at most once. Safe to call while holding [[requestLock]]. */
  private def abortRequestSide(cause: Throwable): Unit = requestLock.synchronized {
    if (requestObserver != null && requestSideTerminated.compareAndSet(false, true)) {
      try {
        requestObserver.onError(cause)
      } catch {
        case NonFatal(e) => logger.debug("Error aborting UDF Execute stream", e)
      }
    }
  }

  /** Half-closes the request side at most once. */
  private def completeRequestSide(): Unit = requestLock.synchronized {
    if (requestObserver != null && requestSideTerminated.compareAndSet(false, true)) {
      try {
        requestObserver.onCompleted()
      } catch {
        case NonFatal(e) => logger.debug("Error half-closing UDF Execute stream", e)
      }
    }
  }

  /**
   * Sends a Data or Finish request to the worker. Three invariants are
   * checked inside [[requestLock]]:
   *  - terminal state: writes are unsafe (transport dead or terminal
   *    received). Throws, so the caller's terminal/exception path runs.
   *  - request-side termination: no write may follow `onError` / `onCompleted`.
   *    Throws so the caller settles or surfaces the failure.
   *  - [[cancelRequested]]: a Cancel has been (or is about to be) sent.
   *    Silently no-ops so a Data/Finish never appears on the wire after
   *    Cancel; the caller's iterator falls through to the terminator wait.
   *
   * Init is NOT sent through this helper -- [[doInit]] publishes
   * [[requestObserver]] and writes Init together under [[requestLock]], so a
   * concurrent cancel observes the observer but cannot acquire the lock and send
   * Cancel until after Init.
   */
  private def sendRequest(req: UdfRequest): Unit =
    requestLock.synchronized {
      if (currentState.isTerminal) {
        throw new IllegalStateException(
          "cannot send request: UDF Execute stream is already closed")
      }
      if (requestSideTerminated.get()) {
        throw new IllegalStateException(
          "cannot send request: UDF Execute request stream is already closed")
      }
      // Suppress the write when a cancel has been (or is about to be) flushed
      // through this lock; that preserves the proto ordering invariant (no
      // Data/Finish after Cancel). Test the flag rather than `return`-ing from
      // this by-name `synchronized` body: a non-local return compiles to a thrown
      // NonLocalReturnControl, which is brittle (also relied on in sendCancelInternal).
      if (!cancelRequested.get()) {
        if (req.hasControl && req.getControl.hasFinish) {
          // Set before onNext: directExecutor delivery may return FinishResponse
          // reentrantly from inside this call.
          finishSendStarted.set(true)
        }
        sendOnNext(req)
      }
    }

  /**
   * Sends a `Cancel` control message, returning `true` iff the request observer
   * accepts the `Cancel`. Idempotent across ALL call sites (close()'s
   * cancel, in-band cancels from `handleControl`'s INIT error / ERROR paths,
   * and engine-internal cancels from the iterator) via [[cancelRequested]]:
   * only the first caller attempts a `Cancel`, and the `cancel` thunk is evaluated
   * only by that caller and only when the send is about to start --
   * so a side-effecting thunk (e.g. one carrying a client cancel callback) runs
   * at most once. Setting [[cancelRequested]] also blocks subsequent Data/Finish
   * writes from [[ProcessIterator]] (see [[sendRequest]]), preserving the proto
   * invariant that nothing follows `Cancel` on the engine-to-worker side.
   */
  private def sendCancelInternal(cancel: () => Cancel): Boolean = {
    // Do not consume the one-shot cancellation intent until there is an observer
    // that can carry it. doInit publishes the observer before sending Init while
    // holding requestLock, so every response to Init sees a non-null observer.
    if (requestObserver == null) return false
    if (!cancelRequested.compareAndSet(false, true)) return false
    if (currentState.isTerminal) return false
    try {
      requestLock.synchronized {
        // A terminal that arrived first must win; bail without writing. Compute
        // the block's value rather than `return` (see sendRequest for why non-local
        // return is avoided -- here it would also escape the NonFatal catch below).
        val cur = currentState
        if (cur.isTerminal || requestSideTerminated.get()) {
          false
        } else {
          val request = UdfRequest.newBuilder()
            .setControl(UdfControlRequest.newBuilder().setCancel(cancel()).build())
            .build()
          // Record that a Cancel send has started by advancing to Cancelling.
          Transitions.beginCancelFrom(cur)
          sendOnNext(request)
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
      case _: InterruptedException => handleInterrupt("waiting for stream terminator")
    }
  }

  /**
   * Handles an [[InterruptedException]] observed while blocked on a worker event
   * (init, terminal drain, close, or the result-iterator poll). An interrupt is
   * an engine-side event -- typically a cancelled query / killed task -- not a
   * worker fault, so we cancel cooperatively and keep the worker salvageable only
   * when it acks:
   *
   *  1. Send a best-effort `Cancel` (idempotent across call sites).
   *  2. Wait up to [[interruptCancelTimeoutMs]] -- short, so the interrupted
   *     thread unwinds promptly -- for the worker's `CancelResponse`. A healthy
   *     worker acks in that window and the callback settles a clean `Cancelled`
   *     terminal (worker salvageable, response drained).
   *  3. If the ack does not arrive in time, settle `Interrupted`. Without a
   *     terminator or liveness proof the worker is not salvageable and must not
   *     be returned to a reuse pool.
   *
   * The bounded wait deliberately runs '''before''' the thread's interrupt flag
   * is restored: [[InterruptedException]] clears the flag on throw, so re-setting
   * it first would make the wait below throw immediately and defeat the drain.
   * The flag is restored at the end so the caller's unwind still observes the
   * interrupt.
   *
   * TODO [SPARK-57640]: add a worker liveness/heartbeat check so a future version
   * may prove that an interrupted worker which missed the short ack window is
   * nevertheless safe to recycle.
   */
  private def handleInterrupt(waitContext: String): Unit = {
    // NB: flag is currently clear (InterruptedException cleared it); do not
    // restore it until after the bounded drain below.
    if (!currentState.isTerminal) {
      sendCancelInternal(() => cancelWithReason(s"interrupted while $waitContext"))
      val acked = try {
        terminalLatch.await(interruptCancelTimeoutMs, TimeUnit.MILLISECONDS)
      } catch {
        case _: InterruptedException =>
          // A second interrupt during the drain: give up the wait immediately.
          false
      }
      if (!acked && !currentState.isTerminal) {
        // No CancelResponse in time: settle Interrupted rather than
        // TransportFailed, and stop a later close() from blocking on a terminator
        // that will not be drained on this thread's behalf. Interrupted remains
        // unsalvageable until a separate liveness check can prove otherwise.
        Transitions.interrupted(
          new InterruptedException(s"interrupted while $waitContext"))
      }
    }
    Thread.currentThread().interrupt()
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

    /**
     * Runs a caller-supplied thunk (`input.hasNext`, `input.next()`, or the
     * `finish` builder) that may throw, and on failure Cancels the in-flight
     * stream before rethrowing. Without the Cancel a failure would leave the
     * worker awaiting more input with no terminator owed on the wire; with it the
     * worker tears down and the exception still propagates to the engine. Setting
     * `cancelRequested` also suppresses any further Data/Finish this loop might
     * otherwise attempt (see [[sendRequest]]). An [[InterruptedException]] uses
     * [[handleInterrupt]] so its CancelResponse drain remains bounded by
     * [[interruptCancelTimeoutMs]].
     */
    private def cancelOnThrow[T](reason: String)(op: => T): T =
      try op catch {
        case _: InterruptedException =>
          handleInterrupt(reason)
          throw new InterruptedException(s"interrupted while $reason")
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
          val request = cancelOnThrow("input iterator failed") {
            val batch = Objects.requireNonNull(
              input.next(), "input iterator returned null")
            UdfRequest.newBuilder().setData(batch).build()
          }
          if (!sendOrEndOnRacedTerminal(request)) return
        } else if (!cancelRequested.get() && Transitions.beginFinish()) {
          // (3) No more input; send Finish exactly once (unless cancelled). The
          // `finish` thunk is caller-supplied (it may run a finish callback), so
          // it can throw; cancelOnThrow Cancels the stream before rethrowing. The
          // Streaming -> Finishing CAS has already run, but we have not written
          // Finish yet, so the Cancel is the only engine-to-worker message that
          // reaches the wire -- the proto "nothing after Cancel" invariant holds.
          val request = cancelOnThrow("finish callback failed") {
            val finishMsg = Objects.requireNonNull(
              finish(), "finish callback returned null")
            UdfRequest.newBuilder()
              .setControl(UdfControlRequest.newBuilder().setFinish(finishMsg).build())
              .build()
          }
          if (!sendOrEndOnRacedTerminal(request)) {
            return
          }
        } else {
          // (4) Block for late output or the terminator. See class doc
          //     above for the per-poll-vs-total-session timeout semantics.
          val item = try {
            outputQueue.poll(terminalTimeoutMs, TimeUnit.MILLISECONDS)
          } catch {
            case _: InterruptedException =>
              // Interrupt (cancelled query / killed task) while reading results:
              // cooperatively Cancel with a bounded drain, settling Cancelled if
              // the worker acks in time, else Interrupted. See handleInterrupt.
              handleInterrupt("reading UDF result")
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
     * On a write failure a terminator has usually already settled (the worker
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
      case SessionState.Terminal(Termination.Interrupted(t)) =>
        throw new GrpcWorkerSessionException("UDF execution was interrupted", t)
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

  /**
   * Upper bound on the wait for a `CancelResponse` after an interrupt (e.g. a
   * cancelled query / killed task) sends `Cancel`. Much shorter than
   * [[DEFAULT_TERMINAL_TIMEOUT_MS]]: the interrupted thread must unwind
   * promptly, so a healthy worker gets a brief window to ack the Cancel (clean
   * `Cancelled` terminal, worker salvageable) before the session falls back to
   * an unsalvageable `Interrupted` terminal. See `handleInterrupt`.
   */
  val DEFAULT_INTERRUPT_CANCEL_TIMEOUT_MS: Long = 2000L

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
   * be released without a value ([[signalWithoutValue]]) when init fails through
   * a pre-init error or terminal. All reads/writes go through the latch, so a
   * waiter released by [[await]] has a happens-before edge to the [[complete]]
   * that set the value.
   */
  private final class OneShotValue[A] {
    private val latch = new CountDownLatch(1)
    private val ref = new AtomicReference[Option[A]](None)
    private val completed = new AtomicBoolean(false)

    /** Publishes the value (first writer wins) and releases any waiter. */
    def complete(value: A): Unit = {
      if (completed.compareAndSet(false, true)) {
        ref.set(Some(value))
        latch.countDown()
      }
    }

    /** Completes without a value (init failed through another event) and releases the waiter. */
    def signalWithoutValue(): Unit = {
      if (completed.compareAndSet(false, true)) {
        latch.countDown()
      }
    }

    /**
     * Blocks up to `timeoutMs` for a release, throwing [[TimeoutException]] if
     * none arrives so the caller handles the timeout on the exception path rather
     * than by inspecting a return value.
     */
    def await(timeoutMs: Long): Unit =
      if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
        throw new TimeoutException(
          s"timed out waiting for value after ${timeoutMs}ms")
      }

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
 * @param executionError the structured protocol error, when present: a worker
 *                       `ErrorResponse`, an init error, or a failure terminator
 *                       with an error. `null` when there is no structured cause:
 *                       a transport failure, a timeout, or a cancellation without
 *                       an error. Callers must null-check before use.
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
