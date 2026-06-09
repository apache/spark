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
package org.apache.spark.udf.worker.core

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.util.control.NonFatal

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.{Cancel, CancelResponse, DataRequest, DataResponse,
  ExecutionError, Finish, FinishResponse, Init, InitResponse}

/**
 * :: Experimental ::
 * One UDF execution on a worker -- the main interface Spark uses to run UDFs.
 *
 * A [[WorkerSession]] is the '''per-UDF-invocation''' handle that Spark
 * obtains from [[WorkerDispatcher#createSession]]. It carries the full
 * init / data-stream / finish lifecycle for a single UDF evaluation.
 * Concrete protocol implementations subclass this and provide the
 * protocol-specific request/response I/O; the base class owns the
 * [[SessionState]] machine and handles dispatcher-side cleanup via
 * [[WorkerHandle]].
 *
 * '''Usage:'''
 * {{{
 *   val session = dispatcher.createSession(securityScope = None)
 *   val initResponse = session.init(Init.newBuilder()...build())
 *   val results = session.process(inputBatches, () => Finish.newBuilder()...build())
 *   try {
 *     results.foreach(handleBatch)        // may throw on a UDF / data-phase error
 *   } finally {
 *     val termination = session.close(() => Cancel.newBuilder()...build())
 *   }
 * }}}
 *
 * '''Lifecycle:'''
 *  - [[init]] must be called exactly once before [[process]]; it returns the
 *    worker's [[InitResponse]] (init-callback result + metrics).
 *  - [[process]] must be called at most once per session. It returns the
 *    result iterator; consuming that iterator drives the data exchange and
 *    surfaces any UDF / data-phase error (as an exception from `next()` /
 *    `hasNext`). `Finish` is sent (via the supplied `finish` thunk) when the
 *    input iterator is exhausted, so the result iterator yields the trailing
 *    output before ending.
 *  - [[close]] must always be called (use try-finally). It finalizes the
 *    session: if the stream already finished it returns the [[Finish]]
 *    terminator, otherwise it cancels anything still in flight (sending the
 *    [[Cancel]] from the supplied thunk so the cancel callback is carried) and
 *    waits for the terminator. It returns the [[Termination]] either way.
 *
 * '''State machine.''' A single [[SessionState]] reference tracks the whole
 * session -- the call-ordering contract, the protocol phase, and the terminal
 * outcome -- so there is one source of truth rather than separate lifecycle and
 * wire machines:
 * {{{
 *   Created
 *     | init()
 *     v
 *   Initializing --(InitResponse ok)--> Initialized
 *     |                                     | process()
 *     | init failure / transport            v
 *     |                                   Streaming
 *     |                                     | input exhausted (Finish sent)
 *     |                                     v
 *     |                                   Finishing
 *     |                                     | terminator
 *     |                                     v
 *     +---------------------------------> (terminal)
 *                                           ^
 *                                           | terminator
 *                                         Cancelling
 *                                           ^
 *                                           | close() / cancel / ErrorResponse
 *                                           |   (from any non-terminal state)
 * }}}
 * The clean path (via Finishing) and the cancel path (via Cancelling) settle the
 * same `(terminal)`. The four terminals are:
 * {{{
 *   Finished(FinishResponse) | Cancelled(CancelResponse)
 *   Failed(ExecutionError)   | TransportFailed(Throwable)
 * }}}
 * The two clean terminals carry the worker's `FinishResponse` / `CancelResponse`
 * (metrics + finish/cancel callback `data`/`error`); the failure terminals carry
 * the cause. Reaching any terminal means the session is over -- no further
 * protocol writes are valid and [[close]] derives its [[Termination]] from it.
 * The terminal states subsume what would otherwise be a separate "closed" state:
 * [[close]] settles a terminal (cancelling first if needed) rather than moving to
 * a distinct closed marker.
 *
 * '''Who drives transitions.''' Transitions are split between this base and the
 * subclass, but all act on the one [[SessionState]]:
 *  - the base drives the API-call edges -- `Created -> Initializing` in [[init]]
 *    and `Initialized -> Streaming` in [[process]] -- with a single atomic
 *    transition before delegating to [[doInit]] / [[doProcess]]; an out-of-order
 *    or repeat call fails the transition and throws `IllegalStateException`;
 *  - the subclass drives the protocol-event edges as it exchanges messages
 *    (`Initializing -> Initialized` on `InitResponse`, `Streaming -> Finishing`
 *    on `Finish`, any non-terminal `-> Cancelling` on `Cancel`, and every
 *    terminal) through [[compareAndSetState]] and [[completeTerminal]].
 *
 * [[close]] is idempotent (it finalizes once and releases the worker exactly
 * once); a failed init leaves a terminal state, so a subsequent [[process]] is
 * rejected just like a call after [[close]].
 *
 * @param workerHandle dispatcher-side handle used to release the worker
 *                     on [[close]]. Package-private to the udf-worker
 *                     modules (`private[worker]`): the handle is a dispatcher
 *                     implementation detail.
 * @param logger       logger for lifecycle diagnostics.
 */
@Experimental
abstract class WorkerSession(
    private[worker] val workerHandle: WorkerHandle,
    protected val logger: WorkerLogger) {

  import WorkerSession.SessionState

  require(workerHandle != null, "workerHandle is required")

  // Single source of truth for the session: call-ordering contract, protocol
  // phase, and terminal outcome. CAS transitions make the contract independent
  // of caller threading, serialise the base's API-call edges against the
  // subclass's protocol-event edges, and let the first terminal win. See the
  // class-level "State machine" note.
  private val state = new AtomicReference[SessionState](SessionState.Created)

  // close() is the finalizer; this records whether it has run so the worker
  // handle is released exactly once. Kept separate from `state` because the
  // terminal that close() settles describes the protocol outcome, not "close was
  // called" -- a session can reach a terminal (finished / failed) before close().
  private val closeInvoked = new AtomicBoolean(false)

  /**
   * Initializes the UDF execution. Must be called exactly once before
   * [[process]]. Returns the worker's [[InitResponse]] (carrying the
   * init-callback result and metrics) on success; throws if the worker
   * reports an init error or the stream fails before init completes.
   *
   * @param message the [[Init]] message carrying the UDF body, data
   *                format, optional schemas, and any session context
   *                the worker needs to start processing.
   */
  final def init(message: Init): InitResponse = {
    if (!state.compareAndSet(SessionState.Created, SessionState.Initializing)) {
      throw new IllegalStateException(
        s"init must be called exactly once before process (current state: ${state.get()})")
    }
    val response = doInit(message)
    // doInit returned normally, so the worker accepted the session: ensure the
    // state reflects that. A subclass MAY advance `Initializing -> Initialized`
    // itself (e.g. to mark init resolved the moment `InitResponse` arrives), in
    // which case this is a no-op; and a terminal that raced in keeps the CAS
    // from clobbering it. On failure doInit throws and we never get here.
    state.compareAndSet(SessionState.Initializing, SessionState.Initialized)
    response
  }

  /**
   * Processes input data through the worker and returns the result iterator.
   *
   * Follows Spark's Iterator-to-Iterator pattern: input batches are streamed
   * to the worker, and result batches are lazily pulled from the returned
   * iterator. When the input iterator is exhausted the session sends `Finish`
   * -- built lazily from `finish` so the caller can carry a finish callback --
   * and the result iterator yields any trailing output before ending. A UDF or
   * data-phase error surfaces as an exception while the result iterator is
   * consumed.
   *
   * Must be called after [[init]] has succeeded and at most once per session.
   * Throws `IllegalStateException` if called before [[init]] completes, after
   * the session has terminated (including a failed init), or more than once.
   *
   * @param input  iterator of input data batches ([[DataRequest]], each
   *               wrapping e.g. an Arrow IPC payload).
   * @param finish supplies the [[Finish]] message sent once input is
   *               exhausted; invoked at most once. Defaults to an empty
   *               [[Finish]], which is sufficient when the caller has no extra
   *               information to convey to the worker on finish.
   * @return iterator of result data batches ([[DataResponse]]).
   */
  final def process(
      input: Iterator[DataRequest],
      finish: () => Finish = () => Finish.getDefaultInstance): Iterator[DataResponse] = {
    if (!state.compareAndSet(SessionState.Initialized, SessionState.Streaming)) {
      state.get() match {
        case SessionState.Created | SessionState.Initializing =>
          throw new IllegalStateException("process called before init completed")
        case s if s.isTerminal =>
          throw new IllegalStateException(
            s"process called after the session terminated (state: $s)")
        case _ =>
          throw new IllegalStateException("process has already been called on this session")
      }
    }
    doProcess(input, finish)
  }

  /**
   * Finalizes the session and releases resources. Idempotent; safe to call
   * from a `finally` block regardless of whether [[init]] or [[process]] ran.
   *
   * If the stream already finished (the result iterator was fully drained, so
   * `Finish` has been sent and `FinishResponse` received), this returns the
   * [[Termination.Finished]] terminator. Otherwise it cancels anything still in
   * flight -- sending the [[Cancel]] produced by `cancel` so the cancel
   * callback is carried -- waits for the terminator, and returns it. The
   * `cancel` thunk is invoked only when a cancel actually needs to be sent.
   *
   * Does not raise application or transport failures -- they are reported through
   * the returned [[Termination]], not thrown (though a thread interruption, or
   * another fatal error, may still propagate). An execution error yields
   * [[Termination.Failed]] and a transport failure / timeout yields
   * [[Termination.TransportFailed]], each carrying the cause. A data-phase
   * failure also surfaces while the [[process]] result iterator is consumed, but
   * a failure that occurs during finish/close -- after
   * the data is drained -- is reported only here, so the caller must inspect the
   * [[Termination]]. After the terminator is settled the worker handle's ref
   * count is decremented; if [[isWorkerSalvageable]] is false the handle is
   * marked invalid so the dispatcher will not recycle the worker.
   *
   * @param cancel supplies the [[Cancel]] message used to abort in-flight work;
   *               invoked at most once, and only if the stream had not already
   *               finished. Defaults to an empty [[Cancel]], which is sufficient
   *               when the caller has no extra information to convey to the
   *               worker on cancellation (e.g. a reason).
   * @return the [[Termination]] the stream settled on.
   */
  final def close(cancel: () => Cancel = () => Cancel.getDefaultInstance): Termination = {
    val firstClose = closeInvoked.compareAndSet(false, true)
    try {
      doClose(cancel)
      // Enforce the doClose post-condition: it must leave the session terminal,
      // because isWorkerSalvageable (read below) classifies the worker off the
      // settled SessionState. A non-terminal state here is a subclass contract
      // violation. close() is the finalizer and must not throw, so rather than
      // raise (or silently misclassify a healthy-looking worker as salvageable)
      // we settle a failure terminal -- which makes the worker unsalvageable --
      // and log loudly. Terminals are sticky, so this is a no-op on the normal
      // path where doClose already settled one.
      if (!currentState.isTerminal) {
        completeTerminal(SessionState.TransportFailed(new IllegalStateException(
          s"doClose returned without settling a terminal (state: ${currentState})")))
        logger.warn(
          "doClose did not settle a terminal; treating the worker as unsalvageable")
      }
      // Derive the result from the settled terminal rather than trusting
      // doClose's return value, so the Termination always agrees with the state
      // close() classified the worker on -- in particular when the guard above
      // had to override a misbehaving doClose. On the normal path this equals
      // what doClose returned.
      settledTermination
    } finally {
      if (firstClose) {
        // close() is a finalizer (commonly invoked from a task-completion
        // listener), so it avoids throwing: a misbehaving handle is logged via
        // NonFatal, not propagated (a thread interrupt may still escape).
        // isWorkerSalvageable reads the settled terminal (doClose, or the guard
        // above, leaves the session terminal). releaseSession is still attempted
        // even if markInvalid fails, so the ref count is not leaked; markInvalid
        // runs first so a released worker is never recycled.
        try {
          if (!isWorkerSalvageable) workerHandle.markInvalid()
        } catch {
          case NonFatal(e) =>
            logger.warn(s"markInvalid for worker ${workerHandle.id} on close() failed", e)
        }
        try {
          workerHandle.releaseSession()
        } catch {
          case NonFatal(e) =>
            logger.warn(s"releaseSession for worker ${workerHandle.id} on close() failed", e)
        }
      }
    }
  }

  /**
   * Subclass hook for [[init]]. Called exactly once, after the state
   * transition to `Initializing`. Implementations MUST establish the worker
   * conversation here, not earlier, and advance `Initializing -> Initialized`
   * (via [[compareAndSetState]]) once the worker accepts the session; on
   * failure they settle a terminal (via [[completeTerminal]]) and throw.
   */
  protected def doInit(message: Init): InitResponse

  /**
   * Subclass hook for [[process]]. Called at most once, after the state
   * transition to `Streaming`.
   */
  protected def doProcess(
      input: Iterator[DataRequest],
      finish: () => Finish): Iterator[DataResponse]

  /**
   * Subclass hook for [[close]]. Settles the stream terminator (cancelling
   * in-flight work via `cancel` if it has not already finished), releases any
   * protocol resources, and returns the [[Termination]] (typically via
   * [[settledTermination]]).
   *
   * '''Must settle a terminal before returning''' (via [[completeTerminal]] /
   * [[settledTermination]]): [[close]] reads [[isWorkerSalvageable]] -- which
   * inspects the settled [[SessionState]] -- the instant this returns, so a
   * non-terminal state here would misclassify the worker. The base asserts this
   * post-condition.
   *
   * '''Must be thread-safe and idempotent.''' [[close]] does not serialise calls
   * to this hook: it can be invoked '''concurrently''' (e.g. a task-completion
   * listener on one thread while the result iterator is consumed on another) and
   * '''repeatedly''' (a second [[close]]). Implementations must tolerate both --
   * e.g. settle the terminal through the first-wins [[completeTerminal]] and
   * guard any wire write under the same lock the data path uses. The `cancel`
   * thunk must be evaluated at most once across all callers.
   *
   * As the cleanup path it does not re-throw a failed terminal -- it reports a
   * best-effort [[Termination]] (the underlying failure surfaces through the
   * result iterator).
   */
  protected def doClose(cancel: () => Cancel): Termination

  /**
   * Whether the underlying worker is in a state safe to reuse after this
   * session ends. The default treats only a dead or unknown transport as
   * unsafe: a [[SessionState.TransportFailed]] terminal (transport failure,
   * timeout, or interrupt) -- or a session that never settled -- leaves the
   * worker in an unknown state and is not salvageable. Every other terminal is
   * salvageable: a clean [[SessionState.Finished]], a cooperative
   * [[SessionState.Cancelled]], and also an execution [[SessionState.Failed]],
   * which is typically a user-code (UDF) error reported by a still-healthy
   * worker rather than a worker fault. A `false` result tells [[close]] to mark
   * `workerHandle` invalid so no reuse pool recycles the worker. Subclasses may
   * override for protocol-specific nuances.
   */
  protected def isWorkerSalvageable: Boolean = state.get() match {
    case _: SessionState.TransportFailed => false
    case t if t.isTerminal => true
    case _ => false
  }

  // ---- State-machine primitives for subclasses ------------------------------

  /** The current [[SessionState]]. */
  protected final def currentState: SessionState = state.get()

  /**
   * Atomically advances the state from `expect` to `update`, returning whether
   * the transition was applied. Used by subclasses to drive the protocol-event
   * edges (e.g. `Initializing -> Initialized`, `Streaming -> Finishing`). A
   * terminal that arrived first makes the CAS fail, so the terminal wins.
   * Terminals must be reached via [[completeTerminal]], not this method.
   */
  protected final def compareAndSetState(
      expect: SessionState, update: SessionState): Boolean =
    state.compareAndSet(expect, update)

  /**
   * Settles the session on its terminal outcome. The first caller wins (an
   * already-terminal state is left untouched); only the winner runs
   * [[onTerminalSettled]] and this returns `true`. All later callers observe
   * the existing terminal and return `false`.
   */
  protected final def completeTerminal(terminal: SessionState.Terminal): Boolean = {
    var cur = state.get()
    while (!cur.isTerminal) {
      if (state.compareAndSet(cur, terminal)) {
        onTerminalSettled(terminal)
        return true
      }
      cur = state.get()
    }
    false
  }

  /**
   * Hook invoked exactly once, by the caller that settles the terminal in
   * [[completeTerminal]]. Subclasses override it to wake whatever is blocked on
   * the terminator (output queues, latches). The default does nothing.
   */
  protected def onTerminalSettled(terminal: SessionState.Terminal): Unit = ()

  /**
   * Derives the [[Termination]] from the settled terminal, for [[doClose]] to
   * return -- a 1:1 mapping of the four [[SessionState]] terminals. The clean
   * terminators carry the worker's response proto (metrics + finish/cancel
   * callback data/error); the failure terminals carry their cause
   * ([[Termination.Failed]] the [[ExecutionError]], [[Termination.TransportFailed]]
   * the [[Throwable]]) and have no proto terminator, so no metrics. The failure
   * also surfaces through the result iterator when it is consumed, but is carried
   * here too because an error raised during finish/close -- after the data has
   * been drained -- reaches no one through the iterator. Throws only if called
   * before a terminal is settled, which [[doClose]] must not do.
   */
  protected final def settledTermination: Termination = state.get() match {
    case SessionState.Finished(response) => Termination.Finished(response)
    case SessionState.Cancelled(response) => Termination.Cancelled(response)
    case SessionState.Failed(error) => Termination.Failed(error)
    case SessionState.TransportFailed(cause) => Termination.TransportFailed(cause)
    case other =>
      throw new IllegalStateException(s"settledTermination called in non-terminal state: $other")
  }
}

object WorkerSession {

  /**
   * The single state machine for a [[WorkerSession]]: the call-ordering
   * contract, the protocol phase, and the terminal outcome. The normal
   * progression is `Created -> Initializing -> Initialized -> Streaming ->
   * Finishing -> Finished`; `close`/cancel and errors fork to `Cancelling` and
   * the failure terminals. See [[WorkerSession]] for the full diagram and which
   * party drives each edge.
   *
   * Visible to the udf-worker modules (`private[worker]`) so concrete protocol
   * subclasses can drive and inspect the state; it is an implementation detail,
   * not part of the public API.
   */
  private[worker] sealed trait SessionState {
    /** Whether this is a terminal state (the session is over). */
    def isTerminal: Boolean = false
  }

  private[worker] object SessionState {
    /** Constructed; [[WorkerSession.init]] not yet called. */
    case object Created extends SessionState
    /** [[WorkerSession.init]] is in flight; awaiting the worker's `InitResponse`. */
    case object Initializing extends SessionState
    /** `InitResponse` OK; [[WorkerSession.process]] not yet called. */
    case object Initialized extends SessionState
    /** [[WorkerSession.process]] is active; input may still be sent. */
    case object Streaming extends SessionState
    /** Input exhausted, `Finish` sent; awaiting trailing output + terminator. */
    case object Finishing extends SessionState
    /** `Cancel` sent (engine cancel or post-error cleanup); awaiting terminator. */
    case object Cancelling extends SessionState

    /** Terminal outcomes; the session is over and no further writes are valid. */
    sealed trait Terminal extends SessionState {
      override def isTerminal: Boolean = true
    }
    /** Clean `FinishResponse` (carries metrics + finish-callback data/error). */
    final case class Finished(response: FinishResponse) extends Terminal
    /** `CancelResponse` (carries metrics + cancel-callback error). */
    final case class Cancelled(response: CancelResponse) extends Terminal
    /** Execution error with no proto terminator (e.g. ERROR before init). */
    final case class Failed(error: ExecutionError) extends Terminal
    /** Transport failure, timeout, or interrupt. */
    final case class TransportFailed(cause: Throwable) extends Terminal
  }
}
