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

// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker.{Cancel, CancelResponse, DataRequest, DataResponse,
  ExecutionError, Finish, FinishResponse, Init, InitResponse}
import org.apache.spark.udf.worker.core.WorkerSession.SessionState

/**
 * Unit tests for the [[WorkerSession]] state machine, exercised through a fake
 * subclass that drives the protocol-event edges directly with no transport.
 *
 * The concrete transport-backed sessions test this same machine end-to-end over
 * the wire; these tests pin the transport-agnostic base-class contract on its
 * own: call ordering, terminal settling (first-wins), the [[Termination]]
 * mapping, and the `close()` finalizer (release-once, invalidation, and the
 * doClose post-condition guard).
 */
class WorkerSessionSuite extends AnyFunSuite {
// scalastyle:on funsuite

  /** A [[WorkerHandle]] that counts the lifecycle callbacks made against it. */
  private final class RecordingHandle extends WorkerHandle {
    var released = 0
    var invalidated = 0
    override def id: String = "test-worker"
    override def markInvalid(): Unit = invalidated += 1
    override def releaseSession(): Unit = released += 1
  }

  /**
   * A [[WorkerSession]] whose protocol hooks are supplied by the test and whose
   * protected state-machine primitives are re-exposed, so a test can drive the
   * machine (settle terminals, CAS edges) without a real worker.
   */
  private final class FakeWorkerSession(
      handle: WorkerHandle = new RecordingHandle,
      onInit: FakeWorkerSession => InitResponse = _ => InitResponse.getDefaultInstance,
      onProcess: (FakeWorkerSession, Iterator[DataRequest], () => Finish) =>
        Iterator[DataResponse] = (_, _, _) => Iterator.empty[DataResponse],
      onCloseHook: (FakeWorkerSession, () => Cancel) => Termination =
        (self, _) => {
          self.settle(SessionState.Finished(FinishResponse.getDefaultInstance))
          self.term
        })
    extends WorkerSession(handle, WorkerLogger.NoOp) {

    var terminalSettledCount = 0

    override protected def doInit(message: Init): InitResponse = onInit(this)
    override protected def doProcess(
        input: Iterator[DataRequest], finish: () => Finish): Iterator[DataResponse] =
      onProcess(this, input, finish)
    override protected def doClose(cancel: () => Cancel): Termination = onCloseHook(this, cancel)
    override protected def onTerminalSettled(terminal: SessionState.Terminal): Unit =
      terminalSettledCount += 1

    // Re-expose the protected primitives so the test can drive the machine.
    def state: SessionState = currentState
    def cas(expect: SessionState, update: SessionState): Boolean =
      compareAndSetState(expect, update)
    def settle(t: SessionState.Terminal): Boolean = completeTerminal(t)
    def term: Termination = settledTermination
  }

  test("init returns the worker InitResponse and advances Created -> Initialized") {
    val resp = InitResponse.getDefaultInstance
    val s = new FakeWorkerSession(onInit = _ => resp)
    assert(s.state == SessionState.Created)
    assert(s.init(Init.getDefaultInstance) eq resp)
    assert(s.state == SessionState.Initialized)
  }

  test("init must be called exactly once") {
    val s = new FakeWorkerSession()
    s.init(Init.getDefaultInstance)
    val ex = intercept[IllegalStateException](s.init(Init.getDefaultInstance))
    assert(ex.getMessage.contains("exactly once"))
  }

  test("process before init is rejected") {
    val s = new FakeWorkerSession()
    val ex = intercept[IllegalStateException](s.process(Iterator.empty))
    assert(ex.getMessage.contains("before init"))
  }

  test("process advances to Streaming and may only be called once") {
    val s = new FakeWorkerSession()
    s.init(Init.getDefaultInstance)
    s.process(Iterator.empty)
    assert(s.state == SessionState.Streaming)
    val ex = intercept[IllegalStateException](s.process(Iterator.empty))
    assert(ex.getMessage.contains("already been called"))
  }

  test("a failed doInit settles a terminal and rejects a later process") {
    val err = ExecutionError.getDefaultInstance
    val s = new FakeWorkerSession(onInit = self => {
      self.settle(SessionState.Failed(err))
      throw new RuntimeException("init boom")
    })
    val initEx = intercept[RuntimeException](s.init(Init.getDefaultInstance))
    assert(initEx.getMessage.contains("init boom"))
    assert(s.state == SessionState.Failed(err))
    val ex = intercept[IllegalStateException](s.process(Iterator.empty))
    assert(ex.getMessage.contains("terminated"))
  }

  test("close returns the Termination produced by doClose") {
    val resp = FinishResponse.getDefaultInstance
    val s = new FakeWorkerSession(onCloseHook = (self, _) => {
      self.settle(SessionState.Finished(resp))
      self.term
    })
    assert(s.close() == Termination.Finished(resp))
  }

  test("close releases the worker exactly once and leaves a salvageable worker valid") {
    val h = new RecordingHandle
    val s = new FakeWorkerSession(handle = h)  // default doClose settles Finished
    s.close()
    s.close()
    assert(h.released == 1)
    assert(h.invalidated == 0)
  }

  test("close marks a transport-failed worker invalid") {
    val h = new RecordingHandle
    val s = new FakeWorkerSession(handle = h, onCloseHook = (self, _) => {
      self.settle(SessionState.TransportFailed(new RuntimeException("transport down")))
      self.term
    })
    s.close()
    assert(h.invalidated == 1)
    assert(h.released == 1)
  }

  test("close leaves a worker salvageable after an execution Failed") {
    val h = new RecordingHandle
    val err = ExecutionError.getDefaultInstance
    // A Failed terminal is typically a user-code (UDF) error, not a worker fault,
    // so the worker stays reusable: markInvalid must NOT be called.
    val s = new FakeWorkerSession(handle = h, onCloseHook = (self, _) => {
      self.settle(SessionState.Failed(err))
      self.term
    })
    assert(s.close() == Termination.Failed(err))
    assert(h.invalidated == 0)
    assert(h.released == 1)
  }

  test("close enforces the doClose terminal post-condition") {
    val h = new RecordingHandle
    // doClose returns a Termination without settling any terminal -- a subclass
    // contract violation. close() must settle a TransportFailed terminal, treat
    // the worker as unsalvageable, and return a Termination consistent with that
    // settled state rather than the (untrustworthy) value doClose produced.
    val s = new FakeWorkerSession(handle = h,
      onCloseHook = (_, _) => Termination.Finished(FinishResponse.getDefaultInstance))
    val termination = s.close()
    assert(s.state.isInstanceOf[SessionState.TransportFailed])
    assert(termination.isInstanceOf[Termination.TransportFailed])
    assert(h.invalidated == 1)
  }

  test("close swallows a non-fatal worker-handle failure") {
    val throwingHandle = new WorkerHandle {
      override def id: String = "boom"
      override def markInvalid(): Unit = throw new RuntimeException("markInvalid boom")
      override def releaseSession(): Unit = throw new RuntimeException("releaseSession boom")
    }
    val cause = new RuntimeException("transport down")
    // A TransportFailed terminal makes the worker unsalvageable, so close()
    // attempts both markInvalid() and releaseSession() -- here both throw.
    // close() must swallow them and still return the settled termination.
    val s = new FakeWorkerSession(handle = throwingHandle, onCloseHook = (self, _) => {
      self.settle(SessionState.TransportFailed(cause))
      self.term
    })
    assert(s.close() == Termination.TransportFailed(cause))
  }

  test("close still releases the worker when markInvalid throws") {
    // markInvalid throws but releaseSession succeeds: close() must swallow the
    // markInvalid failure and still attempt releaseSession, so the ref count is
    // not leaked. A TransportFailed terminal makes the worker unsalvageable, so
    // markInvalid is reached on the way to releaseSession.
    var released = 0
    val handle = new WorkerHandle {
      override def id: String = "throws-on-invalidate"
      override def markInvalid(): Unit = throw new RuntimeException("markInvalid boom")
      override def releaseSession(): Unit = released += 1
    }
    val cause = new RuntimeException("transport down")
    val s = new FakeWorkerSession(handle = handle, onCloseHook = (self, _) => {
      self.settle(SessionState.TransportFailed(cause))
      self.term
    })
    assert(s.close() == Termination.TransportFailed(cause))
    assert(released == 1, "releaseSession must be attempted even when markInvalid throws")
  }

  test("completeTerminal is first-wins and runs onTerminalSettled once") {
    val first = SessionState.Finished(FinishResponse.getDefaultInstance)
    val s = new FakeWorkerSession()
    assert(s.settle(first))
    assert(!s.settle(SessionState.Cancelled(CancelResponse.getDefaultInstance)))
    assert(s.state == first)
    assert(s.terminalSettledCount == 1)
  }

  test("compareAndSetState drives non-terminal edges and loses to a settled terminal") {
    val s = new FakeWorkerSession()
    s.init(Init.getDefaultInstance)
    assert(s.cas(SessionState.Initialized, SessionState.Streaming))
    assert(s.state == SessionState.Streaming)
    assert(s.settle(SessionState.Failed(ExecutionError.getDefaultInstance)))
    assert(!s.cas(SessionState.Streaming, SessionState.Finishing))
  }

  test("settledTermination maps each terminal to its Termination") {
    val fin = FinishResponse.getDefaultInstance
    val can = CancelResponse.getDefaultInstance
    val err = ExecutionError.getDefaultInstance
    val cause = new RuntimeException("transport down")
    def termFor(t: SessionState.Terminal): Termination = {
      val s = new FakeWorkerSession()
      s.settle(t)
      s.term
    }
    assert(termFor(SessionState.Finished(fin)) == Termination.Finished(fin))
    assert(termFor(SessionState.Cancelled(can)) == Termination.Cancelled(can))
    assert(termFor(SessionState.Failed(err)) == Termination.Failed(err))
    assert(termFor(SessionState.TransportFailed(cause)) == Termination.TransportFailed(cause))
  }

  test("settledTermination throws before a terminal is settled") {
    val s = new FakeWorkerSession()
    intercept[IllegalStateException](s.term)
  }
}
