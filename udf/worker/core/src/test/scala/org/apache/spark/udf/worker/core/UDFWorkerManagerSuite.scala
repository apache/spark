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

import scala.collection.mutable.ArrayBuffer

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, verify, when}
import org.mockito.invocation.InvocationOnMock
// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker._

/**
 * A minimal [[WorkerSession]] that tracks cancel/close calls.
 */
private class NoOpWorkerSession
    extends WorkerSession(WorkerLogger.NoOp) {
  var cancelled: Boolean = false
  override protected def doInit(msg: InitMessage): Unit = {}
  override protected def doProcess(
      input: Iterator[Array[Byte]]): Iterator[Array[Byte]] =
    Iterator.empty
  override protected def doCancel(): Unit = { cancelled = true }
  override protected def doClose(): Unit = {}
}

/**
 * Holds a test [[UDFWorkerManager]] and all observable state,
 * so tests can assert on whichever fields they care about.
 */
private case class TestManagerFixture(
    manager: UDFWorkerManager,
    createdDispatchers: ArrayBuffer[WorkerDispatcher],
    closedDispatchers: ArrayBuffer[WorkerDispatcher],
    doStopCalls: ArrayBuffer[Boolean],
    createdSessions: ArrayBuffer[NoOpWorkerSession])

private object TestManagerFixture {
  def apply(): TestManagerFixture = {
    val createdDispatchers = ArrayBuffer[WorkerDispatcher]()
    val closedDispatchers = ArrayBuffer[WorkerDispatcher]()
    val doStopCalls = ArrayBuffer[Boolean]()
    val createdSessions = ArrayBuffer[NoOpWorkerSession]()

    val manager = new UDFWorkerManager {
      override protected def doCreateDispatcher(
          workerSpec: UDFWorkerSpecification,
          logger: WorkerLogger): WorkerDispatcher = {
        val dispatcher = mock(classOf[WorkerDispatcher])
        when(dispatcher.createSession(
          any[Option[WorkerSecurityScope]]))
          .thenAnswer((_: InvocationOnMock) => {
            val session = new NoOpWorkerSession()
            createdSessions += session
            session
          })
        createdDispatchers += dispatcher
        dispatcher
      }
      override protected def onAllDispatcherSessionsClosed(
          dispatcher: WorkerDispatcher): Unit = {
        closedDispatchers += dispatcher
      }
      override protected def doStop(): Unit = {
        doStopCalls += true
      }
    }

    TestManagerFixture(
      manager, createdDispatchers, closedDispatchers,
      doStopCalls, createdSessions)
  }
}

class UDFWorkerManagerSuite
    extends AnyFunSuite { // scalastyle:ignore funsuite

  private def makeSpec(command: String): UDFWorkerSpecification = {
    val callable = ProcessCallable.newBuilder()
    callable.addCommand(command)
    val caps = WorkerCapabilities.newBuilder()
      .addSupportedDataFormats(UDFWorkerDataFormat.ARROW)
      .addSupportedCommunicationPatterns(
        UDFProtoCommunicationPattern.BIDIRECTIONAL_STREAMING)
    val conn = WorkerConnectionSpec.newBuilder()
      .setTcp(LocalTcpConnection.newBuilder())
    val props = UDFWorkerProperties.newBuilder()
      .setConnection(conn)
    val direct = DirectWorker.newBuilder()
      .setRunner(callable).setProperties(props)
    UDFWorkerSpecification.newBuilder()
      .setEnvironment(WorkerEnvironment.newBuilder())
      .setCapabilities(caps).setDirect(direct).build()
  }

  test("Same spec reuses the same dispatcher") {
    val fixture = TestManagerFixture()
    val spec = makeSpec("worker.bin")

    val s1 = fixture.manager.createSession(spec)
    val s2 = fixture.manager.createSession(spec)

    assert(fixture.createdDispatchers.size === 1)
    verify(
      fixture.createdDispatchers.head,
      org.mockito.Mockito.times(2))
      .createSession(any[Option[WorkerSecurityScope]])

    s1.close()
    s2.close()
  }

  test("Structurally equal specs reuse the same dispatcher") {
    val fixture = TestManagerFixture()
    val spec1 = makeSpec("worker.bin")
    val spec2 = makeSpec("worker.bin")
    assert(spec1 ne spec2)
    assert(spec1 == spec2)

    val s1 = fixture.manager.createSession(spec1)
    val s2 = fixture.manager.createSession(spec2)

    assert(fixture.createdDispatchers.size === 1)

    s1.close()
    s2.close()
  }

  test("Different specs create different dispatchers") {
    val fixture = TestManagerFixture()

    val sA = fixture.manager.createSession(makeSpec("worker-a.bin"))
    val sB = fixture.manager.createSession(makeSpec("worker-b.bin"))

    assert(fixture.createdDispatchers.size === 2)
    assert(
      fixture.createdDispatchers(0) ne
        fixture.createdDispatchers(1))

    sA.close()
    sB.close()
  }

  test("onAllDispatcherSessionsClosed when last session closes") {
    val fixture = TestManagerFixture()
    val spec = makeSpec("worker.bin")

    val s1 = fixture.manager.createSession(spec)
    val s2 = fixture.manager.createSession(spec)

    s1.close()
    assert(fixture.closedDispatchers.isEmpty)

    s2.close()
    assert(fixture.closedDispatchers.size === 1)
    assert(
      fixture.closedDispatchers.head eq
        fixture.createdDispatchers.head)
  }

  test("onAllDispatcherSessionsClosed not called while sessions remain") {
    val fixture = TestManagerFixture()
    val spec = makeSpec("worker.bin")

    val s1 = fixture.manager.createSession(spec)
    val s2 = fixture.manager.createSession(spec)
    val s3 = fixture.manager.createSession(spec)

    s1.close()
    s2.close()
    assert(fixture.closedDispatchers.isEmpty)

    s3.close()
    assert(fixture.closedDispatchers.size === 1)
  }

  test("New dispatcher after all sessions closed") {
    val fixture = TestManagerFixture()
    val spec = makeSpec("worker.bin")

    val s1 = fixture.manager.createSession(spec)
    val s2 = fixture.manager.createSession(spec)
    assert(fixture.createdDispatchers.size === 1)

    s1.close()
    s2.close()
    assert(fixture.closedDispatchers.size === 1)

    val s3 = fixture.manager.createSession(spec)
    assert(fixture.createdDispatchers.size === 2)
    assert(
      fixture.createdDispatchers(0) ne
        fixture.createdDispatchers(1))

    s3.close()
    assert(fixture.closedDispatchers.size === 2)
  }

  test("Stop closes all cached dispatchers") {
    val fixture = TestManagerFixture()

    fixture.manager.createSession(makeSpec("worker-a.bin"))
    fixture.manager.createSession(makeSpec("worker-b.bin"))
    fixture.manager.stop()

    fixture.createdDispatchers.foreach(d => verify(d).close())
  }

  test("Stop calls doStop for subclass cleanup") {
    val fixture = TestManagerFixture()

    fixture.manager.createSession(makeSpec("worker.bin"))
    assert(fixture.doStopCalls.isEmpty)

    fixture.manager.stop()
    assert(fixture.doStopCalls.size === 1)
  }

  test("Stop cancels active sessions that were not closed") {
    val fixture = TestManagerFixture()
    val spec = makeSpec("worker.bin")

    val s1 = fixture.manager.createSession(spec)
    fixture.manager.createSession(spec)
    s1.close()

    assert(fixture.createdSessions.size === 2)
    assert(!fixture.createdSessions(0).cancelled)
    assert(!fixture.createdSessions(1).cancelled)

    fixture.manager.stop()

    assert(fixture.createdSessions(1).cancelled)
  }

  test("Stop does not cancel already closed sessions") {
    val fixture = TestManagerFixture()
    val spec = makeSpec("worker.bin")

    val s1 = fixture.manager.createSession(spec)
    s1.close()

    fixture.manager.stop()

    assert(!fixture.createdSessions(0).cancelled)
  }

  test("createSession throws after stop") {
    val fixture = TestManagerFixture()
    fixture.manager.stop()

    intercept[IllegalStateException] {
      fixture.manager.createSession(makeSpec("worker.bin"))
    }
  }
}
