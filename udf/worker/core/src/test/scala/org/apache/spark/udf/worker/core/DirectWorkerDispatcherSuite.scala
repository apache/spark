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

import java.io.File
import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFileAttributeView

import scala.jdk.CollectionConverters._

// scalastyle:off funsuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker.{
  DirectWorker, LocalTcpConnection, ProcessCallable, UDFWorkerProperties,
  UDFWorkerSpecification, UnixDomainSocket, WorkerConnectionSpec,
  WorkerEnvironment}
import org.apache.spark.udf.worker.core.direct.{DirectUnixSocketWorkerDispatcher,
  DirectWorkerException, DirectWorkerProcess, DirectWorkerSession,
  DirectWorkerTimeoutException}

/**
 * A [[WorkerConnection]] test implementation that considers the connection
 * active as long as the socket file exists on disk. Inherits socket-file
 * deletion from [[UnixSocketWorkerConnection.close]].
 */
class SocketFileConnection(socketPath: String)
    extends UnixSocketWorkerConnection(socketPath) {
  override def isActive: Boolean = new File(socketPath).exists()
}

/**
 * A stub [[DirectWorkerSession]] for process-lifecycle tests that don't
 * need actual data transmission.
 *
 * TODO: [[cancel]] is a no-op here. Once a concrete [[DirectWorkerSession]]
 *   with real data-plane wiring lands, add tests exercising cancel() in
 *   particular: cancel from a different thread than process(), cancel
 *   after process() has returned, and cancel before init (should be a
 *   no-op). Tracking the thread-safety contract in the docstring on
 *   [[org.apache.spark.udf.worker.core.WorkerSession.cancel]].
 */
class StubWorkerSession(
    workerProcess: DirectWorkerProcess) extends DirectWorkerSession(workerProcess) {

  override protected def doInit(message: InitMessage): Unit = {}

  override protected def doProcess(
      input: Iterator[Array[Byte]]): Iterator[Array[Byte]] =
    Iterator.empty

  override def cancel(): Unit = {}
}

/**
 * A [[DirectUnixSocketWorkerDispatcher]] subclass for testing that uses
 * a socket-file connection and stub sessions instead of a real protocol
 * implementation.
 */
class TestDirectWorkerDispatcher(spec: UDFWorkerSpecification)
    extends DirectUnixSocketWorkerDispatcher(spec) {

  override protected def createConnection(
      socketPath: String): UnixSocketWorkerConnection =
    new SocketFileConnection(socketPath)

  override protected def createSessionForWorker(
      worker: DirectWorkerProcess): WorkerSession =
    new StubWorkerSession(worker)
}

/**
 * Tests for [[DirectWorkerDispatcher]] process lifecycle: spawning workers
 * and terminating them on close.
 */
class DirectWorkerDispatcherSuite
    extends AnyFunSuite with BeforeAndAfterEach {
// scalastyle:on funsuite

  private val echoWorkerScript =
    """
      |#!/bin/bash
      |SOCKET_PATH=""
      |while [[ $# -gt 0 ]]; do
      |  case "$1" in
      |    --connection) SOCKET_PATH="$2"; shift 2 ;;
      |    *) shift ;;
      |  esac
      |done
      |cleanup() { rm -f "$SOCKET_PATH"; exit 0; }
      |trap cleanup SIGTERM
      |touch "$SOCKET_PATH"
      |while true; do sleep 1; done
    """.stripMargin.trim

  private def defaultRunner: ProcessCallable = ProcessCallable.newBuilder()
    .addCommand("bash").addCommand("-c").addCommand(echoWorkerScript).addCommand("--")
    .build()

  private def udsProperties: UDFWorkerProperties = UDFWorkerProperties.newBuilder()
    .setConnection(WorkerConnectionSpec.newBuilder()
      .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance)
      .build())
    .build()

  private def directWorker(runner: ProcessCallable): DirectWorker =
    DirectWorker.newBuilder().setRunner(runner).setProperties(udsProperties).build()

  private def specWithRunner(runner: ProcessCallable): UDFWorkerSpecification =
    UDFWorkerSpecification.newBuilder()
      .setDirect(directWorker(runner))
      .build()

  private def specWithEnv(
      runner: ProcessCallable = defaultRunner,
      env: WorkerEnvironment): UDFWorkerSpecification =
    UDFWorkerSpecification.newBuilder()
      .setEnvironment(env)
      .setDirect(directWorker(runner))
      .build()

  private var dispatcher: TestDirectWorkerDispatcher = _

  override def afterEach(): Unit = {
    if (dispatcher != null) {
      dispatcher.close()
      dispatcher = null
    }
    super.afterEach()
  }

  // Narrow the publicly-typed WorkerSession returned by `createSession` back
  // down to StubWorkerSession in one place, with a descriptive failure if
  // the cast is ever wrong, so individual tests don't scatter `asInstanceOf`
  // (which would throw ClassCastException rather than a useful message).
  private def createStubSession(): StubWorkerSession =
    dispatcher.createSession(None) match {
      case stub: StubWorkerSession => stub
      case other => fail(
        s"Expected StubWorkerSession, got ${other.getClass.getSimpleName}")
    }

  // The whole suite uses UDS as the only transport, so reaching past the
  // generic WorkerConnection abstraction to read the socket path is fine.
  private def udsPath(w: DirectWorkerProcess): String = w.connection match {
    case uds: UnixSocketWorkerConnection => uds.socketPath
    case other => fail(
      s"Expected UnixSocketWorkerConnection, got ${other.getClass.getSimpleName}")
  }

  test("creates a worker and session") {
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(defaultRunner))

    val session = createStubSession()
    val worker = session.workerProcess

    assert(worker.isAlive, "worker should be alive after creation")
    assert(worker.activeSessions == 1, "should have 1 active session")
    assert(new File(udsPath(worker)).exists(), "socket file should exist")

    session.close()
    assert(worker.activeSessions == 0, "should have 0 sessions after close")
  }

  test("concurrent createSession calls produce distinct workers") {
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(defaultRunner))

    val threads = 8
    val sessions = new java.util.concurrent.ConcurrentLinkedQueue[StubWorkerSession]()
    val startGate = new java.util.concurrent.CountDownLatch(1)
    val doneGate = new java.util.concurrent.CountDownLatch(threads)
    val errors = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()

    (1 to threads).foreach { _ =>
      new Thread(() => {
        try {
          startGate.await()
          sessions.add(createStubSession())
        } catch {
          case t: Throwable => errors.add(t)
        } finally {
          doneGate.countDown()
        }
      }).start()
    }
    startGate.countDown()
    assert(doneGate.await(30, java.util.concurrent.TimeUnit.SECONDS),
      "createSession threads did not finish in time")

    assert(errors.isEmpty,
      s"unexpected errors during concurrent createSession: ${errors.toArray.mkString(", ")}")
    assert(sessions.size == threads, "expected one session per thread")

    val sessionList = sessions.asScala.toList
    val workerObjects = sessionList.map(_.workerProcess)
    assert(workerObjects.distinct.length == threads,
      "each session should have its own DirectWorkerProcess")
    // Object-identity is not sufficient on its own: a future regression
    // that accidentally shared underlying transport resources could still
    // hand out distinct DirectWorkerProcess wrappers pointing at the same
    // socket. Verify socket paths are unique too.
    val socketPaths = workerObjects.map(udsPath)
    assert(socketPaths.distinct.length == threads,
      s"each worker should have its own socket path, got $socketPaths")

    sessionList.foreach(_.close())
  }

  test("close shuts down all workers via SIGTERM") {
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(defaultRunner))

    val session1 = createStubSession()
    val session2 = createStubSession()

    val worker1 = session1.workerProcess
    val worker2 = session2.workerProcess

    session1.close()
    session2.close()
    dispatcher.close()
    dispatcher = null

    assert(!worker1.process.isAlive, "worker1 should be terminated")
    assert(!worker2.process.isAlive, "worker2 should be terminated")
  }

  test("close escalates to SIGKILL when worker ignores SIGTERM") {
    // The worker traps SIGTERM so the graceful stop is ineffective; the
    // dispatcher must escalate to SIGKILL via destroyForciblyAndReap.
    // Using a short gracefulTimeoutMs (500ms) keeps the test bounded:
    // max close time is gracefulTimeoutMs + SIGKILL_REAP_TIMEOUT_MS.
    val sigtermIgnoringScript =
      """
        |#!/bin/bash
        |SOCKET_PATH=""
        |while [[ $# -gt 0 ]]; do
        |  case "$1" in
        |    --connection) SOCKET_PATH="$2"; shift 2 ;;
        |    *) shift ;;
        |  esac
        |done
        |touch "$SOCKET_PATH"
        |trap '' SIGTERM
        |while true; do sleep 1; done
      """.stripMargin.trim
    val runner = ProcessCallable.newBuilder()
      .addCommand("bash").addCommand("-c").addCommand(sigtermIgnoringScript).addCommand("--")
      .build()
    val shortGracefulProps = UDFWorkerProperties.newBuilder()
      .setConnection(WorkerConnectionSpec.newBuilder()
        .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance).build())
      .setGracefulTerminationTimeoutMs(500)
      .build()
    val spec = UDFWorkerSpecification.newBuilder()
      .setDirect(DirectWorker.newBuilder()
        .setRunner(runner).setProperties(shortGracefulProps).build())
      .build()
    dispatcher = new TestDirectWorkerDispatcher(spec)

    val session = createStubSession()
    val worker = session.workerProcess
    assert(worker.process.isAlive, "worker should be alive before close")

    val closeStart = System.nanoTime()
    session.close()
    val closeElapsedMs = (System.nanoTime() - closeStart) / 1000000L

    assert(!worker.process.isAlive,
      s"worker should have been SIGKILLed after ignoring SIGTERM (took ${closeElapsedMs}ms)")
    assert(closeElapsedMs >= 500L,
      s"close should have waited for gracefulTimeoutMs before escalating, " +
        s"took ${closeElapsedMs}ms")
  }

  test("closing a session terminates its worker") {
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(defaultRunner))

    val session = createStubSession()
    val worker = session.workerProcess
    val socketFile = new File(udsPath(worker))

    assert(worker.process.isAlive, "worker should be alive before session close")
    assert(socketFile.exists(), "socket file should exist before session close")

    session.close()

    // The session-close path is synchronous: SIGTERM is sent and the process
    // is reaped before `close` returns.
    assert(!worker.process.isAlive,
      "worker process should be terminated when the session closes")
    assert(!socketFile.exists(),
      "socket file should be cleaned up when the session closes")
  }

  test("concurrent session.close and dispatcher.close do not double-close the worker") {
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(defaultRunner))

    val sessions = (1 to 4).map(_ => createStubSession())
    val workers = sessions.map(_.workerProcess)

    val barrier = new java.util.concurrent.CyclicBarrier(sessions.size + 1)
    val errors = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()

    val sessionThreads = sessions.map { s =>
      val t = new Thread(() => {
        try {
          barrier.await()
          s.close()
        } catch {
          case t: Throwable => errors.add(t)
        }
      })
      t.start()
      t
    }

    val dispatcherThread = new Thread(() => {
      try {
        barrier.await()
        dispatcher.close()
      } catch {
        case t: Throwable => errors.add(t)
      }
    })
    dispatcherThread.start()

    sessionThreads.foreach(_.join(30000))
    dispatcherThread.join(30000)
    dispatcher = null

    assert(errors.isEmpty,
      s"unexpected errors during concurrent close: ${errors.toArray.mkString(", ")}")
    workers.foreach { w =>
      assert(!w.process.isAlive,
        s"worker at ${udsPath(w)} should be terminated after concurrent close")
    }
  }

  test("close racing with in-flight createSession does not leak the worker") {
    // The acquire-before-publish + post-publish closed re-check pattern in
    // createSession is designed for this race: thread A is mid-spawn when
    // thread B calls close(). Thread A must either throw IllegalStateException
    // (post-publish check caught the close) or receive a session whose worker
    // is reaped by close()'s iteration. No orphan process or socket file
    // should remain in either case.
    val readyLatch = new java.util.concurrent.CountDownLatch(1)
    val releaseLatch = new java.util.concurrent.CountDownLatch(1)
    val capturedWorkers =
      new java.util.concurrent.ConcurrentLinkedQueue[DirectWorkerProcess]()
    val racing = new DirectUnixSocketWorkerDispatcher(specWithRunner(defaultRunner)) {
      override protected def createConnection(
          socketPath: String): UnixSocketWorkerConnection =
        new SocketFileConnection(socketPath)
      override protected def createSessionForWorker(
          worker: DirectWorkerProcess): WorkerSession = {
        capturedWorkers.add(worker)
        readyLatch.countDown()
        // Block here so dispatcher.close() runs while createSession is in
        // flight. Use a generous wait so a slow CI doesn't time out.
        if (!releaseLatch.await(30, java.util.concurrent.TimeUnit.SECONDS)) {
          fail("releaseLatch never fired -- test orchestration broken")
        }
        new StubWorkerSession(worker)
      }
    }
    try {
      val outcome =
        new java.util.concurrent.atomic.AtomicReference[Either[Throwable, WorkerSession]]()
      val createThread = new Thread(() => {
        try {
          val s = racing.createSession(None)
          outcome.set(Right(s))
        } catch {
          case t: Throwable => outcome.set(Left(t))
        }
      }, "createSession-racer")
      createThread.start()

      // Wait for thread A to have published the worker and entered the
      // blocking override.
      assert(readyLatch.await(10, java.util.concurrent.TimeUnit.SECONDS),
        "createSession thread never reached createSessionForWorker")

      val closeThread = new Thread(() => racing.close(), "close-racer")
      closeThread.start()
      // Give close() time to flip `closed` and iterate workers.
      Thread.sleep(200)

      // Now release the in-flight createSession.
      releaseLatch.countDown()

      createThread.join(10000)
      closeThread.join(10000)
      assert(!createThread.isAlive, "createSession thread did not finish")
      assert(!closeThread.isAlive, "close thread did not finish")

      val captured = capturedWorkers.toArray(Array.empty[DirectWorkerProcess])
      assert(captured.length == 1,
        s"expected exactly one worker spawned, got ${captured.length}")
      val worker = captured(0)

      outcome.get() match {
        case Left(e: IllegalStateException) =>
          // Contractually allowed, but unreachable with this orchestration:
          // readyLatch only fires after createSession has cleared both
          // `closed` checks, so B's close cannot flip `closed` in time for
          // A to observe it. Kept defensive so a future internal change
          // that introduces a new window is still covered.
          assert(e.getMessage.contains("closed"),
            s"expected dispatcher-closed error, got: ${e.getMessage}")
        case Left(other) =>
          fail(s"unexpected exception from racing createSession: $other")
        case Right(_) =>
          // close() iterated the published worker and tore it down; the
          // returned session points at a worker that should now be dead.
      }

      // Whichever path won, the worker must not still be running and the
      // socket file must be gone.
      val deadline = System.currentTimeMillis() + 5000
      while (worker.process.isAlive && System.currentTimeMillis() < deadline) {
        Thread.sleep(50)
      }
      val sockPath = udsPath(worker)
      assert(!worker.process.isAlive,
        s"worker process should be terminated after close, still alive at $sockPath")
      assert(!new java.io.File(sockPath).exists(),
        s"socket file $sockPath should have been removed")
    } finally {
      releaseLatch.countDown()
      racing.close()
    }
  }

  test("worker-provided graceful timeout is capped at the engine-side maximum") {
    // The proto documents an engine-configurable maximum (fixed at 30s today).
    // A 60s spec value should be clamped down.
    val oversizedProps = UDFWorkerProperties.newBuilder()
      .setConnection(WorkerConnectionSpec.newBuilder()
        .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance).build())
      .setGracefulTerminationTimeoutMs(60000)
      .build()
    val spec = UDFWorkerSpecification.newBuilder()
      .setDirect(DirectWorker.newBuilder()
        .setRunner(defaultRunner).setProperties(oversizedProps).build())
      .build()
    dispatcher = new TestDirectWorkerDispatcher(spec)

    val session = createStubSession()
    assert(session.workerProcess.gracefulTimeoutMs == 30000L,
      s"graceful timeout should be capped at 30000ms, " +
        s"got ${session.workerProcess.gracefulTimeoutMs}")
    session.close()
  }

  test("worker-provided init timeout is capped at the engine-side maximum") {
    val oversizedProps = UDFWorkerProperties.newBuilder()
      .setConnection(WorkerConnectionSpec.newBuilder()
        .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance).build())
      .setInitializationTimeoutMs(60000)
      .build()
    val spec = UDFWorkerSpecification.newBuilder()
      .setDirect(DirectWorker.newBuilder()
        .setRunner(defaultRunner).setProperties(oversizedProps).build())
      .build()
    dispatcher = new TestDirectWorkerDispatcher(spec)

    assert(dispatcher.initTimeoutMs == 30000L,
      s"init timeout should be capped at 30000ms, got ${dispatcher.initTimeoutMs}")
  }

  test("createSession after close is rejected") {
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(defaultRunner))
    dispatcher.close()

    val ex = intercept[IllegalStateException] {
      dispatcher.createSession(None)
    }
    assert(ex.getMessage.contains("closed"),
      s"expected dispatcher-closed error, got: ${ex.getMessage}")
    dispatcher = null
  }

  test("socket directory is owner-only (0700) on POSIX") {
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(defaultRunner))
    // Drive one createSession so a worker (and therefore the socket dir) is
    // observable via the UDS connection's path.
    val session = createStubSession()
    val socketDir: Path = new File(udsPath(session.workerProcess)).toPath.getParent
    session.close()

    val view = Files.getFileAttributeView(socketDir, classOf[PosixFileAttributeView])
    // Skip explicitly on non-POSIX filesystems rather than silently pass,
    // so a CI environment without POSIX attributes is visible in the
    // test report instead of giving false confidence.
    assume(view != null, s"POSIX file attributes required to check $socketDir")
    val perms = view.readAttributes().permissions().asScala.toSet
    val expected = java.nio.file.attribute.PosixFilePermissions
      .fromString("rwx------").asScala.toSet
    assert(perms == expected,
      s"socket directory $socketDir should be 0700, got ${perms.mkString(",")}")
  }

  test("socket directory is removed after dispatcher.close") {
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(defaultRunner))
    val session = createStubSession()
    val socketDir = new File(udsPath(session.workerProcess)).toPath.getParent.toFile
    assert(socketDir.exists(),
      s"socket directory $socketDir should exist while a session is open")
    session.close()

    dispatcher.close()
    dispatcher = null

    assert(!socketDir.exists(),
      s"socket directory $socketDir should be removed after dispatcher.close")
  }

  // -- Error-path tests -------------------------------------------------------

  test("worker is cleaned up when createSessionForWorker throws") {
    // A dispatcher whose createSessionForWorker always throws. The spawned
    // worker must be terminated rather than leaked until dispatcher.close().
    var capturedWorker: DirectWorkerProcess = null
    val failingDispatcher =
      new DirectUnixSocketWorkerDispatcher(specWithRunner(defaultRunner)) {
        override protected def createConnection(
            socketPath: String): UnixSocketWorkerConnection =
          new SocketFileConnection(socketPath)
        override protected def createSessionForWorker(
            worker: DirectWorkerProcess): WorkerSession = {
          capturedWorker = worker
          throw new RuntimeException("session creation failed")
        }
      }

    try {
      val ex = intercept[RuntimeException] {
        failingDispatcher.createSession(None)
      }
      assert(ex.getMessage.contains("session creation failed"))
      assert(capturedWorker != null, "worker should have been spawned before the failure")
      assert(!capturedWorker.process.isAlive,
        "worker process should have been terminated after session creation failed")
      assert(capturedWorker.activeSessions == 0,
        "worker session count should be released after failure")
    } finally {
      failingDispatcher.close()
    }
  }

  test("DirectWorker without a connection is rejected") {
    val badSpec = UDFWorkerSpecification.newBuilder()
      .setDirect(DirectWorker.newBuilder().setRunner(defaultRunner).build())
      .build()
    val ex = intercept[IllegalArgumentException] {
      new TestDirectWorkerDispatcher(badSpec)
    }
    assert(ex.getMessage.contains("connection must be set"),
      s"expected missing-connection error, got: ${ex.getMessage}")
  }

  test("DirectWorker with non-UDS transport is rejected") {
    val tcpProperties = UDFWorkerProperties.newBuilder()
      .setConnection(WorkerConnectionSpec.newBuilder()
        .setTcp(LocalTcpConnection.getDefaultInstance).build())
      .build()
    val badSpec = UDFWorkerSpecification.newBuilder()
      .setDirect(DirectWorker.newBuilder()
        .setRunner(defaultRunner).setProperties(tcpProperties).build())
      .build()
    val ex = intercept[IllegalArgumentException] {
      new TestDirectWorkerDispatcher(badSpec)
    }
    assert(ex.getMessage.contains("UNIX domain socket"),
      s"expected UDS-only error, got: ${ex.getMessage}")
  }

  test("socket file is cleaned up when createConnection throws") {
    val capturedSocketPaths = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    val failingDispatcher =
      new DirectUnixSocketWorkerDispatcher(specWithRunner(defaultRunner)) {
        override protected def createConnection(
            socketPath: String): UnixSocketWorkerConnection = {
          capturedSocketPaths.add(socketPath)
          throw new RuntimeException("connection creation failed")
        }
        override protected def createSessionForWorker(
            worker: DirectWorkerProcess): WorkerSession =
          new StubWorkerSession(worker)
      }
    try {
      val ex = intercept[RuntimeException] {
        failingDispatcher.createSession(None)
      }
      assert(ex.getMessage.contains("connection creation failed"))
      assert(capturedSocketPaths.size == 1, "createConnection should have been called once")
      val socketPath = capturedSocketPaths.peek()
      assert(!new File(socketPath).exists(),
        s"socket file $socketPath should have been cleaned up")
    } finally {
      failingDispatcher.close()
    }
  }

  test("empty ProcessCallable command is rejected with a clear error") {
    val emptyRunner = ProcessCallable.newBuilder().build()
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(emptyRunner))
    val ex = intercept[IllegalArgumentException] {
      dispatcher.createSession(None)
    }
    assert(ex.getMessage.contains("at least one entry"),
      s"expected explicit empty-command error, got: ${ex.getMessage}")
  }

  test("spawnWorker fails when worker process exits immediately") {
    val runner = ProcessCallable.newBuilder()
      .addCommand("bash").addCommand("-c")
      .addCommand("echo 'fatal: bad config' >&2; exit 42").addCommand("--")
      .build()
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(runner))

    val ex = intercept[RuntimeException] {
      dispatcher.createSession(None)
    }
    assert(ex.getMessage.contains("exited with code 42"),
      s"expected early-exit error, got: ${ex.getMessage}")
    assert(ex.getMessage.contains("fatal: bad config"),
      s"expected process output in error, got: ${ex.getMessage}")
  }

  test("spawnWorker times out when worker stays alive but never creates socket") {
    // Distinct from the "process exits immediately" case: here the worker
    // process is healthy but simply doesn't bind the socket, so the
    // dispatcher must time out and SIGKILL-reap it rather than wait forever.
    val hangingRunner = ProcessCallable.newBuilder()
      .addCommand("bash").addCommand("-c")
      .addCommand("while true; do sleep 1; done").addCommand("--")
      .build()
    val shortInitProps = UDFWorkerProperties.newBuilder()
      .setConnection(WorkerConnectionSpec.newBuilder()
        .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance).build())
      .setInitializationTimeoutMs(500)
      .build()
    val spec = UDFWorkerSpecification.newBuilder()
      .setDirect(DirectWorker.newBuilder()
        .setRunner(hangingRunner).setProperties(shortInitProps).build())
      .build()
    dispatcher = new TestDirectWorkerDispatcher(spec)

    val ex = intercept[DirectWorkerTimeoutException] {
      dispatcher.createSession(None)
    }
    assert(ex.getMessage.contains("did not create socket"),
      s"expected init-timeout error, got: ${ex.getMessage}")
    assert(ex.getMessage.contains("500ms"),
      s"expected timeout value in error, got: ${ex.getMessage}")
  }

  // -- Environment lifecycle tests -------------------------------------------

  test("skips installation when verification succeeds") {
    val markerFile = Files.createTempFile("env-install-marker", ".txt").toFile
    markerFile.delete()

    val env = WorkerEnvironment.newBuilder()
      .setEnvironmentVerification(ProcessCallable.newBuilder()
        .addCommand("bash").addCommand("-c").addCommand("exit 0").build())
      .setInstallation(ProcessCallable.newBuilder()
        .addCommand("bash").addCommand("-c")
        .addCommand(s"touch ${markerFile.getAbsolutePath}").build())
      .build()
    dispatcher = new TestDirectWorkerDispatcher(specWithEnv(env = env))

    val session = dispatcher.createSession(None)
    session.close()

    assert(!markerFile.exists(),
      "installation should not run when verification succeeds")
  }

  test("runs installation when verification fails") {
    val markerFile = Files.createTempFile("env-install-marker", ".txt").toFile
    markerFile.delete()

    val env = WorkerEnvironment.newBuilder()
      .setEnvironmentVerification(ProcessCallable.newBuilder()
        .addCommand("bash").addCommand("-c").addCommand("exit 1").build())
      .setInstallation(ProcessCallable.newBuilder()
        .addCommand("bash").addCommand("-c")
        .addCommand(s"touch ${markerFile.getAbsolutePath}").build())
      .build()
    dispatcher = new TestDirectWorkerDispatcher(specWithEnv(env = env))

    val session = dispatcher.createSession(None)
    session.close()

    assert(markerFile.exists(),
      "installation should run when verification fails")
    markerFile.delete()
  }

  test("runs installation when no verification callable is provided") {
    val markerFile = Files.createTempFile("env-install-marker", ".txt").toFile
    markerFile.delete()

    val env = WorkerEnvironment.newBuilder()
      .setInstallation(ProcessCallable.newBuilder()
        .addCommand("bash").addCommand("-c")
        .addCommand(s"touch ${markerFile.getAbsolutePath}").build())
      .build()
    dispatcher = new TestDirectWorkerDispatcher(specWithEnv(env = env))

    val session = dispatcher.createSession(None)
    session.close()

    assert(markerFile.exists(),
      "installation should run when no verification is defined")
    markerFile.delete()
  }

  test("installation failure throws with process output and prevents worker creation") {
    val env = WorkerEnvironment.newBuilder()
      .setInstallation(ProcessCallable.newBuilder()
        .addCommand("bash").addCommand("-c")
        .addCommand("echo 'missing dependency: libfoo' >&2; exit 7").build())
      .build()
    dispatcher = new TestDirectWorkerDispatcher(specWithEnv(env = env))

    val ex = intercept[RuntimeException] {
      dispatcher.createSession(None)
    }
    assert(ex.getMessage.contains("exit code 7"),
      s"expected installation failure, got: ${ex.getMessage}")
    assert(ex.getMessage.contains("missing dependency: libfoo"),
      s"expected process output in error, got: ${ex.getMessage}")
  }

  test("installation that exceeds callableTimeoutMs is killed and reported") {
    // Installation sleeps longer than callableTimeoutMs; the dispatcher
    // must SIGKILL-reap it and surface a "Callable timed out" error
    // rather than hang the caller.
    val slowInstall = ProcessCallable.newBuilder()
      .addCommand("bash").addCommand("-c")
      .addCommand("sleep 30").build()
    val env = WorkerEnvironment.newBuilder().setInstallation(slowInstall).build()
    val shortTimeoutDispatcher =
      new DirectUnixSocketWorkerDispatcher(specWithEnv(env = env)) {
        override protected def callableTimeoutMs: Long = 500L
        override protected def createConnection(
            socketPath: String): UnixSocketWorkerConnection =
          new SocketFileConnection(socketPath)
        override protected def createSessionForWorker(
            worker: DirectWorkerProcess): WorkerSession =
          new StubWorkerSession(worker)
      }
    try {
      val ex = intercept[DirectWorkerTimeoutException] {
        shortTimeoutDispatcher.createSession(None)
      }
      assert(ex.getMessage.contains("Callable timed out"),
        s"expected callable-timeout error, got: ${ex.getMessage}")
      assert(ex.getMessage.contains("500ms"),
        s"expected timeout value in error, got: ${ex.getMessage}")
    } finally {
      shortTimeoutDispatcher.close()
    }
  }

  test("environment setup runs only once across multiple sessions") {
    val counterFile = Files.createTempFile("env-counter", ".txt").toFile
    counterFile.delete()

    val env = WorkerEnvironment.newBuilder()
      .setInstallation(ProcessCallable.newBuilder()
        .addCommand("bash").addCommand("-c")
        .addCommand(s"echo invoked >> ${counterFile.getAbsolutePath}").build())
      .build()
    dispatcher = new TestDirectWorkerDispatcher(specWithEnv(env = env))

    val s1 = dispatcher.createSession(None); s1.close()
    val s2 = dispatcher.createSession(None); s2.close()

    val src = scala.io.Source.fromFile(counterFile)
    val lines = try src.getLines().toList finally src.close()
    assert(lines.size == 1,
      s"installation should run exactly once, but ran ${lines.size} time(s)")
    counterFile.delete()
  }

  test("concurrent createSession still installs exactly once") {
    // The sequential single-install test above cannot catch a missing
    // lock around ensureEnvironmentReady. Race many createSession calls
    // with an install script that takes long enough for the threads to
    // queue on environmentLock, then verify it still ran exactly once.
    val counterFile = Files.createTempFile("env-concurrent-install", ".txt").toFile
    counterFile.delete()

    val env = WorkerEnvironment.newBuilder()
      .setInstallation(ProcessCallable.newBuilder()
        .addCommand("bash").addCommand("-c")
        .addCommand(
          s"sleep 0.2; echo invoked >> ${counterFile.getAbsolutePath}").build())
      .build()
    dispatcher = new TestDirectWorkerDispatcher(specWithEnv(env = env))

    val threads = 4
    val startGate = new java.util.concurrent.CountDownLatch(1)
    val doneGate = new java.util.concurrent.CountDownLatch(threads)
    val sessions = new java.util.concurrent.ConcurrentLinkedQueue[WorkerSession]()
    val errors = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()

    (1 to threads).foreach { _ =>
      new Thread(() => {
        try {
          startGate.await()
          sessions.add(dispatcher.createSession(None))
        } catch {
          case t: Throwable => errors.add(t)
        } finally {
          doneGate.countDown()
        }
      }).start()
    }
    startGate.countDown()
    assert(doneGate.await(30, java.util.concurrent.TimeUnit.SECONDS),
      "createSession threads did not finish in time")
    assert(errors.isEmpty,
      s"unexpected errors during concurrent createSession: ${errors.toArray.mkString(", ")}")

    val src = scala.io.Source.fromFile(counterFile)
    val lines = try src.getLines().toList finally src.close()
    assert(lines.size == 1,
      s"installation should run exactly once under concurrent createSession, " +
        s"but ran ${lines.size} time(s)")

    sessions.asScala.foreach(_.close())
    counterFile.delete()
  }

  test("failed environment setup is not retried on subsequent createSession") {
    val counterFile = Files.createTempFile("env-failed-counter", ".txt").toFile
    counterFile.delete()

    // Installation script appends a line every time it runs, then always
    // fails. The first createSession should run it; the second should be
    // rejected immediately without re-running.
    val env = WorkerEnvironment.newBuilder()
      .setInstallation(ProcessCallable.newBuilder()
        .addCommand("bash").addCommand("-c")
        .addCommand(
          s"echo invoked >> ${counterFile.getAbsolutePath}; exit 1").build())
      .build()
    dispatcher = new TestDirectWorkerDispatcher(specWithEnv(env = env))

    val first = intercept[RuntimeException] { dispatcher.createSession(None) }
    assert(first.getMessage.contains("installation failed"),
      s"expected first-attempt installation failure, got: ${first.getMessage}")

    val second = intercept[RuntimeException] { dispatcher.createSession(None) }
    assert(second.getMessage.contains("previously failed"),
      s"expected cached failure on retry, got: ${second.getMessage}")

    val src = scala.io.Source.fromFile(counterFile)
    val lines = try src.getLines().toList finally src.close()
    assert(lines.size == 1,
      s"installation should run only once across failed retries, got ${lines.size}")
    counterFile.delete()
  }

  test("installation timeout transitions to Failed and is not retried") {
    val counterFile = Files.createTempFile("env-timeout-counter", ".txt").toFile
    counterFile.delete()

    // Install appends to a counter file, then sleeps past callableTimeoutMs
    // so runCallable times out. The dispatcher must mark the env Failed
    // and reject the next createSession without re-running install.
    val env = WorkerEnvironment.newBuilder()
      .setInstallation(ProcessCallable.newBuilder()
        .addCommand("bash").addCommand("-c")
        .addCommand(
          s"echo invoked >> ${counterFile.getAbsolutePath}; sleep 30").build())
      .build()
    val timeoutDispatcher =
      new DirectUnixSocketWorkerDispatcher(specWithEnv(env = env)) {
        override protected def callableTimeoutMs: Long = 500L
        override protected def createConnection(
            socketPath: String): UnixSocketWorkerConnection =
          new SocketFileConnection(socketPath)
        override protected def createSessionForWorker(
            worker: DirectWorkerProcess): WorkerSession =
          new StubWorkerSession(worker)
      }
    try {
      val first = intercept[DirectWorkerTimeoutException] {
        timeoutDispatcher.createSession(None)
      }
      assert(first.getMessage.contains("Callable timed out"),
        s"expected callable-timeout error, got: ${first.getMessage}")

      val second = intercept[DirectWorkerException] {
        timeoutDispatcher.createSession(None)
      }
      assert(second.getMessage.contains("previously failed"),
        s"expected cached failure on retry, got: ${second.getMessage}")

      val src = scala.io.Source.fromFile(counterFile)
      val lines = try src.getLines().toList finally src.close()
      assert(lines.size == 1,
        s"installation should run only once across timed-out retries, got ${lines.size}")
    } finally {
      timeoutDispatcher.close()
      counterFile.delete()
    }
  }

  test("non-None securityScope is rejected until pooling lands") {
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(defaultRunner))
    val scope = new WorkerSecurityScope {
      override def equals(obj: Any): Boolean = obj.isInstanceOf[this.type]
      override def hashCode(): Int = 0
    }
    val ex = intercept[IllegalArgumentException] {
      dispatcher.createSession(Some(scope))
    }
    assert(ex.getMessage.contains("not supported yet"),
      s"expected unsupported-scope error, got: ${ex.getMessage}")
  }

  test("verification without installation is rejected") {
    val env = WorkerEnvironment.newBuilder()
      .setEnvironmentVerification(ProcessCallable.newBuilder()
        .addCommand("bash").addCommand("-c").addCommand("exit 0").build())
      .build()
    val ex = intercept[IllegalArgumentException] {
      new TestDirectWorkerDispatcher(specWithEnv(env = env))
    }
    assert(ex.getMessage.contains("installation"),
      s"expected installation-required error, got: ${ex.getMessage}")
  }

  test("cleanup runs on dispatcher close") {
    val cleanupMarker = Files.createTempFile("env-cleanup-marker", ".txt").toFile
    cleanupMarker.delete()

    val env = WorkerEnvironment.newBuilder()
      .setEnvironmentCleanup(ProcessCallable.newBuilder()
        .addCommand("bash").addCommand("-c")
        .addCommand(s"touch ${cleanupMarker.getAbsolutePath}").build())
      .build()
    dispatcher = new TestDirectWorkerDispatcher(specWithEnv(env = env))

    val session = dispatcher.createSession(None)
    session.close()

    assert(!cleanupMarker.exists(),
      "cleanup should not run until dispatcher is closed")

    dispatcher.close()
    dispatcher = null

    assert(cleanupMarker.exists(),
      "cleanup should run when dispatcher is closed")
    cleanupMarker.delete()
  }
}
