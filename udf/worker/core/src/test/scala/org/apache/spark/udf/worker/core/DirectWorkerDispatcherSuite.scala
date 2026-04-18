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
import java.nio.file.Files

// scalastyle:off funsuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker.{
  DirectWorker, LocalTcpConnection, ProcessCallable, UDFWorkerProperties,
  UDFWorkerSpecification, UnixDomainSocket,
  WorkerConnection => WorkerConnectionProto, WorkerEnvironment}
import org.apache.spark.udf.worker.core.direct.{DirectWorkerDispatcher,
  DirectWorkerProcess, DirectWorkerSession}

/**
 * A [[WorkerConnection]] test implementation that considers the connection
 * active as long as the socket file exists on disk.
 */
class SocketFileConnection(socketPath: String) extends WorkerConnection {
  override def isActive: Boolean = new File(socketPath).exists()
  override def close(): Unit = {
    val f = new File(socketPath)
    if (f.exists()) f.delete()
  }
}

/**
 * A stub [[DirectWorkerSession]] for process-lifecycle tests that don't
 * need actual data transmission.
 */
class StubWorkerSession(
    workerProcess: DirectWorkerProcess) extends DirectWorkerSession(workerProcess) {

  override def init(message: InitMessage): Unit = {}

  override def process(input: Iterator[Array[Byte]]): Iterator[Array[Byte]] =
    Iterator.empty

  override def cancel(): Unit = {}
}

/**
 * A [[DirectWorkerDispatcher]] subclass for testing that uses a socket-file
 * connection and stub sessions instead of a real protocol implementation.
 */
class TestDirectWorkerDispatcher(spec: UDFWorkerSpecification)
    extends DirectWorkerDispatcher(spec) {

  override protected def createConnection(socketPath: String): WorkerConnection = {
    new SocketFileConnection(socketPath)
  }

  override protected def createSessionForWorker(
      worker: DirectWorkerProcess): WorkerSession = {
    new StubWorkerSession(worker)
  }
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
    .addConnections(WorkerConnectionProto.newBuilder()
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

  test("creates a worker and session") {
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(defaultRunner))

    val session = dispatcher.createSession(None).asInstanceOf[StubWorkerSession]
    val worker = session.workerProcess

    assert(worker.isAlive, "worker should be alive after creation")
    assert(worker.activeSessions == 1, "should have 1 active session")
    assert(new File(worker.socketPath).exists(), "socket file should exist")

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
          sessions.add(
            dispatcher.createSession(None).asInstanceOf[StubWorkerSession])
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

    val workerObjects = sessions.toArray.map(_.asInstanceOf[StubWorkerSession].workerProcess)
    assert(workerObjects.distinct.length == threads,
      "each session should have its own DirectWorkerProcess")

    sessions.toArray.foreach(_.asInstanceOf[StubWorkerSession].close())
  }

  test("close shuts down all workers via SIGTERM") {
    dispatcher = new TestDirectWorkerDispatcher(specWithRunner(defaultRunner))

    val session1 = dispatcher.createSession(None).asInstanceOf[StubWorkerSession]
    val session2 = dispatcher.createSession(None).asInstanceOf[StubWorkerSession]

    val worker1 = session1.workerProcess
    val worker2 = session2.workerProcess

    session1.close()
    session2.close()
    dispatcher.close()
    dispatcher = null

    assert(!worker1.process.isAlive, "worker1 should be terminated")
    assert(!worker2.process.isAlive, "worker2 should be terminated")
  }

  // -- Error-path tests -------------------------------------------------------

  test("worker is cleaned up when createSessionForWorker throws") {
    // A dispatcher whose createSessionForWorker always throws. The spawned
    // worker must be terminated rather than leaked until dispatcher.close().
    var capturedWorker: DirectWorkerProcess = null
    val failingDispatcher = new DirectWorkerDispatcher(specWithRunner(defaultRunner)) {
      override protected def createConnection(socketPath: String): WorkerConnection =
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

  test("DirectWorker without connections is rejected") {
    val badSpec = UDFWorkerSpecification.newBuilder()
      .setDirect(DirectWorker.newBuilder().setRunner(defaultRunner).build())
      .build()
    val ex = intercept[IllegalArgumentException] {
      new TestDirectWorkerDispatcher(badSpec)
    }
    assert(ex.getMessage.contains("exactly one entry"),
      s"expected connections-count error, got: ${ex.getMessage}")
  }

  test("DirectWorker with non-UDS transport is rejected") {
    val tcpProperties = UDFWorkerProperties.newBuilder()
      .addConnections(WorkerConnectionProto.newBuilder()
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

  test("DirectWorker with multiple connections is rejected") {
    val twoConnections = UDFWorkerProperties.newBuilder()
      .addConnections(WorkerConnectionProto.newBuilder()
        .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance).build())
      .addConnections(WorkerConnectionProto.newBuilder()
        .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance).build())
      .build()
    val badSpec = UDFWorkerSpecification.newBuilder()
      .setDirect(DirectWorker.newBuilder()
        .setRunner(defaultRunner).setProperties(twoConnections).build())
      .build()
    val ex = intercept[IllegalArgumentException] {
      new TestDirectWorkerDispatcher(badSpec)
    }
    assert(ex.getMessage.contains("exactly one entry"),
      s"expected connections-count error, got: ${ex.getMessage}")
  }

  test("socket file is cleaned up when createConnection throws") {
    val capturedSocketPaths = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    val failingDispatcher = new DirectWorkerDispatcher(specWithRunner(defaultRunner)) {
      override protected def createConnection(socketPath: String): WorkerConnection = {
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
