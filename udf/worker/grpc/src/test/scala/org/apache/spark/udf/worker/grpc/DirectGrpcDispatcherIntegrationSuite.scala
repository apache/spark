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

import java.io.File
import java.nio.file.Paths
import java.util.Locale

import com.google.protobuf.ByteString
import org.scalatest.BeforeAndAfterEach
// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker.{Cancel, DataRequest, DataResponse, DirectWorker, Finish, Init,
  ProcessCallable, UdfPayload, UDFProtoCommunicationPattern, UDFWorkerDataFormat,
  UDFWorkerProperties, UDFWorkerSpecification, UnixDomainSocket, WorkerCapabilities,
  WorkerConnectionSpec}
import org.apache.spark.udf.worker.core.{Termination, WorkerSession}
import org.apache.spark.udf.worker.core.direct.DirectWorkerProcess
import org.apache.spark.udf.worker.grpc.testing.EchoGrpcWorkerMain

/**
 * Integration tests that spawn the [[EchoGrpcWorkerMain]] subprocess via a
 * [[DirectGrpcDispatcher]] and exercise the engine-side
 * dispatcher / session lifecycle end to end (real gRPC, real OS process).
 *
 * These tests run only on platforms where a Netty UDS transport is
 * available (Linux epoll or macOS kqueue); other platforms `assume`-skip.
 */
class DirectGrpcDispatcherIntegrationSuite
    extends AnyFunSuite with BeforeAndAfterEach {
// scalastyle:on funsuite

  /**
   * Full Java classpath inherited from the SBT/Maven test runner. The
   * dispatcher spawns `java -cp <cp> EchoGrpcWorkerMain` with this so the
   * child JVM can resolve the test sources and all transitive deps.
   */
  private val javaClasspath: String = System.getProperty("java.class.path")

  private val javaExecutable: String =
    Paths.get(System.getProperty("java.home"), "bin", "java").toString

  private val workerMainClass: String =
    classOf[EchoGrpcWorkerMain.type].getName.stripSuffix("$")

  private var dispatcher: DirectGrpcDispatcher = _

  override def beforeEach(): Unit = {
    val supported = try {
      UnixDomainSocketTransport.detect(); true
    } catch {
      case _: UnsupportedOperationException => false
    }
    assume(supported,
      "Netty UDS native transport (epoll on Linux or kqueue on macOS) is " +
        "required for the gRPC direct dispatcher")
  }

  override def afterEach(): Unit = {
    if (dispatcher != null) {
      try dispatcher.close() finally dispatcher = null
    }
    super.afterEach()
  }

  // --------------------------------------------------------------------------
  // Spec builders
  // --------------------------------------------------------------------------

  private def echoRunner: ProcessCallable = ProcessCallable.newBuilder()
    .addCommand(javaExecutable)
    .addCommand("-cp").addCommand(javaClasspath)
    .addCommand(workerMainClass)
    .build()

  private def udsProperties: UDFWorkerProperties = UDFWorkerProperties.newBuilder()
    .setConnection(WorkerConnectionSpec.newBuilder()
      .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance)
      .build())
    // Generous init timeout: child JVM startup includes Netty native lib
    // loading, several seconds on first launch.
    .setInitializationTimeoutMs(30000)
    .setGracefulTerminationTimeoutMs(10000)
    .build()

  private def capabilities: WorkerCapabilities = WorkerCapabilities.newBuilder()
    .addSupportedDataFormats(UDFWorkerDataFormat.ARROW)
    .addSupportedCommunicationPatterns(UDFProtoCommunicationPattern.BIDIRECTIONAL_STREAMING)
    .build()

  private def echoSpec: UDFWorkerSpecification = UDFWorkerSpecification.newBuilder()
    .setCapabilities(capabilities)
    .setDirect(DirectWorker.newBuilder()
      .setRunner(echoRunner)
      .setProperties(udsProperties)
      .build())
    .build()

  private def newDispatcher(): DirectGrpcDispatcher = {
    dispatcher = new DirectGrpcDispatcher(echoSpec)
    dispatcher
  }

  private def basicInit(payload: Array[Byte] = "echo".getBytes): Init =
    Init.newBuilder()
      .setProtocolVersion(1)
      .setDataFormat(UDFWorkerDataFormat.ARROW)
      .setUdf(UdfPayload.newBuilder()
        .setPayload(ByteString.copyFrom(payload))
        .setFormat("echo")
        .build())
      .build()

  // Default lifecycle messages for the data phase and finalization.
  private val emptyFinish: () => Finish = () => Finish.getDefaultInstance
  private val emptyCancel: () => Cancel = () => Cancel.getDefaultInstance

  /** Wraps strings as input [[DataRequest]] batches. */
  private def echoIn(batches: String*): Iterator[DataRequest] =
    batches.iterator.map(s => DataRequest.newBuilder()
      .setData(ByteString.copyFrom(s.getBytes)).build())

  /** Decodes a result [[DataResponse]] batch to a string. */
  private def outStr(d: DataResponse): String = new String(d.getData.toByteArray)

  /** Runs `f` with a fresh session, ensuring it is closed afterwards. */
  private def withSession[T](f: WorkerSession => T): T = {
    val session = dispatcher.createSession(None)
    try f(session) finally session.close(emptyCancel)
  }

  /**
   * Centralised cast for tests that need to reach past the
   * [[org.apache.spark.udf.worker.core.WorkerHandle]] interface to the
   * concrete [[DirectWorkerProcess]] (e.g. to assert process liveness
   * or read the underlying [[GrpcWorkerChannel]]). Fails with a clear
   * test failure if the type is unexpected, rather than the opaque
   * `ClassCastException` we'd get from a bare `asInstanceOf`.
   */
  private def workerProcessOf(s: WorkerSession): DirectWorkerProcess =
    s.workerHandle match {
      case dp: DirectWorkerProcess => dp
      case other => fail(
        s"Expected DirectWorkerProcess, got ${other.getClass.getSimpleName}")
    }

  // --------------------------------------------------------------------------
  // Tests
  // --------------------------------------------------------------------------

  test("dispatcher construction: smoke test") {
    newDispatcher()
    assert(dispatcher.getClass == classOf[DirectGrpcDispatcher],
      s"expected a DirectGrpcDispatcher, got ${dispatcher.getClass}")
  }

  test("dispatcher spawns a worker process and a session round-trips an echo batch") {
    newDispatcher()

    withSession { session =>
      session.init(basicInit())
      val results = session.process(echoIn("hello"), emptyFinish).toList
      assert(results.map(outStr) == List("hello"))
    }
  }

  test("multiple batches are echoed in order on a single session") {
    newDispatcher()

    withSession { session =>
      session.init(basicInit())
      val inputs = Seq("alpha", "beta", "gamma", "delta")
      val results = session.process(echoIn(inputs: _*), emptyFinish).map(outStr).toList
      assert(results == inputs)
    }
  }

  test("two sequential sessions each spawn and clean up their own worker") {
    newDispatcher()

    val s1 = dispatcher.createSession(None)
    try {
      s1.init(basicInit())
      val r1 = s1.process(echoIn("one"), emptyFinish).map(outStr).toList
      assert(r1 == List("one"))
    } finally s1.close(emptyCancel)

    val s2 = dispatcher.createSession(None)
    try {
      s2.init(basicInit())
      val r2 = s2.process(echoIn("two"), emptyFinish).map(outStr).toList
      assert(r2 == List("two"))
    } finally s2.close(emptyCancel)
  }

  test("worker process is terminated when session closes") {
    newDispatcher()

    val session = dispatcher.createSession(None)
    val workerProcess = workerProcessOf(session)
    try {
      session.init(basicInit())
      session.process(echoIn("ping"), emptyFinish).toList
      assert(workerProcess.process.isAlive, "worker should be alive while session is open")
    } finally session.close(emptyCancel)

    assert(!workerProcess.process.isAlive,
      "worker process should be terminated after session.close()")
    val socketFile = workerProcess.connection match {
      case g: GrpcWorkerChannel => new File(g.socketPath)
      case _ => null
    }
    if (socketFile != null) {
      assert(!socketFile.exists(),
        s"socket file ${socketFile.getAbsolutePath} should be removed after close")
    }
  }

  test("dispatcher.close terminates any remaining worker processes") {
    newDispatcher()

    val session = dispatcher.createSession(None)
    session.init(basicInit())
    val workerProcess = workerProcessOf(session)
    // Drive one batch through so we are sure the worker is fully up.
    val it = session.process(echoIn("once"), emptyFinish)
    assert(outStr(it.next()) == "once")

    // Deliberately do NOT close the session before closing the dispatcher.
    dispatcher.close()
    dispatcher = null

    val deadline = System.currentTimeMillis() + 5000
    while (workerProcess.process.isAlive && System.currentTimeMillis() < deadline) {
      Thread.sleep(50)
    }
    assert(!workerProcess.process.isAlive,
      "worker process should be terminated by dispatcher.close")
  }

  test("worker user-error is surfaced as GrpcWorkerSessionException") {
    newDispatcher()

    withSession { session =>
      session.init(basicInit())
      // The echo worker emits ErrorResponse(UserError) when it sees this batch.
      val it = session.process(echoIn("ERROR"), emptyFinish)
      val ex = intercept[GrpcWorkerSessionException] { it.toList }
      assert(ex.executionError != null, "execution error must be attached")
      assert(ex.executionError.hasUser, s"expected UserError, got $ex")
      assert(ex.executionError.getUser.getErrorClass == "SimulatedError")
    }
  }

  test("init failure surfaces from session.init()") {
    newDispatcher()

    val session = dispatcher.createSession(None)
    try {
      val init = Init.newBuilder(basicInit())
        // The echo worker treats an init payload of "INIT_ERROR" as a
        // simulated init failure (InitResponse with WorkerError set).
        .setUdf(UdfPayload.newBuilder()
          .setPayload(ByteString.copyFromUtf8("INIT_ERROR"))
          .setFormat("echo")
          .build())
        .build()
      val ex = intercept[GrpcWorkerSessionException] { session.init(init) }
      assert(ex.executionError != null && ex.executionError.hasWorker)
      assert(ex.executionError.getWorker.getMessage == "simulated init failure")
    } finally session.close(emptyCancel)
  }

  test("close after init but before process cancels the worker cleanly") {
    newDispatcher()

    val session = dispatcher.createSession(None)
    session.init(basicInit())
    // No process(): finalizing now must cancel the in-flight session and
    // return the CancelResponse terminator.
    val start = System.currentTimeMillis()
    val termination = session.close(emptyCancel)
    val elapsed = System.currentTimeMillis() - start
    assert(termination.isInstanceOf[Termination.Cancelled],
      s"expected a Cancelled termination, got $termination")
    // A genuine cooperative cancel returns as soon as the worker replies with
    // CancelResponse. A broken handshake would instead block until the
    // terminalTimeoutMs (default 30s) and then settle a Termination.TransportFailed,
    // so the Cancelled assertion above plus a prompt return distinguish the clean
    // case from a timeout.
    assert(elapsed < 10000,
      s"close() should return promptly on a clean cancel, took ${elapsed}ms")
  }

  test("close before init does not leak the worker process") {
    newDispatcher()

    val session = dispatcher.createSession(None)
    val workerProcess = workerProcessOf(session)
    session.close(emptyCancel)
    val deadline = System.currentTimeMillis() + 5000
    while (workerProcess.process.isAlive && System.currentTimeMillis() < deadline) {
      Thread.sleep(50)
    }
    assert(!workerProcess.process.isAlive,
      "worker should be terminated by close() even when init was never called")
  }

  test("backend.validateSpec rejects spec without BIDIRECTIONAL_STREAMING when " +
      "capabilities are explicitly declared") {
    val incompatibleCaps = WorkerCapabilities.newBuilder()
      .addSupportedDataFormats(UDFWorkerDataFormat.ARROW)
      .addSupportedCommunicationPatterns(
        UDFProtoCommunicationPattern.UDF_PROTO_COMMUNICATION_PATTERN_UNSPECIFIED)
      .build()
    val spec = UDFWorkerSpecification.newBuilder()
      .setCapabilities(incompatibleCaps)
      .setDirect(DirectWorker.newBuilder()
        .setRunner(echoRunner).setProperties(udsProperties).build())
      .build()
    val ex = intercept[IllegalArgumentException] {
      new DirectGrpcDispatcher(spec)
    }
    assert(ex.getMessage.contains("BIDIRECTIONAL_STREAMING"),
      s"expected pattern-required error, got: ${ex.getMessage}")
  }

  test("backend.validateSpec rejects a spec that declares no capabilities at all") {
    val spec = UDFWorkerSpecification.newBuilder()
      .setDirect(DirectWorker.newBuilder()
        .setRunner(echoRunner).setProperties(udsProperties).build())
      .build()
    val ex = intercept[IllegalArgumentException] {
      new DirectGrpcDispatcher(spec)
    }
    assert(ex.getMessage.contains("BIDIRECTIONAL_STREAMING"),
      s"expected capabilities-required error, got: ${ex.getMessage}")
  }

  test("dispatcher cleans up socket directory on close") {
    newDispatcher()
    val session = dispatcher.createSession(None)
    val workerProcess = workerProcessOf(session)
    val socketPath = workerProcess.connection match {
      case g: GrpcWorkerChannel => g.socketPath
      case _ => fail("expected GrpcWorkerChannel")
    }
    val socketDir = new File(socketPath).getParentFile
    assert(socketDir.exists(), s"socket dir $socketDir should exist while session is open")
    session.close(emptyCancel)
    dispatcher.close()
    dispatcher = null
    assert(!socketDir.exists(),
      s"socket dir $socketDir should be removed when dispatcher closes")
  }

  test("concurrent sessions on a single dispatcher each get their own worker " +
      "and round-trip an echo batch in parallel") {
    newDispatcher()
    val threads = 4
    val errors = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()
    val results = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    val barrier = new java.util.concurrent.CyclicBarrier(threads)
    val handles = (1 to threads).map { i =>
      val t = new Thread(() => {
        try {
          val session = dispatcher.createSession(None)
          try {
            session.init(basicInit())
            barrier.await(30, java.util.concurrent.TimeUnit.SECONDS)
            val out = session.process(
              echoIn(s"thread-$i"), emptyFinish).toList
            require(out.size == 1)
            results.add(outStr(out.head))
          } finally session.close(emptyCancel)
        } catch {
          case t: Throwable => errors.add(t)
        }
      }, s"concurrent-session-$i")
      t.start()
      t
    }
    handles.foreach(_.join(30000))
    assert(errors.isEmpty,
      s"unexpected errors: ${errors.toArray.mkString(", ")}")
    assert(results.size == threads, s"expected $threads results, got ${results.size}")
    import scala.jdk.CollectionConverters._
    val echoed = results.asScala.toSet
    val expected = (1 to threads).map(i => s"thread-$i").toSet
    assert(echoed == expected, s"echoed=$echoed expected=$expected")
  }

  test("worker crash mid-process surfaces a transport error and marks worker invalid") {
    newDispatcher()
    val session = dispatcher.createSession(None)
    val workerProc = workerProcessOf(session)
    try {
      session.init(basicInit())
      val it = session.process(echoIn("hello"), emptyFinish)
      assert(outStr(it.next()) == "hello", "first batch must echo cleanly")

      // SIGKILL the worker subprocess to simulate an unexpected crash.
      workerProc.process.destroyForcibly()
      // Wait for the OS to reap it so the gRPC channel actually notices.
      val killed = workerProc.process.waitFor(
        5, java.util.concurrent.TimeUnit.SECONDS)
      assert(killed, "worker process should have died after destroyForcibly")

      // The Java iterator must surface the transport failure.
      val ex = intercept[GrpcWorkerSessionException] {
        while (it.hasNext) it.next()
      }
      assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("worker") ||
        ex.getMessage.toLowerCase(Locale.ROOT).contains("stream"),
        s"expected transport-failure message, got: ${ex.getMessage}")
    } finally session.close(emptyCancel)
    // Reuse-readiness: the session should have flagged the worker invalid so
    // a future pool will not recycle it.
    assert(workerProc.isInvalid,
      "worker should be flagged invalid after a transport failure")
  }

  test("spec-driven smoke: start from worker spec, get a working session in one factory call") {
    // The canonical "from a spec, get a dispatcher, get a session, run a UDF"
    // path that documentation can point to.
    val spec = UDFWorkerSpecification.newBuilder()
      .setCapabilities(WorkerCapabilities.newBuilder()
        .addSupportedDataFormats(UDFWorkerDataFormat.ARROW)
        .addSupportedCommunicationPatterns(
          UDFProtoCommunicationPattern.BIDIRECTIONAL_STREAMING)
        .build())
      .setDirect(DirectWorker.newBuilder()
        .setRunner(echoRunner)
        .setProperties(udsProperties)
        .build())
      .build()
    val d = new DirectGrpcDispatcher(spec)
    try {
      val s = d.createSession(None)
      try {
        s.init(basicInit())
        val out = s.process(echoIn("hello"), emptyFinish)
          .map(outStr).toList
        assert(out == List("hello"))
      } finally s.close(emptyCancel)
    } finally d.close()
  }

  test("clean session end leaves worker salvageable (reuse-readiness invariant)") {
    // Future pooling relies on workerProcess.isInvalid being false for a
    // session that completed without errors. Pin this contract now.
    newDispatcher()
    val session = dispatcher.createSession(None)
    val workerProc = workerProcessOf(session)
    try {
      session.init(basicInit())
      val out = session.process(echoIn("ping"), emptyFinish).toList
      assert(out.map(outStr) == List("ping"))
    } finally session.close(emptyCancel)
    assert(!workerProc.isInvalid,
      "worker must remain salvageable after a clean Finish")
  }

}
