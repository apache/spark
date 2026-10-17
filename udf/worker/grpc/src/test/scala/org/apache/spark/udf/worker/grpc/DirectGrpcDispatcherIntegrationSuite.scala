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

import java.io.{File, IOException}
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Callable, TimeUnit}

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString
import org.scalatest.BeforeAndAfterEach
// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker.{Cancel, DataRequest, DirectWorker, Finish, Init,
  ProcessCallable, UdfPayload, UDFProtoCommunicationPattern, UDFWorkerDataFormat,
  UDFWorkerProperties, UDFWorkerSpecification, UnixDomainSocket, WorkerCapabilities,
  WorkerConnectionSpec}
import org.apache.spark.udf.worker.core.{WorkerConnection, WorkerSession}
import org.apache.spark.udf.worker.core.direct.{DirectWorkerProcess, DirectWorkerTimeoutException}
import org.apache.spark.udf.worker.grpc.testing.{EchoGrpcWorkerMain, ForwardingWorkerMain}

/**
 * End-to-end coverage for the integration points unique to [[DirectGrpcDispatcher]]:
 * spawning a real gRPC worker over a Unix domain socket and waiting for gRPC
 * readiness rather than only for the socket file.
 */
class DirectGrpcDispatcherIntegrationSuite
    extends AnyFunSuite with BeforeAndAfterEach {
// scalastyle:on funsuite

  private val javaClasspath = System.getProperty("java.class.path")
  private val javaExecutable =
    Paths.get(System.getProperty("java.home"), "bin", "java").toString
  private val workerMainClass =
    classOf[EchoGrpcWorkerMain.type].getName.stripSuffix("$")

  private var dispatcher: DirectGrpcDispatcher = _

  override def beforeEach(): Unit = {
    val supported = try {
      UnixDomainSocketTransport.detect()
      true
    } catch {
      case _: UnsupportedOperationException => false
    }
    assume(supported,
      "Netty UDS native transport (epoll on Linux or kqueue on macOS) is required")
  }

  override def afterEach(): Unit = {
    if (dispatcher != null) {
      try dispatcher.close() finally dispatcher = null
    }
    super.afterEach()
  }

  private def echoRunner: ProcessCallable = ProcessCallable.newBuilder()
    .addCommand(javaExecutable)
    .addCommand("-cp")
    .addCommand(javaClasspath)
    .addCommand(workerMainClass)
    .build()

  private def workerSpec(
      runner: ProcessCallable = echoRunner,
      initTimeoutMs: Int = 30000): UDFWorkerSpecification =
    UDFWorkerSpecification.newBuilder()
      .setCapabilities(WorkerCapabilities.newBuilder()
        .addSupportedDataFormats(UDFWorkerDataFormat.ARROW)
        .addSupportedCommunicationPatterns(UDFProtoCommunicationPattern.BIDIRECTIONAL_STREAMING)
        .build())
      .setDirect(DirectWorker.newBuilder()
        .setRunner(runner)
        .setProperties(UDFWorkerProperties.newBuilder()
          .setConnection(WorkerConnectionSpec.newBuilder()
            .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance)
            .build())
          .setInitializationTimeoutMs(initTimeoutMs)
          .setGracefulTerminationTimeoutMs(10000)
          .build())
        .build())
      .build()

  private def basicInit: Init = Init.newBuilder()
    .setProtocolVersion(1)
    .setDataFormat(UDFWorkerDataFormat.ARROW)
    .setUdf(UdfPayload.newBuilder()
      .setPayload(ByteString.copyFromUtf8("echo"))
      .setFormat("echo")
      .build())
    .build()

  private val emptyFinish: () => Finish = () => Finish.getDefaultInstance
  private val emptyCancel: () => Cancel = () => Cancel.getDefaultInstance

  private def workerProcess(session: WorkerSession): DirectWorkerProcess =
    session.workerHandle match {
      case process: DirectWorkerProcess => process
      case other => fail(s"Expected DirectWorkerProcess, got ${other.getClass.getSimpleName}")
    }

  private def grpcChannel(process: DirectWorkerProcess): GrpcWorkerChannel =
    process.connection match {
      case channel: GrpcWorkerChannel => channel
      case other => fail(s"Expected GrpcWorkerChannel, got ${other.getClass.getSimpleName}")
    }

  // An orphaned worker is reparented, and an unreaped zombie stays isAlive
  // until PID 1 reaps it. State Z in /proc means the worker has already exited.
  private def assertWorkerStopped(worker: ProcessHandle, what: String): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (System.nanoTime() < deadline && !hasExited(worker)) {
      Thread.sleep(50)
    }
    if (!hasExited(worker)) {
      worker.destroyForcibly()
      fail(s"$what ${worker.pid}")
    }
  }

  private def hasExited(worker: ProcessHandle): Boolean = {
    !worker.isAlive || linuxState(worker.pid).contains('Z')
  }

  private def linuxState(pid: Long): Option[Char] = {
    val stat = Paths.get(s"/proc/$pid/stat")
    val text = try {
      if (Files.isRegularFile(stat)) Some(Files.readString(stat)) else None
    } catch {
      case _: IOException => None
    }
    text.flatMap { raw =>
      val close = raw.lastIndexOf(')')
      if (close < 0 || close + 2 >= raw.length) None else Some(raw.charAt(close + 2))
    }
  }

  test("event-loop threads are named daemons and terminate on shutdown") {
    val eventLoopGroup = UnixDomainSocketTransport.detect().newEventLoopGroup()
    try {
      val eventLoopThread = eventLoopGroup.next().submit(new Callable[Thread] {
        override def call(): Thread = Thread.currentThread()
      }).get(10, TimeUnit.SECONDS)

      assert(eventLoopThread.isDaemon)
      assert(eventLoopThread.getName.startsWith(
        UnixDomainSocketTransport.EVENT_LOOP_THREAD_NAME_PREFIX))
    } finally {
      UnixDomainSocketTransport.shutdown(eventLoopGroup, 5000L)
    }
    assert(eventLoopGroup.terminationFuture().isDone)
  }

  test("spawns a gRPC worker and round-trips a response above gRPC's default limit") {
    dispatcher = new DirectGrpcDispatcher(workerSpec())
    val session = dispatcher.createSession(None)
    val process = workerProcess(session)
    val channel = grpcChannel(process)
    val socketFile = new File(channel.socketPath)
    val socketDir = socketFile.getParentFile
    val payload = ByteString.copyFrom(Array.fill[Byte](5 * 1024 * 1024)(7))

    try {
      session.init(basicInit)
      val input = DataRequest.newBuilder().setData(payload).build()
      assert(session.process(Iterator.single(input), emptyFinish).map(_.getData).toList ==
        List(payload))
    } finally {
      session.close(emptyCancel)
    }

    assert(!process.process.isAlive, "session close should terminate the worker")
    assert(!socketFile.exists(), "session close should remove the worker socket")
    dispatcher.close()
    dispatcher = null
    assert(!socketDir.exists(), "dispatcher close should remove its socket directory")
  }

  test("a launcher command that forwards the connection serves sessions") {
    val launcher = ProcessCallable.newBuilder()
      .addCommand(javaExecutable)
      .addCommand("-cp")
      .addCommand(javaClasspath)
      .addCommand(classOf[ForwardingWorkerMain.type].getName.stripSuffix("$"))
      .addAllCommand(echoRunner.getCommandList)
      .build()
    dispatcher = new DirectGrpcDispatcher(workerSpec(launcher))
    val sessions = Seq.fill(2)(dispatcher.createSession(None))
    val processes = sessions.map(workerProcess)
    val workers = processes.flatMap(_.process.descendants().iterator().asScala)
    val payloads = Seq(
      ByteString.copyFrom(Array.fill[Byte](5 * 1024 * 1024)(3)),
      ByteString.copyFromUtf8("small"))

    try {
      assert(workers.size == 2, s"each launcher should start one worker, got $workers")
      sessions.zip(payloads).foreach { case (session, payload) =>
        session.init(basicInit)
        val input = DataRequest.newBuilder().setData(payload).build()
        assert(session.process(Iterator.single(input), emptyFinish).map(_.getData).toList ==
          List(payload))
      }
    } finally {
      sessions.foreach(_.close(emptyCancel))
    }

    assert(processes.forall(!_.process.isAlive), "session close should terminate the launcher")
    workers.foreach { worker =>
      assertWorkerStopped(worker, "session close should stop the launched worker")
    }
  }

  test("SIGKILL of the forwarding launcher stops the inner worker") {
    val launcher = ProcessCallable.newBuilder()
      .addCommand(javaExecutable)
      .addCommand("-cp")
      .addCommand(javaClasspath)
      .addCommand(classOf[ForwardingWorkerMain.type].getName.stripSuffix("$"))
      .addAllCommand(echoRunner.getCommandList)
      .build()
    dispatcher = new DirectGrpcDispatcher(workerSpec(launcher))
    val session = dispatcher.createSession(None)
    val process = workerProcess(session)
    val workers = process.process.descendants().iterator().asScala.toList
    assert(workers.size == 1, s"the launcher should start one worker, got $workers")
    val worker = workers.head
    try {
      session.init(basicInit)
      process.process.destroyForcibly()
      assertWorkerStopped(worker, "SIGKILL of the launcher should stop worker")
    } finally {
      session.close(emptyCancel)
    }
  }

  test("the channel overrides the authority derived from the socket path") {
    dispatcher = new DirectGrpcDispatcher(workerSpec())
    val session = dispatcher.createSession(None)
    val channel = grpcChannel(workerProcess(session))

    try {
      // Without the override the authority would be the socket path, which a conforming
      // HTTP/2 server rejects with PROTOCOL_ERROR while decoding the HEADERS frame.
      // The in-tree worker is grpc-java, which tolerates the encoded form, so this asserts on
      // the channel's configured authority rather than on an end-to-end failure.
      val authority = channel.channel.authority()
      assert(authority === GrpcWorkerChannel.UDS_AUTHORITY,
        s"expected the overridden authority, got '$authority'")
    } finally {
      session.close(emptyCancel)
    }
  }

  test("a socket path alone does not make a worker ready") {
    val socketOnlyWorker =
      """
        |socket_path=""
        |while [[ $# -gt 0 ]]; do
        |  case "$1" in
        |    --connection) socket_path="$2"; shift 2 ;;
        |    *) shift ;;
        |  esac
        |done
        |trap 'rm -f "$socket_path"; exit 0' SIGTERM
        |touch "$socket_path"
        |echo socket-created
        |while true; do sleep 1; done
      """.stripMargin.trim
    val runner = ProcessCallable.newBuilder()
      .addCommand("bash")
      .addCommand("-c")
      .addCommand(socketOnlyWorker)
      .addCommand("--")
      .build()
    var failedProcess: Process = null
    var failedConnection: GrpcWorkerChannel = null
    dispatcher = new DirectGrpcDispatcher(workerSpec(runner, initTimeoutMs = 1000)) {
      override protected def connectWorker(
          address: String,
          process: Process,
          outputFile: File): WorkerConnection = {
        failedProcess = process
        super.connectWorker(address, process, outputFile)
      }

      override protected def newConnection(address: String): WorkerConnection = {
        val connection = super.newConnection(address)
        failedConnection = connection.asInstanceOf[GrpcWorkerChannel]
        connection
      }
    }

    val error = intercept[DirectWorkerTimeoutException] {
      dispatcher.createSession(None)
    }
    assert(error.getMessage.contains("did not become reachable"))
    assert(error.getMessage.contains("socket-created"))
    assert(failedProcess != null, "the failed worker process should have been captured")
    assert(!failedProcess.isAlive, "the failed worker process should be reaped")
    assert(failedConnection != null, "the failed worker connection should have been captured")
    assert(failedConnection.channel.isShutdown, "the failed gRPC channel should be shut down")
    assert(failedConnection.isEventLoopTerminated,
      "the failed gRPC channel's event loop should be terminated")
    assert(!new File(failedConnection.socketPath).exists(),
      "the failed worker socket should be removed")
  }
}
