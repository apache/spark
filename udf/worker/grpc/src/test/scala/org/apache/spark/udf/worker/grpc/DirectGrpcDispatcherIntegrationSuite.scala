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

import com.google.protobuf.ByteString
import org.scalatest.BeforeAndAfterEach
// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker.{Cancel, DataRequest, DirectWorker, Finish, Init,
  ProcessCallable, UdfPayload, UDFProtoCommunicationPattern, UDFWorkerDataFormat,
  UDFWorkerProperties, UDFWorkerSpecification, UnixDomainSocket, WorkerCapabilities,
  WorkerConnectionSpec}
import org.apache.spark.udf.worker.core.WorkerSession
import org.apache.spark.udf.worker.core.direct.{DirectWorkerProcess, DirectWorkerTimeoutException}
import org.apache.spark.udf.worker.grpc.testing.EchoGrpcWorkerMain

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
    dispatcher = new DirectGrpcDispatcher(workerSpec(runner, initTimeoutMs = 1000))

    val error = intercept[DirectWorkerTimeoutException] {
      dispatcher.createSession(None)
    }
    assert(error.getMessage.contains("did not become reachable"))
    assert(error.getMessage.contains("socket-created"))
  }
}
