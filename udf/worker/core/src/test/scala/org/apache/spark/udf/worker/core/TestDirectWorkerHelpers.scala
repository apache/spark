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

import org.apache.spark.udf.worker.{Cancel, DataRequest, DataResponse, Finish, FinishResponse,
  Init, InitResponse, UDFProtoCommunicationPattern, UDFWorkerSpecification, WorkerConnectionSpec}
import org.apache.spark.udf.worker.core.direct.{DirectWorkerDispatcher, DirectWorkerException,
  DirectWorkerProcess, DirectWorkerTimeoutException, UnixDomainSocketEndpointDirectory}
import org.apache.spark.udf.worker.core.direct.DirectWorkerDispatcher.READY_POLL_INTERVAL_MS

/**
 * A [[WorkerConnection]] test implementation that treats the connection as
 * active as long as the worker's UDS file exists on disk. The socket file is
 * removed on close.
 *
 * Suitable for dispatcher-lifecycle tests that don't need to drive a wire
 * protocol -- e.g. verifying that a worker spec spawns a real worker process
 * that creates the expected socket.
 */
class SocketFileConnection(val socketPath: String) extends WorkerConnection {
  override def isActive: Boolean = new File(socketPath).exists()
  override def close(): Unit = {
    val f = new File(socketPath)
    if (f.exists()) f.delete()
  }
}

/**
 * No-op [[WorkerSession]] for lifecycle-only tests. All protocol methods are
 * inert (init/finish report empty responses); tests that exercise the actual
 * wire protocol use a concrete transport-backed session.
 */
class NoOpWorkerSession(
    workerHandle: WorkerHandle,
    logger: WorkerLogger = WorkerLogger.NoOp)
  extends WorkerSession(workerHandle, logger) {

  override protected def doInit(message: Init): InitResponse = InitResponse.getDefaultInstance
  override protected def doProcess(
      input: Iterator[DataRequest],
      finish: () => Finish): Iterator[DataResponse] =
    Iterator.empty[DataResponse]
  override protected def doClose(cancel: () => Cancel): Termination = {
    // Settle the clean terminal so close() does not fall through to its
    // contract-violation recovery path. A no-op session has no in-flight work,
    // so the cancel thunk is never needed.
    completeTerminal(Termination.Finished(FinishResponse.getDefaultInstance))
    settledTermination
  }
}

/**
 * A concrete [[DirectWorkerDispatcher]] for tests that spawns workers over a
 * Unix domain socket and yields [[SocketFileConnection]]s / [[NoOpWorkerSession]]s,
 * so lifecycle tests exercise the dispatcher's spawn / wait-for-ready / cleanup
 * machinery without driving a wire protocol. Allocates a private 0700 socket
 * directory at construction; each worker is given a UDS path inside it.
 *
 * Reusable across modules: callers in `sql/core` (or anywhere with a test-jar
 * dependency on `udf-worker-core`) can drop this in for tests that only need to
 * verify a worker spec produces a spawnable worker.
 */
class TestDirectWorkerDispatcher(
    workerSpec: UDFWorkerSpecification,
    logger: WorkerLogger = WorkerLogger.NoOp)
  extends DirectWorkerDispatcher(workerSpec, logger) {

  private lazy val endpointDirectory = new UnixDomainSocketEndpointDirectory(logger)

  override protected def initialize(): Unit = {
    super.initialize()
    endpointDirectory
  }

  override protected def newEndpointAddress(workerId: String): String =
    endpointDirectory.newEndpointAddress(workerId)

  override protected def connectWorker(
      address: String,
      process: Process,
      outputFile: File): WorkerConnection = {
    val file = new File(address)
    // At least one poll so very small initTimeouts don't trip a premature
    // timeout before the worker has any chance to create the socket.
    val maxAttempts = math.max(1, (initTimeoutMs / READY_POLL_INTERVAL_MS).toInt)
    var attempts = 0
    while (!file.exists() && attempts < maxAttempts) {
      if (!process.isAlive) throwWorkerExitedBeforeSocket(process, address, outputFile)
      Thread.sleep(READY_POLL_INTERVAL_MS)
      attempts += 1
    }
    if (!file.exists()) {
      if (process.isAlive) {
        DirectWorkerDispatcher.destroyForciblyAndReap(
          process, logger, s"init timeout $address")
        val tail = readOutputTail(outputFile)
        throw new DirectWorkerTimeoutException(
          s"Worker did not create socket at $address within ${initTimeoutMs}ms\n$tail")
      } else {
        // Worker exited after the last poll without creating the socket;
        // prefer the exit-code message over the ambiguous "did not create".
        throwWorkerExitedBeforeSocket(process, address, outputFile)
      }
    }
    newConnection(address)
  }

  override protected def cleanupEndpointAddress(address: String): Unit =
    endpointDirectory.cleanupEndpointAddress(address)

  override protected def closeTransport(): Unit = endpointDirectory.close()

  // `spec` is the same object as the `workerSpec` field but passed explicitly:
  // at the point this runs (parent constructor body), `this` is only partially
  // constructed and reading subclass fields is unsafe.
  override protected def validateTransportSupport(spec: UDFWorkerSpecification): Unit = {
    val props = spec.getDirect.getProperties
    require(props.hasConnection,
      "DirectWorker.properties.connection must be set")
    val conn = props.getConnection
    require(conn.getTransportCase == WorkerConnectionSpec.TransportCase.UNIX_DOMAIN_SOCKET,
      "TestDirectWorkerDispatcher requires UNIX domain socket transport, " +
        s"got ${conn.getTransportCase}")
    require(spec.hasCapabilities,
      "TestDirectWorkerDispatcher requires WorkerCapabilities declaring " +
        "BIDIRECTIONAL_STREAMING in supported_communication_patterns")
    val patterns = spec.getCapabilities.getSupportedCommunicationPatternsList
    val supportsBidi = (0 until patterns.size()).exists { i =>
      patterns.get(i) == UDFProtoCommunicationPattern.BIDIRECTIONAL_STREAMING
    }
    require(supportsBidi,
      "TestDirectWorkerDispatcher requires BIDIRECTIONAL_STREAMING " +
        "in WorkerCapabilities.supported_communication_patterns")
  }

  protected def newConnection(address: String): WorkerConnection =
    new SocketFileConnection(address)

  override protected def newSession(worker: DirectWorkerProcess): WorkerSession =
    new NoOpWorkerSession(worker, logger)

  private def throwWorkerExitedBeforeSocket(
      process: Process,
      address: String,
      outputFile: File): Nothing = {
    val tail = readOutputTail(outputFile)
    throw new DirectWorkerException(
      s"Worker exited with code ${process.exitValue()} " +
        s"before creating socket at $address\n$tail")
  }
}
