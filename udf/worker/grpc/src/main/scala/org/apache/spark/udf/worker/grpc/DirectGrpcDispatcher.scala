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
import java.nio.file.{Files, Paths}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.util.control.NonFatal

import io.grpc.ConnectivityState

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.{UDFProtoCommunicationPattern, UDFWorkerSpecification,
  WorkerConnectionSpec}
import org.apache.spark.udf.worker.core.{WorkerConnection, WorkerLogger, WorkerSession}
import org.apache.spark.udf.worker.core.direct.{DirectWorkerDispatcher, DirectWorkerException,
  DirectWorkerProcess, DirectWorkerTimeoutException, UnixDomainSocketEndpointDirectory}
import org.apache.spark.udf.worker.core.direct.DirectWorkerDispatcher.READY_POLL_INTERVAL_MS

/**
 * :: Experimental ::
 * A concrete [[DirectWorkerDispatcher]] that spawns workers and talks to
 * them over the UDF gRPC protocol on a Unix domain socket. Allocates a
 * private 0700 socket directory at construction; each worker is given a
 * UDS path inside it.
 *
 * @param workerSpec worker specification used to launch each worker.
 * @param logger logger for lifecycle diagnostics.
 */
@Experimental
class DirectGrpcDispatcher(
    workerSpec: UDFWorkerSpecification,
    logger: WorkerLogger = WorkerLogger.NoOp)
  extends DirectWorkerDispatcher(workerSpec, logger) {

  // Laziness avoids reading subclass state during the base constructor's
  // validation phase. initialize() forces deterministic allocation afterward.
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
    val connection = newConnection(address)
    try {
      waitForReady(address, connection, process, outputFile)
      connection
    } catch {
      case e: InterruptedException =>
        closeFailedConnection(connection)
        throw e
      case NonFatal(e) =>
        closeFailedConnection(connection)
        throw e
    }
  }

  private def waitForReady(
      address: String,
      connection: WorkerConnection,
      process: Process,
      outputFile: File): Unit = {
    val grpc = connection match {
      case channel: GrpcWorkerChannel => channel
      case other =>
        throw new IllegalStateException(
          s"DirectGrpcDispatcher.newConnection should have produced a " +
            s"GrpcWorkerChannel but got ${other.getClass.getName}")
    }
    val deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(initTimeoutMs)
    var state = grpc.channel.getState(true)
    while (state != ConnectivityState.READY) {
      if (!process.isAlive) throwWorkerExitedBeforeReady(process, address, outputFile)

      val remainingNanos = deadlineNanos - System.nanoTime()
      if (remainingNanos <= 0L) {
        val tail = readOutputTail(outputFile)
        throw new DirectWorkerTimeoutException(
          s"Worker did not become reachable at $address within ${initTimeoutMs}ms\n$tail")
      }

      // A failed connection attempt enters gRPC backoff. While the worker's
      // fresh socket exists, reset each observed backoff so a server that is
      // still starting does not consume the initialization budget.
      if (DirectGrpcDispatcher.shouldResetConnectBackoff(
          state, Files.exists(Paths.get(address)))) {
        grpc.channel.resetConnectBackoff()
      }
      val stateChanged = new CountDownLatch(1)
      grpc.channel.notifyWhenStateChanged(state, () => stateChanged.countDown())
      val pollNanos = TimeUnit.MILLISECONDS.toNanos(READY_POLL_INTERVAL_MS)
      stateChanged.await(math.min(remainingNanos, pollNanos), TimeUnit.NANOSECONDS)
      state = grpc.channel.getState(true)
    }
  }

  override protected def cleanupEndpointAddress(address: String): Unit =
    endpointDirectory.cleanupEndpointAddress(address)

  override protected def closeTransport(): Unit = endpointDirectory.close()

  // `spec` is the same object as the `workerSpec` field but passed
  // explicitly: at the point this runs (parent constructor body), `this`
  // is only partially constructed and reading subclass fields is unsafe.
  // See the contract on the abstract method in [[DirectWorkerDispatcher]].
  override protected def validateTransportSupport(spec: UDFWorkerSpecification): Unit = {
    val props = spec.getDirect.getProperties
    require(props.hasConnection,
      "DirectWorker.properties.connection must be set")
    val conn = props.getConnection
    require(conn.getTransportCase == WorkerConnectionSpec.TransportCase.UNIX_DOMAIN_SOCKET,
      "DirectGrpcDispatcher requires UNIX domain socket transport, " +
        s"got ${conn.getTransportCase}")
    // BIDIRECTIONAL_STREAMING is the only pattern the gRPC `Execute` RPC
    // speaks, so the spec MUST advertise it. We require the capabilities block
    // and the pattern explicitly rather than treating an unset/empty block as
    // "no constraint": a spec that does not declare bidi gives no evidence the
    // worker can speak this transport, and accepting it would only defer the
    // failure to stream time.
    require(spec.hasCapabilities,
      "DirectGrpcDispatcher requires WorkerCapabilities declaring " +
        "BIDIRECTIONAL_STREAMING in supported_communication_patterns")
    val patterns = spec.getCapabilities.getSupportedCommunicationPatternsList
    val supportsBidi = (0 until patterns.size()).exists { i =>
      patterns.get(i) == UDFProtoCommunicationPattern.BIDIRECTIONAL_STREAMING
    }
    require(supportsBidi,
      "DirectGrpcDispatcher requires BIDIRECTIONAL_STREAMING " +
        "in WorkerCapabilities.supported_communication_patterns")
  }

  protected def newConnection(address: String): WorkerConnection =
    new GrpcWorkerChannel(address, logger)

  override protected def newSession(worker: DirectWorkerProcess): WorkerSession =
    worker.connection match {
      case g: GrpcWorkerChannel =>
        new GrpcWorkerSession(worker, g.channel, logger)
      case other =>
        throw new IllegalStateException(
          s"DirectGrpcDispatcher.newConnection should have produced a " +
            s"GrpcWorkerChannel but got ${other.getClass.getName}")
    }

  private def throwWorkerExitedBeforeReady(
      process: Process,
      address: String,
      outputFile: File): Nothing = {
    val tail = readOutputTail(outputFile)
    throw new DirectWorkerException(
      s"Worker exited with code ${process.exitValue()} " +
        s"before becoming reachable at $address\n$tail")
  }

  private def closeFailedConnection(connection: WorkerConnection): Unit = {
    try connection.close() catch {
      case NonFatal(e) => logger.debug("Failed to close worker connection", e)
    }
  }

}

private[grpc] object DirectGrpcDispatcher {
  private[grpc] def shouldResetConnectBackoff(
      state: ConnectivityState,
      endpointExists: Boolean): Boolean =
    endpointExists && state == ConnectivityState.TRANSIENT_FAILURE
}
