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
package org.apache.spark.udf.worker.core.direct

import java.io.File
import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermissions

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.udf.worker.core.{UnixSocketWorkerConnection, WorkerLogger}
import org.apache.spark.udf.worker.core.direct.DirectWorkerDispatcher.SOCKET_POLL_INTERVAL_MS

/**
 * :: Experimental ::
 * A [[DirectWorkerDispatcher]] using Unix domain sockets as the worker
 * transport. Allocates a private 0700 socket directory at construction;
 * each worker is given a UDS path inside it.
 *
 * Concrete subclasses implement [[createConnection]] (with a UDS protocol
 * of choice) and [[createSessionForWorker]].
 */
@Experimental
abstract class DirectUnixSocketWorkerDispatcher(
    workerSpec: UDFWorkerSpecification,
    logger: WorkerLogger = WorkerLogger.NoOp)
  extends DirectWorkerDispatcher(workerSpec, logger) {

  // Removed explicitly in closeTransport(). deleteOnExit is avoided because
  // the JDK retains the path for the JVM lifetime, which leaks in
  // long-lived drivers.
  private val socketDir: Path = createPrivateTempDirectory()

  override protected def newEndpointAddress(workerId: String): String =
    socketDir.resolve(s"worker-$workerId.sock").toString

  override protected def waitForReady(
      address: String,
      process: Process,
      outputFile: File): Unit = {
    val file = new File(address)
    // At least one poll so very small initTimeouts don't trip a premature
    // timeout before the worker has any chance to create the socket.
    val maxAttempts = math.max(1, (initTimeoutMs / SOCKET_POLL_INTERVAL_MS).toInt)
    var attempts = 0
    while (!file.exists() && attempts < maxAttempts) {
      if (!process.isAlive) throwWorkerExitedBeforeSocket(process, address, outputFile)
      Thread.sleep(SOCKET_POLL_INTERVAL_MS)
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
  }

  override protected def cleanupEndpointAddress(address: String): Unit = {
    Files.deleteIfExists(new File(address).toPath)
  }

  override protected def closeTransport(): Unit = {
    val dir = socketDir.toFile
    if (dir.exists()) {
      val remaining = dir.listFiles()
      if (remaining != null) remaining.foreach(_.delete())
      dir.delete()
    }
  }

  override protected def validateTransportSupport(): Unit = {
    val props = workerSpec.getDirect.getProperties
    require(props.hasConnection,
      "DirectWorker.properties.connection must be set")
    val conn = props.getConnection
    require(conn.hasUnixDomainSocket,
      "DirectUnixSocketWorkerDispatcher requires UNIX domain socket transport, " +
        s"got ${conn.getTransportCase}")
  }

  override protected def createConnection(address: String): UnixSocketWorkerConnection

  private def throwWorkerExitedBeforeSocket(
      process: Process,
      address: String,
      outputFile: File): Nothing = {
    val tail = readOutputTail(outputFile)
    throw new DirectWorkerException(
      s"Worker exited with code ${process.exitValue()} " +
        s"before creating socket at $address\n$tail")
  }

  /**
   * Creates a temp directory with owner-only permissions (0700 on POSIX).
   * On non-POSIX filesystems falls back to best-effort `File.setXxx`,
   * which is TOCTOU-racy and weaker; a WARN surfaces if the platform
   * refuses the setters.
   */
  private def createPrivateTempDirectory(): Path = {
    val attr = PosixFilePermissions.asFileAttribute(
      PosixFilePermissions.fromString("rwx------"))
    try {
      Files.createTempDirectory("spark-udf-worker", attr)
    } catch {
      case _: UnsupportedOperationException =>
        val dir = Files.createTempDirectory("spark-udf-worker")
        val f = dir.toFile
        // `&` (non-short-circuiting) so every setter is attempted even if
        // an earlier one refused.
        val applied =
          f.setReadable(false, false) & f.setWritable(false, false) &
            f.setExecutable(false, false) & f.setReadable(true, true) &
            f.setWritable(true, true) & f.setExecutable(true, true)
        if (!applied) {
          logger.warn(
            s"Could not fully restrict permissions on $dir; socket " +
              s"directory may be accessible to other local users on this " +
              s"filesystem")
        }
        dir
    }
  }
}
