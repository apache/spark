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

import java.io.{File, IOException}
import java.nio.file.{Files, FileVisitResult, Path, Paths, SimpleFileVisitor}
import java.nio.file.attribute.{BasicFileAttributes, PosixFilePermissions}

import org.apache.spark.udf.worker.{Cancel, DataRequest, DataResponse, Finish, FinishResponse,
  Init, InitResponse, UDFProtoCommunicationPattern, UDFWorkerSpecification, WorkerConnectionSpec}
import org.apache.spark.udf.worker.core.direct.{DirectWorkerDispatcher, DirectWorkerException,
  DirectWorkerTimeoutException}
import org.apache.spark.udf.worker.core.direct.DirectWorkerDispatcher.SOCKET_POLL_INTERVAL_MS

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

  // Upper bound on the rename-on-collision retry loop in newEndpointAddress.
  private val MAX_SOCKET_LEAF_RETRIES = 16

  // The private 0700 socket directory, created in [[initialize]] (after the
  // base class has validated the spec) and removed in [[closeTransport]].
  private lazy val socketDir: Path = createPrivateTempDirectory()

  override protected def initialize(): Unit = {
    super.initialize()
    // Force the lazy val now so the directory is created (and any failure
    // surfaces) at construction time, after spec validation has passed.
    socketDir
  }

  /**
   * Returns the UDS path the worker should bind. Uses a short 16-hex-char leaf
   * (the worker's full UUID still travels via `--id`) to stay within the
   * 108-byte UDS `sun_path` limit.
   */
  override protected def newEndpointAddress(workerId: String): String = {
    val short = workerId.replace("-", "").take(16)
    var candidate = socketDir.resolve(s"w-$short.sock")
    var suffix = 0
    while (Files.exists(candidate) && suffix < MAX_SOCKET_LEAF_RETRIES) {
      suffix += 1
      candidate = socketDir.resolve(s"w-$short-$suffix.sock")
    }
    if (Files.exists(candidate)) {
      throw new IllegalStateException(
        s"could not allocate a free UDS path under $socketDir after " +
          s"$MAX_SOCKET_LEAF_RETRIES retries (truncated id=$short)")
    }
    candidate.toString
  }

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
    if (!Files.exists(socketDir)) return
    // Recursive post-order delete: today socketDir contains only socket files
    // at the top level, but a future change that namespaces workers into
    // subdirectories should not silently leak them.
    Files.walkFileTree(socketDir, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        try Files.deleteIfExists(file) catch { case _: IOException => () }
        FileVisitResult.CONTINUE
      }
      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        try Files.deleteIfExists(dir) catch { case _: IOException => () }
        FileVisitResult.CONTINUE
      }
    })
  }

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

  override protected def newConnection(address: String): WorkerConnection =
    new SocketFileConnection(address)

  override protected def newSession(
      workerHandle: WorkerHandle,
      connection: WorkerConnection): WorkerSession =
    new NoOpWorkerSession(workerHandle, logger)

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
   * Creates a private (owner-only, 0700) temp directory for worker sockets.
   * On POSIX filesystems the permissions are applied atomically at creation;
   * the non-POSIX branch tightens best-effort after creation.
   *
   * The directory is anchored under a short base path (see [[shortTempBase]])
   * rather than the configured `java.io.tmpdir`. Builds point the latter at a
   * deep location (e.g. `<module>/target/tmp`), which combined with the
   * generated socket leaf would push the worker's Unix-domain socket path past
   * the platform `sun_path` limit (108 bytes on Linux, 104 on macOS) and fail
   * with "AF_UNIX path too long".
   */
  private def createPrivateTempDirectory(): Path = {
    val attr = PosixFilePermissions.asFileAttribute(
      PosixFilePermissions.fromString("rwx------"))
    val base = shortTempBase()
    try {
      Files.createTempDirectory(base, "spark-udf-worker", attr)
    } catch {
      case _: UnsupportedOperationException =>
        val dir = Files.createTempDirectory(base, "spark-udf-worker")
        val f = dir.toFile
        // Bit-wise AND (NOT &&): all six setters must run even if an earlier
        // one returns false, so the final permission state matches owner-only.
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

  /**
   * Picks the shortest usable base directory for the socket temp dir so the
   * resulting Unix-domain socket path stays within the platform `sun_path`
   * limit. Prefers "/tmp" when it is a writable directory (true on the Linux
   * and macOS hosts these tests run on); otherwise falls back to the
   * configured `java.io.tmpdir`.
   */
  private def shortTempBase(): Path = {
    val fallback = Paths.get(System.getProperty("java.io.tmpdir"))
    val shortTmp = Paths.get("/tmp")
    if (Files.isDirectory(shortTmp) && Files.isWritable(shortTmp)) shortTmp else fallback
  }
}
