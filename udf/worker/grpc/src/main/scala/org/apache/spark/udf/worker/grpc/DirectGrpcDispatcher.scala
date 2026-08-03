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
import java.nio.file.{Files, FileVisitResult, Path, Paths, SimpleFileVisitor}
import java.nio.file.attribute.{BasicFileAttributes, PosixFilePermissions}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.util.control.NonFatal

import io.grpc.ConnectivityState

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.{UDFProtoCommunicationPattern, UDFWorkerSpecification,
  WorkerConnectionSpec}
import org.apache.spark.udf.worker.core.{WorkerConnection, WorkerHandle, WorkerLogger,
  WorkerSession}
import org.apache.spark.udf.worker.core.direct.{DirectWorkerDispatcher, DirectWorkerException,
  DirectWorkerTimeoutException}
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
 * @param grpcMaxInboundMessageSize maximum serialized response size accepted
 *                                  from a worker. Defaults to 128 MiB.
 */
@Experimental
class DirectGrpcDispatcher(
    workerSpec: UDFWorkerSpecification,
    logger: WorkerLogger = WorkerLogger.NoOp,
    grpcMaxInboundMessageSize: Int = GrpcWorkerChannel.DEFAULT_MAX_INBOUND_MESSAGE_SIZE)
  extends DirectWorkerDispatcher(workerSpec, logger) {

  // Upper bound on the rename-on-collision retry loop in newEndpointAddress.
  // 16 is well above any realistic concurrent-worker count and keeps the
  // failure mode bounded if something pathological occupies the directory.
  private val MAX_SOCKET_LEAF_RETRIES = 16

  // The private 0700 socket directory, created in [[initialize]] (after the
  // base class has validated the spec) and removed in [[closeTransport]].
  // `lazy val` + force-in-initialize keeps creation deterministic without
  // depending on subclass field-initialiser ordering. deleteOnExit is
  // avoided because the JDK retains the path for the JVM lifetime, which
  // leaks in long-lived drivers.
  private lazy val socketDir: Path = createPrivateTempDirectory()

  override protected def initialize(): Unit = {
    super.initialize()
    // Force the lazy val now so the directory is created (and any failure
    // surfaces) at construction time, after spec validation has passed.
    socketDir
  }

  /**
   * Returns the UDS path the worker should bind. Uses a short 16-hex-char leaf
   * (the worker's full UUID still travels via `--id`) to stay within the 108-byte
   * UDS `sun_path` limit: on macOS `$TMPDIR` (~47 chars) plus the private dir
   * (~29) plus a full-UUID leaf (49) overflows it and `bind(2)` fails with
   * `ENAMETOOLONG`. 64 bits makes collisions negligible; the retry loop is a
   * defensive guard so a near-impossible collision surfaces as a clear error
   * here rather than an opaque "worker exited" from [[waitForReady]].
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
    var socketTriggeredReconnect = false
    var state = grpc.channel.getState(true)
    while (state != ConnectivityState.READY) {
      if (!process.isAlive) throwWorkerExitedBeforeReady(process, address, outputFile)

      val remainingNanos = deadlineNanos - System.nanoTime()
      if (remainingNanos <= 0L) {
        val tail = readOutputTail(outputFile)
        throw new DirectWorkerTimeoutException(
          s"Worker did not become reachable at $address within ${initTimeoutMs}ms\n$tail")
      }

      // A failed pre-bind connection attempt enters gRPC backoff. Once the
      // worker creates its fresh socket, reset that backoff exactly once so
      // the channel can prove readiness without waiting for a long retry.
      if (state == ConnectivityState.TRANSIENT_FAILURE &&
          !socketTriggeredReconnect && Files.exists(Paths.get(address))) {
        grpc.channel.resetConnectBackoff()
        socketTriggeredReconnect = true
      }
      val stateChanged = new CountDownLatch(1)
      grpc.channel.notifyWhenStateChanged(state, () => stateChanged.countDown())
      val pollNanos = TimeUnit.MILLISECONDS.toNanos(READY_POLL_INTERVAL_MS)
      stateChanged.await(math.min(remainingNanos, pollNanos), TimeUnit.NANOSECONDS)
      state = grpc.channel.getState(true)
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
    new GrpcWorkerChannel(
      address, logger, maxInboundMessageSize = grpcMaxInboundMessageSize)

  override protected def newSession(workerHandle: WorkerHandle): WorkerSession =
    workerHandle.connection match {
      case g: GrpcWorkerChannel =>
        new GrpcWorkerSession(workerHandle, g.channel, logger)
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

  /**
   * Creates a private (owner-only, 0700) temp directory for worker sockets.
   *
   * On POSIX filesystems the permissions are applied atomically at creation via
   * a file attribute, so there is '''no''' TOCTOU window. The non-POSIX branch
   * cannot do that: `Files.createTempDirectory` first creates the directory with
   * the platform default mask, then `File.setXxx` tightens it, leaving a brief
   * window where the directory may be group/other-accessible. That race is an
   * accepted limitation of the best-effort fallback -- Spark UDF workers run on
   * POSIX in practice, the directory lives under the JVM temp dir, and a WARN is
   * logged if the platform refuses the setters outright. Further hardening of
   * the non-POSIX path (e.g. creating under an already-restricted parent) is out
   * of scope here.
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
        // && would short-circuit and silently leave permissions partially open.
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
   * limit (108 bytes on Linux, 104 on macOS).
   *
   * This mirrors how PySpark already chooses its UDS directory in
   * `python/run-tests.py` (`spark.python.unix.domain.socket.dir`): prefer the
   * OS temp-dir env vars (`TMPDIR`/`TEMP`/`TMP`) and fall back to `/tmp`. Builds
   * point `java.io.tmpdir` at a deep `<module>/target/tmp`, which combined with
   * the generated socket leaf would overflow `sun_path`; the OS temp dir is
   * short (e.g. a per-user `/tmp/...` on Linux runners, `/var/folders/...` via
   * `$TMPDIR` on macOS). `java.io.tmpdir` remains the last-resort fallback.
   */
  private def shortTempBase(): Path = {
    val candidates =
      Seq("TMPDIR", "TEMP", "TMP").flatMap(sys.env.get).filter(_.nonEmpty) :+ "/tmp"
    val usable = candidates
      .map(Paths.get(_))
      .filter(p => Files.isDirectory(p) && Files.isWritable(p))
    // Pick the SHORTEST usable base, measured by its *real* (symlink-resolved) path,
    // because that is what actually counts against the `sun_path` limit when the socket
    // is bound. On macOS `$TMPDIR` is a long `/var/folders/<...>/T` path while `/tmp`
    // resolves to the short `/private/tmp`; picking the first writable candidate (the old
    // behavior) chose the long `$TMPDIR` and overflowed the 104-byte macOS limit
    // ("AF_UNIX path too long"). Choosing the shortest real path avoids that.
    def realLen(p: Path): Int =
      try p.toRealPath().toString.length catch { case _: IOException => p.toString.length }
    usable
      .sortBy(realLen)
      .headOption
      .getOrElse(Paths.get(System.getProperty("java.io.tmpdir")))
  }
}
