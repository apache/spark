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
import java.nio.file.{Files, FileVisitResult, Path, SimpleFileVisitor}
import java.nio.file.attribute.{BasicFileAttributes, PosixFilePermissions}

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.{UDFProtoCommunicationPattern, UDFWorkerSpecification,
  WorkerConnectionSpec}
import org.apache.spark.udf.worker.core.{WorkerConnection, WorkerHandle, WorkerLogger,
  WorkerSession}
import org.apache.spark.udf.worker.core.direct.{DirectWorkerDispatcher, DirectWorkerException,
  DirectWorkerTimeoutException}
import org.apache.spark.udf.worker.core.direct.DirectWorkerDispatcher.SOCKET_POLL_INTERVAL_MS

/**
 * :: Experimental ::
 * A concrete [[DirectWorkerDispatcher]] that spawns workers and talks to
 * them over the UDF gRPC protocol on a Unix domain socket. Allocates a
 * private 0700 socket directory at construction; each worker is given a
 * UDS path inside it.
 */
@Experimental
class DirectGrpcDispatcher(
    workerSpec: UDFWorkerSpecification,
    logger: WorkerLogger = WorkerLogger.NoOp)
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

  override protected def newConnection(address: String): WorkerConnection =
    new GrpcWorkerChannel(address, logger)

  override protected def newSession(workerHandle: WorkerHandle): WorkerSession =
    workerHandle.connection match {
      case g: GrpcWorkerChannel =>
        new GrpcWorkerSession(workerHandle, g.channel, logger)
      case other =>
        throw new IllegalStateException(
          s"DirectGrpcDispatcher.newConnection should have produced a " +
            s"GrpcWorkerChannel but got ${other.getClass.getName}")
    }

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
}
