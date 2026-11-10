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

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, FileVisitResult, Path, Paths, SimpleFileVisitor}
import java.nio.file.attribute.{BasicFileAttributes, PosixFilePermissions}
import java.util.Locale

import org.apache.spark.udf.worker.core.WorkerLogger

/**
 * Owns the filesystem lifecycle for direct-worker Unix domain sockets.
 *
 * The directory is private to its owner and anchored under a short temp path
 * so generated socket addresses fit the platform `sockaddr_un.sun_path` limit.
 * Individual socket files are created by workers and removed through
 * [[cleanupEndpointAddress]]; [[close]] recursively removes the directory.
 */
private[worker] final class UnixDomainSocketEndpointDirectory(
    logger: WorkerLogger,
    osName: String = System.getProperty("os.name", "")) extends AutoCloseable {

  import UnixDomainSocketEndpointDirectory._

  private val socketDir: Path = createPrivateTempDirectory()
  private val maxPathBytes: Int = maxPathBytesForOs(osName)

  /** Returns a collision-checked socket path for `workerId`. */
  def newEndpointAddress(workerId: String): String = {
    val shortId = workerId.replace("-", "").take(WORKER_ID_PREFIX_LENGTH)
    var candidate = socketDir.resolve(s"w-$shortId.sock")
    var suffix = 0
    while (Files.exists(candidate) && suffix < MAX_SOCKET_LEAF_RETRIES) {
      suffix += 1
      candidate = socketDir.resolve(s"w-$shortId-$suffix.sock")
    }
    if (Files.exists(candidate)) {
      throw new IllegalStateException(
        s"could not allocate a free UDS path under $socketDir after " +
          s"$MAX_SOCKET_LEAF_RETRIES retries (truncated id=$shortId)")
    }
    val address = candidate.toString
    val pathBytes = address.getBytes(StandardCharsets.UTF_8).length
    if (pathBytes > maxPathBytes) {
      throw new IllegalStateException(
        s"UDS path requires $pathBytes UTF-8 bytes but this platform allows " +
          s"at most $maxPathBytes: $address")
    }
    address
  }

  /** Removes the socket file created by a worker. */
  def cleanupEndpointAddress(address: String): Unit = {
    Files.deleteIfExists(Paths.get(address))
  }

  /** Recursively removes all endpoint artifacts and the private directory. */
  override def close(): Unit = {
    if (!Files.exists(socketDir)) return
    var firstFailure: Option[IOException] = None
    def recordFailure(e: IOException): Unit = firstFailure match {
      case Some(first) => first.addSuppressed(e)
      case None => firstFailure = Some(e)
    }
    def delete(path: Path): Unit = {
      try Files.deleteIfExists(path) catch { case e: IOException => recordFailure(e) }
    }
    Files.walkFileTree(socketDir, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        delete(file)
        FileVisitResult.CONTINUE
      }
      override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = {
        recordFailure(exc)
        FileVisitResult.CONTINUE
      }
      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        if (exc != null) recordFailure(exc)
        delete(dir)
        FileVisitResult.CONTINUE
      }
    })
    firstFailure.foreach(e => throw e)
  }

  private def createPrivateTempDirectory(): Path = {
    val attr = PosixFilePermissions.asFileAttribute(
      PosixFilePermissions.fromString("rwx------"))
    val base = shortTempBase()
    try {
      Files.createTempDirectory(base, "spark-udf-worker", attr)
    } catch {
      case _: UnsupportedOperationException =>
        val dir = Files.createTempDirectory(base, "spark-udf-worker")
        val file = dir.toFile
        // Non-short-circuiting Boolean AND ensures every setter runs.
        val applied =
          file.setReadable(false, false) & file.setWritable(false, false) &
            file.setExecutable(false, false) & file.setReadable(true, true) &
            file.setWritable(true, true) & file.setExecutable(true, true)
        if (!applied) {
          logger.warn(
            s"Could not fully restrict permissions on $dir; socket " +
              s"directory may be accessible to other local users on this " +
              s"filesystem")
        }
        dir
    }
  }

  private def shortTempBase(): Path = {
    val candidates =
      Seq("TMPDIR", "TEMP", "TMP").flatMap(sys.env.get).filter(_.nonEmpty) :+ "/tmp"
    val usable = candidates
      .map(Paths.get(_))
      .filter(path => Files.isDirectory(path) && Files.isWritable(path))
    def encodedLength(path: Path): Int =
      path.toString.getBytes(StandardCharsets.UTF_8).length
    usable
      .sortBy(encodedLength)
      .headOption
      .getOrElse(Paths.get(System.getProperty("java.io.tmpdir")))
  }
}

private[worker] object UnixDomainSocketEndpointDirectory {
  private val MAX_SOCKET_LEAF_RETRIES = 16
  private val WORKER_ID_PREFIX_LENGTH = 16
  private val LINUX_PATH_BYTES = 107
  private val KQUEUE_PATH_BYTES = 103

  private[worker] def maxPathBytesForOs(osName: String): Int = {
    val normalized = osName.toLowerCase(Locale.ROOT)
    if (normalized.contains("mac") || normalized.contains("darwin") ||
        normalized.contains("bsd")) {
      KQUEUE_PATH_BYTES
    } else {
      LINUX_PATH_BYTES
    }
  }
}
