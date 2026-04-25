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

import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.util.control.NonFatal

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.core.{WorkerConnection, WorkerLogger}

/**
 * :: Experimental ::
 * A locally-spawned OS process running a UDF worker, together with its
 * transport connection. Wraps a [[WorkerArtifacts]] bundle (process +
 * connection + output log) plus a session ref-count scaffolding for
 * future pooling -- today one process per session.
 *
 * Closing sends SIGTERM, waits up to [[gracefulTimeoutMs]], then
 * delegates connection close + forced kill + file cleanup to
 * [[WorkerArtifacts.close]].
 *
 * @param id stable worker identifier (UUID passed to the binary as `--id`).
 * @param artifacts process + connection + output-log, disposed together.
 * @param gracefulTimeoutMs wait after SIGTERM before escalating to SIGKILL.
 * @param logger [[WorkerLogger]] for process-level messages.
 * @param onLastSessionReleased fires when the ref-count hits 0. Runs on
 *     the thread calling [[releaseSession]]. May fire more than once
 *     across a worker's lifetime; a concurrent `acquireSession` can
 *     re-increment the count before the callback returns, so pooling
 *     dispatchers must arbitrate reuse themselves.
 */
@Experimental
class DirectWorkerProcess(
    val id: String,
    private[direct] val artifacts: WorkerArtifacts,
    val gracefulTimeoutMs: Long,
    protected val logger: WorkerLogger = WorkerLogger.NoOp,
    private[direct] val onLastSessionReleased: DirectWorkerProcess => Unit = _ => ())
  extends AutoCloseable {

  // TODO: idle-timeout tracking and concurrent session capacity.

  private val activeSessionCount = new AtomicInteger(0)
  private val closed = new AtomicBoolean(false)

  /** The OS process handle for this worker. */
  def process: Process = artifacts.process

  /** The transport connection for this worker. */
  def connection: WorkerConnection = artifacts.connection

  /** Path to the merged stdout/stderr log for this worker. */
  def outputFile: Path = artifacts.outputFile

  /** Number of sessions currently using this worker. */
  def activeSessions: Int = activeSessionCount.get()

  /** Increments the active session count. */
  def acquireSession(): Unit = activeSessionCount.incrementAndGet()

  /**
   * Decrements the active session count. Fires [[onLastSessionReleased]]
   * on the 0-transition. A negative count indicates an unbalanced
   * acquire/release; we log and reset to 0 rather than silently mask it.
   */
  def releaseSession(): Unit = {
    val c = activeSessionCount.decrementAndGet()
    if (c < 0) {
      logger.warn(
        s"releaseSession called without a matching acquireSession (count=$c)")
      activeSessionCount.set(0)
    } else if (c == 0) {
      // Swallow callback errors so session.close cannot throw.
      try onLastSessionReleased(this) catch {
        case NonFatal(e) =>
          logger.warn(s"onLastSessionReleased callback failed for worker $id", e)
      }
    }
  }

  /** Returns true if the OS process is running and the connection is usable. */
  def isAlive: Boolean = process.isAlive && connection.isActive

  /**
   * Sends SIGTERM, waits up to [[gracefulTimeoutMs]] for the worker to
   * exit, then disposes artifacts (connection close + SIGKILL + file
   * cleanup). Idempotent via CAS.
   */
  override def close(): Unit = {
    if (!closed.compareAndSet(false, true)) return

    if (process.isAlive) {
      process.destroy() // SIGTERM
      try {
        // Ignore the return value: artifacts.close() SIGKILLs if still
        // alive and no-ops if already dead.
        process.waitFor(gracefulTimeoutMs, TimeUnit.MILLISECONDS)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
      }
    }

    artifacts.close()
  }
}

/**
 * Closeable bundle of per-worker OS resources: the child [[Process]], its
 * transport [[WorkerConnection]], and its merged stdout/stderr log.
 * [[close]] runs connection close (which for UDS removes the socket
 * file), then SIGKILL-reaps the process, then deletes the output log.
 * Graceful SIGTERM is the higher layer's responsibility (see
 * [[DirectWorkerProcess#close]]).
 */
private[direct] final class WorkerArtifacts(
    val process: Process,
    val connection: WorkerConnection,
    val outputFile: Path,
    private[this] val logger: WorkerLogger) extends AutoCloseable {

  private[this] val closed = new AtomicBoolean(false)

  /**
   * Idempotently closes the connection (transport teardown + any
   * transport-specific cleanup such as deleting a UDS socket file),
   * SIGKILL-reaps the process, and deletes the output log. Each step
   * is guarded so a failure in one does not skip the next.
   */
  override def close(): Unit = {
    if (!closed.compareAndSet(false, true)) return

    try connection.close() catch {
      case NonFatal(e) =>
        logger.warn("Error closing worker connection", e)
    }

    DirectWorkerDispatcher.destroyForciblyAndReap(process, logger, "worker artifacts")

    try Files.deleteIfExists(outputFile) catch {
      case NonFatal(e) =>
        logger.warn(s"Error cleaning up worker output file $outputFile", e)
    }
  }
}
