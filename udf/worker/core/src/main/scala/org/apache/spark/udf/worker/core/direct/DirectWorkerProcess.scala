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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.util.control.NonFatal

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.core.{WorkerConnection, WorkerLogger}

/**
 * :: Experimental ::
 * A locally-spawned OS process running a UDF worker, together with the
 * transport connection to it.
 *
 * A [[DirectWorkerProcess]] combines three things:
 *  - An OS '''process''' (the worker binary, started by the dispatcher).
 *  - A '''[[WorkerConnection]]''' (the transport channel to that process).
 *  - A '''socket path''' (a UDS socket file) that both sides use.
 *
 * Multiple [[DirectWorkerSession]]s may share the same process when the
 * worker supports concurrent UDFs. The [[acquireSession]]/[[releaseSession]]
 * ref-count tracks how many sessions are active.
 *
 * Closing tears down everything: closes the connection, sends SIGTERM
 * (then SIGKILL), and removes the socket file and the process output log.
 *
 * @param process           the OS process handle
 * @param connection        the transport connection to this worker
 * @param socketPath        the UDS socket path used by this worker
 * @param outputFile        the merged stdout/stderr log for this worker.
 *                          Kept open for the lifetime of the worker (so it
 *                          remains a valid target for the child's file
 *                          descriptor and so its contents can be inspected
 *                          while the worker runs) and deleted in [[close]].
 * @param gracefulTimeoutMs milliseconds to wait after SIGTERM before
 *                          escalating to SIGKILL.
 * @param logger            [[WorkerLogger]] used for process-level
 *                          messages. Defaults to [[WorkerLogger.NoOp]];
 *                          the dispatcher normally passes its own
 *                          logger so all messages share a category.
 */
@Experimental
class DirectWorkerProcess(
    val process: Process,
    val connection: WorkerConnection,
    val socketPath: String,
    val outputFile: Path,
    val gracefulTimeoutMs: Long,
    protected val logger: WorkerLogger = WorkerLogger.NoOp)
  extends AutoCloseable {

  // The active-session ref-count below is scaffolding for future connection
  // pooling. With pooling, the dispatcher would keep an idle worker alive
  // when its ref-count drops to 0 and hand it out to the next session.
  // TODO: Idle timeout tracking and concurrent session capacity.
  //
  // Until pooling lands, the dispatcher spawns one worker per session and
  // tears it down at dispatcher close; the ref-count is informational only.

  private val activeSessionCount = new AtomicInteger(0)
  private val closed = new AtomicBoolean(false)

  /** Number of sessions currently using this worker. */
  def activeSessions: Int = activeSessionCount.get()

  /** Increments the active session count. */
  def acquireSession(): Unit = activeSessionCount.incrementAndGet()

  /**
   * Decrements the active session count. Logs a warning and resets to zero
   * if the count goes negative, which would indicate an unbalanced
   * acquire/release (a bug we want to surface rather than paper over,
   * especially once pooling consumes this count).
   */
  def releaseSession(): Unit = {
    val c = activeSessionCount.decrementAndGet()
    if (c < 0) {
      logger.warn(
        s"releaseSession called without a matching acquireSession (count=$c)")
      activeSessionCount.set(0)
    }
  }

  /** Returns true if the OS process is running and the connection is usable. */
  def isAlive: Boolean = process.isAlive && connection.isActive

  /**
   * Shuts down the connection, then terminates the OS process.
   * Sends SIGTERM first; escalates to SIGKILL after [[gracefulTimeoutMs]].
   * Idempotent: only the first call performs teardown; subsequent calls
   * are no-ops so the dispatcher's close path and the createSession error
   * path can both invoke close without double-releasing resources.
   */
  override def close(): Unit = {
    if (!closed.compareAndSet(false, true)) return

    try {
      connection.close()
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Error closing connection to worker at $socketPath", e)
    }

    if (process.isAlive) {
      process.destroy() // SIGTERM
      try {
        if (!process.waitFor(gracefulTimeoutMs, TimeUnit.MILLISECONDS)) {
          process.destroyForcibly() // SIGKILL
        }
      } catch {
        case _: InterruptedException =>
          process.destroyForcibly()
          Thread.currentThread().interrupt()
      }
    }

    try {
      val f = new File(socketPath)
      if (f.exists()) f.delete()
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Error cleaning up socket file $socketPath", e)
    }

    try {
      Files.deleteIfExists(outputFile)
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Error cleaning up worker output file $outputFile", e)
    }
  }
}
