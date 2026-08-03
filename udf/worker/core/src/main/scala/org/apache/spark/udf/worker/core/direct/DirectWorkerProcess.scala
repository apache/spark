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
import java.util.concurrent.{CopyOnWriteArrayList, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.util.control.NonFatal

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.core.{WorkerConnection, WorkerHandle, WorkerLogger}

/**
 * :: Experimental ::
 * A locally-spawned OS process running a UDF worker, together with its
 * transport connection. Wraps a [[WorkerArtifacts]] bundle (process +
 * connection + output log) plus a session ref-count scaffolding for
 * future pooling -- today one process per session.
 *
 * Closing sends SIGTERM, waits up to [[gracefulTimeoutMs]], then
 * delegates connection close + forced kill + socket/log cleanup to
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
    override val id: String,
    private[direct] val artifacts: WorkerArtifacts,
    val gracefulTimeoutMs: Long,
    protected val logger: WorkerLogger = WorkerLogger.NoOp,
    private[direct] val onLastSessionReleased: DirectWorkerProcess => Unit = _ => ())
  extends AutoCloseable with WorkerHandle {

  // TODO: idle-timeout tracking and concurrent session capacity.

  private val activeSessionCount = new AtomicInteger(0)
  private val closed = new AtomicBoolean(false)

  // Reuse-readiness flag. Sessions set this to `true` when they observe the
  // worker in a state that is unsafe to recycle (transport error, hung
  // worker, observed protocol violation). Pool-aware dispatchers consult
  // [[isInvalid]] in [[onLastSessionReleased]] to decide between
  // terminate-and-discard vs. return-to-pool. Today the non-pooling path
  // always terminates, so this is forward-looking; setting it now is
  // harmless and locks in the reuse contract.
  private val invalid = new AtomicBoolean(false)

  /** The OS process handle for this worker. */
  def process: Process = artifacts.process

  /** The transport connection for this worker (see [[WorkerHandle.connection]]). */
  override def connection: WorkerConnection = artifacts.connection

  /** Path to the merged stdout/stderr log for this worker. */
  def outputFile: Path = artifacts.outputFile

  /** Number of sessions currently using this worker. */
  def activeSessions: Int = activeSessionCount.get()

  /**
   * Increments the active session count.
   *
   * Preconditions: must not be called after [[close]]. The dispatcher
   * arbitrates acquire-vs-close ordering; calling this on a closed worker
   * indicates a dispatcher-side bug.
   *
   * @throws IllegalStateException if the worker has already been closed.
   */
  def acquireSession(): Unit = {
    if (closed.get()) {
      throw new IllegalStateException(
        s"cannot acquire session: worker $id is already closed")
    }
    activeSessionCount.incrementAndGet()
  }

  /**
   * Returns true if a session has marked this worker as unsafe to reuse.
   * Pool implementations MUST treat [[isInvalid]] as a hard rejection in
   * [[onLastSessionReleased]] -- the worker must be torn down, not
   * recycled. Once set, the flag is sticky for the worker's lifetime.
   */
  def isInvalid: Boolean = invalid.get()

  /**
   * Marks this worker as unsafe to return to a reuse pool. Idempotent;
   * sticky once set. Sessions call this when they observe a transport
   * failure, hung worker, or any termination path that leaves the worker
   * in an unknown state.
   */
  override def markInvalid(): Unit = invalid.set(true)

  /**
   * Decrements the active session count. Fires [[onLastSessionReleased]]
   * on the 0-transition.
   *
   * A negative count means this was called without a matching
   * [[acquireSession]] -- an unbalanced acquire/release, which is a
   * dispatcher-side bug -- and throws `IllegalStateException`.
   *
   * @throws IllegalStateException if the active session count goes negative.
   */
  override def releaseSession(): Unit = {
    val c = activeSessionCount.decrementAndGet()
    if (c < 0) {
      // A negative count means releaseSession was called without a matching
      // acquireSession -- an unbalanced acquire/release, which is a
      // dispatcher-side bug. Fail fast: a corrupt count could later let a
      // release prematurely fire onLastSessionReleased and tear the worker
      // down under a live session.
      throw new IllegalStateException(
        s"releaseSession called without a matching acquireSession (count=$c)")
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
 * [[close]] tears down the connection, SIGKILL-reaps the process, runs any
 * cleanup hooks registered via [[registerCleanup]] (e.g. removing the worker's
 * UDS socket file), then deletes the output log. Graceful SIGTERM is the higher
 * layer's responsibility (see [[DirectWorkerProcess#close]]).
 *
 * Transport-specific teardown is attached via [[registerCleanup]] rather than
 * baked into this bundle, so `WorkerArtifacts` stays transport-agnostic: a
 * dispatcher registers whatever per-worker resources it created without this
 * class knowing what they are.
 */
private[direct] final class WorkerArtifacts(
    val process: Process,
    val connection: WorkerConnection,
    val outputFile: Path,
    private[this] val logger: WorkerLogger) extends AutoCloseable {

  private[this] val closed = new AtomicBoolean(false)

  // Resource-cleanup callbacks run during close(), in registration order, after
  // the process is reaped. A CopyOnWriteArrayList gives thread-safe registration
  // and lock-free iteration at close without an explicit lock; in practice all
  // hooks are registered at spawn time, before the bundle is exposed to any
  // close path.
  private[this] val cleanupHooks = new CopyOnWriteArrayList[() => Unit]()

  /**
   * Registers a resource-cleanup callback to run during [[close]], in
   * registration order, after the process is reaped. Lets a dispatcher attach
   * the per-worker resources it created -- e.g. `() => cleanupEndpointAddress(address)`
   * to delete the UDS socket file -- without `WorkerArtifacts` knowing what they
   * are. Each hook is invoked at most once and guarded independently during
   * close, so one failing hook does not skip the rest.
   *
   * Must be called before [[close]]; registering on an already-closed bundle is
   * a dispatcher-side bug and throws `IllegalStateException`.
   */
  def registerCleanup(hook: () => Unit): Unit = {
    if (closed.get()) {
      throw new IllegalStateException(
        "cannot register a cleanup hook on an already-closed WorkerArtifacts")
    }
    cleanupHooks.add(hook)
  }

  /**
   * Idempotently closes the connection (transport teardown),
   * SIGKILL-reaps the process, runs the dispatcher cleanup hooks, and deletes
   * the output log. Each step (and each hook) is guarded so a failure in one
   * does not skip the next.
   */
  override def close(): Unit = {
    if (!closed.compareAndSet(false, true)) return

    try connection.close() catch {
      case NonFatal(e) =>
        logger.warn("Error closing worker connection", e)
    }

    DirectWorkerDispatcher.destroyForciblyAndReap(process, logger, "worker artifacts")

    // `closed` is already set, so registerCleanup rejects any further hook;
    // every hook registered before close is iterated here (CopyOnWriteArrayList
    // iteration needs no lock and no snapshot copy).
    cleanupHooks.forEach { hook =>
      try hook() catch {
        case NonFatal(e) =>
          logger.warn("Error running worker resource-cleanup hook", e)
      }
    }

    try Files.deleteIfExists(outputFile) catch {
      case NonFatal(e) =>
        logger.warn(s"Error cleaning up worker output file $outputFile", e)
    }
  }
}
