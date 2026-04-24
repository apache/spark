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

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermissions
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.{Queue => MQueue}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.{ProcessCallable, UDFWorkerSpecification}
import org.apache.spark.udf.worker.core.{WorkerConnection, WorkerDispatcher,
  WorkerLogger, WorkerSecurityScope, WorkerSession}
import org.apache.spark.udf.worker.core.direct.DirectWorkerDispatcher.{CallableResult,
  DEFAULT_CALLABLE_TIMEOUT_MS, DEFAULT_GRACEFUL_TIMEOUT_MS, DEFAULT_INIT_TIMEOUT_MS,
  EnvironmentState, MAX_OUTPUT_SCAN_BYTES, PROCESS_OUTPUT_TAIL_LINES,
  SOCKET_POLL_INTERVAL_MS}

/**
 * :: Experimental ::
 * A [[WorkerDispatcher]] that creates workers by spawning local OS processes
 * ("direct" creation mode from the worker specification).
 *
 * On the first [[createSession]], the dispatcher ensures the environment is
 * ready (verify / install) and registers the cleanup hook. Each session
 * currently gets a fresh worker that is terminated when the session closes
 * (the single-reference case of the future pooling policy).
 *
 * Subclasses implement [[createConnection]] and [[createSessionForWorker]]
 * to provide protocol-specific behavior (e.g., gRPC, raw sockets).
 *
 * For workers obtained through a provisioning service or daemon (indirect
 * creation), see the `indirect` package (TODO).
 *
 * @param workerSpec worker specification (proto)
 * @param logger [[WorkerLogger]] used for dispatcher-internal messages.
 *               The framework does not depend on any concrete logging
 *               backend; callers should pass an adapter that forwards
 *               to their preferred logger (Spark's `Logging` trait,
 *               SLF4J, etc.). Defaults to [[WorkerLogger.NoOp]].
 */
@Experimental
abstract class DirectWorkerDispatcher(
    override val workerSpec: UDFWorkerSpecification,
    protected val logger: WorkerLogger = WorkerLogger.NoOp)
  extends WorkerDispatcher {

  // TODO: Connection pooling -- reuse idle workers across sessions.
  // TODO: Security scope isolation -- partition pool by WorkerSecurityScope.

  validateTransportSupport()
  validateEnvironmentCallables()

  /**
   * Maximum time to wait for a setup/verify/cleanup callable to finish.
   * Subclasses may override this to accommodate slow installation steps
   * (e.g., a large dependency install). Defaults to 120 seconds.
   */
  protected def callableTimeoutMs: Long = DEFAULT_CALLABLE_TIMEOUT_MS

  private val initTimeoutMs: Long = {
    val props = workerSpec.getDirect.getProperties
    if (props.hasInitializationTimeoutMs && props.getInitializationTimeoutMs > 0) {
      props.getInitializationTimeoutMs.toLong
    } else {
      DEFAULT_INIT_TIMEOUT_MS
    }
  }

  private val gracefulTimeoutMs: Long = {
    val props = workerSpec.getDirect.getProperties
    if (props.hasGracefulTerminationTimeoutMs && props.getGracefulTerminationTimeoutMs > 0) {
      props.getGracefulTerminationTimeoutMs.toLong
    } else {
      DEFAULT_GRACEFUL_TIMEOUT_MS
    }
  }

  // The socket directory is removed explicitly in close(). deleteOnExit is
  // deliberately not registered: it is redundant with the explicit cleanup,
  // it leaks memory in long-lived JVMs (the JDK retains the path string for
  // the process lifetime), and it only works on empty directories.
  //
  // Created with POSIX 0700 so UDS sockets inside are not reachable by other
  // local users. On non-POSIX filesystems the JDK rejects the attribute with
  // UnsupportedOperationException; fall back to owner-only permissions via
  // the File API, which is best-effort but still narrows the mode.
  private val socketDir: Path = createPrivateTempDirectory()
  // Keyed by worker id so release is O(1) and lock-free. Iterating for
  // shutdown is weakly consistent, which is fine: any createSession racing
  // with close() is caught by the `closed` flag checks in createSession, and
  // any in-flight releaseWorker that overlaps with close() is idempotent on
  // both sides (remove-missing is a no-op, DirectWorkerProcess.close() is
  // CAS-guarded).
  private[this] val workers = new ConcurrentHashMap[String, DirectWorkerProcess]()
  // Flips to true on close(); createSession rejects afterwards and any
  // already-spawned worker caught between `workers.put` and the post-publish
  // check tears itself down instead of leaking.
  private[this] val closed = new AtomicBoolean(false)

  @volatile private var environmentState: EnvironmentState = EnvironmentState.Pending
  private val environmentLock = new Object
  private[this] var cleanupHook: Option[Thread] = None

  /** Creates a protocol-specific connection to a worker at the given socket path. */
  protected def createConnection(socketPath: String): WorkerConnection

  /** Creates a protocol-specific session for the given worker. */
  protected def createSessionForWorker(worker: DirectWorkerProcess): WorkerSession

  override def createSession(
      securityScope: Option[WorkerSecurityScope]): WorkerSession = {
    // Pooling keyed by security scope is not yet implemented. Accepting a
    // non-None scope here would silently create a one-off worker and give
    // the caller a false expectation of isolation, so reject it until the
    // dispatcher actually honors the scope.
    require(securityScope.isEmpty,
      "securityScope is not supported yet; pass None until pooling lands")
    if (closed.get()) throwClosed()
    ensureEnvironmentReady()
    val worker = spawnWorker()
    // Acquire the session ref-count BEFORE publishing to `workers`: otherwise
    // close() could iterate `workers`, tear this worker down, and leave the
    // subsequent acquireSession handing the caller a dead worker.
    worker.acquireSession()
    workers.put(worker.id, worker)
    // Re-check after publishing. close() may have iterated `workers` before
    // our put (orphan case) or after (already closed here). Either way,
    // releasing our ref-count fires the callback that removes and closes.
    if (closed.get()) {
      worker.releaseSession()
      throwClosed()
    }
    try {
      createSessionForWorker(worker)
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        // Drop the ref-count to 0, firing `releaseWorker` via the worker's
        // callback to remove and tear down the worker.
        worker.releaseSession()
        throw e
      case NonFatal(e) =>
        worker.releaseSession()
        throw e
    }
  }

  /**
   * Called from [[DirectWorkerProcess.releaseSession]] when the last
   * active session on `worker` closes. Today this always terminates the
   * worker; with pooling, this is where the decision to reuse vs. evict
   * will live (idle-pool handoff, capacity limits, health checks).
   *
   * Safe to invoke after dispatcher [[close]] has already reaped this
   * worker: the worker's own idempotent close guard turns the second
   * teardown into a no-op.
   */
  private def releaseWorker(worker: DirectWorkerProcess): Unit = {
    workers.remove(worker.id)
    try {
      worker.close()
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Error closing worker ${worker.id}", e)
    }
  }

  private def throwClosed(): Nothing =
    throw new IllegalStateException("Dispatcher is closed")

  /**
   * Shuts down the dispatcher: terminates tracked workers, removes the
   * socket directory, and runs environment cleanup. Idempotent via CAS --
   * only the first caller performs teardown; subsequent calls are no-ops.
   *
   * Does not block in-flight `createSession` calls. A createSession caller
   * racing past the post-publish `closed` check will still produce a
   * spawned worker; that worker tears itself down via the ref-count
   * callback, but the subprocess teardown may outlive this `close()`.
   * Callers needing a fully quiescent state must externally synchronize
   * with their `createSession` callers.
   */
  override def close(): Unit = {
    // Flip `closed` first so that any concurrent createSession either bails
    // on its fast-path check or cleans up its own worker via the post-publish
    // check. Only the first caller performs the teardown; subsequent calls
    // are no-ops.
    if (!closed.compareAndSet(false, true)) {
      return
    }
    // TODO: Close workers in parallel. Worst-case shutdown today is
    //   N * gracefulTimeoutMs because each worker waits for SIGTERM to
    //   complete before the next one is signalled. A small pool of
    //   short-lived threads would bound shutdown to ~gracefulTimeoutMs.
    workers.values().iterator().asScala.foreach { w =>
      try {
        w.close()
      } catch {
        case NonFatal(e) =>
          logger.warn(s"Error closing worker ${w.id}", e)
      }
    }
    workers.clear()
    try {
      val dir = socketDir.toFile
      if (dir.exists()) {
        val remaining = dir.listFiles()
        if (remaining != null) remaining.foreach(_.delete())
        dir.delete()
      }
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Error cleaning up socket directory $socketDir", e)
    }
    deregisterEnvironmentCleanupHook()
    runEnvironmentCleanup()
  }

  // -- Environment lifecycle -------------------------------------------------

  // TODO: Handle permanently unrecoverable environment failures (e.g., wrong
  //   CPU architecture, unavailable system resources) differently from transient
  //   ones. Currently all failures are treated as permanent, but some callers
  //   may want to distinguish retriable vs. fatal failures.
  private def ensureEnvironmentReady(): Unit = {
    environmentLock.synchronized {
      environmentState match {
        case EnvironmentState.Ready | EnvironmentState.CleanedUp =>
          // Already set up (or torn down); nothing to do.
        case EnvironmentState.Failed(msg) =>
          throw new DirectWorkerException(s"Environment setup previously failed: $msg")
        case EnvironmentState.Pending =>
          val env = workerSpec.getEnvironment
          // Register the cleanup hook up front. Cleanup is user-defined
          // and may tear down more than install artifacts (worker state,
          // temp files, etc.), so we honor it whenever it is configured,
          // independent of whether verify/install run. Registering before
          // any callable runs also ensures a partially-successful install
          // (e.g., half-copied files) still gets cleaned up at JVM
          // shutdown if the caller never reaches dispatcher.close(). The
          // hook is a no-op when environment_cleanup is not configured.
          registerEnvironmentCleanupHook()
          val verified = env.hasEnvironmentVerification &&
            runCallable(env.getEnvironmentVerification).exitCode == 0
          if (!verified && env.hasInstallation) {
            val result = runCallable(env.getInstallation)
            if (result.exitCode != 0) {
              val detail = s"exit code ${result.exitCode}\n${result.outputTail}"
              environmentState = EnvironmentState.Failed(detail)
              throw new DirectWorkerException(
                s"Environment installation failed with $detail")
            }
          }
          environmentState = EnvironmentState.Ready
      }
    }
  }

  // TODO: Share a single JVM shutdown hook across all dispatchers in the
  //   process. Today each dispatcher (when environment_cleanup is set)
  //   registers its own Thread, which the JVM retains until shutdown.
  //   In a long-running driver that creates many dispatchers without
  //   explicit close() (e.g., failed-task paths), this keeps per-dispatcher
  //   memory live for the process lifetime. A shared cleanup coordinator
  //   draining a ConcurrentLinkedQueue<Runnable> would collapse the N hooks
  //   into one.

  /**
   * Registers the JVM shutdown hook that runs the cleanup callable.
   *
   * '''Caller must hold `environmentLock`''' -- this method reads and
   * writes `cleanupHook` without its own synchronization. It is only
   * called from `ensureEnvironmentReady`, which already owns the lock.
   */
  private def registerEnvironmentCleanupHook(): Unit = {
    assert(Thread.holdsLock(environmentLock),
      "registerEnvironmentCleanupHook must be called while holding environmentLock")
    if (cleanupHook.isDefined) return
    if (workerSpec.getEnvironment.hasEnvironmentCleanup) {
      val hook = new Thread(() => runEnvironmentCleanup(), "udf-env-cleanup")
      cleanupHook = Some(hook)
      // scalastyle:off runtimeaddshutdownhook
      Runtime.getRuntime.addShutdownHook(hook)
      // scalastyle:on runtimeaddshutdownhook
    }
  }

  private def deregisterEnvironmentCleanupHook(): Unit = {
    environmentLock.synchronized {
      cleanupHook.foreach { hook =>
        try {
          Runtime.getRuntime.removeShutdownHook(hook)
        } catch {
          case _: IllegalStateException => // JVM already shutting down
        }
        cleanupHook = None
      }
    }
  }

  private def runEnvironmentCleanup(): Unit = {
    environmentLock.synchronized {
      environmentState match {
        case EnvironmentState.CleanedUp =>
          // Already cleaned up; nothing to do.
        case _ =>
          if (workerSpec.getEnvironment.hasEnvironmentCleanup) {
            try {
              val result = runCallable(workerSpec.getEnvironment.getEnvironmentCleanup)
              if (result.exitCode != 0) {
                logger.warn(s"Environment cleanup exited with code ${result.exitCode}" +
                  s"\n${result.outputTail}")
              }
            } catch {
              case NonFatal(e) => logger.warn("Environment cleanup failed", e)
            }
          }
          environmentState = EnvironmentState.CleanedUp
      }
    }
  }

  // -- Process helpers -------------------------------------------------------

  /**
   * Runs a [[ProcessCallable]] synchronously and returns the result.
   * Always throws on timeout; callers check `exitCode` for non-timeout failures.
   */
  private[core] def runCallable(callable: ProcessCallable): CallableResult = {
    val cmd = (callable.getCommandList.asScala ++ callable.getArgumentsList.asScala).toSeq
    require(cmd.nonEmpty,
      "ProcessCallable must have at least one entry in command or arguments")
    val outputFile = Files.createTempFile("udf-callable-", ".log")
    try {
      val process = launchProcess(
        cmd, callable.getEnvironmentVariablesMap.asScala.toMap, outputFile.toFile)
      val timeoutMs = callableTimeoutMs
      if (!process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
        DirectWorkerDispatcher.destroyForciblyAndReap(
          process, logger, s"callable timeout: ${cmd.head}")
        val tail = readOutputTail(outputFile.toFile)
        throw new DirectWorkerException(
          s"Callable timed out after ${timeoutMs}ms: " +
            s"${cmd.mkString(" ")}\n$tail")
      }
      val tail = readOutputTail(outputFile.toFile)
      CallableResult(process.exitValue(), tail)
    } finally {
      Files.deleteIfExists(outputFile)
    }
  }

  private def spawnWorker(): DirectWorkerProcess = {
    val runner = workerSpec.getDirect.getRunner
    val baseCmd = (runner.getCommandList.asScala ++ runner.getArgumentsList.asScala).toSeq
    require(baseCmd.nonEmpty,
      "DirectWorker.runner must have at least one entry in command or arguments")
    val workerId = UUID.randomUUID().toString
    val socketPath = socketDir.resolve(s"worker-$workerId.sock").toString
    // Per the ProcessCallable contract in worker_spec.proto, the engine must
    // always pass --id (worker identifier for logs) and --connection (the
    // engine-assigned endpoint, format depending on transport).
    val cmd = baseCmd ++ Seq("--id", workerId, "--connection", socketPath)
    val env = runner.getEnvironmentVariablesMap.asScala.toMap
    val outputFile = Files.createTempFile("udf-worker-", ".log")
    val process = launchProcess(cmd, env, outputFile.toFile)

    try {
      waitForSocket(socketPath, process, outputFile.toFile)
      val connection = createConnection(socketPath)
      // Ownership of `outputFile` transfers to the DirectWorkerProcess: it
      // remains valid for the child's file descriptor and is deleted in
      // DirectWorkerProcess.close().
      new DirectWorkerProcess(
        workerId, process, connection, socketPath, outputFile,
        gracefulTimeoutMs, logger, onLastSessionReleased = releaseWorker)
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        cleanupFailedSpawn(process, socketPath, outputFile)
        throw e
      case NonFatal(e) =>
        cleanupFailedSpawn(process, socketPath, outputFile)
        throw e
    }
  }

  private def cleanupFailedSpawn(
      process: Process,
      socketPath: String,
      outputFile: Path): Unit = {
    DirectWorkerDispatcher.destroyForciblyAndReap(process, logger, "failed spawn")
    // If the worker (or createConnection) had already created the socket
    // file, remove it so it doesn't linger until dispatcher.close().
    try Files.deleteIfExists(new File(socketPath).toPath) catch {
      case NonFatal(cleanupEx) =>
        logger.debug(s"Failed to clean up socket file $socketPath", cleanupEx)
    }
    // Swallow IOException here so we don't replace the original spawn
    // failure with a cleanup failure.
    try Files.deleteIfExists(outputFile) catch {
      case NonFatal(cleanupEx) =>
        logger.debug(s"Failed to clean up worker output file $outputFile", cleanupEx)
    }
  }

  /**
   * Creates a temp directory with owner-only permissions (0700 on POSIX).
   * Falls back to a best-effort `File.setXxx` on non-POSIX filesystems
   * that cannot honor the attribute. The fallback is racy and weaker
   * than the POSIX path; the logger call surfaces when the platform
   * refuses the `setXxx` calls so operators see the degraded mode.
   */
  private def createPrivateTempDirectory(): Path = {
    val attr = PosixFilePermissions.asFileAttribute(
      PosixFilePermissions.fromString("rwx------"))
    try {
      Files.createTempDirectory("spark-udf-worker", attr)
    } catch {
      case _: UnsupportedOperationException =>
        // Non-POSIX filesystem. The dir exists with default perms between
        // `createTempDirectory` and the `setXxx` calls below, so this
        // fallback is TOCTOU-racy by nature.
        val dir = Files.createTempDirectory("spark-udf-worker")
        val f = dir.toFile
        // Strip group/other access, then restore owner rwx. `&` (non-short-
        // circuiting) so every call is attempted before we decide whether
        // to warn; any `false` return means the platform silently refused
        // the change and the directory is less private than advertised.
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
   * Starts an OS process. stdout and stderr are merged and redirected to the
   * given file so that output can be read back for error reporting.
   */
  private def launchProcess(
      command: Seq[String],
      env: Map[String, String],
      outputFile: File): Process = {
    val builder = new ProcessBuilder(command: _*)
    env.foreach { case (k, v) => builder.environment().put(k, v) }
    builder.redirectErrorStream(true)
    builder.redirectOutput(outputFile)
    builder.start()
  }

  private def waitForSocket(
      socketPath: String,
      process: Process,
      outputFile: File): Unit = {
    val file = new File(socketPath)
    // Ensure at least one poll attempt even for very small init timeouts,
    // so we don't declare a premature timeout before the worker has any
    // chance to create the socket.
    val maxAttempts = math.max(1, (initTimeoutMs / SOCKET_POLL_INTERVAL_MS).toInt)
    var attempts = 0
    while (!file.exists() && attempts < maxAttempts) {
      if (!process.isAlive) throwWorkerExitedBeforeSocket(process, socketPath, outputFile)
      Thread.sleep(SOCKET_POLL_INTERVAL_MS)
      attempts += 1
    }
    if (!file.exists()) {
      if (process.isAlive) {
        DirectWorkerDispatcher.destroyForciblyAndReap(
          process, logger, s"init timeout $socketPath")
        val tail = readOutputTail(outputFile)
        throw new DirectWorkerException(
          s"Worker did not create socket at $socketPath within ${initTimeoutMs}ms\n$tail")
      } else {
        // The worker exited between the last file.exists() poll and here
        // without creating the socket -- report the exit code rather than
        // an ambiguous "did not create" message.
        throwWorkerExitedBeforeSocket(process, socketPath, outputFile)
      }
    }
  }

  private def throwWorkerExitedBeforeSocket(
      process: Process,
      socketPath: String,
      outputFile: File): Nothing = {
    val tail = readOutputTail(outputFile)
    throw new DirectWorkerException(
      s"Worker exited with code ${process.exitValue()} " +
        s"before creating socket at $socketPath\n$tail")
  }

  // Reads at most the final MAX_OUTPUT_SCAN_BYTES of `file` and returns the
  // last PROCESS_OUTPUT_TAIL_LINES lines via a fixed-size ring buffer, so a
  // runaway worker that writes gigabytes of output does not OOM the caller
  // during error reporting.
  private def readOutputTail(file: File): String = {
    if (!file.exists() || file.length() == 0) return ""
    val fileLen = file.length()
    val startPos = math.max(0L, fileLen - MAX_OUTPUT_SCAN_BYTES)
    val fis = new FileInputStream(file)
    try {
      // FileChannel.position is O(1) and unambiguously seeks, unlike
      // FileInputStream.skip which is allowed to return 0 even when bytes
      // remain.
      if (startPos > 0) fis.getChannel.position(startPos)
      val reader = new BufferedReader(
        new InputStreamReader(fis, StandardCharsets.UTF_8))
      // If we started mid-line, the first line is partial -- discard it so
      // the tail never shows a line fragment.
      if (startPos > 0) reader.readLine()
      val buffer = new MQueue[String]()
      var line = reader.readLine()
      while (line != null) {
        if (buffer.size >= PROCESS_OUTPUT_TAIL_LINES) buffer.dequeue()
        buffer.enqueue(line)
        line = reader.readLine()
      }
      if (buffer.isEmpty) ""
      else "Process output (last lines):\n" + buffer.mkString("\n")
    } catch {
      case NonFatal(e) =>
        logger.debug(s"Failed to read process output from $file", e)
        ""
    } finally {
      fis.close()
    }
  }

  // -- Spec validation -------------------------------------------------------

  // Multi-connection workers (e.g., a separate control channel) are a future
  // extension; today the proto field is `repeated` but the engine requires
  // exactly one. TCP transport is declared in the proto but not yet
  // implemented; the engine currently only supports UDS.
  private def validateTransportSupport(): Unit = {
    val props = workerSpec.getDirect.getProperties
    val n = props.getConnectionsCount
    require(n == 1,
      s"DirectWorker.properties.connections must have exactly one entry, got $n")
    val conn = props.getConnections(0)
    require(conn.hasUnixDomainSocket,
      "DirectWorker currently only supports UNIX domain socket transport, " +
        s"got ${conn.getTransportCase}")
  }

  // worker_spec.proto documents that verification is only meaningful together
  // with installation -- verification exists so the engine can skip running
  // installation when the environment is already prepared. A verification
  // callable with no installation callable would either always succeed (no-op)
  // or always fail (worker spawn then fails) -- both user errors worth
  // catching at spec-validation time.
  private def validateEnvironmentCallables(): Unit = {
    val env = workerSpec.getEnvironment
    require(!env.hasEnvironmentVerification || env.hasInstallation,
      "WorkerEnvironment.environment_verification requires installation to be set")
  }
}

private[direct] object DirectWorkerDispatcher {
  private[direct] val SOCKET_POLL_INTERVAL_MS = 100L
  private[direct] val DEFAULT_INIT_TIMEOUT_MS = 10000L
  private[direct] val DEFAULT_CALLABLE_TIMEOUT_MS = 120000L
  private[direct] val DEFAULT_GRACEFUL_TIMEOUT_MS = 5000L
  private[direct] val PROCESS_OUTPUT_TAIL_LINES = 50
  // Cap the amount of log file scanned by readOutputTail so a runaway worker
  // producing gigabytes of output cannot OOM the caller during error
  // reporting. The tail is still limited to PROCESS_OUTPUT_TAIL_LINES.
  private[direct] val MAX_OUTPUT_SCAN_BYTES = 1024L * 1024L // 1 MiB
  // Bound on how long we wait for the kernel to reap a SIGKILL'd child.
  // Distinct from the user-configurable gracefulTimeoutMs: SIGKILL is
  // unblockable, so the only delay is kernel latency (milliseconds in the
  // common case). Five seconds is generous; if we exceed it the child is
  // likely stuck in uninterruptible I/O (D-state) and further waiting
  // won't help.
  private[direct] val SIGKILL_REAP_TIMEOUT_MS = 5000L

  /**
   * Sends SIGKILL to `process` and waits up to [[SIGKILL_REAP_TIMEOUT_MS]]
   * for the kernel to reap it.
   *
   * `destroyForcibly()` returns before the child has been reaped from the
   * process table. Without a bounded `waitFor` the child lingers as a
   * zombie until the JVM itself exits; in long-lived drivers with repeated
   * failed spawns or session teardowns, the zombies accumulate. Callers
   * should use this helper instead of calling `destroyForcibly()` directly.
   *
   * Behaviour:
   *  - No-op if the process is already dead.
   *  - If the reap times out, logs a warning and returns -- we do not
   *    block forever on a wedged child. The process remains a zombie;
   *    this is a real (but rare) operational condition the warning
   *    surfaces.
   *  - If the current thread is interrupted while waiting, re-raises
   *    the interrupt and returns without logging a zombie warning --
   *    we have not actually waited the full reap window.
   *
   * @param context short human-readable tag included in the warn log so
   *                operators can correlate a wedged child with its source
   *                (e.g. worker id, or the callable that was killed).
   */
  private[direct] def destroyForciblyAndReap(
      process: Process,
      logger: WorkerLogger,
      context: String = ""): Unit = {
    if (!process.isAlive) return
    process.destroyForcibly()
    val reaped = try {
      process.waitFor(SIGKILL_REAP_TIMEOUT_MS, TimeUnit.MILLISECONDS)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        return
    }
    if (!reaped && process.isAlive) {
      val suffix = if (context.nonEmpty) s" [$context]" else ""
      logger.warn(
        s"Process ${process.pid()}$suffix still alive ${SIGKILL_REAP_TIMEOUT_MS}ms " +
          s"after SIGKILL; leaving behind as zombie " +
          s"(likely stuck in uninterruptible kernel state)")
    }
  }

  /** Result of running a [[ProcessCallable]]. */
  private[core] case class CallableResult(exitCode: Int, outputTail: String)

  private[direct] sealed trait EnvironmentState
  private[direct] object EnvironmentState {
    case object Pending extends EnvironmentState
    case object Ready extends EnvironmentState
    case class Failed(detail: String) extends EnvironmentState
    case object CleanedUp extends EnvironmentState
  }
}
