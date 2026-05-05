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
  ENGINE_MAX_TIMEOUT_MS, EnvironmentState, MAX_OUTPUT_SCAN_BYTES,
  PROCESS_OUTPUT_TAIL_LINES}

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

  // Proto-provided timeouts are clamped to ENGINE_MAX_TIMEOUT_MS. The
  // dispatcher-internal callableTimeoutMs above is subclass-controlled and
  // not subject to the cap.
  // Package-private for test access.
  private[core] val initTimeoutMs: Long = {
    val props = workerSpec.getDirect.getProperties
    val raw = if (props.hasInitializationTimeoutMs && props.getInitializationTimeoutMs > 0) {
      props.getInitializationTimeoutMs.toLong
    } else {
      DEFAULT_INIT_TIMEOUT_MS
    }
    clampTimeout("initialization_timeout_ms", raw)
  }

  private val gracefulTimeoutMs: Long = {
    val props = workerSpec.getDirect.getProperties
    val raw = if (props.hasGracefulTerminationTimeoutMs &&
      props.getGracefulTerminationTimeoutMs > 0) {
      props.getGracefulTerminationTimeoutMs.toLong
    } else {
      DEFAULT_GRACEFUL_TIMEOUT_MS
    }
    clampTimeout("graceful_termination_timeout_ms", raw)
  }

  private def clampTimeout(field: String, raw: Long): Long = {
    if (raw > ENGINE_MAX_TIMEOUT_MS) {
      logger.warn(
        s"Worker-provided $field=${raw}ms exceeds engine maximum " +
          s"${ENGINE_MAX_TIMEOUT_MS}ms; using ${ENGINE_MAX_TIMEOUT_MS}ms instead")
      ENGINE_MAX_TIMEOUT_MS
    } else {
      raw
    }
  }

  private[this] val workers = new ConcurrentHashMap[String, DirectWorkerProcess]()
  private[this] val closed = new AtomicBoolean(false)

  @volatile private var environmentState: EnvironmentState = EnvironmentState.Pending
  private val environmentLock = new Object
  private[this] var cleanupHook: Option[Thread] = None

  /**
   * Allocates a fresh endpoint address for a new worker. The string is
   * passed to the worker binary as `--connection <address>`.
   */
  protected def newEndpointAddress(workerId: String): String

  /**
   * Waits for the worker process to be ready to accept connections at
   * `address`. Throws [[DirectWorkerTimeoutException]] on timeout, or
   * [[DirectWorkerException]] if the process exits early.
   */
  protected def waitForReady(
      address: String,
      process: Process,
      outputFile: File): Unit

  /**
   * Best-effort per-endpoint cleanup, called from the spawn-failure path
   * before any [[WorkerArtifacts]] / [[WorkerConnection]] exists.
   */
  protected def cleanupEndpointAddress(address: String): Unit

  /**
   * Cleans up dispatcher-level transport state (e.g., a UDS socket
   * directory). Called from [[close]].
   */
  protected def closeTransport(): Unit

  /**
   * Validates the worker spec's transport choice. Subclasses declare
   * which transports they support. Called from the base constructor;
   * implementations must only read base-class state (`workerSpec`).
   */
  protected def validateTransportSupport(): Unit

  /** Creates a protocol-specific connection to a worker at the given address. */
  protected def createConnection(address: String): WorkerConnection

  /** Creates a protocol-specific session for the given worker. */
  protected def createSessionForWorker(worker: DirectWorkerProcess): WorkerSession

  override def createSession(
      securityScope: Option[WorkerSecurityScope]): WorkerSession = {
    require(securityScope.isEmpty,
      "securityScope is not supported yet; pass None until pooling lands")
    if (closed.get()) throwClosed()
    ensureEnvironmentReady()
    val worker = spawnWorker()
    // Acquire before publish: a concurrent close() iterating `workers` must
    // not tear down this worker before we hand it to the caller.
    worker.acquireSession()
    workers.put(worker.id, worker)
    // Re-check for close() that ran concurrently. Releasing fires the
    // ref-count callback, which removes and tears down the worker.
    if (closed.get()) {
      worker.releaseSession()
      throwClosed()
    }
    try {
      createSessionForWorker(worker)
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        worker.releaseSession()
        throw e
      case NonFatal(e) =>
        worker.releaseSession()
        throw e
    }
  }

  /**
   * Invoked when a worker's last session closes. Terminates the worker
   * today; future pooling can reuse it here instead. Safe to call after
   * dispatcher close -- the worker's own CAS-idempotent close makes a
   * second teardown a no-op.
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
   * Terminates tracked workers, removes the socket directory, and runs
   * environment cleanup. Idempotent via CAS. Does not drain in-flight
   * createSession calls -- a worker spawned racing with close tears
   * itself down through the ref-count callback, which may outlive this
   * method.
   */
  override def close(): Unit = {
    if (!closed.compareAndSet(false, true)) {
      return
    }
    // TODO: close workers in parallel -- today shutdown is serialised at
    //   N * gracefulTimeoutMs worst case.
    workers.values().iterator().asScala.foreach { w =>
      try {
        w.close()
      } catch {
        case NonFatal(e) =>
          logger.warn(s"Error closing worker ${w.id}", e)
      }
    }
    workers.clear()
    try closeTransport() catch {
      case NonFatal(e) =>
        logger.warn("Error cleaning up transport state", e)
    }
    deregisterEnvironmentCleanupHook()
    runEnvironmentCleanup()
  }

  // -- Environment lifecycle -------------------------------------------------

  // TODO: distinguish retriable vs permanent environment failures.
  private def ensureEnvironmentReady(): Unit = {
    environmentLock.synchronized {
      environmentState match {
        case EnvironmentState.Ready | EnvironmentState.CleanedUp =>
        case EnvironmentState.Failed(msg) =>
          throw new DirectWorkerException(s"Environment setup previously failed: $msg")
        case EnvironmentState.Pending =>
          val env = workerSpec.getEnvironment
          // Register up front so a partially-successful install still gets
          // torn down at JVM shutdown if dispatcher.close is never called.
          // No-op when environment_cleanup is not configured.
          registerEnvironmentCleanupHook()
          val verified = env.hasEnvironmentVerification &&
            runCallable(env.getEnvironmentVerification).exitCode == 0
          if (!verified && env.hasInstallation) {
            // Treat any install failure (timeout or non-zero exit) as
            // permanent. A partially-completed install can leave files on
            // disk that a retry would race with; retry policy belongs in
            // the future predicate (see TODO above).
            val result = try {
              runCallable(env.getInstallation)
            } catch {
              case e: DirectWorkerException =>
                environmentState = EnvironmentState.Failed(
                  s"installation failed: ${e.getMessage}")
                throw e
            }
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

  // TODO: share one JVM shutdown hook across all dispatchers in the
  //   process. Each live dispatcher is retained by the JVM until shutdown.

  /** Registers the JVM shutdown hook that runs the cleanup callable. */
  private def registerEnvironmentCleanupHook(): Unit = {
    if (!Thread.holdsLock(environmentLock)) {
      throw new IllegalStateException(
        "registerEnvironmentCleanupHook must be called while holding environmentLock")
    }
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
        throw new DirectWorkerTimeoutException(
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
    val address = newEndpointAddress(workerId)
    // Proto contract: the engine must pass --id and --connection.
    val cmd = baseCmd ++ Seq("--id", workerId, "--connection", address)
    val env = runner.getEnvironmentVariablesMap.asScala.toMap
    val outputFile = Files.createTempFile("udf-worker-", ".log")
    val process = launchProcess(cmd, env, outputFile.toFile)

    try {
      waitForReady(address, process, outputFile.toFile)
      val connection = createConnection(address)
      val artifacts = new WorkerArtifacts(process, connection, outputFile, logger)
      new DirectWorkerProcess(
        workerId, artifacts, gracefulTimeoutMs, logger,
        onLastSessionReleased = releaseWorker)
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        cleanupRawSpawn(process, address, outputFile)
        throw e
      case NonFatal(e) =>
        cleanupRawSpawn(process, address, outputFile)
        throw e
    }
  }

  // Pre-WorkerArtifacts cleanup: the connection has not been built yet,
  // so we have no bundle to close(). Each step is independent.
  private def cleanupRawSpawn(p: Process, address: String, outputFile: Path): Unit = {
    DirectWorkerDispatcher.destroyForciblyAndReap(p, logger, "failed spawn")
    try cleanupEndpointAddress(address) catch {
      case NonFatal(e) =>
        logger.debug(s"Failed to clean up endpoint address $address", e)
    }
    try Files.deleteIfExists(outputFile) catch {
      case NonFatal(e) =>
        logger.debug(s"Failed to clean up worker output file $outputFile", e)
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

  // Bounded scan so a runaway worker that writes gigabytes of output does
  // not OOM the caller during error reporting.
  protected def readOutputTail(file: File): String = {
    if (!file.exists() || file.length() == 0) return ""
    val fileLen = file.length()
    val startPos = math.max(0L, fileLen - MAX_OUTPUT_SCAN_BYTES)
    val fis = new FileInputStream(file)
    try {
      if (startPos > 0) fis.getChannel.position(startPos)
      val reader = new BufferedReader(
        new InputStreamReader(fis, StandardCharsets.UTF_8))
      // Discard the first (partial) line when we seeked into the middle.
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

  // Verification exists to short-circuit installation when the environment
  // is already prepared, so requiring installation alongside verification
  // catches user errors at spec-validation time.
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
  // Engine-side cap on proto-provided worker timeouts. The defaults below
  // must stay at or under this cap so the clamp only fires on
  // user-provided values.
  private[direct] val ENGINE_MAX_TIMEOUT_MS = 30000L
  require(DEFAULT_INIT_TIMEOUT_MS <= ENGINE_MAX_TIMEOUT_MS &&
    DEFAULT_GRACEFUL_TIMEOUT_MS <= ENGINE_MAX_TIMEOUT_MS,
    "default timeouts must not exceed ENGINE_MAX_TIMEOUT_MS")
  private[direct] val PROCESS_OUTPUT_TAIL_LINES = 50
  private[direct] val MAX_OUTPUT_SCAN_BYTES = 1024L * 1024L // 1 MiB
  // 5s bounds the wait for the kernel to reap a SIGKILL'd child. SIGKILL
  // is unblockable, so exceeding this usually means the process is stuck
  // in uninterruptible I/O (D-state) and further waiting will not help.
  private[direct] val SIGKILL_REAP_TIMEOUT_MS = 5000L

  /**
   * SIGKILL `process` and wait up to [[SIGKILL_REAP_TIMEOUT_MS]] for the
   * kernel to reap it. `destroyForcibly()` alone returns before the child
   * is reaped, which leaks a zombie until JVM exit. On reap-timeout logs
   * a warning; on interrupt re-raises the interrupt and returns.
   *
   * @param context short tag included in the timeout warning so operators
   *                can correlate a stuck child with its source.
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
