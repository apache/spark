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
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.mutable.{ArrayBuffer, Queue => MQueue}
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
 * ready (verify / install) and registers the cleanup hook. Currently spawns
 * a fresh worker per session; pooling/reuse is TODO.
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
  private val socketDir = Files.createTempDirectory("spark-udf-worker")
  private val workers = new ArrayBuffer[DirectWorkerProcess]()
  private val workersLock = new Object

  @volatile private var environmentState: EnvironmentState = EnvironmentState.Pending
  private val environmentLock = new Object
  private var cleanupHook: Option[Thread] = None

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
    ensureEnvironmentReady()
    val worker = spawnWorker()
    workersLock.synchronized { workers += worker }
    worker.acquireSession()
    try {
      createSessionForWorker(worker)
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        cleanupFailedSession(worker)
        throw e
      case NonFatal(e) =>
        cleanupFailedSession(worker)
        throw e
    }
  }

  private def cleanupFailedSession(worker: DirectWorkerProcess): Unit = {
    worker.releaseSession()
    workersLock.synchronized { workers -= worker }
    try {
      worker.close()
    } catch {
      case NonFatal(closeEx) =>
        logger.warn("Error closing worker after session creation failed", closeEx)
    }
  }

  override def close(): Unit = {
    // TODO: Close workers in parallel. Worst-case shutdown today is
    //   N * gracefulTimeoutMs because each worker waits for SIGTERM to
    //   complete before the next one is signalled. A small pool of
    //   short-lived threads would bound shutdown to ~gracefulTimeoutMs.
    workersLock.synchronized {
      workers.foreach { w =>
        try {
          w.close()
        } catch {
          case NonFatal(e) =>
            logger.warn(s"Error closing worker at ${w.socketPath}", e)
        }
      }
      workers.clear()
    }
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
          throw new RuntimeException(s"Environment setup previously failed: $msg")
        case EnvironmentState.Pending =>
          val env = workerSpec.getEnvironment
          val verified = env.hasEnvironmentVerification &&
            runCallable(env.getEnvironmentVerification).exitCode == 0
          if (!verified && env.hasInstallation) {
            val result = runCallable(env.getInstallation)
            if (result.exitCode != 0) {
              val detail = s"exit code ${result.exitCode}\n${result.outputTail}"
              environmentState = EnvironmentState.Failed(detail)
              throw new RuntimeException(
                s"Environment installation failed with $detail")
            }
          }
          registerEnvironmentCleanupHook()
          environmentState = EnvironmentState.Ready
      }
    }
  }

  /**
   * Registers the JVM shutdown hook that runs the cleanup callable.
   *
   * '''Caller must hold `environmentLock`''' -- this method reads and
   * writes `cleanupHook` without its own synchronization. It is only
   * called from `ensureEnvironmentReady`, which already owns the lock.
   */
  private def registerEnvironmentCleanupHook(): Unit = {
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
        process.destroyForcibly()
        val tail = readOutputTail(outputFile.toFile)
        throw new RuntimeException(
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
        process, connection, socketPath, outputFile, gracefulTimeoutMs, logger)
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
      outputFile: java.nio.file.Path): Unit = {
    if (process.isAlive) process.destroyForcibly()
    // If the worker (or createConnection) had already created the socket
    // file, remove it so it doesn't linger until dispatcher.close().
    try Files.deleteIfExists(new File(socketPath).toPath) catch {
      case NonFatal(cleanupEx) =>
        logger.debug(s"Failed to clean up socket file $socketPath", cleanupEx)
    }
    Files.deleteIfExists(outputFile)
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
      if (!process.isAlive) {
        val tail = readOutputTail(outputFile)
        throw new RuntimeException(
          s"Worker exited with code ${process.exitValue()} " +
            s"before creating socket at $socketPath\n$tail")
      }
      Thread.sleep(SOCKET_POLL_INTERVAL_MS)
      attempts += 1
    }
    if (!file.exists()) {
      val tail = readOutputTail(outputFile)
      if (process.isAlive) process.destroyForcibly()
      throw new RuntimeException(
        s"Worker did not create socket at $socketPath within ${initTimeoutMs}ms\n$tail")
    }
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
      var remaining = startPos
      while (remaining > 0) {
        val n = fis.skip(remaining)
        if (n <= 0) remaining = 0 else remaining -= n
      }
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
