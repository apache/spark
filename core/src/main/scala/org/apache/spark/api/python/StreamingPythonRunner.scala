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

package org.apache.spark.api.python

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.nio.channels.Channels
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkEnv, SparkPythonException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{PYTHON_WORKER_MODULE, PYTHON_WORKER_RESPONSE, SESSION_ID}
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.Python.{PYTHON_AUTH_SOCKET_TIMEOUT, PYTHON_UNIX_DOMAIN_SOCKET_ENABLED}


private[spark] object StreamingPythonRunner {
  // Poll interval used by `readInterruptibly`'s watchdog to re-check the abort conditions (thread
  // interrupted or the owning query stopping) while it waits for a worker response. Short enough
  // that a streaming query stop() unblocks well within `spark.sql.streaming.stopTimeout`, long
  // enough to add negligible overhead.
  private val readWatchdogPollMillis = 500

  // Grace period the watchdog allows after the abort condition first trips before it force-stops
  // the worker. A responsive worker finishes the in-flight batch in well under this window, so a
  // normal stop() lets the read complete cleanly (no spurious foreachBatch error); only a genuinely
  // wedged worker is still blocked after the grace and gets killed. Kept comfortably below the
  // default `spark.sql.streaming.stopTimeout` used by the tests (30s).
  private val readAbortGraceMillis = 10000L

  def apply(
      func: PythonFunction,
      connectUrl: String,
      sessionId: String,
      workerModule: String
  ): StreamingPythonRunner = {
    new StreamingPythonRunner(func, connectUrl, sessionId, workerModule)
  }
}

private[spark] class StreamingPythonRunner(
    func: PythonFunction,
    connectUrl: String,
    sessionId: String,
    workerModule: String) extends Logging {
  private val conf = SparkEnv.get.conf
  private val isUnixDomainSock = conf.get(PYTHON_UNIX_DOMAIN_SOCKET_ENABLED)
  protected val bufferSize: Int = conf.get(BUFFER_SIZE)
  protected val authSocketTimeout = conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)

  protected val envVars: java.util.Map[String, String] = func.envVars
  protected val pythonExec: String = func.pythonExec
  protected var pythonWorker: Option[PythonWorker] = None
  protected var pythonWorkerFactory: Option[PythonWorkerFactory] = None
  protected val pythonVer: String = func.pythonVer

  // Predicate consulted by `readInterruptibly` to learn when a blocking worker read should be
  // abandoned (e.g. the owning streaming query is being stopped). Defaults to never; callers that
  // know the lifecycle (the Connect foreachBatch cleaner cache) install a real check.
  @volatile private var shouldAbortRead: () => Boolean = () => false

  /**
   * Initializes the Python worker for streaming functions. Sets up Spark Connect session
   * to be used with the functions.
   */
  def init(): (DataOutputStream, DataInputStream) = {
    logInfo(log"[session: ${MDC(SESSION_ID, sessionId)}] Sending necessary information to the " +
      log"Python worker")
    val env = SparkEnv.get

    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir)

    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", authSocketTimeout.toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)
    if (!connectUrl.isEmpty) {
      envVars.put("SPARK_CONNECT_LOCAL_URL", connectUrl)
    }
    envVars.put("SPARK_PYTHON_RUNTIME", "PYTHON_WORKER")

    val workerFactory =
      new PythonWorkerFactory(pythonExec, workerModule, envVars.asScala.toMap, false)
    val (worker: PythonWorker, _) = workerFactory.createSimpleWorker(blockingMode = true)
    pythonWorker = Some(worker)
    pythonWorkerFactory = Some(workerFactory)

    val socketChannel = pythonWorker.get.channel
    val stream = new BufferedOutputStream(Channels.newOutputStream(socketChannel), bufferSize)
    val dataIn = new DataInputStream(
      new BufferedInputStream(Channels.newInputStream(socketChannel), bufferSize))
    val dataOut = new DataOutputStream(stream)

    val originalTimeout = if (!isUnixDomainSock) {
      val timeout = socketChannel.socket().getSoTimeout()
      // Set timeout to 5 minute during initialization config transmission
      socketChannel.socket().setSoTimeout(5 * 60 * 1000)
      Some(timeout)
    } else {
      None
    }

    val resFromPython = try {
      PythonWorkerUtils.writePythonVersion(pythonVer, dataOut)

      // Send sessionId
      if (!sessionId.isEmpty) {
        PythonRDD.writeUTF(sessionId, dataOut)
      }

      // Send the user function to python process
      PythonWorkerUtils.writePythonFunction(func, dataOut)
      dataOut.flush()

      logInfo(log"[session: ${MDC(SESSION_ID, sessionId)}] Reading initialization response from " +
        log"Python runner.")
      dataIn.readInt()
    } catch {
      case e: java.net.SocketTimeoutException =>
        throw new StreamingPythonRunnerInitializationTimeoutException(e.getMessage)
      case e: Exception =>
        throw new StreamingPythonRunnerInitializationCommunicationException(e.getMessage)
    }

    // Set timeout back to the original timeout
    // Should be infinity by default
    originalTimeout.foreach(v => socketChannel.socket().setSoTimeout(v))

    if (resFromPython != 0) {
      val errMessage = PythonWorkerUtils.readUTF(dataIn)
      throw new StreamingPythonRunnerInitializationException(resFromPython, errMessage)
    }
    logInfo(log"[session: ${MDC(SESSION_ID, sessionId)}] Runner initialization succeeded " +
      log"(returned ${MDC(PYTHON_WORKER_RESPONSE, resFromPython)}).")

    (dataOut, dataIn)
  }

  class StreamingPythonRunnerInitializationCommunicationException(errMessage: String)
    extends SparkPythonException(
      errorClass = "STREAMING_PYTHON_RUNNER_INITIALIZATION_COMMUNICATION_FAILURE",
      messageParameters = Map("msg" -> errMessage))

  class StreamingPythonRunnerInitializationTimeoutException(errMessage: String)
    extends SparkPythonException(
      errorClass = "STREAMING_PYTHON_RUNNER_INITIALIZATION_TIMEOUT_FAILURE",
      messageParameters = Map("msg" -> errMessage))

  class StreamingPythonRunnerInitializationException(resFromPython: Int, errMessage: String)
    extends SparkPythonException(
      errorClass = "STREAMING_PYTHON_RUNNER_INITIALIZATION_FAILURE",
      messageParameters = Map(
        "resFromPython" -> resFromPython.toString,
        "msg" -> errMessage))

  /**
   * Stops the Python worker.
   */
  def stop(): Unit = {
    logInfo(log"[session: ${MDC(SESSION_ID, sessionId)}] Stopping streaming runner," +
      log" module: ${MDC(PYTHON_WORKER_MODULE, workerModule)}.")

    try {
      pythonWorkerFactory.foreach { factory =>
        pythonWorker.foreach { worker =>
          factory.stopWorker(worker)
          factory.stop()
        }
      }
    } catch {
      case e: Exception =>
        logError("Exception when trying to kill worker", e)
    }
  }

  /**
   * Returns whether the Python worker has been stopped.
   * @return Some(true) if the Python worker has been stopped.
   *         None if either the Python worker or the Python worker factory is not initialized.
   */
  def isWorkerStopped(): Option[Boolean] = {
    pythonWorkerFactory.flatMap { factory =>
      pythonWorker.map { worker =>
        factory.isWorkerStopped(worker)
      }
    }
  }

  /**
   * Installs a predicate telling [[readInterruptibly]] when an in-flight blocking worker read
   * should be abandoned -- typically `() => !query.isActive`, so a streaming query stop() can
   * promptly break a wedged read. Idempotent; the latest check wins.
   */
  def setReadAbortCheck(check: () => Boolean): Unit = {
    shouldAbortRead = check
  }

  /**
   * Reads a single Int from the Python worker in a way that is responsive to the owning streaming
   * query being stopped.
   *
   * A streaming query `stop()` interrupts its micro-batch execution thread to unwind it, but the
   * blocking socket read below honors neither `Thread.interrupt()` nor a socket read timeout: the
   * worker channel is read via `Channels.newInputStream`, which is not closed by interrupting the
   * reader thread, and `SO_TIMEOUT` has no effect on channel reads. So if a foreachBatch Python
   * worker is wedged -- e.g. deadlocked on a re-entrant Spark Connect call -- a plain
   * `dataIn.readInt()` keeps the stream thread blocked until `spark.sql.streaming.stopTimeout`
   * elapses (and, with the default infinite timeout, forever), so the stop fails to take effect.
   *
   * To make the read abortable we guard it with a watchdog thread that watches for an abort
   * condition -- the calling thread is interrupted, or the installed [[shouldAbortRead]] check
   * trips (the query is being stopped). Once the condition has held for a short grace period (so a
   * responsive worker can finish the in-flight batch and let the read complete cleanly), the
   * watchdog stops the worker. `stop()` closes the worker channel, which unblocks the in-flight
   * read with an `AsynchronousCloseException` so a genuinely wedged batch (and the query) can
   * unwind promptly. When no abort is requested, or the read completes within the grace, the
   * worker is left untouched, so neither a healthy stop nor a legitimately slow batch is disrupted.
   */
  def readInterruptibly(dataIn: DataInputStream): Int = {
    val callerThread = Thread.currentThread()
    val readDone = new AtomicBoolean(false)
    val watchdogBody: Runnable = () => {
      try {
        var triggered = false
        var abortSinceNanos = -1L
        while (!triggered && !readDone.get()) {
          if (callerThread.isInterrupted || shouldAbortRead()) {
            if (abortSinceNanos < 0L) {
              abortSinceNanos = System.nanoTime()
            }
            val elapsedMillis = (System.nanoTime() - abortSinceNanos) / 1000000L
            if (elapsedMillis >= StreamingPythonRunner.readAbortGraceMillis) {
              logWarning(
                log"[session: ${MDC(SESSION_ID, sessionId)}] Python worker read still blocked " +
                  log"after the query began stopping; stopping the worker to unblock it.")
              stop()
              triggered = true
            } else {
              Thread.sleep(StreamingPythonRunner.readWatchdogPollMillis)
            }
          } else {
            // Not aborting (the common case): reset the grace timer and keep waiting.
            abortSinceNanos = -1L
            Thread.sleep(StreamingPythonRunner.readWatchdogPollMillis)
          }
        }
      } catch {
        case _: InterruptedException => // The read finished; readDone was set. Just exit.
      }
    }
    val watchdog = new Thread(watchdogBody, s"streaming-python-runner-read-watchdog-$sessionId")
    watchdog.setDaemon(true)
    watchdog.start()
    try {
      dataIn.readInt()
    } finally {
      readDone.set(true)
      watchdog.interrupt()
    }
  }
}
