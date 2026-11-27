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

package org.apache.spark.sql.execution.python

import java.io.{BufferedInputStream, DataInputStream, DataOutputStream, InputStream, InterruptedIOException, IOException}
import java.net.{InetSocketAddress, StandardSocketOptions}
import java.nio.ByteBuffer
import java.nio.channels.{CancelledKeyException, ClosedSelectorException, SelectionKey}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, PythonException, PythonFunction, PythonWorker, PythonWorkerHandle, PythonWorkerUtils, SpecialLengths}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{BUFFER_SIZE, EXECUTOR_CORES}
import org.apache.spark.internal.config.Python._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ByteBufferOutputStream

/**
 * The messages that the JVM and the Python worker module
 * `pyspark.sql.worker.data_source_lookup` exchange after the initialization. They must match the
 * constants of that module.
 *
 * From the JVM: `LOOKUP_REQUEST`, followed by one Arrow stream with one batch of parameter rows;
 * `LOOKUP_CANCEL`, sent only after the worker has read a request, to stop its response; and
 * `LOOKUP_END`, to close the reader. From Python, per request: `START_ARROW_STREAM` once it has
 * read the request, one Arrow stream of result rows, whose first column is the ordinal of the
 * parameter row that a row answers, and `LOOKUP_RESPONSE_END`, followed by the status of the
 * response. A cancel can arrive after the response that it cancels has ended; Python then ignores
 * it.
 */
object PythonExternalLookupProtocol {
  val WORKER_MODULE = "pyspark.sql.worker.data_source_lookup"

  val LOOKUP_REQUEST = 1
  val LOOKUP_CANCEL = 2
  val LOOKUP_END = 3
  val LOOKUP_RESPONSE_END = 4

  val STATUS_COMPLETE = 0
  val STATUS_CANCELLED = 1
}

/**
 * One Python worker that runs the module `pyspark.sql.worker.data_source_lookup` for one external
 * lookup reader, inside a task, and serves the reader's requests one at a time.
 *
 * It is modeled on [[PythonPlannerRunner]], which runs a named module on the driver for one
 * call, and is not a [[BasePythonRunner]], which writes its input as soon as the worker can take
 * it, while a lookup has no next request until the join has read the current response. Like the
 * planner runner, it starts the worker through `SparkEnv.createPythonWorker`, with the daemon
 * and worker reuse, and uses non-blocking I/O on the worker's selector, through [[WorkerChannel]].
 *
 * The worker is used from one thread at a time, except [[destroy]], which any thread may call:
 * it is the only call that the interrupt listener and a close from another thread make. The
 * completion listener closes the reader, which may close the worker gracefully, on the thread
 * that made the reader's last call. A worker that is released or destroyed is not used again.
 *
 * @param func the planned lookup reader, pickled, with the Python environment of the data source
 * @param context the task that the worker runs in
 * @param envVars the environment of the worker, which identifies its worker pool
 */
private[spark] class PythonExternalLookupWorker private (
    func: PythonFunction,
    context: TaskContext,
    val worker: PythonWorker,
    handle: Option[PythonWorkerHandle],
    envVars: Map[String, String],
    reuseWorker: Boolean,
    faultHandlerEnabled: Boolean,
    idleTimeoutSeconds: Long,
    killOnIdleTimeout: Boolean,
    bufferSize: Int) extends Logging {
  import PythonExternalLookupProtocol._

  // Whether the worker is released into the idle pool or destroyed. Only the one that flips it
  // releases or destroys the worker, as in `BasePythonRunner`.
  private val releasedOrClosed = new AtomicBoolean(false)

  private val channel = new WorkerChannel(
    worker,
    new BasePythonRunner.IdleTimeoutHandler(
      worker, handle, idleTimeoutSeconds, killOnIdleTimeout, Some(context), logName,
      workerName = "Python lookup worker"),
    context)

  /** The stream of the worker's replies. */
  val dataIn = new DataInputStream(new BufferedInputStream(channel, bufferSize))

  /** The bytes written to and read from the worker. */
  def bytesSent: Long = channel.bytesSent
  def bytesReceived: Long = channel.bytesReceived

  /** Whether the worker is released or destroyed. */
  def isReleasedOrClosed: Boolean = releasedOrClosed.get()

  /**
   * Sends a message: its bytes are written while the next reply is read, after the bytes of the
   * messages before it.
   */
  def send(message: ByteBuffer): Unit = channel.send(message)

  /** Sends a cancel of the response that is being read. */
  def sendCancel(): Unit = {
    val message = ByteBuffer.allocate(4).putInt(LOOKUP_CANCEL)
    message.flip()
    send(message)
  }

  /** Fails the reads that do not end within `timeoutMs`, from now; a negative value disables. */
  def setReadTimeout(timeoutMs: Long): Unit = {
    channel.deadlineNs =
      if (timeoutMs < 0) 0L else System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
  }

  /**
   * Reads the next int of a reply, and throws the Python exception that the worker reports
   * instead, if any: the worker exits after it, so it is destroyed.
   */
  def readReplyInt(): Int = {
    val value = dataIn.readInt()
    if (value == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val traceback = PythonWorkerUtils.readUTF(dataIn)
      destroy()
      // As `BasePythonRunner` reports an exception of a Python worker.
      throw new PythonException(
        errorClass = "PYTHON_EXCEPTION",
        messageParameters = Map(
          "msg" -> "An exception was thrown from the Python worker",
          "traceback" -> traceback))
    }
    value
  }

  /**
   * Ends the worker after the reader's last request: Python closes the reader, and sends its
   * accumulator updates. The worker is then released into the idle pool if worker reuse is
   * enabled, or destroyed. If Python has not finished within `timeoutMs`, as when the reader's
   * `close()` blocks, the worker is destroyed and the close fails, as the updates are lost.
   */
  def closeGracefully(timeoutMs: Long): Unit = {
    val message = ByteBuffer.allocate(8).putInt(LOOKUP_END).putInt(SpecialLengths.END_OF_STREAM)
    message.flip()
    send(message)
    setReadTimeout(timeoutMs)
    try {
      val reply = readReplyInt()
      if (reply != 0) throw protocolError(s"close reply $reply")
      PythonWorkerUtils.receiveAccumulatorUpdates(Option(func.accumulator), dataIn)
      dataIn.readInt() match {
        case SpecialLengths.END_OF_STREAM if reuseWorker =>
          if (releasedOrClosed.compareAndSet(false, true)) {
            SparkEnv.get.releasePythonWorker(
              func.pythonExec, WORKER_MODULE, envVars, worker)
          }
        case _ => destroy()
      }
    } catch {
      case _: WorkerReadTimeoutException =>
        destroy()
        throw QueryCompilationErrors.pythonDataSourceError(
          action = "close",
          tpe = "lookup reader",
          msg = s"close() did not return within $timeoutMs ms, the value of " +
            s"${PYTHON_TASK_KILL_TIMEOUT.key}.")
    } finally {
      setReadTimeout(-1)
    }
  }

  /** Destroys the worker unless it is released or destroyed already. Any thread may call it. */
  def destroy(): Unit = {
    if (releasedOrClosed.compareAndSet(false, true)) {
      try {
        SparkEnv.get.destroyPythonWorker(func.pythonExec, WORKER_MODULE, envVars, worker)
      } catch {
        case e: Exception => logWarning("Failed to destroy the Python lookup worker", e)
      }
    }
  }

  /** The error of an unexpected message from the worker. */
  def protocolError(found: String): SparkException =
    SparkException.internalError(s"Unexpected message from the Python lookup worker: $found")

  /**
   * Maps an I/O failure of the worker to the error that the task reports, as
   * [[PythonPlannerRunner.runInPython]] does.
   */
  def ioError(e: IOException): SparkException = if (!faultHandlerEnabled) {
    new SparkException(
      "Python worker exited unexpectedly (crashed). " +
        "Consider setting 'spark.sql.execution.pyspark.udf.faulthandler.enabled' or " +
        s"'${PYTHON_WORKER_FAULTHANLDER_ENABLED.key}' configuration to 'true' for " +
        "the better Python traceback.", e)
  } else {
    new SparkException(
      BasePythonRunner.withTerminationDiagnostics(
        "Python worker exited unexpectedly (crashed)", handle), e)
  }

  /** Writes the initialization and reads the worker's reply to it. */
  private def initialize(
      runnerConf: Map[String, String],
      jobArtifactUUID: Option[String],
      writeInit: DataOutputStream => Unit): Unit = {
    val buffer = new ByteBufferOutputStream()
    val dataOut = new DataOutputStream(buffer)
    PythonWorkerUtils.writePythonVersion(func.pythonVer, dataOut)
    PythonWorkerUtils.writeSparkFiles(
      jobArtifactUUID, func.pythonIncludes.asScala.toSet, dataOut)
    PythonWorkerUtils.writeBroadcasts(
      func.broadcastVars.asScala.toSeq, worker, SparkEnv.get, dataOut)
    PythonWorkerUtils.writeConf(runnerConf, dataOut)
    // The task is not a barrier task, so the connection info of a barrier server is not used.
    PythonWorkerUtils.writeTaskContext(context, Right(-1), None, dataOut)
    PythonWorkerUtils.writePythonFunction(func, dataOut)
    writeInit(dataOut)
    dataOut.close()
    send(buffer.toByteBuffer)
    readReplyInt() match {
      case 0 =>
      case other => throw protocolError(s"initialization reply $other")
    }
  }
}

private[spark] object PythonExternalLookupWorker extends Logging {
  import PythonExternalLookupProtocol._

  /**
   * Starts a worker for a reader in the current task, and initializes it.
   *
   * The environment of the worker is the one that [[PythonPlannerRunner.runInPython]] builds,
   * with the session configurations read through `SQLConf.get`, which a task reads from the
   * session, as [[PythonUDFRunner]] does, plus what `BasePythonRunner.compute` sets for a task:
   * `OMP_NUM_THREADS` and the memory limit of the worker. The worker runs no profiler.
   */
  def start(
      func: PythonFunction,
      context: TaskContext,
      runnerConf: Map[String, String],
      sessionUUID: Option[String],
      jobArtifactUUID: Option[String])(writeInit: DataOutputStream => Unit)
    : PythonExternalLookupWorker = {
    val env = SparkEnv.get
    val sparkConf = env.conf
    val sqlConf = SQLConf.get
    val useDaemon = sparkConf.get(PYTHON_USE_DAEMON)
    val reuseWorker = sparkConf.get(PYTHON_WORKER_REUSE)
    val bufferSize = sparkConf.get(BUFFER_SIZE)
    val faultHandlerEnabled = sqlConf.pythonUDFWorkerFaulthandlerEnabled
    val envVars = PythonWorkerEnvironment.mergeValidated(func.envVars, sqlConf)

    envVars.put("SPARK_LOCAL_DIRS",
      env.blockManager.diskBlockManager.localDirs.map(_.getPath).mkString(","))
    if (reuseWorker) {
      envVars.put("SPARK_REUSE_WORKER", "1")
    }
    if (sqlConf.pysparkHideTraceback) {
      envVars.put("SPARK_HIDE_TRACEBACK", "1")
    }
    if (sqlConf.pysparkSimplifiedTraceback) {
      envVars.put("SPARK_SIMPLIFIED_TRACEBACK", "1")
    }
    if (sqlConf.pysparkTracebackWithLocals) {
      envVars.put("SPARK_TRACEBACK_WITH_LOCALS", "1")
    }
    // As BasePythonRunner.compute does for the workers of a task.
    if (sparkConf.getOption("spark.executorEnv.OMP_NUM_THREADS").isEmpty) {
      envVars.put("OMP_NUM_THREADS",
        context.cpuAmount().setScale(0, BigDecimal.RoundingMode.CEILING).intValue.toString)
    }
    workerMemoryMb(context, sparkConf.get(EXECUTOR_CORES)).foreach { memoryMb =>
      envVars.put("PYSPARK_EXECUTOR_MEMORY_MB", memoryMb.toString)
    }
    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", sparkConf.get(PYTHON_AUTH_SOCKET_TIMEOUT).toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)
    if (faultHandlerEnabled) {
      envVars.put("PYTHON_FAULTHANDLER_DIR", BasePythonRunner.faultHandlerLogDir.toString)
    }
    val tracebackDumpIntervalSeconds = sqlConf.pythonUDFWorkerTracebackDumpIntervalSeconds
    if (tracebackDumpIntervalSeconds > 0L) {
      envVars.put("PYTHON_TRACEBACK_DUMP_INTERVAL_SECONDS", tracebackDumpIntervalSeconds.toString)
    }
    if (useDaemon && sqlConf.pythonUDFDaemonKillWorkerOnFlushFailure) {
      envVars.put("PYTHON_DAEMON_KILL_WORKER_ON_FLUSH_FAILURE", "1")
    }
    envVars.put("SPARK_JOB_ARTIFACT_UUID", jobArtifactUUID.getOrElse("default"))
    sessionUUID.foreach(uuid => envVars.put("PYSPARK_SPARK_SESSION_UUID", uuid))
    envVars.put("SPARK_PYTHON_RUNTIME", "PYTHON_WORKER")

    val workerEnv = envVars.asScala.toMap
    val (worker, handle) = env.createPythonWorker(func.pythonExec, WORKER_MODULE, workerEnv,
      useDaemon)
    val lookupWorker = new PythonExternalLookupWorker(
      func, context, worker, handle, workerEnv, reuseWorker, faultHandlerEnabled,
      sqlConf.pythonUDFWorkerIdleTimeoutSeconds, sqlConf.pythonUDFWorkerKillOnIdleTimeout,
      bufferSize)
    try {
      // As the worker does on its side, in `disable_nagle`. Otherwise a cancel that reaches the
      // worker after the end of its response, which the worker does not answer, would hold the
      // next message until the worker's delayed acknowledgement, about 40 ms on Linux. A Unix
      // domain socket has no such delay.
      if (worker.channel.getLocalAddress.isInstanceOf[InetSocketAddress]) {
        worker.channel.setOption[java.lang.Boolean](StandardSocketOptions.TCP_NODELAY, true)
      }
      // The worker runs no profiler: its results would travel through an accumulator that no
      // executor-side path reads.
      lookupWorker.initialize(
        runnerConf - SQLConf.PYTHON_DATA_SOURCE_PROFILER.key, jobArtifactUUID, writeInit)
    } catch {
      case e: PythonException =>
        lookupWorker.destroy()
        throw QueryCompilationErrors.pythonDataSourceError(
          action = "initialize", tpe = "lookup reader", msg = e.getMessage)
      case e: IOException =>
        lookupWorker.destroy()
        throw lookupWorker.ioError(e)
      case e: Throwable =>
        lookupWorker.destroy()
        throw e
    }
    lookupWorker
  }

  /** The memory limit of a worker of the task, as `BasePythonRunner.compute` computes it. */
  private def workerMemoryMb(context: TaskContext, defaultExecutorCores: Int): Option[Long] = {
    val memoryMb = Option(context.getLocalProperty(ResourceProfile.PYSPARK_MEMORY_LOCAL_PROPERTY))
      .map(_.toLong)
    val executorCores =
      Option(context.getLocalProperty(ResourceProfile.EXECUTOR_CORES_LOCAL_PROPERTY))
        .map(_.toInt).getOrElse(defaultExecutorCores)
    val maxConcurrentTasks =
      Option(context.getLocalProperty(ResourceProfile.MAX_TASKS_PER_EXECUTOR_LOCAL_PROPERTY))
        .map(_.toInt)
        .getOrElse(ResourceProfile.numTasksBasedOnCores(
          BigDecimal(executorCores), context.cpuAmount()))
    BasePythonRunner.getWorkerMemoryMb(memoryMb, maxConcurrentTasks)
  }
}

/**
 * The non-blocking I/O with a lookup worker, on the worker's selector, as
 * `PythonPlannerRunner.WorkerInputStream` does, generalized from "write everything, then read
 * everything" to a message at a time: the bytes of a message that [[send]] queues are written
 * while the reply is read. Keeping the channel non-blocking avoids switching a pooled daemon
 * worker to blocking mode and back.
 *
 * A wait for the worker counts toward the idle timeout of [[BasePythonRunner.IdleTimeoutHandler]],
 * unless a deadline is set: a read then fails once the deadline passes. Between requests the JVM
 * does not read, so a long gap between requests is never an idle timeout. It is used from one
 * thread at a time; the byte counts may be read from any thread.
 */
private class WorkerChannel(
    worker: PythonWorker,
    idleTimeoutHandler: BasePythonRunner.IdleTimeoutHandler,
    context: TaskContext) extends InputStream {

  private[this] val temp = new Array[Byte](1)
  private[this] var output: ByteBuffer = ByteBuffer.allocate(0)

  @volatile var bytesSent = 0L
  @volatile var bytesReceived = 0L

  /** The time, by `System.nanoTime()`, after which a read fails, or 0 for none. */
  var deadlineNs = 0L

  /**
   * Queues a message after the bytes that are still pending. A cancel stays pending when the rest
   * of its response was already buffered; it is then written before the next message, and the
   * worker ignores it.
   */
  def send(message: ByteBuffer): Unit = {
    output = if (!output.hasRemaining) message else {
      val pending = ByteBuffer.allocate(output.remaining() + message.remaining())
      pending.put(output).put(message).flip()
      pending
    }
    worker.selectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE)
  }

  override def read(): Int = {
    val n = read(temp)
    if (n <= 0) {
      -1
    } else {
      // Signed byte to unsigned integer
      temp(0) & 0xff
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val buf = ByteBuffer.wrap(b, off, len)
    var n = 0
    try {
      while (n == 0) {
        waitForWorker()
        // A wait returns at once while the thread is interrupted, so the task stops here once
        // it is killed, and so does any other interrupt.
        context.killTaskIfInterrupted()
        if (Thread.currentThread().isInterrupted) {
          throw new InterruptedIOException("Interrupted while waiting for the Python lookup worker")
        }
        if (worker.selectionKey.isReadable) {
          n = idleTimeoutHandler.read(buf)
          if (n > 0) bytesReceived += n
        }
        if (worker.selectionKey.isWritable) {
          var acceptsInput = true
          while (acceptsInput && output.hasRemaining) {
            val written = idleTimeoutHandler.write(output)
            bytesSent += written
            acceptsInput = written > 0
          }
          if (!output.hasRemaining) {
            // There is no more data to write to the socket.
            worker.selectionKey.interestOps(SelectionKey.OP_READ)
          }
        }
      }
    } catch {
      // Another thread destroyed the worker, which closes its selector.
      case e @ (_: ClosedSelectorException | _: CancelledKeyException) =>
        throw new IOException("The Python lookup worker was destroyed", e)
    }
    n
  }

  private def waitForWorker(): Unit = {
    if (deadlineNs == 0L) {
      idleTimeoutHandler.select(output.hasRemaining)
    } else {
      val remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNs - System.nanoTime())
      if (remainingMs <= 0) {
        throw new WorkerReadTimeoutException
      }
      worker.selector.select(remainingMs)
    }
  }
}

/** A read from a lookup worker that did not end by its deadline. */
private[spark] class WorkerReadTimeoutException
  extends IOException("The Python lookup worker did not reply in time")
