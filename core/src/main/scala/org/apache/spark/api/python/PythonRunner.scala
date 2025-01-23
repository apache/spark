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

import java.io._
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files => JavaFiles, Path}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.api.python.PythonFunction.PythonAccumulator
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.internal.LogKeys.TASK_NAME
import org.apache.spark.internal.config.{BUFFER_SIZE, EXECUTOR_CORES}
import org.apache.spark.internal.config.Python._
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.resource.ResourceProfile.{EXECUTOR_CORES_LOCAL_PROPERTY, PYSPARK_MEMORY_LOCAL_PROPERTY}
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.util._


/**
 * Enumerate the type of command that will be sent to the Python worker
 */
private[spark] object PythonEvalType {
  val NON_UDF = 0

  val SQL_BATCHED_UDF = 100
  val SQL_ARROW_BATCHED_UDF = 101

  val SQL_SCALAR_PANDAS_UDF = 200
  val SQL_GROUPED_MAP_PANDAS_UDF = 201
  val SQL_GROUPED_AGG_PANDAS_UDF = 202
  val SQL_WINDOW_AGG_PANDAS_UDF = 203
  val SQL_SCALAR_PANDAS_ITER_UDF = 204
  val SQL_MAP_PANDAS_ITER_UDF = 205
  val SQL_COGROUPED_MAP_PANDAS_UDF = 206
  val SQL_MAP_ARROW_ITER_UDF = 207
  val SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE = 208
  val SQL_GROUPED_MAP_ARROW_UDF = 209
  val SQL_COGROUPED_MAP_ARROW_UDF = 210
  val SQL_TRANSFORM_WITH_STATE_PANDAS_UDF = 211
  val SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF = 212

  val SQL_TABLE_UDF = 300
  val SQL_ARROW_TABLE_UDF = 301

  def toString(pythonEvalType: Int): String = pythonEvalType match {
    case NON_UDF => "NON_UDF"
    case SQL_BATCHED_UDF => "SQL_BATCHED_UDF"
    case SQL_ARROW_BATCHED_UDF => "SQL_ARROW_BATCHED_UDF"
    case SQL_SCALAR_PANDAS_UDF => "SQL_SCALAR_PANDAS_UDF"
    case SQL_GROUPED_MAP_PANDAS_UDF => "SQL_GROUPED_MAP_PANDAS_UDF"
    case SQL_GROUPED_AGG_PANDAS_UDF => "SQL_GROUPED_AGG_PANDAS_UDF"
    case SQL_WINDOW_AGG_PANDAS_UDF => "SQL_WINDOW_AGG_PANDAS_UDF"
    case SQL_SCALAR_PANDAS_ITER_UDF => "SQL_SCALAR_PANDAS_ITER_UDF"
    case SQL_MAP_PANDAS_ITER_UDF => "SQL_MAP_PANDAS_ITER_UDF"
    case SQL_COGROUPED_MAP_PANDAS_UDF => "SQL_COGROUPED_MAP_PANDAS_UDF"
    case SQL_MAP_ARROW_ITER_UDF => "SQL_MAP_ARROW_ITER_UDF"
    case SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE => "SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE"
    case SQL_GROUPED_MAP_ARROW_UDF => "SQL_GROUPED_MAP_ARROW_UDF"
    case SQL_COGROUPED_MAP_ARROW_UDF => "SQL_COGROUPED_MAP_ARROW_UDF"
    case SQL_TABLE_UDF => "SQL_TABLE_UDF"
    case SQL_ARROW_TABLE_UDF => "SQL_ARROW_TABLE_UDF"
    case SQL_TRANSFORM_WITH_STATE_PANDAS_UDF => "SQL_TRANSFORM_WITH_STATE_PANDAS_UDF"
    case SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF =>
      "SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF"
  }
}

private[spark] object BasePythonRunner {

  private[spark] lazy val faultHandlerLogDir = Utils.createTempDir(namePrefix = "faulthandler")

  private[spark] def faultHandlerLogPath(pid: Int): Path = {
    new File(faultHandlerLogDir, pid.toString).toPath
  }
}

/**
 * A helper class to run Python mapPartition/UDFs in Spark.
 *
 * funcs is a list of independent Python functions, each one of them is a list of chained Python
 * functions (from bottom to top).
 */
private[spark] abstract class BasePythonRunner[IN, OUT](
    protected val funcs: Seq[ChainedPythonFunctions],
    protected val evalType: Int,
    protected val argOffsets: Array[Array[Int]],
    protected val jobArtifactUUID: Option[String],
    protected val metrics: Map[String, AccumulatorV2[Long, Long]])
  extends Logging {

  require(funcs.length == argOffsets.length, "argOffsets should have the same length as funcs")

  private val conf = SparkEnv.get.conf
  protected val bufferSize: Int = conf.get(BUFFER_SIZE)
  protected val timelyFlushEnabled: Boolean = false
  protected val timelyFlushTimeoutNanos: Long = 0
  protected val authSocketTimeout = conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)
  private val reuseWorker = conf.get(PYTHON_WORKER_REUSE)
  protected val faultHandlerEnabled: Boolean = conf.get(PYTHON_WORKER_FAULTHANLDER_ENABLED)
  protected val simplifiedTraceback: Boolean = false

  // All the Python functions should have the same exec, version and envvars.
  protected val envVars: java.util.Map[String, String] = funcs.head.funcs.head.envVars
  protected val pythonExec: String = funcs.head.funcs.head.pythonExec
  protected val pythonVer: String = funcs.head.funcs.head.pythonVer

  protected val batchSizeForPythonUDF: Int = 100

  // WARN: Both configurations, 'spark.python.daemon.module' and 'spark.python.worker.module' are
  // for very advanced users and they are experimental. This should be considered
  // as expert-only option, and shouldn't be used before knowing what it means exactly.

  // This configuration indicates the module to run the daemon to execute its Python workers.
  private val daemonModule =
    conf.get(PYTHON_DAEMON_MODULE).map { value =>
      logInfo(
        log"Python daemon module in PySpark is set to " +
        log"[${MDC(LogKeys.VALUE, value)}] in '${MDC(LogKeys.CONFIG,
          PYTHON_DAEMON_MODULE.key)}', using this to start the daemon up. Note that this " +
          log"configuration only has an effect when '${MDC(LogKeys.CONFIG2,
            PYTHON_USE_DAEMON.key)}' is enabled and the platform is not Windows.")
      value
    }.getOrElse("pyspark.daemon")

  // This configuration indicates the module to run each Python worker.
  private val workerModule =
    conf.get(PYTHON_WORKER_MODULE).map { value =>
      logInfo(
        log"Python worker module in PySpark is set to ${MDC(LogKeys.VALUE, value)} " +
        log"in ${MDC(LogKeys.CONFIG, PYTHON_WORKER_MODULE.key)}, " +
        log"using this to start the worker up. Note that this configuration only has " +
        log"an effect when ${MDC(LogKeys.CONFIG2, PYTHON_USE_DAEMON.key)} " +
        log"is disabled or the platform is Windows.")
      value
    }.getOrElse("pyspark.worker")

  // TODO: support accumulator in multiple UDF
  protected val accumulator: PythonAccumulator = funcs.head.funcs.head.accumulator

  // Python accumulator is always set in production except in tests. See SPARK-27893
  private val maybeAccumulator: Option[PythonAccumulator] = Option(accumulator)

  // Expose a ServerSocket to support method calls via socket from Python side. Only relevant for
  // for tasks that are a part of barrier stage, refer [[BarrierTaskContext]] for details.
  private[spark] var serverSocket: Option[ServerSocket] = None

  // Authentication helper used when serving method calls via socket from Python side.
  private lazy val authHelper = new SocketAuthHelper(conf)

  // each python worker gets an equal part of the allocation. the worker pool will grow to the
  // number of concurrent tasks, which is determined by the number of cores in this executor.
  private def getWorkerMemoryMb(mem: Option[Long], cores: Int): Option[Long] = {
    mem.map(_ / cores)
  }

  def compute(
      inputIterator: Iterator[IN],
      partitionIndex: Int,
      context: TaskContext): Iterator[OUT] = {
    val startTime = System.currentTimeMillis
    val env = SparkEnv.get

    // Get the executor cores and pyspark memory, they are passed via the local properties when
    // the user specified them in a ResourceProfile.
    val execCoresProp = Option(context.getLocalProperty(EXECUTOR_CORES_LOCAL_PROPERTY))
    val memoryMb = Option(context.getLocalProperty(PYSPARK_MEMORY_LOCAL_PROPERTY)).map(_.toLong)
    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    // If OMP_NUM_THREADS is not explicitly set, override it with the number of task cpus.
    // See SPARK-42613 for details.
    if (conf.getOption("spark.executorEnv.OMP_NUM_THREADS").isEmpty) {
      envVars.put("OMP_NUM_THREADS", conf.get("spark.task.cpus", "1"))
    }
    envVars.put("SPARK_LOCAL_DIRS", localdir) // it's also used in monitor thread
    if (reuseWorker) {
      envVars.put("SPARK_REUSE_WORKER", "1")
    }
    if (simplifiedTraceback) {
      envVars.put("SPARK_SIMPLIFIED_TRACEBACK", "1")
    }
    // SPARK-30299 this could be wrong with standalone mode when executor
    // cores might not be correct because it defaults to all cores on the box.
    val execCores = execCoresProp.map(_.toInt).getOrElse(conf.get(EXECUTOR_CORES))
    val workerMemoryMb = getWorkerMemoryMb(memoryMb, execCores)
    if (workerMemoryMb.isDefined) {
      envVars.put("PYSPARK_EXECUTOR_MEMORY_MB", workerMemoryMb.get.toString)
    }
    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", authSocketTimeout.toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)
    if (faultHandlerEnabled) {
      envVars.put("PYTHON_FAULTHANDLER_DIR", BasePythonRunner.faultHandlerLogDir.toString)
    }
    // allow the user to set the batch size for the BatchedSerializer on UDFs
    envVars.put("PYTHON_UDF_BATCH_SIZE", batchSizeForPythonUDF.toString)

    envVars.put("SPARK_JOB_ARTIFACT_UUID", jobArtifactUUID.getOrElse("default"))

    val (worker: PythonWorker, pid: Option[Int]) = env.createPythonWorker(
      pythonExec, workerModule, daemonModule, envVars.asScala.toMap)
    // Whether is the worker released into idle pool or closed. When any codes try to release or
    // close a worker, they should use `releasedOrClosed.compareAndSet` to flip the state to make
    // sure there is only one winner that is going to release or close the worker.
    val releasedOrClosed = new AtomicBoolean(false)

    // Start a thread to feed the process input from our parent's iterator
    val writer = newWriter(env, worker, inputIterator, partitionIndex, context)

    context.addTaskCompletionListener[Unit] { _ =>
      if (!reuseWorker || releasedOrClosed.compareAndSet(false, true)) {
        try {
          worker.stop()
        } catch {
          case e: Exception =>
            logWarning("Failed to stop worker")
        }
      }
    }

    if (reuseWorker) {
      val key = (worker, context.taskAttemptId())
      // SPARK-35009: avoid creating multiple monitor threads for the same python worker
      // and task context
      if (PythonRunner.runningMonitorThreads.add(key)) {
        new MonitorThread(SparkEnv.get, worker, context).start()
      }
    } else {
      new MonitorThread(SparkEnv.get, worker, context).start()
    }

    // Return an iterator that read lines from the process's stdout
    val dataIn = new DataInputStream(
      new BufferedInputStream(new ReaderInputStream(worker, writer), bufferSize))
    val stdoutIterator = newReaderIterator(
      dataIn, writer, startTime, env, worker, pid, releasedOrClosed, context)
    new InterruptibleIterator(context, stdoutIterator)
  }

  protected def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[IN],
      partitionIndex: Int,
      context: TaskContext): Writer

  protected def newReaderIterator(
      stream: DataInputStream,
      writer: Writer,
      startTime: Long,
      env: SparkEnv,
      worker: PythonWorker,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[OUT]

  /**
   * Responsible for writing the data from the PythonRDD's parent iterator to the
   * Python process.
   */
  abstract class Writer(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[IN],
      partitionIndex: Int,
      context: TaskContext) {

    @volatile private var _exception: Throwable = _

    private val pythonIncludes = funcs.flatMap(_.funcs.flatMap(_.pythonIncludes.asScala)).toSet
    private val broadcastVars = funcs.flatMap(_.funcs.flatMap(_.broadcastVars.asScala))

    /** Contains the throwable thrown while writing the parent iterator to the Python process. */
    def exception: Option[Throwable] = Option(_exception)

    /**
     * Writes a command section to the stream connected to the Python worker.
     */
    protected def writeCommand(dataOut: DataOutputStream): Unit

    /**
     * Writes input data to the stream connected to the Python worker.
     * Returns true if any data was written to the stream, false if the input is exhausted.
     */
    def writeNextInputToStream(dataOut: DataOutputStream): Boolean

    def open(dataOut: DataOutputStream): Unit = Utils.logUncaughtExceptions {
      try {
        // Partition index
        dataOut.writeInt(partitionIndex)

        PythonWorkerUtils.writePythonVersion(pythonVer, dataOut)

        // Init a ServerSocket to accept method calls from Python side.
        val isBarrier = context.isInstanceOf[BarrierTaskContext]
        if (isBarrier) {
          serverSocket = Some(new ServerSocket(/* port */ 0,
            /* backlog */ 1,
            InetAddress.getByName("localhost")))
          // A call to accept() for ServerSocket shall block infinitely.
          serverSocket.foreach(_.setSoTimeout(0))
          new Thread("accept-connections") {
            setDaemon(true)

            override def run(): Unit = {
              while (!serverSocket.get.isClosed()) {
                var sock: Socket = null
                try {
                  sock = serverSocket.get.accept()
                  // Wait for function call from python side.
                  sock.setSoTimeout(10000)
                  authHelper.authClient(sock)
                  val input = new DataInputStream(sock.getInputStream())
                  val requestMethod = input.readInt()
                  // The BarrierTaskContext function may wait infinitely, socket shall not timeout
                  // before the function finishes.
                  sock.setSoTimeout(0)
                  requestMethod match {
                    case BarrierTaskContextMessageProtocol.BARRIER_FUNCTION =>
                      barrierAndServe(requestMethod, sock)
                    case BarrierTaskContextMessageProtocol.ALL_GATHER_FUNCTION =>
                      val message = PythonWorkerUtils.readUTF(input)
                      barrierAndServe(requestMethod, sock, message)
                    case _ =>
                      val out = new DataOutputStream(new BufferedOutputStream(
                        sock.getOutputStream))
                      writeUTF(BarrierTaskContextMessageProtocol.ERROR_UNRECOGNIZED_FUNCTION, out)
                  }
                } catch {
                  case e: SocketException if e.getMessage.contains("Socket closed") =>
                    // It is possible that the ServerSocket is not closed, but the native socket
                    // has already been closed, we shall catch and silently ignore this case.
                } finally {
                  if (sock != null) {
                    sock.close()
                  }
                }
              }
            }
          }.start()
        }
        val secret = if (isBarrier) {
          authHelper.secret
        } else {
          ""
        }
        if (isBarrier) {
          // Close ServerSocket on task completion.
          serverSocket.foreach { server =>
            context.addTaskCompletionListener[Unit](_ => server.close())
          }
          val boundPort: Int = serverSocket.map(_.getLocalPort).getOrElse(0)
          if (boundPort == -1) {
            val message = "ServerSocket failed to bind to Java side."
            logError(message)
            throw new SparkException(message)
          }
          logDebug(s"Started ServerSocket on port $boundPort.")
          dataOut.writeBoolean(/* isBarrier = */true)
          dataOut.writeInt(boundPort)
        } else {
          dataOut.writeBoolean(/* isBarrier = */false)
          dataOut.writeInt(0)
        }
        // Write out the TaskContextInfo
        val secretBytes = secret.getBytes(UTF_8)
        dataOut.writeInt(secretBytes.length)
        dataOut.write(secretBytes, 0, secretBytes.length)
        dataOut.writeInt(context.stageId())
        dataOut.writeInt(context.partitionId())
        dataOut.writeInt(context.attemptNumber())
        dataOut.writeLong(context.taskAttemptId())
        dataOut.writeInt(context.cpus())
        val resources = context.resources()
        dataOut.writeInt(resources.size)
        resources.foreach { case (k, v) =>
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v.name, dataOut)
          dataOut.writeInt(v.addresses.length)
          v.addresses.foreach { case addr =>
            PythonRDD.writeUTF(addr, dataOut)
          }
        }
        val localProps = context.getLocalProperties.asScala
        dataOut.writeInt(localProps.size)
        localProps.foreach { case (k, v) =>
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }

        PythonWorkerUtils.writeSparkFiles(jobArtifactUUID, pythonIncludes, dataOut)
        PythonWorkerUtils.writeBroadcasts(broadcastVars, worker, env, dataOut)

        dataOut.writeInt(evalType)
        writeCommand(dataOut)

        dataOut.flush()
      } catch {
        case t: Throwable if NonFatal(t) || t.isInstanceOf[Exception] =>
          if (context.isCompleted() || context.isInterrupted()) {
            logDebug("Exception/NonFatal Error thrown after task completion (likely due to " +
              "cleanup)", t)
            if (worker.channel.isConnected) {
              Utils.tryLog(worker.channel.shutdownOutput())
            }
          } else {
            // We must avoid throwing exceptions/NonFatals here, because the thread uncaught
            // exception handler will kill the whole executor (see
            // org.apache.spark.executor.Executor).
            _exception = t
            if (worker.channel.isConnected) {
              Utils.tryLog(worker.channel.shutdownOutput())
            }
          }
      }
    }

    def close(dataOut: DataOutputStream): Unit = {
      dataOut.writeInt(SpecialLengths.END_OF_STREAM)
      dataOut.flush()
    }

    /**
     * Gateway to call BarrierTaskContext methods.
     */
    def barrierAndServe(requestMethod: Int, sock: Socket, message: String = ""): Unit = {
      require(
        serverSocket.isDefined,
        "No available ServerSocket to redirect the BarrierTaskContext method call."
      )
      val out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream))
      try {
        val messages = requestMethod match {
          case BarrierTaskContextMessageProtocol.BARRIER_FUNCTION =>
            context.asInstanceOf[BarrierTaskContext].barrier()
            Array(BarrierTaskContextMessageProtocol.BARRIER_RESULT_SUCCESS)
          case BarrierTaskContextMessageProtocol.ALL_GATHER_FUNCTION =>
            context.asInstanceOf[BarrierTaskContext].allGather(message)
        }
        out.writeInt(messages.length)
        messages.foreach(writeUTF(_, out))
      } catch {
        case e: SparkException =>
          writeUTF(e.getMessage, out)
      } finally {
        out.close()
      }
    }

    def writeUTF(str: String, dataOut: DataOutputStream): Unit = {
      PythonWorkerUtils.writeUTF(str, dataOut)
    }
  }

  abstract class ReaderIterator(
      stream: DataInputStream,
      writer: Writer,
      startTime: Long,
      env: SparkEnv,
      worker: PythonWorker,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext)
    extends Iterator[OUT] {

    private var nextObj: OUT = _
    private var eos = false

    override def hasNext: Boolean = nextObj != null || {
      if (!eos) {
        nextObj = read()
        hasNext
      } else {
        false
      }
    }

    override def next(): OUT = {
      if (hasNext) {
        val obj = nextObj
        nextObj = null.asInstanceOf[OUT]
        obj
      } else {
        Iterator.empty.next()
      }
    }

    /**
     * Reads next object from the stream.
     * When the stream reaches end of data, needs to process the following sections,
     * and then returns null.
     */
    protected def read(): OUT

    protected def handleTimingData(): Unit = {
      // Timing data from worker
      val bootTime = stream.readLong()
      val initTime = stream.readLong()
      val finishTime = stream.readLong()
      val boot = bootTime - startTime
      val init = initTime - bootTime
      val finish = finishTime - initTime
      val total = finishTime - startTime
      logInfo(log"Times: total = ${MDC(LogKeys.TOTAL_TIME, total)}, " +
        log"boot = ${MDC(LogKeys.BOOT_TIME, boot)}, " +
        log"init = ${MDC(LogKeys.INIT_TIME, init)}, " +
        log"finish = ${MDC(LogKeys.FINISH_TIME, finish)}")
      metrics.get("pythonBootTime").foreach(_.add(boot))
      metrics.get("pythonInitTime").foreach(_.add(init))
      metrics.get("pythonTotalTime").foreach(_.add(total))
      val memoryBytesSpilled = stream.readLong()
      val diskBytesSpilled = stream.readLong()
      context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
      context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    }

    protected def handlePythonException(): PythonException = {
      // Signals that an exception has been thrown in python
      val msg = PythonWorkerUtils.readUTF(stream)
      new PythonException(msg, writer.exception.orNull)
    }

    protected def handleEndOfDataSection(): Unit = {
      // We've finished the data section of the output, but we can still
      // read some accumulator updates:
      PythonWorkerUtils.receiveAccumulatorUpdates(maybeAccumulator, stream)

      // Check whether the worker is ready to be re-used.
      if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
        if (reuseWorker && releasedOrClosed.compareAndSet(false, true)) {
          env.releasePythonWorker(
            pythonExec, workerModule, daemonModule, envVars.asScala.toMap, worker)
        }
      }
      eos = true
    }

    protected val handleException: PartialFunction[Throwable, OUT] = {
      case e: Exception if context.isInterrupted() =>
        logDebug("Exception thrown after task interruption", e)
        throw new TaskKilledException(context.getKillReason().getOrElse("unknown reason"))

      case e: Exception if writer.exception.isDefined =>
        logError("Python worker exited unexpectedly (crashed)", e)
        logError("This may have been caused by a prior exception:", writer.exception.get)
        throw writer.exception.get

      case e: IOException if faultHandlerEnabled && pid.isDefined &&
          JavaFiles.exists(BasePythonRunner.faultHandlerLogPath(pid.get)) =>
        val path = BasePythonRunner.faultHandlerLogPath(pid.get)
        val error = String.join("\n", JavaFiles.readAllLines(path)) + "\n"
        JavaFiles.deleteIfExists(path)
        throw new SparkException(s"Python worker exited unexpectedly (crashed): $error", e)

      case e: IOException if !faultHandlerEnabled =>
        throw new SparkException(
          s"Python worker exited unexpectedly (crashed). " +
            "Consider setting 'spark.sql.execution.pyspark.udf.faulthandler.enabled' or" +
            s"'${PYTHON_WORKER_FAULTHANLDER_ENABLED.key}' configuration to 'true' for " +
            "the better Python traceback.", e)

      case e: IOException =>
        throw new SparkException("Python worker exited unexpectedly (crashed)", e)
    }
  }

  /**
   * It is necessary to have a monitor thread for python workers if the user cancels with
   * interrupts disabled. In that case we will need to explicitly kill the worker, otherwise the
   * threads can block indefinitely.
   */
  class MonitorThread(env: SparkEnv, worker: PythonWorker, context: TaskContext)
    extends Thread(s"Worker Monitor for $pythonExec") {

    /** How long to wait before killing the python worker if a task cannot be interrupted. */
    private val taskKillTimeout = env.conf.get(PYTHON_TASK_KILL_TIMEOUT)

    setDaemon(true)

    private def monitorWorker(): Unit = {
      // Kill the worker if it is interrupted, checking until task completion.
      // TODO: This has a race condition if interruption occurs, as completed may still become true.
      while (!context.isInterrupted() && !context.isCompleted()) {
        Thread.sleep(2000)
      }
      if (!context.isCompleted()) {
        Thread.sleep(taskKillTimeout)
        if (!context.isCompleted()) {
          try {
            // Mimic the task name used in `Executor` to help the user find out the task to blame.
            val taskName = s"${context.partitionId()}.${context.attemptNumber()} " +
              s"in stage ${context.stageId()} (TID ${context.taskAttemptId()})"
            logWarning(log"Incomplete task ${MDC(TASK_NAME, taskName)} " +
              log"interrupted: Attempting to kill Python Worker")
            env.destroyPythonWorker(
              pythonExec, workerModule, daemonModule, envVars.asScala.toMap, worker)
          } catch {
            case e: Exception =>
              logError("Exception when trying to kill worker", e)
          }
        }
      }
    }

    override def run(): Unit = {
      try {
        monitorWorker()
      } finally {
        if (reuseWorker) {
          val key = (worker, context.taskAttemptId())
          PythonRunner.runningMonitorThreads.remove(key)
        }
      }
    }
  }

  class ReaderInputStream(worker: PythonWorker, writer: Writer) extends InputStream {
    private[this] var writerIfbhThreadLocalValue: Object = null
    private[this] val temp = new Array[Byte](1)
    private[this] val bufferStream = new DirectByteBufferOutputStream()
    /**
     * Buffers data to be written to the Python worker until the socket is
     * available for write.
     * A best-effort attempt is made to not grow the buffer beyond "spark.buffer.size". See
     * `writeAdditionalInputToPythonWorker()` for details.
     */
    private[this] var buffer: ByteBuffer = _
    private[this] var hasInput = true

    writer.open(new DataOutputStream(bufferStream))
    buffer = bufferStream.toByteBuffer

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
      // The code below manipulates the InputFileBlockHolder thread local in order
      // to prevent behavior changes in the input_file_name() expression due to the switch from
      // multi-threaded to single-threaded Python execution (SPARK-44705).
      //
      // Prior to that change, scan operations feeding into PythonRunner would be evaluated in
      // "writer" threads that were child threads of the main task thread. As a result, when
      // a scan operation hit end-of-input and called InputFileBlockHolder.unset(), the effects
      // of unset() would only occur in the writer thread and not the main task thread: this
      // meant that code "downstream" of a PythonRunner would continue to observe the writer's
      // last pre-unset() value (i.e. the last read filename).
      //
      // Switching to a single-threaded Python runner changed this behavior: now, unset() would
      // impact operators both upstream and downstream of the PythonRunner and this would cause
      // unset()'s effects to be immediately visible to downstream operators, in turn causing the
      // input_file_name() expression to return empty filenames in situations where it previously
      // would have returned the last non-empty filename.
      //
      // To avoid this behavior change, the code below simulates the behavior of the
      // InputFileBlockHolder's inheritable thread local:
      //
      //  - Detect whether code that previously would have run in the writer thread has changed
      //    the thread local value itself. Note that the thread local holds a mutable
      //    AtomicReference, so the thread local's value only changes objects when unset() is
      //    called.
      //  - If an object change was detected, then henceforth we will swap between the "main"
      //    and "writer" thread local values when context switching between upstream and
      //    downstream operator execution.
      //
      // This issue is subtle and several other alternative approaches were considered
      val buf = ByteBuffer.wrap(b, off, len)
      var n = 0
      while (n == 0) {
        worker.selector.select()
        if (worker.selectionKey.isReadable) {
          n = worker.channel.read(buf)
        }
        if (worker.selectionKey.isWritable) {
          val mainIfbhThreadLocalValue = InputFileBlockHolder.getThreadLocalValue()
          // Check whether the writer's thread local value has diverged from its parent's value:
          if (writerIfbhThreadLocalValue eq null) {
            // Default case (which is why it appears first): the writer's thread local value
            // is the same object as the main code, so no need to swap before executing the
            // writer code.
            try {
              // Execute the writer code:
              writeAdditionalInputToPythonWorker()
            } finally {
              // Check whether the writer code changed the thread local value:
              val maybeNewIfbh = InputFileBlockHolder.getThreadLocalValue()
              if (maybeNewIfbh ne mainIfbhThreadLocalValue) {
                // The writer thread change the thread local, so henceforth we need to
                // swap. Store the writer thread's value and restore the old main thread
                // value:
                writerIfbhThreadLocalValue = maybeNewIfbh
                InputFileBlockHolder.setThreadLocalValue(mainIfbhThreadLocalValue)
              }
            }
          } else {
            // The writer thread and parent thread have different values, so we must swap
            // them when switching between writer and parent code:
            try {
              // Swap in the writer value:
              InputFileBlockHolder.setThreadLocalValue(writerIfbhThreadLocalValue)
              try {
                // Execute the writer code:
                writeAdditionalInputToPythonWorker()
              } finally {
                // Store an updated writer thread value:
                writerIfbhThreadLocalValue = InputFileBlockHolder.getThreadLocalValue()
              }
            } finally {
              // Restore the main thread's value:
              InputFileBlockHolder.setThreadLocalValue(mainIfbhThreadLocalValue)
            }
          }
        }
      }
      n
    }

    private var lastFlushTime = System.nanoTime()

    /**
     * Returns false if `timelyFlushEnabled` is disabled.
     *
     * Otherwise, returns true if `buffer` should be flushed before any additional data is
     * written to it.
     * For small input rows the data might stay in the buffer for long before it is sent to the
     * Python worker. We should flush the buffer periodically so that the downstream can make
     * continued progress.
     */
    private def shouldFlush(): Boolean = {
      if (!timelyFlushEnabled) {
        false
      } else {
        val currentTime = System.nanoTime()
        if (currentTime - lastFlushTime > timelyFlushTimeoutNanos) {
          lastFlushTime = currentTime
          bufferStream.size() > 0
        } else {
          false
        }
      }
    }

    /**
     * Reads input data from `writer.inputIterator` into `buffer` and writes the buffer to the
     * Python worker if the socket is available for writing.
     */
    private def writeAdditionalInputToPythonWorker(): Unit = {
      var acceptsInput = true
      while (acceptsInput && (hasInput || buffer.hasRemaining)) {
        if (!buffer.hasRemaining && hasInput) {
          // No buffered data is available. Try to read input into the buffer.
          bufferStream.reset()
          // Set the `buffer` to null to make it eligible for GC
          buffer = null

          val dataOut = new DataOutputStream(bufferStream)
          // Try not to grow the buffer much beyond `bufferSize`. This is inevitable for large
          // input rows.
          while (bufferStream.size() < bufferSize && hasInput && !shouldFlush()) {
            hasInput = writer.writeNextInputToStream(dataOut)
          }
          if (!hasInput) {
            // Reached the end of the input.
            writer.close(dataOut)
          }
          buffer = bufferStream.toByteBuffer
        }

        // Try to write as much buffered data as possible to the socket.
        while (buffer.hasRemaining && acceptsInput) {
          val n = worker.channel.write(buffer)
          acceptsInput = n > 0
        }
      }

      if (!hasInput && !buffer.hasRemaining) {
        // We no longer have any data to write to the socket.
        worker.selectionKey.interestOps(SelectionKey.OP_READ)
        bufferStream.close()
      }
    }
  }

}

private[spark] object PythonRunner {

  // already running worker monitor threads for worker and task attempts ID pairs
  val runningMonitorThreads = ConcurrentHashMap.newKeySet[(PythonWorker, Long)]()

  private val printPythonInfo: AtomicBoolean = new AtomicBoolean(true)

  def apply(func: PythonFunction, jobArtifactUUID: Option[String]): PythonRunner = {
    if (printPythonInfo.compareAndSet(true, false)) {
      PythonUtils.logPythonInfo(func.pythonExec)
    }
    new PythonRunner(Seq(ChainedPythonFunctions(Seq(func))), jobArtifactUUID)
  }
}

/**
 * A helper class to run Python mapPartition in Spark.
 */
private[spark] class PythonRunner(
    funcs: Seq[ChainedPythonFunctions], jobArtifactUUID: Option[String])
  extends BasePythonRunner[Array[Byte], Array[Byte]](
    funcs, PythonEvalType.NON_UDF, Array(Array(0)), jobArtifactUUID, Map.empty) {

  protected override def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[Array[Byte]],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        PythonWorkerUtils.writePythonFunction(funcs.head.funcs.head, dataOut)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        if (PythonRDD.writeNextElementToStream(inputIterator, dataOut)) {
          true
        } else {
          dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
          false
        }
      }
    }
  }

  protected override def newReaderIterator(
      stream: DataInputStream,
      writer: Writer,
      startTime: Long,
      env: SparkEnv,
      worker: PythonWorker,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[Array[Byte]] = {
    new ReaderIterator(
      stream, writer, startTime, env, worker, pid, releasedOrClosed, context) {

      protected override def read(): Array[Byte] = {
        if (writer.exception.isDefined) {
          throw writer.exception.get
        }
        try {
          stream.readInt() match {
            case length if length >= 0 =>
              PythonWorkerUtils.readBytes(length, stream)
            case SpecialLengths.TIMING_DATA =>
              handleTimingData()
              read()
            case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
              throw handlePythonException()
            case SpecialLengths.END_OF_DATA_SECTION =>
              handleEndOfDataSection()
              null
          }
        } catch handleException
      }
    }
  }
}

private[spark] object SpecialLengths {
  val END_OF_DATA_SECTION = -1
  val PYTHON_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
  val END_OF_STREAM = -4
  val NULL = -5
  val START_ARROW_STREAM = -6
  val END_OF_MICRO_BATCH = -7
}

private[spark] object BarrierTaskContextMessageProtocol {
  val BARRIER_FUNCTION = 1
  val ALL_GATHER_FUNCTION = 2
  val BARRIER_RESULT_SUCCESS = "success"
  val ERROR_UNRECOGNIZED_FUNCTION = "Not recognized function call from python side."
}
