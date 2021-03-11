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
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{BUFFER_SIZE, EXECUTOR_CORES}
import org.apache.spark.internal.config.Python._
import org.apache.spark.resource.ResourceProfile.{EXECUTOR_CORES_LOCAL_PROPERTY, PYSPARK_MEMORY_LOCAL_PROPERTY}
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.util._


/**
 * Enumerate the type of command that will be sent to the Python worker
 */
private[spark] object PythonEvalType {
  val NON_UDF = 0

  val SQL_BATCHED_UDF = 100

  val SQL_SCALAR_PANDAS_UDF = 200
  val SQL_GROUPED_MAP_PANDAS_UDF = 201
  val SQL_GROUPED_AGG_PANDAS_UDF = 202
  val SQL_WINDOW_AGG_PANDAS_UDF = 203
  val SQL_SCALAR_PANDAS_ITER_UDF = 204
  val SQL_MAP_PANDAS_ITER_UDF = 205
  val SQL_COGROUPED_MAP_PANDAS_UDF = 206

  def toString(pythonEvalType: Int): String = pythonEvalType match {
    case NON_UDF => "NON_UDF"
    case SQL_BATCHED_UDF => "SQL_BATCHED_UDF"
    case SQL_SCALAR_PANDAS_UDF => "SQL_SCALAR_PANDAS_UDF"
    case SQL_GROUPED_MAP_PANDAS_UDF => "SQL_GROUPED_MAP_PANDAS_UDF"
    case SQL_GROUPED_AGG_PANDAS_UDF => "SQL_GROUPED_AGG_PANDAS_UDF"
    case SQL_WINDOW_AGG_PANDAS_UDF => "SQL_WINDOW_AGG_PANDAS_UDF"
    case SQL_SCALAR_PANDAS_ITER_UDF => "SQL_SCALAR_PANDAS_ITER_UDF"
    case SQL_MAP_PANDAS_ITER_UDF => "SQL_MAP_PANDAS_ITER_UDF"
    case SQL_COGROUPED_MAP_PANDAS_UDF => "SQL_COGROUPED_MAP_PANDAS_UDF"
  }
}

/**
 * A helper class to run Python mapPartition/UDFs in Spark.
 *
 * funcs is a list of independent Python functions, each one of them is a list of chained Python
 * functions (from bottom to top).
 */
private[spark] abstract class BasePythonRunner[IN, OUT](
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]])
  extends Logging {

  require(funcs.length == argOffsets.length, "argOffsets should have the same length as funcs")

  private val conf = SparkEnv.get.conf
  protected val bufferSize: Int = conf.get(BUFFER_SIZE)
  protected val authSocketTimeout = conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)
  private val reuseWorker = conf.get(PYTHON_WORKER_REUSE)
  protected val simplifiedTraceback: Boolean = false

  // All the Python functions should have the same exec, version and envvars.
  protected val envVars: java.util.Map[String, String] = funcs.head.funcs.head.envVars
  protected val pythonExec: String = funcs.head.funcs.head.pythonExec
  protected val pythonVer: String = funcs.head.funcs.head.pythonVer

  // TODO: support accumulator in multiple UDF
  protected val accumulator: PythonAccumulatorV2 = funcs.head.funcs.head.accumulator

  // Python accumulator is always set in production except in tests. See SPARK-27893
  private val maybeAccumulator: Option[PythonAccumulatorV2] = Option(accumulator)

  // Expose a ServerSocket to support method calls via socket from Python side.
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
    // if OMP_NUM_THREADS is not explicitly set, override it with the number of cores
    if (conf.getOption("spark.executorEnv.OMP_NUM_THREADS").isEmpty) {
      // SPARK-28843: limit the OpenMP thread pool to the number of cores assigned to this executor
      // this avoids high memory consumption with pandas/numpy because of a large OpenMP thread pool
      // see https://github.com/numpy/numpy/issues/10455
      execCoresProp.foreach(envVars.put("OMP_NUM_THREADS", _))
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
    val worker: Socket = env.createPythonWorker(pythonExec, envVars.asScala.toMap)
    // Whether is the worker released into idle pool or closed. When any codes try to release or
    // close a worker, they should use `releasedOrClosed.compareAndSet` to flip the state to make
    // sure there is only one winner that is going to release or close the worker.
    val releasedOrClosed = new AtomicBoolean(false)

    // Start a thread to feed the process input from our parent's iterator
    val writerThread = newWriterThread(env, worker, inputIterator, partitionIndex, context)

    context.addTaskCompletionListener[Unit] { _ =>
      writerThread.shutdownOnTaskCompletion()
      if (!reuseWorker || releasedOrClosed.compareAndSet(false, true)) {
        try {
          worker.close()
        } catch {
          case e: Exception =>
            logWarning("Failed to close worker socket", e)
        }
      }
    }

    writerThread.start()
    new MonitorThread(env, worker, context).start()

    // Return an iterator that read lines from the process's stdout
    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))

    val stdoutIterator = newReaderIterator(
      stream, writerThread, startTime, env, worker, releasedOrClosed, context)
    new InterruptibleIterator(context, stdoutIterator)
  }

  protected def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[IN],
      partitionIndex: Int,
      context: TaskContext): WriterThread

  protected def newReaderIterator(
      stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[OUT]

  /**
   * The thread responsible for writing the data from the PythonRDD's parent iterator to the
   * Python process.
   */
  abstract class WriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[IN],
      partitionIndex: Int,
      context: TaskContext)
    extends Thread(s"stdout writer for $pythonExec") {

    @volatile private var _exception: Throwable = null

    private val pythonIncludes = funcs.flatMap(_.funcs.flatMap(_.pythonIncludes.asScala)).toSet
    private val broadcastVars = funcs.flatMap(_.funcs.flatMap(_.broadcastVars.asScala))

    setDaemon(true)

    /** Contains the throwable thrown while writing the parent iterator to the Python process. */
    def exception: Option[Throwable] = Option(_exception)

    /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
    def shutdownOnTaskCompletion(): Unit = {
      assert(context.isCompleted)
      this.interrupt()
    }

    /**
     * Writes a command section to the stream connected to the Python worker.
     */
    protected def writeCommand(dataOut: DataOutputStream): Unit

    /**
     * Writes input data to the stream connected to the Python worker.
     */
    protected def writeIteratorToStream(dataOut: DataOutputStream): Unit

    override def run(): Unit = Utils.logUncaughtExceptions {
      try {
        TaskContext.setTaskContext(context)
        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(partitionIndex)
        // Python version of driver
        PythonRDD.writeUTF(pythonVer, dataOut)
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
                      val length = input.readInt()
                      val message = new Array[Byte](length)
                      input.readFully(message)
                      barrierAndServe(requestMethod, sock, new String(message, UTF_8))
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
        // Close ServerSocket on task completion.
        serverSocket.foreach { server =>
          context.addTaskCompletionListener[Unit](_ => server.close())
        }
        val boundPort: Int = serverSocket.map(_.getLocalPort).getOrElse(0)
        if (boundPort == -1) {
          val message = "ServerSocket failed to bind to Java side."
          logError(message)
          throw new SparkException(message)
        } else if (isBarrier) {
          logDebug(s"Started ServerSocket on port $boundPort.")
        }
        // Write out the TaskContextInfo
        dataOut.writeBoolean(isBarrier)
        dataOut.writeInt(boundPort)
        val secretBytes = secret.getBytes(UTF_8)
        dataOut.writeInt(secretBytes.length)
        dataOut.write(secretBytes, 0, secretBytes.length)
        dataOut.writeInt(context.stageId())
        dataOut.writeInt(context.partitionId())
        dataOut.writeInt(context.attemptNumber())
        dataOut.writeLong(context.taskAttemptId())
        val resources = context.resources()
        dataOut.writeInt(resources.size)
        resources.foreach { case (k, v) =>
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v.name, dataOut)
          dataOut.writeInt(v.addresses.size)
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

        // sparkFilesDir
        PythonRDD.writeUTF(SparkFiles.getRootDirectory(), dataOut)
        // Python includes (*.zip and *.egg files)
        dataOut.writeInt(pythonIncludes.size)
        for (include <- pythonIncludes) {
          PythonRDD.writeUTF(include, dataOut)
        }
        // Broadcast variables
        val oldBids = PythonRDD.getWorkerBroadcasts(worker)
        val newBids = broadcastVars.map(_.id).toSet
        // number of different broadcasts
        val toRemove = oldBids.diff(newBids)
        val addedBids = newBids.diff(oldBids)
        val cnt = toRemove.size + addedBids.size
        val needsDecryptionServer = env.serializerManager.encryptionEnabled && addedBids.nonEmpty
        dataOut.writeBoolean(needsDecryptionServer)
        dataOut.writeInt(cnt)
        def sendBidsToRemove(): Unit = {
          for (bid <- toRemove) {
            // remove the broadcast from worker
            dataOut.writeLong(-bid - 1) // bid >= 0
            oldBids.remove(bid)
          }
        }
        if (needsDecryptionServer) {
          // if there is encryption, we setup a server which reads the encrypted files, and sends
          // the decrypted data to python
          val idsAndFiles = broadcastVars.flatMap { broadcast =>
            if (!oldBids.contains(broadcast.id)) {
              Some((broadcast.id, broadcast.value.path))
            } else {
              None
            }
          }
          val server = new EncryptedPythonBroadcastServer(env, idsAndFiles)
          dataOut.writeInt(server.port)
          logTrace(s"broadcast decryption server setup on ${server.port}")
          PythonRDD.writeUTF(server.secret, dataOut)
          sendBidsToRemove()
          idsAndFiles.foreach { case (id, _) =>
            // send new broadcast
            dataOut.writeLong(id)
            oldBids.add(id)
          }
          dataOut.flush()
          logTrace("waiting for python to read decrypted broadcast data from server")
          server.waitTillBroadcastDataSent()
          logTrace("done sending decrypted data to python")
        } else {
          sendBidsToRemove()
          for (broadcast <- broadcastVars) {
            if (!oldBids.contains(broadcast.id)) {
              // send new broadcast
              dataOut.writeLong(broadcast.id)
              PythonRDD.writeUTF(broadcast.value.path, dataOut)
              oldBids.add(broadcast.id)
            }
          }
        }
        dataOut.flush()

        dataOut.writeInt(evalType)
        writeCommand(dataOut)
        writeIteratorToStream(dataOut)

        dataOut.writeInt(SpecialLengths.END_OF_STREAM)
        dataOut.flush()
      } catch {
        case t: Throwable if (NonFatal(t) || t.isInstanceOf[Exception]) =>
          if (context.isCompleted || context.isInterrupted) {
            logDebug("Exception/NonFatal Error thrown after task completion (likely due to " +
              "cleanup)", t)
            if (!worker.isClosed) {
              Utils.tryLog(worker.shutdownOutput())
            }
          } else {
            // We must avoid throwing exceptions/NonFatals here, because the thread uncaught
            // exception handler will kill the whole executor (see
            // org.apache.spark.executor.Executor).
            _exception = t
            if (!worker.isClosed) {
              Utils.tryLog(worker.shutdownOutput())
            }
          }
      }
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
      val bytes = str.getBytes(UTF_8)
      dataOut.writeInt(bytes.length)
      dataOut.write(bytes)
    }
  }

  abstract class ReaderIterator(
      stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
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
      logInfo("Times: total = %s, boot = %s, init = %s, finish = %s".format(total, boot,
        init, finish))
      val memoryBytesSpilled = stream.readLong()
      val diskBytesSpilled = stream.readLong()
      context.taskMetrics.incMemoryBytesSpilled(memoryBytesSpilled)
      context.taskMetrics.incDiskBytesSpilled(diskBytesSpilled)
    }

    protected def handlePythonException(): PythonException = {
      // Signals that an exception has been thrown in python
      val exLength = stream.readInt()
      val obj = new Array[Byte](exLength)
      stream.readFully(obj)
      new PythonException(new String(obj, StandardCharsets.UTF_8),
        writerThread.exception.getOrElse(null))
    }

    protected def handleEndOfDataSection(): Unit = {
      // We've finished the data section of the output, but we can still
      // read some accumulator updates:
      val numAccumulatorUpdates = stream.readInt()
      (1 to numAccumulatorUpdates).foreach { _ =>
        val updateLen = stream.readInt()
        val update = new Array[Byte](updateLen)
        stream.readFully(update)
        maybeAccumulator.foreach(_.add(update))
      }
      // Check whether the worker is ready to be re-used.
      if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
        if (reuseWorker && releasedOrClosed.compareAndSet(false, true)) {
          env.releasePythonWorker(pythonExec, envVars.asScala.toMap, worker)
        }
      }
      eos = true
    }

    protected val handleException: PartialFunction[Throwable, OUT] = {
      case e: Exception if context.isInterrupted =>
        logDebug("Exception thrown after task interruption", e)
        throw new TaskKilledException(context.getKillReason().getOrElse("unknown reason"))

      case e: Exception if writerThread.exception.isDefined =>
        logError("Python worker exited unexpectedly (crashed)", e)
        logError("This may have been caused by a prior exception:", writerThread.exception.get)
        throw writerThread.exception.get

      case eof: EOFException =>
        throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
    }
  }

  /**
   * It is necessary to have a monitor thread for python workers if the user cancels with
   * interrupts disabled. In that case we will need to explicitly kill the worker, otherwise the
   * threads can block indefinitely.
   */
  class MonitorThread(env: SparkEnv, worker: Socket, context: TaskContext)
    extends Thread(s"Worker Monitor for $pythonExec") {

    /** How long to wait before killing the python worker if a task cannot be interrupted. */
    private val taskKillTimeout = env.conf.get(PYTHON_TASK_KILL_TIMEOUT)

    setDaemon(true)

    override def run(): Unit = {
      // Kill the worker if it is interrupted, checking until task completion.
      // TODO: This has a race condition if interruption occurs, as completed may still become true.
      while (!context.isInterrupted && !context.isCompleted) {
        Thread.sleep(2000)
      }
      if (!context.isCompleted) {
        Thread.sleep(taskKillTimeout)
        if (!context.isCompleted) {
          try {
            // Mimic the task name used in `Executor` to help the user find out the task to blame.
            val taskName = s"${context.partitionId}.${context.attemptNumber} " +
              s"in stage ${context.stageId} (TID ${context.taskAttemptId})"
            logWarning(s"Incomplete task $taskName interrupted: Attempting to kill Python Worker")
            env.destroyPythonWorker(pythonExec, envVars.asScala.toMap, worker)
          } catch {
            case e: Exception =>
              logError("Exception when trying to kill worker", e)
          }
        }
      }
    }
  }
}

private[spark] object PythonRunner {

  def apply(func: PythonFunction): PythonRunner = {
    new PythonRunner(Seq(ChainedPythonFunctions(Seq(func))))
  }
}

/**
 * A helper class to run Python mapPartition in Spark.
 */
private[spark] class PythonRunner(funcs: Seq[ChainedPythonFunctions])
  extends BasePythonRunner[Array[Byte], Array[Byte]](
    funcs, PythonEvalType.NON_UDF, Array(Array(0))) {

  protected override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[Array[Byte]],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        val command = funcs.head.funcs.head.command
        dataOut.writeInt(command.length)
        dataOut.write(command.toArray)
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        PythonRDD.writeIteratorToStream(inputIterator, dataOut)
        dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
      }
    }
  }

  protected override def newReaderIterator(
      stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[Array[Byte]] = {
    new ReaderIterator(stream, writerThread, startTime, env, worker, releasedOrClosed, context) {

      protected override def read(): Array[Byte] = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          stream.readInt() match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
              obj
            case 0 => Array.emptyByteArray
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
}

private[spark] object BarrierTaskContextMessageProtocol {
  val BARRIER_FUNCTION = 1
  val ALL_GATHER_FUNCTION = 2
  val BARRIER_RESULT_SUCCESS = "success"
  val ERROR_UNRECOGNIZED_FUNCTION = "Not recognized function call from python side."
}
