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

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.util._


/**
 * Enumerate the type of command that will be sent to the Python worker
 */
private[spark] object PythonEvalType {
  val NON_UDF = 0
  val SQL_BATCHED_UDF = 1
  val SQL_PANDAS_UDF = 2
}

/**
 * A helper class to run Python mapPartition/UDFs in Spark.
 *
 * funcs is a list of independent Python functions, each one of them is a list of chained Python
 * functions (from bottom to top).
 */
private[spark] abstract class PythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    bufferSize: Int,
    reuseWorker: Boolean,
    evalType: Int,
    argOffsets: Array[Array[Int]])
  extends Logging {

  require(funcs.length == argOffsets.length, "argOffsets should have the same length as funcs")

  // All the Python functions should have the same exec, version and envvars.
  protected val envVars = funcs.head.funcs.head.envVars
  protected val pythonExec = funcs.head.funcs.head.pythonExec
  protected val pythonVer = funcs.head.funcs.head.pythonVer

  // TODO: support accumulator in multiple UDF
  protected val accumulator = funcs.head.funcs.head.accumulator

  def compute(
      inputIterator: Iterator[_],
      partitionIndex: Int,
      context: TaskContext): Iterator[Array[Byte]] = {
    val startTime = System.currentTimeMillis
    val env = SparkEnv.get
    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir) // it's also used in monitor thread
    if (reuseWorker) {
      envVars.put("SPARK_REUSE_WORKER", "1")
    }
    val worker: Socket = env.createPythonWorker(pythonExec, envVars.asScala.toMap)
    // Whether is the worker released into idle pool
    @volatile var released = false

    // Start a thread to feed the process input from our parent's iterator
    val writerThread = newWriterThread(env, worker, inputIterator, partitionIndex, context)

    context.addTaskCompletionListener { context =>
      writerThread.shutdownOnTaskCompletion()
      if (!reuseWorker || !released) {
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
    val stdoutIterator = new Iterator[Array[Byte]] {
      override def next(): Array[Byte] = {
        val obj = _nextObj
        if (hasNext) {
          _nextObj = read()
        }
        obj
      }

      private def read(): Array[Byte] = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          stream.readInt() match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
              obj
            case 0 => Array.empty[Byte]
            case SpecialLengths.TIMING_DATA =>
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
              read()
            case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
              // Signals that an exception has been thrown in python
              val exLength = stream.readInt()
              val obj = new Array[Byte](exLength)
              stream.readFully(obj)
              throw new PythonException(new String(obj, StandardCharsets.UTF_8),
                writerThread.exception.getOrElse(null))
            case SpecialLengths.END_OF_DATA_SECTION =>
              // We've finished the data section of the output, but we can still
              // read some accumulator updates:
              val numAccumulatorUpdates = stream.readInt()
              (1 to numAccumulatorUpdates).foreach { _ =>
                val updateLen = stream.readInt()
                val update = new Array[Byte](updateLen)
                stream.readFully(update)
                accumulator.add(update)
              }
              // Check whether the worker is ready to be re-used.
              if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
                if (reuseWorker) {
                  env.releasePythonWorker(pythonExec, envVars.asScala.toMap, worker)
                  released = true
                }
              }
              null
          }
        } catch {

          case e: Exception if context.isInterrupted =>
            logDebug("Exception thrown after task interruption", e)
            throw new TaskKilledException(context.getKillReason().getOrElse("unknown reason"))

          case e: Exception if env.isStopped =>
            logDebug("Exception thrown after context is stopped", e)
            null  // exit silently

          case e: Exception if writerThread.exception.isDefined =>
            logError("Python worker exited unexpectedly (crashed)", e)
            logError("This may have been caused by a prior exception:", writerThread.exception.get)
            throw writerThread.exception.get

          case eof: EOFException =>
            throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
        }
      }

      var _nextObj = read()

      override def hasNext: Boolean = _nextObj != null
    }
    new InterruptibleIterator(context, stdoutIterator)
  }

  def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[_],
      partitionIndex: Int,
      context: TaskContext): WriterThread

  /**
   * The thread responsible for writing the data from the PythonRDD's parent iterator to the
   * Python process.
   */
  abstract class WriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[_],
      partitionIndex: Int,
      context: TaskContext)
    extends Thread(s"stdout writer for $pythonExec") {

    @volatile private var _exception: Exception = null

    private val pythonIncludes = funcs.flatMap(_.funcs.flatMap(_.pythonIncludes.asScala)).toSet
    private val broadcastVars = funcs.flatMap(_.funcs.flatMap(_.broadcastVars.asScala))

    setDaemon(true)

    /** Contains the exception thrown while writing the parent iterator to the Python process. */
    def exception: Option[Exception] = Option(_exception)

    /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
    def shutdownOnTaskCompletion() {
      assert(context.isCompleted)
      this.interrupt()
    }

    def writeCommand(dataOut: DataOutputStream): Unit
    def writeIteratorToStream(dataOut: DataOutputStream): Unit

    override def run(): Unit = Utils.logUncaughtExceptions {
      try {
        TaskContext.setTaskContext(context)
        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(partitionIndex)
        // Python version of driver
        PythonRDD.writeUTF(pythonVer, dataOut)
        // Write out the TaskContextInfo
        dataOut.writeInt(context.stageId())
        dataOut.writeInt(context.partitionId())
        dataOut.writeInt(context.attemptNumber())
        dataOut.writeLong(context.taskAttemptId())
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
        val cnt = toRemove.size + newBids.diff(oldBids).size
        dataOut.writeInt(cnt)
        for (bid <- toRemove) {
          // remove the broadcast from worker
          dataOut.writeLong(- bid - 1)  // bid >= 0
          oldBids.remove(bid)
        }
        for (broadcast <- broadcastVars) {
          if (!oldBids.contains(broadcast.id)) {
            // send new broadcast
            dataOut.writeLong(broadcast.id)
            PythonRDD.writeUTF(broadcast.value.path, dataOut)
            oldBids.add(broadcast.id)
          }
        }
        dataOut.flush()

        dataOut.writeInt(evalType)
        writeCommand(dataOut)
        writeIteratorToStream(dataOut)

        dataOut.writeInt(SpecialLengths.END_OF_STREAM)
        dataOut.flush()
      } catch {
        case e: Exception if context.isCompleted || context.isInterrupted =>
          logDebug("Exception thrown after task completion (likely due to cleanup)", e)
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }

        case e: Exception =>
          // We must avoid throwing exceptions here, because the thread uncaught exception handler
          // will kill the whole executor (see org.apache.spark.executor.Executor).
          _exception = e
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }
      }
    }
  }

  /**
   * It is necessary to have a monitor thread for python workers if the user cancels with
   * interrupts disabled. In that case we will need to explicitly kill the worker, otherwise the
   * threads can block indefinitely.
   */
  class MonitorThread(env: SparkEnv, worker: Socket, context: TaskContext)
    extends Thread(s"Worker Monitor for $pythonExec") {

    setDaemon(true)

    override def run() {
      // Kill the worker if it is interrupted, checking until task completion.
      // TODO: This has a race condition if interruption occurs, as completed may still become true.
      while (!context.isInterrupted && !context.isCompleted) {
        Thread.sleep(2000)
      }
      if (!context.isCompleted) {
        try {
          logWarning("Incomplete task interrupted: Attempting to kill Python Worker")
          env.destroyPythonWorker(pythonExec, envVars.asScala.toMap, worker)
        } catch {
          case e: Exception =>
            logError("Exception when trying to kill worker", e)
        }
      }
    }
  }
}

private[spark] object PythonCommandRunner {

  def apply(func: PythonFunction, bufferSize: Int, reuseWorker: Boolean): PythonCommandRunner = {
    new PythonCommandRunner(Seq(ChainedPythonFunctions(Seq(func))), bufferSize, reuseWorker)
  }
}

/**
 * A helper class to run Python mapPartition in Spark.
 */
private[spark] class PythonCommandRunner(
    funcs: Seq[ChainedPythonFunctions],
    bufferSize: Int,
    reuseWorker: Boolean)
  extends PythonRunner(funcs, bufferSize, reuseWorker, PythonEvalType.NON_UDF, Array(Array(0))) {

  override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[_],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      override def writeCommand(dataOut: DataOutputStream): Unit = {
        val command = funcs.head.funcs.head.command
        dataOut.writeInt(command.length)
        dataOut.write(command)
      }

      override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        PythonRDD.writeIteratorToStream(inputIterator, dataOut)
        dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
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
}
