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

package org.apache.spark.graphx.api.python

import java.io._
import java.net.Socket
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap, Collections}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.PythonRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.util.Utils

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

private[python] class PythonVertexRDD[VD: ClassTag](
    parent: JavaRDD[Array[Byte]],
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    preservePartitoning: Boolean,
    pythonExec: String,
    broadcastVars: JList[Broadcast[Array[Byte]]],
    accumulator: Accumulator[JList[Array[Byte]]],
    targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends VertexRDD[VD](parent.firstParent, targetStorageLevel) {

  import PythonVertexRDD._

  val bufferSize = conf.getInt("spark.buffer.size", DEFAULT_SPARK_BUFFER_SIZE)
  val reuse_worker = conf.getBoolean("spark.python.worker.reuse", true)

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val startTime = System.currentTimeMillis
    val env = SparkEnv.get
    val localdir = env.blockManager.diskBlockManager.localDirs.map(
      f => f.getPath()).mkString(",")
    envVars += ("SPARK_LOCAL_DIRS" -> localdir) // it's also used in monitor thread
    if (reuse_worker) {
      envVars += ("SPARK_REUSE_WORKER" -> "1")
    }
    val worker: Socket = env.createPythonWorker(pythonExec, envVars.toMap)

    // Start a thread to feed the process input from our parent's iterator
    val writerThread = new WriterThread(env, worker, split, context)

    var complete_cleanly = false
    context.addTaskCompletionListener { context =>
      writerThread.shutdownOnTaskCompletion()
      if (reuse_worker && complete_cleanly) {
        env.releasePythonWorker(pythonExec, envVars.toMap, worker)
      } else {
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
      def next(): Array[Byte] = {
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
              context.taskMetrics.memoryBytesSpilled += memoryBytesSpilled
              context.taskMetrics.diskBytesSpilled += diskBytesSpilled
              read()
            case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
              // Signals that an exception has been thrown in python
              val exLength = stream.readInt()
              val obj = new Array[Byte](exLength)
              stream.readFully(obj)
              throw new PythonException(new String(obj, "utf-8"),
                writerThread.exception.getOrElse(null))
            case SpecialLengths.END_OF_DATA_SECTION =>
              // We've finished the data section of the output, but we can still
              // read some accumulator updates:
              val numAccumulatorUpdates = stream.readInt()
              (1 to numAccumulatorUpdates).foreach { _ =>
                val updateLen = stream.readInt()
                val update = new Array[Byte](updateLen)
                stream.readFully(update)
                accumulator += Collections.singletonList(update)
              }
              complete_cleanly = true
              null
          }
        } catch {

          case e: Exception if context.isInterrupted =>
            logDebug("Exception thrown after task interruption", e)
            throw new TaskKilledException

          case e: Exception if writerThread.exception.isDefined =>
            logError("Python worker exited unexpectedly (crashed)", e)
            logError("This may have been caused by a prior exception:", writerThread.exception.get)
            throw writerThread.exception.get

          case eof: EOFException =>
            throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
        }
      }

      var _nextObj = read()

      def hasNext = _nextObj != null
    }
    new InterruptibleIterator(context, stdoutIterator)
//    override def compute(part: Partition, context: TaskContext): Iterator[(VertexId, VD)] = {
//      firstParent[ShippableVertexPartition[VD]].iterator(part, context).next.iterator
//    }
  }

  val asJavaRDD : JavaRDD[Array[Byte]] = this.parent

  /**
   * The thread responsible for writing the data from the PythonRDD's parent iterator to the
   * Python process.
   */
  class WriterThread(env: SparkEnv, worker: Socket, split: Partition, context: TaskContext)
    extends Thread(s"stdout writer for $pythonExec") {

    @volatile private var _exception: Exception = null

    setDaemon(true)

    /** Contains the exception thrown while writing the parent iterator to the Python process. */
    def exception: Option[Exception] = Option(_exception)

    /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
    def shutdownOnTaskCompletion() {
      assert(context.isCompleted)
      this.interrupt()
    }

    override def run(): Unit = Utils.logUncaughtExceptions {
      try {
        SparkEnv.set(env)
        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(split.index)
        // sparkFilesDir
        PythonRDD.writeUTF(SparkFiles.getRootDirectory, dataOut)
        // Python includes (*.zip and *.egg files)
        dataOut.writeInt(pythonIncludes.length)
        for (include <- pythonIncludes) {
          PythonRDD.writeUTF(include, dataOut)
        }
        // Broadcast variables
        val oldBids = PythonRDD.getWorkerBroadcasts(worker)
        val newBids = broadcastVars.map(_.id).toSet
        // number of different broadcasts
        val cnt = oldBids.diff(newBids).size + newBids.diff(oldBids).size
        dataOut.writeInt(cnt)
        for (bid <- oldBids) {
          if (!newBids.contains(bid)) {
            // remove the broadcast from worker
            dataOut.writeLong(- bid - 1)  // bid >= 0
            oldBids.remove(bid)
          }
        }
        for (broadcast <- broadcastVars) {
          if (!oldBids.contains(broadcast.id)) {
            // send new broadcast
            dataOut.writeLong(broadcast.id)
            dataOut.writeInt(broadcast.value.length)
            dataOut.write(broadcast.value)
            oldBids.add(broadcast.id)
          }
        }
        dataOut.flush()
        // Serialized command:
        dataOut.writeInt(command.length)
        dataOut.write(command)
        // Data values
        PythonRDD.writeIteratorToStream(parent.iterator(split, context), dataOut)
        dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
        dataOut.flush()
      } catch {
        case e: Exception if context.isCompleted || context.isInterrupted =>
          logDebug("Exception thrown after task completion (likely due to cleanup)", e)
          worker.shutdownOutput()

        case e: Exception =>
          // We must avoid throwing exceptions here, because the thread uncaught exception handler
          // will kill the whole executor (see org.apache.spark.executor.Executor).
          _exception = e
          worker.shutdownOutput()
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
          env.destroyPythonWorker(pythonExec, envVars.toMap, worker)
        } catch {
          case e: Exception =>
            logError("Exception when trying to kill worker", e)
        }
      }
    }
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  private object SpecialLengths {
    val END_OF_DATA_SECTION = -1
    val PYTHON_EXCEPTION_THROWN = -2
    val TIMING_DATA = -3
  }
}

/** Thrown for exceptions in user Python code. */
private class PythonException(msg: String, cause: Exception) extends RuntimeException(msg, cause)

object PythonVertexRDD {
  val DEFAULT_SPARK_BUFFER_SIZE = 65536
}
