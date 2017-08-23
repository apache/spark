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

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.stream.{ArrowStreamReader, ArrowStreamWriter}

import org.apache.spark.{SparkEnv, SparkFiles, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType, PythonException, PythonRDD, SpecialLengths}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.execution.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Similar to `PythonRunner`, but exchange data with Python worker via columnar format.
 */
class VectorizedPythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    batchSize: Int,
    bufferSize: Int,
    reuse_worker: Boolean,
    argOffsets: Array[Array[Int]]) extends Logging {

  require(funcs.length == argOffsets.length, "argOffsets should have the same length as funcs")

  // All the Python functions should have the same exec, version and envvars.
  private val envVars = funcs.head.funcs.head.envVars
  private val pythonExec = funcs.head.funcs.head.pythonExec
  private val pythonVer = funcs.head.funcs.head.pythonVer

  // TODO: support accumulator in multiple UDF
  private val accumulator = funcs.head.funcs.head.accumulator

  // todo: return column batch?
  def compute(
      inputRows: Iterator[InternalRow],
      schema: StructType,
      partitionIndex: Int,
      context: TaskContext): Iterator[InternalRow] = {
    val startTime = System.currentTimeMillis
    val env = SparkEnv.get
    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir) // it's also used in monitor thread
    if (reuse_worker) {
      envVars.put("SPARK_REUSE_WORKER", "1")
    }
    val worker: Socket = env.createPythonWorker(pythonExec, envVars.asScala.toMap)
    // Whether is the worker released into idle pool
    @volatile var released = false

    // Start a thread to feed the process input from our parent's iterator
    val writerThread = new WriterThread(
      env, worker, inputRows, schema, partitionIndex, context)

    context.addTaskCompletionListener { context =>
      writerThread.shutdownOnTaskCompletion()
      if (!reuse_worker || !released) {
        try {
          worker.close()
        } catch {
          case e: Exception =>
            logWarning("Failed to close worker socket", e)
        }
      }
    }

    writerThread.start()

    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))

    val allocator = ArrowUtils.rootAllocator.newChildAllocator(
      s"stdin reader for $pythonExec", 0, Long.MaxValue)
    val reader = new ArrowStreamReader(stream, allocator)

    new Iterator[InternalRow] {
      private val root = reader.getVectorSchemaRoot
      private val vectors = root.getFieldVectors.asScala.map { vector =>
        new ArrowColumnVector(vector)
      }.toArray[ColumnVector]

      var closed = false

      context.addTaskCompletionListener { _ =>
        // todo: we need something like `read.end()`, which release all the resources, but leave
        // the input stream open. `reader.close` will close the socket and we can't reuse worker.
        // So here we simply not close the reader, which is problematic.
        if (!closed) {
          root.close()
          allocator.close()
        }
      }

      private[this] var batchLoaded = true
      private[this] var currentIter: Iterator[InternalRow] = Iterator.empty

      override def hasNext: Boolean = batchLoaded && (currentIter.hasNext || loadNextBatch()) || {
        root.close()
        allocator.close()
        closed = true
        false
      }

      private def loadNextBatch(): Boolean = {
        batchLoaded = reader.loadNextBatch()
        if (batchLoaded) {
          val batch = new ColumnarBatch(schema, vectors, root.getRowCount)
          batch.setNumRows(root.getRowCount)
          currentIter = batch.rowIterator().asScala
        } else {
          // end of arrow batches, handle some special signal
          val signal = stream.readInt()
          if (signal == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
            val exLength = stream.readInt()
            val obj = new Array[Byte](exLength)
            stream.readFully(obj)
            throw new PythonException(new String(obj, StandardCharsets.UTF_8),
              writerThread.exception.getOrElse(null))
          }

          assert(signal == SpecialLengths.TIMING_DATA)
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

          assert(stream.readInt() == SpecialLengths.END_OF_DATA_SECTION)
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
            if (reuse_worker) {
              env.releasePythonWorker(pythonExec, envVars.asScala.toMap, worker)
              released = true
            }
          }
        }
        hasNext // skip empty batches if any
      }

      override def next(): InternalRow = {
        currentIter.next()
      }
    }
  }

  class WriterThread(
      env: SparkEnv,
      worker: Socket,
      inputRows: Iterator[InternalRow],
      schema: StructType,
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

        // SQL_VECTORIZED_UDF means arrow mode
        dataOut.writeInt(PythonEvalType.SQL_VECTORIZED_UDF)
        dataOut.writeInt(funcs.length)
        funcs.zip(argOffsets).foreach { case (chained, offsets) =>
          dataOut.writeInt(offsets.length)
          offsets.foreach(dataOut.writeInt)
          dataOut.writeInt(chained.funcs.length)
          chained.funcs.foreach { f =>
            dataOut.writeInt(f.command.length)
            dataOut.write(f.command)
          }
        }
        dataOut.flush()

        val arrowSchema = ArrowUtils.toArrowSchema(schema)
        val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for $pythonExec", 0, Long.MaxValue)

        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        val arrowWriter = ArrowWriter.create(root)

        var closed = false

        context.addTaskCompletionListener { _ =>
          if (!closed) {
            root.close()
            allocator.close()
          }
        }

        // TODO: does ArrowStreamWriter buffer data?
        // TODO: who decides the dictionary?
        val writer = new ArrowStreamWriter(root, null, dataOut)
        writer.start()

        Utils.tryWithSafeFinally {
          while (inputRows.hasNext) {
            var rowCount = 0
            while (inputRows.hasNext && rowCount < batchSize) {
              val row = inputRows.next()
              arrowWriter.write(row)
              rowCount += 1
            }
            arrowWriter.finish()
            writer.writeBatch()
            arrowWriter.reset()
          }
        } {
          writer.end()
          root.close()
          allocator.close()
          closed = true
        }

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
}
