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

import java.io.DataOutputStream
import java.net.Socket

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, PythonRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

/**
 * A trait that can be mixed-in with [[BasePythonRunner]]. It implements the logic from
 * JVM (an iterator of internal rows + additional data if required) to Python (Arrow).
 */
private[python] trait PythonArrowInput[IN] { self: BasePythonRunner[IN, _] =>
  protected val workerConf: Map[String, String]

  protected val schema: StructType

  protected val timeZoneId: String

  protected def pythonMetrics: Map[String, SQLMetric]

  protected def writeIteratorToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[IN]): Unit

  protected def handleMetadataBeforeExec(stream: DataOutputStream): Unit = {
    // Write config for the worker as a number of key -> value pairs of strings
    stream.writeInt(workerConf.size)
    for ((k, v) <- workerConf) {
      PythonRDD.writeUTF(k, stream)
      PythonRDD.writeUTF(v, stream)
    }
  }

  protected override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[IN],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        handleMetadataBeforeExec(dataOut)
        PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
        val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for $pythonExec", 0, Long.MaxValue)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)

        Utils.tryWithSafeFinally {
          val writer = new ArrowStreamWriter(root, null, dataOut)
          writer.start()

          writeIteratorToArrowStream(root, writer, dataOut, inputIterator)

          // end writes footer to the output stream and doesn't clean any resources.
          // It could throw exception if the output stream is closed, so it should be
          // in the try block.
          writer.end()
        } {
          // If we close root and allocator in TaskCompletionListener, there could be a race
          // condition where the writer thread keeps writing to the VectorSchemaRoot while
          // it's being closed by the TaskCompletion listener.
          // Closing root and allocator here is cleaner because root and allocator is owned
          // by the writer thread and is only visible to the writer thread.
          //
          // If the writer thread is interrupted by TaskCompletionListener, it should either
          // (1) in the try block, in which case it will get an InterruptedException when
          // performing io, and goes into the finally block or (2) in the finally block,
          // in which case it will ignore the interruption and close the resources.
          root.close()
          allocator.close()
        }
      }
    }
  }
}

private[python] trait BasicPythonArrowInput extends PythonArrowInput[Iterator[InternalRow]] {
  self: BasePythonRunner[Iterator[InternalRow], _] =>

  protected def writeIteratorToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[Iterator[InternalRow]]): Unit = {
    val arrowWriter = ArrowWriter.create(root)

    while (inputIterator.hasNext) {
      val startData = dataOut.size()
      val nextBatch = inputIterator.next()

      while (nextBatch.hasNext) {
        arrowWriter.write(nextBatch.next())
      }

      arrowWriter.finish()
      writer.writeBatch()
      arrowWriter.reset()
      val deltaData = dataOut.size() - startData
      pythonMetrics("pythonDataSent") += deltaData
    }
  }
}
