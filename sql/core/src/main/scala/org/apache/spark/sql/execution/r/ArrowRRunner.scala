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

package org.apache.spark.sql.execution.r

import java.io._
import java.nio.channels.Channels

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel

import org.apache.spark.TaskContext
import org.apache.spark.api.r._
import org.apache.spark.api.r.SpecialLengths
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.{LogKeys, MDC}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils


/**
 * Similar to `ArrowPythonRunner`, but exchange data with R worker via Arrow stream.
 */
class ArrowRRunner(
    func: Array[Byte],
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    schema: StructType,
    timeZoneId: String,
    mode: Int)
  extends BaseRRunner[Iterator[InternalRow], ColumnarBatch](
    func,
    "arrow",
    "arrow",
    packageNames,
    broadcastVars,
    numPartitions = -1,
    isDataFrame = true,
    schema.fieldNames,
    mode) {

  protected def bufferedWrite(
      dataOut: DataOutputStream)(writeFunc: ByteArrayOutputStream => Unit): Unit = {
    val out = new ByteArrayOutputStream()
    writeFunc(out)

    // Currently, there looks no way to read batch by batch by socket connection in R side,
    // See ARROW-4512. Therefore, it writes the whole Arrow streaming-formatted binary at
    // once for now.
    val data = out.toByteArray
    dataOut.writeInt(data.length)
    dataOut.write(data)
  }

  protected override def newWriterThread(
      output: OutputStream,
      inputIterator: Iterator[Iterator[InternalRow]],
      partitionIndex: Int): WriterThread = {
    new WriterThread(output, inputIterator, partitionIndex) {

      /**
       * Writes input data to the stream connected to the R worker.
       */
      override protected def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        if (inputIterator.hasNext) {
          val arrowSchema =
            ArrowUtils.toArrowSchema(
              schema, timeZoneId, errorOnDuplicatedFieldNames = true, largeVarTypes = false)
          val allocator = ArrowUtils.rootAllocator.newChildAllocator(
            "stdout writer for R", 0, Long.MaxValue)
          val root = VectorSchemaRoot.create(arrowSchema, allocator)

          bufferedWrite(dataOut) { out =>
            Utils.tryWithSafeFinally {
              val arrowWriter = ArrowWriter.create(root)
              val writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))
              writer.start()

              while (inputIterator.hasNext) {
                val nextBatch: Iterator[InternalRow] = inputIterator.next()

                while (nextBatch.hasNext) {
                  arrowWriter.write(nextBatch.next())
                }

                arrowWriter.finish()
                writer.writeBatch()
                arrowWriter.reset()
              }
              writer.end()
            } {
              // Don't close root and allocator in TaskCompletionListener to prevent
              // a race condition. See `ArrowPythonRunner`.
              root.close()
              allocator.close()
            }
          }
        }
      }
    }
  }

  protected override def newReaderIterator(
      dataStream: DataInputStream, errThread: BufferedStreamThread): ReaderIterator = {
    new ReaderIterator(dataStream, errThread) {
      private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        "stdin reader for R", 0, Long.MaxValue)

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var vectors: Array[ColumnVector] = _

      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        if (reader != null) {
          reader.close(false)
        }
        allocator.close()
      }

      private var batchLoaded = true

      private def format(v: Double): String = {
        "%.3f".format(v)
      }

      protected override def read(): ColumnarBatch = try {
        if (reader != null && batchLoaded) {
          batchLoaded = reader.loadNextBatch()
          if (batchLoaded) {
            val batch = new ColumnarBatch(vectors)
            batch.setNumRows(root.getRowCount)
            batch
          } else {
            reader.close(false)
            allocator.close()
            // Should read timing data after this.
            read()
          }
        } else {
          dataStream.readInt() match {
            case SpecialLengths.TIMING_DATA =>
              // Timing data from R worker
              val boot = dataStream.readDouble - bootTime
              val init = dataStream.readDouble
              val broadcast = dataStream.readDouble
              val input = dataStream.readDouble
              val compute = dataStream.readDouble
              val output = dataStream.readDouble
              logInfo(log"Times: boot = ${MDC(LogKeys.BOOT, format(boot))} s, " +
                log"init = ${MDC(LogKeys.INIT, format(init))} s, " +
                log"broadcast = ${MDC(LogKeys.BROADCAST, format(broadcast))} s, " +
                log"read-input = ${MDC(LogKeys.INPUT, format(input))} s, " +
                log"compute = ${MDC(LogKeys.COMPUTE, format(compute))} s, " +
                log"write-output = ${MDC(LogKeys.OUTPUT, format(output))} s, " +
                log"total = ${MDC(LogKeys.TOTAL,
                  format(boot + init + broadcast + input + compute + output))} s")
              read()
            case length if length > 0 =>
              // Likewise, there looks no way to send each batch in streaming format via socket
              // connection. See ARROW-4512.
              // So, it reads the whole Arrow streaming-formatted binary at once for now.
              val buffer = new Array[Byte](length)
              dataStream.readFully(buffer)
              val in = new ByteArrayReadableSeekableByteChannel(buffer)
              reader = new ArrowStreamReader(in, allocator)
              root = reader.getVectorSchemaRoot
              vectors = root.getFieldVectors.asScala.map { vector =>
                new ArrowColumnVector(vector)
              }.toArray[ColumnVector]
              read()
            case length if length == 0 =>
              // End of stream
              eos = true
              null
          }
        }
      } catch handleException
    }
  }
}
