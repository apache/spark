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

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.api.r._
import org.apache.spark.api.r.SpecialLengths
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.types.StructType
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
  extends RRunner[ColumnarBatch](
    func,
    "arrow",
    "arrow",
    packageNames,
    broadcastVars,
    numPartitions = -1,
    isDataFrame = true,
    schema.fieldNames,
    mode) {

  // TODO: it needs to refactor to share the same code with RRunner, and have separate
  // ArrowRRunners.
  private val getNextBatch = {
    if (mode == RRunnerModes.DATAFRAME_GAPPLY) {
      // gapply
      (inputIterator: Iterator[_], keys: collection.mutable.ArrayBuffer[Array[Byte]]) => {
        val (key, nextBatch) = inputIterator
          .asInstanceOf[Iterator[(Array[Byte], Iterator[InternalRow])]].next()
        keys.append(key)
        nextBatch
      }
    } else {
      // dapply
      (inputIterator: Iterator[_], keys: collection.mutable.ArrayBuffer[Array[Byte]]) => {
        inputIterator
          .asInstanceOf[Iterator[Iterator[InternalRow]]].next()
      }
    }
  }

  protected override def writeData(
      dataOut: DataOutputStream,
      printOut: PrintStream,
      inputIterator: Iterator[_]): Unit = if (inputIterator.hasNext) {
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val allocator = ArrowUtils.rootAllocator.newChildAllocator(
      "stdout writer for R", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val out = new ByteArrayOutputStream()
    val keys = collection.mutable.ArrayBuffer.empty[Array[Byte]]

    Utils.tryWithSafeFinally {
      val arrowWriter = ArrowWriter.create(root)
      val writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))
      writer.start()

      while (inputIterator.hasNext) {
        val nextBatch: Iterator[InternalRow] = getNextBatch(inputIterator, keys)

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

    // Currently, there looks no way to read batch by batch by socket connection in R side,
    // See ARROW-4512. Therefore, it writes the whole Arrow streaming-formatted binary at
    // once for now.
    val data = out.toByteArray
    dataOut.writeInt(data.length)
    dataOut.write(data)

    keys.foreach(dataOut.write)
  }

  protected override def newReaderIterator(
      dataStream: DataInputStream, errThread: BufferedStreamThread): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
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
      private var nextObj: ColumnarBatch = _
      private var eos = false

      override def hasNext: Boolean = nextObj != null || {
        if (!eos) {
          nextObj = read()
          hasNext
        } else {
          false
        }
      }

      override def next(): ColumnarBatch = {
        if (hasNext) {
          val obj = nextObj
          nextObj = null.asInstanceOf[ColumnarBatch]
          obj
        } else {
          Iterator.empty.next()
        }
      }

      private def read(): ColumnarBatch = try {
        if (reader != null && batchLoaded) {
          batchLoaded = reader.loadNextBatch()
          if (batchLoaded) {
            val batch = new ColumnarBatch(vectors)
            batch.setNumRows(root.getRowCount)
            batch
          } else {
            reader.close(false)
            allocator.close()
            eos = true
            null
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
              logInfo(
                ("Times: boot = %.3f s, init = %.3f s, broadcast = %.3f s, " +
                  "read-input = %.3f s, compute = %.3f s, write-output = %.3f s, " +
                  "total = %.3f s").format(
                  boot,
                  init,
                  broadcast,
                  input,
                  compute,
                  output,
                  boot + init + broadcast + input + compute + output))
              read()
            case length if length > 0 =>
              // Likewise, there looks no way to send each batch in streaming format via socket
              // connection. See ARROW-4512.
              // So, it reads the whole Arrow streaming-formatted binary at once for now.
              val in = new ByteArrayReadableSeekableByteChannel(readByteArrayData(length))
              reader = new ArrowStreamReader(in, allocator)
              root = reader.getVectorSchemaRoot
              vectors = root.getFieldVectors.asScala.map { vector =>
                new ArrowColumnVector(vector)
              }.toArray[ColumnVector]
              read()
            case length if length == 0 =>
              eos = true
              null
          }
        }
      } catch {
        case eof: EOFException =>
          throw new SparkException(
            "R worker exited unexpectedly (crashed)\n " + errThread.getLines(), eof)
      }
    }
  }
}
