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

import java.io.DataInputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamReader

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, SpecialLengths}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

/**
 * A trait that can be mixed-in with [[BasePythonRunner]]. It implements the logic from
 * Python (Arrow) to JVM (output type being deserialized from ColumnarBatch).
 */
private[python] trait PythonArrowOutput[OUT <: AnyRef] { self: BasePythonRunner[_, OUT] =>

  protected def pythonMetrics: Map[String, SQLMetric]

  protected def handleMetadataAfterExec(stream: DataInputStream): Unit = { }

  protected def deserializeColumnarBatch(batch: ColumnarBatch, schema: StructType): OUT

  protected def newReaderIterator(
      stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[OUT] = {

    new ReaderIterator(
      stream, writerThread, startTime, env, worker, pid, releasedOrClosed, context) {

      private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdin reader for $pythonExec", 0, Long.MaxValue)

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var schema: StructType = _
      private var vectors: Array[ColumnVector] = _

      context.addTaskCompletionListener[Unit] { _ =>
        if (reader != null) {
          reader.close(false)
        }
        allocator.close()
      }

      private var batchLoaded = true

      protected override def handleEndOfDataSection(): Unit = {
        handleMetadataAfterExec(stream)
        super.handleEndOfDataSection()
      }

      protected override def read(): OUT = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          if (reader != null && batchLoaded) {
            val bytesReadStart = reader.bytesRead()
            batchLoaded = reader.loadNextBatch()
            if (batchLoaded) {
              val batch = new ColumnarBatch(vectors)
              val rowCount = root.getRowCount
              batch.setNumRows(root.getRowCount)
              val bytesReadEnd = reader.bytesRead()
              pythonMetrics("pythonNumRowsReceived") += rowCount
              pythonMetrics("pythonDataReceived") += bytesReadEnd - bytesReadStart
              deserializeColumnarBatch(batch, schema)
            } else {
              reader.close(false)
              allocator.close()
              // Reach end of stream. Call `read()` again to read control data.
              read()
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                reader = new ArrowStreamReader(stream, allocator)
                root = reader.getVectorSchemaRoot()
                schema = ArrowUtils.fromArrowSchema(root.getSchema())
                vectors = root.getFieldVectors().asScala.map { vector =>
                  new ArrowColumnVector(vector)
                }.toArray[ColumnVector]
                read()
              case SpecialLengths.TIMING_DATA =>
                handleTimingData()
                read()
              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException()
              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection()
                null.asInstanceOf[OUT]
            }
          }
        } catch handleException
      }
    }
  }
}

private[python] trait BasicPythonArrowOutput extends PythonArrowOutput[ColumnarBatch] {
  self: BasePythonRunner[_, ColumnarBatch] =>

  protected def deserializeColumnarBatch(
      batch: ColumnarBatch,
      schema: StructType): ColumnarBatch = batch
}
