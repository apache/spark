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
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.{BaseFixedWidthVector, BaseVariableWidthVector, BaseVariableWidthViewVector, BitVectorHelper, FieldVector, ValueVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{BaseRepeatedValueVector, ListVector}
import org.apache.arrow.vector.ipc.ArrowStreamReader

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, PythonWorker, PythonWorkerHandle, SpecialLengths}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.ReusedArrowColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * A trait that can be mixed-in with [[BasePythonRunner]]. It implements the logic from
 * Python (Arrow) to JVM (output type being deserialized from ColumnarBatch).
 */
private[python] trait PythonArrowOutput[OUT <: AnyRef] { self: BasePythonRunner[_, OUT] =>

  protected def pythonMetrics: Map[String, SQLMetric]

  protected def handleMetadataAfterExec(stream: DataInputStream): Unit = { }

  protected def deserializeColumnarBatch(batch: ColumnarBatch, schema: StructType): OUT

  // The limits for slicing the batches returned from Python. A runner whose consumer cannot take
  // a batch split into several, e.g. one that pairs each batch with one input row, overrides both
  // to -1, as `ArrowPythonUDTFRunner` does.
  protected def arrowMaxRecordsPerOutputBatch: Int = SQLConf.get.arrowMaxRecordsPerOutputBatch

  protected def arrowMaxBytesPerOutputBatch: Int = SQLConf.get.arrowMaxBytesPerOutputBatch

  if (arrowMaxRecordsPerOutputBatch > 0 && arrowMaxBytesPerOutputBatch > 0) {
    throw new IllegalArgumentException(
      "Both max records and max bytes of an Arrow output batch cannot be set at the same time.")
  }

  protected def newReaderIterator(
      stream: DataInputStream,
      writer: Writer,
      startTime: Long,
      env: SparkEnv,
      worker: PythonWorker,
      handle: Option[PythonWorkerHandle],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[OUT] = {

    new ReaderIterator(
      stream, writer, startTime, env, worker, handle, releasedOrClosed, context) {

      private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdin reader for $pythonExec", 0, Long.MaxValue)

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var schema: StructType = _
      private var processor: ArrowOutputProcessor = _

      context.addTaskCompletionListener[Unit] { _ =>
        if (processor != null) {
          processor.close()
        }
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

      protected override def handleMetricsData(): Unit = {
        // Get data size from pythonMetrics which is already being tracked
        totalDataReceived = pythonMetrics.get("pythonDataReceived")
          .map(_.value)
          .getOrElse(0L)
        super.handleMetricsData()
      }

      protected override def read(): OUT = {
        if (writer.exception.isDefined) {
          throw writer.exception.get
        }
        try {
          if (reader != null && batchLoaded) {
            batchLoaded = processor.loadBatch()
            if (batchLoaded) {
              batchesProcessed += 1
              val batch = processor.produceBatch()
              deserializeColumnarBatch(batch, schema)
            } else {
              processor.close()
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

                if (arrowMaxRecordsPerOutputBatch > 0) {
                  processor = new SliceRecordsArrowOutputProcessorImpl(
                    reader, pythonMetrics, arrowMaxRecordsPerOutputBatch)
                } else if (arrowMaxBytesPerOutputBatch > 0) {
                  processor = new SliceBytesArrowOutputProcessorImpl(
                    reader, pythonMetrics, arrowMaxBytesPerOutputBatch)
                } else {
                  processor = new ArrowOutputProcessorImpl(reader, pythonMetrics)
                }

                read()
              case SpecialLengths.METRICS_DATA =>
                handleMetricsData()
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

trait ArrowOutputProcessor {
  def loadBatch(): Boolean
  protected def getRoot: VectorSchemaRoot
  protected def getVectors(root: VectorSchemaRoot): Array[ColumnVector]
  def produceBatch(): ColumnarBatch
  def close(): Unit
}

/**
 * Loads every batch from Python into the reader's root and returns batches over the same vectors,
 * so the vectors of a batch are only valid until the next one is loaded. They are
 * [[ReusedArrowColumnVector]]s: a consumer does not hold them past the next batch, and its
 * `closeIfFreeable` leaves them to this processor, which closes them in `close`.
 */
class ArrowOutputProcessorImpl(reader: ArrowStreamReader, pythonMetrics: Map[String, SQLMetric])
    extends ArrowOutputProcessor {
  protected val root = reader.getVectorSchemaRoot()
  protected val schema: StructType = ArrowUtils.fromArrowSchema(root.getSchema())
  private val vectors: Array[ColumnVector] = root.getFieldVectors().asScala.map { vector =>
    new ReusedArrowColumnVector(vector)
  }.toArray[ColumnVector]

  protected var rowCount = -1

  override def loadBatch(): Boolean = {
    val bytesReadStart = reader.bytesRead()
    val batchLoaded = reader.loadNextBatch()
    if (batchLoaded) {
      rowCount = root.getRowCount
      val bytesReadEnd = reader.bytesRead()
      pythonMetrics("pythonNumRowsReceived") += rowCount
      pythonMetrics("pythonDataReceived") += bytesReadEnd - bytesReadStart
    }
    batchLoaded
  }

  protected override def getRoot: VectorSchemaRoot = root
  protected override def getVectors(root: VectorSchemaRoot): Array[ColumnVector] = vectors
  override def produceBatch(): ColumnarBatch = {
    val batchRoot = getRoot
    val vectors = getVectors(batchRoot)
    val batch = new ColumnarBatch(vectors)
    batch.setNumRows(batchRoot.getRowCount)
    batch
  }
  override def close(): Unit = {
    vectors.foreach(_.close())
    root.close()
  }
}

abstract class BaseSliceArrowOutputProcessor(
    reader: ArrowStreamReader,
    pythonMetrics: Map[String, SQLMetric])
  extends ArrowOutputProcessorImpl(reader, pythonMetrics) {

  protected var currentRowIdx = -1
  protected var prevRoot: VectorSchemaRoot = root

  override def loadBatch(): Boolean = {
    if (currentRowIdx < rowCount) {
      true
    } else {
      val loaded = super.loadBatch()
      currentRowIdx = 0
      loaded
    }
  }

  override def produceBatch(): ColumnarBatch = {
    val batchRoot = getRoot
    closePrevSlice()
    prevRoot = batchRoot

    val batch = new ColumnarBatch(getVectors(batchRoot))
    batch.setNumRows(batchRoot.getRowCount)
    batch
  }

  // The vectors are either those of the reader's root, into which the next batch is loaded, or
  // those of a slice of it, which is closed once the next one is produced, so both are reused
  // vectors.
  protected override def getVectors(root: VectorSchemaRoot): Array[ColumnVector] = {
    if (root eq this.root) {
      super.getVectors(root)
    } else {
      root.getFieldVectors.asScala.map { vector =>
        new ReusedArrowColumnVector(vector)
      }.toArray[ColumnVector]
    }
  }

  // Releases the previous output batch if it is a slice. The reader loads every batch from Python
  // into the same root, so it must not be closed here; `ArrowOutputProcessorImpl.close` closes it.
  private def closePrevSlice(): Unit = {
    if (prevRoot ne root) {
      prevRoot.close()
    }
  }

  override def close(): Unit = {
    closePrevSlice()
    super.close()
  }

  private[python] def canSlice(root: VectorSchemaRoot): Boolean =
    root.getFieldVectors.asScala.forall(BaseSliceArrowOutputProcessor.canSlice)
}

object BaseSliceArrowOutputProcessor {
  /**
   * Returns whether `VectorSchemaRoot.slice` can slice `vector` and its children. Arrow computes
   * some byte positions of a slice in int, so it throws, and leaks the vectors sliced so far, for
   * a fixed-width vector of 2 GiB or more, a view vector with 2^27 rows or more, or a list vector
   * with 2^29 - 1 rows or more. A batch that holds one is output whole instead.
   *
   * The offset positions of a list vector overflow only from 2^29 rows, but a slice of `length`
   * rows also allocates `(length + 1) * 4` bytes of offsets in int, and a list vector nested in
   * another one is sliced over all its rows when the slice of its parent covers them.
   */
  private[python] def canSlice(vector: FieldVector): Boolean = {
    val fits = vector match {
      case v: BaseFixedWidthVector => v.getValueCount.toLong * v.getTypeWidth <= Int.MaxValue
      case v: BaseVariableWidthViewVector =>
        v.getValueCount.toLong * BaseVariableWidthViewVector.ELEMENT_SIZE <= Int.MaxValue
      case v: ListVector =>
        (v.getValueCount + 1L) * BaseRepeatedValueVector.OFFSET_WIDTH <= Int.MaxValue
      case _ => true
    }
    fits && vector.getChildrenFromFields.asScala.forall(canSlice)
  }
}

class SliceBytesArrowOutputProcessorImpl(
    reader: ArrowStreamReader,
    pythonMetrics: Map[String, SQLMetric],
    arrowMaxBytesPerOutputBatch: Int)
  extends BaseSliceArrowOutputProcessor(reader, pythonMetrics) {

  // The number of rows of the last slice output from the current batch, from which the search
  // for the next slice starts.
  private var sliceRowCount = 0

  protected override def getRoot: VectorSchemaRoot = {
    if (currentRowIdx == 0) {
      // Output the whole root if it fits, which happens only if no row of it has been output yet.
      if (batchBytes(root) <= arrowMaxBytesPerOutputBatch) {
        currentRowIdx = rowCount
        return root
      }
      sliceRowCount = (rowCount * SliceBytesArrowOutputProcessorImpl.stepRatio).toInt
    }
    // We iteratively shrink the slice by 10% until it fits. Each search starts from the number of
    // rows of the last slice that fit, since every slice that does not start at row 0 copies its
    // offset buffers, and restarting from all the remaining rows would copy them again and again.
    var numRows = math.min(sliceRowCount, rowCount - currentRowIdx)
    while (numRows > 0) {
      val rootSlice = root.slice(currentRowIdx, numRows)
      if (batchBytes(rootSlice) <= arrowMaxBytesPerOutputBatch) {
        sliceRowCount = numRows
        currentRowIdx += numRows
        return rootSlice
      }
      rootSlice.close()
      numRows = (numRows * SliceBytesArrowOutputProcessorImpl.stepRatio).toInt
    }
    // Even a single row is larger than the limit, so we output a batch with that one row.
    // Normally, this should not happen, but it is possible if the data is very large.
    sliceRowCount = 1
    val rootSlice = root.slice(currentRowIdx, 1)
    currentRowIdx += 1
    rootSlice
  }

  protected def batchBytes(root: VectorSchemaRoot): Long = {
    root.getFieldVectors.asScala.map(SliceBytesArrowOutputProcessorImpl.bufferSize).sum
  }
}

object SliceBytesArrowOutputProcessorImpl {
  private val stepRatio = 0.9

  /**
   * Returns the size in bytes of the buffers of `vector`, like `ValueVector.getBufferSize` but as
   * a Long. `getBufferSize` computes the size in int, so it wraps around for a vector over 2 GiB,
   * e.g. a struct vector, which adds up the sizes of its children, such as the one top-level
   * struct vector that holds the output of mapInArrow, mapInPandas, applyInPandas and Python
   * UDTFs. Other vectors, e.g. `NullVector` and the large variable-width ones, which cap their
   * size at Int.MaxValue, fall back to `getBufferSize`.
   */
  def bufferSize(vector: ValueVector): Long = vector match {
    case v: BaseFixedWidthVector if v.getTypeWidth > 0 && v.getValueCount > 0 =>
      BitVectorHelper.getValidityBufferSizeFromCount(v.getValueCount) +
        v.getValueCount.toLong * v.getTypeWidth
    case v: BaseVariableWidthVector if v.getValueCount > 0 =>
      val offsetWidth = BaseVariableWidthVector.OFFSET_WIDTH
      BitVectorHelper.getValidityBufferSizeFromCount(v.getValueCount) +
        (v.getValueCount + 1L) * offsetWidth +
        v.getOffsetBuffer.getInt(v.getValueCount.toLong * offsetWidth)
    case v: FieldVector if !v.getChildrenFromFields.isEmpty =>
      val children = v.getChildrenFromFields.asScala.toSeq
      // The size of the buffers of the nested vector itself, e.g. the validity and offsets. Its
      // `getBufferSize` adds them to the sizes of its children in int, so subtracting the sizes
      // of the children in int cancels out any wrap-around of the sum.
      val ownBytes = v.getBufferSize - children.map(_.getBufferSize).sum
      ownBytes + children.map(bufferSize).sum
    case v => v.getBufferSize
  }
}

class SliceRecordsArrowOutputProcessorImpl(
    reader: ArrowStreamReader,
    pythonMetrics: Map[String, SQLMetric],
    arrowMaxRecordsPerOutputBatch: Int)
  extends BaseSliceArrowOutputProcessor(reader, pythonMetrics) {

  protected override def getRoot: VectorSchemaRoot = {
    val numRows = math.min(rowCount - currentRowIdx, arrowMaxRecordsPerOutputBatch)
    // Output the whole root only if this chunk is the whole batch, or if Arrow cannot slice the
    // batch, which is checked before any row of it has been output.
    if (numRows == rowCount || (currentRowIdx == 0 && !canSlice(root))) {
      currentRowIdx = rowCount
      root
    } else {
      val rootSlice = root.slice(currentRowIdx, numRows)
      currentRowIdx = currentRowIdx + numRows
      rootSlice
    }
  }
}
