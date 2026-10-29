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
package org.apache.spark.sql.execution.datasources.v2.columnar

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.util.VectorSchemaRootAppender

import org.apache.spark.{SparkContext, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.{ColumnarInputDataWriter, SupportsColumnarInput}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

class ColumnarWriteBuilder(
    shortName: String,
    info: LogicalWriteInfo,
    createDataSource: () => ColumnarDataSource)
  extends WriteBuilder with SupportsTruncate {

  private var overwrite = false

  override def truncate(): WriteBuilder = {
    overwrite = true
    this
  }

  override def build(): Write = new ColumnarWrite(shortName, info, overwrite, createDataSource)
}

/**
 * A write to a [[ColumnarDataSource]]. Its data writers also take the batches of a columnar
 * query, which Spark then does not convert to rows (see [[ArrowBatchingDataWriter]]).
 */
class ColumnarWrite(
    shortName: String,
    info: LogicalWriteInfo,
    overwrite: Boolean,
    createDataSource: () => ColumnarDataSource)
  extends Write with SupportsColumnarInput {

  override def description(): String = shortName

  override def toBatch: BatchWrite = {
    new ColumnarBatchWrite(
      () => createDataSource().writer(info.schema(), overwrite), info.schema())
  }

  override def toStreaming: StreamingWrite = {
    new ColumnarStreamingWrite(
      shortName,
      () => createDataSource().streamWriter(info.schema(), overwrite, info.queryId()),
      info.schema())
  }
}

/**
 * A batch write. The writer of the data source is created with the writer factory, right before
 * the tasks of the write start: after the shuffle stages that adaptive query execution runs
 * before the write, so no writer is created if they fail. The rest of the data, such as its last
 * stage, is computed in the tasks of the write, and a failure there, as any later failure, aborts
 * the write. So irreversible work belongs in the commit of the writer.
 */
class ColumnarBatchWrite(createWriter: () => ColumnarWriter, schema: StructType)
  extends BatchWrite {

  private var writer: ColumnarWriter = _

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    if (writer == null) {
      writer = createWriter()
    }
    new ColumnarDataWriterFactory(writer, ArrowBatchingOptions(schema))
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = writer.commit(messages)

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    if (writer != null) {
      writer.abort(messages)
    }
  }
}

/**
 * A streaming write. As for a [[ColumnarBatchWrite]], the writer of the data source is created
 * with the writer factory, right before the tasks of the micro-batch start. A stateful
 * micro-batch runs without adaptive query execution, so all its stages, as those of any write
 * without it, run after the writer is created.
 *
 * Continuous processing is not supported: its tasks do not commit or abort the data writers of
 * the last epoch when the query stops, so it fails before a writer is created.
 */
class ColumnarStreamingWrite(
    description: String,
    createWriter: () => ColumnarStreamWriter,
    schema: StructType)
  extends StreamingWrite with SupportsColumnarInput {

  private var writer: ColumnarStreamWriter = _

  override def createStreamingWriterFactory(
      info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    // Continuous processing sets this property before it runs the write, and only it does.
    if (SparkContext.getActive.exists(
        _.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY) != null)) {
      throw new SparkUnsupportedOperationException(
        "DATA_SOURCE_CONTINUOUS_WRITE_NOT_SUPPORTED", Map("description" -> description))
    }
    if (writer == null) {
      writer = createWriter()
    }
    new ColumnarStreamingDataWriterFactory(writer, ArrowBatchingOptions(schema))
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    writer.commit(epochId, messages)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    if (writer != null) {
      writer.abort(epochId, messages)
    }
  }
}

class ColumnarDataWriterFactory(writer: ColumnarWriter, options: ArrowBatchingOptions)
  extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new ArrowBatchingDataWriter(writer.createWriter(partitionId, taskId), options)
  }
}

class ColumnarStreamingDataWriterFactory(
    writer: ColumnarStreamWriter,
    options: ArrowBatchingOptions)
  extends StreamingDataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    new ArrowBatchingDataWriter(writer.createWriter(partitionId, taskId, epochId), options)
  }
}

/**
 * How [[ArrowBatchingDataWriter]] converts rows to Arrow, captured on the driver from the
 * session configuration. Like the other Arrow batches of Spark, a batch ends when it reaches
 * `spark.sql.execution.arrow.maxRecordsPerBatch` rows or, after a row, holds at least
 * `spark.sql.execution.arrow.maxBytesPerBatch` bytes.
 */
case class ArrowBatchingOptions(
    schema: StructType,
    timeZoneId: String,
    maxRecordsPerBatch: Int,
    maxBytesPerBatch: Long)

object ArrowBatchingOptions {
  def apply(schema: StructType): ArrowBatchingOptions = {
    val conf = SQLConf.get
    val maxRecords = conf.arrowMaxRecordsPerBatch
    ArrowBatchingOptions(
      schema,
      conf.sessionLocalTimeZone,
      if (maxRecords > 0) maxRecords else Int.MaxValue,
      conf.arrowMaxBytesPerBatch)
  }
}

/**
 * Converts the data of a write task to Arrow-backed [[ColumnarBatch]]es for the [[DataWriter]]
 * of a [[ColumnarDataSource]]: its rows, or the columnar batches of a columnar query. Each batch
 * gets new Arrow vectors, so a native writer that took ownership of the data of a batch can keep
 * it after the call, while the producer of a columnar batch may reuse its vectors once the call
 * returns.
 *
 * The columns of a columnar batch are copied buffer by buffer when they are all Arrow vectors in
 * the layout of the Arrow schema of the write, and converted value by value otherwise. Either way,
 * the batches end as those of rows do, though a copied batch also ends with the columnar batch.
 */
class ArrowBatchingDataWriter(delegate: DataWriter[ColumnarBatch], options: ArrowBatchingOptions)
  extends ColumnarInputDataWriter {

  private val arrowSchema = ArrowUtils.toArrowSchema(
    options.schema, options.timeZoneId, errorOnDuplicatedFieldNames = true, largeVarTypes = false)
  private val allocator =
    ArrowUtils.rootAllocator.newChildAllocator("columnar data source writer", 0, Long.MaxValue)
  // The batch of rows being written, created on its first row.
  private var root: VectorSchemaRoot = _
  private var arrowWriter: ArrowWriter = _
  private var numRows = 0

  override def write(record: InternalRow): Unit = {
    if (root == null) {
      root = VectorSchemaRoot.create(arrowSchema, allocator)
      arrowWriter = ArrowWriter.create(root)
    }
    arrowWriter.write(record)
    numRows += 1
    // The size is not computed when the limit is the largest possible one, as in Python UDFs.
    if (numRows >= options.maxRecordsPerBatch || (options.maxBytesPerBatch < Int.MaxValue &&
        arrowWriter.sizeInBytes() >= options.maxBytesPerBatch)) {
      flush()
    }
  }

  override def writeBatch(batch: ColumnarBatch): Unit = {
    val numBatchRows = batch.numRows()
    arrowVectors(batch) match {
      case Some(vectors) =>
        flush()
        // The vectors of the batch, which this root does not own: their producer releases them.
        val batchRoot =
          new VectorSchemaRoot(vectors.map(_.getField).asJava, vectors.asJava, numBatchRows)
        var start = 0
        while (start < numBatchRows) {
          // A slice shares the buffers of the batch. Arrow Java copies a vector of variable
          // width, or a list, as if its offsets started at 0, which those of a slice do.
          val slice = batchRoot.slice(
            start, math.min(numBatchRows - start, options.maxRecordsPerBatch))
          try {
            val length = batchLength(slice)
            if (length == slice.getRowCount) {
              writeCopy(slice)
            } else {
              val head = slice.slice(0, length)
              try writeCopy(head) finally head.close()
            }
            start += length
          } finally {
            slice.close()
          }
        }
      case None =>
        var i = 0
        while (i < numBatchRows) {
          write(batch.getRow(i))
          i += 1
        }
    }
  }

  /**
   * Returns the Arrow vectors of the columns of a batch, if its columns can be copied buffer by
   * buffer: Arrow vectors with as many values as the batch has rows, whose fields are physically
   * congruent with those of `arrowSchema`, though they may differ in the names, nullability and
   * metadata of their fields, and in the time zones of timestamps, which the copy keeps.
   */
  private def arrowVectors(batch: ColumnarBatch): Option[Seq[FieldVector]] = {
    val fields = arrowSchema.getFields.asScala
    // Without columns, the number of rows of a batch is only kept by the conversion.
    if (fields.isEmpty || batch.numCols() != fields.length) {
      None
    } else {
      val vectors = fields.indices.map { i =>
        batch.column(i) match {
          case column: ArrowColumnVector => column.getValueVector match {
            case vector: FieldVector if vector.getValueCount == batch.numRows() &&
                ArrowUtils.isCompatibleWithDeclaredField(vector.getField, fields(i)) =>
              Some(vector)
            case _ => None
          }
          case _ => None
        }
      }
      if (vectors.forall(_.isDefined)) Some(vectors.flatten) else None
    }
  }

  /**
   * Returns the number of rows of the batch that starts with a slice of the rows of a columnar
   * batch: all of them, or as a batch of rows ends, up to the first row with which it holds
   * `maxBytesPerBatch` bytes, by the same measure of its size.
   */
  private def batchLength(slice: VectorSchemaRoot): Int = {
    val numSliceRows = slice.getRowCount
    // The size is not computed when the limit is the largest possible one, as for rows.
    if (options.maxBytesPerBatch >= Int.MaxValue) {
      numSliceRows
    } else {
      def sizeInBytes(rows: Int): Long =
        slice.getFieldVectors.asScala.map(_.getBufferSizeFor(rows).toLong).sum
      // The size grows with the number of rows.
      var lo = 1
      var hi = numSliceRows
      while (lo < hi) {
        val mid = (lo + hi) >>> 1
        if (sizeInBytes(mid) >= options.maxBytesPerBatch) hi = mid else lo = mid + 1
      }
      lo
    }
  }

  /** Writes a slice of the Arrow vectors of a columnar batch, copied buffer by buffer. */
  private def writeCopy(slice: VectorSchemaRoot): Unit = {
    val length = slice.getRowCount
    val copy = VectorSchemaRoot.create(slice.getSchema, allocator)
    try {
      copy.getFieldVectors.forEach(_.setInitialCapacity(length))
      copy.allocateNew()
      VectorSchemaRootAppender.append(false, copy, slice)
      // Also sets the number of values of a null vector, which the copy leaves at 0.
      copy.setRowCount(length)
      writeArrow(copy)
    } finally {
      copy.close()
    }
  }

  private def writeArrow(batchRoot: VectorSchemaRoot): Unit = {
    val columns = batchRoot.getFieldVectors.asScala.map(new ArrowColumnVector(_): ColumnVector)
    delegate.write(new ColumnarBatch(columns.toArray, batchRoot.getRowCount))
  }

  private def flush(): Unit = {
    if (root != null) {
      arrowWriter.finish()
      try {
        writeArrow(root)
      } finally {
        releaseBatch()
      }
    }
  }

  private def releaseBatch(): Unit = {
    if (root != null) {
      root.close()
      root = null
      arrowWriter = null
      numRows = 0
    }
  }

  override def commit(): WriterCommitMessage = {
    flush()
    delegate.commit()
  }

  override def abort(): Unit = {
    releaseBatch()
    delegate.abort()
  }

  override def close(): Unit = {
    try {
      delegate.close()
    } finally {
      releaseBatch()
      Utils.tryLogNonFatalError(allocator.close())
    }
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = delegate.currentMetricsValues()
}
