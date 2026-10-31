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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.{BaseFixedWidthVector, BaseVariableWidthVector, BitVector, BitVectorHelper, FieldVector, NullVector, ValueVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.complex.{BaseRepeatedValueVector, ListVector, NonNullableStructVector, StructVector}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.{SparkContext, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.execution.arrow.{ArrowFieldWriter, ArrowWriter}
import org.apache.spark.sql.execution.python.BaseSliceArrowOutputProcessor
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
    new ColumnarDataWriterFactory(writer, ArrowBatchingOptions(schema, writer.timeZoneId))
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
    new ColumnarStreamingDataWriterFactory(writer, ArrowBatchingOptions(schema, writer.timeZoneId))
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
  /** @param timeZoneId the time zone of the writer of the data source */
  def apply(schema: StructType, timeZoneId: String): ArrowBatchingOptions = {
    val conf = SQLConf.get
    val maxRecords = conf.arrowMaxRecordsPerBatch
    ArrowBatchingOptions(
      schema,
      timeZoneId,
      if (maxRecords > 0) maxRecords else Int.MaxValue,
      conf.arrowMaxBytesPerBatch)
  }
}

/**
 * Converts the data of a write task to Arrow-backed [[ColumnarBatch]]es for the [[DataWriter]]
 * of a [[ColumnarDataSource]]: its rows, or the columnar batches of a columnar query. Each batch
 * gets new Arrow vectors, so a native writer that took ownership of the data of a batch can keep
 * it after the call, until the data writer is closed, which closes the allocator of the vectors,
 * while the producer of a columnar batch may reuse its vectors once the call returns.
 *
 * Each column of a columnar batch that is an Arrow vector in the layout of the Arrow schema of the
 * write, and that Arrow can slice, is copied buffer by buffer, with its values as they are, which
 * are not checked as the converted ones are, and the other columns are converted value by value,
 * row by row up to the end of each batch, so that a conversion does not grow past the size of a
 * batch. The batches end as those of rows do, though a batch with copied columns also ends with
 * the columnar batch, so small columnar batches give small batches, which are not merged. A
 * columnar batch without columns to copy is converted as rows are.
 */
class ArrowBatchingDataWriter(delegate: DataWriter[ColumnarBatch], options: ArrowBatchingOptions)
  extends ColumnarInputDataWriter {

  private val arrowSchema = ArrowUtils.toArrowSchema(
    options.schema, options.timeZoneId, errorOnDuplicatedFieldNames = true, largeVarTypes = false)
  // The allocator of the vectors of the batches, which holds nothing once the writer is closed.
  private[columnar] val allocator =
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
    val copied = copiedVectors(batch)
    if (copied.forall(_.isEmpty)) {
      // Without columns to copy, the values are converted as rows are.
      var i = 0
      while (i < numBatchRows) {
        write(batch.getRow(i))
        i += 1
      }
    } else if (numBatchRows > 0) {
      // A batch without rows leaves the pending batch of rows as it is.
      flush()
      // Each batch starts where the last one ended, so that the batches end as those of rows do.
      var start = 0
      while (start < numBatchRows) {
        start += writeRows(
          batch, copied, start, start + math.min(numBatchRows - start, options.maxRecordsPerBatch))
      }
    }
  }

  /**
   * Returns, for each column of a batch, its Arrow vector if it can be copied buffer by buffer, see
   * `ArrowBatchingDataWriter.canCopy`. The other columns are converted value by value.
   */
  private def copiedVectors(batch: ColumnarBatch): Seq[Option[FieldVector]] = {
    val fields = arrowSchema.getFields.asScala
    if (batch.numCols() != fields.length) {
      Seq.fill(fields.length)(None)
    } else {
      fields.indices.map { i =>
        batch.column(i) match {
          case column: ArrowColumnVector => column.getValueVector match {
            case vector: FieldVector
                if ArrowBatchingDataWriter.canCopy(vector, fields(i), batch.numRows()) =>
              Some(vector)
            case _ => None
          }
          case _ => None
        }
      }
    }
  }

  /**
   * Writes the batch of the rows of a columnar batch that starts at the row `start`, and returns
   * its number of rows: up to `end`, or as a batch of rows ends, up to the first row with which it
   * holds `maxBytesPerBatch` bytes, by the measure of `getBufferSizeFor`, which `rangeSize`
   * computes for the copied columns without slicing them.
   *
   * @param copied the vector of each column of the batch that is copied, see `copiedVectors`
   */
  private def writeRows(
      batch: ColumnarBatch,
      copied: Seq[Option[FieldVector]],
      start: Int,
      end: Int): Int = {
    // The columns that are not copied, and the vectors that they are converted into.
    val ordinals = copied.indices.filter(copied(_).isEmpty)
    val converted = ArrayBuffer.empty[FieldVector]
    Utils.tryWithSafeFinally {
      val writers = ordinals.map { ordinal =>
        val vector = arrowSchema.getFields.get(ordinal).createVector(allocator)
        converted += vector
        vector.setInitialCapacity(end - start)
        vector.allocateNew()
        ArrowWriter.createFieldWriter(vector)
      }
      val length = if (writers.isEmpty) {
        batchLength(copied.flatten, start, end)
      } else {
        convertRows(batch, copied.flatten, ordinals.zip(writers), start, end)
      }
      writers.foreach(_.finish())
      writeSlices(copied, converted.toSeq, start, length)
      length
    } {
      AutoCloseables.close(converted.asJava)
    }
  }

  /**
   * Returns the number of rows of the batch of copied columns that starts at the row `start`, see
   * `writeRows`.
   */
  private def batchLength(copied: Seq[FieldVector], start: Int, end: Int): Int = {
    def sizeInBytes(rows: Int): Long =
      copied.map(ArrowBatchingDataWriter.rangeSize(_, start, start + rows)).sum
    // The size is not computed when the limit is the largest possible one, as for rows.
    if (options.maxBytesPerBatch >= Int.MaxValue ||
        sizeInBytes(end - start) < options.maxBytesPerBatch) {
      end - start
    } else {
      // The size grows with the number of rows.
      var lo = 1
      var hi = end - start
      while (lo < hi) {
        val mid = (lo + hi) >>> 1
        if (sizeInBytes(mid) >= options.maxBytesPerBatch) hi = mid else lo = mid + 1
      }
      lo
    }
  }

  /**
   * Converts the columns of a columnar batch that are not copied with their writers, by ordinal,
   * row by row from the row `start` up to the end of the batch, and returns its number of rows, see
   * `writeRows`. As for rows, the size of the batch is computed after each row, so that the
   * conversion stops at the row with which the batch holds `maxBytesPerBatch` bytes.
   */
  private def convertRows(
      batch: ColumnarBatch,
      copied: Seq[FieldVector],
      writers: Seq[(Int, ArrowFieldWriter)],
      start: Int,
      end: Int): Int = {
    val copiedVectors = copied.toArray
    val ordinals = writers.map(_._1).toArray
    val fieldWriters = writers.map(_._2).toArray
    // The size of the batch once it holds the rows [start, rowEnd).
    def sizeInBytes(rowEnd: Int): Long = {
      var size = 0L
      var i = 0
      while (i < copiedVectors.length) {
        size += ArrowBatchingDataWriter.rangeSize(copiedVectors(i), start, rowEnd)
        i += 1
      }
      i = 0
      while (i < fieldWriters.length) {
        size += fieldWriters(i).getSizeInBytes()
        i += 1
      }
      size
    }
    // The size is not computed when the limit is the largest possible one, as for rows.
    val sized = options.maxBytesPerBatch < Int.MaxValue
    var next = start
    var full = false
    while (next < end && !full) {
      val row = batch.getRow(next)
      var i = 0
      while (i < fieldWriters.length) {
        fieldWriters(i).write(row, ordinals(i))
        i += 1
      }
      next += 1
      full = sized && sizeInBytes(next) >= options.maxBytesPerBatch
    }
    next - start
  }

  /**
   * Writes the rows [start, start + length) of a columnar batch, with the vectors that its columns
   * that are not copied were converted into from these rows, in their order. The copied columns are
   * sliced, which shares most buffers of the batch, though it allocates new offsets for lists, and
   * for strings and binaries unless the offset of the row `start` is 0, and new validity buffers,
   * and value buffers for booleans, unless `start` is a multiple of 8. Then each buffer of the
   * slice is copied at its size. Arrow Java reads the offsets of a vector of variable width, or of
   * a list, as if they started at 0, which those of a slice do. The converted vectors, which
   * nothing else writes to, are written as they are.
   */
  private def writeSlices(
      copied: Seq[Option[FieldVector]],
      converted: Seq[FieldVector],
      start: Int,
      length: Int): Unit = {
    // The slices of the copied columns share the buffers of the batch, which their producer
    // releases, so they are closed once they are copied.
    val slices = ArrayBuffer.empty[FieldVector]
    val copy = Utils.tryWithSafeFinally {
      copied.flatten.foreach { vector =>
        // The slice is closed also when the buffers that it allocates fail to allocate.
        val pair = vector.getTransferPair(vector.getAllocator)
        slices += pair.getTo.asInstanceOf[FieldVector]
        pair.splitAndTransfer(start, length)
      }
      copyRoot(new VectorSchemaRoot(slices.map(_.getField).asJava, slices.asJava, length))
    } {
      AutoCloseables.close(slices.asJava)
    }
    Utils.tryWithSafeFinally {
      val copies = copy.getFieldVectors.asScala.iterator
      val conversions = converted.iterator
      val vectors = copied.map { column =>
        if (column.isDefined) copies.next() else conversions.next()
      }
      delegate.write(new ColumnarBatch(
        vectors.map(new ArrowColumnVector(_): ColumnVector).toArray, length))
    } {
      copy.close()
    }
  }

  /** Copies the buffers of the vectors of a root into new vectors, once each, at their size. */
  private def copyRoot(source: VectorSchemaRoot): VectorSchemaRoot = {
    val copy = VectorSchemaRoot.create(source.getSchema, allocator)
    try {
      val unloaded = new VectorUnloader(source).getRecordBatch
      try {
        val buffers = ArrayBuffer.empty[ArrowBuf]
        try {
          unloaded.getBuffers.asScala.foreach { buffer =>
            val size = buffer.readableBytes()
            val copied = allocator.buffer(size)
            buffers += copied
            copied.setBytes(0, buffer, buffer.readerIndex(), size)
            copied.writerIndex(size)
          }
          val batch = new ArrowRecordBatch(source.getRowCount, unloaded.getNodes, buffers.asJava)
          try new VectorLoader(copy).load(batch) finally batch.close()
        } finally {
          buffers.foreach(_.close())
        }
      } finally {
        unloaded.close()
      }
      copy
    } catch {
      case e: Throwable =>
        copy.close()
        throw e
    }
  }

  private def writeArrow(batchRoot: VectorSchemaRoot): Unit = {
    val columns = batchRoot.getFieldVectors.asScala.map(new ArrowColumnVector(_): ColumnVector)
    delegate.write(new ColumnarBatch(columns.toArray, batchRoot.getRowCount))
  }

  private def flush(): Unit = {
    if (root != null) {
      arrowWriter.finish()
      Utils.tryWithSafeFinally(writeArrow(root))(releaseBatch())
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

object ArrowBatchingDataWriter {
  /**
   * Returns whether the Arrow vector of a column of a batch of `numRows` rows can be copied buffer
   * by buffer as a column of the field of a write: it has a value for each row, it is physically
   * congruent with the field, though it may differ in the names, nullability and metadata of its
   * fields, and in the time zones of timestamps, which the copy keeps, and Arrow can slice it.
   * Arrow computes some byte positions of a slice in int, so it cannot slice a fixed-width vector
   * of 2 GiB or more, for example (see `BaseSliceArrowOutputProcessor.canSlice`), whose values are
   * then converted.
   */
  def canCopy(vector: FieldVector, field: Field, numRows: Int): Boolean = {
    vector.getValueCount == numRows &&
      ArrowUtils.isCompatibleWithDeclaredField(vector.getField, field) &&
      BaseSliceArrowOutputProcessor.canSlice(vector)
  }

  /**
   * Returns the size in bytes of the rows [start, end) of a vector, as `getBufferSizeFor` measures
   * a slice of these rows, without slicing them: from the offsets of the vectors of variable width
   * and of the lists, at every level. The vectors of other types than those of `toArrowSchema` are
   * sliced.
   */
  def rangeSize(vector: ValueVector, start: Int, end: Int): Long = {
    val n = end - start
    def validity: Long = BitVectorHelper.getValidityBufferSizeFromCount(n)
    if (n == 0) {
      0L
    } else {
      vector match {
        case _: NullVector => 0L
        case _: BitVector => 2 * validity
        case v: BaseFixedWidthVector => n.toLong * v.getTypeWidth + validity
        case v: BaseVariableWidthVector =>
          validity + (n + 1L) * BaseVariableWidthVector.OFFSET_WIDTH +
            (v.getStartOffset(end) - v.getStartOffset(start))
        case v: ListVector =>
          val offsets = v.getOffsetBuffer
          val width = BaseRepeatedValueVector.OFFSET_WIDTH.toLong
          validity + (n + 1L) * width + rangeSize(v.getDataVector,
            offsets.getInt(start * width), offsets.getInt(end * width))
        case v: StructVector =>
          validity + v.getChildrenFromFields.asScala.map(rangeSize(_, start, end)).sum
        case v: NonNullableStructVector =>
          v.getChildrenFromFields.asScala.map(rangeSize(_, start, end)).sum
        case v =>
          val pair = v.getTransferPair(v.getAllocator)
          pair.splitAndTransfer(start, n)
          try pair.getTo.getBufferSizeFor(n).toLong finally pair.getTo.close()
      }
    }
  }
}
