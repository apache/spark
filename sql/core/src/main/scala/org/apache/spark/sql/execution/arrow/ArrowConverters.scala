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

package org.apache.spark.sql.execution.arrow

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, OutputStream}
import java.nio.channels.{Channels, ReadableByteChannel}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.arrow.compression.{Lz4CompressionCodec, ZstdCompressionCodec}
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.compression.{CompressionCodec, NoCompressionCodec}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter, ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, IpcOption, MessageSerializer}

import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.classic.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{ArrowUtils, CloseableIterator, ConcatenatingArrowStreamReader, MessageIterator}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.{ByteBufferOutputStream, SizeEstimator, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * Writes serialized ArrowRecordBatches to a DataOutputStream in the Arrow stream format.
 */
private[sql] class ArrowBatchStreamWriter(
    schema: StructType,
    out: OutputStream,
    timeZoneId: String,
    errorOnDuplicatedFieldNames: Boolean,
    largeVarTypes: Boolean) {

  val arrowSchema = ArrowUtils.toArrowSchema(
    schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes)
  val writeChannel = new WriteChannel(Channels.newChannel(out))

  // Write the Arrow schema first, before batches
  MessageSerializer.serialize(writeChannel, arrowSchema)

  /**
   * Consume iterator to write each serialized ArrowRecordBatch to the stream.
   */
  def writeBatches(arrowBatchIter: Iterator[Array[Byte]]): Unit = {
    arrowBatchIter.foreach(writeChannel.write)
  }

  /**
   * End the Arrow stream, does not close output stream.
   */
  def end(): Unit = {
    ArrowStreamWriter.writeEndOfStream(writeChannel, new IpcOption)
  }
}

private[sql] object ArrowConverters extends Logging {
  private[sql] class ArrowBatchIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean,
      context: TaskContext) extends Iterator[Array[Byte]] with AutoCloseable {

    protected val arrowSchema =
      ArrowUtils.toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes)
    private val allocator =
      ArrowUtils.rootAllocator.newChildAllocator(
        s"to${this.getClass.getSimpleName}", 0, Long.MaxValue)

    protected val root = VectorSchemaRoot.create(arrowSchema, allocator)

    // Create compression codec based on config
    private val compressionCodecName = SQLConf.get.arrowCompressionCodec
    private val codec = compressionCodecName match {
      case "none" => NoCompressionCodec.INSTANCE
      case "zstd" =>
        val compressionLevel = SQLConf.get.arrowZstdCompressionLevel
        val factory = CompressionCodec.Factory.INSTANCE
        val codecType = new ZstdCompressionCodec(compressionLevel).getCodecType()
        factory.createCodec(codecType)
      case "lz4" =>
        val factory = CompressionCodec.Factory.INSTANCE
        val codecType = new Lz4CompressionCodec().getCodecType()
        factory.createCodec(codecType)
      case other =>
        throw SparkException.internalError(
          s"Unsupported Arrow compression codec: $other. Supported values: none, zstd, lz4")
    }
    protected val unloader = new VectorUnloader(root, true, codec, true)
    protected val arrowWriter = ArrowWriter.create(root)

    Option(context).foreach {_.addTaskCompletionListener[Unit] { _ =>
      close()
    }}

    override def hasNext: Boolean = rowIter.hasNext || {
      close()
      false
    }

    override def next(): Array[Byte] = {
      var bytes: Array[Byte] = null

      Utils.tryWithSafeFinally {
        var rowCount = 0L
        while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
          val row = rowIter.next()
          arrowWriter.write(row)
          rowCount += 1
        }
        arrowWriter.finish()
        val batch = unloader.getRecordBatch()
        bytes = serializeBatch(batch)
        batch.close()
      } {
        arrowWriter.reset()
      }

      bytes
    }

    override def close(): Unit = {
        root.close()
        allocator.close()
    }
  }

  private[sql] class ArrowBatchWithSchemaIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      maxEstimatedBatchSize: Long,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean,
      context: TaskContext)
    extends ArrowBatchIterator(
      rowIter,
      schema,
      maxRecordsPerBatch,
      timeZoneId,
      errorOnDuplicatedFieldNames,
      largeVarTypes,
      context) {

    private val arrowSchemaSize = SizeEstimator.estimate(arrowSchema)
    var rowCountInLastBatch: Long = 0

    override def next(): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val writeChannel = new WriteChannel(Channels.newChannel(out))

      rowCountInLastBatch = 0
      var estimatedBatchSize = arrowSchemaSize
      Utils.tryWithSafeFinally {
        // Always write the schema.
        MessageSerializer.serialize(writeChannel, arrowSchema)

        def isBatchSizeLimitExceeded: Boolean = {
          // If `maxEstimatedBatchSize` is zero or negative, it implies unlimited.
          maxEstimatedBatchSize > 0 && estimatedBatchSize >= maxEstimatedBatchSize
        }
        def isRecordLimitExceeded: Boolean = {
          // If `maxRecordsPerBatch` is zero or negative, it implies unlimited.
          maxRecordsPerBatch > 0 && rowCountInLastBatch >= maxRecordsPerBatch
        }
        // Always write the first row.
        while (rowIter.hasNext && (
          // If the size in bytes is positive (set properly), always write the first row.
          (rowCountInLastBatch == 0 && maxEstimatedBatchSize > 0) ||
            // If either limit is hit, create a batch. This implies that the limit that is hit first
            // triggers the creation of a batch even if the other limit is not yet hit, hence
            // preferring the more restrictive limit.
            (!isBatchSizeLimitExceeded && !isRecordLimitExceeded))) {
          val row = rowIter.next()
          arrowWriter.write(row)
          estimatedBatchSize += (row match {
            case ur: UnsafeRow => ur.getSizeInBytes
            // Trying to estimate the size of the current row, assuming 16 bytes per value.
            case ir: InternalRow => ir.numFields * 16
          })
          rowCountInLastBatch += 1
        }
        arrowWriter.finish()
        val batch = unloader.getRecordBatch()
        MessageSerializer.serialize(writeChannel, batch)

        // Always write the Ipc options at the end.
        ArrowStreamWriter.writeEndOfStream(writeChannel, IpcOption.DEFAULT)

        batch.close()
      } {
        arrowWriter.reset()
      }

      out.toByteArray
    }
  }

  /**
   * Maps Iterator from InternalRow to serialized ArrowRecordBatches. Limit ArrowRecordBatch size
   * in a batch by setting maxRecordsPerBatch or use 0 to fully consume rowIter.
   */
  private[sql] def toBatchIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean,
      context: TaskContext): ArrowBatchIterator = {
    new ArrowBatchIterator(
      rowIter,
      schema,
      maxRecordsPerBatch,
      timeZoneId,
      errorOnDuplicatedFieldNames,
      largeVarTypes,
      context)
  }

  /**
   * Convert the input rows into fully contained arrow batches.
   * Different from [[toBatchIterator]], each output arrow batch starts with the schema.
   */
  private[sql] def toBatchWithSchemaIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      maxEstimatedBatchSize: Long,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean): ArrowBatchWithSchemaIterator = {
    new ArrowBatchWithSchemaIterator(
      rowIter, schema, maxRecordsPerBatch, maxEstimatedBatchSize,
      timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes, TaskContext.get())
  }

  private[sql] def createEmptyArrowBatch(
      schema: StructType,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean): Array[Byte] = {
    val batches = new ArrowBatchWithSchemaIterator(
        Iterator.empty, schema, 0L, 0L,
        timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes, TaskContext.get()) {
      override def hasNext: Boolean = true
    }
    Utils.tryWithSafeFinally {
      batches.next()
    } {
      // If taskContext is null, `batches.close()` should be called to avoid memory leak.
      if (TaskContext.get() == null) {
        batches.close()
      }
    }
  }

  /**
   * This is a class that converts input data in the form of a Byte array to InternalRow instances
   * implementing the Iterator interface.
   *
   * The input data must be a valid Arrow IPC stream, this means that the first message is always
   * the schema followed by N record batches.
   *
   * @param input Input Data
   * @param context Task Context for Spark
   */
  private[sql] class InternalRowIteratorFromIPCStream(
      ipcStreams: Iterator[Array[Byte]],
      context: TaskContext)
    extends CloseableIterator[InternalRow] {

    // Create an allocator used for all Arrow related memory.
    protected val allocator: BufferAllocator = ArrowUtils.rootAllocator.newChildAllocator(
      s"to${this.getClass.getSimpleName}",
      0,
      Long.MaxValue)

    private val reader = {
      val messages = ipcStreams.map { bytes =>
        new MessageIterator(new ByteArrayInputStream(bytes), allocator)
      }
      new ConcatenatingArrowStreamReader(allocator, messages, destructive = true)
    }

    lazy val schema: StructType = try {
      ArrowUtils.fromArrowSchema(reader.getVectorSchemaRoot.getSchema)
    } catch {
      case NonFatal(e) =>
        // Since this triggers a read (which involves allocating buffers) we have to clean-up.
        close()
        throw e
    }

    private var rowIterator: Iterator[InternalRow] = Iterator.empty

    // Metrics to track batch processing
    private var _batchesLoaded: Int = 0
    private var _totalRowsProcessed: Long = 0L

    if (context != null) {
      context.addTaskCompletionListener[Unit] { _ =>
        close()
      }
    }

    // Public accessors for metrics
    def batchesLoaded: Int = _batchesLoaded
    def totalRowsProcessed: Long = _totalRowsProcessed
    def allocatedMemory: Long = allocator.getAllocatedMemory
    def peakMemoryAllocation: Long = allocator.getPeakMemoryAllocation

    override def hasNext: Boolean = {
      while (!rowIterator.hasNext) {
        if (reader.loadNextBatch()) {
          rowIterator = vectorSchemaRootToIter(reader.getVectorSchemaRoot)
          _batchesLoaded += 1
        } else {
          close()
          return false
        }
      }
      true
    }

    override def next(): InternalRow = {
      if (!hasNext) {
        throw new NoSuchElementException("No more elements in iterator")
      }
      _totalRowsProcessed += 1
      rowIterator.next()
    }

    override def close(): Unit = {
      closeAll(reader, allocator)
    }
  }

  /**
   * An InternalRow iterator which parse data from serialized ArrowRecordBatches, subclass should
   * implement [[nextBatch]] to parse data from binary records.
   */
  private[sql] abstract class InternalRowIterator(
      arrowBatchIter: Iterator[Array[Byte]],
      context: TaskContext)
      extends Iterator[InternalRow] {
    // Keep all the resources we have opened in order, should be closed in reverse order finally.
    val resources = new ArrayBuffer[AutoCloseable]()
    protected val allocator: BufferAllocator = ArrowUtils.rootAllocator.newChildAllocator(
      s"to${this.getClass.getSimpleName}",
      0,
      Long.MaxValue)
    resources.append(allocator)

    private var rowIterAndSchema =
      if (arrowBatchIter.hasNext) nextBatch() else (Iterator.empty, null)
    // We will ensure schemas parsed from every batch are the same.
    val schema: StructType = rowIterAndSchema._2

    if (context != null) context.addTaskCompletionListener[Unit] { _ =>
      closeAll(resources.toSeq.reverse: _*)
    }

    override def hasNext: Boolean = rowIterAndSchema._1.hasNext || {
      if (arrowBatchIter.hasNext) {
        rowIterAndSchema = nextBatch()
        if (schema != rowIterAndSchema._2) {
          throw new IllegalArgumentException(
            s"ArrowBatch iterator contain 2 batches with" +
              s" different schema: $schema and ${rowIterAndSchema._2}")
        }
        rowIterAndSchema._1.hasNext
      } else {
        closeAll(resources.toSeq.reverse: _*)
        false
      }
    }

    override def next(): InternalRow = rowIterAndSchema._1.next()

    def nextBatch(): (Iterator[InternalRow], StructType)
  }

  /**
   * Parse data from serialized ArrowRecordBatches, the [[arrowBatchIter]] only contains serialized
   * arrow batch records, the schema is passed in through [[schema]].
   */
  private[sql] class InternalRowIteratorWithoutSchema(
      arrowBatchIter: Iterator[Array[Byte]],
      schema: StructType,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean,
      context: TaskContext)
      extends InternalRowIterator(arrowBatchIter, context) {

    override def nextBatch(): (Iterator[InternalRow], StructType) = {
      val arrowSchema =
        ArrowUtils.toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      resources.append(root)
      val arrowRecordBatch = ArrowConverters.loadBatch(arrowBatchIter.next(), allocator)
      val vectorLoader = new VectorLoader(root)
      vectorLoader.load(arrowRecordBatch)
      arrowRecordBatch.close()
      (vectorSchemaRootToIter(root), schema)
    }
  }

  /**
   * Parse data from serialized ArrowRecordBatches, the arrowBatch in [[arrowBatchIter]] starts with
   * the schema so we should parse schema from it first.
   */
  private[sql] class InternalRowIteratorWithSchema(
      arrowBatchIter: Iterator[Array[Byte]],
      context: TaskContext)
      extends InternalRowIterator(arrowBatchIter, context) {
    override def nextBatch(): (Iterator[InternalRow], StructType) = {
      val reader =
        new ArrowStreamReader(new ByteArrayInputStream(arrowBatchIter.next()), allocator)
      val root = if (reader.loadNextBatch()) reader.getVectorSchemaRoot else null
      resources.appendAll(Seq(reader, root))
      if (root == null) {
        (Iterator.empty, null)
      } else {
        (vectorSchemaRootToIter(root), ArrowUtils.fromArrowSchema(root.getSchema))
      }
    }
  }

  /**
   * Maps iterator from serialized ArrowRecordBatches to InternalRows.
   */
  private[sql] def fromBatchIterator(
      arrowBatchIter: Iterator[Array[Byte]],
      schema: StructType,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean,
      context: TaskContext): Iterator[InternalRow] = {
    new InternalRowIteratorWithoutSchema(
      arrowBatchIter, schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes, context
    )
  }

  /**
   * Maps iterator from serialized ArrowRecordBatches to InternalRows. Different from
   * [[fromBatchIterator]], each input arrow batch starts with the schema.
   */
  private[sql] def fromBatchWithSchemaIterator(
      arrowBatchIter: Iterator[Array[Byte]],
      context: TaskContext): (Iterator[InternalRow], StructType) = {
    val iterator = new InternalRowIteratorWithSchema(arrowBatchIter, context)
    (iterator, iterator.schema)
  }

  /**
   * Creates an iterator from a Byte array to deserialize an Arrow IPC stream with exactly
   * one schema and a varying number of record batches. Returns an iterator over the
   * created InternalRow.
   */
  private[sql] def fromIPCStream(input: Array[Byte]):
    (CloseableIterator[InternalRow], StructType) = {
    fromIPCStream(Iterator.single(input))
  }

  private[sql] def fromIPCStream(inputs: Iterator[Array[Byte]]):
    (CloseableIterator[InternalRow], StructType) = {
    val iterator = new InternalRowIteratorFromIPCStream(inputs, null)
    (iterator, iterator.schema)
  }

  // Overloaded method for tests to access the iterator with metrics
  private[sql] def fromIPCStreamWithIterator(input: Array[Byte], context: TaskContext):
    (InternalRowIteratorFromIPCStream, StructType) = {
    val iterator = new InternalRowIteratorFromIPCStream(Iterator.single(input), context)
    (iterator, iterator.schema)
  }

  /**
   * Convert an arrow batch container into an iterator of InternalRow.
   */
  private def vectorSchemaRootToIter(root: VectorSchemaRoot): Iterator[InternalRow] = {
    val columns = root.getFieldVectors.asScala.map { vector =>
      new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
    }.toArray

    val batch = new ColumnarBatch(columns)
    batch.setNumRows(root.getRowCount)
    batch.rowIterator().asScala
  }

  /**
   * Load a serialized ArrowRecordBatch.
   */
  private[arrow] def loadBatch(
      batchBytes: Array[Byte],
      allocator: BufferAllocator): ArrowRecordBatch = {
    val in = new ByteArrayInputStream(batchBytes)
    MessageSerializer.deserializeRecordBatch(
      new ReadChannel(Channels.newChannel(in)), allocator)  // throws IOException
  }

  private[arrow] def serializeBatch(batch: ArrowRecordBatch): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val writeChannel = new WriteChannel(Channels.newChannel(out))
    MessageSerializer.serialize(writeChannel, batch)
    out.toByteArray
  }

  /**
   * Create a DataFrame from an iterator of serialized ArrowRecordBatches.
   */
  def toDataFrame(
      arrowBatches: Iterator[Array[Byte]],
      schemaString: String,
      session: SparkSession): DataFrame = {
    toDataFrame(
      arrowBatches,
      DataType.fromJson(schemaString).asInstanceOf[StructType],
      session,
      session.sessionState.conf.sessionLocalTimeZone,
      false,
      session.sessionState.conf.arrowUseLargeVarTypes)
  }

  /**
   * Create a DataFrame from an iterator of serialized ArrowRecordBatches.
   */
  private[sql] def toDataFrame(
      arrowBatches: Iterator[Array[Byte]],
      schema: StructType,
      session: SparkSession,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean): DataFrame = {
    val attrs = toAttributes(schema)
    val batchesInDriver = arrowBatches.toArray
    val shouldUseRDD = session.sessionState.conf
      .arrowLocalRelationThreshold < batchesInDriver.map(_.length.toLong).sum

    if (shouldUseRDD) {
      logDebug("Using RDD-based createDataFrame with Arrow optimization.")
      val rdd = session.sparkContext
        .parallelize(batchesInDriver.toImmutableArraySeq, batchesInDriver.length)
        .mapPartitions { batchesInExecutors =>
          ArrowConverters.fromBatchIterator(
            batchesInExecutors,
            schema,
            timeZoneId,
            errorOnDuplicatedFieldNames,
            largeVarTypes,
            TaskContext.get())
        }
      session.internalCreateDataFrame(rdd.setName("arrow"), schema)
    } else {
      logDebug("Using LocalRelation in createDataFrame with Arrow optimization.")
      val data = ArrowConverters.fromBatchIterator(
        batchesInDriver.iterator,
        schema,
        timeZoneId,
        errorOnDuplicatedFieldNames,
        largeVarTypes,
        TaskContext.get())

      // Project/copy it. Otherwise, the Arrow column vectors will be closed and released out.
      val proj = UnsafeProjection.create(attrs, attrs)
      Dataset.ofRows(session,
        LocalRelation(attrs, data.map(r => proj(r).copy()).toArray.toImmutableArraySeq))
    }
  }

  /**
   * Read a file as an Arrow stream and return an array of serialized ArrowRecordBatches.
   */
  private[sql] def readArrowStreamFromFile(filename: String): Array[Array[Byte]] = {
    Utils.tryWithResource(new FileInputStream(filename)) { fileStream =>
      // Create array to consume iterator so that we can safely close the file
      getBatchesFromStream(fileStream.getChannel).toArray
    }
  }

  /**
   * Read an Arrow stream input and return an iterator of serialized ArrowRecordBatches.
   */
  private[sql] def getBatchesFromStream(in: ReadableByteChannel): Iterator[Array[Byte]] = {

    // Iterate over the serialized Arrow RecordBatch messages from a stream
    new Iterator[Array[Byte]] {
      var batch: Array[Byte] = readNextBatch()

      override def hasNext: Boolean = batch != null

      override def next(): Array[Byte] = {
        val prevBatch = batch
        batch = readNextBatch()
        prevBatch
      }

      // This gets the next serialized ArrowRecordBatch by reading message metadata to check if it
      // is a RecordBatch message and then returning the complete serialized message which consists
      // of a int32 length, serialized message metadata and a serialized RecordBatch message body
      @scala.annotation.tailrec
      def readNextBatch(): Array[Byte] = {
        val msgMetadata = MessageSerializer.readMessage(new ReadChannel(in))
        if (msgMetadata == null) {
          return null
        }

        // Get the length of the body, which has not been read at this point
        val bodyLength = msgMetadata.getMessageBodyLength.toInt

        // Only care about RecordBatch messages, skip Schema and unsupported Dictionary messages
        if (msgMetadata.getMessage.headerType() == MessageHeader.RecordBatch) {

          // Buffer backed output large enough to hold 8-byte length + complete serialized message
          val bbout = new ByteBufferOutputStream(8 + msgMetadata.getMessageLength + bodyLength)

          // Write message metadata to ByteBuffer output stream
          MessageSerializer.writeMessageBuffer(
            new WriteChannel(Channels.newChannel(bbout)),
            msgMetadata.getMessageLength,
            msgMetadata.getMessageBuffer)

          // Get a zero-copy ByteBuffer with already contains message metadata, must close first
          bbout.close()
          val bb = bbout.toByteBuffer
          bb.position(bbout.getCount())

          // Read message body directly into the ByteBuffer to avoid copy, return backed byte array
          bb.limit(bb.capacity())
          JavaUtils.readFully(in, bb)
          bb.array()
        } else {
          if (bodyLength > 0) {
            // Skip message body if not a RecordBatch
            Channels.newInputStream(in).skip(bodyLength)
          }

          // Proceed to next message
          readNextBatch()
        }
      }
    }
  }

  private def closeAll(closeables: AutoCloseable*): Unit = {
    for (closeable <- closeables) {
      if (closeable != null) {
        closeable.close()
      }
    }
  }
}
