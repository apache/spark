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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter, ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, IpcOption, MessageSerializer}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.{ByteBufferOutputStream, SizeEstimator, Utils}


/**
 * Writes serialized ArrowRecordBatches to a DataOutputStream in the Arrow stream format.
 */
private[sql] class ArrowBatchStreamWriter(
    schema: StructType,
    out: OutputStream,
    timeZoneId: String) {

  val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
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
      context: TaskContext) extends Iterator[Array[Byte]] {

    protected val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    private val allocator =
      ArrowUtils.rootAllocator.newChildAllocator(
        s"to${this.getClass.getSimpleName}", 0, Long.MaxValue)

    private val root = VectorSchemaRoot.create(arrowSchema, allocator)
    protected val unloader = new VectorUnloader(root)
    protected val arrowWriter = ArrowWriter.create(root)

    Option(context).foreach {_.addTaskCompletionListener[Unit] { _ =>
      root.close()
      allocator.close()
    }}

    override def hasNext: Boolean = rowIter.hasNext || {
      root.close()
      allocator.close()
      false
    }

    override def next(): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val writeChannel = new WriteChannel(Channels.newChannel(out))

      Utils.tryWithSafeFinally {
        var rowCount = 0L
        while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
          val row = rowIter.next()
          arrowWriter.write(row)
          rowCount += 1
        }
        arrowWriter.finish()
        val batch = unloader.getRecordBatch()
        MessageSerializer.serialize(writeChannel, batch)
        batch.close()
      } {
        arrowWriter.reset()
      }

      out.toByteArray
    }
  }

  private[sql] class ArrowBatchWithSchemaIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      maxEstimatedBatchSize: Long,
      timeZoneId: String,
      context: TaskContext)
    extends ArrowBatchIterator(
      rowIter, schema, maxRecordsPerBatch, timeZoneId, context) {

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

        // Always write the first row.
        while (rowIter.hasNext && (
          // For maxBatchSize and maxRecordsPerBatch, respect whatever smaller.
          // If the size in bytes is positive (set properly), always write the first row.
          rowCountInLastBatch == 0 && maxEstimatedBatchSize > 0 ||
            // If the size in bytes of rows are 0 or negative, unlimit it.
            estimatedBatchSize <= 0 ||
            estimatedBatchSize < maxEstimatedBatchSize ||
            // If the size of rows are 0 or negative, unlimit it.
            maxRecordsPerBatch <= 0 ||
            rowCountInLastBatch < maxRecordsPerBatch)) {
          val row = rowIter.next()
          arrowWriter.write(row)
          estimatedBatchSize += row.asInstanceOf[UnsafeRow].getSizeInBytes
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
      context: TaskContext): ArrowBatchIterator = {
    new ArrowBatchIterator(
      rowIter, schema, maxRecordsPerBatch, timeZoneId, context)
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
      timeZoneId: String): ArrowBatchWithSchemaIterator = {
    new ArrowBatchWithSchemaIterator(
      rowIter, schema, maxRecordsPerBatch, maxEstimatedBatchSize, timeZoneId, TaskContext.get)
  }

  private[sql] def createEmptyArrowBatch(
      schema: StructType,
      timeZoneId: String): Array[Byte] = {
    new ArrowBatchWithSchemaIterator(
        Iterator.empty, schema, 0L, 0L, timeZoneId, TaskContext.get) {
      override def hasNext: Boolean = true
    }.next()
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
      context: TaskContext)
      extends InternalRowIterator(arrowBatchIter, context) {

    override def nextBatch(): (Iterator[InternalRow], StructType) = {
      val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
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
      resources.append(reader, root)
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
      context: TaskContext): Iterator[InternalRow] = new InternalRowIteratorWithoutSchema(
    arrowBatchIter, schema, timeZoneId, context
  )

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

  /**
   * Create a DataFrame from an iterator of serialized ArrowRecordBatches.
   */
  /**
   * Create a DataFrame from an iterator of serialized ArrowRecordBatches.
   */
  def toDataFrame(
      arrowBatches: Iterator[Array[Byte]],
      schemaString: String,
      session: SparkSession): DataFrame = {
    val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
    val attrs = schema.toAttributes
    val batchesInDriver = arrowBatches.toArray
    val shouldUseRDD = session.sessionState.conf
      .arrowLocalRelationThreshold < batchesInDriver.map(_.length.toLong).sum

    if (shouldUseRDD) {
      logDebug("Using RDD-based createDataFrame with Arrow optimization.")
      val timezone = session.sessionState.conf.sessionLocalTimeZone
      val rdd = session.sparkContext.parallelize(batchesInDriver, batchesInDriver.length)
        .mapPartitions { batchesInExecutors =>
          ArrowConverters.fromBatchIterator(
            batchesInExecutors,
            schema,
            timezone,
            TaskContext.get())
        }
      session.internalCreateDataFrame(rdd.setName("arrow"), schema)
    } else {
      logDebug("Using LocalRelation in createDataFrame with Arrow optimization.")
      val data = ArrowConverters.fromBatchIterator(
        batchesInDriver.toIterator,
        schema,
        session.sessionState.conf.sessionLocalTimeZone,
        TaskContext.get())

      // Project/copy it. Otherwise, the Arrow column vectors will be closed and released out.
      val proj = UnsafeProjection.create(attrs, attrs)
      Dataset.ofRows(session, LocalRelation(attrs, data.map(r => proj(r).copy()).toArray))
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
