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
import java.nio.ByteBuffer
import java.nio.channels.{Channels, SeekableByteChannel}

import scala.collection.JavaConverters._

import org.apache.arrow.flatbuf.{Message, MessageHeader}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}

import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils


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
    arrowBatchIter.foreach { batchBytes =>
      writeChannel.write(batchBytes)
    }
  }

  /**
   * End the Arrow stream, does not close output stream.
   */
  def end(): Unit = {
    // Write End of Stream
    // TODO: this could be a static function in ArrowStreamWriter
    writeChannel.writeIntLittleEndian(0)
  }
}

private[sql] object ArrowConverters {

  /**
   * Maps Iterator from InternalRow to serialized ArrowRecordBatches. Limit ArrowRecordBatch size
   * in a batch by setting maxRecordsPerBatch or use 0 to fully consume rowIter.
   */
  private[sql] def toBatchIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Int,
      timeZoneId: String,
      context: TaskContext): Iterator[Array[Byte]] = {

    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("toBatchIterator", 0, Long.MaxValue)

    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val unloader = new VectorUnloader(root)
    val arrowWriter = ArrowWriter.create(root)

    context.addTaskCompletionListener { _ =>
      root.close()
      allocator.close()
    }

    new Iterator[Array[Byte]] {

      override def hasNext: Boolean = rowIter.hasNext || {
        root.close()
        allocator.close()
        false
      }

      override def next(): Array[Byte] = {
        val out = new ByteArrayOutputStream()
        val writeChannel = new WriteChannel(Channels.newChannel(out))

        Utils.tryWithSafeFinally {
          var rowCount = 0
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
  }

  /**
   * Maps iterator from serialized ArrowRecordBatches to InternalRows.
   */
  private[sql] def fromBatchIterator(
      arrowBatchIter: Iterator[Array[Byte]],
      schema: StructType,
      timeZoneId: String,
      context: TaskContext): Iterator[InternalRow] = {
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("fromBatchIterator", 0, Long.MaxValue)

    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)

    new Iterator[InternalRow] {
      private var rowIter = if (arrowBatchIter.hasNext) nextBatch() else Iterator.empty

      context.addTaskCompletionListener { _ =>
        root.close()
        allocator.close()
      }

      override def hasNext: Boolean = rowIter.hasNext || {
        if (arrowBatchIter.hasNext) {
          rowIter = nextBatch()
          true
        } else {
          root.close()
          allocator.close()
          false
        }
      }

      override def next(): InternalRow = rowIter.next()

      private def nextBatch(): Iterator[InternalRow] = {
        val arrowRecordBatch = ArrowConverters.loadBatch(arrowBatchIter.next(), allocator)
        val vectorLoader = new VectorLoader(root)
        vectorLoader.load(arrowRecordBatch)
        arrowRecordBatch.close()

        val columns = root.getFieldVectors.asScala.map { vector =>
          new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
        }.toArray

        val batch = new ColumnarBatch(columns)
        batch.setNumRows(root.getRowCount)
        batch.rowIterator().asScala
      }
    }
  }

  /**
   * Load a serialized ArrowRecordBatch.
   */
  private[arrow] def loadBatch(
      batchBytes: Array[Byte],
      allocator: BufferAllocator): ArrowRecordBatch = {
    val in = new ByteArrayInputStream(batchBytes)
    MessageSerializer.deserializeMessageBatch(new ReadChannel(Channels.newChannel(in)), allocator)
      .asInstanceOf[ArrowRecordBatch]  // throws IOException
  }

  /**
   * Create a DataFrame from a JavaRDD of serialized ArrowRecordBatches.
   */
  private[sql] def toDataFrame(
      arrowBatchRDD: JavaRDD[Array[Byte]],
      schemaString: String,
      sqlContext: SQLContext): DataFrame = {
    val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
    val timeZoneId = sqlContext.sessionState.conf.sessionLocalTimeZone
    val rdd = arrowBatchRDD.rdd.mapPartitions { iter =>
      val context = TaskContext.get()
      ArrowConverters.fromBatchIterator(iter, schema, timeZoneId, context)
    }
    sqlContext.internalCreateDataFrame(rdd, schema)
  }

  /**
   * Read a file as an Arrow stream and return an RDD of serialized ArrowRecordBatches.
   */
  private[sql] def readArrowStreamFromFile(sqlContext: SQLContext, filename: String):
  JavaRDD[Array[Byte]] = {
    val fileStream = new FileInputStream(filename)
    try {
      // Create array so that we can safely close the file
      val batches = getBatchesFromStream(fileStream.getChannel).toArray
      // Parallelize the record batches to create an RDD
      JavaRDD.fromRDD(sqlContext.sparkContext.parallelize(batches, batches.length))
    } finally {
      fileStream.close()
    }
  }

  /**
   * Read an Arrow stream input and return an iterator of serialized ArrowRecordBatches.
   */
  private[sql] def getBatchesFromStream(in: SeekableByteChannel): Iterator[Array[Byte]] = {

    // TODO: simplify in super class
    class RecordBatchMessageReader(inputChannel: SeekableByteChannel) {
      // TODO: need ReadChannel to be protected
      // extends MessageChannelReader(new ReadChannel(fileChannel)) {
      val in = new ReadChannel(inputChannel)
      private var lastBatch: Array[Byte] = null

      def getLastBatch() = lastBatch

      def readNextMessage(): Message = {
        lastBatch = null
        val buffer = ByteBuffer.allocate(4)
        if (in.readFully(buffer) != 4) {
          return null
        }
        val messageLength = MessageSerializer.bytesToInt(buffer.array())
        if (messageLength == 0) {
          return null
        }

        loadMessageOverride(messageLength, ByteBuffer.allocate(messageLength))
      }

      protected def loadMessage(messageLength: Int, buffer: ByteBuffer): Message = {
        if (in.readFully(buffer) != messageLength) {
          throw new java.io.IOException(
            "Unexpected end of stream trying to read message.")
        }
        buffer.rewind()
        Message.getRootAsMessage(buffer)
      }

      protected def loadMessageOverride(messageLength: Int, buffer: ByteBuffer): Message = {
        val msg = loadMessage(messageLength, buffer)
        val bodyLength = msg.bodyLength().asInstanceOf[Int]

        if (msg.headerType() == MessageHeader.RecordBatch) {
          val allbuf = ByteBuffer.allocate(4 + messageLength + bodyLength)
          allbuf.put(WriteChannel.intToBytes(messageLength))
          allbuf.put(buffer)
          in.readFully(allbuf)
          lastBatch = allbuf.array()
        } else if (bodyLength > 0) {
          // Skip message body if not a record batch
          inputChannel.position(inputChannel.position() + bodyLength)
        }

        msg
      }
    }

    // Create an iterator to get each serialized ArrowRecordBatch in an stream
    new Iterator[Array[Byte]] {
      val msgReader = new RecordBatchMessageReader(in)
      var batch: Array[Byte] = null
      readNextBatch()

      override def hasNext: Boolean = batch != null

      override def next(): Array[Byte] = {
        val prevBatch = batch
        readNextBatch()
        prevBatch
      }

      def readNextBatch(): Unit = {
        var stop = false
        while (!stop) {
          val msg = msgReader.readNextMessage()
          batch = msgReader.getLastBatch()
          if (msg == null || (msg != null && batch != null)) {
            stop = true
          }
        }
      }
    }
  }
}
