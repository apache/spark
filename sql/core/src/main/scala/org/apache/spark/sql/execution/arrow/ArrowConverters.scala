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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
import java.nio.channels.Channels
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter, ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}

import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Writes serialized ArrowRecordBatches to a DataOutputStream in the Arrow stream format
 */
private[sql] class ArrowBatchStreamWriter(
    schema: StructType,
    out: OutputStream,
    timeZoneId: String) {

  val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
  val writeChannel = new WriteChannel(Channels.newChannel(out))
  MessageSerializer.serialize(writeChannel, arrowSchema)

  def writeBatches(arrowBatchIter: Iterator[Array[Byte]]): Unit = {
    arrowBatchIter.foreach { batchBytes =>
      writeChannel.write(batchBytes)
    }
  }

  def close(): Unit = {
    // Write End of Stream
    writeChannel.writeIntLittleEndian(0)
  }
}


/**
 * Iterator interface to iterate over Arrow record batches and return rows
 */
private[sql] trait ArrowRowIterator extends Iterator[InternalRow] {

  /**
   * Return the schema loaded from the Arrow record batch being iterated over
   */
  def schema: StructType
}


private[sql] object ArrowConverters {

  /**
   * Maps Iterator from InternalRow to Arrow batches as byte arrays. Limit ArrowRecordBatch size
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
   * Maps iterator from Arrow batches as byte arrays to InternalRows.
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
   * Maps Iterator from InternalRow to Arrow stream format as a byte array. Each Arrow stream
   * will have 1 ArrowRecordBatch. Limit ArrowRecordBatch size in a batch by setting
   * maxRecordsPerBatch or use 0 to fully consume rowIter. Once this limit is reach, a new Arrow
   * stream will be started.
   */
  private[sql] def toStreamIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Int,
      timeZoneId: String,
      context: TaskContext): Iterator[Array[Byte]] = {

    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("toStreamIterator", 0, Long.MaxValue)

    val root = VectorSchemaRoot.create(arrowSchema, allocator)
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
        val writer = new ArrowStreamWriter(root, null, out)
        writer.start()

        Utils.tryWithSafeFinally {
          var rowCount = 0
          while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
            val row = rowIter.next()
            arrowWriter.write(row)
            rowCount += 1
          }
          arrowWriter.finish()
          writer.writeBatch()
        } {
          writer.close()
          arrowWriter.reset()
        }

        out.toByteArray
      }
    }
  }

  /**
   * Maps Iterator from Arrow stream format to InternalRow. Returns an ArrowRowIterator that can
   * iterate over record batch rows and has the schema from the first batch of Arrow data read.
   */
   private[sql] def fromStreamIterator(
      arrowStreamIter: Iterator[Array[Byte]],
      context: TaskContext): ArrowRowIterator = {
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("fromStreamIterator", 0, Long.MaxValue)

    new ArrowRowIterator {
      private var reader: ArrowStreamReader = null
      private var schemaRead = StructType(Seq.empty)
      private var rowIter = if (arrowStreamIter.hasNext) nextStream() else Iterator.empty

      context.addTaskCompletionListener { _ =>
        closeReader()
        allocator.close()
      }

      override def schema: StructType = schemaRead

      override def hasNext: Boolean = rowIter.hasNext || {
        if (reader != null && reader.loadNextBatch()) {
          rowIter = nextBatch(reader.getVectorSchemaRoot)
          true
        }
        else if (arrowStreamIter.hasNext) {
          closeReader()
          rowIter = nextStream()
          true
        } else {
          closeReader()
          allocator.close()
          false
        }
      }

      override def next(): InternalRow = rowIter.next()

      private def closeReader(): Unit = {
        if (reader != null) {
          reader.close()
          reader = null
        }
      }

      private def nextStream(): Iterator[InternalRow] = {
        val in = new ByteArrayInputStream(arrowStreamIter.next())
        reader = new ArrowStreamReader(in, allocator)
        reader.loadNextBatch()  // throws IOException
        val root = reader.getVectorSchemaRoot  // throws IOException
        schemaRead = ArrowUtils.fromArrowSchema(root.getSchema)
        nextBatch(root)
      }

      private def nextBatch(root: VectorSchemaRoot): Iterator[InternalRow] = {
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
   * Convert a byte array to an ArrowRecordBatch.
   */
  private[arrow] def loadBatch(
      batchBytes: Array[Byte],
      allocator: BufferAllocator): ArrowRecordBatch = {
    val in = new ByteArrayInputStream(batchBytes)
    MessageSerializer.deserializeMessageBatch(new ReadChannel(Channels.newChannel(in)), allocator)
      .asInstanceOf[ArrowRecordBatch]  // throws IOException
  }

  /**
   * Convert a JavaRDD of Arrow streams as byte arrays to a DataFrame
   */
  private[sql] def toDataFrame(
      arrowStreamRDD: JavaRDD[Array[Byte]],
      schemaString: String,
      sqlContext: SQLContext): DataFrame = {
    val rdd = arrowStreamRDD.rdd.mapPartitions { iter =>
      val context = TaskContext.get()
      ArrowConverters.fromStreamIterator(iter, context)
    }
    val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
    sqlContext.internalCreateDataFrame(rdd, schema)
  }

  /**
   * Read files entirely and parallelize into a JavaRDD with 1 partition per file
   */
  private[sql] def readArrowStreamFromFiles(sqlContext: SQLContext, filenames: Array[String]):
  JavaRDD[Array[Byte]] = {
    val fileData = filenames.map { filename =>
      Files.readAllBytes(Paths.get(filename))  // throws IOException
    }
    JavaRDD.fromRDD(sqlContext.sparkContext.parallelize(fileData, filenames.length))
  }
}
