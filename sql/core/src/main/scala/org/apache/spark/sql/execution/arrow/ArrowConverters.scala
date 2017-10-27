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

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

import scala.collection.JavaConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.file._
import org.apache.arrow.vector.schema.ArrowRecordBatch
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel

import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * Store Arrow data in a form that can be serialized by Spark and served to a Python process.
 */
private[sql] class ArrowPayload private[sql] (payload: Array[Byte]) extends Serializable {

  /**
   * Convert the ArrowPayload to an ArrowRecordBatch.
   */
  def loadBatch(allocator: BufferAllocator): ArrowRecordBatch = {
    ArrowConverters.byteArrayToBatch(payload, allocator)
  }

  /**
   * Get the ArrowPayload as a type that can be served to Python.
   */
  def asPythonSerializable: Array[Byte] = payload
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
   * Maps Iterator from InternalRow to ArrowPayload. Limit ArrowRecordBatch size in ArrowPayload
   * by setting maxRecordsPerBatch or use 0 to fully consume rowIter.
   */
  private[sql] def toPayloadIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Int,
      timeZoneId: String,
      context: TaskContext): Iterator[ArrowPayload] = {

    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("toPayloadIterator", 0, Long.MaxValue)

    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = ArrowWriter.create(root)

    var closed = false

    context.addTaskCompletionListener { _ =>
      if (!closed) {
        root.close()
        allocator.close()
      }
    }

    new Iterator[ArrowPayload] {

      override def hasNext: Boolean = rowIter.hasNext || {
        root.close()
        allocator.close()
        closed = true
        false
      }

      override def next(): ArrowPayload = {
        val out = new ByteArrayOutputStream()
        val writer = new ArrowFileWriter(root, null, Channels.newChannel(out))

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
          arrowWriter.reset()
          writer.close()
        }

        new ArrowPayload(out.toByteArray)
      }
    }
  }

  /**
   * Maps Iterator from ArrowPayload to InternalRow. Returns a pair containing the row iterator
   * and the schema from the first batch of Arrow data read.
   */
  private[sql] def fromPayloadIterator(
      payloadIter: Iterator[ArrowPayload],
      context: TaskContext): ArrowRowIterator = {
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("fromPayloadIterator", 0, Long.MaxValue)

    new ArrowRowIterator {
      private var reader: ArrowFileReader = null
      private var schemaRead = StructType(Seq.empty)
      private var rowIter = if (payloadIter.hasNext) nextBatch() else Iterator.empty

      context.addTaskCompletionListener { _ =>
        closeReader()
        allocator.close()
      }

      override def schema: StructType = schemaRead

      override def hasNext: Boolean = rowIter.hasNext || {
        closeReader()
        if (payloadIter.hasNext) {
          rowIter = nextBatch()
          true
        } else {
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

      private def nextBatch(): Iterator[InternalRow] = {
        val in = new ByteArrayReadableSeekableByteChannel(payloadIter.next().asPythonSerializable)
        reader = new ArrowFileReader(in, allocator)
        reader.loadNextBatch()  // throws IOException
        val root = reader.getVectorSchemaRoot  // throws IOException
        schemaRead = ArrowUtils.fromArrowSchema(root.getSchema)

        val columns = root.getFieldVectors.asScala.map { vector =>
          new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
        }.toArray

        val batch = new ColumnarBatch(schemaRead, columns, root.getRowCount)
        batch.setNumRows(root.getRowCount)
        batch.rowIterator().asScala
      }
    }
  }

  /**
   * Convert a byte array to an ArrowRecordBatch.
   */
  private[arrow] def byteArrayToBatch(
      batchBytes: Array[Byte],
      allocator: BufferAllocator): ArrowRecordBatch = {
    val in = new ByteArrayReadableSeekableByteChannel(batchBytes)
    val reader = new ArrowFileReader(in, allocator)

    // Read a batch from a byte stream, ensure the reader is closed
    Utils.tryWithSafeFinally {
      val root = reader.getVectorSchemaRoot  // throws IOException
      val unloader = new VectorUnloader(root)
      reader.loadNextBatch()  // throws IOException
      unloader.getRecordBatch
    } {
      reader.close()
    }
  }

  private[sql] def toDataFrame(
      payloadRDD: JavaRDD[Array[Byte]],
      schemaString: String,
      sqlContext: SQLContext): DataFrame = {
    val rdd = payloadRDD.rdd.mapPartitions { iter =>
      val context = TaskContext.get()
      ArrowConverters.fromPayloadIterator(iter.map(new ArrowPayload(_)), context)
    }
    val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
    sqlContext.internalCreateDataFrame(rdd, schema)
  }
}
