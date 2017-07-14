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

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.file._
import org.apache.arrow.vector.schema.ArrowRecordBatch
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * Store Arrow data in a form that can be serialized by Spark and served to a Python process.
 */
private[sql] class ArrowPayload private[arrow] (payload: Array[Byte]) extends Serializable {

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

private[sql] object ArrowPayload {

  /**
   * Create an ArrowPayload from an ArrowRecordBatch and Spark schema.
   */
  def apply(
      batch: ArrowRecordBatch,
      schema: StructType,
      allocator: BufferAllocator): ArrowPayload = {
    new ArrowPayload(ArrowConverters.batchToByteArray(batch, schema, allocator))
  }
}

private[sql] object ArrowConverters {

  /**
   * Maps Iterator from InternalRow to ArrowPayload. Limit ArrowRecordBatch size in ArrowPayload
   * by setting maxRecordsPerBatch or use 0 to fully consume rowIter.
   */
  private[sql] def toPayloadIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Int): Iterator[ArrowPayload] = {
    new Iterator[ArrowPayload] {
      private val _allocator = new RootAllocator(Long.MaxValue)
      private var _nextPayload = if (rowIter.nonEmpty) convert() else null

      override def hasNext: Boolean = _nextPayload != null

      override def next(): ArrowPayload = {
        val obj = _nextPayload
        if (hasNext) {
          if (rowIter.hasNext) {
            _nextPayload = convert()
          } else {
            _allocator.close()
            _nextPayload = null
          }
        }
        obj
      }

      private def convert(): ArrowPayload = {
        val batch = internalRowIterToArrowBatch(rowIter, schema, _allocator, maxRecordsPerBatch)
        ArrowPayload(batch, schema, _allocator)
      }
    }
  }

  /**
   * Iterate over InternalRows and write to an ArrowRecordBatch, stopping when rowIter is consumed
   * or the number of records in the batch equals maxRecordsInBatch.  If maxRecordsPerBatch is 0,
   * then rowIter will be fully consumed.
   */
  private def internalRowIterToArrowBatch(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      allocator: BufferAllocator,
      maxRecordsPerBatch: Int = 0): ArrowRecordBatch = {

    val arrowSchema = ArrowUtils.toArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = ArrowWriter.create(root)

    var recordsInBatch = 0
    while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || recordsInBatch < maxRecordsPerBatch)) {
      val row = rowIter.next()
      arrowWriter.write(row)
      recordsInBatch += 1
    }
    arrowWriter.finish()

    Utils.tryWithSafeFinally {
      val unloader = new VectorUnloader(arrowWriter.root)
      unloader.getRecordBatch()
    } {
      root.close()
    }
  }

  /**
   * Convert an ArrowRecordBatch to a byte array and close batch to release resources. Once closed,
   * the batch can no longer be used.
   */
  private[arrow] def batchToByteArray(
      batch: ArrowRecordBatch,
      schema: StructType,
      allocator: BufferAllocator): Array[Byte] = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val out = new ByteArrayOutputStream()
    val writer = new ArrowFileWriter(root, null, Channels.newChannel(out))

    // Write a batch to byte stream, ensure the batch, allocator and writer are closed
    Utils.tryWithSafeFinally {
      val loader = new VectorLoader(root)
      loader.load(batch)
      writer.writeBatch()  // writeBatch can throw IOException
    } {
      batch.close()
      root.close()
      writer.close()
    }
    out.toByteArray
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
}
