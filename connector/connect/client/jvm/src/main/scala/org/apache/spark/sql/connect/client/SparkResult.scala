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
package org.apache.spark.sql.connect.client

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.ipc.ArrowStreamReader

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.UnboundRowEncoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.Deserializer
import org.apache.spark.sql.connect.client.util.{AutoCloseables, Cleanable}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

private[sql] class SparkResult[T](
    responses: java.util.Iterator[proto.ExecutePlanResponse],
    allocator: BufferAllocator,
    encoder: AgnosticEncoder[T])
    extends AutoCloseable
    with Cleanable {

  private[this] var numRecords: Int = 0
  private[this] var structType: StructType = _
  private[this] var boundEncoder: ExpressionEncoder[T] = _
  private[this] val batches = mutable.Buffer.empty[ColumnarBatch]

  private def createEncoder(schema: StructType): ExpressionEncoder[T] = {
    val agnosticEncoder = if (encoder == UnboundRowEncoder) {
      // Create a row encoder based on the schema.
      RowEncoder.encoderFor(schema).asInstanceOf[AgnosticEncoder[T]]
    } else {
      encoder
    }
    ExpressionEncoder(agnosticEncoder)
  }

  private def processResponses(stopOnFirstNonEmptyResponse: Boolean): Boolean = {
    while (responses.hasNext) {
      val response = responses.next()
      if (response.hasSchema) {
        // The original schema should arrive before ArrowBatches.
        structType =
          DataTypeProtoConverter.toCatalystType(response.getSchema).asInstanceOf[StructType]
      } else if (response.hasArrowBatch) {
        val ipcStreamBytes = response.getArrowBatch.getData
        val reader = new ArrowStreamReader(ipcStreamBytes.newInput(), allocator)
        try {
          val root = reader.getVectorSchemaRoot
          if (batches.isEmpty) {
            if (structType == null) {
              // If the schema is not available yet, fallback to the schema from Arrow.
              structType = ArrowUtils.fromArrowSchema(root.getSchema)
            }
            // TODO: create encoders that directly operate on arrow vectors.
            boundEncoder = createEncoder(structType).resolveAndBind(structType.toAttributes)
          }
          while (reader.loadNextBatch()) {
            val rowCount = root.getRowCount
            assert(root.getRowCount == response.getArrowBatch.getRowCount) // HUH!
            if (rowCount > 0) {
              val vectors = root.getFieldVectors.asScala
                .map(v => new ArrowColumnVector(transferToNewVector(v)))
                .toArray[ColumnVector]
              batches += new ColumnarBatch(vectors, rowCount)
              numRecords += rowCount
              if (stopOnFirstNonEmptyResponse) {
                return true
              }
            }
          }
        } finally {
          reader.close()
        }
      }
    }
    false
  }

  private def transferToNewVector(in: FieldVector): FieldVector = {
    val pair = in.getTransferPair(allocator)
    pair.transfer()
    pair.getTo.asInstanceOf[FieldVector]
  }

  /**
   * Returns the number of elements in the result.
   */
  def length: Int = {
    // We need to process all responses to make sure numRecords is correct.
    processResponses(stopOnFirstNonEmptyResponse = false)
    numRecords
  }

  /**
   * @return
   *   the schema of the result.
   */
  def schema: StructType = {
    processResponses(stopOnFirstNonEmptyResponse = true)
    structType
  }

  /**
   * Create an Array with the contents of the result.
   */
  def toArray: Array[T] = {
    val result = encoder.clsTag.newArray(length)
    val rows = iterator
    var i = 0
    while (rows.hasNext) {
      result(i) = rows.next()
      assert(i < numRecords)
      i += 1
    }
    result
  }

  /**
   * Returns an iterator over the contents of the result.
   */
  def iterator: java.util.Iterator[T] with AutoCloseable = {
    new java.util.Iterator[T] with AutoCloseable {
      private[this] var batchIndex: Int = -1
      private[this] var iterator: java.util.Iterator[InternalRow] = Collections.emptyIterator()
      private[this] var deserializer: Deserializer[T] = _
      override def hasNext: Boolean = {
        if (iterator.hasNext) {
          return true
        }
        val nextBatchIndex = batchIndex + 1
        val hasNextBatch = if (nextBatchIndex == batches.size) {
          processResponses(stopOnFirstNonEmptyResponse = true)
        } else {
          true
        }
        if (hasNextBatch) {
          batchIndex = nextBatchIndex
          iterator = batches(nextBatchIndex).rowIterator()
          if (deserializer == null) {
            deserializer = boundEncoder.createDeserializer()
          }
        }
        hasNextBatch
      }

      override def next(): T = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        deserializer(iterator.next())
      }

      override def close(): Unit = SparkResult.this.close()
    }
  }

  /**
   * Close this result, freeing any underlying resources.
   */
  override def close(): Unit = {
    batches.foreach(_.close())
  }

  override def cleaner: AutoCloseable = AutoCloseables(batches.toSeq)
}
