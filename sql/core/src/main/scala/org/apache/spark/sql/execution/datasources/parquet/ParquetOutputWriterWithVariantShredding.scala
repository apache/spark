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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.mapreduce._
import org.apache.parquet.hadoop.metadata.ParquetMetadata

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.parquet.InferVariantShreddingSchema
import org.apache.spark.sql.execution.datasources.parquet.ParquetOutputWriter
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport

/**
 * A wrapper around ParquetOutputWriter that defers creating the writer until some number of input
 * rows have been buffered and analyzed to determine an appropriate schema for shredded Variant.
 * @inferrenceHelper An object that is aware of the input schema. It takes the buffered rows, and
 * returns a schema that replaces VariantType with its shredded representation.
 * If `shreddingSchemaForced` is true, shredding schema inference is not performed because a forced
 * schema is assumed to be used.
 */
// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
class ParquetOutputWriterWithVariantShredding(
    val path: String,
    context: TaskAttemptContext,
    inferenceHelper: InferVariantShreddingSchema,
    val isShreddingSchemaForced: Boolean)
    extends OutputWriter {

  private var parquetOutputWriter: Option[ParquetOutputWriter] = None
  private var latestFooter: Option[ParquetMetadata] = None

  private val rows = scala.collection.mutable.Buffer[InternalRow]()
  private var bufferedSize = 0L
  private lazy val toUnsafeRow = UnsafeProjection.create(inferenceHelper.schema)

  private val maxBufferSize = 64L * 1024L * 1024L
  private val maxBufferRows = 4096

  private def finalizeSchemaAndFlush(): Unit = {
    if (!isShreddingSchemaForced) {
      val finalSchema = inferenceHelper.inferSchema(rows.toSeq)
      ParquetWriteSupport.setShreddingSchema(finalSchema, context.getConfiguration)
    }
    parquetOutputWriter = Some(new ParquetOutputWriter(path, context))
    rows.foreach(row => parquetOutputWriter.get.write(row))
    rows.clear()
    bufferedSize = 0
  }

  def getLatestFooterOpt: Option[ParquetMetadata] = latestFooter

  // Check if adding the current row will exceed the thresholds for buffering
  // As a side effect, updates `bufferedSize` on the assumption that if we return false, the row
  // will be added to the list of buffered rows.
  private def stopBuffering(row: UnsafeRow): Boolean = {
    // Buffer the first `maxBufferRows` rows.
    // Use +1 to account for the current row, which will be added by the caller if we return true
    // here.
    if (rows.size + 1 >= maxBufferRows) {
      true
    } else {
      bufferedSize += row.getSizeInBytes
      bufferedSize > maxBufferSize
    }
  }

  override def write(row: InternalRow): Unit = {
    if (parquetOutputWriter.isEmpty) {
      // We need an UnsafeRow in order to determine the memory usage. Normally, the row should
      // already be UnsafeRow, but if it isn't, make one.
      val unsafeRow = row match {
        case u: UnsafeRow => u
        case _ => toUnsafeRow(row)
      }
      if (stopBuffering(unsafeRow)) {
        // We're ready to pick a schema.
        // Copy the last row by reference, since we'll clear it after finalizeSchemaAndFlush.
        rows += unsafeRow
        finalizeSchemaAndFlush()
      } else {
        rows += unsafeRow.copy()
      }
    } else {
      // We've already picked a schema, and can write directly.
      parquetOutputWriter.get.write(row)
    }
  }

  override def close(): Unit = {
    try {
      if (parquetOutputWriter.isEmpty) {
        // We haven't written any rows yet. Pick a schema, and write them all.
        finalizeSchemaAndFlush()
      }
    } finally {
      parquetOutputWriter.foreach { writer =>
        writer.close()
      }
      parquetOutputWriter = None
    }
  }
}
