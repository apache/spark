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

package org.apache.spark.sql.execution.columnar.compression

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.columnar.{ColumnBuilder, NativeColumnBuilder}
import org.apache.spark.sql.types.AtomicType
import org.apache.spark.unsafe.Platform

/**
 * A stackable trait that builds optionally compressed byte buffer for a column.  Memory layout of
 * the final byte buffer is:
 * {{{
 *    .----------------------- Null count N (4 bytes)
 *    |   .------------------- Null positions (4 x N bytes, empty if null count is zero)
 *    |   |     .------------- Compression scheme ID (4 bytes)
 *    |   |     |   .--------- Compressed non-null elements
 *    V   V     V   V
 *   +---+-----+---+---------+
 *   |   | ... |   | ... ... |
 *   +---+-----+---+---------+
 *    \-------/ \-----------/
 *     header         body
 * }}}
 */
private[columnar] trait CompressibleColumnBuilder[T <: AtomicType]
  extends ColumnBuilder with Logging {

  this: NativeColumnBuilder[T] with WithCompressionSchemes =>

  var compressionEncoders: Seq[Encoder[T]] = _

  abstract override def initialize(
      initialSize: Int,
      columnName: String,
      useCompression: Boolean): Unit = {

    compressionEncoders =
      if (useCompression) {
        schemes.filter(_.supports(columnType)).map(_.encoder[T](columnType))
      } else {
        Seq(PassThrough.encoder(columnType))
      }
    super.initialize(initialSize, columnName, useCompression)
  }

  // The various compression schemes, while saving memory use, cause all of the data within
  // the row to become unaligned, thus causing crashes.  Until a way of fixing the compression
  // is found to also allow aligned accesses this must be disabled for SPARC.

  protected def isWorthCompressing(encoder: Encoder[T]) = {
    CompressibleColumnBuilder.unaligned && encoder.compressionRatio < 0.8
  }

  private def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
    compressionEncoders.foreach(_.gatherCompressibilityStats(row, ordinal))
  }

  abstract override def appendFrom(row: InternalRow, ordinal: Int): Unit = {
    super.appendFrom(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      gatherCompressibilityStats(row, ordinal)
    }
  }

  override def build(): ByteBuffer = {
    val nonNullBuffer = buildNonNulls()
    val encoder: Encoder[T] = {
      val candidate = compressionEncoders.minBy(_.compressionRatio)
      if (isWorthCompressing(candidate)) candidate else PassThrough.encoder(columnType)
    }

    // Header = null count + null positions
    val headerSize = 4 + nulls.limit()
    val compressedSize = if (encoder.compressedSize == 0) {
      nonNullBuffer.remaining()
    } else {
      encoder.compressedSize
    }

    val compressedBuffer = ByteBuffer
      // Reserves 4 bytes for compression scheme ID
      .allocate(headerSize + 4 + compressedSize)
      .order(ByteOrder.nativeOrder)
      // Write the header
      .putInt(nullCount)
      .put(nulls)

    logDebug(s"Compressor for [$columnName]: $encoder, ratio: ${encoder.compressionRatio}")
    encoder.compress(nonNullBuffer, compressedBuffer)
  }
}

private[columnar] object CompressibleColumnBuilder { 
  val unaligned = Platform.unaligned()
}
