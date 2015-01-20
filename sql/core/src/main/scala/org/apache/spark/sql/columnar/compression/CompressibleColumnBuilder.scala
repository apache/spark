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

package org.apache.spark.sql.columnar.compression

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.columnar.{ColumnBuilder, NativeColumnBuilder}
import org.apache.spark.sql.types.NativeType

/**
 * A stackable trait that builds optionally compressed byte buffer for a column.  Memory layout of
 * the final byte buffer is:
 * {{{
 *    .--------------------------- Column type ID (4 bytes)
 *    |   .----------------------- Null count N (4 bytes)
 *    |   |   .------------------- Null positions (4 x N bytes, empty if null count is zero)
 *    |   |   |     .------------- Compression scheme ID (4 bytes)
 *    |   |   |     |   .--------- Compressed non-null elements
 *    V   V   V     V   V
 *   +---+---+-----+---+---------+
 *   |   |   | ... |   | ... ... |
 *   +---+---+-----+---+---------+
 *    \-----------/ \-----------/
 *       header         body
 * }}}
 */
private[sql] trait CompressibleColumnBuilder[T <: NativeType]
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

  protected def isWorthCompressing(encoder: Encoder[T]) = {
    encoder.compressionRatio < 0.8
  }

  private def gatherCompressibilityStats(row: Row, ordinal: Int): Unit = {
    var i = 0
    while (i < compressionEncoders.length) {
      compressionEncoders(i).gatherCompressibilityStats(row, ordinal)
      i += 1
    }
  }

  abstract override def appendFrom(row: Row, ordinal: Int): Unit = {
    super.appendFrom(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      gatherCompressibilityStats(row, ordinal)
    }
  }

  override def build() = {
    val nonNullBuffer = buildNonNulls()
    val typeId = nonNullBuffer.getInt()
    val encoder: Encoder[T] = {
      val candidate = compressionEncoders.minBy(_.compressionRatio)
      if (isWorthCompressing(candidate)) candidate else PassThrough.encoder(columnType)
    }

    // Header = column type ID + null count + null positions
    val headerSize = 4 + 4 + nulls.limit()
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
      .putInt(typeId)
      .putInt(nullCount)
      .put(nulls)

    logDebug(s"Compressor for [$columnName]: $encoder, ratio: ${encoder.compressionRatio}")
    encoder.compress(nonNullBuffer, compressedBuffer)
  }
}
