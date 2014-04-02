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

import java.nio.ByteBuffer

import org.apache.spark.sql.catalyst.types.NativeType
import org.apache.spark.sql.columnar.{ColumnType, NativeColumnType}

private[sql] trait Encoder {
  def gatherCompressibilityStats[T <: NativeType](
      value: T#JvmType,
      columnType: ColumnType[T, T#JvmType]) {}

  def compressedSize: Int

  def uncompressedSize: Int

  def compressionRatio: Double = {
    if (uncompressedSize > 0) compressedSize.toDouble / uncompressedSize else 1.0
  }

  def compress[T <: NativeType](
      from: ByteBuffer,
      to: ByteBuffer,
      columnType: ColumnType[T, T#JvmType]): ByteBuffer
}

private[sql] trait Decoder[T <: NativeType] extends Iterator[T#JvmType]

private[sql] trait CompressionScheme {
  def typeId: Int

  def supports(columnType: ColumnType[_, _]): Boolean

  def encoder: Encoder

  def decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T]): Decoder[T]
}

private[sql] trait WithCompressionSchemes {
  def schemes: Seq[CompressionScheme]
}

private[sql] trait AllCompressionSchemes extends WithCompressionSchemes {
  override val schemes: Seq[CompressionScheme] = {
    Seq(PassThrough, RunLengthEncoding, DictionaryEncoding)
  }
}

private[sql] object CompressionScheme {
  def apply(typeId: Int): CompressionScheme = typeId match {
    case PassThrough.typeId => PassThrough
    case _ => throw new UnsupportedOperationException()
  }

  def copyColumnHeader(from: ByteBuffer, to: ByteBuffer) {
    // Writes column type ID
    to.putInt(from.getInt())

    // Writes null count
    val nullCount = from.getInt()
    to.putInt(nullCount)

    // Writes null positions
    var i = 0
    while (i < nullCount) {
      to.putInt(from.getInt())
      i += 1
    }
  }

  def columnHeaderSize(columnBuffer: ByteBuffer): Int = {
    val header = columnBuffer.duplicate()
    val nullCount = header.getInt(4)
    // Column type ID + null count + null positions
    4 + 4 + 4 * nullCount
  }
}
