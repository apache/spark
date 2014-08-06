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

import java.nio.{ByteOrder, ByteBuffer}

import org.apache.spark.sql.catalyst.types.NativeType
import org.apache.spark.sql.columnar.{ColumnType, NativeColumnType}

private[sql] trait Encoder[T <: NativeType] {
  def gatherCompressibilityStats(value: T#JvmType, columnType: NativeColumnType[T]) {}

  def compressedSize: Int

  def uncompressedSize: Int

  def compressionRatio: Double = {
    if (uncompressedSize > 0) compressedSize.toDouble / uncompressedSize else 1.0
  }

  def compress(from: ByteBuffer, to: ByteBuffer, columnType: NativeColumnType[T]): ByteBuffer
}

private[sql] trait Decoder[T <: NativeType] extends Iterator[T#JvmType]

private[sql] trait CompressionScheme {
  def typeId: Int

  def supports(columnType: ColumnType[_, _]): Boolean

  def encoder[T <: NativeType]: Encoder[T]

  def decoder[T <: NativeType](buffer: ByteBuffer, columnType: NativeColumnType[T]): Decoder[T]
}

private[sql] trait WithCompressionSchemes {
  def schemes: Seq[CompressionScheme]
}

private[sql] trait AllCompressionSchemes extends WithCompressionSchemes {
  override val schemes: Seq[CompressionScheme] = CompressionScheme.all
}

private[sql] object CompressionScheme {
  val all: Seq[CompressionScheme] =
    Seq(PassThrough, RunLengthEncoding, DictionaryEncoding, BooleanBitSet, IntDelta, LongDelta)

  private val typeIdToScheme = all.map(scheme => scheme.typeId -> scheme).toMap

  def apply(typeId: Int): CompressionScheme = {
    typeIdToScheme.getOrElse(typeId, throw new UnsupportedOperationException(
      s"Unrecognized compression scheme type ID: $typeId"))
  }

  def columnHeaderSize(columnBuffer: ByteBuffer): Int = {
    val header = columnBuffer.duplicate().order(ByteOrder.nativeOrder)
    val nullCount = header.getInt(4)
    // Column type ID + null count + null positions
    4 + 4 + 4 * nullCount
  }
}
