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

import java.io.ByteArrayOutputStream
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.parquet.bytes.HeapByteBufferAllocator
import org.apache.parquet.column.{ColumnDescriptor, Dictionary}
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Types

import org.apache.spark.sql.execution.vectorized.WritableColumnVector

/**
 * Shared helpers for tests and benchmarks that exercise
 * `VectorizedRleValuesReader` directly.
 */
object VectorizedRleValuesReaderTestUtils {

  /** Encodes values via parquet-mr's RLE/bit-packing hybrid encoder (no length prefix). */
  def encodeRle(values: Array[Int], bitWidth: Int): Array[Byte] = {
    val enc = new RunLengthBitPackingHybridEncoder(
      bitWidth, 64 * 1024, 1024 * 1024, new HeapByteBufferAllocator)
    values.foreach(enc.writeInt)
    val out = new ByteArrayOutputStream()
    enc.toBytes.writeAllTo(out)
    out.toByteArray
  }

  /** Little-endian plain INT32 bytes. Layout matches `VectorizedPlainValuesReader.readIntegers`. */
  def plainIntBytes(count: Int)(f: Int => Int): Array[Byte] = {
    val buf = ByteBuffer.allocate(count * 4).order(ByteOrder.LITTLE_ENDIAN)
    var i = 0
    while (i < count) {
      buf.putInt(f(i))
      i += 1
    }
    buf.array()
  }

  /** Single-column INT32 descriptor with the given `maxDefinitionLevel`. */
  def intColumnDescriptor(maxDef: Int): ColumnDescriptor = {
    val rep = if (maxDef == 0) Repetition.REQUIRED else Repetition.OPTIONAL
    val prim = Types.primitive(PrimitiveTypeName.INT32, rep).named("col")
    new ColumnDescriptor(Array("col"), prim, 0, maxDef)
  }

  /** Stateless INT32 updater equivalent to the package-private `IntegerUpdater`. */
  val integerUpdater: ParquetVectorUpdater = new ParquetVectorUpdater {
    override def readValues(
        total: Int, offset: Int,
        values: WritableColumnVector, reader: VectorizedValuesReader): Unit =
      reader.readIntegers(total, values, offset)

    override def skipValues(total: Int, reader: VectorizedValuesReader): Unit =
      reader.skipIntegers(total)

    override def readValue(
        offset: Int, values: WritableColumnVector, reader: VectorizedValuesReader): Unit =
      values.putInt(offset, reader.readInteger())

    override def decodeSingleDictionaryId(
        offset: Int,
        values: WritableColumnVector,
        dictionaryIds: WritableColumnVector,
        dictionary: Dictionary): Unit =
      values.putInt(offset, dictionary.decodeToInt(dictionaryIds.getDictId(offset)))
  }
}
