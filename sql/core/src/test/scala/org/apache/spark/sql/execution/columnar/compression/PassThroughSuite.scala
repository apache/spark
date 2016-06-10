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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._
import org.apache.spark.sql.types.{AtomicType, IntegralType}

class PassThroughSuite extends SparkFunSuite {
  val nullValue = -1
  testPassThrough(new FloatColumnStats, FLOAT)
  testPassThrough(new DoubleColumnStats, DOUBLE)

  def testPassThrough[T <: AtomicType](
      columnStats: ColumnStats,
      columnType: NativeColumnType[T]) {

    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")

    def skeleton(input: Seq[T#InternalType]) {
      // -------------
      // Tests encoder
      // -------------

      val builder = TestCompressibleColumnBuilder(columnStats, columnType, PassThrough)

      input.map { value =>
        val row = new GenericMutableRow(1)
        columnType.setField(row, 0, value)
        builder.appendFrom(row, 0)
      }

      val buffer = builder.build()
      // Column type ID + null count + null positions
      val headerSize = CompressionScheme.columnHeaderSize(buffer)

      // Compression scheme ID + compressed contents
      val compressedSize = 4 + input.size * columnType.defaultSize

      // 4 extra bytes for compression scheme type ID
      assertResult(headerSize + compressedSize, "Wrong buffer capacity")(buffer.capacity)

      buffer.position(headerSize)
      assertResult(PassThrough.typeId, "Wrong compression scheme ID")(buffer.getInt())

      if (input.nonEmpty) {
        input.foreach { value =>
          assertResult(value, "Wrong value")(columnType.extract(buffer))
        }
      }

      // -------------
      // Tests decoder
      // -------------

      // Rewinds, skips column header and 4 more bytes for compression scheme ID
      buffer.rewind().position(headerSize + 4)

      val decoder = PassThrough.decoder(buffer, columnType)
      val mutableRow = new GenericMutableRow(1)

      if (input.nonEmpty) {
        input.foreach{
          assert(decoder.hasNext)
          assertResult(_, "Wrong decoded value") {
            decoder.next(mutableRow, 0)
            columnType.getField(mutableRow, 0)
          }
        }
      }
      assert(!decoder.hasNext)
    }

    def skeletonForDecompress(input: Seq[T#InternalType]) {
      val builder = TestCompressibleColumnBuilder(columnStats, columnType, PassThrough)
      val row = new GenericMutableRow(1)
      val nullRow = new GenericMutableRow(1)
      nullRow.setNullAt(0)
      input.map { value =>
        if (value == nullValue) {
          builder.appendFrom(nullRow, 0)
        } else {
          columnType.setField(row, 0, value)
          builder.appendFrom(row, 0)
        }
      }
      val buffer = builder.build()

      // ----------------
      // Tests decompress
      // ----------------
      // Rewinds, skips column header and 4 more bytes for compression scheme ID
      val headerSize = CompressionScheme.columnHeaderSize(buffer)
      buffer.position(headerSize)
      assertResult(PassThrough.typeId, "Wrong compression scheme ID")(buffer.getInt())

      val decoder = PassThrough.decoder(buffer, columnType)
      val (decodeBuffer, nullsBuffer) = decoder.decompress(input.length)

      if (input.nonEmpty) {
        val numNulls = ByteBufferHelper.getInt(nullsBuffer)
        var cntNulls = 0
        var nullPos = if (numNulls == 0) -1 else ByteBufferHelper.getInt(nullsBuffer)
        input.zipWithIndex.foreach {
          case (expected: Any, index: Int) if expected == nullValue =>
            assertResult(index, "Wrong null position") {
              nullPos
            }
            decodeBuffer.position(decodeBuffer.position + columnType.defaultSize)
            cntNulls += 1
            if (cntNulls < numNulls) {
              nullPos = ByteBufferHelper.getInt(nullsBuffer)
            }
          case (expected: Byte, index: Int) =>
            assertResult(expected, s"Wrong ${index}-th decoded byte value") {
              decodeBuffer.get()
            }
          case (expected: Short, index: Int) =>
            assertResult(expected, s"Wrong ${index}-th decoded short value") {
              ByteBufferHelper.getShort(decodeBuffer)
            }
          case (expected: Int, index: Int) =>
            assertResult(expected, s"Wrong ${index}-th decoded int value") {
              ByteBufferHelper.getInt(decodeBuffer)
            }
          case (expected: Long, index: Int) =>
            assertResult(expected, s"Wrong ${index}-th decoded long value") {
              ByteBufferHelper.getLong(decodeBuffer)
            }
          case (expected: Float, index: Int) =>
            assertResult(expected, s"Wrong ${index}-th decoded float value") {
              ByteBufferHelper.getFloat(decodeBuffer)
            }
          case (expected: Double, index: Int) =>
            assertResult(expected, s"Wrong ${index}-th decoded double value") {
              ByteBufferHelper.getDouble(decodeBuffer)
            }
          case _ => fail("Unsupported type")
        }
      }
      assert(!decodeBuffer.hasRemaining)
    }

    test(s"$PassThrough with $typeName: empty column") {
      skeleton(Seq.empty)
    }

    test(s"$PassThrough with $typeName: long random series") {
      val input = Array.fill[Any](10000)(makeRandomValue(columnType))
      skeleton(input.map(_.asInstanceOf[T#InternalType]))
    }

    test(s"$PassThrough with $typeName: empty column for decompress()") {
      skeletonForDecompress(Seq.empty)
    }

    test(s"$PassThrough with $typeName: long random series for decompress()") {
      val input = Array.fill[Any](10000)(makeRandomValue(columnType))
      skeletonForDecompress(input.map(_.asInstanceOf[T#InternalType]))
    }

    test(s"$PassThrough with $typeName: simple case with null for decompress()") {
      val input = columnType match {
        case FLOAT => Seq(2: Float, 1: Float, 2: Float, nullValue: Float, 5: Float)
        case DOUBLE => Seq(2: Double, 1: Double, 2: Double, nullValue: Double, 5: Double)
      }

      skeletonForDecompress(input.map(_.asInstanceOf[T#InternalType]))
    }
  }
}
