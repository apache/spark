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
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.AtomicType

class PassThroughSuite extends SparkFunSuite {
  val nullValue = -1
  testPassThrough(new ByteColumnStats, BYTE)
  testPassThrough(new ShortColumnStats, SHORT)
  testPassThrough(new IntColumnStats, INT)
  testPassThrough(new LongColumnStats, LONG)
  testPassThrough(new FloatColumnStats, FLOAT)
  testPassThrough(new DoubleColumnStats, DOUBLE)

  def testPassThrough[T <: AtomicType](
      columnStats: ColumnStats,
      columnType: NativeColumnType[T]): Unit = {

    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")

    def skeleton(input: Seq[T#InternalType]): Unit = {
      // -------------
      // Tests encoder
      // -------------

      val builder = TestCompressibleColumnBuilder(columnStats, columnType, PassThrough)

      input.foreach { value =>
        val row = new GenericInternalRow(1)
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
      val mutableRow = new GenericInternalRow(1)

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

    def skeletonForDecompress(input: Seq[T#InternalType]): Unit = {
      val builder = TestCompressibleColumnBuilder(columnStats, columnType, PassThrough)
      val row = new GenericInternalRow(1)
      val nullRow = new GenericInternalRow(1)
      nullRow.setNullAt(0)
      input.foreach { value =>
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
      val columnVector = new OnHeapColumnVector(input.length, columnType.dataType)
      decoder.decompress(columnVector, input.length)

      if (input.nonEmpty) {
        input.zipWithIndex.foreach {
          case (expected: Any, index: Int) if expected == nullValue =>
            assertResult(true, s"Wrong null ${index}th-position") {
              columnVector.isNullAt(index)
            }
          case (expected: Byte, index: Int) =>
            assertResult(expected, s"Wrong ${index}-th decoded byte value") {
              columnVector.getByte(index)
            }
          case (expected: Short, index: Int) =>
            assertResult(expected, s"Wrong ${index}-th decoded short value") {
              columnVector.getShort(index)
            }
          case (expected: Int, index: Int) =>
            assertResult(expected, s"Wrong ${index}-th decoded int value") {
              columnVector.getInt(index)
            }
          case (expected: Long, index: Int) =>
            assertResult(expected, s"Wrong ${index}-th decoded long value") {
              columnVector.getLong(index)
            }
          case (expected: Float, index: Int) =>
            assertResult(expected, s"Wrong ${index}-th decoded float value") {
              columnVector.getFloat(index)
            }
          case (expected: Double, index: Int) =>
            assertResult(expected, s"Wrong ${index}-th decoded double value") {
              columnVector.getDouble(index)
            }
          case _ => fail("Unsupported type")
        }
      }
    }

    test(s"$PassThrough with $typeName: empty column") {
      skeleton(Seq.empty)
    }

    test(s"$PassThrough with $typeName: long random series") {
      skeleton(Seq.fill[T#InternalType](10000)(makeRandomValue(columnType)))
    }

    test(s"$PassThrough with $typeName: empty column for decompress()") {
      skeletonForDecompress(Seq.empty)
    }

    test(s"$PassThrough with $typeName: long random series for decompress()") {
      skeletonForDecompress(Seq.fill[T#InternalType](10000)(makeRandomValue(columnType)))
    }

    test(s"$PassThrough with $typeName: simple case with null for decompress()") {
      val input = columnType match {
        case BYTE => Seq(2: Byte, 1: Byte, 2: Byte, nullValue.toByte: Byte, 5: Byte)
        case SHORT => Seq(2: Short, 1: Short, 2: Short, nullValue.toShort: Short, 5: Short)
        case INT => Seq(2: Int, 1: Int, 2: Int, nullValue: Int, 5: Int)
        case LONG => Seq(2: Long, 1: Long, 2: Long, nullValue: Long, 5: Long)
        case FLOAT => Seq(2: Float, 1: Float, 2: Float, nullValue: Float, 5: Float)
        case DOUBLE => Seq(2: Double, 1: Double, 2: Double, nullValue: Double, 5: Double)
      }

      skeletonForDecompress(input.map(_.asInstanceOf[T#InternalType]))
    }
  }
}
