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
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.StringType

class RunLengthEncodingSuite extends SparkFunSuite {
  val nullValue = -1
  testRunLengthEncoding(new NoopColumnStats, BOOLEAN)
  testRunLengthEncoding(new ByteColumnStats, BYTE)
  testRunLengthEncoding(new ShortColumnStats, SHORT)
  testRunLengthEncoding(new IntColumnStats, INT)
  testRunLengthEncoding(new LongColumnStats, LONG)
  Seq(
    "UTF8_BINARY", "UTF8_LCASE", "UNICODE", "UNICODE_CI"
  ).foreach(collation => {
    val dt = StringType(collation)
    val typeName = if (collation == "UTF8_BINARY") "STRING" else s"STRING($collation)"
    testRunLengthEncoding(new StringColumnStats(dt), STRING(dt), false, Some(typeName))
  })

  def testRunLengthEncoding[T <: PhysicalDataType](
      columnStats: ColumnStats,
      columnType: NativeColumnType[T],
      testDecompress: Boolean = true,
      testTypeName: Option[String] = None): Unit = {

    val typeName = testTypeName.getOrElse(columnType.getClass.getSimpleName.stripSuffix("$"))

    def skeleton(uniqueValueCount: Int, inputRuns: Seq[(Int, Int)]): Unit = {
      // -------------
      // Tests encoder
      // -------------

      val builder = TestCompressibleColumnBuilder(columnStats, columnType, RunLengthEncoding)
      val (values, rows) = makeUniqueValuesAndSingleValueRows(columnType, uniqueValueCount)
      val inputSeq = inputRuns.flatMap { case (index, run) =>
        Seq.fill(run)(index)
      }

      inputSeq.foreach(i => builder.appendFrom(rows(i), 0))
      val buffer = builder.build()

      // Column type ID + null count + null positions
      val headerSize = CompressionScheme.columnHeaderSize(buffer)

      // Compression scheme ID + compressed contents
      val compressedSize = 4 + inputRuns.map { case (index, _) =>
        // 4 extra bytes each run for run length
        columnType.actualSize(rows(index), 0) + 4
      }.sum

      // 4 extra bytes for compression scheme type ID
      assertResult(headerSize + compressedSize, "Wrong buffer capacity")(buffer.capacity)

      // Skips column header
      buffer.position(headerSize)
      assertResult(RunLengthEncoding.typeId, "Wrong compression scheme ID")(buffer.getInt())

      inputRuns.foreach { case (index, run) =>
        assertResult(values(index), "Wrong column element value")(columnType.extract(buffer))
        assertResult(run, "Wrong run length")(buffer.getInt())
      }

      // -------------
      // Tests decoder
      // -------------

      // Rewinds, skips column header and 4 more bytes for compression scheme ID
      buffer.rewind().position(headerSize + 4)

      val decoder = RunLengthEncoding.decoder(buffer, columnType)
      val mutableRow = new GenericInternalRow(1)

      if (inputSeq.nonEmpty) {
        inputSeq.foreach { i =>
          assert(decoder.hasNext)
          assertResult(values(i), "Wrong decoded value") {
            decoder.next(mutableRow, 0)
            columnType.getField(mutableRow, 0)
          }
        }
      }

      assert(!decoder.hasNext)
    }

    def skeletonForDecompress(uniqueValueCount: Int, inputRuns: Seq[(Int, Int)]): Unit = {
      if (!testDecompress) return
      val builder = TestCompressibleColumnBuilder(columnStats, columnType, RunLengthEncoding)
      val (values, rows) = makeUniqueValuesAndSingleValueRows(columnType, uniqueValueCount)
      val inputSeq = inputRuns.flatMap { case (index, run) =>
        Seq.fill(run)(index)
      }

      val nullRow = new GenericInternalRow(1)
      nullRow.setNullAt(0)
      inputSeq.foreach { i =>
        if (i == nullValue) {
          builder.appendFrom(nullRow, 0)
        } else {
          builder.appendFrom(rows(i), 0)
        }
      }
      val buffer = builder.build()

      // ----------------
      // Tests decompress
      // ----------------
      // Rewinds, skips column header and 4 more bytes for compression scheme ID
      val headerSize = CompressionScheme.columnHeaderSize(buffer)
      buffer.position(headerSize)
      assertResult(RunLengthEncoding.typeId, "Wrong compression scheme ID")(buffer.getInt())

      val decoder = RunLengthEncoding.decoder(buffer, columnType)
      val columnVector = new OnHeapColumnVector(inputSeq.length,
        ColumnarDataTypeUtils.toLogicalDataType(columnType.dataType))
      decoder.decompress(columnVector, inputSeq.length)

      if (inputSeq.nonEmpty) {
        inputSeq.zipWithIndex.foreach {
          case (expected: Any, index: Int) if expected == nullValue =>
            assertResult(true, s"Wrong null ${index}th-position") {
              columnVector.isNullAt(index)
            }
          case (i: Int, index: Int) =>
            columnType match {
              case BOOLEAN =>
                assertResult(values(i), s"Wrong ${index}-th decoded boolean value") {
                  columnVector.getBoolean(index)
                }
              case BYTE =>
                assertResult(values(i), s"Wrong ${index}-th decoded byte value") {
                  columnVector.getByte(index)
                }
              case SHORT =>
                assertResult(values(i), s"Wrong ${index}-th decoded short value") {
                  columnVector.getShort(index)
                }
              case INT =>
                assertResult(values(i), s"Wrong ${index}-th decoded int value") {
                  columnVector.getInt(index)
                }
              case LONG =>
                assertResult(values(i), s"Wrong ${index}-th decoded long value") {
                  columnVector.getLong(index)
                }
              case _ => fail("Unsupported type")
            }
          case _ => fail("Unsupported type")
        }
      }
    }

    test(s"$RunLengthEncoding with $typeName: empty column") {
      skeleton(0, Seq.empty)
    }

    test(s"$RunLengthEncoding with $typeName: simple case") {
      skeleton(2, Seq(0 -> 2, 1 -> 2))
    }

    test(s"$RunLengthEncoding with $typeName: run length == 1") {
      skeleton(2, Seq(0 -> 1, 1 -> 1))
    }

    test(s"$RunLengthEncoding with $typeName: single long run") {
      skeleton(1, Seq(0 -> 1000))
    }

    test(s"$RunLengthEncoding with $typeName: empty column for decompress()") {
      skeletonForDecompress(0, Seq.empty)
    }

    test(s"$RunLengthEncoding with $typeName: simple case for decompress()") {
      skeletonForDecompress(2, Seq(0 -> 2, 1 -> 2))
    }

    test(s"$RunLengthEncoding with $typeName: single long run for decompress()") {
      skeletonForDecompress(1, Seq(0 -> 1000))
    }

    test(s"$RunLengthEncoding with $typeName: single case with null for decompress()") {
      skeletonForDecompress(2, Seq(0 -> 2, nullValue -> 2))
    }
  }
}
