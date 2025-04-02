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

import java.nio.ByteBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.StringType

class DictionaryEncodingSuite extends SparkFunSuite {
  val nullValue = -1
  testDictionaryEncoding(new IntColumnStats, INT)
  testDictionaryEncoding(new LongColumnStats, LONG)
  Seq(
    "UTF8_BINARY", "UTF8_LCASE", "UNICODE", "UNICODE_CI"
  ).foreach(collation => {
    val dt = StringType(collation)
    val typeName = if (collation == "UTF8_BINARY") "STRING" else s"STRING($collation)"
    testDictionaryEncoding(new StringColumnStats(dt), STRING(dt), false, Some(typeName))
  })

  def testDictionaryEncoding[T <: PhysicalDataType](
      columnStats: ColumnStats,
      columnType: NativeColumnType[T],
      testDecompress: Boolean = true,
      testTypeName: Option[String] = None): Unit = {

    val typeName = testTypeName.getOrElse(columnType.getClass.getSimpleName.stripSuffix("$"))

    def buildDictionary(buffer: ByteBuffer) = {
      (0 until buffer.getInt()).map(columnType.extract(buffer) -> _.toShort).toMap
    }

    def stableDistinct(seq: Seq[Int]): Seq[Int] = if (seq.isEmpty) {
      Seq.empty
    } else {
      seq.head +: seq.tail.filterNot(_ == seq.head)
    }

    def skeleton(uniqueValueCount: Int, inputSeq: Seq[Int]): Unit = {
      // -------------
      // Tests encoder
      // -------------

      val builder = TestCompressibleColumnBuilder(columnStats, columnType, DictionaryEncoding)
      val (values, rows) = makeUniqueValuesAndSingleValueRows(columnType, uniqueValueCount)
      val dictValues = stableDistinct(inputSeq)

      inputSeq.foreach(i => builder.appendFrom(rows(i), 0))

      if (dictValues.length > DictionaryEncoding.MAX_DICT_SIZE) {
        withClue("Dictionary overflowed, compression should fail") {
          intercept[Throwable] {
            builder.build()
          }
        }
      } else {
        val buffer = builder.build()
        val headerSize = CompressionScheme.columnHeaderSize(buffer)
        // 4 extra bytes for dictionary size
        val dictionarySize = 4 + rows.map(columnType.actualSize(_, 0)).sum
        // 2 bytes for each `Short`
        val compressedSize = 4 + dictionarySize + 2 * inputSeq.length
        // 4 extra bytes for compression scheme type ID
        assertResult(headerSize + compressedSize, "Wrong buffer capacity")(buffer.capacity)

        // Skips column header
        buffer.position(headerSize)
        assertResult(DictionaryEncoding.typeId, "Wrong compression scheme ID")(buffer.getInt())

        val dictionary = buildDictionary(buffer).toMap

        dictValues.foreach { i =>
          assertResult(i, "Wrong dictionary entry") {
            dictionary(values(i))
          }
        }

        inputSeq.foreach { i =>
          assertResult(i.toShort, "Wrong column element value")(buffer.getShort())
        }

        // -------------
        // Tests decoder
        // -------------

        // Rewinds, skips column header and 4 more bytes for compression scheme ID
        buffer.rewind().position(headerSize + 4)

        val decoder = DictionaryEncoding.decoder(buffer, columnType)
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
    }

    def skeletonForDecompress(uniqueValueCount: Int, inputSeq: Seq[Int]): Unit = {
      if (!testDecompress) return
      val builder = TestCompressibleColumnBuilder(columnStats, columnType, DictionaryEncoding)
      val (values, rows) = makeUniqueValuesAndSingleValueRows(columnType, uniqueValueCount)
      val dictValues = stableDistinct(inputSeq)

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
      assertResult(DictionaryEncoding.typeId, "Wrong compression scheme ID")(buffer.getInt())

      val decoder = DictionaryEncoding.decoder(buffer, columnType)
      val columnVector = new OnHeapColumnVector(inputSeq.length,
        ColumnarDataTypeUtils.toLogicalDataType(columnType.dataType))
      decoder.decompress(columnVector, inputSeq.length)

      if (inputSeq.nonEmpty) {
        inputSeq.zipWithIndex.foreach { case (i: Any, index: Int) =>
          if (i == nullValue) {
            assertResult(true, s"Wrong null ${index}-th position") {
              columnVector.isNullAt(index)
            }
          } else {
            columnType match {
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
          }
        }
      }
    }

    test(s"$DictionaryEncoding with $typeName: empty") {
      skeleton(0, Seq.empty)
    }

    test(s"$DictionaryEncoding with $typeName: simple case") {
      skeleton(2, Seq(0, 1, 0, 1))
    }

    test(s"$DictionaryEncoding with $typeName: dictionary overflow") {
      skeleton(DictionaryEncoding.MAX_DICT_SIZE + 1, 0 to DictionaryEncoding.MAX_DICT_SIZE)
    }

    test(s"$DictionaryEncoding with $typeName: empty for decompress()") {
      skeletonForDecompress(0, Seq.empty)
    }

    test(s"$DictionaryEncoding with $typeName: simple case for decompress()") {
      skeletonForDecompress(2, Seq(0, nullValue, 0, nullValue))
    }

    test(s"$DictionaryEncoding with $typeName: dictionary overflow for decompress()") {
      skeletonForDecompress(DictionaryEncoding.MAX_DICT_SIZE + 2,
        Seq(nullValue) ++ (0 to DictionaryEncoding.MAX_DICT_SIZE - 1) ++ Seq(nullValue))
    }
  }
}
