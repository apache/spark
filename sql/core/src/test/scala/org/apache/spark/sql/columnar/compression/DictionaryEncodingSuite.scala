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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.columnar._
import org.apache.spark.sql.columnar.ColumnarTestUtils._
import org.apache.spark.sql.types.AtomicType

class DictionaryEncodingSuite extends SparkFunSuite {
  testDictionaryEncoding(new IntColumnStats, INT)
  testDictionaryEncoding(new LongColumnStats, LONG)
  testDictionaryEncoding(new StringColumnStats, STRING)

  def testDictionaryEncoding[T <: AtomicType](
      columnStats: ColumnStats,
      columnType: NativeColumnType[T]) {

    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")

    def buildDictionary(buffer: ByteBuffer) = {
      (0 until buffer.getInt()).map(columnType.extract(buffer) -> _.toShort).toMap
    }

    def stableDistinct(seq: Seq[Int]): Seq[Int] = if (seq.isEmpty) {
      Seq.empty
    } else {
      seq.head +: seq.tail.filterNot(_ == seq.head)
    }

    def skeleton(uniqueValueCount: Int, inputSeq: Seq[Int]) {
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
        val mutableRow = new GenericMutableRow(1)

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

    test(s"$DictionaryEncoding with $typeName: empty") {
      skeleton(0, Seq.empty)
    }

    test(s"$DictionaryEncoding with $typeName: simple case") {
      skeleton(2, Seq(0, 1, 0, 1))
    }

    test(s"$DictionaryEncoding with $typeName: dictionary overflow") {
      skeleton(DictionaryEncoding.MAX_DICT_SIZE + 1, 0 to DictionaryEncoding.MAX_DICT_SIZE)
    }
  }
}
