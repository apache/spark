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

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.types.NativeType
import org.apache.spark.sql.columnar._
import org.apache.spark.sql.columnar.ColumnarTestUtils._
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow

class DictionaryEncodingSuite extends FunSuite {
  testDictionaryEncoding(new IntColumnStats,    INT)
  testDictionaryEncoding(new LongColumnStats,   LONG)
  testDictionaryEncoding(new StringColumnStats, STRING)

  def testDictionaryEncoding[T <: NativeType](
      columnStats: NativeColumnStats[T],
      columnType: NativeColumnType[T]) {

    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")

    def buildDictionary(buffer: ByteBuffer) = {
      (0 until buffer.getInt()).map(columnType.extract(buffer) -> _.toShort).toMap
    }

    test(s"$DictionaryEncoding with $typeName: simple case") {
      // -------------
      // Tests encoder
      // -------------

      val builder = TestCompressibleColumnBuilder(columnStats, columnType, DictionaryEncoding)
      val (values, rows) = makeUniqueValuesAndSingleValueRows(columnType, 2)

      builder.initialize(0)
      builder.appendFrom(rows(0), 0)
      builder.appendFrom(rows(1), 0)
      builder.appendFrom(rows(0), 0)
      builder.appendFrom(rows(1), 0)

      val buffer = builder.build()
      val headerSize = CompressionScheme.columnHeaderSize(buffer)
      // 4 extra bytes for dictionary size
      val dictionarySize = 4 + values.map(columnType.actualSize).sum
      // 4 `Short`s, 2 bytes each
      val compressedSize = dictionarySize + 2 * 4
      // 4 extra bytes for compression scheme type ID
      expectResult(headerSize + 4 + compressedSize, "Wrong buffer capacity")(buffer.capacity)

      // Skips column header
      buffer.position(headerSize)
      expectResult(DictionaryEncoding.typeId, "Wrong compression scheme ID")(buffer.getInt())

      val dictionary = buildDictionary(buffer)
      Array[Short](0, 1).foreach { i =>
        expectResult(i, "Wrong dictionary entry")(dictionary(values(i)))
      }

      Array[Short](0, 1, 0, 1).foreach {
        expectResult(_, "Wrong column element value")(buffer.getShort())
      }

      // -------------
      // Tests decoder
      // -------------

      // Rewinds, skips column header and 4 more bytes for compression scheme ID
      buffer.rewind().position(headerSize + 4)

      val decoder = new DictionaryEncoding.Decoder[T](buffer, columnType)

      Array[Short](0, 1, 0, 1).foreach { i =>
        expectResult(values(i), "Wrong decoded value")(decoder.next())
      }

      assert(!decoder.hasNext)
    }
  }

  test(s"$DictionaryEncoding: overflow") {
    val builder = TestCompressibleColumnBuilder(new IntColumnStats, INT, DictionaryEncoding)
    builder.initialize(0)

    (0 to Short.MaxValue).foreach { n =>
      val row = new GenericMutableRow(1)
      row.setInt(0, n)
      builder.appendFrom(row, 0)
    }

    withClue("Dictionary overflowed, encoding should fail") {
      intercept[Throwable] {
        builder.build()
      }
    }
  }
}
