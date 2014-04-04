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

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.types.NativeType
import org.apache.spark.sql.columnar._
import org.apache.spark.sql.columnar.ColumnarTestUtils._

class RunLengthEncodingSuite extends FunSuite {
  testRunLengthEncoding(new BooleanColumnStats, BOOLEAN)
  testRunLengthEncoding(new ByteColumnStats,    BYTE)
  testRunLengthEncoding(new ShortColumnStats,   SHORT)
  testRunLengthEncoding(new IntColumnStats,     INT)
  testRunLengthEncoding(new LongColumnStats,    LONG)
  testRunLengthEncoding(new StringColumnStats,  STRING)

  def testRunLengthEncoding[T <: NativeType](
      columnStats: NativeColumnStats[T],
      columnType: NativeColumnType[T]) {

    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")

    test(s"$RunLengthEncoding with $typeName: simple case") {
      // -------------
      // Tests encoder
      // -------------

      val builder = TestCompressibleColumnBuilder(columnStats, columnType, RunLengthEncoding)
      val (values, rows) = makeUniqueValuesAndSingleValueRows(columnType, 2)

      builder.initialize(0)
      builder.appendFrom(rows(0), 0)
      builder.appendFrom(rows(0), 0)
      builder.appendFrom(rows(1), 0)
      builder.appendFrom(rows(1), 0)

      val buffer = builder.build()
      val headerSize = CompressionScheme.columnHeaderSize(buffer)
      // 4 extra bytes each run for run length
      val compressedSize = values.map(columnType.actualSize(_) + 4).sum
      // 4 extra bytes for compression scheme type ID
      expectResult(headerSize + 4 + compressedSize, "Wrong buffer capacity")(buffer.capacity)

      // Skips column header
      buffer.position(headerSize)
      expectResult(RunLengthEncoding.typeId, "Wrong compression scheme ID")(buffer.getInt())

      Array(0, 1).foreach { i =>
        expectResult(values(i), "Wrong column element value")(columnType.extract(buffer))
        expectResult(2, "Wrong run length")(buffer.getInt())
      }

      // -------------
      // Tests decoder
      // -------------

      // Rewinds, skips column header and 4 more bytes for compression scheme ID
      buffer.rewind().position(headerSize + 4)

      val decoder = new RunLengthEncoding.Decoder[T](buffer, columnType)

      Array(0, 0, 1, 1).foreach { i =>
        expectResult(values(i), "Wrong decoded value")(decoder.next())
      }

      assert(!decoder.hasNext)
    }

    test(s"$RunLengthEncoding with $typeName: run length == 1") {
      // -------------
      // Tests encoder
      // -------------

      val builder = TestCompressibleColumnBuilder(columnStats, columnType, RunLengthEncoding)
      val (values, rows) = makeUniqueValuesAndSingleValueRows(columnType, 2)

      builder.initialize(0)
      builder.appendFrom(rows(0), 0)
      builder.appendFrom(rows(1), 0)

      val buffer = builder.build()
      val headerSize = CompressionScheme.columnHeaderSize(buffer)
      // 4 bytes each run for run length
      val compressedSize = values.map(columnType.actualSize(_) + 4).sum
      // 4 bytes for compression scheme type ID
      expectResult(headerSize + 4 + compressedSize, "Wrong buffer capacity")(buffer.capacity)

      // Skips column header
      buffer.position(headerSize)
      expectResult(RunLengthEncoding.typeId, "Wrong compression scheme ID")(buffer.getInt())

      Array(0, 1).foreach { i =>
        expectResult(values(i), "Wrong column element value")(columnType.extract(buffer))
        expectResult(1, "Wrong run length")(buffer.getInt())
      }

      // -------------
      // Tests decoder
      // -------------

      // Rewinds, skips column header and 4 more bytes for compression scheme ID
      buffer.rewind().position(headerSize + 4)

      val decoder = new RunLengthEncoding.Decoder[T](buffer, columnType)

      Array(0, 1).foreach { i =>
        expectResult(values(i), "Wrong decoded value")(decoder.next())
      }

      assert(!decoder.hasNext)
    }
  }
}
