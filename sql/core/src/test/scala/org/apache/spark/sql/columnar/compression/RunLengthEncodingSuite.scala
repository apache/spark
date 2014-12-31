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

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.types.NativeType
import org.apache.spark.sql.columnar._
import org.apache.spark.sql.columnar.ColumnarTestUtils._

class RunLengthEncodingSuite extends FunSuite {
  testRunLengthEncoding(new NoopColumnStats, BOOLEAN)
  testRunLengthEncoding(new ByteColumnStats,    BYTE)
  testRunLengthEncoding(new ShortColumnStats,   SHORT)
  testRunLengthEncoding(new IntColumnStats,     INT)
  testRunLengthEncoding(new LongColumnStats,    LONG)
  testRunLengthEncoding(new StringColumnStats,  STRING)

  def testRunLengthEncoding[T <: NativeType](
      columnStats: ColumnStats,
      columnType: NativeColumnType[T]) {

    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")

    def skeleton(uniqueValueCount: Int, inputRuns: Seq[(Int, Int)]) {
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

    test(s"$RunLengthEncoding with $typeName: empty column") {
      skeleton(0, Seq.empty)
    }

    test(s"$RunLengthEncoding with $typeName: simple case") {
      skeleton(2, Seq(0 -> 2, 1 ->2))
    }

    test(s"$RunLengthEncoding with $typeName: run length == 1") {
      skeleton(2, Seq(0 -> 1, 1 ->1))
    }

    test(s"$RunLengthEncoding with $typeName: single long run") {
      skeleton(1, Seq(0 -> 1000))
    }
  }
}
