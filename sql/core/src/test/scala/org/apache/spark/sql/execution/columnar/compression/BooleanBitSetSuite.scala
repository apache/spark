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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.columnar.{BOOLEAN, NoopColumnStats}
import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.BooleanType

class BooleanBitSetSuite extends SparkFunSuite {
  import BooleanBitSet._

  def skeleton(count: Int): Unit = {
    // -------------
    // Tests encoder
    // -------------

    val builder = TestCompressibleColumnBuilder(new NoopColumnStats, BOOLEAN, BooleanBitSet)
    val rows = Seq.fill[InternalRow](count)(makeRandomRow(BOOLEAN))
    val values = rows.map(_.getBoolean(0))

    rows.foreach(builder.appendFrom(_, 0))
    val buffer = builder.build()

    // Column type ID + null count + null positions
    val headerSize = CompressionScheme.columnHeaderSize(buffer)

    // Compression scheme ID + element count + bitset words
    val compressedSize = 4 + 4 + {
      val extra = if (count % BITS_PER_LONG == 0) 0 else 1
      (count / BITS_PER_LONG + extra) * 8
    }

    // 4 extra bytes for compression scheme type ID
    assertResult(headerSize + compressedSize, "Wrong buffer capacity")(buffer.capacity)

    // Skips column header
    buffer.position(headerSize)
    assertResult(BooleanBitSet.typeId, "Wrong compression scheme ID")(buffer.getInt())
    assertResult(count, "Wrong element count")(buffer.getInt())

    var word = 0: Long
    for (i <- 0 until count) {
      val bit = i % BITS_PER_LONG
      word = if (bit == 0) buffer.getLong() else word
      assertResult(values(i), s"Wrong value in compressed buffer, index=$i") {
        (word & ((1: Long) << bit)) != 0
      }
    }

    // -------------
    // Tests decoder
    // -------------

    // Rewinds, skips column header and 4 more bytes for compression scheme ID
    buffer.rewind().position(headerSize + 4)

    val decoder = BooleanBitSet.decoder(buffer, BOOLEAN)
    val mutableRow = new GenericInternalRow(1)
    if (values.nonEmpty) {
      values.foreach {
        assert(decoder.hasNext)
        assertResult(_, "Wrong decoded value") {
          decoder.next(mutableRow, 0)
          mutableRow.getBoolean(0)
        }
      }
    }
    assert(!decoder.hasNext)
  }

  def skeletonForDecompress(count: Int): Unit = {
    val builder = TestCompressibleColumnBuilder(new NoopColumnStats, BOOLEAN, BooleanBitSet)
    val rows = Seq.fill[InternalRow](count)(makeRandomRow(BOOLEAN))
    val values = rows.map(_.getBoolean(0))

    rows.foreach(builder.appendFrom(_, 0))
    val buffer = builder.build()

    // ----------------
    // Tests decompress
    // ----------------

    // Rewinds, skips column header and 4 more bytes for compression scheme ID
    val headerSize = CompressionScheme.columnHeaderSize(buffer)
    buffer.position(headerSize)
    assertResult(BooleanBitSet.typeId, "Wrong compression scheme ID")(buffer.getInt())

    val decoder = BooleanBitSet.decoder(buffer, BOOLEAN)
    val columnVector = new OnHeapColumnVector(values.length, BooleanType)
    decoder.decompress(columnVector, values.length)

    if (values.nonEmpty) {
      values.zipWithIndex.foreach { case (b: Boolean, index: Int) =>
        assertResult(b, s"Wrong ${index}-th decoded boolean value") {
          columnVector.getBoolean(index)
        }
      }
    }
  }

  test(s"$BooleanBitSet: empty") {
    skeleton(0)
  }

  test(s"$BooleanBitSet: less than 1 word") {
    skeleton(BITS_PER_LONG - 1)
  }

  test(s"$BooleanBitSet: exactly 1 word") {
    skeleton(BITS_PER_LONG)
  }

  test(s"$BooleanBitSet: multiple whole words") {
    skeleton(BITS_PER_LONG * 2)
  }

  test(s"$BooleanBitSet: multiple words and 1 more bit") {
    skeleton(BITS_PER_LONG * 2 + 1)
  }

  test(s"$BooleanBitSet: empty for decompression()") {
    skeletonForDecompress(0)
  }

  test(s"$BooleanBitSet: less than 1 word for decompression()") {
    skeletonForDecompress(BITS_PER_LONG - 1)
  }

  test(s"$BooleanBitSet: exactly 1 word for decompression()") {
    skeletonForDecompress(BITS_PER_LONG)
  }

  test(s"$BooleanBitSet: multiple whole words for decompression()") {
    skeletonForDecompress(BITS_PER_LONG * 2)
  }

  test(s"$BooleanBitSet: multiple words and 1 more bit for decompression()") {
    skeletonForDecompress(BITS_PER_LONG * 2 + 1)
  }

  test(s"$BooleanBitSet: Only nulls for decompression()") {
    val builder = TestCompressibleColumnBuilder(new NoopColumnStats, BOOLEAN, BooleanBitSet)
    val numRows = 10

    val rows = Seq.fill[InternalRow](numRows)({
      val row = new GenericInternalRow(1)
      row.setNullAt(0)
      row
    })
    rows.foreach(builder.appendFrom(_, 0))
    val buffer = builder.build()

    // Rewinds, skips column header and 4 more bytes for compression scheme ID
    val headerSize = CompressionScheme.columnHeaderSize(buffer)
    buffer.position(headerSize)
    assertResult(BooleanBitSet.typeId, "Wrong compression scheme ID")(buffer.getInt())

    val decoder = BooleanBitSet.decoder(buffer, BOOLEAN)
    val columnVector = new OnHeapColumnVector(numRows, BooleanType)
    decoder.decompress(columnVector, numRows)

    (0 until numRows).foreach { rowNum =>
      assert(columnVector.isNullAt(rowNum))
    }
  }
}
