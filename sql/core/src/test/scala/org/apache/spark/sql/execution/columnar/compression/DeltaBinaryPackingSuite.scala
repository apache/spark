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
import org.apache.spark.sql.types.IntegralType

class DeltaBinaryPackingSuite extends SparkFunSuite {
  import DeltaBinaryPacking._

  testDeltaBinaryPacking(new IntColumnStats, INT, IntDeltaBinaryPacking)

  def testDeltaBinaryPacking[I <: IntegralType](
      columnStats: ColumnStats,
      columnType: NativeColumnType[I],
      scheme: CompressionScheme) {

    def skeleton(input: Seq[I#InternalType]) {
      // -------------
      // Tests encoder
      // -------------

      val builder = TestCompressibleColumnBuilder(columnStats, columnType, scheme)
      input.map { value =>
        val row = new GenericMutableRow(1)
        columnType.setField(row, 0, value)
        builder.appendFrom(row, 0)
      }

      val buffer = builder.build()

      // -------------
      // Tests decoder
      // -------------

      // Column type ID + null count + null positions
      val headerSize = CompressionScheme.columnHeaderSize(buffer)

      // Rewinds, skips column header and 4 more bytes for compression scheme ID
      buffer.rewind().position(headerSize + 4)

      val decoder = scheme.decoder(buffer, columnType)
      val mutableRow = new GenericMutableRow(1)

      if (input.nonEmpty) {
        input.zipWithIndex.foreach { case (value, index) =>
          assert(decoder.hasNext)
          assertResult(value, s"Wrong ${index}-th decoded value") {
            decoder.next(mutableRow, 0)
            columnType.getField(mutableRow, 0)
          }
        }
      }
      assert(!decoder.hasNext)
    }

    test(s"$scheme: empty column") {
      skeleton(Seq.empty)
    }

    test(s"$scheme: write data when data aligned with blocks") {
      val input = Array.fill[Any](5 * NUM_VALUES_IN_BLOCK)(makeRandomValue(columnType))
      skeleton(input.map(_.asInstanceOf[I#InternalType]))
    }

    test(s"$scheme: write data when blocks not fully written") {
      val input = Array.fill[Any](3 * NUM_VALUES_IN_BLOCK - 3)(makeRandomValue(columnType))
      skeleton(input.map(_.asInstanceOf[I#InternalType]))
    }

    test(s"$scheme: write data when mini blocks not fully written") {
      val input = Array.fill[Any](3 * NUM_VALUES_IN_MINIBLOCK - 3)(makeRandomValue(columnType))
      skeleton(input.map(_.asInstanceOf[I#InternalType]))
    }

    test(s"$scheme: write negative deltas") {
      val input = Array.fill[Any](3 * NUM_VALUES_IN_BLOCK)(makeRandomValue(INT))
        .zipWithIndex.map {
          case (value: Int, index: Int) =>
            (10 - (index * 32 - value % 6)).asInstanceOf[I#InternalType]
        }
      skeleton(input)
    }

    test(s"$scheme: write same deltas") {
      val input = (0 until 3 * NUM_VALUES_IN_BLOCK).zipWithIndex.map {
        case (value, index) => (32 * index).asInstanceOf[I#InternalType]
      }
      skeleton(input)
    }

    test(s"$scheme: write same values") {
      skeleton((0 until 3 * NUM_VALUES_IN_BLOCK).map(_ => 32.asInstanceOf[I#InternalType]))
    }

    test(s"$scheme: write data when deltas having 0") {
      val input = (0 until 3 * NUM_VALUES_IN_BLOCK).zipWithIndex.map {
        case (value, index) => (index / 15).asInstanceOf[I#InternalType]
      }
      skeleton(input)
    }

    test(s"$scheme: write min and max data") {
      val input = (0 until 3 * NUM_VALUES_IN_BLOCK).zipWithIndex.map {
        case (value, index) if columnType == INT =>
          (if (index % 2 == 0) Int.MinValue else Int.MaxValue).asInstanceOf[I#InternalType]
      }
      skeleton(input)
    }
  }
}
