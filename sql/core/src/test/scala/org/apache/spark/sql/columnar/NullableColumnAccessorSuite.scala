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

package org.apache.spark.sql.columnar

import java.nio.ByteBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.{StringType, ArrayType, DataType}

class TestNullableColumnAccessor[JvmType](
    buffer: ByteBuffer,
    columnType: ColumnType[JvmType])
  extends BasicColumnAccessor(buffer, columnType)
  with NullableColumnAccessor

object TestNullableColumnAccessor {
  def apply[JvmType](buffer: ByteBuffer, columnType: ColumnType[JvmType])
    : TestNullableColumnAccessor[JvmType] = {
    // Skips the column type ID
    buffer.getInt()
    new TestNullableColumnAccessor(buffer, columnType)
  }
}

class NullableColumnAccessorSuite extends SparkFunSuite {
  import ColumnarTestUtils._

  Seq(
    BOOLEAN, BYTE, SHORT, INT, DATE, LONG, TIMESTAMP, FLOAT, DOUBLE,
    STRING, BINARY, FIXED_DECIMAL(15, 10), GENERIC(ArrayType(StringType)))
    .foreach {
    testNullableColumnAccessor(_)
  }

  def testNullableColumnAccessor[JvmType](
      columnType: ColumnType[JvmType]): Unit = {

    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")
    val nullRow = makeNullRow(1)

    test(s"Nullable $typeName column accessor: empty column") {
      val builder = TestNullableColumnBuilder(columnType)
      val accessor = TestNullableColumnAccessor(builder.build(), columnType)
      assert(!accessor.hasNext)
    }

    test(s"Nullable $typeName column accessor: access null values") {
      val builder = TestNullableColumnBuilder(columnType)
      val randomRow = makeRandomRow(columnType)

      (0 until 4).foreach { _ =>
        builder.appendFrom(randomRow, 0)
        builder.appendFrom(nullRow, 0)
      }

      val accessor = TestNullableColumnAccessor(builder.build(), columnType)
      val row = new GenericMutableRow(1)

      (0 until 4).foreach { _ =>
        assert(accessor.hasNext)
        accessor.extractTo(row, 0)
        assert(row.get(0, columnType.dataType) === randomRow.get(0, columnType.dataType))

        assert(accessor.hasNext)
        accessor.extractTo(row, 0)
        assert(row.isNullAt(0))
      }

      assert(!accessor.hasNext)
    }
  }
}
