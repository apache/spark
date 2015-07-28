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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.types._

class TestNullableColumnBuilder[JvmType](columnType: ColumnType[JvmType])
  extends BasicColumnBuilder[JvmType](new NoopColumnStats, columnType)
  with NullableColumnBuilder

object TestNullableColumnBuilder {
  def apply[JvmType](columnType: ColumnType[JvmType], initialSize: Int = 0)
    : TestNullableColumnBuilder[JvmType] = {
    val builder = new TestNullableColumnBuilder(columnType)
    builder.initialize(initialSize)
    builder
  }
}

class NullableColumnBuilderSuite extends SparkFunSuite {
  import ColumnarTestUtils._

  Seq(
    BOOLEAN, BYTE, SHORT, INT, DATE, LONG, TIMESTAMP, FLOAT, DOUBLE,
    STRING, BINARY, FIXED_DECIMAL(15, 10), GENERIC(ArrayType(StringType)))
    .foreach {
    testNullableColumnBuilder(_)
  }

  def testNullableColumnBuilder[JvmType](
      columnType: ColumnType[JvmType]): Unit = {

    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")

    test(s"$typeName column builder: empty column") {
      val columnBuilder = TestNullableColumnBuilder(columnType)
      val buffer = columnBuilder.build()

      assertResult(columnType.typeId, "Wrong column type ID")(buffer.getInt())
      assertResult(0, "Wrong null count")(buffer.getInt())
      assert(!buffer.hasRemaining)
    }

    test(s"$typeName column builder: buffer size auto growth") {
      val columnBuilder = TestNullableColumnBuilder(columnType)
      val randomRow = makeRandomRow(columnType)

      (0 until 4).foreach { _ =>
        columnBuilder.appendFrom(randomRow, 0)
      }

      val buffer = columnBuilder.build()

      assertResult(columnType.typeId, "Wrong column type ID")(buffer.getInt())
      assertResult(0, "Wrong null count")(buffer.getInt())
    }

    test(s"$typeName column builder: null values") {
      val columnBuilder = TestNullableColumnBuilder(columnType)
      val randomRow = makeRandomRow(columnType)
      val nullRow = makeNullRow(1)

      (0 until 4).foreach { _ =>
        columnBuilder.appendFrom(randomRow, 0)
        columnBuilder.appendFrom(nullRow, 0)
      }

      val buffer = columnBuilder.build()

      assertResult(columnType.typeId, "Wrong column type ID")(buffer.getInt())
      assertResult(4, "Wrong null count")(buffer.getInt())

      // For null positions
      (1 to 7 by 2).foreach(assertResult(_, "Wrong null position")(buffer.getInt()))

      // For non-null values
      (0 until 4).foreach { _ =>
        val actual = if (columnType.isInstanceOf[GENERIC]) {
          SparkSqlSerializer.deserialize[Any](columnType.extract(buffer).asInstanceOf[Array[Byte]])
        } else {
          columnType.extract(buffer)
        }

        assert(actual === randomRow.get(0, columnType.dataType),
          "Extracted value didn't equal to the original one")
      }

      assert(!buffer.hasRemaining)
    }
  }
}
