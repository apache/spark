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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.collation.CollationFactory
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.types.{PhysicalArrayType, PhysicalMapType, PhysicalStructType}
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
  import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._

  val stringTypes = Seq(
    STRING(StringType), // UTF8_BINARY
    STRING(StringType("UTF8_LCASE")),
    STRING(StringType("UNICODE")),
    STRING(StringType("UNICODE_CI")))
  val otherTypes = Seq(
    BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE,
    BINARY, COMPACT_DECIMAL(15, 10), LARGE_DECIMAL(20, 10),
    STRUCT(PhysicalStructType(Array(StructField("a", StringType)))),
    ARRAY(PhysicalArrayType(IntegerType, true)),
    MAP(PhysicalMapType(IntegerType, StringType, true)),
    CALENDAR_INTERVAL)

  stringTypes.foreach(s => {
    val collation = CollationFactory.fetchCollation(s.collationId).collationName
    val typeName = if (collation == "UTF8_BINARY") "STRING" else s"STRING($collation)"
    testNullableColumnBuilder(s, Some(typeName))
  })
  otherTypes.foreach {
    testNullableColumnBuilder(_)
  }

  def testNullableColumnBuilder[JvmType](
      columnType: ColumnType[JvmType],
      testTypeName: Option[String] = None): Unit = {

    val typeName = testTypeName.getOrElse(columnType.getClass.getSimpleName.stripSuffix("$"))
    val dataType = columnType.dataType
    val proj = UnsafeProjection.create(Array[DataType](
      ColumnarDataTypeUtils.toLogicalDataType(dataType)))
    val converter = CatalystTypeConverters.createToScalaConverter(
      ColumnarDataTypeUtils.toLogicalDataType(dataType))

    test(s"$typeName column builder: empty column") {
      val columnBuilder = TestNullableColumnBuilder(columnType)
      val buffer = columnBuilder.build()

      assertResult(0, "Wrong null count")(buffer.getInt())
      assert(!buffer.hasRemaining)
    }

    test(s"$typeName column builder: buffer size auto growth") {
      val columnBuilder = TestNullableColumnBuilder(columnType)
      val randomRow = makeRandomRow(columnType)

      (0 until 4).foreach { _ =>
        columnBuilder.appendFrom(proj(randomRow), 0)
      }

      val buffer = columnBuilder.build()

      assertResult(0, "Wrong null count")(buffer.getInt())
    }

    test(s"$typeName column builder: null values") {
      val columnBuilder = TestNullableColumnBuilder(columnType)
      val randomRow = makeRandomRow(columnType)
      val nullRow = makeNullRow(1)

      (0 until 4).foreach { _ =>
        columnBuilder.appendFrom(proj(randomRow), 0)
        columnBuilder.appendFrom(proj(nullRow), 0)
      }

      val buffer = columnBuilder.build()

      assertResult(4, "Wrong null count")(buffer.getInt())

      // For null positions
      (1 to 7 by 2).foreach(assertResult(_, "Wrong null position")(buffer.getInt()))

      // For non-null values
      val actual = new GenericInternalRow(new Array[Any](1))
      (0 until 4).foreach { _ =>
        columnType.extract(buffer, actual, 0)
        assert(converter(actual.get(0,
          ColumnarDataTypeUtils.toLogicalDataType(dataType))) ===
          converter(randomRow.get(0, ColumnarDataTypeUtils.toLogicalDataType(dataType))),
          "Extracted value didn't equal to the original one")
      }

      assert(!buffer.hasRemaining)
    }
  }
}
