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

import java.nio.ByteBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.collation.CollationFactory
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.types.{PhysicalArrayType, PhysicalMapType, PhysicalStructType}
import org.apache.spark.sql.types._

class TestNullableColumnAccessor[JvmType](
    buffer: ByteBuffer,
    columnType: ColumnType[JvmType])
  extends BasicColumnAccessor(buffer, columnType)
  with NullableColumnAccessor

object TestNullableColumnAccessor {
  def apply[JvmType](buffer: ByteBuffer, columnType: ColumnType[JvmType])
    : TestNullableColumnAccessor[JvmType] = {
    new TestNullableColumnAccessor(buffer, columnType)
  }
}

class NullableColumnAccessorSuite extends SparkFunSuite {
  import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._

  val stringTypes = Seq(
    STRING(StringType), // UTF8_BINARY
    STRING(StringType("UTF8_LCASE")),
    STRING(StringType("UNICODE")),
    STRING(StringType("UNICODE_CI")))
  val otherTypes = Seq(
    NULL, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE,
    BINARY, COMPACT_DECIMAL(15, 10), LARGE_DECIMAL(20, 10),
    STRUCT(PhysicalStructType(Array(StructField("a", StringType)))),
    ARRAY(PhysicalArrayType(IntegerType, true)),
    MAP(PhysicalMapType(IntegerType, StringType, true)),
    CALENDAR_INTERVAL)

  stringTypes.foreach(s => {
    val collation = CollationFactory.fetchCollation(s.collationId).collationName
    val typeName = if (collation == "UTF8_BINARY") "STRING" else s"STRING($collation)"
    testNullableColumnAccessor(s, Some(typeName))
  })
  otherTypes.foreach {
    testNullableColumnAccessor(_)
  }

  def testNullableColumnAccessor[JvmType](
      columnType: ColumnType[JvmType],
      testTypeName: Option[String] = None): Unit = {

    val typeName = testTypeName.getOrElse(columnType.getClass.getSimpleName.stripSuffix("$"))
    val nullRow = makeNullRow(1)

    test(s"Nullable $typeName column accessor: empty column") {
      val builder = TestNullableColumnBuilder(columnType)
      val accessor = TestNullableColumnAccessor(builder.build(), columnType)
      assert(!accessor.hasNext)
    }

    test(s"Nullable $typeName column accessor: access null values") {
      val builder = TestNullableColumnBuilder(columnType)
      val randomRow = makeRandomRow(columnType)
      val proj = UnsafeProjection.create(Array[DataType](
        ColumnarDataTypeUtils.toLogicalDataType(columnType.dataType)))

      (0 until 4).foreach { _ =>
        builder.appendFrom(proj(randomRow), 0)
        builder.appendFrom(proj(nullRow), 0)
      }

      val accessor = TestNullableColumnAccessor(builder.build(), columnType)
      val row = new GenericInternalRow(1)
      val converter = CatalystTypeConverters.createToScalaConverter(
        ColumnarDataTypeUtils.toLogicalDataType(columnType.dataType))

      (0 until 4).foreach { _ =>
        assert(accessor.hasNext)
        accessor.extractTo(row, 0)
        assert(converter(row.get(0,
          ColumnarDataTypeUtils.toLogicalDataType(columnType.dataType)))
          === converter(randomRow.get(0,
          ColumnarDataTypeUtils.toLogicalDataType(columnType.dataType))))

        assert(accessor.hasNext)
        accessor.extractTo(row, 0)
        assert(row.isNullAt(0))
      }

      assert(!accessor.hasNext)
    }
  }
}
