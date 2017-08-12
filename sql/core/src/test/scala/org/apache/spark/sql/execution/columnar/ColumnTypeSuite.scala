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

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._
import org.apache.spark.sql.types._

class ColumnTypeSuite extends SparkFunSuite with Logging {
  private val DEFAULT_BUFFER_SIZE = 512
  private val MAP_TYPE = MAP(MapType(IntegerType, StringType))
  private val ARRAY_TYPE = ARRAY(ArrayType(IntegerType))
  private val STRUCT_TYPE = STRUCT(StructType(StructField("a", StringType) :: Nil))

  test("defaultSize") {
    val checks = Map(
      NULL -> 0, BOOLEAN -> 1, BYTE -> 1, SHORT -> 2, INT -> 4, LONG -> 8,
      FLOAT -> 4, DOUBLE -> 8, COMPACT_DECIMAL(15, 10) -> 8, LARGE_DECIMAL(20, 10) -> 12,
      STRING -> 8, BINARY -> 16, STRUCT_TYPE -> 20, ARRAY_TYPE -> 28, MAP_TYPE -> 68)

    checks.foreach { case (columnType, expectedSize) =>
      assertResult(expectedSize, s"Wrong defaultSize for $columnType") {
        columnType.defaultSize
      }
    }
  }

  test("actualSize") {
    def checkActualSize(
        columnType: ColumnType[_],
        value: Any,
        expected: Int): Unit = {

      assertResult(expected, s"Wrong actualSize for $columnType") {
        val row = new GenericInternalRow(1)
        row.update(0, CatalystTypeConverters.convertToCatalyst(value))
        val proj = UnsafeProjection.create(Array[DataType](columnType.dataType))
        columnType.actualSize(proj(row), 0)
      }
    }

    checkActualSize(NULL, null, 0)
    checkActualSize(BOOLEAN, true, 1)
    checkActualSize(BYTE, Byte.MaxValue, 1)
    checkActualSize(SHORT, Short.MaxValue, 2)
    checkActualSize(INT, Int.MaxValue, 4)
    checkActualSize(LONG, Long.MaxValue, 8)
    checkActualSize(FLOAT, Float.MaxValue, 4)
    checkActualSize(DOUBLE, Double.MaxValue, 8)
    checkActualSize(STRING, "hello", 4 + "hello".getBytes(StandardCharsets.UTF_8).length)
    checkActualSize(BINARY, Array.fill[Byte](4)(0.toByte), 4 + 4)
    checkActualSize(COMPACT_DECIMAL(15, 10), Decimal(0, 15, 10), 8)
    checkActualSize(LARGE_DECIMAL(20, 10), Decimal(0, 20, 10), 5)
    checkActualSize(ARRAY_TYPE, Array[Any](1), 4 + 8 + 8 + 8)
    checkActualSize(MAP_TYPE, Map(1 -> "a"), 4 + (8 + 8 + 8 + 8) + (8 + 8 + 8 + 8))
    checkActualSize(STRUCT_TYPE, Row("hello"), 28)
  }

  testNativeColumnType(BOOLEAN)
  testNativeColumnType(BYTE)
  testNativeColumnType(SHORT)
  testNativeColumnType(INT)
  testNativeColumnType(LONG)
  testNativeColumnType(FLOAT)
  testNativeColumnType(DOUBLE)
  testNativeColumnType(COMPACT_DECIMAL(15, 10))
  testNativeColumnType(STRING)

  testColumnType(NULL)
  testColumnType(BINARY)
  testColumnType(LARGE_DECIMAL(20, 10))
  testColumnType(STRUCT_TYPE)
  testColumnType(ARRAY_TYPE)
  testColumnType(MAP_TYPE)

  def testNativeColumnType[T <: AtomicType](columnType: NativeColumnType[T]): Unit = {
    testColumnType[T#InternalType](columnType)
  }

  def testColumnType[JvmType](columnType: ColumnType[JvmType]): Unit = {

    val proj = UnsafeProjection.create(Array[DataType](columnType.dataType))
    val converter = CatalystTypeConverters.createToScalaConverter(columnType.dataType)
    val seq = (0 until 4).map(_ => proj(makeRandomRow(columnType)).copy())
    val totalSize = seq.map(_.getSizeInBytes).sum
    val bufferSize = Math.max(DEFAULT_BUFFER_SIZE, totalSize)

    test(s"$columnType append/extract") {
      val buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.nativeOrder())
      seq.foreach(r => columnType.append(columnType.getField(r, 0), buffer))

      buffer.rewind()
      seq.foreach { row =>
        logInfo("buffer = " + buffer + ", expected = " + row)
        val expected = converter(row.get(0, columnType.dataType))
        val extracted = converter(columnType.extract(buffer))
        assert(expected === extracted,
          s"Extracted value didn't equal to the original one. $expected != $extracted, buffer =" +
          dumpBuffer(buffer.duplicate().rewind().asInstanceOf[ByteBuffer]))
      }
    }
  }

  private def dumpBuffer(buff: ByteBuffer): Any = {
    val sb = new StringBuilder()
    while (buff.hasRemaining) {
      val b = buff.get()
      sb.append(Integer.toHexString(b & 0xff)).append(' ')
    }
    if (sb.nonEmpty) sb.setLength(sb.length - 1)
    sb.toString()
  }

  test("column type for decimal types with different precision") {
    (1 to 18).foreach { i =>
      assertResult(COMPACT_DECIMAL(i, 0)) {
        ColumnType(DecimalType(i, 0))
      }
    }

    assertResult(LARGE_DECIMAL(19, 0)) {
      ColumnType(DecimalType(19, 0))
    }
  }

  test("show type name in type mismatch error") {
    val invalidType = new DataType {
        override def defaultSize: Int = 1
        override private[spark] def asNullable: DataType = this
        override def typeName: String = "invalid type name"
    }

    val message = intercept[java.lang.Exception] {
      ColumnType(invalidType)
    }.getMessage

    assert(message.contains("Unsupported type: invalid type name"))
  }
}
