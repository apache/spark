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

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.columnar.ColumnarTestUtils._
import org.apache.spark.sql.execution.SparkSqlSerializer

class ColumnTypeSuite extends FunSuite {
  val DEFAULT_BUFFER_SIZE = 512

  val columnTypes = Seq(INT, SHORT, LONG, BYTE, DOUBLE, FLOAT, STRING, BINARY, GENERIC)

  test("defaultSize") {
    val defaultSize = Seq(4, 2, 8, 1, 8, 4, 8, 16, 16)

    columnTypes.zip(defaultSize).foreach { case (columnType, size) =>
      assert(columnType.defaultSize === size)
    }
  }

  test("actualSize") {
    val expectedSizes = Seq(4, 2, 8, 1, 8, 4, 4 + 5, 4 + 4, 4 + 11)
    val actualSizes = Seq(
      INT.actualSize(Int.MaxValue),
      SHORT.actualSize(Short.MaxValue),
      LONG.actualSize(Long.MaxValue),
      BYTE.actualSize(Byte.MaxValue),
      DOUBLE.actualSize(Double.MaxValue),
      FLOAT.actualSize(Float.MaxValue),
      STRING.actualSize("hello"),
      BINARY.actualSize(new Array[Byte](4)),
      GENERIC.actualSize(SparkSqlSerializer.serialize(Map(1 -> "a"))))

    expectedSizes.zip(actualSizes).foreach { case (expected, actual) =>
      assert(expected === actual)
    }
  }

  testNativeColumnStats[BooleanType.type](
    BOOLEAN,
    (buffer: ByteBuffer, v: Boolean) => {
      buffer.put((if (v) 1 else 0).toByte)
    },
    (buffer: ByteBuffer) => {
      buffer.get() == 1
    })

  testNativeColumnStats[IntegerType.type](
    INT,
    (_: ByteBuffer).putInt(_),
    (_: ByteBuffer).getInt)

  testNativeColumnStats[ShortType.type](
    SHORT,
    (_: ByteBuffer).putShort(_),
    (_: ByteBuffer).getShort)

  testNativeColumnStats[LongType.type](
    LONG,
    (_: ByteBuffer).putLong(_),
    (_: ByteBuffer).getLong)

  testNativeColumnStats[ByteType.type](
    BYTE,
    (_: ByteBuffer).put(_),
    (_: ByteBuffer).get)

  testNativeColumnStats[DoubleType.type](
    DOUBLE,
    (_: ByteBuffer).putDouble(_),
    (_: ByteBuffer).getDouble)

  testNativeColumnStats[FloatType.type](
    FLOAT,
    (_: ByteBuffer).putFloat(_),
    (_: ByteBuffer).getFloat)

  testNativeColumnStats[StringType.type](
    STRING,
    (buffer: ByteBuffer, string: String) => {
      val bytes = string.getBytes()
      buffer.putInt(bytes.length).put(string.getBytes)
    },
    (buffer: ByteBuffer) => {
      val length = buffer.getInt()
      val bytes = new Array[Byte](length)
      buffer.get(bytes, 0, length)
      new String(bytes)
    })

  testColumnStats[BinaryType.type, Array[Byte]](
    BINARY,
    (buffer: ByteBuffer, bytes: Array[Byte]) => {
      buffer.putInt(bytes.length).put(bytes)
    },
    (buffer: ByteBuffer) => {
      val length = buffer.getInt()
      val bytes = new Array[Byte](length)
      buffer.get(bytes, 0, length)
      bytes
    })

  test("GENERIC") {
    val buffer = ByteBuffer.allocate(512)
    val obj = Map(1 -> "spark", 2 -> "sql")
    val serializedObj = SparkSqlSerializer.serialize(obj)

    GENERIC.append(SparkSqlSerializer.serialize(obj), buffer)
    buffer.rewind()

    val length = buffer.getInt()
    assert(length === serializedObj.length)

    val bytes = new Array[Byte](length)
    buffer.get(bytes, 0, length)
    assert(obj === SparkSqlSerializer.deserialize(bytes))

    buffer.rewind()
    buffer.putInt(serializedObj.length).put(serializedObj)

    buffer.rewind()
    assert(obj === SparkSqlSerializer.deserialize(GENERIC.extract(buffer)))
  }

  def testNativeColumnStats[T <: NativeType](
      columnType: NativeColumnType[T],
      putter: (ByteBuffer, T#JvmType) => Unit,
      getter: (ByteBuffer) => T#JvmType) {

    testColumnStats[T, T#JvmType](columnType, putter, getter)
  }

  def testColumnStats[T <: DataType, JvmType](
      columnType: ColumnType[T, JvmType],
      putter: (ByteBuffer, JvmType) => Unit,
      getter: (ByteBuffer) => JvmType) {

    val buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE)
    val columnTypeName = columnType.getClass.getSimpleName.stripSuffix("$")
    val seq = (0 until 4).map(_ => makeRandomValue(columnType))

    test(s"$columnTypeName.extract") {
      buffer.rewind()
      seq.foreach(putter(buffer, _))

      buffer.rewind()
      seq.foreach { i =>
        assert(i === columnType.extract(buffer))
      }
    }

    test(s"$columnTypeName.append") {
      buffer.rewind()
      seq.foreach(columnType.append(_, buffer))

      buffer.rewind()
      seq.foreach { i =>
        assert(i === getter(buffer))
      }
    }
  }
}
