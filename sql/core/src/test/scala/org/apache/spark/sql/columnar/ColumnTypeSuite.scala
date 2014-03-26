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

import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.SparkSqlSerializer

class ColumnTypeSuite extends FunSuite {
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

  testNumericColumnType[BooleanType.type, Boolean](
    BOOLEAN,
    Array.fill(4)(Random.nextBoolean()),
    ByteBuffer.allocate(32),
    (buffer: ByteBuffer, v: Boolean) => {
      buffer.put((if (v) 1 else 0).toByte)
    },
    (buffer: ByteBuffer) => {
      buffer.get() == 1
    })

  testNumericColumnType[IntegerType.type, Int](
    INT,
    Array.fill(4)(Random.nextInt()),
    ByteBuffer.allocate(32),
    (_: ByteBuffer).putInt(_),
    (_: ByteBuffer).getInt)

  testNumericColumnType[ShortType.type, Short](
    SHORT,
    Array.fill(4)(Random.nextInt(Short.MaxValue).asInstanceOf[Short]),
    ByteBuffer.allocate(32),
    (_: ByteBuffer).putShort(_),
    (_: ByteBuffer).getShort)

  testNumericColumnType[LongType.type, Long](
    LONG,
    Array.fill(4)(Random.nextLong()),
    ByteBuffer.allocate(64),
    (_: ByteBuffer).putLong(_),
    (_: ByteBuffer).getLong)

  testNumericColumnType[ByteType.type, Byte](
    BYTE,
    Array.fill(4)(Random.nextInt(Byte.MaxValue).asInstanceOf[Byte]),
    ByteBuffer.allocate(64),
    (_: ByteBuffer).put(_),
    (_: ByteBuffer).get)

  testNumericColumnType[DoubleType.type, Double](
    DOUBLE,
    Array.fill(4)(Random.nextDouble()),
    ByteBuffer.allocate(64),
    (_: ByteBuffer).putDouble(_),
    (_: ByteBuffer).getDouble)

  testNumericColumnType[FloatType.type, Float](
    FLOAT,
    Array.fill(4)(Random.nextFloat()),
    ByteBuffer.allocate(64),
    (_: ByteBuffer).putFloat(_),
    (_: ByteBuffer).getFloat)

  test("STRING") {
    val buffer = ByteBuffer.allocate(128)
    val seq = Array("hello", "world", "spark", "sql")

    seq.map(_.getBytes).foreach { bytes: Array[Byte] =>
      buffer.putInt(bytes.length).put(bytes)
    }

    buffer.rewind()
    seq.foreach { s =>
      assert(s === STRING.extract(buffer))
    }

    buffer.rewind()
    seq.foreach(STRING.append(_, buffer))

    buffer.rewind()
    seq.foreach { s =>
      val length = buffer.getInt
      assert(length === s.getBytes.length)

      val bytes = new Array[Byte](length)
      buffer.get(bytes, 0, length)
      assert(s === new String(bytes))
    }
  }

  test("BINARY") {
    val buffer = ByteBuffer.allocate(128)
    val seq = Array.fill(4) {
      val bytes = new Array[Byte](4)
      Random.nextBytes(bytes)
      bytes
    }

    seq.foreach { bytes =>
      buffer.putInt(bytes.length).put(bytes)
    }

    buffer.rewind()
    seq.foreach { b =>
      assert(b === BINARY.extract(buffer))
    }

    buffer.rewind()
    seq.foreach(BINARY.append(_, buffer))

    buffer.rewind()
    seq.foreach { b =>
      val length = buffer.getInt
      assert(length === b.length)

      val bytes = new Array[Byte](length)
      buffer.get(bytes, 0, length)
      assert(b === bytes)
    }
  }

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

  def testNumericColumnType[T <: DataType, JvmType](
      columnType: ColumnType[T, JvmType],
      seq: Seq[JvmType],
      buffer: ByteBuffer,
      putter: (ByteBuffer, JvmType) => Unit,
      getter: (ByteBuffer) => JvmType) {

    val columnTypeName = columnType.getClass.getSimpleName.stripSuffix("$")

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
