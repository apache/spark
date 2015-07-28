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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

import org.apache.spark.{Logging, SparkConf, SparkFunSuite}
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.columnar.ColumnarTestUtils._
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


class ColumnTypeSuite extends SparkFunSuite with Logging {
  private val DEFAULT_BUFFER_SIZE = 512
  private val MAP_GENERIC = GENERIC(MapType(IntegerType, StringType))

  test("defaultSize") {
    val checks = Map(
      BOOLEAN -> 1, BYTE -> 1, SHORT -> 2, INT -> 4, DATE -> 4,
      LONG -> 8, TIMESTAMP -> 8, FLOAT -> 4, DOUBLE -> 8,
      STRING -> 8, BINARY -> 16, FIXED_DECIMAL(15, 10) -> 8,
      MAP_GENERIC -> 16)

    checks.foreach { case (columnType, expectedSize) =>
      assertResult(expectedSize, s"Wrong defaultSize for $columnType") {
        columnType.defaultSize
      }
    }
  }

  test("actualSize") {
    def checkActualSize[JvmType](
        columnType: ColumnType[JvmType],
        value: JvmType,
        expected: Int): Unit = {

      assertResult(expected, s"Wrong actualSize for $columnType") {
        val row = new GenericMutableRow(1)
        columnType.setField(row, 0, value)
        columnType.actualSize(row, 0)
      }
    }

    checkActualSize(BOOLEAN, true, 1)
    checkActualSize(BYTE, Byte.MaxValue, 1)
    checkActualSize(SHORT, Short.MaxValue, 2)
    checkActualSize(INT, Int.MaxValue, 4)
    checkActualSize(DATE, Int.MaxValue, 4)
    checkActualSize(LONG, Long.MaxValue, 8)
    checkActualSize(TIMESTAMP, Long.MaxValue, 8)
    checkActualSize(FLOAT, Float.MaxValue, 4)
    checkActualSize(DOUBLE, Double.MaxValue, 8)
    checkActualSize(STRING, UTF8String.fromString("hello"), 4 + "hello".getBytes("utf-8").length)
    checkActualSize(BINARY, Array.fill[Byte](4)(0.toByte), 4 + 4)
    checkActualSize(FIXED_DECIMAL(15, 10), Decimal(0, 15, 10), 8)

    val generic = Map(1 -> "a")
    checkActualSize(MAP_GENERIC, SparkSqlSerializer.serialize(generic), 4 + 8)
  }

  testNativeColumnType(BOOLEAN)(
    (buffer: ByteBuffer, v: Boolean) => {
      buffer.put((if (v) 1 else 0).toByte)
    },
    (buffer: ByteBuffer) => {
      buffer.get() == 1
    })

  testNativeColumnType(BYTE)(_.put(_), _.get)

  testNativeColumnType(SHORT)(_.putShort(_), _.getShort)

  testNativeColumnType(INT)(_.putInt(_), _.getInt)

  testNativeColumnType(DATE)(_.putInt(_), _.getInt)

  testNativeColumnType(LONG)(_.putLong(_), _.getLong)

  testNativeColumnType(TIMESTAMP)(_.putLong(_), _.getLong)

  testNativeColumnType(FLOAT)(_.putFloat(_), _.getFloat)

  testNativeColumnType(DOUBLE)(_.putDouble(_), _.getDouble)

  testNativeColumnType(FIXED_DECIMAL(15, 10))(
    (buffer: ByteBuffer, decimal: Decimal) => {
      buffer.putLong(decimal.toUnscaledLong)
    },
    (buffer: ByteBuffer) => {
      Decimal(buffer.getLong(), 15, 10)
    })


  testNativeColumnType(STRING)(
    (buffer: ByteBuffer, string: UTF8String) => {
      val bytes = string.getBytes
      buffer.putInt(bytes.length)
      buffer.put(bytes)
    },
    (buffer: ByteBuffer) => {
      val length = buffer.getInt()
      val bytes = new Array[Byte](length)
      buffer.get(bytes)
      UTF8String.fromBytes(bytes)
    })

  testColumnType[Array[Byte]](
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

    MAP_GENERIC.append(SparkSqlSerializer.serialize(obj), buffer)
    buffer.rewind()

    val length = buffer.getInt()
    assert(length === serializedObj.length)

    assertResult(obj, "Deserialized object didn't equal to the original object") {
      val bytes = new Array[Byte](length)
      buffer.get(bytes, 0, length)
      SparkSqlSerializer.deserialize(bytes)
    }

    buffer.rewind()
    buffer.putInt(serializedObj.length).put(serializedObj)

    assertResult(obj, "Deserialized object didn't equal to the original object") {
      buffer.rewind()
      SparkSqlSerializer.deserialize(MAP_GENERIC.extract(buffer))
    }
  }

  test("CUSTOM") {
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "org.apache.spark.sql.columnar.Registrator")
    val serializer = new SparkSqlSerializer(conf).newInstance()

    val buffer = ByteBuffer.allocate(512)
    val obj = CustomClass(Int.MaxValue, Long.MaxValue)
    val serializedObj = serializer.serialize(obj).array()

    MAP_GENERIC.append(serializer.serialize(obj).array(), buffer)
    buffer.rewind()

    val length = buffer.getInt
    assert(length === serializedObj.length)
    assert(13 == length) // id (1) + int (4) + long (8)

    val genericSerializedObj = SparkSqlSerializer.serialize(obj)
    assert(length != genericSerializedObj.length)
    assert(length < genericSerializedObj.length)

    assertResult(obj, "Custom deserialized object didn't equal the original object") {
      val bytes = new Array[Byte](length)
      buffer.get(bytes, 0, length)
      serializer.deserialize(ByteBuffer.wrap(bytes))
    }

    buffer.rewind()
    buffer.putInt(serializedObj.length).put(serializedObj)

    assertResult(obj, "Custom deserialized object didn't equal the original object") {
      buffer.rewind()
      serializer.deserialize(ByteBuffer.wrap(MAP_GENERIC.extract(buffer)))
    }
  }

  def testNativeColumnType[T <: AtomicType](
      columnType: NativeColumnType[T])
      (putter: (ByteBuffer, T#InternalType) => Unit,
      getter: (ByteBuffer) => T#InternalType): Unit = {

    testColumnType[T#InternalType](columnType, putter, getter)
  }

  def testColumnType[JvmType](
      columnType: ColumnType[JvmType],
      putter: (ByteBuffer, JvmType) => Unit,
      getter: (ByteBuffer) => JvmType): Unit = {

    val buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE)
    val seq = (0 until 4).map(_ => makeRandomValue(columnType))

    test(s"$columnType.extract") {
      buffer.rewind()
      seq.foreach(putter(buffer, _))

      buffer.rewind()
      seq.foreach { expected =>
        logInfo("buffer = " + buffer + ", expected = " + expected)
        val extracted = columnType.extract(buffer)
        assert(
          expected === extracted,
          "Extracted value didn't equal to the original one. " +
            hexDump(expected) + " != " + hexDump(extracted) +
            ", buffer = " + dumpBuffer(buffer.duplicate().rewind().asInstanceOf[ByteBuffer]))
      }
    }

    test(s"$columnType.append") {
      buffer.rewind()
      seq.foreach(columnType.append(_, buffer))

      buffer.rewind()
      seq.foreach { expected =>
        assert(
          expected === getter(buffer),
          "Extracted value didn't equal to the original one")
      }
    }
  }

  private def hexDump(value: Any): String = {
    value.toString.map(ch => Integer.toHexString(ch & 0xffff)).mkString(" ")
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
      assertResult(FIXED_DECIMAL(i, 0)) {
        ColumnType(DecimalType(i, 0))
      }
    }

    assertResult(GENERIC(DecimalType(19, 0))) {
      ColumnType(DecimalType(19, 0))
    }
  }
}

private[columnar] final case class CustomClass(a: Int, b: Long)

private[columnar] object CustomerSerializer extends Serializer[CustomClass] {
  override def write(kryo: Kryo, output: Output, t: CustomClass) {
    output.writeInt(t.a)
    output.writeLong(t.b)
  }
  override def read(kryo: Kryo, input: Input, aClass: Class[CustomClass]): CustomClass = {
    val a = input.readInt()
    val b = input.readLong()
    CustomClass(a, b)
  }
}

private[columnar] final class Registrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[CustomClass], CustomerSerializer)
  }
}
