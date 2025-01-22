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

package org.apache.spark.sql

import java.io.ByteArrayOutputStream

import org.apache.spark.{SparkConf, SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryAllocator
import org.apache.spark.unsafe.types.UTF8String

class UnsafeRowSuite extends SparkFunSuite {

  test("UnsafeRow Java serialization") {
    // serializing an UnsafeRow pointing to a large buffer should only serialize the relevant data
    val data = new Array[Byte](1024)
    val row = new UnsafeRow(1)
    row.pointTo(data, 16)
    row.setLong(0, 19285)

    val ser = new JavaSerializer(new SparkConf).newInstance()
    val row1 = ser.deserialize[UnsafeRow](ser.serialize(row))
    assert(row1.getLong(0) == 19285)
    assert(row1.getBaseObject().asInstanceOf[Array[Byte]].length == 16)
  }

  test("UnsafeRow Kryo serialization") {
    // serializing an UnsafeRow pointing to a large buffer should only serialize the relevant data
    val data = new Array[Byte](1024)
    val row = new UnsafeRow(1)
    row.pointTo(data, 16)
    row.setLong(0, 19285)

    val ser = new KryoSerializer(new SparkConf).newInstance()
    val row1 = ser.deserialize[UnsafeRow](ser.serialize(row))
    assert(row1.getLong(0) == 19285)
    assert(row1.getBaseObject().asInstanceOf[Array[Byte]].length == 16)
  }

  test("bitset width calculation") {
    assert(UnsafeRow.calculateBitSetWidthInBytes(0) === 0)
    assert(UnsafeRow.calculateBitSetWidthInBytes(1) === 8)
    assert(UnsafeRow.calculateBitSetWidthInBytes(32) === 8)
    assert(UnsafeRow.calculateBitSetWidthInBytes(64) === 8)
    assert(UnsafeRow.calculateBitSetWidthInBytes(65) === 16)
    assert(UnsafeRow.calculateBitSetWidthInBytes(128) === 16)
  }

  test("writeToStream") {
    val row = InternalRow.apply(UTF8String.fromString("hello"), UTF8String.fromString("world"), 123)
    val arrayBackedUnsafeRow: UnsafeRow =
      UnsafeProjection.create(Array[DataType](StringType, StringType, IntegerType)).apply(row)
    assert(arrayBackedUnsafeRow.getBaseObject.isInstanceOf[Array[Byte]])
    val (bytesFromArrayBackedRow, field0StringFromArrayBackedRow): (Array[Byte], String) = {
      val baos = new ByteArrayOutputStream()
      arrayBackedUnsafeRow.writeToStream(baos, null)
      (baos.toByteArray, arrayBackedUnsafeRow.getString(0))
    }
    val (bytesFromOffheapRow, field0StringFromOffheapRow): (Array[Byte], String) = {
      val offheapRowPage = MemoryAllocator.UNSAFE.allocate(arrayBackedUnsafeRow.getSizeInBytes)
      try {
        Platform.copyMemory(
          arrayBackedUnsafeRow.getBaseObject,
          arrayBackedUnsafeRow.getBaseOffset,
          offheapRowPage.getBaseObject,
          offheapRowPage.getBaseOffset,
          arrayBackedUnsafeRow.getSizeInBytes
        )
        val offheapUnsafeRow: UnsafeRow = new UnsafeRow(3)
        offheapUnsafeRow.pointTo(
          offheapRowPage.getBaseObject,
          offheapRowPage.getBaseOffset,
          arrayBackedUnsafeRow.getSizeInBytes
        )
        assert(offheapUnsafeRow.getBaseObject === null)
        val baos = new ByteArrayOutputStream()
        val writeBuffer = new Array[Byte](1024)
        offheapUnsafeRow.writeToStream(baos, writeBuffer)
        (baos.toByteArray, offheapUnsafeRow.getString(0))
      } finally {
        MemoryAllocator.UNSAFE.free(offheapRowPage)
      }
    }
    val (bytesFromArrayBackedRowWithOffset, field0StringFromArrayBackedRowWithOffset) = {
      val baos = new ByteArrayOutputStream()
      val numBytes = arrayBackedUnsafeRow.getSizeInBytes
      val bytesWithOffset = new Array[Byte](numBytes + 100)
      System.arraycopy(arrayBackedUnsafeRow.getBaseObject.asInstanceOf[Array[Byte]], 0,
        bytesWithOffset, 100, numBytes)
      val arrayBackedRow = new UnsafeRow(arrayBackedUnsafeRow.numFields())
      arrayBackedRow.pointTo(bytesWithOffset, Platform.BYTE_ARRAY_OFFSET + 100, numBytes)
      arrayBackedRow.writeToStream(baos, null)
      (baos.toByteArray, arrayBackedRow.getString(0))
    }

    assert(bytesFromArrayBackedRow === bytesFromOffheapRow)
    assert(field0StringFromArrayBackedRow === field0StringFromOffheapRow)
    assert(bytesFromArrayBackedRow === bytesFromArrayBackedRowWithOffset)
    assert(field0StringFromArrayBackedRow === field0StringFromArrayBackedRowWithOffset)
  }

  test("calling getDouble() and getFloat() on null columns") {
    val row = InternalRow.apply(null, null)
    val unsafeRow = UnsafeProjection.create(Array[DataType](FloatType, DoubleType)).apply(row)
    assert(unsafeRow.getFloat(0) === row.getFloat(0))
    assert(unsafeRow.getDouble(1) === row.getDouble(1))
  }

  test("calling get(ordinal, datatype) on null columns") {
    val row = InternalRow.apply(null)
    val unsafeRow = UnsafeProjection.create(Array[DataType](NullType)).apply(row)
    for (dataType <- DataTypeTestUtils.atomicTypes) {
      assert(unsafeRow.get(0, dataType) === null)
    }
  }

  test("createFromByteArray and copyFrom") {
    val row = InternalRow(1, UTF8String.fromString("abc"))
    val converter = UnsafeProjection.create(Array[DataType](IntegerType, StringType))
    val unsafeRow = converter.apply(row)

    val emptyRow = UnsafeRow.createFromByteArray(64, 2)
    val buffer = emptyRow.getBaseObject

    emptyRow.copyFrom(unsafeRow)
    assert(emptyRow.getSizeInBytes() === unsafeRow.getSizeInBytes)
    assert(emptyRow.getInt(0) === unsafeRow.getInt(0))
    assert(emptyRow.getUTF8String(1) === unsafeRow.getUTF8String(1))
    // make sure we reuse the buffer.
    assert(emptyRow.getBaseObject === buffer)

    // make sure we really copied the input row.
    unsafeRow.setInt(0, 2)
    assert(emptyRow.getInt(0) === 1)

    val longString = UTF8String.fromString((1 to 100).map(_ => "abc").reduce(_ + _))
    val row2 = InternalRow(3, longString)
    val unsafeRow2 = converter.apply(row2)

    // make sure we can resize.
    emptyRow.copyFrom(unsafeRow2)
    assert(emptyRow.getSizeInBytes() === unsafeRow2.getSizeInBytes)
    assert(emptyRow.getInt(0) === 3)
    assert(emptyRow.getUTF8String(1) === longString)
    // make sure we really resized.
    assert(emptyRow.getBaseObject != buffer)

    // make sure we can still handle small rows after resize.
    emptyRow.copyFrom(unsafeRow)
    assert(emptyRow.getSizeInBytes() === unsafeRow.getSizeInBytes)
    assert(emptyRow.getInt(0) === unsafeRow.getInt(0))
    assert(emptyRow.getUTF8String(1) === unsafeRow.getUTF8String(1))
  }

  test("calling hashCode on unsafe array returned by getArray(ordinal)") {
    val row = InternalRow.apply(new GenericArrayData(Array(1L)))
    val unsafeRow = UnsafeProjection.create(Array[DataType](ArrayType(LongType))).apply(row)
    // Makes sure hashCode on unsafe array won't crash
    unsafeRow.getArray(0).hashCode()
  }

  test("SPARK-32018: setDecimal with overflowed value") {
    val d1 = new Decimal().set(BigDecimal("10000000000000000000")).toPrecision(38, 18)
    val row = InternalRow.apply(d1)
    val unsafeRow = UnsafeProjection.create(Array[DataType](DecimalType(38, 18))).apply(row)
    assert(unsafeRow.getDecimal(0, 38, 18) === d1)
    val d2 = (d1 * Decimal(10)).toPrecision(39, 18)
    unsafeRow.setDecimal(0, d2, 38)
    assert(unsafeRow.getDecimal(0, 38, 18) === null)
  }

  test("SPARK-48713: throw SparkIllegalArgumentException for illegal UnsafeRow.pointTo") {
    val emptyRow = UnsafeRow.createFromByteArray(64, 2)
    val byteArray = new Array[Byte](64)

    // Out of bounds
    var errorMsg = intercept[SparkIllegalArgumentException] {
      emptyRow.pointTo(byteArray, Platform.BYTE_ARRAY_OFFSET + 50, 32)
    }.getMessage
    assert(
      errorMsg.contains(
        "Invalid byte array backed UnsafeRow: byte array length=64, offset=50, byte size=32"
      )
    )

    // Negative size
    errorMsg = intercept[SparkIllegalArgumentException] {
      emptyRow.pointTo(byteArray, Platform.BYTE_ARRAY_OFFSET + 50, -32)
    }.getMessage
    assert(
      errorMsg.contains(
        "Invalid byte array backed UnsafeRow: byte array length=64, offset=50, byte size=-32"
      )
    )

    // Negative offset
    errorMsg = intercept[SparkIllegalArgumentException] {
      emptyRow.pointTo(byteArray, -5, 32)
    }.getMessage
    assert(
      errorMsg.contains(
        s"Invalid byte array backed UnsafeRow: byte array length=64, " +
          s"offset=${-5 - Platform.BYTE_ARRAY_OFFSET}, byte size=32"
      )
    )
  }
}
