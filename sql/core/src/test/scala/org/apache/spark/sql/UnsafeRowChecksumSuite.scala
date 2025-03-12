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

import java.nio.ByteBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{UnsafeRow, UnsafeRowChecksum}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

class UnsafeRowChecksumSuite extends SparkFunSuite {
  private val schema = new StructType().add("value", IntegerType)
  private val toUnsafeRow = ExpressionEncoder(schema).createSerializer()

  private val schemaComplex = new StructType()
      .add("stringCol", StringType)
      .add("doubleCol", DoubleType)
      .add("longCol", LongType)
      .add("int32Col", IntegerType)
      .add("int16Col", ShortType)
      .add("int8Col", ByteType)
      .add("boolCol", BooleanType)
  private val toUnsafeRowComplex = ExpressionEncoder(schemaComplex).createSerializer()

  private def setUnsafeRowValue(
      stringCol: String,
      doubleCol: Double,
      longCol: Long,
      int32Col: Int,
      int16Col: Short,
      int8Col: Byte,
      boolCol: Boolean,
      unsafeRowOffheap: UnsafeRow): Unit = {
    unsafeRowOffheap.writeFieldTo(0, ByteBuffer.wrap(stringCol.getBytes))
    unsafeRowOffheap.setDouble(1, doubleCol)
    unsafeRowOffheap.setLong(2, longCol)
    unsafeRowOffheap.setInt(3, int32Col)
    unsafeRowOffheap.setShort(4, int16Col)
    unsafeRowOffheap.setByte(5, int8Col)
    unsafeRowOffheap.setBoolean(6, boolCol)
  }

  test("Non-UnsafeRow value should fail") {
    val rowBasedChecksum = new UnsafeRowChecksum()
    rowBasedChecksum.update(1, Long.box(20))
    // We fail to compute the checksum, and getValue returns 0.
    assert(rowBasedChecksum.getValue == 0)
  }

  test("Two identical rows should have a checksum of zero with XOR") {
    val rowBasedChecksum = new UnsafeRowChecksum()
    assert(rowBasedChecksum.getValue == 0)

    // Updates the checksum with one row.
    rowBasedChecksum.update(1, toUnsafeRow(Row(20)))
    assert(rowBasedChecksum.getValue == 8551541565481898028L)

    // Updates the checksum with the same row again, and the row-based checksum should become 0.
    rowBasedChecksum.update(1, toUnsafeRow(Row(20)))
    assert(rowBasedChecksum.getValue == 0)
  }

  test("The checksum is independent of row order - two rows") {
    val rowBasedChecksum1 = new UnsafeRowChecksum()
    val rowBasedChecksum2 = new UnsafeRowChecksum()
    assert(rowBasedChecksum1.getValue == 0)
    assert(rowBasedChecksum2.getValue == 0)

    rowBasedChecksum1.update(1, toUnsafeRow(Row(20)))
    rowBasedChecksum2.update(1, toUnsafeRow(Row(40)))
    assert(rowBasedChecksum1.getValue != rowBasedChecksum2.getValue)

    rowBasedChecksum1.update(2, toUnsafeRow(Row(40)))
    rowBasedChecksum2.update(2, toUnsafeRow(Row(20)))
    assert(rowBasedChecksum1.getValue == rowBasedChecksum2.getValue)

    assert(rowBasedChecksum1.getValue != 0)
    assert(rowBasedChecksum2.getValue != 0)
  }

  test("The checksum is independent of row order - multiple rows") {
    val rowBasedChecksum1 = new UnsafeRowChecksum()
    val rowBasedChecksum2 = new UnsafeRowChecksum()
    assert(rowBasedChecksum1.getValue == 0)
    assert(rowBasedChecksum2.getValue == 0)

    rowBasedChecksum1.update(1, toUnsafeRow(Row(20)))
    rowBasedChecksum2.update(1, toUnsafeRow(Row(100)))
    assert(rowBasedChecksum1.getValue != rowBasedChecksum2.getValue)

    rowBasedChecksum1.update(2, toUnsafeRow(Row(40)))
    rowBasedChecksum2.update(2, toUnsafeRow(Row(80)))
    assert(rowBasedChecksum1.getValue != rowBasedChecksum2.getValue)

    rowBasedChecksum1.update(3, toUnsafeRow(Row(60)))
    rowBasedChecksum2.update(3, toUnsafeRow(Row(60)))
    assert(rowBasedChecksum1.getValue != rowBasedChecksum2.getValue)

    rowBasedChecksum1.update(4, toUnsafeRow(Row(80)))
    rowBasedChecksum2.update(4, toUnsafeRow(Row(40)))
    assert(rowBasedChecksum1.getValue != rowBasedChecksum2.getValue)

    rowBasedChecksum1.update(5, toUnsafeRow(Row(100)))
    rowBasedChecksum2.update(5, toUnsafeRow(Row(20)))
    assert(rowBasedChecksum1.getValue == rowBasedChecksum2.getValue)

    assert(rowBasedChecksum1.getValue != 0)
    assert(rowBasedChecksum2.getValue != 0)
  }

  test("The checksum is the same for byte array and non-byte array object") {
    val buffer = Platform.allocateMemory(16)
    val unsafeRowOffheap = new UnsafeRow(1)
    unsafeRowOffheap.pointTo(null, buffer, 16)
    assert(!unsafeRowOffheap.getBaseObject.isInstanceOf[Array[Byte]])

    val rowBasedChecksum1 = new UnsafeRowChecksum()
    val rowBasedChecksum2 = new UnsafeRowChecksum()
    assert(rowBasedChecksum1.getValue == 0)
    assert(rowBasedChecksum2.getValue == 0)

    rowBasedChecksum1.update(1, toUnsafeRow(Row(20)))
    unsafeRowOffheap.setInt(0, 20)
    rowBasedChecksum2.update(1, unsafeRowOffheap)
    assert(rowBasedChecksum1.getValue == rowBasedChecksum2.getValue)

    rowBasedChecksum1.update(2, toUnsafeRow(Row(40)))
    unsafeRowOffheap.setInt(0, 40)
    rowBasedChecksum2.update(2, unsafeRowOffheap)
    assert(rowBasedChecksum1.getValue == rowBasedChecksum2.getValue)

    assert(rowBasedChecksum1.getValue != 0)
    assert(rowBasedChecksum2.getValue != 0)

    Platform.freeMemory(buffer)
  }

  test("The checksum is independent of row order - complex rows") {
    val rowBasedChecksum1 = new UnsafeRowChecksum()
    val rowBasedChecksum2 = new UnsafeRowChecksum()
    assert(rowBasedChecksum1.getValue == 0)
    assert(rowBasedChecksum2.getValue == 0)

    rowBasedChecksum1.update(1, toUnsafeRowComplex(Row(
      "Some string", 0.99, 10000L, 1000, 100.toShort, 10.toByte, true)))
    rowBasedChecksum2.update(1, toUnsafeRowComplex(Row(
      "Some other string", 10.88, 20000L, 2000, 200.toShort, 20.toByte, false)))
    assert(rowBasedChecksum1.getValue != rowBasedChecksum2.getValue)

    rowBasedChecksum1.update(2, toUnsafeRowComplex(Row(
      "Some other string", 10.88, 20000L, 2000, 200.toShort, 20.toByte, false)))
    rowBasedChecksum2.update(2, toUnsafeRowComplex(Row(
      "Some string", 0.99, 10000L, 1000, 100.toShort, 10.toByte, true)))
    assert(rowBasedChecksum1.getValue == rowBasedChecksum2.getValue)

    assert(rowBasedChecksum1.getValue != 0)
    assert(rowBasedChecksum2.getValue != 0)
  }

  test("The checksum is independent of row order - complex offheap rows") {
    val buffer1 = Platform.allocateMemory(128)
    val unsafeRowOffheap1 = new UnsafeRow(7)
    unsafeRowOffheap1.pointTo(null, buffer1, 128)
    setUnsafeRowValue(
      "Some string", 0.99, 10000L, 1000, 100.toShort, 10.toByte, true, unsafeRowOffheap1)
    assert(!unsafeRowOffheap1.getBaseObject.isInstanceOf[Array[Byte]])

    val buffer2 = Platform.allocateMemory(128)
    val unsafeRowOffheap2 = new UnsafeRow(7)
    unsafeRowOffheap2.pointTo(null, buffer2, 128)
    setUnsafeRowValue(
      "Some other string", 10.88, 20000L, 2000, 200.toShort, 20.toByte, false, unsafeRowOffheap2)
    assert(!unsafeRowOffheap2.getBaseObject.isInstanceOf[Array[Byte]])

    val rowBasedChecksum1 = new UnsafeRowChecksum()
    val rowBasedChecksum2 = new UnsafeRowChecksum()
    assert(rowBasedChecksum1.getValue == 0)
    assert(rowBasedChecksum2.getValue == 0)

    rowBasedChecksum1.update(1, unsafeRowOffheap1)
    rowBasedChecksum2.update(1, unsafeRowOffheap2)
    assert(rowBasedChecksum1.getValue != rowBasedChecksum2.getValue)

    rowBasedChecksum1.update(2, unsafeRowOffheap2)
    rowBasedChecksum2.update(2, unsafeRowOffheap1)
    assert(rowBasedChecksum1.getValue == rowBasedChecksum2.getValue)

    assert(rowBasedChecksum1.getValue != 0)
    assert(rowBasedChecksum2.getValue != 0)
  }
}
