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

  test("Two identical rows should not have a checksum of zero") {
    val rowBasedChecksum = new UnsafeRowChecksum()
    assert(rowBasedChecksum.getValue == 0)

    // Updates the checksum with one row.
    rowBasedChecksum.update(1, toUnsafeRow(Row(20)))
    assert(rowBasedChecksum.getValue == -9094624449814316735L)

    // Updates the checksum with the same row again, since we mix the final xor and sum
    // of the row-based checksum, the result would not be 0.
    rowBasedChecksum.update(1, toUnsafeRow(Row(20)))
    assert(rowBasedChecksum.getValue == -1240577858172431653L)
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
}
