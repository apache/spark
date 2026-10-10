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

import org.apache.spark.{SparkException, SparkFunSuite}
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

  // --- Invalid-row guard ---
  // Production crashed with a SIGSEGV in XXH64.hashBytesByWords -> Platform.getLong (si_addr=0x0)
  // when the checksum read an UnsafeRow whose backing pointer was invalid. The guard flags such a
  // row before it is dereferenced and either recovers (default) or fails the task.

  // The production crash shape: an off-heap row (null baseObject) whose baseOffset points into the
  // first memory page. Reading it would fault near si_addr 0x0.
  private def nullLowAddressRow(size: Int = 16): UnsafeRow = {
    val row = new UnsafeRow(1)
    row.pointTo(null, 0L, size)
    row
  }

  test("validate accepts a well-formed row and flags invalid backing memory") {
    assert(UnsafeRowChecksum.validate(toUnsafeRow(Row(20)).asInstanceOf[UnsafeRow]).isEmpty)

    // Empty row: nothing is dereferenced.
    val empty = new UnsafeRow(1)
    empty.pointTo(null, 0L, 0)
    assert(UnsafeRowChecksum.validate(empty).isEmpty)

    // A long[]-backed on-heap row (a common UnsafeRow buffer) must be accepted, not mistaken for
    // an invalid base type.
    val longBacked = new UnsafeRow(1)
    longBacked.pointTo(new Array[Long](2), Platform.LONG_ARRAY_OFFSET, 16)
    assert(UnsafeRowChecksum.validate(longBacked).isEmpty)

    // Null baseObject pointing into the first page (the si_addr=0x0 crash).
    assert(UnsafeRowChecksum.validate(nullLowAddressRow()).exists(_.contains("first memory page")))

    // Negative size (corrupt size field). A large-but-positive size is deliberately NOT flagged.
    val negSize = new UnsafeRow(1)
    negSize.pointTo(null, 0x100000L, -8)
    assert(UnsafeRowChecksum.validate(negSize).exists(_.contains("negative sizeInBytes")))
  }

  test("recover mode disables the checksum on an invalid row instead of crashing") {
    val rowBasedChecksum = new UnsafeRowChecksum(failOnInvalidRow = false)
    // Must neither throw nor dereference the bad pointer; getValue then returns the default 0.
    rowBasedChecksum.update(0, nullLowAddressRow())
    assert(rowBasedChecksum.getValue == 0L)
    // A subsequent valid row does not revive the checksum once it is in the error state.
    rowBasedChecksum.update(0, toUnsafeRow(Row(20)))
    assert(rowBasedChecksum.getValue == 0L)
  }

  test("fail mode (the default) raises a descriptive error on an invalid row") {
    val rowBasedChecksum = new UnsafeRowChecksum(failOnInvalidRow = true)
    val e = intercept[SparkException] {
      rowBasedChecksum.update(0, nullLowAddressRow())
    }
    assert(e.getMessage.contains("Invalid row in shuffle row-based checksum"))
    // The no-arg constructor defaults to fail mode.
    intercept[SparkException] {
      new UnsafeRowChecksum().update(0, nullLowAddressRow())
    }
  }

  test("createUnsafeRowChecksums threads the fail-on-invalid-row flag") {
    val recover = UnsafeRowChecksum.createUnsafeRowChecksums(1, failOnInvalidRow = false)
    recover(0).update(0, nullLowAddressRow())
    assert(recover(0).getValue == 0L)

    val fail = UnsafeRowChecksum.createUnsafeRowChecksums(1, failOnInvalidRow = true)
    intercept[SparkException] {
      fail(0).update(0, nullLowAddressRow())
    }
  }
}
