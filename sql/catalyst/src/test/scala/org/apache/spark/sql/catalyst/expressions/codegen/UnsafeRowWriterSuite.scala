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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types._

class UnsafeRowWriterSuite extends SparkFunSuite {

  test("writeNullable matches the setNullAt/write split for null and non-null inputs") {
    // Primitive: a null write sets the null bit, a non-null write stores the value.
    val intWriter = new UnsafeRowWriter(2)
    intWriter.resetRowWriter()
    intWriter.writeNullable(0, 42, false)
    intWriter.writeNullable(1, -1, true)
    val intRow = intWriter.getRow
    assert(!intRow.isNullAt(0) && intRow.getInt(0) == 42)
    assert(intRow.isNullAt(1))

    // Reference type: the staged value is ignored when isNull is set.
    val strWriter = new UnsafeRowWriter(1)
    strWriter.resetRowWriter()
    strWriter.writeNullable(0, UTF8String.fromString("ignored"), true)
    assert(strWriter.getRow.isNullAt(0))

    // Wide decimal (precision > 18): a null write must still reserve the fixed-width slot, exactly
    // like the write(ordinal, null, precision, scale) call the codegen setNull branch used.
    val decWriter = new UnsafeRowWriter(2)
    decWriter.resetRowWriter()
    decWriter.writeNullable(0, null, true, 38, 18)
    val dec = Decimal(123456789.123456789)
    assert(dec.changePrecision(38, 18))
    decWriter.writeNullable(1, dec, false, 38, 18)
    val decRow = decWriter.getRow
    assert(decRow.isNullAt(0))
    assert(decRow.getDecimal(1, 38, 18) == dec)

    // Compact decimal (precision <= 18): the null bit must be set through
    // write(ordinal, null, precision, scale)'s internal null check rather than an explicit
    // setNullAt branch, so it lands on the same setNullAt the codegen previously emitted.
    val compactDecWriter = new UnsafeRowWriter(2)
    compactDecWriter.resetRowWriter()
    compactDecWriter.writeNullable(0, null, true, 18, 2)
    val compactDec = Decimal(123456789.12)
    assert(compactDec.changePrecision(18, 2))
    compactDecWriter.writeNullable(1, compactDec, false, 18, 2)
    val compactDecRow = compactDecWriter.getRow
    assert(compactDecRow.isNullAt(0))
    assert(compactDecRow.getDecimal(1, 18, 2) == compactDec)

    // CalendarInterval: a null write reserves the variable-length slot via write(ordinal, null).
    val intervalWriter = new UnsafeRowWriter(2)
    intervalWriter.resetRowWriter()
    intervalWriter.writeNullable(0, null.asInstanceOf[CalendarInterval], true)
    val interval = new CalendarInterval(1, 2, 3L)
    intervalWriter.writeNullable(1, interval, false)
    val intervalRow = intervalWriter.getRow
    assert(intervalRow.isNullAt(0))
    assert(intervalRow.getInterval(1) == interval)
  }

  def checkDecimalSizeInBytes(decimal: Decimal, numBytes: Int): Unit = {
    assert(decimal.toJavaBigDecimal.unscaledValue().toByteArray.length == numBytes)
  }

  test("SPARK-25538: zero-out all bits for decimals") {
    val decimal1 = Decimal(0.431)
    decimal1.changePrecision(38, 18)
    checkDecimalSizeInBytes(decimal1, 8)

    val decimal2 = Decimal(123456789.1232456789)
    decimal2.changePrecision(38, 18)
    checkDecimalSizeInBytes(decimal2, 11)
    // On an UnsafeRowWriter we write decimal2 first and then decimal1
    val unsafeRowWriter1 = new UnsafeRowWriter(1)
    unsafeRowWriter1.resetRowWriter()
    unsafeRowWriter1.write(0, decimal2, decimal2.precision, decimal2.scale)
    unsafeRowWriter1.reset()
    unsafeRowWriter1.write(0, decimal1, decimal1.precision, decimal1.scale)
    val res1 = unsafeRowWriter1.getRow
    // On a second UnsafeRowWriter we write directly decimal1
    val unsafeRowWriter2 = new UnsafeRowWriter(1)
    unsafeRowWriter2.resetRowWriter()
    unsafeRowWriter2.write(0, decimal1, decimal1.precision, decimal1.scale)
    val res2 = unsafeRowWriter2.getRow
    // The two rows should be the equal
    assert(res1 == res2)
  }

  test("write and get geography through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.setNullAt(0)
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getBinaryView(0) === null)
    val geography = BinaryView.fromBytes(Array[Byte](1, 2, 3))
    rowWriter.write(1, geography)
    assert(rowWriter.getRow.getBinaryView(1).getBytes sameElements geography.getBytes)
  }

  test("write and get geometry through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.setNullAt(0)
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getBinaryView(0) === null)
    val geometry = BinaryView.fromBytes(Array[Byte](1, 2, 3))
    rowWriter.write(1, geometry)
    assert(rowWriter.getRow.getBinaryView(1).getBytes sameElements geometry.getBytes)
  }

  test("write and get calendar intervals through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.write(0, null.asInstanceOf[CalendarInterval])
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getInterval(0) === null)
    val interval = new CalendarInterval(0, 1, 0)
    rowWriter.write(1, interval)
    assert(rowWriter.getRow.getInterval(1) === interval)
  }

  test("write and get variant through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.setNullAt(0)
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getVariant(0) === null)
    val variant = new VariantVal(Array[Byte](1, 2, 3), Array[Byte](-1, -2, -3, -4))
    rowWriter.write(1, variant)
    assert(rowWriter.getRow.getVariant(1).debugString() == variant.debugString())
  }
}
