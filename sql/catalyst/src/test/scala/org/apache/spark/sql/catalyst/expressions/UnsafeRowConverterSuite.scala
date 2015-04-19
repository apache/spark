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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.{StringType, DataType, LongType, IntegerType}
import org.apache.spark.unsafe.PlatformDependent
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.scalatest.{FunSuite, Matchers}


class UnsafeRowConverterSuite extends FunSuite with Matchers {

  test("basic conversion with only primitive types") {
    val fieldTypes: Array[DataType] = Array(LongType, LongType, IntegerType)
    val row = new SpecificMutableRow(fieldTypes)
    row.setLong(0, 0)
    row.setLong(1, 1)
    row.setInt(2, 2)
    val converter = new UnsafeRowConverter(fieldTypes)
    val sizeRequired: Int = converter.getSizeRequirement(row)
    sizeRequired should be (8 + (3 * 8))
    val buffer: Array[Long] = new Array[Long](sizeRequired / 8)
    val numBytesWritten = converter.writeRow(row, buffer, PlatformDependent.LONG_ARRAY_OFFSET)
    numBytesWritten should be (sizeRequired)
    val unsafeRow = new UnsafeRow()
    unsafeRow.set(buffer, PlatformDependent.LONG_ARRAY_OFFSET, fieldTypes.length, null)
    unsafeRow.getLong(0) should be (0)
    unsafeRow.getLong(1) should be (1)
    unsafeRow.getInt(2) should be (2)
  }

  test("basic conversion with primitive and string types") {
    val fieldTypes: Array[DataType] = Array(LongType, StringType, StringType)
    val row = new SpecificMutableRow(fieldTypes)
    row.setLong(0, 0)
    row.setString(1, "Hello")
    row.setString(2, "World")
    val converter = new UnsafeRowConverter(fieldTypes)
    val sizeRequired: Int = converter.getSizeRequirement(row)
    sizeRequired should be (8 + (8 * 3) +
      ByteArrayMethods.roundNumberOfBytesToNearestWord("Hello".getBytes.length + 8) +
      ByteArrayMethods.roundNumberOfBytesToNearestWord("World".getBytes.length + 8))
    val buffer: Array[Long] = new Array[Long](sizeRequired / 8)
    val numBytesWritten = converter.writeRow(row, buffer, PlatformDependent.LONG_ARRAY_OFFSET)
    numBytesWritten should be (sizeRequired)
    val unsafeRow = new UnsafeRow()
    unsafeRow.set(buffer, PlatformDependent.LONG_ARRAY_OFFSET, fieldTypes.length, null)
    unsafeRow.getLong(0) should be (0)
    unsafeRow.getString(1) should be ("Hello")
    unsafeRow.getString(2) should be ("World")
  }
}
