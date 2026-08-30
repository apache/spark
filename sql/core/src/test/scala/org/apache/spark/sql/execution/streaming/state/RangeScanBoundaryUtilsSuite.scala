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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class RangeScanBoundaryUtilsSuite extends SparkFunSuite {

  test("defaultInternalRow / defaultUnsafeRow accept schemas with common types") {
    val schema = new StructType()
      .add("ts", LongType)
      .add("s", StringType)
      .add("i", IntegerType)
      .add("nested", new StructType().add("b", BinaryType).add("d", DoubleType))
      .add("arr", ArrayType(IntegerType))
      .add("m", MapType(StringType, IntegerType))

    val row = RangeScanBoundaryUtils.defaultInternalRow(schema)
    assert(row.numFields === schema.length)

    val unsafe = RangeScanBoundaryUtils.defaultUnsafeRow(schema)
    assert(unsafe.numFields === schema.length)
  }

  test("CharType field uses n zero-bytes, which is byte-wise <= any legitimate value") {
    val n = 5
    val schema = new StructType().add("ts", LongType).add("c", CharType(n))
    val boundary = RangeScanBoundaryUtils.defaultUnsafeRow(schema)
    val boundaryStr = boundary.getUTF8String(1)

    // UTF-8 encoding of n U+0000 code points is exactly n bytes of 0x00.
    assert(boundaryStr.numBytes() === n)
    (0 until n).foreach { i =>
      assert(boundaryStr.getByte(i) === 0.toByte, s"byte $i is not 0x00")
    }

    // Sanity: a legitimate CharType(5) value written through UnsafeProjection
    // (e.g. "abcde") should produce a row whose raw bytes are >= the boundary row's.
    def encode(value: UTF8String): Array[Byte] = {
      val input = new GenericInternalRow(2)
      input.setLong(0, 0L)
      input.update(1, value)
      UnsafeProjection.create(schema).apply(input).getBytes
    }

    val boundaryBytes = boundary.getBytes
    val realBytes = encode(UTF8String.fromString("abcde"))
    assert(compareBytes(boundaryBytes, realBytes) <= 0,
      "boundary bytes should be <= 'abcde' bytes")

    // Also <= a value with legitimate low code points (e.g. all U+0001 padded).
    val lowValue = UTF8String.fromBytes(Array.fill[Byte](n)(1.toByte))
    assert(compareBytes(boundaryBytes, encode(lowValue)) <= 0,
      "boundary bytes should be <= all-0x01 bytes")
  }

  test("CharType nested inside a struct is still handled") {
    val inner = new StructType().add("c", CharType(3))
    val schema = new StructType().add("ts", LongType).add("nested", inner)
    // Should not throw, and the nested struct's char field should be 3 zero-bytes.
    val row = RangeScanBoundaryUtils.defaultInternalRow(schema)
    val nested = row.getStruct(1, inner.length)
    val str = nested.getUTF8String(0)
    assert(str.numBytes() === 3)
    (0 until 3).foreach(i => assert(str.getByte(i) === 0.toByte))
  }

  test("VarcharType default (empty string) is still used and is byte-wise smallest") {
    val schema = new StructType().add("ts", LongType).add("v", VarcharType(10))
    // Should not throw; VarcharType's Literal.default is empty string (no padding).
    val row = RangeScanBoundaryUtils.defaultInternalRow(schema)
    val s = row.getUTF8String(1)
    assert(s.numBytes() === 0)
  }

  test("defaultInternalRow rejects schemas containing VariantType at top level") {
    val schema = new StructType().add("ts", LongType).add("v", VariantType)
    val e = intercept[AssertionError] {
      RangeScanBoundaryUtils.defaultInternalRow(schema)
    }
    assert(e.getMessage.contains("VariantType"))
  }

  test("defaultInternalRow rejects schemas containing VariantType nested in array") {
    val schema = new StructType()
      .add("ts", LongType)
      .add("arr", ArrayType(VariantType))
    val e = intercept[AssertionError] {
      RangeScanBoundaryUtils.defaultInternalRow(schema)
    }
    assert(e.getMessage.contains("VariantType"))
  }

  test("defaultInternalRow rejects schemas containing VariantType nested in struct") {
    val schema = new StructType()
      .add("ts", LongType)
      .add("nested", new StructType().add("v", VariantType))
    val e = intercept[AssertionError] {
      RangeScanBoundaryUtils.defaultInternalRow(schema)
    }
    assert(e.getMessage.contains("VariantType"))
  }

  test("defaultUnsafeRow also triggers the schema check") {
    val schema = new StructType().add("ts", LongType).add("v", VariantType)
    val e = intercept[AssertionError] {
      RangeScanBoundaryUtils.defaultUnsafeRow(schema)
    }
    assert(e.getMessage.contains("VariantType"))
  }

  // Byte-wise unsigned lexicographic comparison, matching the RocksDB key order.
  private def compareBytes(a: Array[Byte], b: Array[Byte]): Int = {
    val len = math.min(a.length, b.length)
    var i = 0
    while (i < len) {
      val ai = a(i) & 0xFF
      val bi = b(i) & 0xFF
      if (ai != bi) return ai - bi
      i += 1
    }
    a.length - b.length
  }
}
