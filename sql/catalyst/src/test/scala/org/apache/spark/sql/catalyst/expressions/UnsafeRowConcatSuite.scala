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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.PlatformDependent
import org.apache.spark.unsafe.types.UTF8String

class UnsafeRowConcatSuite extends SparkFunSuite {
  test("null bit set") {
    def fillRow(row: UnsafeRow, nulls: Set[Int]): Unit = {
      (0 to row.numFields() -1).foreach { i =>
        if (nulls.contains(i)) row.setNullAt(i) else row.setDouble(i, 0.0)
      }
    }
    def checkResultNulls(left: UnsafeRow, right: UnsafeRow,
        leftNulls: Set[Int], rightNulls: Set[Int], rowConcat: UnsafeRowConcat): Unit = {
      fillRow(left, leftNulls)
      fillRow(right, rightNulls)
      val result = rowConcat.concat(left, right)
      val resultNulls = leftNulls ++ rightNulls.map(_ + left.numFields())
      val resultNotNulls = (0 to left.numFields() + right.numFields() - 1).toSet -- resultNulls

      resultNulls.foreach { i =>
        assert(result.isNullAt(i), s"result.isNullAt($i) should be true")
      }
      resultNotNulls.foreach { i =>
        assert(!result.isNullAt(i), s"result.isNullAt($i) should be false")
      }
      assert(result.numFields() === left.numFields() + right.numFields())
    }

    val schema1 = StructType((0 to 99).map(i => StructField(s"s1_$i", DoubleType, true)))
    val schema2 = StructType((0 to 99).map(i => StructField(s"s2_$i", DoubleType, true)))
    val schema3 = StructType((0 to 63).map(i => StructField(s"s3_$i", DoubleType, true)))
    val schema4 = StructType((0 to 63).map(i => StructField(s"s4_$i", DoubleType, true)))
    val schema5 = StructType((0 to 69).map(i => StructField(s"s5_$i", DoubleType, true)))
    val schema6 = StructType((0 to 27).map(i => StructField(s"s6_$i", DoubleType, true)))

    val r1 = new UnsafeRow()
    val r2 = new UnsafeRow()

    // join two big rows
    val rc = new InterpretedUnsafeRowConcat(schema1, schema2)
    r1.pointTo(new Array[Byte](2048), PlatformDependent.BYTE_ARRAY_OFFSET, 100, 816)
    r2.pointTo(new Array[Byte](2048), PlatformDependent.BYTE_ARRAY_OFFSET, 100, 816)
    val r1nulls = Set(0, 5, 10, 15, 20, 25, 30, 35, 40, 60, 63, 80, 99)
    val r2nulls = Set(2, 4, 10, 40, 60, 67, 80, 99)
    checkResultNulls(r1, r2, r1nulls, r2nulls, rc)

    // both row null bit set end at word boundary
    val rc2 = new InterpretedUnsafeRowConcat(schema3, schema4)
    r1.pointTo(new Array[Byte](2048), PlatformDependent.BYTE_ARRAY_OFFSET, 64, 520)
    r2.pointTo(new Array[Byte](2048), PlatformDependent.BYTE_ARRAY_OFFSET, 64, 520)
    val r1nulls2 = Set(0, 5, 30, 45, 63)
    val r2nulls2 = Set(17, 32, 50)
    checkResultNulls(r1, r2, r1nulls2, r2nulls2, rc2)

    // left row null bit set end at word boundary
    val rc3 = new InterpretedUnsafeRowConcat(schema3, schema5)
    r1.pointTo(new Array[Byte](2048), PlatformDependent.BYTE_ARRAY_OFFSET, 64, 520)
    r2.pointTo(new Array[Byte](2048), PlatformDependent.BYTE_ARRAY_OFFSET, 70, 576)
    val r1nulls3 = Set(0, 5, 30, 45, 63)
    val r2nulls3 = Set(17, 32, 50, 67, 69)
    checkResultNulls(r1, r2, r1nulls3, r2nulls3, rc3)

    // right row null bit set end at word boundary
    val rc4 = new InterpretedUnsafeRowConcat(schema5, schema3)
    r1.pointTo(new Array[Byte](2048), PlatformDependent.BYTE_ARRAY_OFFSET, 70, 576)
    r2.pointTo(new Array[Byte](2048), PlatformDependent.BYTE_ARRAY_OFFSET, 64, 520)
    val r1nulls4 = Set(17, 32, 50, 67, 69)
    val r2nulls4 = Set(0, 5, 30, 45, 63)
    checkResultNulls(r1, r2, r1nulls4, r2nulls4, rc4)

    // right row null bit set end at word boundary
    val rc5 = new InterpretedUnsafeRowConcat(schema1, schema6)
    r1.pointTo(new Array[Byte](2048), PlatformDependent.BYTE_ARRAY_OFFSET, 100, 816)
    r2.pointTo(new Array[Byte](2048), PlatformDependent.BYTE_ARRAY_OFFSET, 28, 232)
    val r1nulls5 = Set(0, 5, 10, 15, 20, 25, 30, 35, 40, 60, 63, 80, 99)
    val r2nulls5 = Set(0, 5, 18, 27)
    checkResultNulls(r1, r2, r1nulls5, r2nulls5, rc5)
  }

  test("var length fields") {
    val r1 = InternalRow.apply(
      1, UTF8String.fromString("hello"), UTF8String.fromString("across word boundary"), 123L)
    val r2 = InternalRow.apply(1, 5.0f, UTF8String.fromString("world"))
    val correctResult = InternalRow.apply(
      1, UTF8String.fromString("hello"), UTF8String.fromString("across word boundary"), 123L,
      1, 5.0f, UTF8String.fromString("world"))

    val schema1 = StructType(Seq(IntegerType, StringType, StringType, LongType).zipWithIndex.map {
      case (dt, i) => StructField(s"t1_$i", dt, true)
    })
    val schema2 = StructType(Seq(IntegerType, FloatType, StringType).zipWithIndex.map {
      case (dt, i) => StructField(s"t2_$i", dt, true)
    })

    val unsafe1 = UnsafeProjection.create(schema1).apply(r1)
    val unsafe2 = UnsafeProjection.create(schema2).apply(r2)

    val rc = new InterpretedUnsafeRowConcat(schema1, schema2)
    val result = rc.concat(unsafe1, unsafe2)
    val correctUnsafe = UnsafeProjection.create(schema1.merge(schema2)).apply(correctResult)

    assert(result.numFields() === 7)
    assert(result === correctUnsafe)
  }

}
