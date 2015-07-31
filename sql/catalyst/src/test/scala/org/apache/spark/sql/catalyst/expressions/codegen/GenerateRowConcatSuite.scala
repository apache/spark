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

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeRow, UnsafeProjection}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


class GenerateRowConcatSuite extends SparkFunSuite {

  private def createUnsafeRow(numFields: Int): UnsafeRow = {
    val row = new UnsafeRow
    val sizeInBytes = numFields * 8 + ((numFields + 63) / 64) * 8
    val buf = new Array[Byte](sizeInBytes)
    row.pointTo(buf, numFields, sizeInBytes)
    row
  }

  private def testBitsets(numFields1: Int, numFields2: Int): Unit = {
    val schema1 = StructType(Seq.tabulate(numFields1) { i => StructField(s"a_$i", IntegerType) })
    val schema2 = StructType(Seq.tabulate(numFields2) { i => StructField(s"b_$i", IntegerType) })

    val row1 = createUnsafeRow(numFields1)
    val row2 = createUnsafeRow(numFields2)

    if (numFields1 > 0) {
      for (i <- 0 until Random.nextInt(numFields1)) {
        row1.setNullAt(Random.nextInt(numFields1))
      }
    }
    if (numFields2 > 0) {
      for (i <- 0 until Random.nextInt(numFields2)) {
        row2.setNullAt(Random.nextInt(numFields2))
      }
    }

    val concater = GenerateRowConcat.create(schema1, schema2)
    val output = concater.concat(row1, row2)

    def dumpDebug(): String = {
      val set1 = Seq.tabulate(numFields1) { i => if (row1.isNullAt(i)) "1" else "0" }
      val set2 = Seq.tabulate(numFields2) { i => if (row2.isNullAt(i)) "1" else "0" }
      val out = Seq.tabulate(numFields1 + numFields2) { i => if (output.isNullAt(i)) "1" else "0" }

      s"""
         |input1: ${set1.mkString}
          |input2: ${set2.mkString}
          |output: ${out.mkString}
       """.stripMargin
    }

    for (i <- 0 until (numFields1 + numFields2)) {
      if (i < numFields1) {
        assert(output.isNullAt(i) === row1.isNullAt(i), dumpDebug())
      } else {
        assert(output.isNullAt(i) === row2.isNullAt(i - numFields1), dumpDebug())
      }
    }
  }

  test("boundary size 0, 0") {
    testBitsets(0, 0)
  }

  test("boundary size 0, 64") {
    testBitsets(0, 64)
  }

  test("boundary size 64, 0") {
    testBitsets(64, 0)
  }

  test("boundary size 64, 64") {
    testBitsets(64, 64)
  }

  test("boundary size 0, 128") {
    testBitsets(0, 128)
  }

  test("boundary size 128, 0") {
    testBitsets(128, 0)
  }

  test("boundary size 128, 128") {
    testBitsets(128, 128)
  }

  test("single word bitsets") {
    testBitsets(10, 5)
  }

  test("first bitset larger than a word") {
    testBitsets(67, 5)
  }

  test("second bitset larger than a word") {
    testBitsets(6, 67)
  }

  test("no reduction in bitset size") {
    testBitsets(33, 34)
  }

  test("two words") {
    testBitsets(120, 95)
  }

  test("bitset 595, 960") {
    testBitsets(65, 128)
  }

  test("randomized tests") {
    for (i <- 1 until 20) {
      val numFields1 = Random.nextInt(1000)
      val numFields2 = Random.nextInt(1000)
      testBitsets(numFields1, numFields2)
    }
  }

  ignore("concat of two bitmaps") {
    val schema1 = new StructType()
      .add("a", ByteType)
      .add("b", ShortType)
      .add("c", IntegerType)
      .add("d", LongType)

    val schema2 = new StructType()
      .add("e", FloatType)
      .add("f", DoubleType)
      .add("g", StringType)
      .add("h", StringType)

    val mergedSchema = StructType(schema1 ++ schema2)

    val converter1 = UnsafeProjection.create(schema1)
    val converter2 = UnsafeProjection.create(schema2)

    val row1 = converter1.apply(InternalRow(1.toByte, 2.toShort, 3, 4.toLong))
    val row2 = converter2.apply(InternalRow(5.0F, 6.0D, null, UTF8String.fromString("abcd")))

    val concater = GenerateRowConcat.create(schema1, schema2)
    val output = concater.concat(row1, row2)

    for (i <- mergedSchema.indices) {
      if (i < schema1.size) {
        assert(output.isNullAt(i) === row1.isNullAt(i))
//        if (!row1.isNullAt(i)) {
//          assert(output.get(i, schema1(i).dataType) === row1.get(i, schema1(i).dataType))
//        }
      } else {
        assert(output.isNullAt(i) === row2.isNullAt(i - schema1.size))
//        if (!row2.isNullAt(i)) {
//          assert(
//            output.get(i, schema2(i - schema1.size).dataType) ===
//              row2.get(i, schema2(i - schema1.size).dataType))
//        }
      }
    }
  }

}
