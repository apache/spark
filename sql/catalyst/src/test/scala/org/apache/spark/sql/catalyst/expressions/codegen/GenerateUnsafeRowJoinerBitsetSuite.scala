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
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

/**
 * A test suite for the bitset portion of the row concatenation.
 */
class GenerateUnsafeRowJoinerBitsetSuite extends SparkFunSuite {

  test("bitset concat: boundary size 0, 0") {
    testBitsets(0, 0)
  }

  test("bitset concat: boundary size 0, 64") {
    testBitsets(0, 64)
  }

  test("bitset concat: boundary size 64, 0") {
    testBitsets(64, 0)
  }

  test("bitset concat: boundary size 64, 64") {
    testBitsets(64, 64)
  }

  test("bitset concat: boundary size 0, 128") {
    testBitsets(0, 128)
  }

  test("bitset concat: boundary size 128, 0") {
    testBitsets(128, 0)
  }

  test("bitset concat: boundary size 128, 128") {
    testBitsets(128, 128)
  }

  test("bitset concat: single word bitsets") {
    testBitsets(10, 5)
  }

  test("bitset concat: first bitset larger than a word") {
    testBitsets(67, 5)
  }

  test("bitset concat: second bitset larger than a word") {
    testBitsets(6, 67)
  }

  test("bitset concat: no reduction in bitset size") {
    testBitsets(33, 34)
  }

  test("bitset concat: two words") {
    testBitsets(120, 95)
  }

  test("bitset concat: bitset 65, 128") {
    testBitsets(65, 128)
  }

  test("bitset concat: randomized tests") {
    for (i <- 1 until 20) {
      val numFields1 = Random.nextInt(1000)
      val numFields2 = Random.nextInt(1000)
      testBitsetsOnce(numFields1, numFields2)
    }
  }

  private def createUnsafeRow(numFields: Int): UnsafeRow = {
    val row = new UnsafeRow
    val sizeInBytes = numFields * 8 + ((numFields + 63) / 64) * 8
    // Allocate a larger buffer than needed and point the UnsafeRow to somewhere in the middle.
    // This way we can test the joiner when the input UnsafeRows are not the entire arrays.
    val offset = numFields * 8
    val buf = new Array[Byte](sizeInBytes + offset)
    row.pointTo(buf, Platform.BYTE_ARRAY_OFFSET + offset, numFields, sizeInBytes)
    row
  }

  private def testBitsets(numFields1: Int, numFields2: Int): Unit = {
    for (i <- 0 until 5) {
      testBitsetsOnce(numFields1, numFields2)
    }
  }

  private def testBitsetsOnce(numFields1: Int, numFields2: Int): Unit = {
    info(s"num fields: $numFields1 and $numFields2")
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

    val concater = GenerateUnsafeRowJoiner.create(schema1, schema2)
    val output = concater.join(row1, row2)

    def dumpDebug(): String = {
      val set1 = Seq.tabulate(numFields1) { i => if (row1.isNullAt(i)) "1" else "0" }
      val set2 = Seq.tabulate(numFields2) { i => if (row2.isNullAt(i)) "1" else "0" }
      val out = Seq.tabulate(numFields1 + numFields2) { i => if (output.isNullAt(i)) "1" else "0" }

      s"""
         |input1: ${set1.mkString}
         |input2: ${set2.mkString}
         |output: ${out.mkString}
         |expect: ${set1.mkString}${set2.mkString}
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
}
