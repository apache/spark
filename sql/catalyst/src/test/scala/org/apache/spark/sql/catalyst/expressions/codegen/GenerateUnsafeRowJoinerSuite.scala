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
import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types._

/**
 * Test suite for [[GenerateUnsafeRowJoiner]].
 *
 * There is also a separate [[GenerateUnsafeRowJoinerBitsetSuite]] that tests specifically
 * concatenation for the bitset portion, since that is the hardest one to get right.
 */
class GenerateUnsafeRowJoinerSuite extends SparkFunSuite {

  private val fixed = Seq(IntegerType)
  private val variable = Seq(IntegerType, StringType)

  test("simple fixed width types") {
    testConcat(0, 0, fixed)
    testConcat(0, 1, fixed)
    testConcat(1, 0, fixed)
    testConcat(64, 0, fixed)
    testConcat(0, 64, fixed)
    testConcat(64, 64, fixed)
  }

  test("randomized fix width types") {
    for (i <- 0 until 20) {
      testConcatOnce(Random.nextInt(100), Random.nextInt(100), fixed)
    }
  }

  test("simple variable width types") {
    testConcat(0, 0, variable)
    testConcat(0, 1, variable)
    testConcat(1, 0, variable)
    testConcat(64, 0, variable)
    testConcat(0, 64, variable)
    testConcat(64, 64, variable)
  }

  test("randomized variable width types") {
    for (i <- 0 until 10) {
      testConcatOnce(Random.nextInt(100), Random.nextInt(100), variable)
    }
  }

  private def testConcat(numFields1: Int, numFields2: Int, candidateTypes: Seq[DataType]): Unit = {
    for (i <- 0 until 10) {
      testConcatOnce(numFields1, numFields2, candidateTypes)
    }
  }

  private def testConcatOnce(numFields1: Int, numFields2: Int, candidateTypes: Seq[DataType]) {
    info(s"schema size $numFields1, $numFields2")
    val schema1 = RandomDataGenerator.randomSchema(numFields1, candidateTypes)
    val schema2 = RandomDataGenerator.randomSchema(numFields2, candidateTypes)

    // Create the converters needed to convert from external row to internal row and to UnsafeRows.
    val internalConverter1 = CatalystTypeConverters.createToCatalystConverter(schema1)
    val internalConverter2 = CatalystTypeConverters.createToCatalystConverter(schema2)
    val converter1 = UnsafeProjection.create(schema1)
    val converter2 = UnsafeProjection.create(schema2)

    // Create the input rows, convert them into UnsafeRows.
    val extRow1 = RandomDataGenerator.forType(schema1, nullable = false).get.apply()
    val extRow2 = RandomDataGenerator.forType(schema2, nullable = false).get.apply()
    val row1 = converter1.apply(internalConverter1.apply(extRow1).asInstanceOf[InternalRow])
    val row2 = converter2.apply(internalConverter2.apply(extRow2).asInstanceOf[InternalRow])

    // Run the joiner.
    val mergedSchema = StructType(schema1 ++ schema2)
    val concater = GenerateUnsafeRowJoiner.create(schema1, schema2)
    val output = concater.join(row1, row2)

    // Test everything equals ...
    for (i <- mergedSchema.indices) {
      if (i < schema1.size) {
        assert(output.isNullAt(i) === row1.isNullAt(i))
        if (!output.isNullAt(i)) {
          assert(output.get(i, mergedSchema(i).dataType) === row1.get(i, mergedSchema(i).dataType))
        }
      } else {
        assert(output.isNullAt(i) === row2.isNullAt(i - schema1.size))
        if (!output.isNullAt(i)) {
          assert(output.get(i, mergedSchema(i).dataType) ===
            row2.get(i - schema1.size, mergedSchema(i).dataType))
        }
      }
    }
  }

}
