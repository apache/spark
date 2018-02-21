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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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

  test("rows with all empty strings") {
    val schema = StructType(Seq(
      StructField("f1", StringType), StructField("f2", StringType)))
    val row: UnsafeRow = UnsafeProjection.create(schema).apply(
      InternalRow(UTF8String.EMPTY_UTF8, UTF8String.EMPTY_UTF8))
     testConcat(schema, row, schema, row)
  }

  test("rows with all empty int arrays") {
    val schema = StructType(Seq(
      StructField("f1", ArrayType(IntegerType)), StructField("f2", ArrayType(IntegerType))))
    val emptyIntArray =
      ExpressionEncoder[Array[Int]]().resolveAndBind().toRow(Array.emptyIntArray).getArray(0)
    val row: UnsafeRow = UnsafeProjection.create(schema).apply(
      InternalRow(emptyIntArray, emptyIntArray))
    testConcat(schema, row, schema, row)
  }

  test("alternating empty and non-empty strings") {
    val schema = StructType(Seq(
      StructField("f1", StringType), StructField("f2", StringType)))
    val row: UnsafeRow = UnsafeProjection.create(schema).apply(
      InternalRow(UTF8String.EMPTY_UTF8, UTF8String.fromString("foo")))
    testConcat(schema, row, schema, row)
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

  test("SPARK-22508: GenerateUnsafeRowJoiner.create should not generate codes beyond 64KB") {
    val N = 3000
    testConcatOnce(N, N, variable)
  }

  private def testConcat(numFields1: Int, numFields2: Int, candidateTypes: Seq[DataType]): Unit = {
    for (i <- 0 until 10) {
      testConcatOnce(numFields1, numFields2, candidateTypes)
    }
  }

  private def testConcatOnce(numFields1: Int, numFields2: Int, candidateTypes: Seq[DataType]) {
    info(s"schema size $numFields1, $numFields2")
    val random = new Random()
    val schema1 = RandomDataGenerator.randomSchema(random, numFields1, candidateTypes)
    val schema2 = RandomDataGenerator.randomSchema(random, numFields2, candidateTypes)

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
    testConcat(schema1, row1, schema2, row2)
  }

  private def testConcat(
      schema1: StructType,
      row1: UnsafeRow,
      schema2: StructType,
      row2: UnsafeRow) {

    // Run the joiner.
    val mergedSchema = StructType(schema1 ++ schema2)
    val concater = GenerateUnsafeRowJoiner.create(schema1, schema2)
    val output: UnsafeRow = concater.join(row1, row2)

    // We'll also compare to an UnsafeRow produced with JoinedRow + UnsafeProjection. This ensures
    // that unused space in the row (e.g. leftover bits in the null-tracking bitmap) is written
    // correctly.
    val expectedOutput: UnsafeRow = {
      val joinedRowProjection = UnsafeProjection.create(mergedSchema)
      val joined = new JoinedRow()
      joinedRowProjection.apply(joined.apply(row1, row2))
    }

    // Test everything equals ...
    for (i <- mergedSchema.indices) {
      val dataType = mergedSchema(i).dataType
      if (i < schema1.size) {
        assert(output.isNullAt(i) === row1.isNullAt(i))
        if (!output.isNullAt(i)) {
          assert(output.get(i, dataType) === row1.get(i, dataType))
          assert(output.get(i, dataType) === expectedOutput.get(i, dataType))
        }
      } else {
        assert(output.isNullAt(i) === row2.isNullAt(i - schema1.size))
        if (!output.isNullAt(i)) {
          assert(output.get(i, dataType) === row2.get(i - schema1.size, dataType))
          assert(output.get(i, dataType) === expectedOutput.get(i, dataType))
        }
      }
    }


    assert(
      expectedOutput.getSizeInBytes == output.getSizeInBytes,
      "output isn't same size in bytes as slow path")

    // Compare the UnsafeRows byte-by-byte so that we can print more useful debug information in
    // case this assertion fails:
    val actualBytes = output.getBaseObject.asInstanceOf[Array[Byte]]
      .take(output.getSizeInBytes)
    val expectedBytes = expectedOutput.getBaseObject.asInstanceOf[Array[Byte]]
      .take(expectedOutput.getSizeInBytes)

    val bitsetWidth = UnsafeRow.calculateBitSetWidthInBytes(expectedOutput.numFields())
    val actualBitset = actualBytes.take(bitsetWidth)
    val expectedBitset = expectedBytes.take(bitsetWidth)
    assert(actualBitset === expectedBitset, "bitsets were not equal")

    val fixedLengthSize = expectedOutput.numFields() * 8
    val actualFixedLength = actualBytes.slice(bitsetWidth, bitsetWidth + fixedLengthSize)
    val expectedFixedLength = expectedBytes.slice(bitsetWidth, bitsetWidth + fixedLengthSize)
    if (actualFixedLength !== expectedFixedLength) {
      actualFixedLength.grouped(8)
        .zip(expectedFixedLength.grouped(8))
        .zip(mergedSchema.fields.toIterator)
        .foreach {
          case ((actual, expected), field) =>
            assert(actual === expected, s"Fixed length sections are not equal for field $field")
      }
      fail("Fixed length sections were not equal")
    }

    val variableLengthStart = bitsetWidth + fixedLengthSize
    val actualVariableLength = actualBytes.drop(variableLengthStart)
    val expectedVariableLength = expectedBytes.drop(variableLengthStart)
    assert(actualVariableLength === expectedVariableLength, "fixed length sections were not equal")

    assert(output.hashCode() == expectedOutput.hashCode(), "hash codes were not equal")
  }

}
