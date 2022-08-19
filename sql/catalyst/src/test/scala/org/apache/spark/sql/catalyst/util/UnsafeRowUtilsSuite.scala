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

package org.apache.spark.sql.catalyst.util

import java.math.{BigDecimal => JavaBigDecimal}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{Decimal, DecimalType, IntegerType, StringType, StructField, StructType}

class UnsafeRowUtilsSuite extends SparkFunSuite {

  val testKeys: Seq[String] = Seq("key1", "key2")
  val testValues: Seq[String] = Seq("sum(key1)", "sum(key2)")

  val testOutputSchema: StructType = StructType(
    testKeys.map(createIntegerField) ++ testValues.map(createIntegerField))

  val testRow: UnsafeRow = {
    val unsafeRowProjection = UnsafeProjection.create(testOutputSchema)
    val row = unsafeRowProjection(new SpecificInternalRow(testOutputSchema))
    (testKeys ++ testValues).zipWithIndex.foreach { case (_, index) => row.setInt(index, index) }
    row
  }

  private def createIntegerField(name: String): StructField = {
    StructField(name, IntegerType, nullable = false)
  }

  test("UnsafeRow format invalidation") {
    // Pass the checking
    UnsafeRowUtils.validateStructuralIntegrity(testRow, testOutputSchema)
    // Fail for fields number not match
    assert(!UnsafeRowUtils.validateStructuralIntegrity(
      testRow, StructType(testKeys.map(createIntegerField))))
    // Fail for invalid schema
    val invalidSchema = StructType(testKeys.map(createIntegerField) ++
      Seq(StructField("struct", StructType(Seq(StructField("value1", StringType, true))), true),
        StructField("value2", IntegerType, false)))
    assert(!UnsafeRowUtils.validateStructuralIntegrity(testRow, invalidSchema))
  }

  test("Handle special case for null variable-length Decimal") {
    val schema = StructType(StructField("d", DecimalType(19, 0), nullable = true) :: Nil)
    val unsafeRowProjection = UnsafeProjection.create(schema)
    val row = unsafeRowProjection(new SpecificInternalRow(schema))

    // row is empty at this point
    assert(row.isNullAt(0) && UnsafeRowUtils.getOffsetAndSize(row, 0) == (16, 0))
    assert(UnsafeRowUtils.validateStructuralIntegrity(row, schema))

    // set Decimal field to precision-overflowed value
    val bigDecimalVal = Decimal(new JavaBigDecimal("12345678901234567890")) // precision=20, scale=0
    row.setDecimal(0, bigDecimalVal, 19) // should overflow and become null
    assert(row.isNullAt(0) && UnsafeRowUtils.getOffsetAndSize(row, 0) == (16, 0))
    assert(UnsafeRowUtils.validateStructuralIntegrity(row, schema))

    // set Decimal field to valid non-null value
    val bigDecimalVal2 = Decimal(new JavaBigDecimal("1234567890123456789")) // precision=19, scale=0
    row.setDecimal(0, bigDecimalVal2, 19) // should succeed
    assert(!row.isNullAt(0) && UnsafeRowUtils.getOffsetAndSize(row, 0) == (16, 8))
    assert(UnsafeRowUtils.validateStructuralIntegrity(row, schema))

    // set Decimal field to null explicitly, after which this field no longer supports updating
    row.setNullAt(0)
    assert(row.isNullAt(0) && UnsafeRowUtils.getOffsetAndSize(row, 0) == (0, 0))
    assert(UnsafeRowUtils.validateStructuralIntegrity(row, schema))
  }
}
