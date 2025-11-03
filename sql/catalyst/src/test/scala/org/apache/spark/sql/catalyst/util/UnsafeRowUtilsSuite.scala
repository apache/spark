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
import org.apache.spark.sql.types.{ArrayType, Decimal, DecimalType, IntegerType, MapType, StringType, StructField, StructType}

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
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(testRow, testOutputSchema).isEmpty)
    // Fail for fields number not match
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(
      testRow, StructType(testKeys.map(createIntegerField))).isDefined)
    // Fail for invalid schema
    val invalidSchema = StructType(testKeys.map(createIntegerField) ++
      Seq(StructField("struct", StructType(Seq(StructField("value1", StringType, true))), true),
        StructField("value2", IntegerType, false)))
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(testRow, invalidSchema).isDefined)
  }

  test("Handle special case for null variable-length Decimal") {
    val schema = StructType(StructField("d", DecimalType(19, 0), nullable = true) :: Nil)
    val unsafeRowProjection = UnsafeProjection.create(schema)
    val row = unsafeRowProjection(new SpecificInternalRow(schema))

    // row is empty at this point
    assert(row.isNullAt(0) && UnsafeRowUtils.getOffsetAndSize(row, 0) == (16, 0))
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(row, schema).isEmpty)

    // set Decimal field to precision-overflowed value
    val bigDecimalVal = Decimal(new JavaBigDecimal("12345678901234567890")) // precision=20, scale=0
    row.setDecimal(0, bigDecimalVal, 19) // should overflow and become null
    assert(row.isNullAt(0) && UnsafeRowUtils.getOffsetAndSize(row, 0) == (16, 0))
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(row, schema).isEmpty)

    // set Decimal field to valid non-null value
    val bigDecimalVal2 = Decimal(new JavaBigDecimal("1234567890123456789")) // precision=19, scale=0
    row.setDecimal(0, bigDecimalVal2, 19) // should succeed
    assert(!row.isNullAt(0) && UnsafeRowUtils.getOffsetAndSize(row, 0) == (16, 8))
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(row, schema).isEmpty)

    // set Decimal field to null explicitly, after which this field no longer supports updating
    row.setNullAt(0)
    assert(row.isNullAt(0) && UnsafeRowUtils.getOffsetAndSize(row, 0) == (0, 0))
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(row, schema).isEmpty)
  }

  test("Better schema status message") {
    assert(UnsafeRowUtils.getStructuralIntegrityStatus(testRow, testOutputSchema)
      .contains("[UnsafeRowStatus] expectedSchema: StructType(" +
        "StructField(key1,IntegerType,false),StructField(key2,IntegerType,false)," +
        "StructField(sum(key1),IntegerType,false),StructField(sum(key2),IntegerType,false)), " +
        "expectedSchemaNumFields: 4, numFields: 4, bitSetWidthInBytes: 8, rowSizeInBytes: 40\n" +
        "fieldStatus:\n" +
        "[UnsafeRowFieldStatus] index: 0, expectedFieldType: IntegerType,"))
  }

  test("isBinaryStable on complex types containing collated strings") {
    val nonBinaryStringType = StringType(CollationFactory.collationNameToId("UTF8_LCASE"))

    // simple checks
    assert(UnsafeRowUtils.isBinaryStable(IntegerType))
    assert(UnsafeRowUtils.isBinaryStable(StringType))
    assert(!UnsafeRowUtils.isBinaryStable(nonBinaryStringType))

    assert(UnsafeRowUtils.isBinaryStable(ArrayType(IntegerType)))
    assert(UnsafeRowUtils.isBinaryStable(ArrayType(StringType)))
    assert(!UnsafeRowUtils.isBinaryStable(ArrayType(nonBinaryStringType)))

    assert(UnsafeRowUtils.isBinaryStable(MapType(StringType, StringType)))
    assert(!UnsafeRowUtils.isBinaryStable(MapType(nonBinaryStringType, StringType)))
    assert(!UnsafeRowUtils.isBinaryStable(MapType(StringType, nonBinaryStringType)))
    assert(!UnsafeRowUtils.isBinaryStable(MapType(nonBinaryStringType, nonBinaryStringType)))
    assert(!UnsafeRowUtils.isBinaryStable(MapType(nonBinaryStringType, IntegerType)))
    assert(!UnsafeRowUtils.isBinaryStable(MapType(IntegerType, nonBinaryStringType)))

    assert(UnsafeRowUtils.isBinaryStable(StructType(StructField("field", IntegerType) :: Nil)))
    assert(UnsafeRowUtils.isBinaryStable(StructType(StructField("field", StringType) :: Nil)))
    assert(!UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field", nonBinaryStringType) :: Nil)))

    // nested complex types
    assert(UnsafeRowUtils.isBinaryStable(ArrayType(ArrayType(StringType))))
    assert(UnsafeRowUtils.isBinaryStable(ArrayType(MapType(StringType, IntegerType))))
    assert(UnsafeRowUtils.isBinaryStable(
      ArrayType(StructType(StructField("field", StringType) :: Nil))))
    assert(!UnsafeRowUtils.isBinaryStable(ArrayType(ArrayType(nonBinaryStringType))))
    assert(!UnsafeRowUtils.isBinaryStable(ArrayType(MapType(IntegerType, nonBinaryStringType))))
    assert(!UnsafeRowUtils.isBinaryStable(
      ArrayType(MapType(IntegerType, ArrayType(nonBinaryStringType)))))
    assert(!UnsafeRowUtils.isBinaryStable(
      ArrayType(StructType(StructField("field", nonBinaryStringType) :: Nil))))
    assert(!UnsafeRowUtils.isBinaryStable(ArrayType(StructType(
      Seq(StructField("second", IntegerType), StructField("second", nonBinaryStringType))))))

    assert(UnsafeRowUtils.isBinaryStable(MapType(ArrayType(StringType), ArrayType(IntegerType))))
    assert(UnsafeRowUtils.isBinaryStable(MapType(MapType(StringType, StringType), IntegerType)))
    assert(UnsafeRowUtils.isBinaryStable(
      MapType(StructType(StructField("field", StringType) :: Nil), IntegerType)))
    assert(!UnsafeRowUtils.isBinaryStable(
      MapType(ArrayType(nonBinaryStringType), ArrayType(IntegerType))))
    assert(!UnsafeRowUtils.isBinaryStable(
      MapType(IntegerType, ArrayType(nonBinaryStringType))))
    assert(!UnsafeRowUtils.isBinaryStable(
      MapType(MapType(IntegerType, nonBinaryStringType), IntegerType)))
    assert(!UnsafeRowUtils.isBinaryStable(
      MapType(StructType(StructField("field", nonBinaryStringType) :: Nil), IntegerType)))

    assert(UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field", ArrayType(IntegerType)) :: Nil)))
    assert(UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field", MapType(StringType, IntegerType)) :: Nil)))
    assert(UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field", StructType(StructField("sub", IntegerType) :: Nil)) :: Nil)))
    assert(!UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field", ArrayType(nonBinaryStringType)) :: Nil)))
    assert(!UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field", MapType(nonBinaryStringType, IntegerType)) :: Nil)))
    assert(!UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field",
        StructType(StructField("sub", nonBinaryStringType) :: Nil)) :: Nil)))
  }
}
