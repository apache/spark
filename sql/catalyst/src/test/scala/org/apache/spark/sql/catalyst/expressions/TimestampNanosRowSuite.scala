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
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.TimestampNanosVal
import org.apache.spark.util.ArrayImplicits._

class TimestampNanosRowSuite extends SparkFunSuite with ExpressionEvalHelper {

  private val ntzValue = TimestampNanosVal.fromParts(1234567890123L, 42.toShort)
  private val ltzValue = TimestampNanosVal.fromParts(9876543210987L, 999.toShort)

  test("GenerateUnsafeProjection.canSupport for nanos timestamp types") {
    assert(GenerateUnsafeProjection.canSupport(TimestampNTZNanosType(9)))
    assert(GenerateUnsafeProjection.canSupport(TimestampLTZNanosType(7)))
  }

  test("GenericInternalRow roundtrip for TIMESTAMP_NTZ nanos") {
    val row = new GenericInternalRow(Array[Any](ntzValue, null))
    val accessor = InternalRow.getAccessor(TimestampNTZNanosType(9))
    val writer = InternalRow.getWriter(0, TimestampNTZNanosType(9))
    assert(accessor(row, 0) === ntzValue)
    assert(accessor(row, 1) === null)

    val row2 = new GenericInternalRow(Array[Any](null, null))
    writer(row2, ntzValue)
    assert(accessor(row2, 0) === ntzValue)
  }

  test("GenericInternalRow roundtrip for TIMESTAMP_LTZ nanos") {
    val row = new GenericInternalRow(Array[Any](ltzValue, null))
    val accessor = InternalRow.getAccessor(TimestampLTZNanosType(8))
    val writer = InternalRow.getWriter(0, TimestampLTZNanosType(8))
    assert(accessor(row, 0) === ltzValue)
    assert(accessor(row, 1) === null)

    val row2 = new GenericInternalRow(Array[Any](null, null))
    writer(row2, ltzValue)
    assert(accessor(row2, 0) === ltzValue)
  }

  testBothCodegenAndInterpreted("UnsafeRow roundtrip for nanos timestamp columns") {
    val schema = StructType(Seq(
      StructField("ntz", TimestampNTZNanosType(9), nullable = true),
      StructField("ltz", TimestampLTZNanosType(7), nullable = true)))
    val fieldTypes = schema.map(_.dataType).toArray
    val converter = UnsafeProjection.create(fieldTypes)

    val input = new SpecificInternalRow(fieldTypes.toIndexedSeq)
    input.update(0, ntzValue)
    input.update(1, ltzValue)

    val unsafeRow = converter.apply(input)
    assert(unsafeRow.getTimestampNTZNanos(0) === ntzValue)
    assert(unsafeRow.getTimestampLTZNanos(1) === ltzValue)

    val updatedNtz = TimestampNanosVal.fromParts(1L, 0.toShort)
    unsafeRow.setTimestampNTZNanos(0, updatedNtz)
    assert(unsafeRow.getTimestampNTZNanos(0) === updatedNtz)

    val offset = unsafeRow.getLong(0) >>> 32
    unsafeRow.setTimestampNTZNanos(0, null)
    assert(unsafeRow.getTimestampNTZNanos(0) === null)
    assert(unsafeRow.getLong(0) >>> 32 === offset)
  }

  // Nanosecond timestamps use the UnsafeRow variable-length region, like CalendarInterval.
  // SPARK-41535 fixed null CalendarInterval columns not being marked null when an
  // UnsafeProjection buffer is first created from a row of null intervals (see
  // MutableProjectionSuite and UnsafeRowConverterSuite). The same applies here:
  // nullAtCreation must have null bits set for every nanos column, and later
  // setTimestampNTZNanos / setTimestampLTZNanos must work in place.
  testBothCodegenAndInterpreted("nanos timestamps initialized as null in unsafe projection") {
    val fieldTypes: Array[DataType] =
      Array(TimestampNTZNanosType(9), TimestampLTZNanosType(7))
    val converter = UnsafeProjection.create(fieldTypes)

    val row = new SpecificInternalRow(fieldTypes.toImmutableArraySeq)
    row.setTimestampNTZNanos(0, null)
    row.setTimestampLTZNanos(1, null)

    val nullAtCreation = converter.apply(row)

    for (i <- 0 until row.numFields) {
      assert(nullAtCreation.isNullAt(i))
    }

    val ntz = TimestampNanosVal.fromParts(100L, 50.toShort)
    val ltz = TimestampNanosVal.fromParts(200L, 100.toShort)
    nullAtCreation.setTimestampNTZNanos(0, ntz)
    nullAtCreation.setTimestampLTZNanos(1, ltz)
    assert(nullAtCreation.getTimestampNTZNanos(0) === ntz)
    assert(nullAtCreation.getTimestampLTZNanos(1) === ltz)
  }

  // Exercises UnsafeArrayWriter.write(int, TimestampNanosVal) via the codegen path through
  // GenerateUnsafeProjection.writeArrayToBuffer, mirroring the CalendarInterval-array tests
  // in UnsafeRowConverterSuite.
  testBothCodegenAndInterpreted("UnsafeArrayWriter for nanos timestamp arrays") {
    val arrType = ArrayType(TimestampNTZNanosType(9), containsNull = true)
    val converter = UnsafeProjection.create(Array[DataType](arrType))
    val input = new GenericInternalRow(Array[Any](
      new GenericArrayData(Array[Any](ntzValue, null, ntzValue))))
    val output = converter.apply(input)
    val arr = output.getArray(0)
    assert(arr.numElements() == 3)
    assert(arr.getTimestampNTZNanos(0) === ntzValue)
    assert(arr.isNullAt(1))
    assert(arr.getTimestampNTZNanos(2) === ntzValue)
  }

  testBothCodegenAndInterpreted("UnsafeArrayWriter for LTZ nanos timestamp arrays") {
    val arrType = ArrayType(TimestampLTZNanosType(7), containsNull = true)
    val converter = UnsafeProjection.create(Array[DataType](arrType))
    val input = new GenericInternalRow(Array[Any](
      new GenericArrayData(Array[Any](ltzValue, null, ltzValue))))
    val output = converter.apply(input)
    val arr = output.getArray(0)
    assert(arr.numElements() == 3)
    assert(arr.getTimestampLTZNanos(0) === ltzValue)
    assert(arr.isNullAt(1))
    assert(arr.getTimestampLTZNanos(2) === ltzValue)
  }

  testBothCodegenAndInterpreted("codegen projection reads nanos timestamp column") {
    val boundRef = BoundReference(0, TimestampNTZNanosType(9), nullable = false)
    val projection = GenerateUnsafeProjection.generate(Seq(boundRef))
    val input = new GenericInternalRow(Array[Any](ntzValue))
    val output = projection.apply(input)
    assert(output.getTimestampNTZNanos(0) === ntzValue)
  }

  test("literal validation for nanosecond timestamp types") {
    Literal.validateLiteralValue(ntzValue, TimestampNTZNanosType(9))
    Literal.validateLiteralValue(ltzValue, TimestampLTZNanosType(7))
    // NTZ and LTZ share the same physical TimestampNanosVal; logical type is schema metadata.
    Literal.validateLiteralValue(ntzValue, TimestampLTZNanosType(7))
    intercept[IllegalArgumentException] {
      Literal.validateLiteralValue(0L, TimestampNTZNanosType(9))
    }
  }

  test("checkEvaluation roundtrip for nanos timestamp Literal") {
    checkEvaluation(Literal.create(ntzValue, TimestampNTZNanosType(9)), ntzValue)
    checkEvaluation(Literal.create(ltzValue, TimestampLTZNanosType(7)), ltzValue)
  }
}
