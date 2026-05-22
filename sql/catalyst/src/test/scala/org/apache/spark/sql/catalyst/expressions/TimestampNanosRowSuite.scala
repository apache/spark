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
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{TimestampLTZNanos, TimestampNTZNanos}

class TimestampNanosRowSuite extends SparkFunSuite with ExpressionEvalHelper {

  private val ntzValue = new TimestampNTZNanos(1234567890123L, 42.toShort)
  private val ltzValue = new TimestampLTZNanos(9876543210987L, 999.toShort)

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

    val updatedNtz = new TimestampNTZNanos(1L, 0.toShort)
    unsafeRow.setTimestampNTZNanos(0, updatedNtz)
    assert(unsafeRow.getTimestampNTZNanos(0) === updatedNtz)

    val offset = unsafeRow.getLong(0) >>> 32
    unsafeRow.setTimestampNTZNanos(0, null)
    assert(unsafeRow.getTimestampNTZNanos(0) === null)
    assert(unsafeRow.getLong(0) >>> 32 === offset)
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
    intercept[IllegalArgumentException] {
      Literal.validateLiteralValue(ntzValue, TimestampLTZNanosType(7))
    }
  }
}
