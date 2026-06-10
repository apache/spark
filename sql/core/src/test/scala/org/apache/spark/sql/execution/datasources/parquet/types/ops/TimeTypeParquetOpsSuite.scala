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

package org.apache.spark.sql.execution.datasources.parquet.types.ops

import org.apache.parquet.schema.{LogicalTypeAnnotation, Type, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{INT32, INT64}
import org.apache.parquet.schema.Type.Repetition.REQUIRED

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.types.TimeType

/**
 * Unit tests for [[TimeTypeParquetOps.requireCompatibleParquetType]].
 *
 * TimeType is stored in Parquet as INT64 TIME(MICROS, isAdjustedToUTC=false).
 * Any other encoding is rejected so that reading fails loudly rather than
 * silently mis-decoding (e.g. interpreting NANOS as MICROS, which would be off
 * by 1000x).
 *
 * These tests document the exact reject set and pin the intended behavior of
 * the read-path guard. They also serve as a regression hook for the
 * isAdjustedToUTC=true ON/OFF behavior difference flagged on
 * https://github.com/apache/spark/pull/55326 - whichever resolution lands
 * (mirror the original guard, or tighten both paths), the corresponding test
 * below must be updated, which makes the intent explicit in the diff.
 */
class TimeTypeParquetOpsSuite extends SparkFunSuite {

  private val timeMicros = TimeType(TimeType.MICROS_PRECISION)

  // ---------- accept ----------

  test("accepts INT64 TIME(MICROS, isAdjustedToUTC=false) - the canonical encoding") {
    val field = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS))
      .named("c")
    // Must not throw.
    TimeTypeParquetOps.requireCompatibleParquetType(timeMicros, field)
  }

  // ---------- the four reject paths called out in code review ----------

  test("rejects raw INT64 with no logical type annotation") {
    val field = Types.primitive(INT64, REQUIRED).named("c")
    assertRejects(timeMicros, field)
  }

  test("rejects INT64 TIME(NANOS, isAdjustedToUTC=false)") {
    val field = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timeType(false, TimeUnit.NANOS))
      .named("c")
    assertRejects(timeMicros, field)
  }

  test("rejects INT32 TIME(MILLIS, isAdjustedToUTC=false)") {
    // Per Parquet spec TIME(MILLIS) is INT32; the primitive-type guard catches it.
    val field = Types.primitive(INT32, REQUIRED)
      .as(LogicalTypeAnnotation.timeType(false, TimeUnit.MILLIS))
      .named("c")
    assertRejects(timeMicros, field)
  }

  test("rejects INT64 TIME(MICROS, isAdjustedToUTC=true) - ON/OFF divergence pin") {
    // Regression hook for the discussion on apache/spark#55326. The legacy
    // ParquetRowConverter guard did NOT check isAdjustedToUTC, so framework-OFF
    // reads accept this encoding while framework-ON rejects it. If/when that
    // divergence is resolved (either by dropping the isAdjustedToUTC check here
    // or by tightening the legacy path to match), update THIS test accordingly
    // so the intent of the change is explicit.
    val field = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timeType(true, TimeUnit.MICROS))
      .named("c")
    assertRejects(timeMicros, field)
  }

  // ---------- additional rejects for full reject-set coverage ----------

  test("rejects INT64 TIMESTAMP(MICROS) - wrong annotation kind") {
    val field = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS))
      .named("c")
    assertRejects(timeMicros, field)
  }

  test("rejects INT64 DECIMAL - wrong annotation kind") {
    val field = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.decimalType(2, 18))
      .named("c")
    assertRejects(timeMicros, field)
  }

  test("rejects non-primitive (group) type") {
    val field: Type = Types.buildGroup(REQUIRED).named("c")
    assertRejects(timeMicros, field)
  }

  // Note: a "BINARY with TIME(MICROS) annotation" combination is impossible to
  // construct - the parquet-mr Types builder itself rejects it with
  // IllegalStateException("TIME(MICROS,false) can only annotate INT64"). So the
  // wrong-primitive branch of requireCompatibleParquetType is unreachable for
  // the TIME annotation; the raw-INT64 / TIMESTAMP / DECIMAL / group tests
  // above already exercise the !isPrimitive and "non-TIME annotation" branches.

  // ---------- helper ----------

  private def assertRejects(sparkType: TimeType, field: Type): Unit = {
    val ex = intercept[SparkRuntimeException] {
      TimeTypeParquetOps.requireCompatibleParquetType(sparkType, field)
    }
    assert(ex.getCondition === "PARQUET_CONVERSION_FAILURE.UNSUPPORTED",
      s"expected PARQUET_CONVERSION_FAILURE.UNSUPPORTED, got ${ex.getCondition}")
  }
}
