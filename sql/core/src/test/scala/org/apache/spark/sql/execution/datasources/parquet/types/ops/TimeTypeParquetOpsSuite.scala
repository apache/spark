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
import org.apache.spark.sql.types.{IntegerType, TimeType}

/**
 * Unit tests for [[TimeTypeParquetOps.requireCompatibleParquetType]].
 *
 * TimeType is written to Parquet as INT64 TIME(MICROS, isAdjustedToUTC=false). The
 * read-path guard accepts any INT64 TIME(MICROS) column - both isAdjustedToUTC values -
 * and rejects every other primitive/annotation combination so that reading fails loudly
 * rather than silently mis-decoding (e.g. interpreting NANOS as MICROS, which would be
 * off by 1000x).
 *
 * SPARK-57416: the guard accepts isAdjustedToUTC=true to mirror the legacy
 * ParquetRowConverter guard (which only checks the TIME annotation and the MICROS unit).
 * Spark's TimeType is zone-less local time, so the flag carries no extra information on
 * read and the raw micros-of-day value decodes identically either way. This keeps the
 * framework read path consistent with both the legacy row-based reader and the vectorized
 * reader.
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

  test("accepts INT64 TIME(MICROS, isAdjustedToUTC=true) - matches legacy lenient read") {
    // SPARK-57416: the framework read guard mirrors the legacy ParquetRowConverter guard,
    // which accepts INT64 TIME(MICROS) regardless of isAdjustedToUTC. Spark's TimeType is
    // zone-less, so the raw micros-of-day value decodes identically either way; rejecting
    // this encoding would diverge from both the legacy row-based reader and the (lenient)
    // vectorized reader.
    val field = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timeType(true, TimeUnit.MICROS))
      .named("c")
    // Must not throw.
    TimeTypeParquetOps.requireCompatibleParquetType(timeMicros, field)
  }

  // ---------- the primary reject paths ----------

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

  // ---------- vectorized read updater ----------

  test("getVectorUpdater returns a framework updater for TimeType") {
    // descriptor is unused by TimeType's updater (micros -> nanos is precision-independent).
    assert(TimeTypeParquetOps(timeMicros).getVectorUpdater(null).isDefined)
    // Java-friendly companion entry point used by ParquetVectorUpdaterFactory.
    assert(ParquetTypeOps.getVectorUpdaterOrNull(timeMicros, null) != null)
  }

  test("getVectorUpdaterOrNull returns null for non-framework types") {
    assert(ParquetTypeOps.getVectorUpdaterOrNull(IntegerType, null) == null)
  }

  // ---------- helper ----------

  private def assertRejects(sparkType: TimeType, field: Type): Unit = {
    val ex = intercept[SparkRuntimeException] {
      TimeTypeParquetOps.requireCompatibleParquetType(sparkType, field)
    }
    assert(ex.getCondition === "PARQUET_CONVERSION_FAILURE.UNSUPPORTED",
      s"expected PARQUET_CONVERSION_FAILURE.UNSUPPORTED, got ${ex.getCondition}")
  }
}
