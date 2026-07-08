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

import java.time.LocalTime
import java.time.temporal.ChronoField.MICRO_OF_DAY

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.SparkFilterApi.longColumn
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{INT32, INT64}
import org.apache.parquet.schema.Type.Repetition.REQUIRED

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.types.{IntegerType, TimeType}

/**
 * Unit tests for [[TimeTypeParquetOps]]'s Parquet read guards - both the row-based
 * [[TimeTypeParquetOps.requireCompatibleParquetType]] and the vectorized-read
 * getVectorUpdater / getVectorUpdaterOrNull dispatch, which share the same
 * compatible-encoding check so the two readers accept and reject the same set.
 *
 * TimeType is stored in Parquet as INT64 TIME(MICROS, isAdjustedToUTC=false) for precision
 * 0..6 and INT64 TIME(NANOS, isAdjustedToUTC=false) for precision 7..9. The read-path guard
 * accepts both of those local-time encodings and rejects every other primitive/annotation
 * combination so that reading fails loudly rather than silently mis-decoding.
 *
 * SPARK-57416: the guard accepts isAdjustedToUTC=true to mirror the legacy
 * ParquetRowConverter guard (which only checks the TIME annotation and unit). Spark's
 * TimeType is zone-less local time, so the flag carries no extra information on read and the
 * raw time-of-day value decodes identically either way. This keeps the framework read path
 * consistent with both the legacy row-based reader and the vectorized reader.
 *
 * Also covers the filter-pushdown ops ([[TimeTypeParquetOps.filterOps]]) and the
 * [[ParquetTypeOps.filterOpsFor]] reverse lookup that resolves them.
 */
class TimeTypeParquetOpsSuite extends SparkFunSuite {

  private val timeMicros = TimeType(TimeType.MICROS_PRECISION)
  private val timeNanos = TimeType(TimeType.NANOS_PRECISION)

  // ---------- accept ----------

  test("accepts INT64 TIME(MICROS, isAdjustedToUTC=false) - the canonical micros encoding") {
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

  test("accepts INT64 TIME(NANOS, isAdjustedToUTC=false) - the canonical nanos encoding") {
    val field = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timeType(false, TimeUnit.NANOS))
      .named("c")
    // Must not throw.
    TimeTypeParquetOps.requireCompatibleParquetType(timeNanos, field)
  }

  test("accepts INT64 TIME(NANOS, isAdjustedToUTC=true) - matches legacy lenient read") {
    // Same zone-less reasoning as the MICROS case (SPARK-57416): the UTC-adjustment flag
    // carries no information for Spark's local-time TimeType, so a NANOS column is accepted
    // regardless of the flag.
    val field = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timeType(true, TimeUnit.NANOS))
      .named("c")
    // Must not throw.
    TimeTypeParquetOps.requireCompatibleParquetType(timeNanos, field)
  }

  // ---------- the primary reject paths ----------

  test("rejects raw INT64 with no logical type annotation") {
    val field = Types.primitive(INT64, REQUIRED).named("c")
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

  private def timeColumn(unit: TimeUnit): ColumnDescriptor =
    new ColumnDescriptor(
      Array("c"),
      Types.primitive(INT64, REQUIRED).as(LogicalTypeAnnotation.timeType(false, unit)).named("c"),
      0, 0)

  test("getVectorUpdater returns a framework updater for TimeType (micros and nanos)") {
    assert(TimeTypeParquetOps(timeMicros).getVectorUpdater(timeColumn(TimeUnit.MICROS)).isDefined)
    assert(TimeTypeParquetOps(timeNanos).getVectorUpdater(timeColumn(TimeUnit.NANOS)).isDefined)
    // Java-friendly companion entry point used by ParquetVectorUpdaterFactory.
    assert(ParquetTypeOps.getVectorUpdaterOrNull(timeMicros, timeColumn(TimeUnit.MICROS)) != null)
  }

  test("getVectorUpdater returns None for incompatible encodings (clean reject, vectorized path)") {
    val int32Millis = Types.primitive(INT32, REQUIRED)
      .as(LogicalTypeAnnotation.timeType(false, TimeUnit.MILLIS)).named("c")
    val rawInt64 = Types.primitive(INT64, REQUIRED).named("c")
    val int64Timestamp = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS)).named("c")
    // None -> the factory falls through to a clean SchemaColumnConvertNotSupportedException,
    // matching the row-path reject set (requireCompatibleParquetType), instead of silently
    // mis-decoding (e.g. readLongs over an INT32 column).
    Seq(int32Millis, rawInt64, int64Timestamp).foreach { field =>
      val descriptor = new ColumnDescriptor(Array("c"), field, 0, 0)
      assert(TimeTypeParquetOps(timeMicros).getVectorUpdater(descriptor).isEmpty)
    }
  }

  test("getVectorUpdaterOrNull returns null for non-framework types") {
    assert(ParquetTypeOps.getVectorUpdaterOrNull(IntegerType, null) == null)
  }

  test("supportsLazyDictionaryDecoding is false for TimeType (updater does per-value work)") {
    // TIME decoding does per-value micros->nanos + truncation in the updater, so lazy dictionary
    // decoding (which would bypass the updater) must stay disabled on the vectorized path.
    assert(!TimeTypeParquetOps(timeMicros)
      .supportsLazyDictionaryDecoding(timeColumn(TimeUnit.MICROS)))
    assert(!TimeTypeParquetOps(timeNanos)
      .supportsLazyDictionaryDecoding(timeColumn(TimeUnit.NANOS)))
    // Java-friendly companion entry point used by VectorizedColumnReader: FALSE for TimeType,
    // null for a non-framework type (so the reader keeps its built-in lazy-decoding decision).
    assert(ParquetTypeOps.supportsLazyDictionaryDecodingOrNull(
      timeMicros, timeColumn(TimeUnit.MICROS)) === java.lang.Boolean.FALSE)
    assert(ParquetTypeOps.supportsLazyDictionaryDecodingOrNull(IntegerType, null) == null)
  }

  // ---------- filter pushdown ops ----------

  test("filterOps accepts LocalTime values and rejects others") {
    val ops = TimeTypeParquetOps.filterOps
    assert(ops.acceptsValue(LocalTime.of(1, 2, 3)))
    assert(!ops.acceptsValue(java.lang.Long.valueOf(1L)))
    assert(!ops.acceptsValue("12:00:00"))
  }

  test("filterOps declares the canonical TimeType Parquet encoding") {
    val ops = TimeTypeParquetOps.filterOps
    assert(ops.primitiveTypeName === INT64)
    assert(ops.logicalTypeAnnotation ===
      LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS))
  }

  test("filterOps builds predicates for LocalTime, converting to micros-of-day") {
    val ops = TimeTypeParquetOps.filterOps
    val path = Array("c")
    val col = longColumn(path)
    val t = LocalTime.of(23, 59, 59, 123456000)
    // parquet-mr operators implement value equality, so we can pin the exact pushed-down value:
    // LocalTime -> micros-of-day Long, the same conversion the removed inline TimeType arms used.
    val micros = java.lang.Long.valueOf(t.getLong(MICRO_OF_DAY))
    assert(ops.makeEq(path, t) === FilterApi.eq(col, micros))
    assert(ops.makeNotEq(path, t) === FilterApi.notEq(col, micros))
    assert(ops.makeLt(path, t) === FilterApi.lt(col, micros))
    assert(ops.makeLtEq(path, t) === FilterApi.ltEq(col, micros))
    assert(ops.makeGt(path, t) === FilterApi.gt(col, micros))
    assert(ops.makeGtEq(path, t) === FilterApi.gtEq(col, micros))
    val set = new java.util.HashSet[java.lang.Long]()
    set.add(micros)
    set.add(null)
    assert(ops.makeIn(path, Array[Any](t, null)) === FilterApi.in(col, set))
  }

  test("filterOps eq/notEq/in tolerate a null value (IsNull / IsNotNull)") {
    val ops = TimeTypeParquetOps.filterOps
    val path = Array("c")
    val col = longColumn(path)
    // null value -> null Long comparand; used by ParquetFilters for IsNull / IsNotNull.
    assert(ops.makeEq(path, null) === FilterApi.eq(col, null.asInstanceOf[java.lang.Long]))
    assert(ops.makeNotEq(path, null) === FilterApi.notEq(col, null.asInstanceOf[java.lang.Long]))
    val set = new java.util.HashSet[java.lang.Long]()
    set.add(null)
    assert(ops.makeIn(path, Array[Any](null)) === FilterApi.in(col, set))
  }

  test("ParquetTypeOps.filterOpsFor resolves the TimeType encoding and nothing else") {
    assert(ParquetTypeOps.filterOpsFor(
      LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS), INT64).isDefined)
    // A different unit, isAdjustedToUTC=true, primitive, or annotation kind is not the
    // TimeType encoding, so no framework filter ops is returned (pushdown falls through).
    assert(ParquetTypeOps.filterOpsFor(
      LogicalTypeAnnotation.timeType(false, TimeUnit.NANOS), INT64).isEmpty)
    assert(ParquetTypeOps.filterOpsFor(
      LogicalTypeAnnotation.timeType(true, TimeUnit.MICROS), INT64).isEmpty)
    assert(ParquetTypeOps.filterOpsFor(
      LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS), INT32).isEmpty)
    assert(ParquetTypeOps.filterOpsFor(null, INT64).isEmpty)
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
