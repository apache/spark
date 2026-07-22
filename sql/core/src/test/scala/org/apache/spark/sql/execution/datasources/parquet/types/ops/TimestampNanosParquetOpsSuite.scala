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

import java.time.{Instant, LocalDateTime, ZoneOffset}

import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.SparkFilterApi.longColumn
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.{TimestampLogicalTypeAnnotation, TimeUnit}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{INT32, INT64}
import org.apache.parquet.schema.Type.Repetition.REQUIRED

import org.apache.spark.{SparkArithmeticException, SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.util.{DateTimeConstants, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.parquet.ParentContainerUpdater
import org.apache.spark.sql.types.{TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Unit tests for the nanosecond-timestamp Parquet ops
 * ([[TimestampLTZNanosParquetOps]] / [[TimestampNTZNanosParquetOps]]), the Types Framework
 * integration for [[TimestampLTZNanosType]] / [[TimestampNTZNanosType]] in Parquet.
 *
 * End-to-end read/write/round-trip behavior is covered by `ParquetTimestampNanosSuite`; this
 * suite pins the ops-level contracts the framework dispatch relies on:
 *   - the write schema annotation (INT64 TIMESTAMP(NANOS), isAdjustedToUTC = true for LTZ /
 *     false for NTZ);
 *   - the read-path guard that fails loudly when a nanos type is requested over a non-NANOS
 *     Parquet column (so framework-first dispatch never silently mis-decodes);
 *   - the (epochMicros, nanosWithinMicro) -> INT64 epoch-nanos packing and its overflow error.
 */
class TimestampNanosParquetOpsSuite extends SparkFunSuite {

  private val ltz = TimestampLTZNanosParquetOps(
    TimestampLTZNanosType(TimestampLTZNanosType.NANOS_PRECISION))
  private val ntz = TimestampNTZNanosParquetOps(
    TimestampNTZNanosType(TimestampNTZNanosType.NANOS_PRECISION))

  // ---------- schema conversion (write path) ----------

  test("LTZ converts to INT64 TIMESTAMP(NANOS, isAdjustedToUTC=true)") {
    assertNanosTimestampSchema(ltz.convertToParquetType("c", REQUIRED, inShredded = false),
      expectedAdjustedToUTC = true)
  }

  test("NTZ converts to INT64 TIMESTAMP(NANOS, isAdjustedToUTC=false)") {
    assertNanosTimestampSchema(ntz.convertToParquetType("c", REQUIRED, inShredded = false),
      expectedAdjustedToUTC = false)
  }

  // ---------- read-path guard ----------

  test("newConverter rejects a non-NANOS Parquet column (TIMESTAMP(MICROS))") {
    val microsField = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS))
      .named("c")
    val ex = intercept[SparkRuntimeException] {
      ltz.newConverter(microsField, new ParentContainerUpdater {})
    }
    assert(ex.getCondition === "PARQUET_CONVERSION_FAILURE.UNSUPPORTED")
  }

  test("newConverter rejects a raw INT64 column with no annotation") {
    val rawField = Types.primitive(INT64, REQUIRED).named("c")
    val ex = intercept[SparkRuntimeException] {
      ntz.newConverter(rawField, new ParentContainerUpdater {})
    }
    assert(ex.getCondition === "PARQUET_CONVERSION_FAILURE.UNSUPPORTED")
  }

  test("newConverter decodes INT64 epoch-nanos into the (epochMicros, nanosWithinMicro) pair") {
    // 1_000_000_500 ns = 1_000_000 micros + 500 ns; precision 9 keeps all sub-micro digits.
    assert(decode(ltz, nanosField(isAdjustedToUTC = true), 1000000500L) ===
      TimestampNanosVal.fromParts(1000000L, 500.toShort))
    // Floor semantics keep nanosWithinMicro in [0, 999] for pre-epoch values.
    assert(decode(ltz, nanosField(isAdjustedToUTC = true), -1L) ===
      TimestampNanosVal.fromParts(-1L, 999.toShort))
  }

  test("newConverter truncates sub-precision nanos to an explicit lower read precision") {
    val ntz7 = TimestampNTZNanosParquetOps(TimestampNTZNanosType(7))
    // nanosWithinMicro 123 -> truncated to 100 at precision 7.
    assert(decode(ntz7, nanosField(isAdjustedToUTC = false), 2000000123L) ===
      TimestampNanosVal.fromParts(2000000L, 100.toShort))
  }

  // ---------- isNanosTimestamp helper ----------

  test("isNanosTimestamp recognizes only INT64 TIMESTAMP(NANOS)") {
    val nanos = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS)).named("c")
    val micros = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS)).named("c")
    val timeNanos = Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timeType(false, TimeUnit.NANOS)).named("c")
    val raw = Types.primitive(INT64, REQUIRED).named("c")

    assert(TimestampNanosParquetOps.isNanosTimestamp(nanos))
    assert(!TimestampNanosParquetOps.isNanosTimestamp(micros))
    assert(!TimestampNanosParquetOps.isNanosTimestamp(timeNanos))
    assert(!TimestampNanosParquetOps.isNanosTimestamp(raw))
  }

  // ---------- (epochMicros, nanosWithinMicro) -> INT64 epoch-nanos packing ----------

  test("timestampNanosToEpochNanos combines micros and sub-micro nanos") {
    val value = TimestampNanosVal.fromParts(1000000L, 500.toShort)
    val packed = DateTimeUtils.timestampNanosToEpochNanos(
      value, isNtz = false, sink = "Parquet INT64")
    assert(packed === 1000000L * DateTimeConstants.NANOS_PER_MICROS + 500L)
  }

  test("timestampNanosToEpochNanos throws DATETIME_OVERFLOW outside the INT64 epoch-nanos range") {
    // Year ~5138; well past the int64 epoch-nanos cutoff (2262) but still renderable, so the
    // multiply overflows and the overflow error - not a rendering error - is what surfaces.
    val tooLarge = TimestampNanosVal.fromParts(100000000000000000L, 0.toShort)
    Seq(true, false).foreach { isNtz =>
      val ex = intercept[SparkArithmeticException] {
        DateTimeUtils.timestampNanosToEpochNanos(tooLarge, isNtz, sink = "Parquet INT64")
      }
      assert(ex.getCondition === "DATETIME_OVERFLOW")
    }
  }

  // ---------- filter-pushdown ops ----------

  test("filterOps accepts the matching Java time value and rejects others") {
    // LTZ pushes down java.time.Instant; NTZ pushes down java.time.LocalDateTime. Each rejects
    // the other type (and non-temporal values) so a mismatched literal falls through to no
    // pushdown rather than a ClassCastException in the converter.
    val ltzOps = TimestampNanosParquetOps.ltzFilterOps
    val ntzOps = TimestampNanosParquetOps.ntzFilterOps
    assert(ltzOps.acceptsValue(Instant.parse("2020-01-01T00:00:00Z")))
    assert(!ltzOps.acceptsValue(LocalDateTime.parse("2020-01-01T00:00:00")))
    assert(!ltzOps.acceptsValue(java.lang.Long.valueOf(1L)))
    assert(ntzOps.acceptsValue(LocalDateTime.parse("2020-01-01T00:00:00")))
    assert(!ntzOps.acceptsValue(Instant.parse("2020-01-01T00:00:00Z")))
    assert(!ntzOps.acceptsValue("2020-01-01"))
  }

  test("filterOps rejects values outside the INT64 epoch-nanos range (falls back to full scan)") {
    // Year 2300 is past the ~2262 int64 epoch-nanos cutoff: encoding would overflow, so
    // acceptsValue must reject it (SPARK-46092-style guard) instead of throwing during filter
    // creation. An in-range value is accepted.
    val ltzOps = TimestampNanosParquetOps.ltzFilterOps
    val ntzOps = TimestampNanosParquetOps.ntzFilterOps
    assert(!ltzOps.acceptsValue(Instant.parse("2300-01-01T00:00:00Z")))
    assert(!ntzOps.acceptsValue(LocalDateTime.parse("2300-01-01T00:00:00")))
    assert(ltzOps.acceptsValue(Instant.parse("2020-01-01T00:00:00Z")))
    assert(ntzOps.acceptsValue(LocalDateTime.parse("2020-01-01T00:00:00")))
  }

  test("filterOps declares the canonical nanos-timestamp Parquet encoding") {
    // LTZ is isAdjustedToUTC=true, NTZ is false; both INT64 TIMESTAMP(NANOS). These are the keys
    // the ParquetFilters reverse lookup matches against the file schema.
    val ltzOps = TimestampNanosParquetOps.ltzFilterOps
    val ntzOps = TimestampNanosParquetOps.ntzFilterOps
    assert(ltzOps.primitiveTypeName === INT64)
    assert(ltzOps.logicalTypeAnnotation ===
      LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS))
    assert(ntzOps.primitiveTypeName === INT64)
    assert(ntzOps.logicalTypeAnnotation ===
      LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS))
  }

  test("filterOps builds predicates converting to signed INT64 epoch-nanoseconds") {
    val path = Array("c")
    val col = longColumn(path)

    // LTZ: Instant -> epoch-nanos. Sub-microsecond digits are preserved (not truncated to micros).
    val ltzOps = TimestampNanosParquetOps.ltzFilterOps
    val instant = Instant.parse("2020-01-01T12:34:56.000000789Z")
    val ltzNanos = java.lang.Long.valueOf(
      instant.getEpochSecond * DateTimeConstants.NANOS_PER_SECOND + instant.getNano)
    assert(ltzOps.makeEq(path, instant) === FilterApi.eq(col, ltzNanos))
    assert(ltzOps.makeNotEq(path, instant) === FilterApi.notEq(col, ltzNanos))
    assert(ltzOps.makeLt(path, instant) === FilterApi.lt(col, ltzNanos))
    assert(ltzOps.makeLtEq(path, instant) === FilterApi.ltEq(col, ltzNanos))
    assert(ltzOps.makeGt(path, instant) === FilterApi.gt(col, ltzNanos))
    assert(ltzOps.makeGtEq(path, instant) === FilterApi.gtEq(col, ltzNanos))

    // NTZ: LocalDateTime (interpreted at UTC) -> epoch-nanos.
    val ntzOps = TimestampNanosParquetOps.ntzFilterOps
    val ldt = LocalDateTime.parse("2020-01-01T12:34:56.000000789")
    val ntzInstant = ldt.toInstant(ZoneOffset.UTC)
    val ntzNanos = java.lang.Long.valueOf(
      ntzInstant.getEpochSecond * DateTimeConstants.NANOS_PER_SECOND + ntzInstant.getNano)
    assert(ntzOps.makeEq(path, ldt) === FilterApi.eq(col, ntzNanos))
    assert(ntzOps.makeIn(path, Array[Any](ldt)) === {
      val set = new java.util.HashSet[java.lang.Long]()
      set.add(ntzNanos)
      FilterApi.in(col, set)
    })
  }

  test("filterOps eq/notEq/in tolerate a null value (IsNull / IsNotNull)") {
    val path = Array("c")
    val col = longColumn(path)
    val nullLong = null.asInstanceOf[java.lang.Long]
    // null value -> null Long comparand; used by ParquetFilters for IsNull / IsNotNull.
    Seq(TimestampNanosParquetOps.ltzFilterOps, TimestampNanosParquetOps.ntzFilterOps).foreach {
      ops =>
        assert(ops.makeEq(path, null) === FilterApi.eq(col, nullLong))
        assert(ops.makeNotEq(path, null) === FilterApi.notEq(col, nullLong))
        val set = new java.util.HashSet[java.lang.Long]()
        set.add(null)
        assert(ops.makeIn(path, Array[Any](null)) === FilterApi.in(col, set))
    }
  }

  test("ParquetTypeOps.filterOpsFor resolves each nanos encoding and nothing else") {
    // The LTZ and NTZ encodings differ only in isAdjustedToUTC; each resolves to its own ops.
    assert(ParquetTypeOps.filterOpsFor(
      LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS), INT64)
      .contains(TimestampNanosParquetOps.ltzFilterOps))
    assert(ParquetTypeOps.filterOpsFor(
      LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS), INT64)
      .contains(TimestampNanosParquetOps.ntzFilterOps))
    // MICROS unit, or an INT32 primitive, is not a nanos-timestamp encoding (pushdown falls
    // through to no framework ops).
    assert(ParquetTypeOps.filterOpsFor(
      LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS), INT64).isEmpty)
    assert(ParquetTypeOps.filterOpsFor(
      LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS), INT32).isEmpty)
  }

  // ---------- helpers ----------

  private def nanosField(isAdjustedToUTC: Boolean): Type =
    Types.primitive(INT64, REQUIRED)
      .as(LogicalTypeAnnotation.timestampType(isAdjustedToUTC, TimeUnit.NANOS))
      .named("c")

  // Builds the converter, feeds one INT64 epoch-nanos value through addLong, and returns the
  // value the converter set into its updater (the decoded TimestampNanosVal).
  private def decode(ops: TimestampNanosParquetOps, field: Type, epochNanos: Long): Any = {
    var captured: Any = null
    val updater = new ParentContainerUpdater {
      override def set(value: Any): Unit = captured = value
    }
    ops.newConverter(field, updater).asInstanceOf[PrimitiveConverter].addLong(epochNanos)
    captured
  }

  private def assertNanosTimestampSchema(
      parquetType: Type, expectedAdjustedToUTC: Boolean): Unit = {
    assert(parquetType.isPrimitive, s"expected a primitive type, got $parquetType")
    assert(parquetType.asPrimitiveType.getPrimitiveTypeName === INT64)
    parquetType.getLogicalTypeAnnotation match {
      case ts: TimestampLogicalTypeAnnotation =>
        assert(ts.getUnit === TimeUnit.NANOS)
        assert(ts.isAdjustedToUTC === expectedAdjustedToUTC)
      case other =>
        fail(s"expected a TIMESTAMP logical type annotation, got $other")
    }
  }
}
