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

import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.{TimestampLogicalTypeAnnotation, TimeUnit}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Type.Repetition.REQUIRED

import org.apache.spark.{SparkArithmeticException, SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.util.DateTimeConstants
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
    assert(TimestampNanosParquetOps.timestampNanosToEpochNanos(value, isNtz = false) ===
      1000000L * DateTimeConstants.NANOS_PER_MICROS + 500L)
  }

  test("timestampNanosToEpochNanos throws DATETIME_OVERFLOW outside the INT64 epoch-nanos range") {
    // Year ~5138; well past the int64 epoch-nanos cutoff (2262) but still renderable, so the
    // multiply overflows and the overflow error - not a rendering error - is what surfaces.
    val tooLarge = TimestampNanosVal.fromParts(100000000000000000L, 0.toShort)
    Seq(true, false).foreach { isNtz =>
      val ex = intercept[SparkArithmeticException] {
        TimestampNanosParquetOps.timestampNanosToEpochNanos(tooLarge, isNtz)
      }
      assert(ex.getCondition === "DATETIME_OVERFLOW")
    }
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
