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

import org.apache.parquet.io.api.{Converter, RecordConsumer}
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.{TimestampLogicalTypeAnnotation, TimeUnit}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.parquet.{HasParentContainerUpdater, ParentContainerUpdater, ParquetPrimitiveConverter}
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Parquet operations shared by the nanosecond-precision timestamp types
 * ([[TimestampLTZNanosType]] / [[TimestampNTZNanosType]]).
 *
 * Both are primitive types stored in Parquet as INT64 with a TIMESTAMP(NANOS) annotation. The two
 * differ only in the `isAdjustedToUTC` flag (LTZ = true, NTZ = false) and in which row accessor /
 * overflow-error flavor the write path uses; the schema annotation unit and the entire read path
 * are identical, so they share this trait and supply the differences via the abstract members.
 *
 * IMPORTANT - internal vs Parquet representation:
 *   - Spark internal: [[TimestampNanosVal]] = (epochMicros: Long, nanosWithinMicro: Short in
 *     [0, 999])
 *   - Parquet storage: INT64 epoch-nanoseconds (signed), so the on-disk range is bounded to
 *     ~1677-09-21 .. 2262-04-11
 *   - Write path: (epochMicros, nanosWithinMicro) -> epochMicros * 1000 + nanosWithinMicro, via
 *     `DateTimeUtils.timestampNanosToEpochNanos` (exact arithmetic); out-of-range values throw
 *     `timestampNanosEpochNanosOverflowError`
 *   - Read path: epoch-nanos -> floorDiv / floorMod 1000 -> (epochMicros, nanosWithinMicro) (floor
 *     semantics keep `nanosWithinMicro` in [0, 999] for pre-epoch values), then the
 *     sub-microsecond digits are truncated to the requested precision
 *
 * TIMESTAMP(NANOS) postdates Spark's switch to the proleptic Gregorian calendar, so the values are
 * exempt from datetime rebasing (the rebase modes only cover DATE, TIMESTAMP_MILLIS and
 * TIMESTAMP_MICROS). Vectorized read is not supported: the value is a 16-byte composite rather
 * than a single long slot, so `isBatchReadSupported` stays false (the trait default) and reads go
 * through the row-based converter.
 *
 * @see ParquetTypeOps for the dispatch contract
 * @since 4.3.0
 */
private[parquet] trait TimestampNanosParquetOps extends ParquetTypeOps {

  /** The Spark type this ops handles, used for error messages. */
  protected def sparkType: DataType

  /** The requested fractional-second precision; sub-microsecond digits are truncated to it. */
  protected def precision: Int

  /** True for [[TimestampNTZNanosType]] (no time zone), false for [[TimestampLTZNanosType]]. */
  protected def isNtz: Boolean

  /** Reads the nanos value from the row using the type-specific accessor. */
  protected def getNanos(row: SpecializedGetters, ordinal: Int): TimestampNanosVal

  // The Parquet TIMESTAMP `isAdjustedToUTC` flag: LTZ is UTC-adjusted, NTZ is not.
  private def isAdjustedToUTC: Boolean = !isNtz

  // ==================== Schema Conversion ====================

  override def convertToParquetType(
      fieldName: String, repetition: Repetition, inShredded: Boolean): Type =
    Types.primitive(INT64, repetition)
      .as(LogicalTypeAnnotation.timestampType(isAdjustedToUTC, TimeUnit.NANOS))
      .named(fieldName)

  // ==================== Value Write ====================

  override def makeWriter(
      recordConsumer: () => RecordConsumer,
      makeFieldWriter: DataType => (SpecializedGetters, Int) => Unit
  ): (SpecializedGetters, Int) => Unit =
    // TIMESTAMP(NANOS) values are always proleptic Gregorian and are exempt from datetime
    // rebasing. The supplier is evaluated at write time (not creation time) because the
    // RecordConsumer is null during init() and set later in prepareForWrite().
    (row: SpecializedGetters, ordinal: Int) =>
      recordConsumer().addLong(
        TimestampNanosParquetOps.timestampNanosToEpochNanos(getNanos(row, ordinal), isNtz))

  // ==================== Row-Based Read ====================

  override def newConverter(
      parquetType: Type,
      updater: ParentContainerUpdater): Converter with HasParentContainerUpdater = {
    // Framework-first dispatch in ParquetRowConverter routes here for any nanos catalyst type,
    // regardless of the actual Parquet encoding. Only an INT64 TIMESTAMP(NANOS) column can be
    // decoded as a nanos timestamp; anything else (a non-NANOS timestamp, a foreign annotation,
    // etc.) must fail loudly, matching the legacy ParquetRowConverter behavior where the guarded
    // nanos arms fell through to the cannot-create-converter error.
    if (!TimestampNanosParquetOps.isNanosTimestamp(parquetType)) {
      throw QueryExecutionErrors.cannotCreateParquetConverterForDataTypeError(
        sparkType, parquetType.toString)
    }
    val p = precision
    new ParquetPrimitiveConverter(updater) {
      override def addLong(value: Long): Unit = {
        this.updater.set(DateTimeUtils.epochNanosToTimestampNanos(value, p))
      }
    }
  }
}

/**
 * Parquet operations for [[TimestampLTZNanosType]] (nanosecond precision, with time zone).
 * Stored as INT64 TIMESTAMP(NANOS, isAdjustedToUTC=true).
 *
 * @since 4.3.0
 */
case class TimestampLTZNanosParquetOps(t: TimestampLTZNanosType) extends TimestampNanosParquetOps {
  override protected def sparkType: DataType = t
  override protected def precision: Int = t.precision
  override protected def isNtz: Boolean = false
  override protected def getNanos(row: SpecializedGetters, ordinal: Int): TimestampNanosVal =
    row.getTimestampLTZNanos(ordinal)
}

/**
 * Parquet operations for [[TimestampNTZNanosType]] (nanosecond precision, without time zone).
 * Stored as INT64 TIMESTAMP(NANOS, isAdjustedToUTC=false).
 *
 * @since 4.3.0
 */
case class TimestampNTZNanosParquetOps(t: TimestampNTZNanosType) extends TimestampNanosParquetOps {
  override protected def sparkType: DataType = t
  override protected def precision: Int = t.precision
  override protected def isNtz: Boolean = true
  override protected def getNanos(row: SpecializedGetters, ordinal: Int): TimestampNanosVal =
    row.getTimestampNTZNanos(ordinal)
}

private[ops] object TimestampNanosParquetOps {

  /**
   * Whether the Parquet field is an INT64 TIMESTAMP(NANOS) column. The physical type is checked
   * (isPrimitive && INT64) in addition to the logical annotation so a malformed file that carries
   * a TIMESTAMP(NANOS) annotation on a non-INT64 physical type is rejected by the read guard with
   * the clean cannotCreateParquetConverterForDataTypeError rather than failing later in the
   * primitive converter. Mirrors TimeTypeParquetOps.requireCompatibleParquetType.
   */
  private[ops] def isNanosTimestamp(parquetType: Type): Boolean =
    parquetType.isPrimitive &&
      parquetType.asPrimitiveType.getPrimitiveTypeName == INT64 &&
      (parquetType.getLogicalTypeAnnotation match {
        case ts: TimestampLogicalTypeAnnotation => ts.getUnit == TimeUnit.NANOS
        case _ => false
      })

  /**
   * Combines the `(epochMicros, nanosWithinMicro)` pair into a single INT64 epoch-nanoseconds
   * value for Parquet storage. Delegates the exact-arithmetic packing to
   * [[DateTimeUtils.timestampNanosToEpochNanos]]; values outside the signed-int64 epoch-nanos
   * range (~1677-09-21 .. 2262-04-11) throw `timestampNanosEpochNanosOverflowError`.
   */
  private[ops] def timestampNanosToEpochNanos(value: TimestampNanosVal, isNtz: Boolean): Long = {
    try {
      DateTimeUtils.timestampNanosToEpochNanos(value)
    } catch {
      case _: ArithmeticException =>
        throw QueryExecutionErrors.timestampNanosEpochNanosOverflowError(
          value, isNtz, sink = "Parquet INT64")
    }
  }
}
