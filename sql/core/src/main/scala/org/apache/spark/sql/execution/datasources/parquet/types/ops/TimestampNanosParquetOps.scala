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

import org.apache.parquet.column.{ColumnDescriptor, Dictionary}
import org.apache.parquet.io.api.{Converter, RecordConsumer}
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.{TimestampLogicalTypeAnnotation, TimeUnit}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.parquet.{HasParentContainerUpdater, ParentContainerUpdater, ParquetPrimitiveConverter, ParquetVectorUpdater, VectorizedValuesReader}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.internal.SQLConf
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
 * TIMESTAMP_MICROS). The vectorized reader decomposes the INT64 epoch-nanos into the two-child
 * column vector (epochMicros: Long, nanosWithinMicro: Short) via [[TimestampNanosVectorUpdater]],
 * routed through the types framework's `getVectorUpdater` hook.
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

  // ==================== Vectorized Read Support ====================

  override def isBatchReadSupported(sqlConf: SQLConf): Boolean = true

  // Only a canonical INT64 TIMESTAMP(NANOS) column can be vectorized-decoded as a nanos timestamp.
  // Return None for anything else so the factory falls through to its clean
  // SchemaColumnConvertNotSupportedException instead of silently mis-reading.
  override def getVectorUpdater(descriptor: ColumnDescriptor): Option[ParquetVectorUpdater] = {
    val parquetType = descriptor.getPrimitiveType
    if (TimestampNanosParquetOps.isNanosTimestamp(parquetType)) {
      Some(new TimestampNanosVectorUpdater(precision))
    } else {
      None
    }
  }

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
        DateTimeUtils.timestampNanosToEpochNanos(
          getNanos(row, ordinal), isNtz, sink = "Parquet INT64"))

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
}

/**
 * Vectorized (batch) updater for nanosecond-precision timestamps: reads an INT64 epoch-nanos
 * column and decomposes each value into the two-child column vector (epochMicros: Long,
 * nanosWithinMicro: Short). Sub-microsecond digits are truncated to the requested precision.
 * Mirrors the row-based `newConverter` path which calls
 * `DateTimeUtils.epochNanosToTimestampNanos(value, precision)`.
 *
 * No datetime rebase is applied (TIMESTAMP(NANOS) postdates the proleptic Gregorian switch).
 * No timezone conversion is applied at the storage level.
 *
 * Owned by the type's ops ([[TimestampNanosParquetOps]]) and routed through the types
 * framework's `getVectorUpdater` hook (no factory-level branch).
 */
private[ops] class TimestampNanosVectorUpdater(precision: Int) extends ParquetVectorUpdater {

  // Truncation uses the shared DateTimeUtils.truncateNanosWithinMicroToPrecision helper so the
  // vectorized path stays in lock-step with the row-based reader (epochNanosToTimestampNanos).
  // We decompose floorDiv/floorMod inline rather than calling epochNanosToTimestampNanos directly
  // because that method returns a heap-allocated TimestampNanosVal per value, which would add
  // GC pressure on the hot vectorized decode loop. The truncation itself is a pure Int->Int
  // operation with no allocation.

  private def putTimestampNanos(
      offset: Int, values: WritableColumnVector, epochNanos: Long): Unit = {
    val epochMicros = Math.floorDiv(epochNanos, 1000L)
    val rawNanosWithinMicro = Math.floorMod(epochNanos, 1000L).toInt
    val nanosWithinMicro =
      DateTimeUtils.truncateNanosWithinMicroToPrecision(rawNanosWithinMicro, precision).toShort
    values.getChild(0).putLong(offset, epochMicros)
    values.getChild(1).putShort(offset, nanosWithinMicro)
  }

  override def readValues(
      total: Int,
      offset: Int,
      values: WritableColumnVector,
      valuesReader: VectorizedValuesReader): Unit = {
    var i = 0
    while (i < total) {
      putTimestampNanos(offset + i, values, valuesReader.readLong())
      i += 1
    }
  }

  override def skipValues(total: Int, valuesReader: VectorizedValuesReader): Unit =
    valuesReader.skipLongs(total)

  override def readValue(
      offset: Int,
      values: WritableColumnVector,
      valuesReader: VectorizedValuesReader): Unit =
    putTimestampNanos(offset, values, valuesReader.readLong())

  override def decodeSingleDictionaryId(
      offset: Int,
      values: WritableColumnVector,
      dictionaryIds: WritableColumnVector,
      dictionary: Dictionary): Unit = {
    val epochNanos = dictionary.decodeToLong(dictionaryIds.getDictId(offset))
    putTimestampNanos(offset, values, epochNanos)
  }
}
