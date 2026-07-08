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

import java.lang.{Long => JLong}
import java.time.LocalTime
import java.time.temporal.ChronoField.MICRO_OF_DAY

import org.apache.parquet.column.{ColumnDescriptor, Dictionary}
import org.apache.parquet.io.api.{Converter, RecordConsumer}
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.parquet.{HasParentContainerUpdater, ParentContainerUpdater, ParquetPrimitiveConverter, ParquetVectorUpdater, VectorizedValuesReader}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Parquet operations for TimeType.
 *
 * TimeType is a primitive Long-backed type stored in Parquet as INT64 with a
 * TIME(isAdjustedToUTC=false) logical type annotation. The annotation unit depends on the
 * requested precision: precision 0..6 uses TIME(MICROS), precision 7..9 uses TIME(NANOS).
 *
 * IMPORTANT - internal vs Parquet representation:
 *   - Spark internal: nanoseconds since midnight (Long)
 *   - Parquet storage: microseconds (precision <= 6) or nanoseconds (precision >= 7) since midnight
 *   - Write path: nanos -> micros (DateTimeUtils.nanosToMicros) for TIME(MICROS); identity for
 *     TIME(NANOS)
 *   - Read path: micros -> nanos (DateTimeUtils.microsToNanos) for TIME(MICROS); identity for
 *     TIME(NANOS), then truncated to the requested precision
 *
 * @param t the TimeType with precision information
 * @since 4.3.0
 */
case class TimeTypeParquetOps(t: TimeType) extends ParquetTypeOps {

  // Precision 7..9 is stored as TIME(NANOS); precision 0..6 as TIME(MICROS).
  private def storesNanos: Boolean = t.precision > TimeType.MICROS_PRECISION

  // ==================== Schema Conversion ====================

  override def convertToParquetType(
      fieldName: String, repetition: Repetition, inShredded: Boolean): Type = {
    val unit = if (storesNanos) TimeUnit.NANOS else TimeUnit.MICROS
    Types.primitive(INT64, repetition)
      .as(LogicalTypeAnnotation.timeType(false, unit))
      .named(fieldName)
  }

  // ==================== Value Write ====================

  override def makeWriter(
      recordConsumer: () => RecordConsumer,
      makeFieldWriter: DataType => (SpecializedGetters, Int) => Unit
  ): (SpecializedGetters, Int) => Unit =
    // Evaluate the supplier at write time (not creation time) because recordConsumer
    // is null during init() and set later in prepareForWrite().
    if (storesNanos) {
      // Internal storage is already nanoseconds since midnight, so write it unchanged.
      (row: SpecializedGetters, ordinal: Int) =>
        recordConsumer().addLong(row.getLong(ordinal))
    } else {
      (row: SpecializedGetters, ordinal: Int) =>
        recordConsumer().addLong(DateTimeUtils.nanosToMicros(row.getLong(ordinal)))
    }

  // ==================== Row-Based Read ====================

  override def newConverter(
      parquetType: Type,
      updater: ParentContainerUpdater): Converter with HasParentContainerUpdater =
    newConverterInternal(parquetType, updater)

  private def newConverterInternal(
      parquetType: Type,
      updater: ParentContainerUpdater): Converter with HasParentContainerUpdater = {
    // Framework-first dispatch in ParquetRowConverter routes here whenever the
    // requested Spark type is TimeType, regardless of the actual Parquet encoding.
    // Without this guard, files whose column is raw INT64, INT64 TIME(NANOS),
    // INT64 TIMESTAMP(MICROS), INT32 TIME(MILLIS), etc. would silently decode as
    // microsToNanos(value) and produce wrong results. Mirrors the inline guard
    // that existed in ParquetRowConverter before the framework dispatch.
    TimeTypeParquetOps.requireCompatibleParquetType(t, parquetType)
    val fileStoresNanos = TimeTypeParquetOps.isNanosTime(parquetType)
    val precision = t.precision
    new ParquetPrimitiveConverter(updater) {
      override def addLong(value: Long): Unit = {
        val nanos = if (fileStoresNanos) value else DateTimeUtils.microsToNanos(value)
        this.updater.setLong(DateTimeUtils.truncateTimeToPrecision(nanos, precision))
      }
    }
  }

  // ==================== Vectorized Read ====================

  override def isBatchReadSupported(sqlConf: SQLConf): Boolean = true

  // Only a canonical INT64 TIME(MICROS)/TIME(NANOS) column can be vectorized-decoded as TimeType.
  // Return None for anything else (INT32 TIME(MILLIS), raw INT64, INT64 TIMESTAMP, ...) so the
  // factory falls through to its clean SchemaColumnConvertNotSupportedException instead of silently
  // mis-reading. This is the same compatibility check the row path uses
  // (requireCompatibleParquetType), unifying the read guard across both readers. The on-disk unit
  // (MICROS vs NANOS) and the requested precision drive the conversion + truncation.
  override def getVectorUpdater(descriptor: ColumnDescriptor): Option[ParquetVectorUpdater] = {
    val parquetType = descriptor.getPrimitiveType
    if (TimeTypeParquetOps.isCompatibleParquetType(parquetType)) {
      Some(new TimeVectorUpdater(t.precision, TimeTypeParquetOps.isNanosTime(parquetType)))
    } else {
      None
    }
  }

  // TIME decoding converts micros->nanos (for TIME(MICROS)) and truncates to the requested
  // precision per value in the updater; lazy dictionary decoding would attach the raw dictionary
  // and skip that processing, so it must be disabled. This matches the fail-safe trait default,
  // but is stated explicitly because TimeType opts into vectorized reads (isBatchReadSupported =
  // true) and the per-value work is exactly why lazy decoding is unsafe here.
  override def supportsLazyDictionaryDecoding(descriptor: ColumnDescriptor): Boolean = false
}

private[ops] object TimeTypeParquetOps {

  /**
   * Parquet filter-pushdown ops for TimeType, registered in [[ParquetTypeOps.filterOpsList]].
   * Filter dispatch is keyed on the file's on-disk encoding (not the Spark precision), so this
   * single instance targets only the MICROS encoding: TimeType is stored as INT64
   * TIME(MICROS, isAdjustedToUTC=false) for precision 0..6 and TIME(NANOS) for precision 7..9,
   * and only MICROS is pushed down here (filter values are java.time.LocalTime converted to
   * micros-of-day Longs). A TIME(NANOS) column resolves to no framework ops and falls through
   * to no pushdown. This matches the inline TimeType handling in ParquetFilters before filter
   * pushdown was routed through the framework, so pushdown behavior is unchanged.
   */
  private[ops] val filterOps: ParquetFilterOps = new LongParquetFilterOps {
    override val logicalTypeAnnotation: LogicalTypeAnnotation =
      LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS)

    override def acceptsValue(value: Any): Boolean = value.isInstanceOf[LocalTime]

    override protected def toLong(value: Any): JLong =
      value.asInstanceOf[LocalTime].getLong(MICRO_OF_DAY)
  }

  /**
   * Whether the Parquet field is an INT64 TIME(NANOS) column. The isAdjustedToUTC flag is
   * intentionally ignored: Spark's TimeType is zone-less, so a TIME(NANOS) value decodes to the
   * same nanos-of-day regardless of the flag (consistent with requireCompatibleParquetType and
   * the legacy reader; see SPARK-57416).
   */
  private[ops] def isNanosTime(parquetType: Type): Boolean =
    parquetType.getLogicalTypeAnnotation match {
      case t: LogicalTypeAnnotation.TimeLogicalTypeAnnotation =>
        t.getUnit == TimeUnit.NANOS
      case _ => false
    }

  /**
   * Whether a Parquet field is a canonical INT64 TIME(MICROS) (precision 0..6) or TIME(NANOS)
   * (precision 7..9) column - the only encodings Spark can decode as TimeType. The isAdjustedToUTC
   * flag is intentionally ignored: Spark's TimeType is zone-less local time, so the raw
   * time-of-day decodes identically either way, matching the legacy ParquetRowConverter guard
   * (see SPARK-57416). Any other encoding (raw INT64, INT32 TIME(MILLIS), INT64 TIMESTAMP(_),
   * decimal-annotated, etc.) is incompatible. Shared by both the row-based and vectorized read
   * paths so they accept/reject the same set.
   */
  private[ops] def isCompatibleParquetType(parquetType: Type): Boolean =
    parquetType.isPrimitive &&
      parquetType.asPrimitiveType.getPrimitiveTypeName == INT64 &&
      (parquetType.getLogicalTypeAnnotation match {
        case t: LogicalTypeAnnotation.TimeLogicalTypeAnnotation =>
          t.getUnit == TimeUnit.MICROS || t.getUnit == TimeUnit.NANOS
        case _ => false
      })

  /**
   * Validates that a Parquet field can be decoded as TimeType on the row-based path, throwing the
   * same error as the legacy ParquetRowConverter so incompatible reads fail loudly instead of
   * silently misinterpreting bytes. See [[isCompatibleParquetType]] for the accepted encodings.
   */
  private[ops] def requireCompatibleParquetType(
      sparkType: TimeType, parquetType: Type): Unit = {
    if (!isCompatibleParquetType(parquetType)) {
      throw QueryExecutionErrors.cannotCreateParquetConverterForDataTypeError(
        sparkType, parquetType.toString)
    }
  }
}

/**
 * Vectorized (batch) updater for TimeType: reads an INT64 TIME column into the internal
 * nanos-of-day representation and truncates it to the requested precision. `fileStoresNanos`
 * selects the on-disk unit - TIME(NANOS) stores nanos (identity), TIME(MICROS) stores micros
 * (converted to nanos). Mirrors the row-based newConverter path so the vectorized and row-based
 * readers agree; replaces the former `ParquetVectorUpdaterFactory.TimeUpdater`, now owned by the
 * type's ops.
 */
private[ops] class TimeVectorUpdater(precision: Int, fileStoresNanos: Boolean)
    extends ParquetVectorUpdater {
  private def toTruncatedNanos(value: Long): Long = {
    val nanos = if (fileStoresNanos) value else DateTimeUtils.microsToNanos(value)
    // Same conversion + truncation as the row-based newConverter path, via the shared
    // (table-backed) DateTimeUtils.truncateTimeToPrecision, so both readers stay in lock-step.
    DateTimeUtils.truncateTimeToPrecision(nanos, precision)
  }

  override def readValues(
      total: Int,
      offset: Int,
      values: WritableColumnVector,
      valuesReader: VectorizedValuesReader): Unit = {
    valuesReader.readLongs(total, values, offset)
    var i = 0
    while (i < total) {
      values.putLong(offset + i, toTruncatedNanos(values.getLong(offset + i)))
      i += 1
    }
  }

  override def skipValues(total: Int, valuesReader: VectorizedValuesReader): Unit =
    valuesReader.skipLongs(total)

  override def readValue(
      offset: Int,
      values: WritableColumnVector,
      valuesReader: VectorizedValuesReader): Unit =
    values.putLong(offset, toTruncatedNanos(valuesReader.readLong()))

  override def decodeSingleDictionaryId(
      offset: Int,
      values: WritableColumnVector,
      dictionaryIds: WritableColumnVector,
      dictionary: Dictionary): Unit = {
    val value = dictionary.decodeToLong(dictionaryIds.getDictId(offset))
    values.putLong(offset, toTruncatedNanos(value))
  }
}
