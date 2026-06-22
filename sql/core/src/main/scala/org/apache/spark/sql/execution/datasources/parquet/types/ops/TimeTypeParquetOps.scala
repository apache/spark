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
      updater: ParentContainerUpdater): Converter with HasParentContainerUpdater = {
    // Framework-first dispatch in ParquetRowConverter routes here whenever the
    // requested Spark type is TimeType, regardless of the actual Parquet encoding.
    // Without this guard, files whose column is raw INT64, INT64 TIMESTAMP(MICROS),
    // INT32 TIME(MILLIS), etc. would silently decode as microsToNanos(value) and
    // produce wrong results.
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

  // The on-disk unit (MICROS vs NANOS) comes from the file column's annotation; the requested
  // precision comes from the Spark type. Mirrors the row-based newConverter path above.
  override def getVectorUpdater(descriptor: ColumnDescriptor): Option[ParquetVectorUpdater] = {
    val fileStoresNanos = TimeTypeParquetOps.isNanosTime(descriptor.getPrimitiveType)
    Some(new TimeVectorUpdater(t.precision, fileStoresNanos))
  }
}

private[ops] object TimeTypeParquetOps {

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

  // 10^k for k in [0, NANOS_PRECISION], indexed by (NANOS_PRECISION - precision); truncates a
  // nanos-of-day value to the requested fractional-second precision. Mirrors the former
  // ParquetVectorUpdaterFactory.TIME_TRUNCATION_FACTORS (the vectorized hot loop hoists it).
  private val timeTruncationFactors: Array[Long] = Array(
    1L, 10L, 100L, 1000L, 10000L, 100000L,
    1000000L, 10000000L, 100000000L, 1000000000L)

  private[ops] def timeTruncationFactor(precision: Int): Long =
    timeTruncationFactors(timeTruncationFactors.length - 1 - precision)

  /**
   * Validates that a Parquet field can be decoded as TimeType. TimeType is written as INT64 with
   * TIME(MICROS, isAdjustedToUTC=false) for precision 0..6 and TIME(NANOS, isAdjustedToUTC=false)
   * for precision 7..9. On read, any INT64 TIME(MICROS) or TIME(NANOS) column is accepted
   * regardless of the isAdjustedToUTC flag: Spark's zone-less TimeType decodes the raw
   * time-of-day identically either way, matching the legacy ParquetRowConverter guard (see
   * SPARK-57416). Any other encoding (raw INT64, INT32 TIME(MILLIS), INT64 TIMESTAMP(_),
   * decimal-annotated, etc.) cannot be decoded as TimeType - throw the same error as the legacy
   * ParquetRowConverter path so reads fail loudly instead of silently misinterpreting bytes.
   */
  private[ops] def requireCompatibleParquetType(
      sparkType: TimeType, parquetType: Type): Unit = {
    val ok = parquetType.isPrimitive &&
      parquetType.asPrimitiveType.getPrimitiveTypeName == INT64 &&
      (parquetType.getLogicalTypeAnnotation match {
        case t: LogicalTypeAnnotation.TimeLogicalTypeAnnotation =>
          // Accept both MICROS (precision 0..6) and NANOS (precision 7..9), and both
          // isAdjustedToUTC=false and =true. Spark's TimeType is zone-less local time, so the
          // UTC-adjustment flag carries no extra information on read: the raw time-of-day value
          // decodes identically either way. Mirroring the legacy ParquetRowConverter guard keeps
          // the framework row-based read path consistent with the legacy and vectorized readers.
          // SPARK-57416.
          t.getUnit == TimeUnit.MICROS || t.getUnit == TimeUnit.NANOS
        case _ => false
      })
    if (!ok) {
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
  // Precision is constant per column, so hoist the truncation factor instead of recomputing it
  // per value (this is the vectorized hot loop).
  private val truncationFactor: Long = TimeTypeParquetOps.timeTruncationFactor(precision)

  private def toTruncatedNanos(value: Long): Long = {
    val nanos = if (fileStoresNanos) value else DateTimeUtils.microsToNanos(value)
    (nanos / truncationFactor) * truncationFactor
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
