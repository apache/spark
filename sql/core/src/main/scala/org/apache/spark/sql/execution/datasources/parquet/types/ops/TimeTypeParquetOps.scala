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
 * TimeType is a primitive Long-backed type stored in Parquet as INT64 with the
 * TIME(isAdjustedToUTC=false, unit=MICROS) logical type annotation.
 *
 * IMPORTANT - internal vs Parquet representation:
 *   - Spark internal: nanoseconds since midnight (Long)
 *   - Parquet storage: microseconds since midnight (INT64)
 *   - Write path: nanos -> micros (DateTimeUtils.nanosToMicros)
 *   - Read path: micros -> nanos (DateTimeUtils.microsToNanos)
 *
 * @param t the TimeType with precision information
 * @since 4.3.0
 */
case class TimeTypeParquetOps(t: TimeType) extends ParquetTypeOps {

  // ==================== Schema Conversion ====================

  override def convertToParquetType(
      fieldName: String, repetition: Repetition, inShredded: Boolean): Type =
    Types.primitive(INT64, repetition)
      .as(LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS))
      .named(fieldName)

  // ==================== Value Write ====================

  override def makeWriter(
      recordConsumer: () => RecordConsumer,
      makeFieldWriter: DataType => (SpecializedGetters, Int) => Unit
  ): (SpecializedGetters, Int) => Unit =
    // Evaluate the supplier at write time (not creation time) because recordConsumer
    // is null during init() and set later in prepareForWrite().
    (row: SpecializedGetters, ordinal: Int) =>
      recordConsumer().addLong(DateTimeUtils.nanosToMicros(row.getLong(ordinal)))

  // ==================== Row-Based Read ====================

  override def newConverter(
      parquetType: Type,
      updater: ParentContainerUpdater): Converter with HasParentContainerUpdater = {
    // Framework-first dispatch in ParquetRowConverter routes here whenever the
    // requested Spark type is TimeType, regardless of the actual Parquet encoding.
    // Without this guard, files whose column is raw INT64, INT64 TIME(NANOS),
    // INT64 TIMESTAMP(MICROS), INT32 TIME(MILLIS), etc. would silently decode as
    // microsToNanos(value) and produce wrong results. Mirrors the inline guard
    // that existed in ParquetRowConverter before the framework dispatch.
    TimeTypeParquetOps.requireCompatibleParquetType(t, parquetType)
    new ParquetPrimitiveConverter(updater) {
      override def addLong(value: Long): Unit = {
        this.updater.setLong(DateTimeUtils.microsToNanos(value))
      }
    }
  }

  // ==================== Vectorized Read ====================

  override def isBatchReadSupported(sqlConf: SQLConf): Boolean = true

  // Spark internal: nanos-of-day; Parquet storage: INT64 micros-of-day. The updater ignores
  // `descriptor` (the micros -> nanos conversion is the same for every TimeType precision).
  override def getVectorUpdater(descriptor: ColumnDescriptor): Option[ParquetVectorUpdater] =
    Some(new TimeMicrosToNanosUpdater)
}

private[ops] object TimeTypeParquetOps {

  /**
   * Validates that a Parquet field can be decoded as TimeType. TimeType is written
   * as INT64 with TIME(MICROS, isAdjustedToUTC=false). On read, any INT64 TIME(MICROS)
   * column is accepted regardless of the isAdjustedToUTC flag: Spark's zone-less TimeType
   * decodes the raw micros-of-day identically either way, matching the legacy
   * ParquetRowConverter guard (see SPARK-57416). Any other encoding (raw INT64, INT64
   * TIME(NANOS), INT32 TIME(MILLIS), INT64 TIMESTAMP(_), decimal-annotated, etc.) cannot
   * be decoded as TimeType - throw the same error as the legacy ParquetRowConverter path
   * so reads fail loudly instead of silently misinterpreting bytes.
   */
  private[ops] def requireCompatibleParquetType(
      sparkType: TimeType, parquetType: Type): Unit = {
    val ok = parquetType.isPrimitive &&
      parquetType.asPrimitiveType.getPrimitiveTypeName == INT64 &&
      (parquetType.getLogicalTypeAnnotation match {
        case t: LogicalTypeAnnotation.TimeLogicalTypeAnnotation =>
          // Accept both isAdjustedToUTC=false and =true. Spark's TimeType is zone-less
          // local time, so the UTC-adjustment flag carries no extra information on read:
          // the raw micros-of-day value decodes identically either way. Mirroring the
          // legacy ParquetRowConverter guard (which only checked the TIME annotation and
          // the MICROS unit) keeps the framework row-based read path consistent with both
          // the legacy row-based reader and the still-lenient vectorized reader. SPARK-57416.
          t.getUnit == TimeUnit.MICROS
        case _ => false
      })
    if (!ok) {
      throw QueryExecutionErrors.cannotCreateParquetConverterForDataTypeError(
        sparkType, parquetType.toString)
    }
  }
}

/**
 * Vectorized (batch) updater for TimeType: reads INT64 micros-of-day from Parquet and converts
 * to nanos-of-day (Spark's internal TimeType representation). Equivalent to the former
 * `ParquetVectorUpdaterFactory.LongAsNanosUpdater`, now owned by the type's ops.
 */
private[ops] class TimeMicrosToNanosUpdater extends ParquetVectorUpdater {
  override def readValues(
      total: Int,
      offset: Int,
      values: WritableColumnVector,
      valuesReader: VectorizedValuesReader): Unit = {
    valuesReader.readLongs(total, values, offset)
    var i = 0
    while (i < total) {
      values.putLong(offset + i, DateTimeUtils.microsToNanos(values.getLong(offset + i)))
      i += 1
    }
  }

  override def skipValues(total: Int, valuesReader: VectorizedValuesReader): Unit =
    valuesReader.skipLongs(total)

  override def readValue(
      offset: Int,
      values: WritableColumnVector,
      valuesReader: VectorizedValuesReader): Unit =
    values.putLong(offset, DateTimeUtils.microsToNanos(valuesReader.readLong()))

  override def decodeSingleDictionaryId(
      offset: Int,
      values: WritableColumnVector,
      dictionaryIds: WritableColumnVector,
      dictionary: Dictionary): Unit = {
    val micros = dictionary.decodeToLong(dictionaryIds.getDictId(offset))
    values.putLong(offset, DateTimeUtils.microsToNanos(micros))
  }
}
