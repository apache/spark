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

import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.parquet.{HasParentContainerUpdater, ParentContainerUpdater, ParquetPrimitiveConverter}
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

  override def dataType: DataType = t

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
      parquetType: org.apache.parquet.schema.Type,
      updater: ParentContainerUpdater
  ): org.apache.parquet.io.api.Converter with HasParentContainerUpdater = {
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
}

private[ops] object TimeTypeParquetOps {

  /**
   * Validates that a Parquet field can be decoded as TimeType. TimeType is stored
   * as INT64 with TIME(MICROS, isAdjustedToUTC=false). Any other encoding (raw
   * INT64, INT64 TIME(NANOS), INT32 TIME(MILLIS), INT64 TIMESTAMP(_), decimal-
   * annotated, etc.) cannot be decoded as TimeType - throw the same error as
   * the legacy ParquetRowConverter path so reads fail loudly instead of
   * silently misinterpreting bytes.
   */
  private[ops] def requireCompatibleParquetType(
      sparkType: TimeType, parquetType: Type): Unit = {
    val ok = parquetType.isPrimitive &&
      parquetType.asPrimitiveType.getPrimitiveTypeName == INT64 &&
      (parquetType.getLogicalTypeAnnotation match {
        case t: LogicalTypeAnnotation.TimeLogicalTypeAnnotation =>
          t.getUnit == TimeUnit.MICROS && !t.isAdjustedToUTC
        case _ => false
      })
    if (!ok) {
      throw QueryExecutionErrors.cannotCreateParquetConverterForDataTypeError(
        sparkType, parquetType.toString)
    }
  }
}
