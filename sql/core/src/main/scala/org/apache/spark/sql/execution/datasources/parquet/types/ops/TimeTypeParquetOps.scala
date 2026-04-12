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

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
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
 *   - Filter path: LocalTime -> micros (MICRO_OF_DAY) - uses Parquet's representation,
 *     NOT Spark's internal nanos. Mismatch would cause filters to be off by 1000x.
 *
 * @param t the TimeType with precision information
 * @since 4.2.0
 */
case class TimeTypeParquetOps(t: TimeType) extends ParquetTypeOps {

  override def dataType: DataType = t

  // ==================== Schema Conversion ====================

  override def convertToParquetType(fieldName: String, repetition: Repetition): Type =
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
  ): org.apache.parquet.io.api.Converter with HasParentContainerUpdater =
    new ParquetPrimitiveConverter(updater) {
      override def addLong(value: Long): Unit = {
        this.updater.setLong(DateTimeUtils.microsToNanos(value))
      }
    }

  // ==================== Vectorized Read ====================

  override def isBatchReadSupported(sqlConf: SQLConf): Boolean = true

  override def getVectorUpdater(
      descriptor: ColumnDescriptor,
      annotation: LogicalTypeAnnotation): Option[ParquetVectorUpdater] =
    Some(new TimeTypeParquetOps.TimeMicrosToNanosUpdater)

  override def isLazyDecodingSupported(
      typeName: PrimitiveTypeName,
      annotation: LogicalTypeAnnotation): Boolean = false // needs micros->nanos conversion

  // ==================== Filter Pushdown ====================

  override def parquetFilterType: Option[(LogicalTypeAnnotation, PrimitiveTypeName)] =
    Some((LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS), INT64))

  override def makeFilterPredicate(
      op: ParquetFilterOp,
      columnPath: Array[String],
      value: Any): Option[FilterPredicate] =
    Some(ParquetTypeOps.makeLongFilter(
      op, columnPath, value, TimeTypeParquetOps.localTimeToMicros))

  override def makeInFilterPredicate(
      columnPath: Array[String],
      values: Array[Any]): Option[FilterPredicate] =
    Some(ParquetTypeOps.makeLongInFilter(
      columnPath, values, TimeTypeParquetOps.localTimeToMicros))

  override def isFilterableValue(value: Any): Boolean = value.isInstanceOf[LocalTime]
}

private[parquet] object TimeTypeParquetOps {

  /**
   * Converts a LocalTime to microseconds since midnight for Parquet filter comparison.
   * Uses MICRO_OF_DAY to match Parquet's TIME(MICROS) storage - NOT nanos.
   */
  private[ops] val localTimeToMicros: Any => JLong = (v: Any) =>
    JLong.valueOf(v.asInstanceOf[LocalTime].getLong(MICRO_OF_DAY))

  /**
   * Vectorized updater that reads INT64 micros from Parquet and converts to nanos for Spark.
   *
   * Reimplemented here because ParquetVectorUpdaterFactory.LongAsNanosUpdater is
   * private static in Java. Java package-private does not extend to sub-packages
   * (unlike Scala's private[parquet]), so the original class is inaccessible from
   * parquet.types.ops. The logic is identical.
   */
  private[ops] class TimeMicrosToNanosUpdater extends ParquetVectorUpdater {

    override def readValues(
        total: Int,
        offset: Int,
        values: WritableColumnVector,
        valuesReader: VectorizedValuesReader): Unit = {
      var i = 0
      while (i < total) {
        readValue(offset + i, values, valuesReader)
        i += 1
      }
    }

    override def skipValues(
        total: Int,
        valuesReader: VectorizedValuesReader): Unit = {
      valuesReader.skipLongs(total)
    }

    override def readValue(
        offset: Int,
        values: WritableColumnVector,
        valuesReader: VectorizedValuesReader): Unit = {
      values.putLong(offset, DateTimeUtils.microsToNanos(valuesReader.readLong()))
    }

    override def decodeSingleDictionaryId(
        offset: Int,
        values: WritableColumnVector,
        dictionaryIds: WritableColumnVector,
        dictionary: org.apache.parquet.column.Dictionary): Unit = {
      val micros = dictionary.decodeToLong(dictionaryIds.getDictId(offset))
      values.putLong(offset, DateTimeUtils.microsToNanos(micros))
    }
  }
}
