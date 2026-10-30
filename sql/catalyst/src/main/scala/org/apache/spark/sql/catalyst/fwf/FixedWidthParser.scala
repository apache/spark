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

package org.apache.spark.sql.catalyst.fwf

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, OrderedFilters, StructFilters}
import org.apache.spark.sql.catalyst.expressions.{ExprUtils, GenericInternalRow}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.errors.{ExecutionErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class FixedWidthParser(
    dataSchema: StructType,
    requiredSchema: StructType,
    options: FixedWidthOptions,
    filters: Seq[Filter] = Seq.empty) {
  require(requiredSchema.toSet.subsetOf(dataSchema.toSet),
    s"requiredSchema (${requiredSchema.catalogString}) should be the subset of " +
      s"dataSchema (${dataSchema.catalogString}).")

  private val fwfFilters: StructFilters = if (SQLConf.get.fwfFilterPushDown) {
    new OrderedFilters(filters, requiredSchema)
  } else {
    new NoopFilters
  }

  private type ValueConverter = String => Any

  private val columnPruning: Boolean = requiredSchema.length < dataSchema.length
  private val parsedSchema: StructType = if (columnPruning) requiredSchema else dataSchema

  private val tokenIndexArr = requiredSchema.map(f => dataSchema.indexOf(f)).toArray

  private val getToken: (Array[String], Int) => String = if (columnPruning) {
    (tokens, index) => tokens(index)
  } else {
    (tokens, index) => tokens(tokenIndexArr(index))
  }

  private lazy val dateFormatter = DateFormatter(
    options.dateFormatOption,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)
  private lazy val timestampFormatter = TimestampFormatter(
    options.timestampFormatOption,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)
  private lazy val timestampNTZFormatter = TimestampFormatter(
    options.timestampFormatOption,
    options.zoneId,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true,
    forTimestampNTZ = true)

  private val decimalParser = ExprUtils.getDecimalParser(options.locale)

  private val valueConverters: Array[ValueConverter] =
    requiredSchema.map(f => makeConverter(f.name, f.dataType, f.nullable)).toArray

  def makeConverter(name: String, dataType: DataType, nullable: Boolean = true): ValueConverter =
    dataType match {
      case _: ByteType => (d: String) => nullSafeDatum(d, name, nullable)(_.toByte)
      case _: ShortType => (d: String) => nullSafeDatum(d, name, nullable)(_.toShort)
      case _: IntegerType => (d: String) => nullSafeDatum(d, name, nullable)(_.toInt)
      case _: LongType => (d: String) => nullSafeDatum(d, name, nullable)(_.toLong)
      case _: FloatType => (d: String) => nullSafeDatum(d, name, nullable)(_.toFloat)
      case _: DoubleType => (d: String) => nullSafeDatum(d, name, nullable)(_.toDouble)
      case _: BooleanType => (d: String) => nullSafeDatum(d, name, nullable)(_.toBoolean)

      case dt: DecimalType => (d: String) =>
        nullSafeDatum(d, name, nullable) { datum =>
          Decimal(decimalParser(datum), dt.precision, dt.scale)
        }

      case _: DateType => (d: String) =>
        nullSafeDatum(d, name, nullable)(dateFormatter.parse)

      case _: TimestampType => (d: String) =>
        nullSafeDatum(d, name, nullable)(timestampFormatter.parse)

      case _: TimestampNTZType => (d: String) =>
        nullSafeDatum(d, name, nullable) { datum =>
          timestampNTZFormatter.parseWithoutTimeZone(datum, false)
        }

      case _: StringType => (d: String) =>
        nullSafeDatum(d, name, nullable)(UTF8String.fromString)

      case _: BinaryType => (d: String) =>
        nullSafeDatum(d, name, nullable)(_.getBytes)

      case udt: UserDefinedType[_] => makeConverter(name, udt.sqlType, nullable)

      case _ => throw ExecutionErrors.unsupportedDataTypeError(dataType)
    }

  private def nullSafeDatum(
      datum: String,
      name: String,
      nullable: Boolean)(converter: ValueConverter): Any = {
    if (datum == options.nullValue || datum == null) {
      if (!nullable) {
        throw QueryExecutionErrors.foundNullValueForNotNullableFieldError(name)
      }
      null
    } else {
      converter.apply(datum)
    }
  }

  def parse(tokens: Array[String]): Option[InternalRow] = {
    var badRecordException: Option[Throwable] = if (tokens.length != parsedSchema.length) {
      Some(LazyBadRecordCauseWrapper(
        () => QueryExecutionErrors.malformedFwfRecordError(tokens.mkString(" "))))
    } else {
      None
    }

    val row = new GenericInternalRow(requiredSchema.length)
    var i = 0
    var skipRow = false
    while (i < requiredSchema.length) {
      try {
        if (skipRow) {
          row.setNullAt(i)
        } else {
          val tokenIdx = if (columnPruning) i else tokenIndexArr(i)
          val token = if (tokenIdx >= 0 && tokenIdx < tokens.length) getToken(tokens, i) else null
          row(i) = valueConverters(i).apply(token)
          if (fwfFilters.skipRow(row, i)) {
            skipRow = true
          }
        }
      } catch {
        case NonFatal(e) =>
          badRecordException = badRecordException.orElse(Some(e))
          row.setNullAt(i)
      }
      i += 1
    }

    if (skipRow) {
      None
    } else if (badRecordException.isDefined) {
      throw BadRecordException(
        () => UTF8String.fromString(tokens.mkString(" ")),
        () => Array(row),
        badRecordException.get)
    } else {
      Some(row)
    }
  }
}
