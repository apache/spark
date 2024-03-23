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

package org.apache.spark.sql.catalyst.csv

import java.io.Writer

import com.univocity.parsers.csv.CsvWriter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, IntervalStringStyles, IntervalUtils, SparkStringUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

class UnivocityGenerator(
    schema: StructType,
    writer: Writer,
    options: CSVOptions) {
  private val writerSettings = options.asWriterSettings
  writerSettings.setHeaders(schema.fieldNames: _*)
  private val gen = new CsvWriter(writer, writerSettings)

  // A `ValueConverter` is responsible for converting a value of an `SpecializedGetters`
  // to `String`. When the value is null, this converter should not be called.
  private type ValueConverter = (SpecializedGetters, Int) => String

  // `ValueConverter`s for all values in the fields of the schema
  private val valueConverters: Array[ValueConverter] =
    schema.map(_.dataType).map(makeConverter).toArray

  private val timestampFormatter = TimestampFormatter(
    options.timestampFormatInWrite,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)
  private val timestampNTZFormatter = TimestampFormatter(
    options.timestampNTZFormatInWrite,
    options.zoneId,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false,
    forTimestampNTZ = true)
  private val dateFormatter = DateFormatter(
    options.dateFormatInWrite,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)
  private val nullAsQuotedEmptyString =
    SQLConf.get.getConf(SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV)

  private def makeConverter(dataType: DataType): ValueConverter = dataType match {
    case BinaryType =>
      (getter, ordinal) => SparkStringUtils.getHexString(getter.getBinary(ordinal))

    case DateType =>
      (getter, ordinal) => dateFormatter.format(getter.getInt(ordinal))

    case TimestampType =>
      (getter, ordinal) => timestampFormatter.format(getter.getLong(ordinal))

    case TimestampNTZType =>
      (getter, ordinal) =>
        timestampNTZFormatter.format(DateTimeUtils.microsToLocalDateTime(getter.getLong(ordinal)))

    case YearMonthIntervalType(start, end) =>
      (getter, ordinal) =>
        IntervalUtils.toYearMonthIntervalString(
          getter.getInt(ordinal), IntervalStringStyles.ANSI_STYLE, start, end)

    case DayTimeIntervalType(start, end) =>
      (getter, ordinal) =>
        IntervalUtils.toDayTimeIntervalString(
          getter.getLong(ordinal), IntervalStringStyles.ANSI_STYLE, start, end)

    case udt: UserDefinedType[_] => makeConverter(udt.sqlType)

    case ArrayType(et, _) =>
      val elementConverter = makeConverter(et)
      (getter, ordinal) =>
        val array = getter.getArray(ordinal)
        val builder = new StringBuilder
        builder.append("[")
        if (array.numElements() > 0) {
          if (array.isNullAt(0)) {
            appendNull(builder)
          } else {
            builder.append(elementConverter(array, 0))
          }
          var i = 1
          while (i < array.numElements()) {
            builder.append(",")
            if (array.isNullAt(i)) {
              appendNull(builder)
            } else {
              builder.append(" " + elementConverter(array, i))
            }
            i += 1
          }
        }
        builder.append("]")
        builder.toString()

    case MapType(kt, vt, _) =>
      val keyConverter = makeConverter(kt)
      val valueConverter = makeConverter(vt)
      (getter, ordinal) =>
        val map = getter.getMap(ordinal)
        val builder = new StringBuilder
        builder.append("{")
        if (map.numElements() > 0) {
          val keyArray = map.keyArray()
          val valueArray = map.valueArray()
          builder.append(keyConverter(keyArray, 0))
          builder.append(" ->")
          if (valueArray.isNullAt(0)) {
            appendNull(builder)
          } else {
            builder.append(" " + valueConverter(valueArray, 0))
          }
          var i = 1
          while (i < map.numElements()) {
            builder.append(", ")
            builder.append(keyConverter(keyArray, i))
            builder.append(" ->")
            if (valueArray.isNullAt(i)) {
              appendNull(builder)
            } else {
              builder.append(" " + valueConverter(valueArray, i))
            }
            i += 1
          }
        }
        builder.append("}")
        builder.toString()

    case StructType(fields) =>
      val converters = fields.map(_.dataType).map(makeConverter)
      (getter, ordinal) =>
        val row = getter.getStruct(ordinal, fields.length)
        val builder = new StringBuilder
        builder.append("{")
        if (row.numFields > 0) {
          if (row.isNullAt(0)) {
            appendNull(builder)
          } else {
            builder.append(converters(0)(row, 0))
          }
          var i = 1
          while (i < row.numFields) {
            builder.append(",")
            if (row.isNullAt(i)) {
              appendNull(builder)
            } else {
              builder.append(" " + converters(i)(row, i))
            }
            i += 1
          }
        }
        builder.append("}")
        builder.toString()

    case dt: DataType =>
      (getter, ordinal) => getter.get(ordinal, dt).toString
  }

  private def appendNull(builder: StringBuilder): Unit = {
    options.parameters.get(CSVOptions.NULL_VALUE) match {
      case Some(_) => builder.append(" " + options.nullValue)
      case None =>
    }
  }

  private def convertRow(row: InternalRow): Seq[String] = {
    var i = 0
    val values = new Array[String](row.numFields)
    while (i < row.numFields) {
      if (!row.isNullAt(i)) {
        values(i) = valueConverters(i).apply(row, i)
      } else if (nullAsQuotedEmptyString) {
        values(i) = options.nullValue
      }
      i += 1
    }
    values.toImmutableArraySeq
  }

  def writeHeaders(): Unit = {
    gen.writeHeaders()
  }

  /**
   * Writes a single InternalRow to CSV using Univocity.
   */
  def write(row: InternalRow): Unit = {
    gen.writeRow(convertRow(row): _*)
  }

  def writeToString(row: InternalRow): String = {
    gen.writeRowToString(convertRow(row): _*)
  }

  def close(): Unit = gen.close()

  def flush(): Unit = gen.flush()
}
