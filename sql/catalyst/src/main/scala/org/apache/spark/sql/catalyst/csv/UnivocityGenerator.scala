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
import org.apache.spark.sql.catalyst.util.{ArrayData, DateFormatter, DateTimeUtils, IntervalStringStyles, IntervalUtils, MapData, SparkStringUtils, TimestampFormatter}
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

  // A `ValueConverter` is responsible for converting a value of an `Any` to `String`.
  // When the value is null, this converter should not be called.
  private type ValueConverter = Any => String

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

  // Makes the function accept Any type input by doing `asInstanceOf[T]`.
  @inline private def acceptAny[T](func: T => String): ValueConverter =
    i => func(i.asInstanceOf[T])

  private def makeConverter(dataType: DataType): ValueConverter = dataType match {
    case BinaryType =>
      acceptAny[Array[Byte]](binary => SparkStringUtils.getHexString(binary))
    case DateType =>
      acceptAny[Int](d => dateFormatter.format(d))
    case TimestampType =>
      acceptAny[Long](t => timestampFormatter.format(t))
    case TimestampNTZType =>
      acceptAny[Long](t => timestampNTZFormatter.format(DateTimeUtils.microsToLocalDateTime(t)))
    case YearMonthIntervalType(start, end) =>
      acceptAny[Int](i => IntervalUtils.toYearMonthIntervalString(
        i, IntervalStringStyles.ANSI_STYLE, start, end))
    case DayTimeIntervalType(start, end) =>
      acceptAny[Long](i => IntervalUtils.toDayTimeIntervalString(
        i, IntervalStringStyles.ANSI_STYLE, start, end))
    case udt: UserDefinedType[_] => makeConverter(udt.sqlType)
    case ArrayType(et, _) =>
      acceptAny[ArrayData](array => {
        val builder = new StringBuilder
        builder.append("[")
        if (array.numElements() > 0) {
          val converter = makeConverter(et)
          if (array.isNullAt(0)) {
            if (nullAsQuotedEmptyString) {
              builder.append(options.nullValue)
            } else {
              builder.append(null.asInstanceOf[String])
            }
          } else {
            builder.append(converter(array.get(0, et)))
          }
          var i = 1
          while (i < array.numElements()) {
            builder.append(",")
            if (array.isNullAt(i)) {
              if (nullAsQuotedEmptyString) {
                builder.append(" " + options.nullValue)
              } else {
                builder.append(" " + null.asInstanceOf[String])
              }
            } else {
              builder.append(" ")
              builder.append(converter(array.get(i, et)))
            }
            i += 1
          }
        }
        builder.append("]")
        builder.toString()
      })
    case MapType(kt, vt, _) =>
      acceptAny[MapData](map => {
        val builder = new StringBuilder
        builder.append("{")
        if (map.numElements() > 0) {
          val keyArray = map.keyArray()
          val valueArray = map.valueArray()
          val keyConverter = makeConverter(kt)
          val valueConverter = makeConverter(vt)
          builder.append(keyConverter(keyArray.get(0, kt)))
          builder.append(" ->")
          if (valueArray.isNullAt(0)) {
            if (nullAsQuotedEmptyString) {
              builder.append(" " + options.nullValue)
            } else {
              builder.append(" " + null.asInstanceOf[String])
            }
          } else {
            builder.append(" ")
            builder.append(valueConverter(valueArray.get(0, vt)))
          }
          var i = 1
          while (i < map.numElements()) {
            builder.append(", ")
            builder.append(keyConverter(keyArray.get(i, kt)))
            builder.append(" ->")
            if (valueArray.isNullAt(i)) {
              if (nullAsQuotedEmptyString) {
                builder.append(" " + options.nullValue)
              } else {
                builder.append(" " + null.asInstanceOf[String])
              }
            } else {
              builder.append(" ")
              builder.append(valueConverter(valueArray.get(i, vt)))
            }
            i += 1
          }
        }
        builder.append("}")
        builder.toString()
      })
    case StructType(fields) =>
      acceptAny[InternalRow](row => {
        val builder = new StringBuilder
        builder.append("{")
        if (row.numFields > 0) {
          val st = fields.map(_.dataType)
          val converters = st.map(makeConverter)
          if (row.isNullAt(0)) {
            if (nullAsQuotedEmptyString) {
              builder.append(options.nullValue)
            } else {
              builder.append(null.asInstanceOf[String])
            }
          } else {
            builder.append(converters(0)(row.get(0, st(0))))
          }
          var i = 1
          while (i < row.numFields) {
            builder.append(",")
            if (row.isNullAt(i)) {
              if (nullAsQuotedEmptyString) {
                builder.append(" " + options.nullValue)
              } else {
                builder.append(" " + null.asInstanceOf[String])
              }
            } else {
              builder.append(" ")
              builder.append(converters(i)(row.get(i, st(i))))
            }
            i += 1
          }
        }
        builder.append("}")
        builder.toString()
      })
    case _: DataType =>
      value => value.toString
  }

  private def convertRow(row: InternalRow): Seq[String] = {
    var i = 0
    val values = new Array[String](row.numFields)
    while (i < row.numFields) {
      if (!row.isNullAt(i)) {
        values(i) = valueConverters(i).apply(row.get(i, schema(i).dataType))
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
