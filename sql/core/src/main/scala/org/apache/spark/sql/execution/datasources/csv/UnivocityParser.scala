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

package org.apache.spark.sql.execution.datasources.csv

import java.math.BigDecimal
import java.text.NumberFormat
import java.util.Locale

import scala.util.Try
import scala.util.control.NonFatal

import com.univocity.parsers.csv.CsvParser

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[csv] class UnivocityParser(
    schema: StructType,
    requiredSchema: StructType,
    options: CSVOptions) extends Logging {
  require(requiredSchema.toSet.subsetOf(schema.toSet),
    "requiredSchema should be the subset of schema.")

  def this(schema: StructType, options: CSVOptions) = this(schema, schema, options)

  // A `ValueConverter` is responsible for converting the given value to a desired type.
  private type ValueConverter = String => Any

  private val valueConverters =
    schema.map(f => makeConverter(f.name, f.dataType, f.nullable, options)).toArray

  private val parser = new CsvParser(options.asParserSettings)

  private var numMalformedRecords = 0

  private val row = new GenericInternalRow(requiredSchema.length)

  private val indexArr: Array[Int] = {
    val fields = if (options.dropMalformed) {
      // If `dropMalformed` is enabled, then it needs to parse all the values
      // so that we can decide which row is malformed.
      requiredSchema ++ schema.filterNot(requiredSchema.contains(_))
    } else {
      requiredSchema
    }
    fields.map(schema.indexOf(_: StructField)).toArray
  }

  /**
   * Create a converter which converts the string value to a value according to a desired type.
   * Currently, we do not support complex types (`ArrayType`, `MapType`, `StructType`).
   *
   * For other nullable types, returns null if it is null or equals to the value specified
   * in `nullValue` option.
   */
  def makeConverter(
      name: String,
      dataType: DataType,
      nullable: Boolean = true,
      options: CSVOptions): ValueConverter = dataType match {
    case _: ByteType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toByte)

    case _: ShortType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toShort)

    case _: IntegerType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toInt)

    case _: LongType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toLong)

    case _: FloatType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) {
        case options.nanValue => Float.NaN
        case options.negativeInf => Float.NegativeInfinity
        case options.positiveInf => Float.PositiveInfinity
        case datum =>
          Try(datum.toFloat)
            .getOrElse(NumberFormat.getInstance(Locale.US).parse(datum).floatValue())
      }

    case _: DoubleType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) {
        case options.nanValue => Double.NaN
        case options.negativeInf => Double.NegativeInfinity
        case options.positiveInf => Double.PositiveInfinity
        case datum =>
          Try(datum.toDouble)
            .getOrElse(NumberFormat.getInstance(Locale.US).parse(datum).doubleValue())
      }

    case _: BooleanType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toBoolean)

    case dt: DecimalType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        val value = new BigDecimal(datum.replaceAll(",", ""))
        Decimal(value, dt.precision, dt.scale)
      }

    case _: TimestampType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        // This one will lose microseconds parts.
        // See https://issues.apache.org/jira/browse/SPARK-10681.
        Try(options.timestampFormat.parse(datum).getTime * 1000L)
          .getOrElse {
          // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
          // compatibility.
          DateTimeUtils.stringToTime(datum).getTime * 1000L
        }
      }

    case _: DateType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        // This one will lose microseconds parts.
        // See https://issues.apache.org/jira/browse/SPARK-10681.x
        Try(DateTimeUtils.millisToDays(options.dateFormat.parse(datum).getTime))
          .getOrElse {
          // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
          // compatibility.
          DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(datum).getTime)
        }
      }

    case _: StringType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(UTF8String.fromString(_))

    case udt: UserDefinedType[_] => (datum: String) =>
      makeConverter(name, udt.sqlType, nullable, options)

    case _ => throw new RuntimeException(s"Unsupported type: ${dataType.typeName}")
  }

  private def nullSafeDatum(
       datum: String,
       name: String,
       nullable: Boolean,
       options: CSVOptions)(converter: ValueConverter): Any = {
    if (datum == options.nullValue || datum == null) {
      if (!nullable) {
        throw new RuntimeException(s"null value found but field $name is not nullable.")
      }
      null
    } else {
      converter.apply(datum)
    }
  }

  /**
   * Parses a single CSV record (in the form of an array of strings in which
   * each element represents a column) and turns it into either one resulting row or no row (if the
   * the record is malformed).
   */
  def parse(input: String): Option[InternalRow] = {
    convertWithParseMode(parser.parseLine(input)) { tokens =>
      var i: Int = 0
      while (i < indexArr.length) {
        val pos = indexArr(i)
        // It anyway needs to try to parse since it decides if this row is malformed
        // or not after trying to cast in `DROPMALFORMED` mode even if the casted
        // value is not stored in the row.
        val value = valueConverters(pos).apply(tokens(pos))
        if (i < requiredSchema.length) {
          row(i) = value
        }
        i += 1
      }
      row
    }
  }

  private def convertWithParseMode(
      tokens: Array[String])(convert: Array[String] => InternalRow): Option[InternalRow] = {
    if (options.dropMalformed && schema.length != tokens.length) {
      if (numMalformedRecords < options.maxMalformedLogPerPartition) {
        logWarning(s"Dropping malformed line: ${tokens.mkString(options.delimiter.toString)}")
      }
      if (numMalformedRecords == options.maxMalformedLogPerPartition - 1) {
        logWarning(
          s"More than ${options.maxMalformedLogPerPartition} malformed records have been " +
            "found on this partition. Malformed records from now on will not be logged.")
      }
      numMalformedRecords += 1
      None
    } else if (options.failFast && schema.length != tokens.length) {
      throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
        s"${tokens.mkString(options.delimiter.toString)}")
    } else {
      val checkedTokens = if (options.permissive && schema.length > tokens.length) {
        tokens ++ new Array[String](schema.length - tokens.length)
      } else if (options.permissive && schema.length < tokens.length) {
        tokens.take(schema.length)
      } else {
        tokens
      }

      try {
        Some(convert(checkedTokens))
      } catch {
        case NonFatal(e) if options.dropMalformed =>
          if (numMalformedRecords < options.maxMalformedLogPerPartition) {
            logWarning("Parse exception. " +
              s"Dropping malformed line: ${tokens.mkString(options.delimiter.toString)}")
          }
          if (numMalformedRecords == options.maxMalformedLogPerPartition - 1) {
            logWarning(
              s"More than ${options.maxMalformedLogPerPartition} malformed records have been " +
                "found on this partition. Malformed records from now on will not be logged.")
          }
          numMalformedRecords += 1
          None
      }
    }
  }
}
