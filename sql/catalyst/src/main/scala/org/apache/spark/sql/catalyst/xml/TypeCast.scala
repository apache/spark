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
package org.apache.spark.sql.catalyst.xml

import java.math.BigDecimal
import java.text.NumberFormat
import java.util.Locale

import scala.util.Try
import scala.util.control.Exception._
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Utility functions for type casting
 */
private[sql] object TypeCast {

  /**
   * Casts given string datum to specified type.
   * Currently we do not support complex types (ArrayType, MapType, StructType).
   *
   * For string types, this is simply the datum. For other types.
   * For other nullable types, this is null if the string datum is empty.
   *
   * @param datum string value
   * @param castType SparkSQL type
   */
  private[sql] def castTo(
      datum: String,
      castType: DataType,
      options: XmlOptions): Any = {
    if ((datum == options.nullValue) ||
        (options.treatEmptyValuesAsNulls && datum == "")) {
      null
    } else {
      castType match {
        case _: ByteType => datum.toByte
        case _: ShortType => datum.toShort
        case _: IntegerType => datum.toInt
        case _: LongType => datum.toLong
        case _: FloatType => Try(datum.toFloat)
          .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).floatValue())
        case _: DoubleType => Try(datum.toDouble)
          .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).doubleValue())
        case _: BooleanType => parseXmlBoolean(datum)
        case dt: DecimalType =>
          Decimal(new BigDecimal(datum.replaceAll(",", "")), dt.precision, dt.scale)
        case _: TimestampType => parseXmlTimestamp(datum, options)
        case _: DateType => parseXmlDate(datum, options)
        case _: StringType => UTF8String.fromString(datum)
        case _ => throw new IllegalArgumentException(s"Unsupported type: ${castType.typeName}")
      }
    }
  }

  private def parseXmlBoolean(s: String): Boolean = {
    s.toLowerCase(Locale.ROOT) match {
      case "true" | "1" => true
      case "false" | "0" => false
      case _ => throw new IllegalArgumentException(s"For input string: $s")
    }
  }

  private def parseXmlDate(value: String, options: XmlOptions): Int = {
    options.dateFormatter.parse(value)
  }

  private def parseXmlTimestamp(value: String, options: XmlOptions): Long = {
    try {
      options.timestampFormatter.parse(value)
    } catch {
      case NonFatal(e) =>
        // If fails to parse, then tries the way used in 2.0 and 1.x for backwards
        // compatibility if enabled.
        val enableParsingFallbackForTimestampType =
          options.enableDateTimeParsingFallback
            .orElse(SQLConf.get.jsonEnableDateTimeParsingFallback)
            .getOrElse {
              SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY ||
                options.timestampFormatInRead.isEmpty
            }
        if (!enableParsingFallbackForTimestampType) {
          throw e
        }
        val str = DateTimeUtils.cleanLegacyTimestampStr(UTF8String.fromString(value))
        DateTimeUtils.stringToTimestamp(str, options.zoneId).getOrElse(throw e)
    }
  }

  // TODO: This function unnecessarily does type dispatch. Should merge it with `castTo`.
  private[sql] def convertTo(
      datum: String,
      dataType: DataType,
      options: XmlOptions): Any = {
    val value = if (datum != null && options.ignoreSurroundingSpaces) {
      datum.trim()
    } else {
      datum
    }
    if ((value == options.nullValue) ||
      (options.treatEmptyValuesAsNulls && value == "")) {
      null
    } else {
      dataType match {
        case NullType => castTo(value, StringType, options)
        case LongType => signSafeToLong(value, options)
        case DoubleType => signSafeToDouble(value, options)
        case BooleanType => castTo(value, BooleanType, options)
        case StringType => castTo(value, StringType, options)
        case DateType => castTo(value, DateType, options)
        case TimestampType => castTo(value, TimestampType, options)
        case FloatType => signSafeToFloat(value, options)
        case ByteType => castTo(value, ByteType, options)
        case ShortType => castTo(value, ShortType, options)
        case IntegerType => signSafeToInt(value, options)
        case dt: DecimalType => castTo(value, dt, options)
        case _ => throw new IllegalArgumentException(
          s"Failed to parse a value for data type $dataType.")
      }
    }
  }

  /**
   * Helper method that checks and cast string representation of a numeric types.
   */
  private[sql] def isBoolean(value: String): Boolean = {
    value.toLowerCase(Locale.ROOT) match {
      case "true" | "false" => true
      case _ => false
    }
  }

  private[sql] def isDouble(value: String): Boolean = {
    val signSafeValue = if (value.startsWith("+") || value.startsWith("-")) {
      value.substring(1)
    } else {
      value
    }
    // A little shortcut to avoid trying many formatters in the common case that
    // the input isn't a double. All built-in formats will start with a digit or period.
    if (signSafeValue.isEmpty ||
      !(Character.isDigit(signSafeValue.head) || signSafeValue.head == '.')) {
      return false
    }
    // Rule out strings ending in D or F, as they will parse as double but should be disallowed
    if (value.nonEmpty && (value.last match {
          case 'd' | 'D' | 'f' | 'F' => true
          case _ => false
        })) {
      return false
    }
    (allCatch opt signSafeValue.toDouble).isDefined
  }

  private[sql] def isInteger(value: String): Boolean = {
    val signSafeValue = if (value.startsWith("+") || value.startsWith("-")) {
      value.substring(1)
    } else {
      value
    }
    // A little shortcut to avoid trying many formatters in the common case that
    // the input isn't a number. All built-in formats will start with a digit.
    if (signSafeValue.isEmpty || !Character.isDigit(signSafeValue.head)) {
      return false
    }
    (allCatch opt signSafeValue.toInt).isDefined
  }

  private[sql] def isLong(value: String): Boolean = {
    val signSafeValue = if (value.startsWith("+") || value.startsWith("-")) {
      value.substring(1)
    } else {
      value
    }
    // A little shortcut to avoid trying many formatters in the common case that
    // the input isn't a number. All built-in formats will start with a digit.
    if (signSafeValue.isEmpty || !Character.isDigit(signSafeValue.head)) {
      return false
    }
    (allCatch opt signSafeValue.toLong).isDefined
  }

  private[sql] def isTimestamp(value: String, options: XmlOptions): Boolean = {
    try {
      options.timestampFormatter.parseOptional(value).isDefined
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  private[sql] def isDate(value: String, options: XmlOptions): Boolean = {
    (allCatch opt options.dateFormatter.parse(value)).isDefined
  }

  private[sql] def signSafeToLong(value: String, options: XmlOptions): Long = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      TypeCast.castTo(data, LongType, options).asInstanceOf[Long]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -TypeCast.castTo(data, LongType, options).asInstanceOf[Long]
    } else {
      val data = value
      TypeCast.castTo(data, LongType, options).asInstanceOf[Long]
    }
  }

  private[sql] def signSafeToDouble(value: String, options: XmlOptions): Double = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      TypeCast.castTo(data, DoubleType, options).asInstanceOf[Double]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
     -TypeCast.castTo(data, DoubleType, options).asInstanceOf[Double]
    } else {
      val data = value
      TypeCast.castTo(data, DoubleType, options).asInstanceOf[Double]
    }
  }

  private[sql] def signSafeToInt(value: String, options: XmlOptions): Int = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      TypeCast.castTo(data, IntegerType, options).asInstanceOf[Int]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -TypeCast.castTo(data, IntegerType, options).asInstanceOf[Int]
    } else {
      val data = value
      TypeCast.castTo(data, IntegerType, options).asInstanceOf[Int]
    }
  }

  private[sql] def signSafeToFloat(value: String, options: XmlOptions): Float = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      TypeCast.castTo(data, FloatType, options).asInstanceOf[Float]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -TypeCast.castTo(data, FloatType, options).asInstanceOf[Float]
    } else {
      val data = value
      TypeCast.castTo(data, FloatType, options).asInstanceOf[Float]
    }
  }
}
