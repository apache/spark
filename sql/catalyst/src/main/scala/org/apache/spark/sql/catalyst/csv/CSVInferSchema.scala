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

import java.util.Locale

import scala.util.control.Exception.allCatch

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.util.{DateFormatter, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class CSVInferSchema(val options: CSVOptions) extends Serializable {

  private val timestampParser = TimestampFormatter(
    options.timestampFormatInRead,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)

  private val timestampNTZFormatter = TimestampFormatter(
    options.timestampNTZFormatInRead,
    options.zoneId,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true,
    forTimestampNTZ = true)

  private lazy val dateFormatter = DateFormatter(
    options.dateFormatInRead,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)

  private val decimalParser = if (options.locale == Locale.US) {
    // Special handling the default locale for backward compatibility
    s: String => new java.math.BigDecimal(s)
  } else {
    ExprUtils.getDecimalParser(options.locale)
  }

  // Date formats that could be parsed in DefaultTimestampFormatter
  // Reference: DateTimeUtils.parseTimestampString
  // Used to determine inferring a column with mixture of dates and timestamps as TimestampType or
  // StringType when no timestamp format is specified (the lenient timestamp formatter will be used)
  private val LENIENT_TS_FORMATTER_SUPPORTED_DATE_FORMATS = Set(
    "yyyy-MM-dd", "yyyy-M-d", "yyyy-M-dd", "yyyy-MM-d", "yyyy-MM", "yyyy-M", "yyyy")

  private val isDefaultNTZ = SQLConf.get.timestampType == TimestampNTZType

  /**
   * Similar to the JSON schema inference
   *     1. Infer type of each row
   *     2. Merge row types to find common type
   *     3. Replace any null types with string type
   */
  def infer(
      tokenRDD: RDD[Array[String]],
      header: Array[String]): StructType = {
    val fields = if (options.inferSchemaFlag) {
      val startType: Array[DataType] = Array.fill[DataType](header.length)(NullType)
      val rootTypes: Array[DataType] =
        tokenRDD.aggregate(startType)(inferRowType, mergeRowTypes)

      toStructFields(rootTypes, header)
    } else {
      // By default fields are assumed to be StringType
      header.map(fieldName => StructField(fieldName, StringType, nullable = true))
    }

    StructType(fields)
  }

  def toStructFields(
      fieldTypes: Array[DataType],
      header: Array[String]): Array[StructField] = {
    header.zip(fieldTypes).map { case (thisHeader, rootType) =>
      val dType = rootType match {
        case _: NullType => StringType
        case other => other
      }
      StructField(thisHeader, dType, nullable = true)
    }
  }

  def inferRowType(rowSoFar: Array[DataType], next: Array[String]): Array[DataType] = {
    var i = 0
    while (i < math.min(rowSoFar.length, next.length)) {  // May have columns on right missing.
      rowSoFar(i) = inferField(rowSoFar(i), next(i))
      i+=1
    }
    rowSoFar
  }

  def mergeRowTypes(first: Array[DataType], second: Array[DataType]): Array[DataType] = {
    first.zipAll(second, NullType, NullType).map { case (a, b) =>
      compatibleType(a, b).getOrElse(NullType)
    }
  }

  /**
   * Infer type of string field. Given known type Double, and a string "1", there is no
   * point checking if it is an Int, as the final type must be Double or higher.
   */
  def inferField(typeSoFar: DataType, field: String): DataType = {
    if (field == null || field.isEmpty || field == options.nullValue) {
      typeSoFar
    } else {
      val typeElemInfer = typeSoFar match {
        case NullType => tryParseInteger(field)
        case IntegerType => tryParseInteger(field)
        case LongType => tryParseLong(field)
        case _: DecimalType => tryParseDecimal(field)
        case DoubleType => tryParseDouble(field)
        case DateType => tryParseDate(field)
        case TimestampNTZType => tryParseTimestampNTZ(field)
        case TimestampType => tryParseTimestamp(field)
        case BooleanType => tryParseBoolean(field)
        case StringType => StringType
        case other: DataType =>
          throw SparkException.internalError(s"Unexpected data type $other")
      }
      compatibleType(typeSoFar, typeElemInfer).getOrElse(StringType)
    }
  }

  private def isInfOrNan(field: String): Boolean = {
    field == options.nanValue || field == options.negativeInf || field == options.positiveInf
  }

  private def tryParseInteger(field: String): DataType = {
    if ((allCatch opt field.toInt).isDefined) {
      IntegerType
    } else {
      tryParseLong(field)
    }
  }

  private def tryParseLong(field: String): DataType = {
    if ((allCatch opt field.toLong).isDefined) {
      LongType
    } else {
      tryParseDecimal(field)
    }
  }

  private def tryParseDecimal(field: String): DataType = {
    val decimalTry = allCatch opt {
      // The conversion can fail when the `field` is not a form of number.
      val bigDecimal = decimalParser(field)
      // Because many other formats do not support decimal, it reduces the cases for
      // decimals by disallowing values having scale (e.g. `1.1`).
      if (bigDecimal.scale <= 0) {
        // `DecimalType` conversion can fail when
        //   1. The precision is bigger than 38.
        //   2. scale is bigger than precision.
        DecimalType(bigDecimal.precision, bigDecimal.scale)
      } else {
        tryParseDouble(field)
      }
    }
    decimalTry.getOrElse(tryParseDouble(field))
  }

  private def tryParseDouble(field: String): DataType = {
    if ((allCatch opt field.toDouble).isDefined || isInfOrNan(field)) {
      DoubleType
    } else if (options.preferDate) {
      tryParseDate(field)
    } else {
      tryParseTimestampNTZ(field)
    }
  }

  private def tryParseDate(field: String): DataType = {
    if ((allCatch opt dateFormatter.parse(field)).isDefined) {
      DateType
    } else {
      tryParseTimestampNTZ(field)
    }
  }

  private def tryParseTimestampNTZ(field: String): DataType = {
    // For text-based format, it's ambiguous to infer a timestamp string without timezone, as it can
    // be both TIMESTAMP LTZ and NTZ. To avoid behavior changes with the new support of NTZ, here
    // we only try to infer NTZ if the config is set to use NTZ by default.
    if (isDefaultNTZ &&
      timestampNTZFormatter.parseWithoutTimeZoneOptional(field, false).isDefined) {
      TimestampNTZType
    } else {
      tryParseTimestamp(field)
    }
  }

  private def tryParseTimestamp(field: String): DataType = {
    // This case infers a custom `dataFormat` is set.
    if (timestampParser.parseOptional(field).isDefined) {
      TimestampType
    } else {
      tryParseBoolean(field)
    }
  }

  private def tryParseBoolean(field: String): DataType = {
    if ((allCatch opt field.toBoolean).isDefined) {
      BooleanType
    } else {
      stringType()
    }
  }

  // Defining a function to return the StringType constant is necessary in order to work around
  // a Scala compiler issue which leads to runtime incompatibilities with certain Spark versions;
  // see issue #128 for more details.
  private def stringType(): DataType = {
    StringType
  }

  /**
   * Returns the common data type given two input data types so that the return type
   * is compatible with both input data types.
   */
  private def compatibleType(t1: DataType, t2: DataType): Option[DataType] = {
    (t1, t2) match {
      case (DateType, TimestampType) | (DateType, TimestampNTZType) |
           (TimestampNTZType, DateType) | (TimestampType, DateType) =>
        // For a column containing a mixture of dates and timestamps, infer it as timestamp type
        // if its dates can be inferred as timestamp type, otherwise infer it as StringType.
        // This only happens when the timestamp pattern is not specified, as the default timestamp
        // parser is very lenient and can parse date string as well.
        val dateFormat = options.dateFormatInRead.getOrElse(DateFormatter.defaultPattern)
        t1 match {
          case DateType if canParseDateAsTimestamp(dateFormat, t2) =>
            Some(t2)
          case TimestampType | TimestampNTZType if canParseDateAsTimestamp(dateFormat, t1) =>
            Some(t1)
          case _ => Some(StringType)
        }
      case _ => TypeCoercion.findTightestCommonType(t1, t2).orElse(findCompatibleTypeForCSV(t1, t2))
    }
  }

  /**
   * Return true if strings of given date format can be parsed as timestamps
   *  1. If user provides timestamp format, we will parse strings as timestamps using
   *  Iso8601TimestampFormatter (with strict timestamp parsing). Any date string can not be parsed
   *  as timestamp type in this case
   *  2. Otherwise, we will use DefaultTimestampFormatter to parse strings as timestamps, which
   *  is more lenient and can parse strings of some date formats as timestamps.
   */
  private def canParseDateAsTimestamp(dateFormat: String, tsType: DataType): Boolean = {
    if ((tsType.isInstanceOf[TimestampType] && options.timestampFormatInRead.isEmpty) ||
      (tsType.isInstanceOf[TimestampNTZType] && options.timestampNTZFormatInRead.isEmpty)) {
      LENIENT_TS_FORMATTER_SUPPORTED_DATE_FORMATS.contains(dateFormat)
    } else {
      false
    }
  }

  /**
   * The following pattern matching represents additional type promotion rules that
   * are CSV specific.
   */
  private val findCompatibleTypeForCSV: (DataType, DataType) => Option[DataType] = {
    case (StringType, t2) => Some(StringType)
    case (t1, StringType) => Some(StringType)

    // These two cases below deal with when `IntegralType` is larger than `DecimalType`.
    case (t1: IntegralType, t2: DecimalType) =>
      compatibleType(DecimalType.forType(t1), t2)
    case (t1: DecimalType, t2: IntegralType) =>
      compatibleType(t1, DecimalType.forType(t2))

    // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
    // in most case, also have better precision.
    case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) =>
      Some(DoubleType)

    case (t1: DecimalType, t2: DecimalType) =>
      val scale = math.max(t1.scale, t2.scale)
      val range = math.max(t1.precision - t1.scale, t2.precision - t2.scale)
      if (range + scale > 38) {
        // DecimalType can't support precision > 38
        Some(DoubleType)
      } else {
        Some(DecimalType(range + scale, scale))
      }

    case (TimestampNTZType, TimestampType) | (TimestampType, TimestampNTZType) =>
      Some(TimestampType)

    case _ => None
  }
}
