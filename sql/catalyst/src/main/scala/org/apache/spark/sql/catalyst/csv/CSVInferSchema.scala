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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._

class CSVInferSchema(val options: CSVOptions) extends Serializable {

  private val timestampParser = TimestampFormatter(
    options.timestampFormatInRead,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)

  private val decimalParser = if (options.locale == Locale.US) {
    // Special handling the default locale for backward compatibility
    s: String => new java.math.BigDecimal(s)
  } else {
    ExprUtils.getDecimalParser(options.locale)
  }

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
        case TimestampType => tryParseTimestamp(field)
        case BooleanType => tryParseBoolean(field)
        case StringType => StringType
        case other: DataType =>
          throw QueryExecutionErrors.dataTypeUnexpectedError(other)
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
    } else {
      tryParseTimestamp(field)
    }
  }

  private def tryParseTimestamp(field: String): DataType = {
    // This case infers a custom `dataFormat` is set.
    if ((allCatch opt timestampParser.parse(field)).isDefined) {
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
    TypeCoercion.findTightestCommonType(t1, t2).orElse(findCompatibleTypeForCSV(t1, t2))
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
    case _ => None
  }
}
