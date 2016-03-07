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
import java.sql.{Date, Timestamp}
import java.text.NumberFormat
import java.util.Locale

import scala.util.control.Exception._
import scala.util.Try

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.types._

private[csv] object CSVInferSchema {

  /**
    * Similar to the JSON schema inference
    *     1. Infer type of each row
    *     2. Merge row types to find common type
    *     3. Replace any null types with string type
    */
  def infer(
      tokenRdd: RDD[Array[String]],
      header: Array[String],
      nullValue: String = ""): StructType = {

    val startType: Array[DataType] = Array.fill[DataType](header.length)(NullType)
    val rootTypes: Array[DataType] =
      tokenRdd.aggregate(startType)(inferRowType(nullValue), mergeRowTypes)

    val structFields = header.zip(rootTypes).map { case (thisHeader, rootType) =>
      val dType = rootType match {
        case _: NullType => StringType
        case other => other
      }
      StructField(thisHeader, dType, nullable = true)
    }

    StructType(structFields)
  }

  private def inferRowType(nullValue: String)
      (rowSoFar: Array[DataType], next: Array[String]): Array[DataType] = {
    var i = 0
    while (i < math.min(rowSoFar.length, next.length)) {  // May have columns on right missing.
      rowSoFar(i) = inferField(rowSoFar(i), next(i), nullValue)
      i+=1
    }
    rowSoFar
  }

  def mergeRowTypes(first: Array[DataType], second: Array[DataType]): Array[DataType] = {
    first.zipAll(second, NullType, NullType).map { case (a, b) =>
      findTightestCommonType(a, b).getOrElse(NullType)
    }
  }

  /**
    * Infer type of string field. Given known type Double, and a string "1", there is no
    * point checking if it is an Int, as the final type must be Double or higher.
    */
  def inferField(typeSoFar: DataType, field: String, nullValue: String = ""): DataType = {
    if (field == null || field.isEmpty || field == nullValue) {
      typeSoFar
    } else {
      typeSoFar match {
        case NullType => tryParseInteger(field)
        case IntegerType => tryParseInteger(field)
        case LongType => tryParseLong(field)
        case DoubleType => tryParseDouble(field)
        case TimestampType => tryParseTimestamp(field)
        case BooleanType => tryParseBoolean(field)
        case StringType => StringType
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unexpected data type $other")
      }
    }
  }

  private def tryParseInteger(field: String): DataType = if ((allCatch opt field.toInt).isDefined) {
    IntegerType
  } else {
    tryParseLong(field)
  }

  private def tryParseLong(field: String): DataType = if ((allCatch opt field.toLong).isDefined) {
    LongType
  } else {
    tryParseDouble(field)
  }

  private def tryParseDouble(field: String): DataType = {
    if ((allCatch opt field.toDouble).isDefined) {
      DoubleType
    } else {
      tryParseTimestamp(field)
    }
  }

  def tryParseTimestamp(field: String): DataType = {
    if ((allCatch opt Timestamp.valueOf(field)).isDefined) {
      TimestampType
    } else {
      tryParseBoolean(field)
    }
  }

  def tryParseBoolean(field: String): DataType = {
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

  private val numericPrecedence: IndexedSeq[DataType] = HiveTypeCoercion.numericPrecedence

  /**
    * Copied from internal Spark api
    * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion]]
    */
  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)
    case (StringType, t2) => Some(StringType)
    case (t1, StringType) => Some(StringType)

    // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case _ => None
  }
}

private[csv] object CSVTypeCast {

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
  def castTo(
      datum: String,
      castType: DataType,
      nullable: Boolean = true,
      nullValue: String = ""): Any = {

    if (datum == nullValue && nullable && (!castType.isInstanceOf[StringType])) {
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
        case _: BooleanType => datum.toBoolean
        case _: DecimalType => new BigDecimal(datum.replaceAll(",", ""))
        // TODO(hossein): would be good to support other common timestamp formats
        case _: TimestampType => Timestamp.valueOf(datum)
        // TODO(hossein): would be good to support other common date formats
        case _: DateType => Date.valueOf(datum)
        case _: StringType => datum
        case _ => throw new RuntimeException(s"Unsupported type: ${castType.typeName}")
      }
    }
  }

  /**
   * Helper method that converts string representation of a character to actual character.
   * It handles some Java escaped strings and throws exception if given string is longer than one
   * character.
   */
  @throws[IllegalArgumentException]
  def toChar(str: String): Char = {
    if (str.charAt(0) == '\\') {
      str.charAt(1)
      match {
        case 't' => '\t'
        case 'r' => '\r'
        case 'b' => '\b'
        case 'f' => '\f'
        case '\"' => '\"' // In case user changes quote char and uses \" as delimiter in options
        case '\'' => '\''
        case 'u' if str == """\u0000""" => '\u0000'
        case _ =>
          throw new IllegalArgumentException(s"Unsupported special character for delimiter: $str")
      }
    } else if (str.length == 1) {
      str.charAt(0)
    } else {
      throw new IllegalArgumentException(s"Delimiter cannot be more than one character: $str")
    }
  }
}
