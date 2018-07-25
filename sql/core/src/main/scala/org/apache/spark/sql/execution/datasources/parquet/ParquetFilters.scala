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

package org.apache.spark.sql.execution.datasources.parquet

<<<<<<< HEAD
import java.sql.{Date, Timestamp}
||||||| merged common ancestors
import java.sql.Date
=======
import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters.asScalaBufferConverter
>>>>>>> upstream/master

import org.apache.parquet.filter2.predicate._
import org.apache.parquet.filter2.predicate.Operators.{Column, SupportsEqNotEq, SupportsLtGt}
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{DecimalMetadata, MessageType, OriginalType, PrimitiveComparator}
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources
import org.apache.spark.unsafe.types.UTF8String

/**
 * Some utility function to convert Spark data source filters to Parquet filters.
 */
<<<<<<< HEAD
private[parquet] class ParquetFilters(pushDownDate: Boolean, int96AsTimestamp: Boolean) {
||||||| merged common ancestors
private[parquet] class ParquetFilters(pushDownDate: Boolean) {
=======
private[parquet] class ParquetFilters(
    pushDownDate: Boolean,
    pushDownTimestamp: Boolean,
    pushDownDecimal: Boolean,
    pushDownStartWith: Boolean,
    pushDownInFilterThreshold: Int) {

  private case class ParquetSchemaType(
      originalType: OriginalType,
      primitiveTypeName: PrimitiveTypeName,
      length: Int,
      decimalMetadata: DecimalMetadata)

  private val ParquetBooleanType = ParquetSchemaType(null, BOOLEAN, 0, null)
  private val ParquetByteType = ParquetSchemaType(INT_8, INT32, 0, null)
  private val ParquetShortType = ParquetSchemaType(INT_16, INT32, 0, null)
  private val ParquetIntegerType = ParquetSchemaType(null, INT32, 0, null)
  private val ParquetLongType = ParquetSchemaType(null, INT64, 0, null)
  private val ParquetFloatType = ParquetSchemaType(null, FLOAT, 0, null)
  private val ParquetDoubleType = ParquetSchemaType(null, DOUBLE, 0, null)
  private val ParquetStringType = ParquetSchemaType(UTF8, BINARY, 0, null)
  private val ParquetBinaryType = ParquetSchemaType(null, BINARY, 0, null)
  private val ParquetDateType = ParquetSchemaType(DATE, INT32, 0, null)
  private val ParquetTimestampMicrosType = ParquetSchemaType(TIMESTAMP_MICROS, INT64, 0, null)
  private val ParquetTimestampMillisType = ParquetSchemaType(TIMESTAMP_MILLIS, INT64, 0, null)
>>>>>>> upstream/master

  import ParquetColumns._

  private val makeInSet: PartialFunction[DataType, (String, Set[Any]) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(intColumn(n), SetInFilter(v.asInstanceOf[Set[java.lang.Integer]]))
    case LongType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(longColumn(n), SetInFilter(v.asInstanceOf[Set[java.lang.Long]]))
    case FloatType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(floatColumn(n), SetInFilter(v.asInstanceOf[Set[java.lang.Float]]))
    case DoubleType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(doubleColumn(n), SetInFilter(v.asInstanceOf[Set[java.lang.Double]]))
    case StringType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(binaryColumn(n),
          SetInFilter(v.map(s => Binary.fromString(s.asInstanceOf[String]))))
    case BinaryType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(binaryColumn(n),
          SetInFilter(v.map(e => Binary.fromReusedByteArray(e.asInstanceOf[Array[Byte]]))))
  }

  private def decimalToInt32(decimal: JBigDecimal): Integer = decimal.unscaledValue().intValue()

  private def decimalToInt64(decimal: JBigDecimal): JLong = decimal.unscaledValue().longValue()

  private def decimalToByteArray(decimal: JBigDecimal, numBytes: Int): Binary = {
    val decimalBuffer = new Array[Byte](numBytes)
    val bytes = decimal.unscaledValue().toByteArray

    val fixedLengthBytes = if (bytes.length == numBytes) {
      bytes
    } else {
      val signByte = if (bytes.head < 0) -1: Byte else 0: Byte
      java.util.Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte)
      System.arraycopy(bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length)
      decimalBuffer
    }
    Binary.fromConstantByteArray(fixedLengthBytes, 0, numBytes)
  }

  private val makeEq: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetBooleanType =>
      (n: String, v: Any) => FilterApi.eq(booleanColumn(n), v.asInstanceOf[JBoolean])
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: String, v: Any) => FilterApi.eq(
        intColumn(n),
        Option(v).map(_.asInstanceOf[Number].intValue.asInstanceOf[Integer]).orNull)
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.eq(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.eq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.eq(doubleColumn(n), v.asInstanceOf[JDouble])

    // Binary.fromString and Binary.fromByteArray don't accept null values
    case ParquetStringType =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(s => Binary.fromString(s.asInstanceOf[String])).orNull)
    case ParquetBinaryType =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]])).orNull)
<<<<<<< HEAD
    case TimestampType =>
      (n: String, v: Any) => FilterApi.eq(
        longColumn(n), convertTimestamp(v.asInstanceOf[java.sql.Timestamp]))
    case DateType if pushDownDate =>
||||||| merged common ancestors
    case DateType if pushDownDate =>
=======
    case ParquetDateType if pushDownDate =>
>>>>>>> upstream/master
      (n: String, v: Any) => FilterApi.eq(
<<<<<<< HEAD
        intColumn(n), convertDate(v.asInstanceOf[java.sql.Date]))
||||||| merged common ancestors
        intColumn(n),
        Option(v).map(date => dateToDays(date.asInstanceOf[Date]).asInstanceOf[Integer]).orNull)
=======
        intColumn(n),
        Option(v).map(date => dateToDays(date.asInstanceOf[Date]).asInstanceOf[Integer]).orNull)
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: String, v: Any) => FilterApi.eq(
        longColumn(n),
        Option(v).map(t => DateTimeUtils.fromJavaTimestamp(t.asInstanceOf[Timestamp])
          .asInstanceOf[JLong]).orNull)
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: String, v: Any) => FilterApi.eq(
        longColumn(n),
        Option(v).map(_.asInstanceOf[Timestamp].getTime.asInstanceOf[JLong]).orNull)

    case ParquetSchemaType(DECIMAL, INT32, _, _) if pushDownDecimal =>
      (n: String, v: Any) => FilterApi.eq(
        intColumn(n),
        Option(v).map(d => decimalToInt32(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(DECIMAL, INT64, _, _) if pushDownDecimal =>
      (n: String, v: Any) => FilterApi.eq(
        longColumn(n),
        Option(v).map(d => decimalToInt64(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, length, _) if pushDownDecimal =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(d => decimalToByteArray(d.asInstanceOf[JBigDecimal], length)).orNull)
>>>>>>> upstream/master
  }

  private val makeNotEq: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetBooleanType =>
      (n: String, v: Any) => FilterApi.notEq(booleanColumn(n), v.asInstanceOf[JBoolean])
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: String, v: Any) => FilterApi.notEq(
        intColumn(n),
        Option(v).map(_.asInstanceOf[Number].intValue.asInstanceOf[Integer]).orNull)
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.notEq(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.notEq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.notEq(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(s => Binary.fromString(s.asInstanceOf[String])).orNull)
    case ParquetBinaryType =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]])).orNull)
<<<<<<< HEAD
    case TimestampType =>
      (n: String, v: Any) => FilterApi.notEq(
        longColumn(n), convertTimestamp(v.asInstanceOf[java.sql.Timestamp]))
    case DateType if pushDownDate =>
||||||| merged common ancestors
    case DateType if pushDownDate =>
=======
    case ParquetDateType if pushDownDate =>
>>>>>>> upstream/master
      (n: String, v: Any) => FilterApi.notEq(
<<<<<<< HEAD
        intColumn(n), convertDate(v.asInstanceOf[java.sql.Date]))
||||||| merged common ancestors
        intColumn(n),
        Option(v).map(date => dateToDays(date.asInstanceOf[Date]).asInstanceOf[Integer]).orNull)
=======
        intColumn(n),
        Option(v).map(date => dateToDays(date.asInstanceOf[Date]).asInstanceOf[Integer]).orNull)
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: String, v: Any) => FilterApi.notEq(
        longColumn(n),
        Option(v).map(t => DateTimeUtils.fromJavaTimestamp(t.asInstanceOf[Timestamp])
          .asInstanceOf[JLong]).orNull)
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: String, v: Any) => FilterApi.notEq(
        longColumn(n),
        Option(v).map(_.asInstanceOf[Timestamp].getTime.asInstanceOf[JLong]).orNull)

    case ParquetSchemaType(DECIMAL, INT32, _, _) if pushDownDecimal =>
      (n: String, v: Any) => FilterApi.notEq(
        intColumn(n),
        Option(v).map(d => decimalToInt32(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(DECIMAL, INT64, _, _) if pushDownDecimal =>
      (n: String, v: Any) => FilterApi.notEq(
        longColumn(n),
        Option(v).map(d => decimalToInt64(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, length, _) if pushDownDecimal =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(d => decimalToByteArray(d.asInstanceOf[JBigDecimal], length)).orNull)
>>>>>>> upstream/master
  }

  private val makeLt: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: String, v: Any) =>
        FilterApi.lt(intColumn(n), v.asInstanceOf[Number].intValue.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.lt(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.lt(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.lt(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
<<<<<<< HEAD
    case TimestampType =>
      (n: String, v: Any) => FilterApi.lt(
        longColumn(n), convertTimestamp(v.asInstanceOf[java.sql.Timestamp]))
    case DateType if pushDownDate =>
||||||| merged common ancestors
    case DateType if pushDownDate =>
=======
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) =>
        FilterApi.lt(intColumn(n), dateToDays(v.asInstanceOf[Date]).asInstanceOf[Integer])
    case ParquetTimestampMicrosType if pushDownTimestamp =>
>>>>>>> upstream/master
      (n: String, v: Any) => FilterApi.lt(
<<<<<<< HEAD
        intColumn(n), convertDate(v.asInstanceOf[java.sql.Date]))
||||||| merged common ancestors
        intColumn(n),
        Option(v).map(date => dateToDays(date.asInstanceOf[Date]).asInstanceOf[Integer]).orNull)
=======
        longColumn(n),
        DateTimeUtils.fromJavaTimestamp(v.asInstanceOf[Timestamp]).asInstanceOf[JLong])
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: String, v: Any) => FilterApi.lt(
        longColumn(n),
        v.asInstanceOf[Timestamp].getTime.asInstanceOf[JLong])

    case ParquetSchemaType(DECIMAL, INT32, _, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.lt(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, INT64, _, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.lt(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, length, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
>>>>>>> upstream/master
  }

<<<<<<< HEAD
  private val makeLtEq: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Any) => FilterApi.ltEq(intColumn(n), v.asInstanceOf[java.lang.Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.ltEq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.ltEq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.ltEq(doubleColumn(n), v.asInstanceOf[java.lang.Double])
    case StringType =>
||||||| merged common ancestors
  private val makeLtEq: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Any) => FilterApi.ltEq(intColumn(n), v.asInstanceOf[java.lang.Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.ltEq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.ltEq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.ltEq(doubleColumn(n), v.asInstanceOf[java.lang.Double])

    case StringType =>
=======
  private val makeLtEq: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(intColumn(n), v.asInstanceOf[Number].intValue.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.ltEq(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.ltEq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.ltEq(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
>>>>>>> upstream/master
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
<<<<<<< HEAD
    case TimestampType =>
      (n: String, v: Any) => FilterApi.ltEq(
        longColumn(n), convertTimestamp(v.asInstanceOf[java.sql.Timestamp]))
    case DateType if pushDownDate =>
||||||| merged common ancestors
    case DateType if pushDownDate =>
=======
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) =>
        FilterApi.ltEq(intColumn(n), dateToDays(v.asInstanceOf[Date]).asInstanceOf[Integer])
    case ParquetTimestampMicrosType if pushDownTimestamp =>
>>>>>>> upstream/master
      (n: String, v: Any) => FilterApi.ltEq(
<<<<<<< HEAD
        intColumn(n), convertDate(v.asInstanceOf[java.sql.Date]))
||||||| merged common ancestors
        intColumn(n),
        Option(v).map(date => dateToDays(date.asInstanceOf[Date]).asInstanceOf[Integer]).orNull)
=======
        longColumn(n),
        DateTimeUtils.fromJavaTimestamp(v.asInstanceOf[Timestamp]).asInstanceOf[JLong])
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: String, v: Any) => FilterApi.ltEq(
        longColumn(n),
        v.asInstanceOf[Timestamp].getTime.asInstanceOf[JLong])

    case ParquetSchemaType(DECIMAL, INT32, _, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.ltEq(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, INT64, _, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.ltEq(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, length, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
>>>>>>> upstream/master
  }

<<<<<<< HEAD
  private val makeGt: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Any) => FilterApi.gt(intColumn(n), v.asInstanceOf[java.lang.Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.gt(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.gt(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.gt(doubleColumn(n), v.asInstanceOf[java.lang.Double])
    case StringType =>
||||||| merged common ancestors
  private val makeGt: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Any) => FilterApi.gt(intColumn(n), v.asInstanceOf[java.lang.Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.gt(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.gt(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.gt(doubleColumn(n), v.asInstanceOf[java.lang.Double])

    case StringType =>
=======
  private val makeGt: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: String, v: Any) =>
        FilterApi.gt(intColumn(n), v.asInstanceOf[Number].intValue.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.gt(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.gt(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.gt(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
>>>>>>> upstream/master
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
<<<<<<< HEAD
    case TimestampType =>
      (n: String, v: Any) => FilterApi.gt(
        longColumn(n), convertTimestamp(v.asInstanceOf[java.sql.Timestamp]))
    case DateType if pushDownDate =>
||||||| merged common ancestors
    case DateType if pushDownDate =>
=======
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) =>
        FilterApi.gt(intColumn(n), dateToDays(v.asInstanceOf[Date]).asInstanceOf[Integer])
    case ParquetTimestampMicrosType if pushDownTimestamp =>
>>>>>>> upstream/master
      (n: String, v: Any) => FilterApi.gt(
<<<<<<< HEAD
        intColumn(n), convertDate(v.asInstanceOf[java.sql.Date]))
||||||| merged common ancestors
        intColumn(n),
        Option(v).map(date => dateToDays(date.asInstanceOf[Date]).asInstanceOf[Integer]).orNull)
=======
        longColumn(n),
        DateTimeUtils.fromJavaTimestamp(v.asInstanceOf[Timestamp]).asInstanceOf[JLong])
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: String, v: Any) => FilterApi.gt(
        longColumn(n),
        v.asInstanceOf[Timestamp].getTime.asInstanceOf[JLong])

    case ParquetSchemaType(DECIMAL, INT32, _, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.gt(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, INT64, _, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.gt(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, length, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
>>>>>>> upstream/master
  }

<<<<<<< HEAD
  private val makeGtEq: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Any) => FilterApi.gtEq(intColumn(n), v.asInstanceOf[java.lang.Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.gtEq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.gtEq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.gtEq(doubleColumn(n), v.asInstanceOf[java.lang.Double])
    case StringType =>
||||||| merged common ancestors
  private val makeGtEq: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Any) => FilterApi.gtEq(intColumn(n), v.asInstanceOf[java.lang.Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.gtEq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.gtEq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.gtEq(doubleColumn(n), v.asInstanceOf[java.lang.Double])

    case StringType =>
=======
  private val makeGtEq: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(intColumn(n), v.asInstanceOf[Number].intValue.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.gtEq(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.gtEq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.gtEq(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
>>>>>>> upstream/master
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
<<<<<<< HEAD
    case TimestampType =>
      (n: String, v: Any) => FilterApi.gtEq(
        longColumn(n), convertTimestamp(v.asInstanceOf[java.sql.Timestamp]))
    case DateType if pushDownDate =>
||||||| merged common ancestors
    case DateType if pushDownDate =>
=======
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) =>
        FilterApi.gtEq(intColumn(n), dateToDays(v.asInstanceOf[Date]).asInstanceOf[Integer])
    case ParquetTimestampMicrosType if pushDownTimestamp =>
>>>>>>> upstream/master
      (n: String, v: Any) => FilterApi.gtEq(
<<<<<<< HEAD
        intColumn(n), convertDate(v.asInstanceOf[java.sql.Date]))
  }

  private def convertDate(d: Date): Integer = {
    if (d != null) {
      DateTimeUtils.fromJavaDate(d).asInstanceOf[Integer]
    } else {
      null
    }
  }

  private def convertTimestamp(t: Timestamp): java.lang.Long = {
    if (t != null) {
      DateTimeUtils.fromJavaTimestamp(t).asInstanceOf[java.lang.Long]
    } else {
      null
    }
||||||| merged common ancestors
        intColumn(n),
        Option(v).map(date => dateToDays(date.asInstanceOf[Date]).asInstanceOf[Integer]).orNull)
=======
        longColumn(n),
        DateTimeUtils.fromJavaTimestamp(v.asInstanceOf[Timestamp]).asInstanceOf[JLong])
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: String, v: Any) => FilterApi.gtEq(
        longColumn(n),
        v.asInstanceOf[Timestamp].getTime.asInstanceOf[JLong])

    case ParquetSchemaType(DECIMAL, INT32, _, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.gtEq(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, INT64, _, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.gtEq(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, length, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
>>>>>>> upstream/master
  }

  /**
   * Returns a map from name of the column to the data type, if predicate push down applies.
   */
<<<<<<< HEAD
  private def getFieldMap(dataType: DataType, int96AsTimestamp: Boolean): Map[String, DataType] =
    dataType match {
      case StructType(fields) =>
        // Here we don't flatten the fields in the nested schema but just look up through
        // root fields. Currently, accessing to nested fields does not push down filters
        // and it does not support to create filters for them.
        // scalastyle:off println
        fields.filterNot { f =>
          DataTypes.TimestampType.acceptsType(f.dataType) && int96AsTimestamp
        }.map(f => f.name -> f.dataType).toMap
      case _ => Map.empty[String, DataType]
    }
||||||| merged common ancestors
  private def getFieldMap(dataType: DataType): Map[String, DataType] = dataType match {
    case StructType(fields) =>
      // Here we don't flatten the fields in the nested schema but just look up through
      // root fields. Currently, accessing to nested fields does not push down filters
      // and it does not support to create filters for them.
      fields.map(f => f.name -> f.dataType).toMap
    case _ => Map.empty[String, DataType]
  }
=======
  private def getFieldMap(dataType: MessageType): Map[String, ParquetSchemaType] = dataType match {
    case m: MessageType =>
      // Here we don't flatten the fields in the nested schema but just look up through
      // root fields. Currently, accessing to nested fields does not push down filters
      // and it does not support to create filters for them.
      m.getFields.asScala.filter(_.isPrimitive).map(_.asPrimitiveType()).map { f =>
        f.getName -> ParquetSchemaType(
          f.getOriginalType, f.getPrimitiveTypeName, f.getTypeLength, f.getDecimalMetadata)
      }.toMap
    case _ => Map.empty[String, ParquetSchemaType]
  }
>>>>>>> upstream/master

  /**
   * Converts data sources filters to Parquet filter predicates.
   */
<<<<<<< HEAD
  def createFilter(
    schema: StructType,
    predicate: sources.Filter): Option[FilterPredicate] = {
    val nameToType = getFieldMap(schema, int96AsTimestamp)
||||||| merged common ancestors
  def createFilter(schema: StructType, predicate: sources.Filter): Option[FilterPredicate] = {
    val nameToType = getFieldMap(schema)
=======
  def createFilter(schema: MessageType, predicate: sources.Filter): Option[FilterPredicate] = {
    val nameToType = getFieldMap(schema)
>>>>>>> upstream/master

<<<<<<< HEAD
    def canMakeFilterOn(name: String): Boolean = nameToType.contains(name)
||||||| merged common ancestors
    // Parquet does not allow dots in the column name because dots are used as a column path
    // delimiter. Since Parquet 1.8.2 (PARQUET-389), Parquet accepts the filter predicates
    // with missing columns. The incorrect results could be got from Parquet when we push down
    // filters for the column having dots in the names. Thus, we do not push down such filters.
    // See SPARK-20364.
    def canMakeFilterOn(name: String): Boolean = nameToType.contains(name) && !name.contains(".")
=======
    // Decimal type must make sure that filter value's scale matched the file.
    // If doesn't matched, which would cause data corruption.
    def isDecimalMatched(value: Any, decimalMeta: DecimalMetadata): Boolean = value match {
      case decimal: JBigDecimal =>
        decimal.scale == decimalMeta.getScale
      case _ => false
    }

    // Parquet's type in the given file should be matched to the value's type
    // in the pushed filter in order to push down the filter to Parquet.
    def valueCanMakeFilterOn(name: String, value: Any): Boolean = {
      value == null || (nameToType(name) match {
        case ParquetBooleanType => value.isInstanceOf[JBoolean]
        case ParquetByteType | ParquetShortType | ParquetIntegerType => value.isInstanceOf[Number]
        case ParquetLongType => value.isInstanceOf[JLong]
        case ParquetFloatType => value.isInstanceOf[JFloat]
        case ParquetDoubleType => value.isInstanceOf[JDouble]
        case ParquetStringType => value.isInstanceOf[String]
        case ParquetBinaryType => value.isInstanceOf[Array[Byte]]
        case ParquetDateType => value.isInstanceOf[Date]
        case ParquetTimestampMicrosType | ParquetTimestampMillisType =>
          value.isInstanceOf[Timestamp]
        case ParquetSchemaType(DECIMAL, INT32, _, decimalMeta) =>
          isDecimalMatched(value, decimalMeta)
        case ParquetSchemaType(DECIMAL, INT64, _, decimalMeta) =>
          isDecimalMatched(value, decimalMeta)
        case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, _, decimalMeta) =>
          isDecimalMatched(value, decimalMeta)
        case _ => false
      })
    }

    // Parquet does not allow dots in the column name because dots are used as a column path
    // delimiter. Since Parquet 1.8.2 (PARQUET-389), Parquet accepts the filter predicates
    // with missing columns. The incorrect results could be got from Parquet when we push down
    // filters for the column having dots in the names. Thus, we do not push down such filters.
    // See SPARK-20364.
    def canMakeFilterOn(name: String, value: Any): Boolean = {
      nameToType.contains(name) && !name.contains(".") && valueCanMakeFilterOn(name, value)
    }
>>>>>>> upstream/master

    // NOTE:
    //
    // For any comparison operator `cmp`, both `a cmp NULL` and `NULL cmp a` evaluate to `NULL`,
    // which can be casted to `false` implicitly. Please refer to the `eval` method of these
    // operators and the `PruneFilters` rule for details.

    predicate match {
      case sources.IsNull(name) if canMakeFilterOn(name, null) =>
        makeEq.lift(nameToType(name)).map(_(name, null))
      case sources.IsNotNull(name) if canMakeFilterOn(name, null) =>
        makeNotEq.lift(nameToType(name)).map(_(name, null))

      case sources.EqualTo(name, value) if canMakeFilterOn(name, value) =>
        makeEq.lift(nameToType(name)).map(_(name, value))
      case sources.Not(sources.EqualTo(name, value)) if canMakeFilterOn(name, value) =>
        makeNotEq.lift(nameToType(name)).map(_(name, value))

      case sources.EqualNullSafe(name, value) if canMakeFilterOn(name, value) =>
        makeEq.lift(nameToType(name)).map(_(name, value))
      case sources.Not(sources.EqualNullSafe(name, value)) if canMakeFilterOn(name, value) =>
        makeNotEq.lift(nameToType(name)).map(_(name, value))

      case sources.LessThan(name, value) if canMakeFilterOn(name, value) =>
        makeLt.lift(nameToType(name)).map(_(name, value))
      case sources.LessThanOrEqual(name, value) if canMakeFilterOn(name, value) =>
        makeLtEq.lift(nameToType(name)).map(_(name, value))

      case sources.GreaterThan(name, value) if canMakeFilterOn(name, value) =>
        makeGt.lift(nameToType(name)).map(_(name, value))
      case sources.GreaterThanOrEqual(name, value) if canMakeFilterOn(name, value) =>
        makeGtEq.lift(nameToType(name)).map(_(name, value))

      case sources.And(lhs, rhs) =>
        // At here, it is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
        // convert b in ('1'). If we only convert a = 2, we will end up with a filter
        // NOT(a = 2), which will generate wrong results.
        // Pushing one side of AND down is only safe to do at the top level.
        // You can see ParquetRelation's initializeLocalJobFunc method as an example.
        for {
          lhsFilter <- createFilter(schema, lhs)
          rhsFilter <- createFilter(schema, rhs)
        } yield FilterApi.and(lhsFilter, rhsFilter)

      case sources.Or(lhs, rhs) =>
        for {
          lhsFilter <- createFilter(schema, lhs)
          rhsFilter <- createFilter(schema, rhs)
        } yield FilterApi.or(lhsFilter, rhsFilter)

      case sources.Not(pred) =>
        createFilter(schema, pred)
          .map(FilterApi.not)
          .map(LogicalInverseRewriter.rewrite)

      case sources.In(name, values) if canMakeFilterOn(name) =>
        makeInSet.lift(nameToType(name)).map(_(name, values.toSet))

      case sources.In(name, values) if canMakeFilterOn(name, values.head)
        && values.distinct.length <= pushDownInFilterThreshold =>
        values.distinct.flatMap { v =>
          makeEq.lift(nameToType(name)).map(_(name, v))
        }.reduceLeftOption(FilterApi.or)

      case sources.StringStartsWith(name, prefix)
          if pushDownStartWith && canMakeFilterOn(name, prefix) =>
        Option(prefix).map { v =>
          FilterApi.userDefined(binaryColumn(name),
            new UserDefinedPredicate[Binary] with Serializable {
              private val strToBinary = Binary.fromReusedByteArray(v.getBytes)
              private val size = strToBinary.length

              override def canDrop(statistics: Statistics[Binary]): Boolean = {
                val comparator = PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR
                val max = statistics.getMax
                val min = statistics.getMin
                comparator.compare(max.slice(0, math.min(size, max.length)), strToBinary) < 0 ||
                  comparator.compare(min.slice(0, math.min(size, min.length)), strToBinary) > 0
              }

              override def inverseCanDrop(statistics: Statistics[Binary]): Boolean = {
                val comparator = PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR
                val max = statistics.getMax
                val min = statistics.getMin
                comparator.compare(max.slice(0, math.min(size, max.length)), strToBinary) == 0 &&
                  comparator.compare(min.slice(0, math.min(size, min.length)), strToBinary) == 0
              }

              override def keep(value: Binary): Boolean = {
                UTF8String.fromBytes(value.getBytes).startsWith(
                  UTF8String.fromBytes(strToBinary.getBytes))
              }
            }
          )
        }

      case _ => None
    }
  }
}

private[parquet] case class SetInFilter[T <: Comparable[T]](valueSet: Set[T])
  extends UserDefinedPredicate[T] with Serializable {

  override def keep(value: T): Boolean = {
    value != null && valueSet.contains(value)
  }

  // Drop when no value in the set is within the statistics range.
  override def canDrop(statistics: Statistics[T]): Boolean = {
    val statMax = statistics.getMax
    val statMin = statistics.getMin
    val statRange = com.google.common.collect.Range.closed(statMin, statMax)
    !valueSet.exists(value => statRange.contains(value))
  }

  // Can only drop not(in(set)) when we are know that every element in the block is in valueSet.
  // From the statistics, we can only be assured of this when min == max.
  override def inverseCanDrop(statistics: Statistics[T]): Boolean = {
    val statMax = statistics.getMax
    val statMin = statistics.getMin
    statMin == statMax && valueSet.contains(statMin)
  }
}

/**
 * Note that, this is a hacky workaround to allow dots in column names. Currently, column APIs
 * in Parquet's `FilterApi` only allows dot-separated names so here we resemble those columns
 * but only allow single column path that allows dots in the names as we don't currently push
 * down filters with nested fields.
 */
private[parquet] object ParquetColumns {
  def intColumn(columnPath: String): Column[Integer] with SupportsLtGt = {
    new Column[Integer] (ColumnPath.get(columnPath), classOf[Integer]) with SupportsLtGt
  }

  def longColumn(columnPath: String): Column[java.lang.Long] with SupportsLtGt = {
    new Column[java.lang.Long] (
      ColumnPath.get(columnPath), classOf[java.lang.Long]) with SupportsLtGt
  }

  def floatColumn(columnPath: String): Column[java.lang.Float] with SupportsLtGt = {
    new Column[java.lang.Float] (
      ColumnPath.get(columnPath), classOf[java.lang.Float]) with SupportsLtGt
  }

  def doubleColumn(columnPath: String): Column[java.lang.Double] with SupportsLtGt = {
    new Column[java.lang.Double] (
      ColumnPath.get(columnPath), classOf[java.lang.Double]) with SupportsLtGt
  }

  def booleanColumn(columnPath: String): Column[java.lang.Boolean] with SupportsEqNotEq = {
    new Column[java.lang.Boolean] (
      ColumnPath.get(columnPath), classOf[java.lang.Boolean]) with SupportsEqNotEq
  }

  def binaryColumn(columnPath: String): Column[Binary] with SupportsLtGt = {
    new Column[Binary] (ColumnPath.get(columnPath), classOf[Binary]) with SupportsLtGt
  }
}
