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

import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.Date

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.parquet.filter2.predicate._
import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{DecimalMetadata, MessageType, OriginalType, PrimitiveComparator}
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLDate
import org.apache.spark.sql.sources
import org.apache.spark.unsafe.types.UTF8String

/**
 * Some utility function to convert Spark data source filters to Parquet filters.
 */
private[parquet] class ParquetFilters(
    pushDownDate: Boolean,
    pushDownDecimal: Boolean,
    pushDownStartWith: Boolean) {

  private case class ParquetSchemaType(
      originalType: OriginalType,
      primitiveTypeName: PrimitiveTypeName,
      length: Int,
      decimalMeta: DecimalMetadata)

  private val ParquetBooleanType = ParquetSchemaType(null, BOOLEAN, 0, null)
  private val ParquetIntegerType = ParquetSchemaType(null, INT32, 0, null)
  private val ParquetLongType = ParquetSchemaType(null, INT64, 0, null)
  private val ParquetFloatType = ParquetSchemaType(null, FLOAT, 0, null)
  private val ParquetDoubleType = ParquetSchemaType(null, DOUBLE, 0, null)
  private val ParquetStringType = ParquetSchemaType(UTF8, BINARY, 0, null)
  private val ParquetBinaryType = ParquetSchemaType(null, BINARY, 0, null)
  private val ParquetDateType = ParquetSchemaType(DATE, INT32, 0, null)

  private def dateToDays(date: Date): SQLDate = {
    DateTimeUtils.fromJavaDate(date)
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
    Binary.fromReusedByteArray(fixedLengthBytes, 0, numBytes)
  }

  private val makeEq: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetBooleanType =>
      (n: String, v: Any) => FilterApi.eq(booleanColumn(n), v.asInstanceOf[JBoolean])
    case ParquetIntegerType =>
      (n: String, v: Any) => FilterApi.eq(intColumn(n), v.asInstanceOf[Integer])
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
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) => FilterApi.eq(
        intColumn(n),
        Option(v).map(date => dateToDays(date.asInstanceOf[Date]).asInstanceOf[Integer]).orNull)

    case ParquetSchemaType(DECIMAL, INT32, 0, _) if pushDownDecimal =>
      (n: String, v: Any) => FilterApi.eq(
        intColumn(n),
        Option(v).map(d => decimalToInt32(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(DECIMAL, INT64, 0, _) if pushDownDecimal =>
      (n: String, v: Any) => FilterApi.eq(
        longColumn(n),
        Option(v).map(d => decimalToInt64(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, length, _) if pushDownDecimal =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(d => decimalToByteArray(d.asInstanceOf[JBigDecimal], length)).orNull)
  }

  private val makeNotEq: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetBooleanType =>
      (n: String, v: Any) => FilterApi.notEq(booleanColumn(n), v.asInstanceOf[java.lang.Boolean])
    case ParquetIntegerType =>
      (n: String, v: Any) => FilterApi.notEq(intColumn(n), v.asInstanceOf[Integer])
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
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) => FilterApi.notEq(
        intColumn(n),
        Option(v).map(date => dateToDays(date.asInstanceOf[Date]).asInstanceOf[Integer]).orNull)

    case ParquetSchemaType(DECIMAL, INT32, 0, _) if pushDownDecimal =>
      (n: String, v: Any) => FilterApi.notEq(
        intColumn(n),
        Option(v).map(d => decimalToInt32(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(DECIMAL, INT64, 0, _) if pushDownDecimal =>
      (n: String, v: Any) => FilterApi.notEq(
        longColumn(n),
        Option(v).map(d => decimalToInt64(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, length, _) if pushDownDecimal =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(d => decimalToByteArray(d.asInstanceOf[JBigDecimal], length)).orNull)
  }

  private val makeLt: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetIntegerType =>
      (n: String, v: Any) => FilterApi.lt(intColumn(n), v.asInstanceOf[Integer])
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
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) =>
        FilterApi.lt(intColumn(n), dateToDays(v.asInstanceOf[Date]).asInstanceOf[Integer])

    case ParquetSchemaType(DECIMAL, INT32, 0, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.lt(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, INT64, 0, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.lt(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, length, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
  }

  private val makeLtEq: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetIntegerType =>
      (n: String, v: Any) => FilterApi.ltEq(intColumn(n), v.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.ltEq(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.ltEq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.ltEq(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) =>
        FilterApi.ltEq(intColumn(n), dateToDays(v.asInstanceOf[Date]).asInstanceOf[Integer])

    case ParquetSchemaType(DECIMAL, INT32, 0, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.ltEq(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, INT64, 0, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.ltEq(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, length, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
  }

  private val makeGt: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetIntegerType =>
      (n: String, v: Any) => FilterApi.gt(intColumn(n), v.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.gt(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.gt(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.gt(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) =>
        FilterApi.gt(intColumn(n), dateToDays(v.asInstanceOf[Date]).asInstanceOf[Integer])

    case ParquetSchemaType(DECIMAL, INT32, 0, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.gt(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, INT64, 0, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.gt(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, length, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
  }

  private val makeGtEq: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetIntegerType =>
      (n: String, v: Any) => FilterApi.gtEq(intColumn(n), v.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.gtEq(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.gtEq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.gtEq(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) =>
        FilterApi.gtEq(intColumn(n), dateToDays(v.asInstanceOf[Date]).asInstanceOf[Integer])

    case ParquetSchemaType(DECIMAL, INT32, 0, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.gtEq(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, INT64, 0, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.gtEq(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, length, _) if pushDownDecimal =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
  }

  /**
   * Returns a map from name of the column to the data type, if predicate push down applies.
   */
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

  /**
   * Converts data sources filters to Parquet filter predicates.
   */
  def createFilter(schema: MessageType, predicate: sources.Filter): Option[FilterPredicate] = {
    val nameToType = getFieldMap(schema)

    def isDecimalMatched(value: Any, decimalMeta: DecimalMetadata): Boolean = value match {
      case decimal: JBigDecimal =>
        decimal.scale == decimalMeta.getScale
      case _ => false
    }

    // Since SPARK-24716, ParquetFilter accepts parquet file schema to convert to
    // data source Filter. This must make sure that filter value matched the Filter.
    // If doesn't matched, then the schema used to read the file is incorrect,
    // which would cause data corruption.
    def valueCanMakeFilterOn(name: String, value: Any): Boolean = {
      value == null || (nameToType(name) match {
        case ParquetBooleanType if value.isInstanceOf[JBoolean] => true
        case ParquetIntegerType if value.isInstanceOf[Integer] => true
        case ParquetLongType if value.isInstanceOf[JLong] => true
        case ParquetFloatType if value.isInstanceOf[JFloat] => true
        case ParquetDoubleType if value.isInstanceOf[JDouble] => true
        case ParquetStringType if value.isInstanceOf[String] => true
        case ParquetBinaryType if value.isInstanceOf[Array[Byte]] => true
        case ParquetDateType if value.isInstanceOf[Date] => true
        case ParquetBinaryType if value.isInstanceOf[Date] => true
        case ParquetSchemaType(DECIMAL, INT32, 0, decimalMeta)
          if isDecimalMatched(value, decimalMeta) => true
        case ParquetSchemaType(DECIMAL, INT64, 0, decimalMeta)
          if isDecimalMatched(value, decimalMeta) => true
        case ParquetSchemaType(DECIMAL, FIXED_LEN_BYTE_ARRAY, _, decimalMeta)
          if isDecimalMatched(value, decimalMeta) => true
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

    // NOTE:
    //
    // For any comparison operator `cmp`, both `a cmp NULL` and `NULL cmp a` evaluate to `NULL`,
    // which can be casted to `false` implicitly. Please refer to the `eval` method of these
    // operators and the `PruneFilters` rule for details.

    // Hyukjin:
    // I added [[EqualNullSafe]] with [[org.apache.parquet.filter2.predicate.Operators.Eq]].
    // So, it performs equality comparison identically when given [[sources.Filter]] is [[EqualTo]].
    // The reason why I did this is, that the actual Parquet filter checks null-safe equality
    // comparison.
    // So I added this and maybe [[EqualTo]] should be changed. It still seems fine though, because
    // physical planning does not set `NULL` to [[EqualTo]] but changes it to [[IsNull]] and etc.
    // Probably I missed something and obviously this should be changed.

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
        createFilter(schema, pred).map(FilterApi.not)

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
