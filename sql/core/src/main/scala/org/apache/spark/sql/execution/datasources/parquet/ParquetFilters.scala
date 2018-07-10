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

import java.sql.Date

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.parquet.filter2.predicate._
import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{DecimalMetadata, MessageType, OriginalType, PrimitiveComparator, PrimitiveType}
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
private[parquet] class ParquetFilters(pushDownDate: Boolean, pushDownStartWith: Boolean) {

  private case class ParquetSchemaType(
      originalType: OriginalType,
      primitiveTypeName: PrimitiveTypeName,
      decimalMetadata: DecimalMetadata)

  private val ParquetBooleanType = ParquetSchemaType(null, BOOLEAN, null)
  private val ParquetByteType = ParquetSchemaType(INT_8, INT32, null)
  private val ParquetShortType = ParquetSchemaType(INT_16, INT32, null)
  private val ParquetIntegerType = ParquetSchemaType(null, INT32, null)
  private val ParquetLongType = ParquetSchemaType(null, INT64, null)
  private val ParquetFloatType = ParquetSchemaType(null, FLOAT, null)
  private val ParquetDoubleType = ParquetSchemaType(null, DOUBLE, null)
  private val ParquetStringType = ParquetSchemaType(UTF8, BINARY, null)
  private val ParquetBinaryType = ParquetSchemaType(null, BINARY, null)
  private val ParquetDateType = ParquetSchemaType(DATE, INT32, null)

  private def dateToDays(date: Date): SQLDate = {
    DateTimeUtils.fromJavaDate(date)
  }

  private val makeEq: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetBooleanType =>
      (n: String, v: Any) => FilterApi.eq(booleanColumn(n), v.asInstanceOf[java.lang.Boolean])
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: String, v: Any) => FilterApi.eq(
        intColumn(n),
        Option(v).map(_.asInstanceOf[Number].intValue.asInstanceOf[Integer]).orNull)
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.eq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.eq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.eq(doubleColumn(n), v.asInstanceOf[java.lang.Double])

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
  }

  private val makeNotEq: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetBooleanType =>
      (n: String, v: Any) => FilterApi.notEq(booleanColumn(n), v.asInstanceOf[java.lang.Boolean])
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: String, v: Any) => FilterApi.notEq(
        intColumn(n),
        Option(v).map(_.asInstanceOf[Number].intValue.asInstanceOf[Integer]).orNull)
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.notEq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.notEq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.notEq(doubleColumn(n), v.asInstanceOf[java.lang.Double])

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
  }

  private val makeLt: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: String, v: Any) =>
        FilterApi.lt(intColumn(n), v.asInstanceOf[Number].intValue.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.lt(longColumn(n), v.asInstanceOf[java.lang.Long])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.lt(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.lt(doubleColumn(n), v.asInstanceOf[java.lang.Double])

    case ParquetStringType =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) =>
        FilterApi.lt(intColumn(n), dateToDays(v.asInstanceOf[Date]).asInstanceOf[Integer])
  }

  private val makeLtEq: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(intColumn(n), v.asInstanceOf[Number].intValue.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.ltEq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.ltEq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.ltEq(doubleColumn(n), v.asInstanceOf[java.lang.Double])

    case ParquetStringType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) =>
        FilterApi.ltEq(intColumn(n), dateToDays(v.asInstanceOf[Date]).asInstanceOf[Integer])
  }

  private val makeGt: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: String, v: Any) =>
        FilterApi.gt(intColumn(n), v.asInstanceOf[Number].intValue.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.gt(longColumn(n), v.asInstanceOf[java.lang.Long])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.gt(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.gt(doubleColumn(n), v.asInstanceOf[java.lang.Double])

    case ParquetStringType =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) =>
        FilterApi.gt(intColumn(n), dateToDays(v.asInstanceOf[Date]).asInstanceOf[Integer])
  }

  private val makeGtEq: PartialFunction[ParquetSchemaType, (String, Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(intColumn(n), v.asInstanceOf[Number].intValue.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: String, v: Any) => FilterApi.gtEq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case ParquetFloatType =>
      (n: String, v: Any) => FilterApi.gtEq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case ParquetDoubleType =>
      (n: String, v: Any) => FilterApi.gtEq(doubleColumn(n), v.asInstanceOf[java.lang.Double])

    case ParquetStringType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: String, v: Any) =>
        FilterApi.gtEq(intColumn(n), dateToDays(v.asInstanceOf[Date]).asInstanceOf[Integer])
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
          f.getOriginalType, f.getPrimitiveTypeName, f.getDecimalMetadata)
      }.toMap
    case _ => Map.empty[String, ParquetSchemaType]
  }

  /**
   * Converts data sources filters to Parquet filter predicates.
   */
  def createFilter(schema: MessageType, predicate: sources.Filter): Option[FilterPredicate] = {
    val nameToType = getFieldMap(schema)

    // Parquet does not allow dots in the column name because dots are used as a column path
    // delimiter. Since Parquet 1.8.2 (PARQUET-389), Parquet accepts the filter predicates
    // with missing columns. The incorrect results could be got from Parquet when we push down
    // filters for the column having dots in the names. Thus, we do not push down such filters.
    // See SPARK-20364.
    def canMakeFilterOn(name: String): Boolean = nameToType.contains(name) && !name.contains(".")

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
      case sources.IsNull(name) if canMakeFilterOn(name) =>
        makeEq.lift(nameToType(name)).map(_(name, null))
      case sources.IsNotNull(name) if canMakeFilterOn(name) =>
        makeNotEq.lift(nameToType(name)).map(_(name, null))

      case sources.EqualTo(name, value) if canMakeFilterOn(name) =>
        makeEq.lift(nameToType(name)).map(_(name, value))
      case sources.Not(sources.EqualTo(name, value)) if canMakeFilterOn(name) =>
        makeNotEq.lift(nameToType(name)).map(_(name, value))

      case sources.EqualNullSafe(name, value) if canMakeFilterOn(name) =>
        makeEq.lift(nameToType(name)).map(_(name, value))
      case sources.Not(sources.EqualNullSafe(name, value)) if canMakeFilterOn(name) =>
        makeNotEq.lift(nameToType(name)).map(_(name, value))

      case sources.LessThan(name, value) if canMakeFilterOn(name) =>
        makeLt.lift(nameToType(name)).map(_(name, value))
      case sources.LessThanOrEqual(name, value) if canMakeFilterOn(name) =>
        makeLtEq.lift(nameToType(name)).map(_(name, value))

      case sources.GreaterThan(name, value) if canMakeFilterOn(name) =>
        makeGt.lift(nameToType(name)).map(_(name, value))
      case sources.GreaterThanOrEqual(name, value) if canMakeFilterOn(name) =>
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

      case sources.StringStartsWith(name, prefix) if pushDownStartWith && canMakeFilterOn(name) =>
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
