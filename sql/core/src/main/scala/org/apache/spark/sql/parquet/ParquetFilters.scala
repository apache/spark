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

package org.apache.spark.sql.parquet

import java.nio.ByteBuffer

import com.google.common.io.BaseEncoding
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.FilterCompat._
import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources
import org.apache.spark.sql.types._

private[sql] object ParquetFilters {
  val PARQUET_FILTER_DATA = "org.apache.spark.sql.parquet.row.filter"

  def createRecordFilter(filterExpressions: Seq[Expression]): Option[Filter] = {
    filterExpressions.flatMap { filter =>
      createFilter(filter)
    }.reduceOption(FilterApi.and).map(FilterCompat.get)
  }

  private val makeEq: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case BooleanType =>
      (n: String, v: Any) => FilterApi.eq(booleanColumn(n), v.asInstanceOf[java.lang.Boolean])
    case IntegerType =>
      (n: String, v: Any) => FilterApi.eq(intColumn(n), v.asInstanceOf[Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.eq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.eq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.eq(doubleColumn(n), v.asInstanceOf[java.lang.Double])

    // Binary.fromString and Binary.fromByteArray don't accept null values
    case StringType =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(s => Binary.fromByteArray(s.asInstanceOf[UTF8String].getBytes)).orNull)
    case BinaryType =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromByteArray(v.asInstanceOf[Array[Byte]])).orNull)
  }

  private val makeNotEq: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case BooleanType =>
      (n: String, v: Any) => FilterApi.notEq(booleanColumn(n), v.asInstanceOf[java.lang.Boolean])
    case IntegerType =>
      (n: String, v: Any) => FilterApi.notEq(intColumn(n), v.asInstanceOf[Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.notEq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.notEq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.notEq(doubleColumn(n), v.asInstanceOf[java.lang.Double])
    case StringType =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(s => Binary.fromByteArray(s.asInstanceOf[UTF8String].getBytes)).orNull)
    case BinaryType =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromByteArray(v.asInstanceOf[Array[Byte]])).orNull)
  }

  private val makeLt: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Any) => FilterApi.lt(intColumn(n), v.asInstanceOf[Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.lt(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.lt(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.lt(doubleColumn(n), v.asInstanceOf[java.lang.Double])
    case StringType =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[UTF8String].getBytes))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[Array[Byte]]))
  }

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
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[UTF8String].getBytes))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[Array[Byte]]))
  }

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
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[UTF8String].getBytes))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[Array[Byte]]))
  }

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
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[UTF8String].getBytes))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[Array[Byte]]))
  }

  /**
   * Converts data sources filters to Parquet filter predicates.
   */
  def createFilter(schema: StructType, predicate: sources.Filter): Option[FilterPredicate] = {
    val dataTypeOf = schema.map(f => f.name -> f.dataType).toMap

    // NOTE:
    //
    // For any comparison operator `cmp`, both `a cmp NULL` and `NULL cmp a` evaluate to `NULL`,
    // which can be casted to `false` implicitly. Please refer to the `eval` method of these
    // operators and the `SimplifyFilters` rule for details.
    predicate match {
      case sources.IsNull(name) =>
        makeEq.lift(dataTypeOf(name)).map(_(name, null))
      case sources.IsNotNull(name) =>
        makeNotEq.lift(dataTypeOf(name)).map(_(name, null))

      case sources.EqualTo(name, value) =>
        makeEq.lift(dataTypeOf(name)).map(_(name, value))
      case sources.Not(sources.EqualTo(name, value)) =>
        makeNotEq.lift(dataTypeOf(name)).map(_(name, value))

      case sources.LessThan(name, value) =>
        makeLt.lift(dataTypeOf(name)).map(_(name, value))
      case sources.LessThanOrEqual(name, value) =>
        makeLtEq.lift(dataTypeOf(name)).map(_(name, value))

      case sources.GreaterThan(name, value) =>
        makeGt.lift(dataTypeOf(name)).map(_(name, value))
      case sources.GreaterThanOrEqual(name, value) =>
        makeGtEq.lift(dataTypeOf(name)).map(_(name, value))

      case sources.And(lhs, rhs) =>
        (createFilter(schema, lhs) ++ createFilter(schema, rhs)).reduceOption(FilterApi.and)

      case sources.Or(lhs, rhs) =>
        for {
          lhsFilter <- createFilter(schema, lhs)
          rhsFilter <- createFilter(schema, rhs)
        } yield FilterApi.or(lhsFilter, rhsFilter)

      case sources.Not(pred) =>
        createFilter(schema, pred).map(FilterApi.not)

      case _ => None
    }
  }

  /**
   * Converts Catalyst predicate expressions to Parquet filter predicates.
   *
   * @todo This can be removed once we get rid of the old Parquet support.
   */
  def createFilter(predicate: Expression): Option[FilterPredicate] = {
    // NOTE:
    //
    // For any comparison operator `cmp`, both `a cmp NULL` and `NULL cmp a` evaluate to `NULL`,
    // which can be casted to `false` implicitly. Please refer to the `eval` method of these
    // operators and the `SimplifyFilters` rule for details.
    predicate match {
      case IsNull(NamedExpression(name, dataType)) =>
        makeEq.lift(dataType).map(_(name, null))
      case IsNotNull(NamedExpression(name, dataType)) =>
        makeNotEq.lift(dataType).map(_(name, null))

      case EqualTo(NamedExpression(name, _), NonNullLiteral(value, dataType)) =>
        makeEq.lift(dataType).map(_(name, value))
      case EqualTo(Cast(NamedExpression(name, _), dataType), NonNullLiteral(value, _)) =>
        makeEq.lift(dataType).map(_(name, value))
      case EqualTo(NonNullLiteral(value, dataType), NamedExpression(name, _)) =>
        makeEq.lift(dataType).map(_(name, value))
      case EqualTo(NonNullLiteral(value, _), Cast(NamedExpression(name, _), dataType)) =>
        makeEq.lift(dataType).map(_(name, value))

      case Not(EqualTo(NamedExpression(name, _), NonNullLiteral(value, dataType))) =>
        makeNotEq.lift(dataType).map(_(name, value))
      case Not(EqualTo(Cast(NamedExpression(name, _), dataType), NonNullLiteral(value, _))) =>
        makeNotEq.lift(dataType).map(_(name, value))
      case Not(EqualTo(NonNullLiteral(value, dataType), NamedExpression(name, _))) =>
        makeNotEq.lift(dataType).map(_(name, value))
      case Not(EqualTo(NonNullLiteral(value, _), Cast(NamedExpression(name, _), dataType))) =>
        makeNotEq.lift(dataType).map(_(name, value))

      case LessThan(NamedExpression(name, _), NonNullLiteral(value, dataType)) =>
        makeLt.lift(dataType).map(_(name, value))
      case LessThan(Cast(NamedExpression(name, _), dataType), NonNullLiteral(value, _)) =>
        makeLt.lift(dataType).map(_(name, value))
      case LessThan(NonNullLiteral(value, dataType), NamedExpression(name, _)) =>
        makeGt.lift(dataType).map(_(name, value))
      case LessThan(NonNullLiteral(value, _), Cast(NamedExpression(name, _), dataType)) =>
        makeGt.lift(dataType).map(_(name, value))

      case LessThanOrEqual(NamedExpression(name, _), NonNullLiteral(value, dataType)) =>
        makeLtEq.lift(dataType).map(_(name, value))
      case LessThanOrEqual(Cast(NamedExpression(name, _), dataType), NonNullLiteral(value, _)) =>
        makeLtEq.lift(dataType).map(_(name, value))
      case LessThanOrEqual(NonNullLiteral(value, dataType), NamedExpression(name, _)) =>
        makeGtEq.lift(dataType).map(_(name, value))
      case LessThanOrEqual(NonNullLiteral(value, _), Cast(NamedExpression(name, _), dataType)) =>
        makeGtEq.lift(dataType).map(_(name, value))

      case GreaterThan(NamedExpression(name, _), NonNullLiteral(value, dataType)) =>
        makeGt.lift(dataType).map(_(name, value))
      case GreaterThan(Cast(NamedExpression(name, _), dataType), NonNullLiteral(value, _)) =>
        makeGt.lift(dataType).map(_(name, value))
      case GreaterThan(NonNullLiteral(value, dataType), NamedExpression(name, _)) =>
        makeLt.lift(dataType).map(_(name, value))
      case GreaterThan(NonNullLiteral(value, _), Cast(NamedExpression(name, _), dataType)) =>
        makeLt.lift(dataType).map(_(name, value))

      case GreaterThanOrEqual(NamedExpression(name, _), NonNullLiteral(value, dataType)) =>
        makeGtEq.lift(dataType).map(_(name, value))
      case GreaterThanOrEqual(Cast(NamedExpression(name, _), dataType), NonNullLiteral(value, _)) =>
        makeGtEq.lift(dataType).map(_(name, value))
      case GreaterThanOrEqual(NonNullLiteral(value, dataType), NamedExpression(name, _)) =>
        makeLtEq.lift(dataType).map(_(name, value))
      case GreaterThanOrEqual(NonNullLiteral(value, _), Cast(NamedExpression(name, _), dataType)) =>
        makeLtEq.lift(dataType).map(_(name, value))

      case And(lhs, rhs) =>
        (createFilter(lhs) ++ createFilter(rhs)).reduceOption(FilterApi.and)

      case Or(lhs, rhs) =>
        for {
          lhsFilter <- createFilter(lhs)
          rhsFilter <- createFilter(rhs)
        } yield FilterApi.or(lhsFilter, rhsFilter)

      case Not(pred) =>
        createFilter(pred).map(FilterApi.not)

      case _ => None
    }
  }

  /**
   * Note: Inside the Hadoop API we only have access to `Configuration`, not to
   * [[org.apache.spark.SparkContext]], so we cannot use broadcasts to convey
   * the actual filter predicate.
   */
  def serializeFilterExpressions(filters: Seq[Expression], conf: Configuration): Unit = {
    if (filters.nonEmpty) {
      val serialized: Array[Byte] =
        SparkEnv.get.closureSerializer.newInstance().serialize(filters).array()
      val encoded: String = BaseEncoding.base64().encode(serialized)
      conf.set(PARQUET_FILTER_DATA, encoded)
    }
  }

  /**
   * Note: Inside the Hadoop API we only have access to `Configuration`, not to
   * [[org.apache.spark.SparkContext]], so we cannot use broadcasts to convey
   * the actual filter predicate.
   */
  def deserializeFilterExpressions(conf: Configuration): Seq[Expression] = {
    val data = conf.get(PARQUET_FILTER_DATA)
    if (data != null) {
      val decoded: Array[Byte] = BaseEncoding.base64().decode(data)
      SparkEnv.get.closureSerializer.newInstance().deserialize(ByteBuffer.wrap(decoded))
    } else {
      Seq()
    }
  }
}
