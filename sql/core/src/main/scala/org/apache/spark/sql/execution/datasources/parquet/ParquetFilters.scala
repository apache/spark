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

import org.apache.parquet.filter2.predicate._
import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.io.api.Binary

import org.apache.spark.sql.sources
import org.apache.spark.sql.types._

/**
 * Some utility function to convert Spark data source filters to Parquet filters.
 */
private[parquet] object ParquetFilters {

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
        Option(v).map(s => Binary.fromString(s.asInstanceOf[String])).orNull)
    case BinaryType =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]])).orNull)
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
        Option(v).map(s => Binary.fromString(s.asInstanceOf[String])).orNull)
    case BinaryType =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]])).orNull)
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
        FilterApi.lt(binaryColumn(n),
          Binary.fromString(v.asInstanceOf[String]))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
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
        FilterApi.ltEq(binaryColumn(n),
          Binary.fromString(v.asInstanceOf[String]))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
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
        FilterApi.gt(binaryColumn(n),
          Binary.fromString(v.asInstanceOf[String]))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
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
        FilterApi.gtEq(binaryColumn(n),
          Binary.fromString(v.asInstanceOf[String]))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
  }

  /**
   * Returns a map from name of the column to the data type, if predicate push down applies
   * (i.e. not an optional field).
   *
   * SPARK-11955: The optional fields will have metadata StructType.metadataKeyForOptionalField.
   * These fields only exist in one side of merged schemas. Due to that, we can't push down filters
   * using such fields, otherwise Parquet library will throw exception (PARQUET-389).
   * Here we filter out such fields.
   */
  private def getFieldMap(dataType: DataType): Map[String, DataType] = dataType match {
    case StructType(fields) =>
      // Here we don't flatten the fields in the nested schema but just look up through
      // root fields. Currently, accessing to nested fields does not push down filters
      // and it does not support to create filters for them.
      fields.filter { f =>
        !f.metadata.contains(StructType.metadataKeyForOptionalField) ||
          !f.metadata.getBoolean(StructType.metadataKeyForOptionalField)
      }.map(f => f.name -> f.dataType).toMap
    case _ => Map.empty[String, DataType]
  }

  /**
   * Converts data sources filters to Parquet filter predicates.
   */
  def createFilter(schema: StructType, predicate: sources.Filter): Option[FilterPredicate] = {
    val dataTypeOf = getFieldMap(schema)

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
      case sources.IsNull(name) if dataTypeOf.contains(name) =>
        makeEq.lift(dataTypeOf(name)).map(_(name, null))
      case sources.IsNotNull(name) if dataTypeOf.contains(name) =>
        makeNotEq.lift(dataTypeOf(name)).map(_(name, null))

      case sources.EqualTo(name, value) if dataTypeOf.contains(name) =>
        makeEq.lift(dataTypeOf(name)).map(_(name, value))
      case sources.Not(sources.EqualTo(name, value)) if dataTypeOf.contains(name) =>
        makeNotEq.lift(dataTypeOf(name)).map(_(name, value))

      case sources.EqualNullSafe(name, value) if dataTypeOf.contains(name) =>
        makeEq.lift(dataTypeOf(name)).map(_(name, value))
      case sources.Not(sources.EqualNullSafe(name, value)) if dataTypeOf.contains(name) =>
        makeNotEq.lift(dataTypeOf(name)).map(_(name, value))

      case sources.LessThan(name, value) if dataTypeOf.contains(name) =>
        makeLt.lift(dataTypeOf(name)).map(_(name, value))
      case sources.LessThanOrEqual(name, value) if dataTypeOf.contains(name) =>
        makeLtEq.lift(dataTypeOf(name)).map(_(name, value))

      case sources.GreaterThan(name, value) if dataTypeOf.contains(name) =>
        makeGt.lift(dataTypeOf(name)).map(_(name, value))
      case sources.GreaterThanOrEqual(name, value) if dataTypeOf.contains(name) =>
        makeGtEq.lift(dataTypeOf(name)).map(_(name, value))

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

      case sources.In(name, values) if dataTypeOf.contains(name) =>
        values.flatMap { v =>
          makeEq.lift(dataTypeOf(name)).map(_(name, v))
        }.reduceLeftOption(FilterApi.or)

      case _ => None
    }
  }
}
