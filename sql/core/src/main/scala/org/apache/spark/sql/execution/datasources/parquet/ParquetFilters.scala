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

import java.io.Serializable

import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.filter2.predicate._
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.OriginalType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import org.apache.spark.sql.sources
import org.apache.spark.sql.types._

private[sql] object ParquetFilters {
  case class SetInFilter[T <: Comparable[T]](
    valueSet: Set[T]) extends UserDefinedPredicate[T] with Serializable {

    override def keep(value: T): Boolean = {
      value != null && valueSet.contains(value)
    }

    override def canDrop(statistics: Statistics[T]): Boolean = false

    override def inverseCanDrop(statistics: Statistics[T]): Boolean = false
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

    // See https://issues.apache.org/jira/browse/SPARK-11153
    /*
    // Binary.fromString and Binary.fromByteArray don't accept null values
    case StringType =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(s => Binary.fromByteArray(s.asInstanceOf[String].getBytes("utf-8"))).orNull)
    case BinaryType =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromByteArray(v.asInstanceOf[Array[Byte]])).orNull)
     */
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

    // See https://issues.apache.org/jira/browse/SPARK-11153
    /*
    case StringType =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(s => Binary.fromByteArray(s.asInstanceOf[String].getBytes("utf-8"))).orNull)
    case BinaryType =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromByteArray(v.asInstanceOf[Array[Byte]])).orNull)
     */
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

    // See https://issues.apache.org/jira/browse/SPARK-11153
    /*
    case StringType =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n),
          Binary.fromByteArray(v.asInstanceOf[String].getBytes("utf-8")))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[Array[Byte]]))
     */
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

    // See https://issues.apache.org/jira/browse/SPARK-11153
    /*
    case StringType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n),
          Binary.fromByteArray(v.asInstanceOf[String].getBytes("utf-8")))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[Array[Byte]]))
     */
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

    // See https://issues.apache.org/jira/browse/SPARK-11153
    /*
    case StringType =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n),
          Binary.fromByteArray(v.asInstanceOf[String].getBytes("utf-8")))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[Array[Byte]]))
     */
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

    // See https://issues.apache.org/jira/browse/SPARK-11153
    /*
    case StringType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n),
          Binary.fromByteArray(v.asInstanceOf[String].getBytes("utf-8")))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[Array[Byte]]))
     */
  }

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

    // See https://issues.apache.org/jira/browse/SPARK-11153
    /*
    case StringType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(binaryColumn(n),
          SetInFilter(v.map(s => Binary.fromByteArray(s.asInstanceOf[String].getBytes("utf-8")))))
    case BinaryType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(binaryColumn(n),
          SetInFilter(v.map(e => Binary.fromByteArray(e.asInstanceOf[Array[Byte]]))))
     */
  }

  /**
   * Converts data sources filters to Parquet filter predicates.
   */
  def createFilter(schema: StructType, predicate: sources.Filter): Option[FilterPredicate] = {
    val dataTypeOf = schema.map(f => f.name -> f.dataType).toMap

    relaxParquetValidTypeMap

    // NOTE:
    //
    // For any comparison operator `cmp`, both `a cmp NULL` and `NULL cmp a` evaluate to `NULL`,
    // which can be casted to `false` implicitly. Please refer to the `eval` method of these
    // operators and the `SimplifyFilters` rule for details.

    // Hyukjin:
    // I added [[EqualNullSafe]] with [[org.apache.parquet.filter2.predicate.Operators.Eq]].
    // So, it performs equality comparison identically when given [[sources.Filter]] is [[EqualTo]].
    // The reason why I did this is, that the actual Parquet filter checks null-safe equality
    // comparison.
    // So I added this and maybe [[EqualTo]] should be changed. It still seems fine though, because
    // physical planning does not set `NULL` to [[EqualTo]] but changes it to [[IsNull]] and etc.
    // Probably I missed something and obviously this should be changed.

    predicate match {
      case sources.IsNull(name) =>
        makeEq.lift(dataTypeOf(name)).map(_(name, null))
      case sources.IsNotNull(name) =>
        makeNotEq.lift(dataTypeOf(name)).map(_(name, null))

      case sources.EqualTo(name, value) =>
        makeEq.lift(dataTypeOf(name)).map(_(name, value))
      case sources.Not(sources.EqualTo(name, value)) =>
        makeNotEq.lift(dataTypeOf(name)).map(_(name, value))

      case sources.EqualNullSafe(name, value) =>
        makeEq.lift(dataTypeOf(name)).map(_(name, value))
      case sources.Not(sources.EqualNullSafe(name, value)) =>
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

      case _ => None
    }
  }

  // !! HACK ALERT !!
  //
  // This lazy val is a workaround for PARQUET-201, and should be removed once we upgrade to
  // parquet-mr 1.8.1 or higher versions.
  //
  // In Parquet, not all types of columns can be used for filter push-down optimization.  The set
  // of valid column types is controlled by `ValidTypeMap`.  Unfortunately, in parquet-mr 1.7.0 and
  // prior versions, the limitation is too strict, and doesn't allow `BINARY (ENUM)` columns to be
  // pushed down.
  //
  // This restriction is problematic for Spark SQL, because Spark SQL doesn't have a type that maps
  // to Parquet original type `ENUM` directly, and always converts `ENUM` to `StringType`.  Thus,
  // a predicate involving a `ENUM` field can be pushed-down as a string column, which is perfectly
  // legal except that it fails the `ValidTypeMap` check.
  //
  // Here we add `BINARY (ENUM)` into `ValidTypeMap` lazily via reflection to workaround this issue.
  private lazy val relaxParquetValidTypeMap: Unit = {
    val constructor = Class
      .forName(classOf[ValidTypeMap].getCanonicalName + "$FullTypeDescriptor")
      .getDeclaredConstructor(classOf[PrimitiveTypeName], classOf[OriginalType])

    constructor.setAccessible(true)
    val enumTypeDescriptor = constructor
      .newInstance(PrimitiveTypeName.BINARY, OriginalType.ENUM)
      .asInstanceOf[AnyRef]

    val addMethod = classOf[ValidTypeMap].getDeclaredMethods.find(_.getName == "add").get
    addMethod.setAccessible(true)
    addMethod.invoke(null, classOf[Binary], enumTypeDescriptor)
  }
}
