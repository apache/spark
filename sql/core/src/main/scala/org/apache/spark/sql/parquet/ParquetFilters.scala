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

import org.apache.hadoop.conf.Configuration

import parquet.filter._
import parquet.filter.ColumnPredicates._
import parquet.column.ColumnReader

import com.google.common.io.BaseEncoding

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkSqlSerializer

object ParquetFilters {
  val PARQUET_FILTER_DATA = "org.apache.spark.sql.parquet.row.filter"

  def createFilter(filterExpressions: Seq[Expression]): UnboundRecordFilter = {
    def createEqualityFilter(name: String, literal: Literal) = literal.dataType match {
      case BooleanType =>
        ComparisonFilter.createBooleanFilter(name, literal.value.asInstanceOf[Boolean])
      case IntegerType =>
        ComparisonFilter.createIntFilter(name, (x: Int) => x == literal.value.asInstanceOf[Int])
      case LongType =>
        ComparisonFilter.createLongFilter(name, (x: Long) => x == literal.value.asInstanceOf[Long])
      case DoubleType =>
        ComparisonFilter.createDoubleFilter(
          name,
          (x: Double) => x == literal.value.asInstanceOf[Double])
      case FloatType =>
        ComparisonFilter.createFloatFilter(
          name,
          (x: Float) => x == literal.value.asInstanceOf[Float])
      case StringType =>
        ComparisonFilter.createStringFilter(name, literal.value.asInstanceOf[String])
    }
    def createLessThanFilter(name: String, literal: Literal) = literal.dataType match {
      case IntegerType =>
        ComparisonFilter.createIntFilter(name, (x: Int) => x < literal.value.asInstanceOf[Int])
      case LongType =>
        ComparisonFilter.createLongFilter(name, (x: Long) => x < literal.value.asInstanceOf[Long])
      case DoubleType =>
        ComparisonFilter.createDoubleFilter(
          name,
          (x: Double) => x < literal.value.asInstanceOf[Double])
      case FloatType =>
        ComparisonFilter.createFloatFilter(
          name,
          (x: Float) => x < literal.value.asInstanceOf[Float])
    }
    def createLessThanOrEqualFilter(name: String, literal: Literal) = literal.dataType match {
      case IntegerType =>
        ComparisonFilter.createIntFilter(name, (x: Int) => x <= literal.value.asInstanceOf[Int])
      case LongType =>
        ComparisonFilter.createLongFilter(name, (x: Long) => x <= literal.value.asInstanceOf[Long])
      case DoubleType =>
        ComparisonFilter.createDoubleFilter(
          name,
          (x: Double) => x <= literal.value.asInstanceOf[Double])
      case FloatType =>
        ComparisonFilter.createFloatFilter(
          name,
          (x: Float) => x <= literal.value.asInstanceOf[Float])
    }
    // TODO: combine these two types somehow?
    def createGreaterThanFilter(name: String, literal: Literal) = literal.dataType match {
      case IntegerType =>
        ComparisonFilter.createIntFilter(name, (x: Int) => x > literal.value.asInstanceOf[Int])
      case LongType =>
        ComparisonFilter.createLongFilter(name, (x: Long) => x > literal.value.asInstanceOf[Long])
      case DoubleType =>
        ComparisonFilter.createDoubleFilter(
          name,
          (x: Double) => x > literal.value.asInstanceOf[Double])
      case FloatType =>
        ComparisonFilter.createFloatFilter(
          name,
          (x: Float) => x > literal.value.asInstanceOf[Float])
    }
    def createGreaterThanOrEqualFilter(name: String, literal: Literal) = literal.dataType match {
      case IntegerType =>
        ComparisonFilter.createIntFilter(name, (x: Int) => x >= literal.value.asInstanceOf[Int])
      case LongType =>
        ComparisonFilter.createLongFilter(name, (x: Long) => x >= literal.value.asInstanceOf[Long])
      case DoubleType =>
        ComparisonFilter.createDoubleFilter(
          name,
          (x: Double) => x >= literal.value.asInstanceOf[Double])
      case FloatType =>
        ComparisonFilter.createFloatFilter(
          name,
          (x: Float) => x >= literal.value.asInstanceOf[Float])
    }
    // TODO: can we actually rely on the predicate being normalized as in expression < literal?
    // That would simplify this pattern matching
    // TODO: we currently only filter on non-nullable (Parquet REQUIRED) attributes until
    // https://github.com/Parquet/parquet-mr/issues/371
    // has been resolved
    val filters: Seq[UnboundRecordFilter] = filterExpressions.collect {
      case Equals(left: Literal, right: NamedExpression) if !right.nullable =>
        createEqualityFilter(right.name, left)
      case Equals(left: NamedExpression, right: Literal) if !left.nullable =>
        createEqualityFilter(left.name, right)
      case LessThan(left: Literal, right: NamedExpression) if !right.nullable =>
        createLessThanFilter(right.name, left)
      case LessThan(left: NamedExpression, right: Literal) if !left.nullable =>
        createLessThanFilter(left.name, right)
      case LessThanOrEqual(left: Literal, right: NamedExpression) if !right.nullable =>
        createLessThanOrEqualFilter(right.name, left)
      case LessThanOrEqual(left: NamedExpression, right: Literal) if !left.nullable =>
        createLessThanOrEqualFilter(left.name, right)
      case GreaterThan(left: Literal, right: NamedExpression) if !right.nullable =>
        createGreaterThanFilter(right.name, left)
      case GreaterThan(left: NamedExpression, right: Literal) if !left.nullable =>
        createGreaterThanFilter(left.name, right)
      case GreaterThanOrEqual(left: Literal, right: NamedExpression) if !right.nullable =>
        createGreaterThanOrEqualFilter(right.name, left)
      case GreaterThanOrEqual(left: NamedExpression, right: Literal) if !left.nullable =>
        createGreaterThanOrEqualFilter(left.name, right)
    }
    // TODO: How about disjunctions? (Or-ed)
    if (filters.length > 0) filters.reduce(AndRecordFilter.and) else null
  }

  // Note: Inside the Hadoop API we only have access to `Configuration`, not to
  // [[SparkContext]], so we cannot use broadcasts to convey the actual filter
  // predicate.
  def serializeFilterExpressions(filters: Seq[Expression], conf: Configuration): Unit = {
    val serialized: Array[Byte] = SparkSqlSerializer.serialize(filters)
    val encoded: String = BaseEncoding.base64().encode(serialized)
    conf.set(PARQUET_FILTER_DATA, encoded)
  }

  // Note: Inside the Hadoop API we only have access to `Configuration`, not to
  // [[SparkContext]], so we cannot use broadcasts to convey the actual filter
  // predicate.
  def deserializeFilterExpressions(conf: Configuration): Option[Seq[Expression]] = {
    val data = conf.get(PARQUET_FILTER_DATA)
    if (data != null) {
      val decoded: Array[Byte] = BaseEncoding.base64().decode(data)
      Some(SparkSqlSerializer.deserialize(decoded))
    } else {
      None
    }
  }
}

class ComparisonFilter(
    private val columnName: String,
    private var filter: UnboundRecordFilter)
  extends UnboundRecordFilter {
  override def bind(readers: java.lang.Iterable[ColumnReader]): RecordFilter = {
    filter.bind(readers)
  }
}

object ComparisonFilter {
  def createBooleanFilter(columnName: String, value: Boolean): UnboundRecordFilter =
    new ComparisonFilter(
      columnName,
      ColumnRecordFilter.column(
        columnName,
        ColumnPredicates.applyFunctionToBoolean(
          new BooleanPredicateFunction {
            def functionToApply(input: Boolean): Boolean = input == value
          }
        )))
  def createStringFilter(columnName: String, value: String): UnboundRecordFilter =
    new ComparisonFilter(
      columnName,
      ColumnRecordFilter.column(
        columnName,
        ColumnPredicates.applyFunctionToString (
          new ColumnPredicates.PredicateFunction[String]  {
            def functionToApply(input: String): Boolean = input == value
          }
        )))
  def createIntFilter(columnName: String, func: Int => Boolean): UnboundRecordFilter =
    new ComparisonFilter(
      columnName,
      ColumnRecordFilter.column(
        columnName,
        ColumnPredicates.applyFunctionToInteger(
          new IntegerPredicateFunction {
            def functionToApply(input: Int) = func(input)
          }
        )))
  def createLongFilter(columnName: String, func: Long => Boolean): UnboundRecordFilter =
    new ComparisonFilter(
      columnName,
      ColumnRecordFilter.column(
        columnName,
        ColumnPredicates.applyFunctionToLong(
          new LongPredicateFunction {
            def functionToApply(input: Long) = func(input)
          }
        )))
  def createDoubleFilter(columnName: String, func: Double => Boolean): UnboundRecordFilter =
    new ComparisonFilter(
      columnName,
      ColumnRecordFilter.column(
        columnName,
        ColumnPredicates.applyFunctionToDouble(
          new DoublePredicateFunction {
            def functionToApply(input: Double) = func(input)
          }
        )))
  def createFloatFilter(columnName: String, func: Float => Boolean): UnboundRecordFilter =
    new ComparisonFilter(
      columnName,
      ColumnRecordFilter.column(
        columnName,
        ColumnPredicates.applyFunctionToFloat(
          new FloatPredicateFunction {
            def functionToApply(input: Float) = func(input)
          }
        )))
}


