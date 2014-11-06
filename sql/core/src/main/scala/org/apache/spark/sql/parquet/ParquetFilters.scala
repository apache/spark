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

import org.apache.hadoop.conf.Configuration

import parquet.filter2.compat.FilterCompat
import parquet.filter2.compat.FilterCompat._
import parquet.filter2.predicate.FilterPredicate
import parquet.filter2.predicate.FilterApi
import parquet.filter2.predicate.FilterApi._
import parquet.io.api.Binary
import parquet.column.ColumnReader

import com.google.common.io.BaseEncoding

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.expressions.{Predicate => CatalystPredicate}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkSqlSerializer

private[sql] object ParquetFilters {
  val PARQUET_FILTER_DATA = "org.apache.spark.sql.parquet.row.filter"
  // set this to false if pushdown should be disabled
  val PARQUET_FILTER_PUSHDOWN_ENABLED = "spark.sql.hints.parquetFilterPushdown"

  def createRecordFilter(filterExpressions: Seq[Expression]): Filter = {
    val filters: Seq[CatalystFilter] = filterExpressions.collect {
      case (expression: Expression) if createFilter(expression).isDefined =>
        createFilter(expression).get
    }
    if (filters.length > 0) FilterCompat.get(filters.reduce(FilterApi.and)) else null
  }

  def createFilter(expression: Expression): Option[CatalystFilter] ={
    def createEqualityFilter(
        name: String,
        literal: Literal,
        predicate: CatalystPredicate) = literal.dataType match {
      case BooleanType =>
        ComparisonFilter.createBooleanFilter(
          name, 
          literal.value.asInstanceOf[Boolean], 
          predicate)
      case IntegerType =>
        new ComparisonFilter(
          name,
          FilterApi.eq(intColumn(name), literal.value.asInstanceOf[Integer]),
          predicate)
      case LongType =>
        new ComparisonFilter(
          name,
          FilterApi.eq(longColumn(name), literal.value.asInstanceOf[java.lang.Long]),
          predicate)
      case DoubleType =>
        new ComparisonFilter(
          name,
          FilterApi.eq(doubleColumn(name), literal.value.asInstanceOf[java.lang.Double]),
          predicate)
      case FloatType =>
        new ComparisonFilter(
          name,
          FilterApi.eq(floatColumn(name), literal.value.asInstanceOf[java.lang.Float]),
          predicate)
      case StringType =>
        ComparisonFilter.createStringFilter(
          name, 
          literal.value.asInstanceOf[String], 
          predicate)
    }

    def createLessThanFilter(
        name: String,
        literal: Literal,
        predicate: CatalystPredicate) = literal.dataType match {
      case IntegerType =>
       new ComparisonFilter(
          name, 
          FilterApi.lt(intColumn(name), literal.value.asInstanceOf[Integer]),
          predicate)
      case LongType =>
        new ComparisonFilter(
          name,
          FilterApi.lt(longColumn(name), literal.value.asInstanceOf[java.lang.Long]),
          predicate)
      case DoubleType =>
        new ComparisonFilter(
          name,
          FilterApi.lt(doubleColumn(name), literal.value.asInstanceOf[java.lang.Double]),
          predicate)
      case FloatType =>
        new ComparisonFilter(
          name,
          FilterApi.lt(floatColumn(name), literal.value.asInstanceOf[java.lang.Float]),
          predicate)
    }
    def createLessThanOrEqualFilter(
        name: String,
        literal: Literal,
        predicate: CatalystPredicate) = literal.dataType match {
      case IntegerType =>
        new ComparisonFilter(
          name,
          FilterApi.ltEq(intColumn(name), literal.value.asInstanceOf[Integer]),
          predicate)
      case LongType =>
        new ComparisonFilter(
          name,
          FilterApi.ltEq(longColumn(name), literal.value.asInstanceOf[java.lang.Long]),
          predicate)
      case DoubleType =>
        new ComparisonFilter(
          name,
          FilterApi.ltEq(doubleColumn(name), literal.value.asInstanceOf[java.lang.Double]),
          predicate)
      case FloatType =>
        new ComparisonFilter(
          name,
          FilterApi.ltEq(floatColumn(name), literal.value.asInstanceOf[java.lang.Float]),
          predicate)
    }
    // TODO: combine these two types somehow?
    def createGreaterThanFilter(
        name: String,
        literal: Literal,
        predicate: CatalystPredicate) = literal.dataType match {
      case IntegerType =>
        new ComparisonFilter(
          name,
          FilterApi.gt(intColumn(name), literal.value.asInstanceOf[Integer]),
          predicate)
      case LongType =>
        new ComparisonFilter(
          name,
          FilterApi.gt(longColumn(name), literal.value.asInstanceOf[java.lang.Long]),
          predicate)
      case DoubleType =>
        new ComparisonFilter(
          name,
          FilterApi.gt(doubleColumn(name), literal.value.asInstanceOf[java.lang.Double]),
          predicate)
      case FloatType =>
        new ComparisonFilter(
          name,
          FilterApi.gt(floatColumn(name), literal.value.asInstanceOf[java.lang.Float]),
          predicate)
    }
    def createGreaterThanOrEqualFilter(
        name: String,
        literal: Literal,
        predicate: CatalystPredicate) = literal.dataType match {
      case IntegerType =>
        new ComparisonFilter(
          name,
          FilterApi.gtEq(intColumn(name), literal.value.asInstanceOf[Integer]),
          predicate)
      case LongType =>
        new ComparisonFilter(
          name,
          FilterApi.gtEq(longColumn(name), literal.value.asInstanceOf[java.lang.Long]),
          predicate)
      case DoubleType =>
        new ComparisonFilter(
          name,
          FilterApi.gtEq(doubleColumn(name), literal.value.asInstanceOf[java.lang.Double]),
          predicate)
      case FloatType =>
        new ComparisonFilter(
          name,
          FilterApi.gtEq(floatColumn(name), literal.value.asInstanceOf[java.lang.Float]),
          predicate)
    }

    /**
     * TODO: we currently only filter on non-nullable (Parquet REQUIRED) attributes until
     * https://github.com/Parquet/parquet-mr/issues/371
     * has been resolved.
     */
    expression match {
      case p @ Or(left: Expression, right: Expression)
          if createFilter(left).isDefined && createFilter(right).isDefined => {
        // If either side of this Or-predicate is empty then this means
        // it contains a more complex comparison than between attribute and literal
        // (e.g., it contained a CAST). The only safe thing to do is then to disregard
        // this disjunction, which could be contained in a conjunction. If it stands
        // alone then it is also safe to drop it, since a Null return value of this
        // function is interpreted as having no filters at all.
        val leftFilter = createFilter(left).get
        val rightFilter = createFilter(right).get
        Some(new OrFilter(leftFilter, rightFilter))
      }
      case p @ And(left: Expression, right: Expression) => {
        // This treats nested conjunctions; since either side of the conjunction
        // may contain more complex filter expressions we may actually generate
        // strictly weaker filter predicates in the process.
        val leftFilter = createFilter(left)
        val rightFilter = createFilter(right)
        (leftFilter, rightFilter) match {
          case (None, Some(filter)) => Some(filter)
          case (Some(filter), None) => Some(filter)
          case (Some(leftF), Some(rightF)) =>
            Some(new AndFilter(leftF, rightF))
          case _ => None
        }
      }
      case p @ EqualTo(left: Literal, right: NamedExpression) =>
        Some(createEqualityFilter(right.name, left, p))
      case p @ EqualTo(left: NamedExpression, right: Literal) =>
        Some(createEqualityFilter(left.name, right, p))
      case p @ LessThan(left: Literal, right: NamedExpression) =>
        Some(createLessThanFilter(right.name, left, p))
      case p @ LessThan(left: NamedExpression, right: Literal) =>
        Some(createLessThanFilter(left.name, right, p))
      case p @ LessThanOrEqual(left: Literal, right: NamedExpression) =>
        Some(createLessThanOrEqualFilter(right.name, left, p))
      case p @ LessThanOrEqual(left: NamedExpression, right: Literal) =>
        Some(createLessThanOrEqualFilter(left.name, right, p))
      case p @ GreaterThan(left: Literal, right: NamedExpression) =>
        Some(createGreaterThanFilter(right.name, left, p))
      case p @ GreaterThan(left: NamedExpression, right: Literal) =>
        Some(createGreaterThanFilter(left.name, right, p))
      case p @ GreaterThanOrEqual(left: Literal, right: NamedExpression) =>
        Some(createGreaterThanOrEqualFilter(right.name, left, p))
      case p @ GreaterThanOrEqual(left: NamedExpression, right: Literal) =>
        Some(createGreaterThanOrEqualFilter(left.name, right, p))
      case _ => None
    }
  }

  /**
   * Note: Inside the Hadoop API we only have access to `Configuration`, not to
   * [[org.apache.spark.SparkContext]], so we cannot use broadcasts to convey
   * the actual filter predicate.
   */
  def serializeFilterExpressions(filters: Seq[Expression], conf: Configuration): Unit = {
    if (filters.length > 0) {
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

  /**
   * Try to find the given expression in the tree of filters in order to
   * determine whether it is safe to remove it from the higher level filters. Note
   * that strictly speaking we could stop the search whenever an expression is found
   * that contains this expression as subexpression (e.g., when searching for "a"
   * and "(a or c)" is found) but we don't care about optimizations here since the
   * filter tree is assumed to be small.
   *
   * @param filter The [[org.apache.spark.sql.parquet.CatalystFilter]] to expand
   *               and search
   * @param expression The expression to look for
   * @return An optional [[org.apache.spark.sql.parquet.CatalystFilter]] that
   *         contains the expression.
   */
  def findExpression(
      filter: CatalystFilter,
      expression: Expression): Option[CatalystFilter] = filter match {
    case f @ OrFilter(_, leftFilter, rightFilter, _) =>
      if (f.predicate == expression) {
        Some(f)
      } else {
        val left = findExpression(leftFilter, expression)
        if (left.isDefined) left else findExpression(rightFilter, expression)
      }
    case f @ AndFilter(_, leftFilter, rightFilter, _) =>
      if (f.predicate == expression) {
        Some(f)
      } else {
        val left = findExpression(leftFilter, expression)
        if (left.isDefined) left else findExpression(rightFilter, expression)
      }
    case f @ ComparisonFilter(_, _, predicate) =>
      if (predicate == expression) Some(f) else None
    case _ => None
  }
}

abstract private[parquet] class CatalystFilter(
    @transient val predicate: CatalystPredicate) extends FilterPredicate

private[parquet] case class ComparisonFilter(
    val columnName: String,
    private var filter: FilterPredicate,
    @transient override val predicate: CatalystPredicate)
  extends CatalystFilter(predicate) {
  override def accept[R](visitor: FilterPredicate.Visitor[R]): R = {
    filter.accept(visitor)
  }
}

private[parquet] case class OrFilter(
    private var filter: FilterPredicate,
    @transient val left: CatalystFilter,
    @transient val right: CatalystFilter,
    @transient override val predicate: Or)
  extends CatalystFilter(predicate) {
  def this(l: CatalystFilter, r: CatalystFilter) =
    this(
      FilterApi.or(l, r),
      l,
      r,
      Or(l.predicate, r.predicate))

  override def accept[R](visitor: FilterPredicate.Visitor[R]): R  = {
    filter.accept(visitor);
  }

}

private[parquet] case class AndFilter(
    private var filter: FilterPredicate,
    @transient val left: CatalystFilter,
    @transient val right: CatalystFilter,
    @transient override val predicate: And)
  extends CatalystFilter(predicate) {
  def this(l: CatalystFilter, r: CatalystFilter) =
    this(
      FilterApi.and(l, r),
      l,
      r,
      And(l.predicate, r.predicate))

  override def accept[R](visitor: FilterPredicate.Visitor[R]): R = {
    filter.accept(visitor);
  }

}

private[parquet] object ComparisonFilter {
  def createBooleanFilter(
      columnName: String,
      value: Boolean,
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.eq(booleanColumn(columnName), value.asInstanceOf[java.lang.Boolean]),
      predicate)

  def createStringFilter(
      columnName: String,
      value: String,
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.eq(binaryColumn(columnName), Binary.fromString(value)),
      predicate)
}
