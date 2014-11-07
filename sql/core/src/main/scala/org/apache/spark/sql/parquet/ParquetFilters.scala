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
import java.sql.{Date, Timestamp}

import org.apache.hadoop.conf.Configuration

import parquet.common.schema.ColumnPath
import parquet.filter2.compat.FilterCompat
import parquet.filter2.compat.FilterCompat._
import parquet.filter2.predicate.Operators.{Column, SupportsLtGt}
import parquet.filter2.predicate.{FilterApi, FilterPredicate}
import parquet.filter2.predicate.FilterApi._
import parquet.io.api.Binary
import parquet.column.ColumnReader

import com.google.common.io.BaseEncoding

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.types.decimal.Decimal
import org.apache.spark.sql.catalyst.expressions.{Predicate => CatalystPredicate}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.parquet.ParquetColumns._

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

  def createFilter(expression: Expression): Option[CatalystFilter] = {
    def createEqualityFilter(
        name: String,
        literal: Literal,
        predicate: CatalystPredicate) = literal.dataType match {
      case BooleanType =>
        ComparisonFilter.createBooleanEqualityFilter(
          name, 
          literal.value.asInstanceOf[Boolean],
          predicate)
      case ByteType =>
        new ComparisonFilter(
          name,
          FilterApi.eq(byteColumn(name), literal.value.asInstanceOf[java.lang.Byte]),
          predicate)
      case ShortType =>
        new ComparisonFilter(
          name,
          FilterApi.eq(shortColumn(name), literal.value.asInstanceOf[java.lang.Short]),
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
        ComparisonFilter.createStringEqualityFilter(
          name, 
          literal.value.asInstanceOf[String], 
          predicate)
      case BinaryType =>
        ComparisonFilter.createBinaryEqualityFilter(
          name,
          literal.value.asInstanceOf[Array[Byte]],
          predicate)
      case DateType =>
        new ComparisonFilter(
          name,
          FilterApi.eq(dateColumn(name), new WrappedDate(literal.value.asInstanceOf[Date])),
          predicate)
      case TimestampType =>
        new ComparisonFilter(
          name,
          FilterApi.eq(timestampColumn(name),
            new WrappedTimestamp(literal.value.asInstanceOf[Timestamp])),
          predicate)
      case DecimalType.Unlimited =>
        new ComparisonFilter(
          name,
          FilterApi.eq(decimalColumn(name), literal.value.asInstanceOf[Decimal]),
          predicate)
    }

    def createLessThanFilter(
        name: String,
        literal: Literal,
        predicate: CatalystPredicate) = literal.dataType match {
      case ByteType =>
        new ComparisonFilter(
          name,
          FilterApi.lt(byteColumn(name), literal.value.asInstanceOf[java.lang.Byte]),
          predicate)
      case ShortType =>
        new ComparisonFilter(
          name,
          FilterApi.lt(shortColumn(name), literal.value.asInstanceOf[java.lang.Short]),
          predicate)
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
      case StringType =>
        ComparisonFilter.createStringLessThanFilter(
          name,
          literal.value.asInstanceOf[String],
          predicate)
      case BinaryType =>
        ComparisonFilter.createBinaryLessThanFilter(
          name,
          literal.value.asInstanceOf[Array[Byte]],
          predicate)
      case DateType =>
        new ComparisonFilter(
          name,
          FilterApi.lt(dateColumn(name), new WrappedDate(literal.value.asInstanceOf[Date])),
          predicate)
      case TimestampType =>
        new ComparisonFilter(
          name,
          FilterApi.lt(timestampColumn(name),
            new WrappedTimestamp(literal.value.asInstanceOf[Timestamp])),
          predicate)
      case DecimalType.Unlimited =>
        new ComparisonFilter(
          name,
          FilterApi.lt(decimalColumn(name), literal.value.asInstanceOf[Decimal]),
          predicate)
    }
    def createLessThanOrEqualFilter(
        name: String,
        literal: Literal,
        predicate: CatalystPredicate) = literal.dataType match {
      case ByteType =>
        new ComparisonFilter(
          name,
          FilterApi.ltEq(byteColumn(name), literal.value.asInstanceOf[java.lang.Byte]),
          predicate)
      case ShortType =>
        new ComparisonFilter(
          name,
          FilterApi.ltEq(shortColumn(name), literal.value.asInstanceOf[java.lang.Short]),
          predicate)
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
      case StringType =>
        ComparisonFilter.createStringLessThanOrEqualFilter(
          name,
          literal.value.asInstanceOf[String],
          predicate)
      case BinaryType =>
        ComparisonFilter.createBinaryLessThanOrEqualFilter(
          name,
          literal.value.asInstanceOf[Array[Byte]],
          predicate)
      case DateType =>
        new ComparisonFilter(
          name,
          FilterApi.ltEq(dateColumn(name), new WrappedDate(literal.value.asInstanceOf[Date])),
          predicate)
      case TimestampType =>
        new ComparisonFilter(
          name,
          FilterApi.ltEq(timestampColumn(name),
            new WrappedTimestamp(literal.value.asInstanceOf[Timestamp])),
          predicate)
      case DecimalType.Unlimited =>
        new ComparisonFilter(
          name,
          FilterApi.ltEq(decimalColumn(name), literal.value.asInstanceOf[Decimal]),
          predicate)
    }
    // TODO: combine these two types somehow?
    def createGreaterThanFilter(
        name: String,
        literal: Literal,
        predicate: CatalystPredicate) = literal.dataType match {
      case ByteType =>
        new ComparisonFilter(
          name,
          FilterApi.gt(byteColumn(name), literal.value.asInstanceOf[java.lang.Byte]),
          predicate)
      case ShortType =>
        new ComparisonFilter(
          name,
          FilterApi.gt(shortColumn(name), literal.value.asInstanceOf[java.lang.Short]),
          predicate)
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
      case StringType =>
        ComparisonFilter.createStringGreaterThanFilter(
          name,
          literal.value.asInstanceOf[String],
          predicate)
      case BinaryType =>
        ComparisonFilter.createBinaryGreaterThanFilter(
          name,
          literal.value.asInstanceOf[Array[Byte]],
          predicate)
      case DateType =>
        new ComparisonFilter(
          name,
          FilterApi.gt(dateColumn(name), new WrappedDate(literal.value.asInstanceOf[Date])),
          predicate)
      case TimestampType =>
        new ComparisonFilter(
          name,
          FilterApi.gt(timestampColumn(name),
            new WrappedTimestamp(literal.value.asInstanceOf[Timestamp])),
          predicate)
      case DecimalType.Unlimited =>
        new ComparisonFilter(
          name,
          FilterApi.gt(decimalColumn(name), literal.value.asInstanceOf[Decimal]),
          predicate)
    }
    def createGreaterThanOrEqualFilter(
        name: String,
        literal: Literal,
        predicate: CatalystPredicate) = literal.dataType match {
      case ByteType =>
        new ComparisonFilter(
          name,
          FilterApi.gtEq(byteColumn(name), literal.value.asInstanceOf[java.lang.Byte]),
          predicate)
      case ShortType =>
        new ComparisonFilter(
          name,
          FilterApi.gtEq(shortColumn(name), literal.value.asInstanceOf[java.lang.Short]),
          predicate)
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
      case StringType =>
        ComparisonFilter.createStringGreaterThanOrEqualFilter(
          name,
          literal.value.asInstanceOf[String],
          predicate)
      case BinaryType =>
        ComparisonFilter.createBinaryGreaterThanOrEqualFilter(
          name,
          literal.value.asInstanceOf[Array[Byte]],
          predicate)
      case DateType =>
        new ComparisonFilter(
          name,
          FilterApi.gtEq(dateColumn(name), new WrappedDate(literal.value.asInstanceOf[Date])),
          predicate)
      case TimestampType =>
        new ComparisonFilter(
          name,
          FilterApi.gtEq(timestampColumn(name),
            new WrappedTimestamp(literal.value.asInstanceOf[Timestamp])),
          predicate)
      case DecimalType.Unlimited =>
        new ComparisonFilter(
          name,
          FilterApi.gtEq(decimalColumn(name), literal.value.asInstanceOf[Decimal]),
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
      case p @ EqualTo(left: Literal, right: NamedExpression) if left.dataType != NullType =>
        Some(createEqualityFilter(right.name, left, p))
      case p @ EqualTo(left: NamedExpression, right: Literal) if right.dataType != NullType =>
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
  def createBooleanEqualityFilter(
      columnName: String,
      value: Boolean,
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.eq(booleanColumn(columnName), value.asInstanceOf[java.lang.Boolean]),
      predicate)

  def createStringEqualityFilter(
      columnName: String,
      value: String,
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.eq(binaryColumn(columnName), Binary.fromString(value)),
      predicate)

  def createStringLessThanFilter(
      columnName: String,
      value: String,
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.lt(binaryColumn(columnName), Binary.fromString(value)),
      predicate)

  def createStringLessThanOrEqualFilter(
      columnName: String,
      value: String,
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.ltEq(binaryColumn(columnName), Binary.fromString(value)),
      predicate)

  def createStringGreaterThanFilter(
      columnName: String,
      value: String,
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.gt(binaryColumn(columnName), Binary.fromString(value)),
      predicate)

  def createStringGreaterThanOrEqualFilter(
      columnName: String,
      value: String,
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.gtEq(binaryColumn(columnName), Binary.fromString(value)),
      predicate)

  def createBinaryEqualityFilter(
      columnName: String,
      value: Array[Byte],
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.eq(binaryColumn(columnName), Binary.fromByteArray(value)),
      predicate)

  def createBinaryLessThanFilter(
      columnName: String,
      value: Array[Byte],
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.lt(binaryColumn(columnName), Binary.fromByteArray(value)),
      predicate)

  def createBinaryLessThanOrEqualFilter(
      columnName: String,
      value: Array[Byte],
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.ltEq(binaryColumn(columnName), Binary.fromByteArray(value)),
      predicate)

  def createBinaryGreaterThanFilter(
      columnName: String,
      value: Array[Byte],
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.gt(binaryColumn(columnName), Binary.fromByteArray(value)),
      predicate)

  def createBinaryGreaterThanOrEqualFilter(
      columnName: String,
      value: Array[Byte],
      predicate: CatalystPredicate): CatalystFilter =
    new ComparisonFilter(
      columnName,
      FilterApi.gtEq(binaryColumn(columnName), Binary.fromByteArray(value)),
      predicate)
}

private[spark] object ParquetColumns {

  def byteColumn(columnPath: String): ByteColumn = {
    new ByteColumn(ColumnPath.fromDotString(columnPath))
  }

  final class ByteColumn(columnPath: ColumnPath)
    extends Column[java.lang.Byte](columnPath, classOf[java.lang.Byte]) with SupportsLtGt

  def shortColumn(columnPath: String): ShortColumn = {
    new ShortColumn(ColumnPath.fromDotString(columnPath))
  }

  final class ShortColumn(columnPath: ColumnPath)
    extends Column[java.lang.Short](columnPath, classOf[java.lang.Short]) with SupportsLtGt


  def dateColumn(columnPath: String): DateColumn = {
    new DateColumn(ColumnPath.fromDotString(columnPath))
  }

  final class DateColumn(columnPath: ColumnPath)
    extends Column[WrappedDate](columnPath, classOf[WrappedDate]) with SupportsLtGt

  def timestampColumn(columnPath: String): TimestampColumn = {
    new TimestampColumn(ColumnPath.fromDotString(columnPath))
  }

  final class TimestampColumn(columnPath: ColumnPath)
    extends Column[WrappedTimestamp](columnPath, classOf[WrappedTimestamp]) with SupportsLtGt

  def decimalColumn(columnPath: String): DecimalColumn = {
    new DecimalColumn(ColumnPath.fromDotString(columnPath))
  }

  final class DecimalColumn(columnPath: ColumnPath)
    extends Column[Decimal](columnPath, classOf[Decimal]) with SupportsLtGt

  final class WrappedDate(val date: Date) extends Comparable[WrappedDate] {

    override def compareTo(other: WrappedDate): Int = {
      date.compareTo(other.date)
    }
  }

  final class WrappedTimestamp(val timestamp: Timestamp) extends Comparable[WrappedTimestamp] {

    override def compareTo(other: WrappedTimestamp): Int = {
      timestamp.compareTo(other.timestamp)
    }
  }
}
