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
package org.apache.spark.sql

import java.math.{BigDecimal => JBigDecimal}
import java.time.LocalDate

import scala.reflect.runtime.universe.{typeTag, TypeTag}

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.unsupported
import org.apache.spark.sql.expressions.{ScalarUserDefinedFunction, UserDefinedFunction}

/**
 * Commonly used functions available for DataFrame operations. Using functions defined here
 * provides a little bit more compile-time safety to make sure the function exists.
 *
 * Spark also includes more built-in functions that are less common and are not defined here. You
 * can still access them (and all the functions defined here) using the `functions.expr()` API and
 * calling them through a SQL expression string. You can find the entire list of functions at SQL
 * API documentation of your Spark version, see also <a
 * href="https://spark.apache.org/docs/latest/api/sql/index.html">the latest list</a>
 *
 * As an example, `isnan` is a function that is defined here. You can use `isnan(col("myCol"))` to
 * invoke the `isnan` function. This way the programming language's compiler ensures `isnan`
 * exists and is of the proper form. You can also use `expr("isnan(myCol)")` function to invoke
 * the same function. In this case, Spark itself will ensure `isnan` exists when it analyzes the
 * query.
 *
 * `regr_count` is an example of a function that is built-in but not defined here, because it is
 * less commonly used. To invoke it, use `expr("regr_count(yCol, xCol)")`.
 *
 * This function APIs usually have methods with `Column` signature only because it can support not
 * only `Column` but also other types such as a native string. The other variants currently exist
 * for historical reasons.
 *
 * @groupname udf_funcs UDF functions
 * @groupname agg_funcs Aggregate functions
 * @groupname datetime_funcs Date time functions
 * @groupname sort_funcs Sorting functions
 * @groupname normal_funcs Non-aggregate functions
 * @groupname math_funcs Math functions
 * @groupname misc_funcs Misc functions
 * @groupname window_funcs Window functions
 * @groupname string_funcs String functions
 * @groupname collection_funcs Collection functions
 * @groupname partition_transforms Partition transform functions
 * @groupname Ungrouped Support functions for DataFrames
 *
 * @since 3.4.0
 */
// scalastyle:off
object functions {
// scalastyle:on

  /**
   * Returns a [[Column]] based on the given column name.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def col(colName: String): Column = Column(colName)

  /**
   * Returns a [[Column]] based on the given column name. Alias of [[col]].
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def column(colName: String): Column = col(colName)

  private def createLiteral(f: proto.Expression.Literal.Builder => Unit): Column = Column {
    builder =>
      val literalBuilder = proto.Expression.Literal.newBuilder()
      f(literalBuilder)
      builder.setLiteral(literalBuilder)
  }

  private def createDecimalLiteral(precision: Int, scale: Int, value: String): Column =
    createLiteral { builder =>
      builder.getDecimalBuilder
        .setPrecision(precision)
        .setScale(scale)
        .setValue(value)
    }

  /**
   * Creates a [[Column]] of literal value.
   *
   * The passed in object is returned directly if it is already a [[Column]]. If the object is a
   * Scala Symbol, it is converted into a [[Column]] also. Otherwise, a new [[Column]] is created
   * to represent the literal value.
   *
   * @since 3.4.0
   */
  @scala.annotation.tailrec
  def lit(literal: Any): Column = {
    literal match {
      case c: Column => c
      case s: Symbol => Column(s.name)
      case v: Boolean => createLiteral(_.setBoolean(v))
      case v: Byte => createLiteral(_.setByte(v))
      case v: Short => createLiteral(_.setShort(v))
      case v: Int => createLiteral(_.setInteger(v))
      case v: Long => createLiteral(_.setLong(v))
      case v: Float => createLiteral(_.setFloat(v))
      case v: Double => createLiteral(_.setDouble(v))
      case v: BigDecimal => createDecimalLiteral(v.precision, v.scale, v.toString)
      case v: JBigDecimal => createDecimalLiteral(v.precision, v.scale, v.toString)
      case v: String => createLiteral(_.setString(v))
      case v: Char => createLiteral(_.setString(v.toString))
      case v: Array[Char] => createLiteral(_.setString(String.valueOf(v)))
      case v: Array[Byte] => createLiteral(_.setBinary(ByteString.copyFrom(v)))
      case v: collection.mutable.WrappedArray[_] => lit(v.array)
      case v: LocalDate => createLiteral(_.setDate(v.toEpochDay.toInt))
      case null => unsupported("Null literals not supported yet.")
      case _ => unsupported(s"literal $literal not supported (yet).")
    }
  }
  //////////////////////////////////////////////////////////////////////////////////////////////
  // Sort functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a sort expression based on ascending order of the column.
   * {{{
   *   df.sort(asc("dept"), desc("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 3.4.0
   */
  def asc(columnName: String): Column = Column(columnName).asc

  /**
   * Returns a sort expression based on ascending order of the column, and null values return
   * before non-null values.
   * {{{
   *   df.sort(asc_nulls_first("dept"), desc("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 3.4.0
   */
  def asc_nulls_first(columnName: String): Column = Column(columnName).asc_nulls_first

  /**
   * Returns a sort expression based on ascending order of the column, and null values appear
   * after non-null values.
   * {{{
   *   df.sort(asc_nulls_last("dept"), desc("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 3.4.0
   */
  def asc_nulls_last(columnName: String): Column = Column(columnName).asc_nulls_last

  /**
   * Returns a sort expression based on the descending order of the column.
   * {{{
   *   df.sort(asc("dept"), desc("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 3.4.0
   */
  def desc(columnName: String): Column = Column(columnName).desc

  /**
   * Returns a sort expression based on the descending order of the column, and null values appear
   * before non-null values.
   * {{{
   *   df.sort(asc("dept"), desc_nulls_first("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 3.4.0
   */
  def desc_nulls_first(columnName: String): Column = Column(columnName).desc_nulls_first

  /**
   * Returns a sort expression based on the descending order of the column, and null values appear
   * after non-null values.
   * {{{
   *   df.sort(asc("dept"), desc_nulls_last("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 3.4.0
   */
  def desc_nulls_last(columnName: String): Column = Column(columnName).desc_nulls_last

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Aggregate functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * @group agg_funcs
   * @since 3.4.0
   */
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(e: Column): Column = approx_count_distinct(e)

  /**
   * @group agg_funcs
   * @since 3.4.0
   */
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(columnName: String): Column = approx_count_distinct(columnName)

  /**
   * @group agg_funcs
   * @since 3.4.0
   */
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(e: Column, rsd: Double): Column = approx_count_distinct(e, rsd)

  /**
   * @group agg_funcs
   * @since 3.4.0
   */
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(columnName: String, rsd: Double): Column = {
    approx_count_distinct(Column(columnName), rsd)
  }

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def approx_count_distinct(e: Column): Column = Column.fn("approx_count_distinct", e)

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def approx_count_distinct(columnName: String): Column = approx_count_distinct(
    column(columnName))

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @param rsd
   *   maximum relative standard deviation allowed (default = 0.05)
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def approx_count_distinct(e: Column, rsd: Double): Column = {
    Column.fn("approx_count_distinct", e, lit(rsd))
  }

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @param rsd
   *   maximum relative standard deviation allowed (default = 0.05)
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def approx_count_distinct(columnName: String, rsd: Double): Column = {
    approx_count_distinct(Column(columnName), rsd)
  }

  /**
   * Aggregate function: returns the average of the values in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def avg(e: Column): Column = Column.fn("avg", e)

  /**
   * Aggregate function: returns the average of the values in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def avg(columnName: String): Column = avg(Column(columnName))

  /**
   * Aggregate function: returns a list of objects with duplicates.
   *
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def collect_list(e: Column): Column = Column.fn("collect_list", e)

  /**
   * Aggregate function: returns a list of objects with duplicates.
   *
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def collect_list(columnName: String): Column = collect_list(Column(columnName))

  /**
   * Aggregate function: returns a set of objects with duplicate elements eliminated.
   *
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def collect_set(e: Column): Column = Column.fn("collect_set", e)

  /**
   * Aggregate function: returns a set of objects with duplicate elements eliminated.
   *
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def collect_set(columnName: String): Column = collect_set(Column(columnName))

  /**
   * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def corr(column1: Column, column2: Column): Column = Column.fn("corr", column1, column2)

  /**
   * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def corr(columnName1: String, columnName2: String): Column = {
    corr(Column(columnName1), Column(columnName2))
  }

  /**
   * Aggregate function: returns the number of items in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def count(e: Column): Column = Column.fn("count", e)

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * An alias of `count_distinct`, and it is encouraged to use `count_distinct` directly.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def countDistinct(expr: Column, exprs: Column*): Column = count_distinct(expr, exprs: _*)

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * An alias of `count_distinct`, and it is encouraged to use `count_distinct` directly.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def countDistinct(columnName: String, columnNames: String*): Column =
    count_distinct(Column(columnName), columnNames.map(Column.apply): _*)

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def count_distinct(expr: Column, exprs: Column*): Column =
    Column.fn("count", isDistinct = true, expr +: exprs: _*)

  /**
   * Aggregate function: returns the population covariance for two columns.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def covar_pop(column1: Column, column2: Column): Column =
    Column.fn("covar_pop", column1, column2)

  /**
   * Aggregate function: returns the population covariance for two columns.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def covar_pop(columnName1: String, columnName2: String): Column = {
    covar_pop(Column(columnName1), Column(columnName2))
  }

  /**
   * Aggregate function: returns the sample covariance for two columns.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def covar_samp(column1: Column, column2: Column): Column =
    Column.fn("covar_samp", column1, column2)

  /**
   * Aggregate function: returns the sample covariance for two columns.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def covar_samp(columnName1: String, columnName2: String): Column =
    covar_samp(Column(columnName1), Column(columnName2))

  /**
   * Aggregate function: returns the first value in a group.
   *
   * The function by default returns the first values it sees. It will return the first non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def first(e: Column, ignoreNulls: Boolean): Column =
    Column.fn("first", e, lit(ignoreNulls))

  /**
   * Aggregate function: returns the first value of a column in a group.
   *
   * The function by default returns the first values it sees. It will return the first non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def first(columnName: String, ignoreNulls: Boolean): Column = {
    first(Column(columnName), ignoreNulls)
  }

  /**
   * Aggregate function: returns the first value in a group.
   *
   * The function by default returns the first values it sees. It will return the first non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def first(e: Column): Column = first(e, ignoreNulls = false)

  /**
   * Aggregate function: returns the first value of a column in a group.
   *
   * The function by default returns the first values it sees. It will return the first non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def first(columnName: String): Column = first(Column(columnName))

  /**
   * Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated or
   * not, returns 1 for aggregated or 0 for not aggregated in the result set.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def grouping(e: Column): Column = Column.fn("grouping", e)

  /**
   * Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated or
   * not, returns 1 for aggregated or 0 for not aggregated in the result set.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def grouping(columnName: String): Column = grouping(Column(columnName))

  /**
   * Aggregate function: returns the level of grouping, equals to
   *
   * {{{
   *   (grouping(c1) <<; (n-1)) + (grouping(c2) <<; (n-2)) + ... + grouping(cn)
   * }}}
   *
   * @note
   *   The list of columns should match with grouping columns exactly, or empty (means all the
   *   grouping columns).
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def grouping_id(cols: Column*): Column = Column.fn("grouping_id", cols: _*)

  /**
   * Aggregate function: returns the level of grouping, equals to
   *
   * {{{
   *   (grouping(c1) <<; (n-1)) + (grouping(c2) <<; (n-2)) + ... + grouping(cn)
   * }}}
   *
   * @note
   *   The list of columns should match with grouping columns exactly.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def grouping_id(colName: String, colNames: String*): Column =
    grouping_id((Seq(colName) ++ colNames).map(n => Column(n)): _*)

  /**
   * Aggregate function: returns the kurtosis of the values in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def kurtosis(e: Column): Column = Column.fn("kurtosis", e)

  /**
   * Aggregate function: returns the kurtosis of the values in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def kurtosis(columnName: String): Column = kurtosis(Column(columnName))

  /**
   * Aggregate function: returns the last value in a group.
   *
   * The function by default returns the last values it sees. It will return the last non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def last(e: Column, ignoreNulls: Boolean): Column =
    Column.fn("last", e, lit(ignoreNulls))

  /**
   * Aggregate function: returns the last value of the column in a group.
   *
   * The function by default returns the last values it sees. It will return the last non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def last(columnName: String, ignoreNulls: Boolean): Column =
    last(Column(columnName), ignoreNulls)

  /**
   * Aggregate function: returns the last value in a group.
   *
   * The function by default returns the last values it sees. It will return the last non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def last(e: Column): Column = last(e, ignoreNulls = false)

  /**
   * Aggregate function: returns the last value of the column in a group.
   *
   * The function by default returns the last values it sees. It will return the last non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def last(columnName: String): Column = last(Column(columnName), ignoreNulls = false)

  /**
   * Aggregate function: returns the most frequent value in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def mode(e: Column): Column = Column.fn("mode", e)

  /**
   * Aggregate function: returns the maximum value of the expression in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def max(e: Column): Column = Column.fn("max", e)

  /**
   * Aggregate function: returns the maximum value of the column in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def max(columnName: String): Column = max(Column(columnName))

  /**
   * Aggregate function: returns the value associated with the maximum value of ord.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def max_by(e: Column, ord: Column): Column = Column.fn("max_by", e, ord)

  /**
   * Aggregate function: returns the average of the values in a group. Alias for avg.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def mean(e: Column): Column = avg(e)

  /**
   * Aggregate function: returns the average of the values in a group. Alias for avg.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def mean(columnName: String): Column = avg(columnName)

  /**
   * Aggregate function: returns the median of the values in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def median(e: Column): Column = Column.fn("median", e)

  /**
   * Aggregate function: returns the minimum value of the expression in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def min(e: Column): Column = Column.fn("min", e)

  /**
   * Aggregate function: returns the minimum value of the column in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def min(columnName: String): Column = min(Column(columnName))

  /**
   * Aggregate function: returns the value associated with the minimum value of ord.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def min_by(e: Column, ord: Column): Column = Column.fn("min_by", e, ord)

  /**
   * Aggregate function: returns the approximate `percentile` of the numeric column `col` which is
   * the smallest value in the ordered `col` values (sorted from least to greatest) such that no
   * more than `percentage` of `col` values is less than the value or equal to that value.
   *
   * If percentage is an array, each value must be between 0.0 and 1.0. If it is a single floating
   * point value, it must be between 0.0 and 1.0.
   *
   * The accuracy parameter is a positive numeric literal which controls approximation accuracy at
   * the cost of memory. Higher value of accuracy yields better accuracy, 1.0/accuracy is the
   * relative error of the approximation.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def percentile_approx(e: Column, percentage: Column, accuracy: Column): Column =
    Column.fn("percentile_approx", e, percentage, accuracy)

  /**
   * Aggregate function: returns the product of all numerical elements in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def product(e: Column): Column = Column.fn("product", e)

  /**
   * Aggregate function: returns the skewness of the values in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def skewness(e: Column): Column = Column.fn("skewness", e)

  /**
   * Aggregate function: returns the skewness of the values in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def skewness(columnName: String): Column = skewness(Column(columnName))

  /**
   * Aggregate function: alias for `stddev_samp`.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def stddev(e: Column): Column = Column.fn("stddev", e)

  /**
   * Aggregate function: alias for `stddev_samp`.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def stddev(columnName: String): Column = stddev(Column(columnName))

  /**
   * Aggregate function: returns the sample standard deviation of the expression in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def stddev_samp(e: Column): Column = Column.fn("stddev_samp", e)

  /**
   * Aggregate function: returns the sample standard deviation of the expression in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def stddev_samp(columnName: String): Column = stddev_samp(Column(columnName))

  /**
   * Aggregate function: returns the population standard deviation of the expression in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def stddev_pop(e: Column): Column = Column.fn("stddev_pop", e)

  /**
   * Aggregate function: returns the population standard deviation of the expression in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def stddev_pop(columnName: String): Column = stddev_pop(Column(columnName))

  /**
   * Aggregate function: returns the sum of all values in the expression.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def sum(e: Column): Column = Column.fn("sum", e)

  /**
   * Aggregate function: returns the sum of all values in the given column.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def sum(columnName: String): Column = sum(Column(columnName))

  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  @deprecated("Use sum_distinct", "3.2.0")
  def sumDistinct(e: Column): Column = sum_distinct(e)

  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  @deprecated("Use sum_distinct", "3.2.0")
  def sumDistinct(columnName: String): Column = sum_distinct(Column(columnName))

  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def sum_distinct(e: Column): Column = Column.fn("sum", isDistinct = true, e)

  /**
   * Aggregate function: alias for `var_samp`.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def variance(e: Column): Column = Column.fn("variance", e)

  /**
   * Aggregate function: alias for `var_samp`.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def variance(columnName: String): Column = variance(Column(columnName))

  /**
   * Aggregate function: returns the unbiased variance of the values in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def var_samp(e: Column): Column = Column.fn("var_samp", e)

  /**
   * Aggregate function: returns the unbiased variance of the values in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def var_samp(columnName: String): Column = var_samp(Column(columnName))

  /**
   * Aggregate function: returns the population variance of the values in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def var_pop(e: Column): Column = Column.fn("var_pop", e)

  /**
   * Aggregate function: returns the population variance of the values in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def var_pop(columnName: String): Column = var_pop(Column(columnName))

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Non-aggregate functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new array column. The input columns must all have the same data type.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def array(cols: Column*): Column = Column.fn("array", cols: _*)

  /**
   * Creates a new array column. The input columns must all have the same data type.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def array(colName: String, colNames: String*): Column = {
    array((colName +: colNames).map(col): _*)
  }

  /**
   * Creates a new map column. The input columns must be grouped as key-value pairs, e.g. (key1,
   * value1, key2, value2, ...). The key columns must all have the same data type, and can't be
   * null. The value columns must all have the same data type.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def map(cols: Column*): Column = Column.fn("map", cols: _*)

  /**
   * Creates a new map column. The array in the first column is used for keys. The array in the
   * second column is used for values. All elements in the array for key should not be null.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def map_from_arrays(keys: Column, values: Column): Column =
    Column.fn("map_from_arrays", keys, values)

  /**
   * Returns the first column that is not null, or null if all inputs are null.
   *
   * For example, `coalesce(a, b, c)` will return a if a is not null, or b if a is null and b is
   * not null, or c if both a and b are null but c is not null.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def coalesce(e: Column*): Column = Column.fn("coalesce", e: _*)

  /**
   * Creates a string column for the file name of the current Spark task.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def input_file_name(): Column = Column.fn("input_file_name")

  /**
   * Return true iff the column is NaN.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def isnan(e: Column): Column = e.isNaN

  /**
   * Return true iff the column is null.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def isnull(e: Column): Column = e.isNull

  /**
   * A column expression that generates monotonically increasing 64-bit integers.
   *
   * The generated ID is guaranteed to be monotonically increasing and unique, but not
   * consecutive. The current implementation puts the partition ID in the upper 31 bits, and the
   * record number within each partition in the lower 33 bits. The assumption is that the data
   * frame has less than 1 billion partitions, and each partition has less than 8 billion records.
   *
   * As an example, consider a `DataFrame` with two partitions, each with 3 records. This
   * expression would return the following IDs:
   *
   * {{{
   * 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
   * }}}
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  @deprecated("Use monotonically_increasing_id()", "2.0.0")
  def monotonicallyIncreasingId(): Column = monotonically_increasing_id()

  /**
   * A column expression that generates monotonically increasing 64-bit integers.
   *
   * The generated ID is guaranteed to be monotonically increasing and unique, but not
   * consecutive. The current implementation puts the partition ID in the upper 31 bits, and the
   * record number within each partition in the lower 33 bits. The assumption is that the data
   * frame has less than 1 billion partitions, and each partition has less than 8 billion records.
   *
   * As an example, consider a `DataFrame` with two partitions, each with 3 records. This
   * expression would return the following IDs:
   *
   * {{{
   * 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
   * }}}
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def monotonically_increasing_id(): Column = Column.fn("monotonically_increasing_id")

  /**
   * Returns col1 if it is not NaN, or col2 if col1 is NaN.
   *
   * Both inputs should be floating point columns (DoubleType or FloatType).
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def nanvl(col1: Column, col2: Column): Column = Column.fn("nanvl", col1, col2)

  /**
   * Unary minus, i.e. negate the expression.
   * {{{
   *   // Select the amount column and negates all values.
   *   // Scala:
   *   df.select( -df("amount") )
   *
   *   // Java:
   *   df.select( negate(df.col("amount")) );
   * }}}
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def negate(e: Column): Column = -e

  /**
   * Inversion of boolean expression, i.e. NOT.
   * {{{
   *   // Scala: select rows that are not active (isActive === false)
   *   df.filter( !df("isActive") )
   *
   *   // Java:
   *   df.filter( not(df.col("isActive")) );
   * }}}
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def not(e: Column): Column = !e

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [0.0, 1.0).
   *
   * @note
   *   The function is non-deterministic in general case.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def rand(seed: Long): Column = Column.fn("rand", lit(seed))

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [0.0, 1.0).
   *
   * @note
   *   The function is non-deterministic in general case.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def rand(): Column = Column.fn("rand")

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples from the
   * standard normal distribution.
   *
   * @note
   *   The function is non-deterministic in general case.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def randn(seed: Long): Column = Column.fn("randn", lit(seed))

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples from the
   * standard normal distribution.
   *
   * @note
   *   The function is non-deterministic in general case.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def randn(): Column = Column.fn("randn")

  /**
   * Partition ID.
   *
   * @note
   *   This is non-deterministic because it depends on data partitioning and task scheduling.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def spark_partition_id(): Column = Column.fn("spark_partition_id")

  /**
   * Computes the square root of the specified float value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def sqrt(e: Column): Column = Column.fn("sqrt", e)

  /**
   * Computes the square root of the specified float value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def sqrt(colName: String): Column = sqrt(Column(colName))

  /**
   * Creates a new struct column. If the input column is a column in a `DataFrame`, or a derived
   * column expression that is named (i.e. aliased), its name would be retained as the
   * StructField's name, otherwise, the newly generated StructField's name would be auto generated
   * as `col` with a suffix `index + 1`, i.e. col1, col2, col3, ...
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def struct(cols: Column*): Column = Column.fn("struct", cols: _*)

  /**
   * Creates a new struct column that composes multiple input columns.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def struct(colName: String, colNames: String*): Column = {
    struct((colName +: colNames).map(col): _*)
  }

  /**
   * Evaluates a list of conditions and returns one of multiple possible result expressions. If
   * otherwise is not defined at the end, null is returned for unmatched conditions.
   *
   * {{{
   *   // Example: encoding gender string column into integer.
   *
   *   // Scala:
   *   people.select(when(people("gender") === "male", 0)
   *     .when(people("gender") === "female", 1)
   *     .otherwise(2))
   *
   *   // Java:
   *   people.select(when(col("gender").equalTo("male"), 0)
   *     .when(col("gender").equalTo("female"), 1)
   *     .otherwise(2))
   * }}}
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def when(condition: Column, value: Any): Column = Column { builder =>
    builder.getUnresolvedFunctionBuilder
      .setFunctionName("when")
      .addArguments(condition.expr)
      .addArguments(lit(value).expr)
  }

  /**
   * Computes bitwise NOT (~) of a number.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  @deprecated("Use bitwise_not", "3.2.0")
  def bitwiseNOT(e: Column): Column = bitwise_not(e)

  /**
   * Computes bitwise NOT (~) of a number.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def bitwise_not(e: Column): Column = Column.fn("~", e)

  /**
   * Parses the expression string into the column that it represents, similar to
   * [[Dataset#selectExpr]].
   * {{{
   *   // get the number of words of each length
   *   df.groupBy(expr("length(word)")).count()
   * }}}
   *
   * @group normal_funcs
   */
  def expr(expr: String): Column = Column { builder =>
    builder.getExpressionStringBuilder.setExpression(expr)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Math Functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Computes the absolute value of a numeric value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def abs(e: Column): Column = Column.fn("abs", e)

  /**
   * @return
   *   inverse cosine of `e` in radians, as if computed by `java.lang.Math.acos`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def acos(e: Column): Column = Column.fn("acos", e)

  /**
   * @return
   *   inverse cosine of `columnName`, as if computed by `java.lang.Math.acos`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def acos(columnName: String): Column = acos(Column(columnName))

  /**
   * @return
   *   inverse hyperbolic cosine of `e`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def acosh(e: Column): Column = Column.fn("acosh", e)

  /**
   * @return
   *   inverse hyperbolic cosine of `columnName`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def acosh(columnName: String): Column = acosh(Column(columnName))

  /**
   * @return
   *   inverse sine of `e` in radians, as if computed by `java.lang.Math.asin`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def asin(e: Column): Column = Column.fn("asin", e)

  /**
   * @return
   *   inverse sine of `columnName`, as if computed by `java.lang.Math.asin`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def asin(columnName: String): Column = asin(Column(columnName))

  /**
   * @return
   *   inverse hyperbolic sine of `e`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def asinh(e: Column): Column = Column.fn("asinh", e)

  /**
   * @return
   *   inverse hyperbolic sine of `columnName`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def asinh(columnName: String): Column = asinh(Column(columnName))

  /**
   * @return
   *   inverse tangent of `e` as if computed by `java.lang.Math.atan`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def atan(e: Column): Column = Column.fn("atan", e)

  /**
   * @return
   *   inverse tangent of `columnName`, as if computed by `java.lang.Math.atan`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def atan(columnName: String): Column = atan(Column(columnName))

  /**
   * @param y
   *   coordinate on y-axis
   * @param x
   *   coordinate on x-axis
   * @return
   *   the <i>theta</i> component of the point (<i>r</i>, <i>theta</i>) in polar coordinates that
   *   corresponds to the point (<i>x</i>, <i>y</i>) in Cartesian coordinates, as if computed by
   *   `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def atan2(y: Column, x: Column): Column = Column.fn("atan2", y, x)

  /**
   * @param y
   *   coordinate on y-axis
   * @param xName
   *   coordinate on x-axis
   * @return
   *   the <i>theta</i> component of the point (<i>r</i>, <i>theta</i>) in polar coordinates that
   *   corresponds to the point (<i>x</i>, <i>y</i>) in Cartesian coordinates, as if computed by
   *   `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def atan2(y: Column, xName: String): Column = atan2(y, Column(xName))

  /**
   * @param yName
   *   coordinate on y-axis
   * @param x
   *   coordinate on x-axis
   * @return
   *   the <i>theta</i> component of the point (<i>r</i>, <i>theta</i>) in polar coordinates that
   *   corresponds to the point (<i>x</i>, <i>y</i>) in Cartesian coordinates, as if computed by
   *   `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def atan2(yName: String, x: Column): Column = atan2(Column(yName), x)

  /**
   * @param yName
   *   coordinate on y-axis
   * @param xName
   *   coordinate on x-axis
   * @return
   *   the <i>theta</i> component of the point (<i>r</i>, <i>theta</i>) in polar coordinates that
   *   corresponds to the point (<i>x</i>, <i>y</i>) in Cartesian coordinates, as if computed by
   *   `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def atan2(yName: String, xName: String): Column =
    atan2(Column(yName), Column(xName))

  /**
   * @param y
   *   coordinate on y-axis
   * @param xValue
   *   coordinate on x-axis
   * @return
   *   the <i>theta</i> component of the point (<i>r</i>, <i>theta</i>) in polar coordinates that
   *   corresponds to the point (<i>x</i>, <i>y</i>) in Cartesian coordinates, as if computed by
   *   `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def atan2(y: Column, xValue: Double): Column = atan2(y, lit(xValue))

  /**
   * @param yName
   *   coordinate on y-axis
   * @param xValue
   *   coordinate on x-axis
   * @return
   *   the <i>theta</i> component of the point (<i>r</i>, <i>theta</i>) in polar coordinates that
   *   corresponds to the point (<i>x</i>, <i>y</i>) in Cartesian coordinates, as if computed by
   *   `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def atan2(yName: String, xValue: Double): Column = atan2(Column(yName), xValue)

  /**
   * @param yValue
   *   coordinate on y-axis
   * @param x
   *   coordinate on x-axis
   * @return
   *   the <i>theta</i> component of the point (<i>r</i>, <i>theta</i>) in polar coordinates that
   *   corresponds to the point (<i>x</i>, <i>y</i>) in Cartesian coordinates, as if computed by
   *   `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def atan2(yValue: Double, x: Column): Column = atan2(lit(yValue), x)

  /**
   * @param yValue
   *   coordinate on y-axis
   * @param xName
   *   coordinate on x-axis
   * @return
   *   the <i>theta</i> component of the point (<i>r</i>, <i>theta</i>) in polar coordinates that
   *   corresponds to the point (<i>x</i>, <i>y</i>) in Cartesian coordinates, as if computed by
   *   `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def atan2(yValue: Double, xName: String): Column = atan2(yValue, Column(xName))

  /**
   * @return
   *   inverse hyperbolic tangent of `e`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def atanh(e: Column): Column = Column.fn("atanh", e)

  /**
   * @return
   *   inverse hyperbolic tangent of `columnName`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def atanh(columnName: String): Column = atanh(Column(columnName))

  /**
   * An expression that returns the string representation of the binary value of the given long
   * column. For example, bin("12") returns "1100".
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def bin(e: Column): Column = Column.fn("bin", e)

  /**
   * An expression that returns the string representation of the binary value of the given long
   * column. For example, bin("12") returns "1100".
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def bin(columnName: String): Column = bin(Column(columnName))

  /**
   * Computes the cube-root of the given value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def cbrt(e: Column): Column = Column.fn("cbrt", e)

  /**
   * Computes the cube-root of the given column.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def cbrt(columnName: String): Column = cbrt(Column(columnName))

  /**
   * Computes the ceiling of the given value of `e` to `scale` decimal places.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def ceil(e: Column, scale: Column): Column = Column.fn("ceil", e, scale)

  /**
   * Computes the ceiling of the given value of `e` to 0 decimal places.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def ceil(e: Column): Column = Column.fn("ceil", e)

  /**
   * Computes the ceiling of the given value of `e` to 0 decimal places.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def ceil(columnName: String): Column = ceil(Column(columnName))

  /**
   * Convert a number in a string column from one base to another.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def conv(num: Column, fromBase: Int, toBase: Int): Column =
    Column.fn("conv", num, lit(fromBase), lit(toBase))

  /**
   * @param e
   *   angle in radians
   * @return
   *   cosine of the angle, as if computed by `java.lang.Math.cos`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def cos(e: Column): Column = Column.fn("cos", e)

  /**
   * @param columnName
   *   angle in radians
   * @return
   *   cosine of the angle, as if computed by `java.lang.Math.cos`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def cos(columnName: String): Column = cos(Column(columnName))

  /**
   * @param e
   *   hyperbolic angle
   * @return
   *   hyperbolic cosine of the angle, as if computed by `java.lang.Math.cosh`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def cosh(e: Column): Column = Column.fn("cosh", e)

  /**
   * @param columnName
   *   hyperbolic angle
   * @return
   *   hyperbolic cosine of the angle, as if computed by `java.lang.Math.cosh`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def cosh(columnName: String): Column = cosh(Column(columnName))

  /**
   * @param e
   *   angle in radians
   * @return
   *   cotangent of the angle
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def cot(e: Column): Column = Column.fn("cot", e)

  /**
   * @param e
   *   angle in radians
   * @return
   *   cosecant of the angle
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def csc(e: Column): Column = Column.fn("csc", e)

  /**
   * Computes the exponential of the given value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def exp(e: Column): Column = Column.fn("exp", e)

  /**
   * Computes the exponential of the given column.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def exp(columnName: String): Column = exp(Column(columnName))

  /**
   * Computes the exponential of the given value minus one.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def expm1(e: Column): Column = Column.fn("expm1", e)

  /**
   * Computes the exponential of the given column minus one.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def expm1(columnName: String): Column = expm1(Column(columnName))

  /**
   * Computes the factorial of the given value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def factorial(e: Column): Column = Column.fn("factorial", e)

  /**
   * Computes the floor of the given value of `e` to `scale` decimal places.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def floor(e: Column, scale: Column): Column = Column.fn("floor", e, scale)

  /**
   * Computes the floor of the given value of `e` to 0 decimal places.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def floor(e: Column): Column = Column.fn("floor", e)

  /**
   * Computes the floor of the given column value to 0 decimal places.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def floor(columnName: String): Column = floor(Column(columnName))

  /**
   * Returns the greatest value of the list of values, skipping null values. This function takes
   * at least 2 parameters. It will return null iff all parameters are null.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def greatest(exprs: Column*): Column = Column.fn("greatest", exprs: _*)

  /**
   * Returns the greatest value of the list of column names, skipping null values. This function
   * takes at least 2 parameters. It will return null iff all parameters are null.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def greatest(columnName: String, columnNames: String*): Column =
    greatest((columnName +: columnNames).map(Column.apply): _*)

  /**
   * Computes hex value of the given column.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def hex(column: Column): Column = Column.fn("hex", column)

  /**
   * Inverse of hex. Interprets each pair of characters as a hexadecimal number and converts to
   * the byte representation of number.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def unhex(column: Column): Column = Column.fn("unhex", column)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def hypot(l: Column, r: Column): Column = Column.fn("hypot", l, r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def hypot(l: Column, rightName: String): Column = hypot(l, Column(rightName))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def hypot(leftName: String, r: Column): Column = hypot(Column(leftName), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def hypot(leftName: String, rightName: String): Column =
    hypot(Column(leftName), Column(rightName))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def hypot(l: Column, r: Double): Column = hypot(l, lit(r))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def hypot(leftName: String, r: Double): Column = hypot(Column(leftName), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def hypot(l: Double, r: Column): Column = hypot(lit(l), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def hypot(l: Double, rightName: String): Column = hypot(l, Column(rightName))

  /**
   * Returns the least value of the list of values, skipping null values. This function takes at
   * least 2 parameters. It will return null iff all parameters are null.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def least(exprs: Column*): Column = Column.fn("least", exprs: _*)

  /**
   * Returns the least value of the list of column names, skipping null values. This function
   * takes at least 2 parameters. It will return null iff all parameters are null.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def least(columnName: String, columnNames: String*): Column =
    least((columnName +: columnNames).map(Column.apply): _*)

  /**
   * Computes the natural logarithm of the given value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def log(e: Column): Column = Column.fn("log", e)

  /**
   * Computes the natural logarithm of the given column.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def log(columnName: String): Column = log(Column(columnName))

  /**
   * Returns the first argument-base logarithm of the second argument.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def log(base: Double, a: Column): Column = Column.fn("log", lit(base), a)

  /**
   * Returns the first argument-base logarithm of the second argument.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def log(base: Double, columnName: String): Column = log(base, Column(columnName))

  /**
   * Computes the logarithm of the given value in base 10.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def log10(e: Column): Column = Column.fn("log10", e)

  /**
   * Computes the logarithm of the given value in base 10.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def log10(columnName: String): Column = log10(Column(columnName))

  /**
   * Computes the natural logarithm of the given value plus one.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def log1p(e: Column): Column = Column.fn("log1p", e)

  /**
   * Computes the natural logarithm of the given column plus one.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def log1p(columnName: String): Column = log1p(Column(columnName))

  /**
   * Computes the logarithm of the given column in base 2.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def log2(expr: Column): Column = Column.fn("log2", expr)

  /**
   * Computes the logarithm of the given value in base 2.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def log2(columnName: String): Column = log2(Column(columnName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def pow(l: Column, r: Column): Column = Column.fn("power", l, r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def pow(l: Column, rightName: String): Column = pow(l, Column(rightName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def pow(leftName: String, r: Column): Column = pow(Column(leftName), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def pow(leftName: String, rightName: String): Column = pow(Column(leftName), Column(rightName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def pow(l: Column, r: Double): Column = pow(l, lit(r))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def pow(leftName: String, r: Double): Column = pow(Column(leftName), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def pow(l: Double, r: Column): Column = pow(lit(l), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def pow(l: Double, rightName: String): Column = pow(l, Column(rightName))

  /**
   * Returns the positive value of dividend mod divisor.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def pmod(dividend: Column, divisor: Column): Column = Column.fn("pmod", dividend, divisor)

  /**
   * Returns the double value that is closest in value to the argument and is equal to a
   * mathematical integer.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def rint(e: Column): Column = Column.fn("rint", e)

  /**
   * Returns the double value that is closest in value to the argument and is equal to a
   * mathematical integer.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def rint(columnName: String): Column = rint(Column(columnName))

  /**
   * Returns the value of the column `e` rounded to 0 decimal places with HALF_UP round mode.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def round(e: Column): Column = round(e, 0)

  /**
   * Round the value of `e` to `scale` decimal places with HALF_UP round mode if `scale` is
   * greater than or equal to 0 or at integral part when `scale` is less than 0.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def round(e: Column, scale: Int): Column = Column.fn("round", e, lit(scale))

  /**
   * Returns the value of the column `e` rounded to 0 decimal places with HALF_EVEN round mode.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def bround(e: Column): Column = bround(e, 0)

  /**
   * Round the value of `e` to `scale` decimal places with HALF_EVEN round mode if `scale` is
   * greater than or equal to 0 or at integral part when `scale` is less than 0.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def bround(e: Column, scale: Int): Column = Column.fn("bround", e, lit(scale))

  /**
   * @param e
   *   angle in radians
   * @return
   *   secant of the angle
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def sec(e: Column): Column = Column.fn("sec", e)

  /**
   * Shift the given value numBits left. If the given value is a long value, this function will
   * return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  @deprecated("Use shiftleft", "3.2.0")
  def shiftLeft(e: Column, numBits: Int): Column = shiftleft(e, numBits)

  /**
   * Shift the given value numBits left. If the given value is a long value, this function will
   * return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def shiftleft(e: Column, numBits: Int): Column = Column.fn("shiftleft", e, lit(numBits))

  /**
   * (Signed) shift the given value numBits right. If the given value is a long value, it will
   * return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  @deprecated("Use shiftright", "3.2.0")
  def shiftRight(e: Column, numBits: Int): Column = shiftright(e, numBits)

  /**
   * (Signed) shift the given value numBits right. If the given value is a long value, it will
   * return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def shiftright(e: Column, numBits: Int): Column = Column.fn("shiftright", e, lit(numBits))

  /**
   * Unsigned shift the given value numBits right. If the given value is a long value, it will
   * return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  @deprecated("Use shiftrightunsigned", "3.2.0")
  def shiftRightUnsigned(e: Column, numBits: Int): Column = shiftrightunsigned(e, numBits)

  /**
   * Unsigned shift the given value numBits right. If the given value is a long value, it will
   * return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def shiftrightunsigned(e: Column, numBits: Int): Column =
    Column.fn("shiftrightunsigned", e, lit(numBits))

  /**
   * Computes the signum of the given value.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def signum(e: Column): Column = Column.fn("signum", e)

  /**
   * Computes the signum of the given column.
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def signum(columnName: String): Column = signum(Column(columnName))

  /**
   * @param e
   *   angle in radians
   * @return
   *   sine of the angle, as if computed by `java.lang.Math.sin`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def sin(e: Column): Column = Column.fn("sin", e)

  /**
   * @param columnName
   *   angle in radians
   * @return
   *   sine of the angle, as if computed by `java.lang.Math.sin`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def sin(columnName: String): Column = sin(Column(columnName))

  /**
   * @param e
   *   hyperbolic angle
   * @return
   *   hyperbolic sine of the given value, as if computed by `java.lang.Math.sinh`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def sinh(e: Column): Column = Column.fn("sinh", e)

  /**
   * @param columnName
   *   hyperbolic angle
   * @return
   *   hyperbolic sine of the given value, as if computed by `java.lang.Math.sinh`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def sinh(columnName: String): Column = sinh(Column(columnName))

  /**
   * @param e
   *   angle in radians
   * @return
   *   tangent of the given value, as if computed by `java.lang.Math.tan`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def tan(e: Column): Column = Column.fn("tan", e)

  /**
   * @param columnName
   *   angle in radians
   * @return
   *   tangent of the given value, as if computed by `java.lang.Math.tan`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def tan(columnName: String): Column = tan(Column(columnName))

  /**
   * @param e
   *   hyperbolic angle
   * @return
   *   hyperbolic tangent of the given value, as if computed by `java.lang.Math.tanh`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def tanh(e: Column): Column = Column.fn("tanh", e)

  /**
   * @param columnName
   *   hyperbolic angle
   * @return
   *   hyperbolic tangent of the given value, as if computed by `java.lang.Math.tanh`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def tanh(columnName: String): Column = tanh(Column(columnName))

  /**
   * @group math_funcs
   * @since 3.4.0
   */
  @deprecated("Use degrees", "2.1.0")
  def toDegrees(e: Column): Column = degrees(e)

  /**
   * @group math_funcs
   * @since 3.4.0
   */
  @deprecated("Use degrees", "2.1.0")
  def toDegrees(columnName: String): Column = degrees(Column(columnName))

  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in
   * degrees.
   *
   * @param e
   *   angle in radians
   * @return
   *   angle in degrees, as if computed by `java.lang.Math.toDegrees`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def degrees(e: Column): Column = Column.fn("degrees", e)

  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in
   * degrees.
   *
   * @param columnName
   *   angle in radians
   * @return
   *   angle in degrees, as if computed by `java.lang.Math.toDegrees`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def degrees(columnName: String): Column = degrees(Column(columnName))

  /**
   * @group math_funcs
   * @since 3.4.0
   */
  @deprecated("Use radians", "2.1.0")
  def toRadians(e: Column): Column = radians(e)

  /**
   * @group math_funcs
   * @since 3.4.0
   */
  @deprecated("Use radians", "2.1.0")
  def toRadians(columnName: String): Column = radians(Column(columnName))

  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in
   * radians.
   *
   * @param e
   *   angle in degrees
   * @return
   *   angle in radians, as if computed by `java.lang.Math.toRadians`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def radians(e: Column): Column = Column.fn("radians", e)

  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in
   * radians.
   *
   * @param columnName
   *   angle in degrees
   * @return
   *   angle in radians, as if computed by `java.lang.Math.toRadians`
   *
   * @group math_funcs
   * @since 3.4.0
   */
  def radians(columnName: String): Column = radians(Column(columnName))

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Scala UDF functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  // scalastyle:off line.size.limit

  /**
   * Defines a Scala closure of 0 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[RT: TypeTag](f: () => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(f, typeTag[RT])
  }

  /**
   * Defines a Scala closure of 1 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[RT: TypeTag, A1: TypeTag](f: A1 => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(f, typeTag[RT], typeTag[A1])
  }

  /**
   * Defines a Scala closure of 2 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag](f: (A1, A2) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(f, typeTag[RT], typeTag[A1], typeTag[A2])
  }

  /**
   * Defines a Scala closure of 3 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](
      f: (A1, A2, A3) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(f, typeTag[RT], typeTag[A1], typeTag[A2], typeTag[A3])
  }

  /**
   * Defines a Scala closure of 4 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](
      f: (A1, A2, A3, A4) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(f, typeTag[RT], typeTag[A1], typeTag[A2], typeTag[A3], typeTag[A4])
  }

  /**
   * Defines a Scala closure of 5 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](
      f: (A1, A2, A3, A4, A5) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(
      f,
      typeTag[RT],
      typeTag[A1],
      typeTag[A2],
      typeTag[A3],
      typeTag[A4],
      typeTag[A5])
  }

  /**
   * Defines a Scala closure of 6 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag](f: (A1, A2, A3, A4, A5, A6) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(
      f,
      typeTag[RT],
      typeTag[A1],
      typeTag[A2],
      typeTag[A3],
      typeTag[A4],
      typeTag[A5],
      typeTag[A6])
  }

  /**
   * Defines a Scala closure of 7 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(
      f,
      typeTag[RT],
      typeTag[A1],
      typeTag[A2],
      typeTag[A3],
      typeTag[A4],
      typeTag[A5],
      typeTag[A6],
      typeTag[A7])
  }

  /**
   * Defines a Scala closure of 8 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7, A8) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(
      f,
      typeTag[RT],
      typeTag[A1],
      typeTag[A2],
      typeTag[A3],
      typeTag[A4],
      typeTag[A5],
      typeTag[A6],
      typeTag[A7],
      typeTag[A8])
  }

  /**
   * Defines a Scala closure of 9 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(
      f,
      typeTag[RT],
      typeTag[A1],
      typeTag[A2],
      typeTag[A3],
      typeTag[A4],
      typeTag[A5],
      typeTag[A6],
      typeTag[A7],
      typeTag[A8],
      typeTag[A9])
  }

  /**
   * Defines a Scala closure of 10 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(
      f,
      typeTag[RT],
      typeTag[A1],
      typeTag[A2],
      typeTag[A3],
      typeTag[A4],
      typeTag[A5],
      typeTag[A6],
      typeTag[A7],
      typeTag[A8],
      typeTag[A9],
      typeTag[A10])
  }
  // scalastyle:off line.size.limit

}
