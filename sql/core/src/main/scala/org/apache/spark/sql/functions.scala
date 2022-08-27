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

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{Star, UnresolvedFunction}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, HintInfo, ResolvedHint}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, TimestampFormatter}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.expressions.{Aggregator, SparkUserDefinedFunction, UserDefinedAggregator, UserDefinedFunction}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataType.parseTypeWithFallback
import org.apache.spark.util.Utils

/**
 * Commonly used functions available for DataFrame operations. Using functions defined here provides
 * a little bit more compile-time safety to make sure the function exists.
 *
 * Spark also includes more built-in functions that are less common and are not defined here.
 * You can still access them (and all the functions defined here) using the `functions.expr()` API
 * and calling them through a SQL expression string. You can find the entire list of functions
 * at SQL API documentation of your Spark version, see also
 * <a href="https://spark.apache.org/docs/latest/api/sql/index.html">the latest list</a>
 *
 * As an example, `isnan` is a function that is defined here. You can use `isnan(col("myCol"))`
 * to invoke the `isnan` function. This way the programming language's compiler ensures `isnan`
 * exists and is of the proper form. You can also use `expr("isnan(myCol)")` function to invoke the
 * same function. In this case, Spark itself will ensure `isnan` exists when it analyzes the query.
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
 * @since 1.3.0
 */
@Stable
// scalastyle:off
object functions {
// scalastyle:on

  private def withExpr(expr: Expression): Column = Column(expr)

  private def withAggregateFunction(
    func: AggregateFunction,
    isDistinct: Boolean = false): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }

  /**
   * Returns a [[Column]] based on the given column name.
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def col(colName: String): Column = Column(colName)

  /**
   * Returns a [[Column]] based on the given column name. Alias of [[col]].
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def column(colName: String): Column = Column(colName)

  /**
   * Creates a [[Column]] of literal value.
   *
   * The passed in object is returned directly if it is already a [[Column]].
   * If the object is a Scala Symbol, it is converted into a [[Column]] also.
   * Otherwise, a new [[Column]] is created to represent the literal value.
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def lit(literal: Any): Column = literal match {
    case c: Column => c
    case s: Symbol => new ColumnName(s.name)
    case _ =>
      // This is different from `typedlit`. `typedlit` calls `Literal.create` to use
      // `ScalaReflection` to get the type of `literal`. However, since we use `Any` in this method,
      // `typedLit[Any](literal)` will always fail and fallback to `Literal.apply`. Hence, we can
      // just manually call `Literal.apply` to skip the expensive `ScalaReflection` code. This is
      // significantly better when there are many threads calling `lit` concurrently.
      Column(Literal(literal))
  }

  /**
   * Creates a [[Column]] of literal value.
   *
   * An alias of `typedlit`, and it is encouraged to use `typedlit` directly.
   *
   * @group normal_funcs
   * @since 2.2.0
   */
  def typedLit[T : TypeTag](literal: T): Column = typedlit(literal)

  /**
   * Creates a [[Column]] of literal value.
   *
   * The passed in object is returned directly if it is already a [[Column]].
   * If the object is a Scala Symbol, it is converted into a [[Column]] also.
   * Otherwise, a new [[Column]] is created to represent the literal value.
   * The difference between this function and [[lit]] is that this function
   * can handle parameterized scala types e.g.: List, Seq and Map.
   *
   * @note `typedlit` will call expensive Scala reflection APIs. `lit` is preferred if parameterized
   * Scala types are not used.
   *
   * @group normal_funcs
   * @since 3.2.0
   */
  def typedlit[T : TypeTag](literal: T): Column = literal match {
    case c: Column => c
    case s: Symbol => new ColumnName(s.name)
    case _ => Column(Literal.create(literal))
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
   * @since 1.3.0
   */
  def asc(columnName: String): Column = Column(columnName).asc

  /**
   * Returns a sort expression based on ascending order of the column,
   * and null values return before non-null values.
   * {{{
   *   df.sort(asc_nulls_first("dept"), desc("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 2.1.0
   */
  def asc_nulls_first(columnName: String): Column = Column(columnName).asc_nulls_first

  /**
   * Returns a sort expression based on ascending order of the column,
   * and null values appear after non-null values.
   * {{{
   *   df.sort(asc_nulls_last("dept"), desc("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 2.1.0
   */
  def asc_nulls_last(columnName: String): Column = Column(columnName).asc_nulls_last

  /**
   * Returns a sort expression based on the descending order of the column.
   * {{{
   *   df.sort(asc("dept"), desc("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 1.3.0
   */
  def desc(columnName: String): Column = Column(columnName).desc

  /**
   * Returns a sort expression based on the descending order of the column,
   * and null values appear before non-null values.
   * {{{
   *   df.sort(asc("dept"), desc_nulls_first("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 2.1.0
   */
  def desc_nulls_first(columnName: String): Column = Column(columnName).desc_nulls_first

  /**
   * Returns a sort expression based on the descending order of the column,
   * and null values appear after non-null values.
   * {{{
   *   df.sort(asc("dept"), desc_nulls_last("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 2.1.0
   */
  def desc_nulls_last(columnName: String): Column = Column(columnName).desc_nulls_last


  //////////////////////////////////////////////////////////////////////////////////////////////
  // Aggregate functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * @group agg_funcs
   * @since 1.3.0
   */
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(e: Column): Column = approx_count_distinct(e)

  /**
   * @group agg_funcs
   * @since 1.3.0
   */
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(columnName: String): Column = approx_count_distinct(columnName)

  /**
   * @group agg_funcs
   * @since 1.3.0
   */
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(e: Column, rsd: Double): Column = approx_count_distinct(e, rsd)

  /**
   * @group agg_funcs
   * @since 1.3.0
   */
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(columnName: String, rsd: Double): Column = {
    approx_count_distinct(Column(columnName), rsd)
  }

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 2.1.0
   */
  def approx_count_distinct(e: Column): Column = withAggregateFunction {
    HyperLogLogPlusPlus(e.expr)
  }

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 2.1.0
   */
  def approx_count_distinct(columnName: String): Column = approx_count_distinct(column(columnName))

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @param rsd maximum relative standard deviation allowed (default = 0.05)
   *
   * @group agg_funcs
   * @since 2.1.0
   */
  def approx_count_distinct(e: Column, rsd: Double): Column = withAggregateFunction {
    HyperLogLogPlusPlus(e.expr, rsd, 0, 0)
  }

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @param rsd maximum relative standard deviation allowed (default = 0.05)
   *
   * @group agg_funcs
   * @since 2.1.0
   */
  def approx_count_distinct(columnName: String, rsd: Double): Column = {
    approx_count_distinct(Column(columnName), rsd)
  }

  /**
   * Aggregate function: returns the average of the values in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def avg(e: Column): Column = withAggregateFunction { Average(e.expr) }

  /**
   * Aggregate function: returns the average of the values in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def avg(columnName: String): Column = avg(Column(columnName))

  /**
   * Aggregate function: returns a list of objects with duplicates.
   *
   * @note The function is non-deterministic because the order of collected results depends
   * on the order of the rows which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def collect_list(e: Column): Column = withAggregateFunction { CollectList(e.expr) }

  /**
   * Aggregate function: returns a list of objects with duplicates.
   *
   * @note The function is non-deterministic because the order of collected results depends
   * on the order of the rows which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def collect_list(columnName: String): Column = collect_list(Column(columnName))

  /**
   * Aggregate function: returns a set of objects with duplicate elements eliminated.
   *
   * @note The function is non-deterministic because the order of collected results depends
   * on the order of the rows which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def collect_set(e: Column): Column = withAggregateFunction { CollectSet(e.expr) }

  /**
   * Aggregate function: returns a set of objects with duplicate elements eliminated.
   *
   * @note The function is non-deterministic because the order of collected results depends
   * on the order of the rows which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def collect_set(columnName: String): Column = collect_set(Column(columnName))

  /**
   * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def corr(column1: Column, column2: Column): Column = withAggregateFunction {
    Corr(column1.expr, column2.expr)
  }

  /**
   * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def corr(columnName1: String, columnName2: String): Column = {
    corr(Column(columnName1), Column(columnName2))
  }

  /**
   * Aggregate function: returns the number of items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def count(e: Column): Column = withAggregateFunction {
    e.expr match {
      // Turn count(*) into count(1)
      case s: Star => Count(Literal(1))
      case _ => Count(e.expr)
    }
  }

  /**
   * Aggregate function: returns the number of items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def count(columnName: String): TypedColumn[Any, Long] =
    count(Column(columnName)).as(ExpressionEncoder[Long]())

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * An alias of `count_distinct`, and it is encouraged to use `count_distinct` directly.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def countDistinct(expr: Column, exprs: Column*): Column = count_distinct(expr, exprs: _*)

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * An alias of `count_distinct`, and it is encouraged to use `count_distinct` directly.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def countDistinct(columnName: String, columnNames: String*): Column =
    count_distinct(Column(columnName), columnNames.map(Column.apply) : _*)

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 3.2.0
   */
  @scala.annotation.varargs
  def count_distinct(expr: Column, exprs: Column*): Column =
    // For usage like countDistinct("*"), we should let analyzer expand star and
    // resolve function.
    Column(UnresolvedFunction("count", (expr +: exprs).map(_.expr), isDistinct = true))

  /**
   * Aggregate function: returns the population covariance for two columns.
   *
   * @group agg_funcs
   * @since 2.0.0
   */
  def covar_pop(column1: Column, column2: Column): Column = withAggregateFunction {
    CovPopulation(column1.expr, column2.expr)
  }

  /**
   * Aggregate function: returns the population covariance for two columns.
   *
   * @group agg_funcs
   * @since 2.0.0
   */
  def covar_pop(columnName1: String, columnName2: String): Column = {
    covar_pop(Column(columnName1), Column(columnName2))
  }

  /**
   * Aggregate function: returns the sample covariance for two columns.
   *
   * @group agg_funcs
   * @since 2.0.0
   */
  def covar_samp(column1: Column, column2: Column): Column = withAggregateFunction {
    CovSample(column1.expr, column2.expr)
  }

  /**
   * Aggregate function: returns the sample covariance for two columns.
   *
   * @group agg_funcs
   * @since 2.0.0
   */
  def covar_samp(columnName1: String, columnName2: String): Column = {
    covar_samp(Column(columnName1), Column(columnName2))
  }

  /**
   * Aggregate function: returns the first value in a group.
   *
   * The function by default returns the first values it sees. It will return the first non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note The function is non-deterministic because its results depends on the order of the rows
   * which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 2.0.0
   */
  def first(e: Column, ignoreNulls: Boolean): Column = withAggregateFunction {
    First(e.expr, ignoreNulls)
  }

  /**
   * Aggregate function: returns the first value of a column in a group.
   *
   * The function by default returns the first values it sees. It will return the first non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note The function is non-deterministic because its results depends on the order of the rows
   * which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 2.0.0
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
   * @note The function is non-deterministic because its results depends on the order of the rows
   * which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def first(e: Column): Column = first(e, ignoreNulls = false)

  /**
   * Aggregate function: returns the first value of a column in a group.
   *
   * The function by default returns the first values it sees. It will return the first non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note The function is non-deterministic because its results depends on the order of the rows
   * which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def first(columnName: String): Column = first(Column(columnName))

  /**
   * Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated
   * or not, returns 1 for aggregated or 0 for not aggregated in the result set.
   *
   * @group agg_funcs
   * @since 2.0.0
   */
  def grouping(e: Column): Column = Column(Grouping(e.expr))

  /**
   * Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated
   * or not, returns 1 for aggregated or 0 for not aggregated in the result set.
   *
   * @group agg_funcs
   * @since 2.0.0
   */
  def grouping(columnName: String): Column = grouping(Column(columnName))

  /**
   * Aggregate function: returns the level of grouping, equals to
   *
   * {{{
   *   (grouping(c1) <<; (n-1)) + (grouping(c2) <<; (n-2)) + ... + grouping(cn)
   * }}}
   *
   * @note The list of columns should match with grouping columns exactly, or empty (means all the
   * grouping columns).
   *
   * @group agg_funcs
   * @since 2.0.0
   */
  def grouping_id(cols: Column*): Column = Column(GroupingID(cols.map(_.expr)))

  /**
   * Aggregate function: returns the level of grouping, equals to
   *
   * {{{
   *   (grouping(c1) <<; (n-1)) + (grouping(c2) <<; (n-2)) + ... + grouping(cn)
   * }}}
   *
   * @note The list of columns should match with grouping columns exactly.
   *
   * @group agg_funcs
   * @since 2.0.0
   */
  def grouping_id(colName: String, colNames: String*): Column = {
    grouping_id((Seq(colName) ++ colNames).map(n => Column(n)) : _*)
  }

  /**
   * Aggregate function: returns the kurtosis of the values in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def kurtosis(e: Column): Column = withAggregateFunction { Kurtosis(e.expr) }

  /**
   * Aggregate function: returns the kurtosis of the values in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def kurtosis(columnName: String): Column = kurtosis(Column(columnName))

  /**
   * Aggregate function: returns the last value in a group.
   *
   * The function by default returns the last values it sees. It will return the last non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note The function is non-deterministic because its results depends on the order of the rows
   * which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 2.0.0
   */
  def last(e: Column, ignoreNulls: Boolean): Column = withAggregateFunction {
    new Last(e.expr, ignoreNulls)
  }

  /**
   * Aggregate function: returns the last value of the column in a group.
   *
   * The function by default returns the last values it sees. It will return the last non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note The function is non-deterministic because its results depends on the order of the rows
   * which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 2.0.0
   */
  def last(columnName: String, ignoreNulls: Boolean): Column = {
    last(Column(columnName), ignoreNulls)
  }

  /**
   * Aggregate function: returns the last value in a group.
   *
   * The function by default returns the last values it sees. It will return the last non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note The function is non-deterministic because its results depends on the order of the rows
   * which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def last(e: Column): Column = last(e, ignoreNulls = false)

  /**
   * Aggregate function: returns the last value of the column in a group.
   *
   * The function by default returns the last values it sees. It will return the last non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @note The function is non-deterministic because its results depends on the order of the rows
   * which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def last(columnName: String): Column = last(Column(columnName), ignoreNulls = false)

  /**
   * Aggregate function: returns the most frequent value in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def mode(e: Column): Column = withAggregateFunction { Mode(e.expr) }

  /**
   * Aggregate function: returns the maximum value of the expression in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def max(e: Column): Column = withAggregateFunction { Max(e.expr) }

  /**
   * Aggregate function: returns the maximum value of the column in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def max(columnName: String): Column = max(Column(columnName))

  /**
   * Aggregate function: returns the value associated with the maximum value of ord.
   *
   * @group agg_funcs
   * @since 3.3.0
   */
  def max_by(e: Column, ord: Column): Column = withAggregateFunction { MaxBy(e.expr, ord.expr) }

  /**
   * Aggregate function: returns the average of the values in a group.
   * Alias for avg.
   *
   * @group agg_funcs
   * @since 1.4.0
   */
  def mean(e: Column): Column = avg(e)

  /**
   * Aggregate function: returns the average of the values in a group.
   * Alias for avg.
   *
   * @group agg_funcs
   * @since 1.4.0
   */
  def mean(columnName: String): Column = avg(columnName)

  /**
   * Aggregate function: returns the median of the values in a group.
   *
   * @group agg_funcs
   * @since 3.4.0
   */
  def median(e: Column): Column = withAggregateFunction { Median(e.expr) }

  /**
   * Aggregate function: returns the minimum value of the expression in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def min(e: Column): Column = withAggregateFunction { Min(e.expr) }

  /**
   * Aggregate function: returns the minimum value of the column in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def min(columnName: String): Column = min(Column(columnName))

  /**
   * Aggregate function: returns the value associated with the minimum value of ord.
   *
   * @group agg_funcs
   * @since 3.3.0
   */
  def min_by(e: Column, ord: Column): Column = withAggregateFunction { MinBy(e.expr, ord.expr) }

  /**
   * Aggregate function: returns the approximate `percentile` of the numeric column `col` which
   * is the smallest value in the ordered `col` values (sorted from least to greatest) such that
   * no more than `percentage` of `col` values is less than the value or equal to that value.
   *
   * If percentage is an array, each value must be between 0.0 and 1.0.
   * If it is a single floating point value, it must be between 0.0 and 1.0.
   *
   * The accuracy parameter is a positive numeric literal
   * which controls approximation accuracy at the cost of memory.
   * Higher value of accuracy yields better accuracy, 1.0/accuracy
   * is the relative error of the approximation.
   *
   * @group agg_funcs
   * @since 3.1.0
   */
  def percentile_approx(e: Column, percentage: Column, accuracy: Column): Column = {
    withAggregateFunction {
      new ApproximatePercentile(
        e.expr, percentage.expr, accuracy.expr
      )
    }
  }

  /**
   * Aggregate function: returns the product of all numerical elements in a group.
   *
   * @group agg_funcs
   * @since 3.2.0
   */
  def product(e: Column): Column =
    withAggregateFunction { new Product(e.expr) }

  /**
   * Aggregate function: returns the skewness of the values in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def skewness(e: Column): Column = withAggregateFunction { Skewness(e.expr) }

  /**
   * Aggregate function: returns the skewness of the values in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def skewness(columnName: String): Column = skewness(Column(columnName))

  /**
   * Aggregate function: alias for `stddev_samp`.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def stddev(e: Column): Column = withAggregateFunction { StddevSamp(e.expr) }

  /**
   * Aggregate function: alias for `stddev_samp`.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def stddev(columnName: String): Column = stddev(Column(columnName))

  /**
   * Aggregate function: returns the sample standard deviation of
   * the expression in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def stddev_samp(e: Column): Column = withAggregateFunction { StddevSamp(e.expr) }

  /**
   * Aggregate function: returns the sample standard deviation of
   * the expression in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def stddev_samp(columnName: String): Column = stddev_samp(Column(columnName))

  /**
   * Aggregate function: returns the population standard deviation of
   * the expression in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def stddev_pop(e: Column): Column = withAggregateFunction { StddevPop(e.expr) }

  /**
   * Aggregate function: returns the population standard deviation of
   * the expression in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def stddev_pop(columnName: String): Column = stddev_pop(Column(columnName))

  /**
   * Aggregate function: returns the sum of all values in the expression.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def sum(e: Column): Column = withAggregateFunction { Sum(e.expr) }

  /**
   * Aggregate function: returns the sum of all values in the given column.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def sum(columnName: String): Column = sum(Column(columnName))

  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  @deprecated("Use sum_distinct", "3.2.0")
  def sumDistinct(e: Column): Column = withAggregateFunction(Sum(e.expr), isDistinct = true)

  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  @deprecated("Use sum_distinct", "3.2.0")
  def sumDistinct(columnName: String): Column = sum_distinct(Column(columnName))

  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   *
   * @group agg_funcs
   * @since 3.2.0
   */
  def sum_distinct(e: Column): Column = withAggregateFunction(Sum(e.expr), isDistinct = true)

  /**
   * Aggregate function: alias for `var_samp`.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def variance(e: Column): Column = withAggregateFunction { VarianceSamp(e.expr) }

  /**
   * Aggregate function: alias for `var_samp`.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def variance(columnName: String): Column = variance(Column(columnName))

  /**
   * Aggregate function: returns the unbiased variance of the values in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def var_samp(e: Column): Column = withAggregateFunction { VarianceSamp(e.expr) }

  /**
   * Aggregate function: returns the unbiased variance of the values in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def var_samp(columnName: String): Column = var_samp(Column(columnName))

  /**
   * Aggregate function: returns the population variance of the values in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def var_pop(e: Column): Column = withAggregateFunction { VariancePop(e.expr) }

  /**
   * Aggregate function: returns the population variance of the values in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def var_pop(columnName: String): Column = var_pop(Column(columnName))


  //////////////////////////////////////////////////////////////////////////////////////////////
  // Window functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Window function: returns the cumulative distribution of values within a window partition,
   * i.e. the fraction of rows that are below the current row.
   *
   * {{{
   *   N = total number of rows in the partition
   *   cumeDist(x) = number of values before (and including) x / N
   * }}}
   *
   * @group window_funcs
   * @since 1.6.0
   */
  def cume_dist(): Column = withExpr { new CumeDist }

  /**
   * Window function: returns the rank of rows within a window partition, without any gaps.
   *
   * The difference between rank and dense_rank is that denseRank leaves no gaps in ranking
   * sequence when there are ties. That is, if you were ranking a competition using dense_rank
   * and had three people tie for second place, you would say that all three were in second
   * place and that the next person came in third. Rank would give me sequential numbers, making
   * the person that came in third place (after the ties) would register as coming in fifth.
   *
   * This is equivalent to the DENSE_RANK function in SQL.
   *
   * @group window_funcs
   * @since 1.6.0
   */
  def dense_rank(): Column = withExpr { new DenseRank }

  /**
   * Window function: returns the value that is `offset` rows before the current row, and
   * `null` if there is less than `offset` rows before the current row. For example,
   * an `offset` of one will return the previous row at any given point in the window partition.
   *
   * This is equivalent to the LAG function in SQL.
   *
   * @group window_funcs
   * @since 1.4.0
   */
  def lag(e: Column, offset: Int): Column = lag(e, offset, null)

  /**
   * Window function: returns the value that is `offset` rows before the current row, and
   * `null` if there is less than `offset` rows before the current row. For example,
   * an `offset` of one will return the previous row at any given point in the window partition.
   *
   * This is equivalent to the LAG function in SQL.
   *
   * @group window_funcs
   * @since 1.4.0
   */
  def lag(columnName: String, offset: Int): Column = lag(columnName, offset, null)

  /**
   * Window function: returns the value that is `offset` rows before the current row, and
   * `defaultValue` if there is less than `offset` rows before the current row. For example,
   * an `offset` of one will return the previous row at any given point in the window partition.
   *
   * This is equivalent to the LAG function in SQL.
   *
   * @group window_funcs
   * @since 1.4.0
   */
  def lag(columnName: String, offset: Int, defaultValue: Any): Column = {
    lag(Column(columnName), offset, defaultValue)
  }

  /**
   * Window function: returns the value that is `offset` rows before the current row, and
   * `defaultValue` if there is less than `offset` rows before the current row. For example,
   * an `offset` of one will return the previous row at any given point in the window partition.
   *
   * This is equivalent to the LAG function in SQL.
   *
   * @group window_funcs
   * @since 1.4.0
   */
  def lag(e: Column, offset: Int, defaultValue: Any): Column = {
    lag(e, offset, defaultValue, false)
  }

  /**
   * Window function: returns the value that is `offset` rows before the current row, and
   * `defaultValue` if there is less than `offset` rows before the current row. `ignoreNulls`
   * determines whether null values of row are included in or eliminated from the calculation.
   * For example, an `offset` of one will return the previous row at any given point in the
   * window partition.
   *
   * This is equivalent to the LAG function in SQL.
   *
   * @group window_funcs
   * @since 3.2.0
   */
  def lag(e: Column, offset: Int, defaultValue: Any, ignoreNulls: Boolean): Column = withExpr {
    Lag(e.expr, Literal(offset), Literal(defaultValue), ignoreNulls)
  }

  /**
   * Window function: returns the value that is `offset` rows after the current row, and
   * `null` if there is less than `offset` rows after the current row. For example,
   * an `offset` of one will return the next row at any given point in the window partition.
   *
   * This is equivalent to the LEAD function in SQL.
   *
   * @group window_funcs
   * @since 1.4.0
   */
  def lead(columnName: String, offset: Int): Column = { lead(columnName, offset, null) }

  /**
   * Window function: returns the value that is `offset` rows after the current row, and
   * `null` if there is less than `offset` rows after the current row. For example,
   * an `offset` of one will return the next row at any given point in the window partition.
   *
   * This is equivalent to the LEAD function in SQL.
   *
   * @group window_funcs
   * @since 1.4.0
   */
  def lead(e: Column, offset: Int): Column = { lead(e, offset, null) }

  /**
   * Window function: returns the value that is `offset` rows after the current row, and
   * `defaultValue` if there is less than `offset` rows after the current row. For example,
   * an `offset` of one will return the next row at any given point in the window partition.
   *
   * This is equivalent to the LEAD function in SQL.
   *
   * @group window_funcs
   * @since 1.4.0
   */
  def lead(columnName: String, offset: Int, defaultValue: Any): Column = {
    lead(Column(columnName), offset, defaultValue)
  }

  /**
   * Window function: returns the value that is `offset` rows after the current row, and
   * `defaultValue` if there is less than `offset` rows after the current row. For example,
   * an `offset` of one will return the next row at any given point in the window partition.
   *
   * This is equivalent to the LEAD function in SQL.
   *
   * @group window_funcs
   * @since 1.4.0
   */
  def lead(e: Column, offset: Int, defaultValue: Any): Column = {
    lead(e, offset, defaultValue, false)
  }

  /**
   * Window function: returns the value that is `offset` rows after the current row, and
   * `defaultValue` if there is less than `offset` rows after the current row. `ignoreNulls`
   * determines whether null values of row are included in or eliminated from the calculation.
   * The default value of `ignoreNulls` is false. For example, an `offset` of one will return
   * the next row at any given point in the window partition.
   *
   * This is equivalent to the LEAD function in SQL.
   *
   * @group window_funcs
   * @since 3.2.0
   */
  def lead(e: Column, offset: Int, defaultValue: Any, ignoreNulls: Boolean): Column = withExpr {
    Lead(e.expr, Literal(offset), Literal(defaultValue), ignoreNulls)
  }

  /**
   * Window function: returns the value that is the `offset`th row of the window frame
   * (counting from 1), and `null` if the size of window frame is less than `offset` rows.
   *
   * It will return the `offset`th non-null value it sees when ignoreNulls is set to true.
   * If all values are null, then null is returned.
   *
   * This is equivalent to the nth_value function in SQL.
   *
   * @group window_funcs
   * @since 3.1.0
   */
  def nth_value(e: Column, offset: Int, ignoreNulls: Boolean): Column = withExpr {
    NthValue(e.expr, Literal(offset), ignoreNulls)
  }

  /**
   * Window function: returns the value that is the `offset`th row of the window frame
   * (counting from 1), and `null` if the size of window frame is less than `offset` rows.
   *
   * This is equivalent to the nth_value function in SQL.
   *
   * @group window_funcs
   * @since 3.1.0
   */
  def nth_value(e: Column, offset: Int): Column = withExpr {
    NthValue(e.expr, Literal(offset), false)
  }

  /**
   * Window function: returns the ntile group id (from 1 to `n` inclusive) in an ordered window
   * partition. For example, if `n` is 4, the first quarter of the rows will get value 1, the second
   * quarter will get 2, the third quarter will get 3, and the last quarter will get 4.
   *
   * This is equivalent to the NTILE function in SQL.
   *
   * @group window_funcs
   * @since 1.4.0
   */
  def ntile(n: Int): Column = withExpr { new NTile(Literal(n)) }

  /**
   * Window function: returns the relative rank (i.e. percentile) of rows within a window partition.
   *
   * This is computed by:
   * {{{
   *   (rank of row in its partition - 1) / (number of rows in the partition - 1)
   * }}}
   *
   * This is equivalent to the PERCENT_RANK function in SQL.
   *
   * @group window_funcs
   * @since 1.6.0
   */
  def percent_rank(): Column = withExpr { new PercentRank }

  /**
   * Window function: returns the rank of rows within a window partition.
   *
   * The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
   * sequence when there are ties. That is, if you were ranking a competition using dense_rank
   * and had three people tie for second place, you would say that all three were in second
   * place and that the next person came in third. Rank would give me sequential numbers, making
   * the person that came in third place (after the ties) would register as coming in fifth.
   *
   * This is equivalent to the RANK function in SQL.
   *
   * @group window_funcs
   * @since 1.4.0
   */
  def rank(): Column = withExpr { new Rank }

  /**
   * Window function: returns a sequential number starting at 1 within a window partition.
   *
   * @group window_funcs
   * @since 1.6.0
   */
  def row_number(): Column = withExpr { RowNumber() }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Non-aggregate functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new array column. The input columns must all have the same data type.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def array(cols: Column*): Column = withExpr { CreateArray(cols.map(_.expr)) }

  /**
   * Creates a new array column. The input columns must all have the same data type.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def array(colName: String, colNames: String*): Column = {
    array((colName +: colNames).map(col) : _*)
  }

  /**
   * Creates a new map column. The input columns must be grouped as key-value pairs, e.g.
   * (key1, value1, key2, value2, ...). The key columns must all have the same data type, and can't
   * be null. The value columns must all have the same data type.
   *
   * @group normal_funcs
   * @since 2.0
   */
  @scala.annotation.varargs
  def map(cols: Column*): Column = withExpr { CreateMap(cols.map(_.expr)) }

  /**
   * Creates a new map column. The array in the first column is used for keys. The array in the
   * second column is used for values. All elements in the array for key should not be null.
   *
   * @group normal_funcs
   * @since 2.4
   */
  def map_from_arrays(keys: Column, values: Column): Column = withExpr {
    MapFromArrays(keys.expr, values.expr)
  }

  /**
   * Marks a DataFrame as small enough for use in broadcast joins.
   *
   * The following example marks the right DataFrame for broadcast hash join using `joinKey`.
   * {{{
   *   // left and right are DataFrames
   *   left.join(broadcast(right), "joinKey")
   * }}}
   *
   * @group normal_funcs
   * @since 1.5.0
   */
  def broadcast[T](df: Dataset[T]): Dataset[T] = {
    Dataset[T](df.sparkSession,
      ResolvedHint(df.logicalPlan, HintInfo(strategy = Some(BROADCAST))))(df.exprEnc)
  }

  /**
   * Returns the first column that is not null, or null if all inputs are null.
   *
   * For example, `coalesce(a, b, c)` will return a if a is not null,
   * or b if a is null and b is not null, or c if both a and b are null but c is not null.
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def coalesce(e: Column*): Column = withExpr { Coalesce(e.map(_.expr)) }

  /**
   * Creates a string column for the file name of the current Spark task.
   *
   * @group normal_funcs
   * @since 1.6.0
   */
  def input_file_name(): Column = withExpr { InputFileName() }

  /**
   * Return true iff the column is NaN.
   *
   * @group normal_funcs
   * @since 1.6.0
   */
  def isnan(e: Column): Column = withExpr { IsNaN(e.expr) }

  /**
   * Return true iff the column is null.
   *
   * @group normal_funcs
   * @since 1.6.0
   */
  def isnull(e: Column): Column = withExpr { IsNull(e.expr) }

  /**
   * A column expression that generates monotonically increasing 64-bit integers.
   *
   * The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
   * The current implementation puts the partition ID in the upper 31 bits, and the record number
   * within each partition in the lower 33 bits. The assumption is that the data frame has
   * less than 1 billion partitions, and each partition has less than 8 billion records.
   *
   * As an example, consider a `DataFrame` with two partitions, each with 3 records.
   * This expression would return the following IDs:
   *
   * {{{
   * 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
   * }}}
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  @deprecated("Use monotonically_increasing_id()", "2.0.0")
  def monotonicallyIncreasingId(): Column = monotonically_increasing_id()

  /**
   * A column expression that generates monotonically increasing 64-bit integers.
   *
   * The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
   * The current implementation puts the partition ID in the upper 31 bits, and the record number
   * within each partition in the lower 33 bits. The assumption is that the data frame has
   * less than 1 billion partitions, and each partition has less than 8 billion records.
   *
   * As an example, consider a `DataFrame` with two partitions, each with 3 records.
   * This expression would return the following IDs:
   *
   * {{{
   * 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
   * }}}
   *
   * @group normal_funcs
   * @since 1.6.0
   */
  def monotonically_increasing_id(): Column = withExpr { MonotonicallyIncreasingID() }

  /**
   * Returns col1 if it is not NaN, or col2 if col1 is NaN.
   *
   * Both inputs should be floating point columns (DoubleType or FloatType).
   *
   * @group normal_funcs
   * @since 1.5.0
   */
  def nanvl(col1: Column, col2: Column): Column = withExpr { NaNvl(col1.expr, col2.expr) }

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
   * @since 1.3.0
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
   * @since 1.3.0
   */
  def not(e: Column): Column = !e

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [0.0, 1.0).
   *
   * @note The function is non-deterministic in general case.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def rand(seed: Long): Column = withExpr { Rand(seed) }

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [0.0, 1.0).
   *
   * @note The function is non-deterministic in general case.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def rand(): Column = rand(Utils.random.nextLong)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples from
   * the standard normal distribution.
   *
   * @note The function is non-deterministic in general case.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def randn(seed: Long): Column = withExpr { Randn(seed) }

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples from
   * the standard normal distribution.
   *
   * @note The function is non-deterministic in general case.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def randn(): Column = randn(Utils.random.nextLong)

  /**
   * Partition ID.
   *
   * @note This is non-deterministic because it depends on data partitioning and task scheduling.
   *
   * @group normal_funcs
   * @since 1.6.0
   */
  def spark_partition_id(): Column = withExpr { SparkPartitionID() }

  /**
   * Computes the square root of the specified float value.
   *
   * @group math_funcs
   * @since 1.3.0
   */
  def sqrt(e: Column): Column = withExpr { Sqrt(e.expr) }

  /**
   * Computes the square root of the specified float value.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def sqrt(colName: String): Column = sqrt(Column(colName))

  /**
   * Creates a new struct column.
   * If the input column is a column in a `DataFrame`, or a derived column expression
   * that is named (i.e. aliased), its name would be retained as the StructField's name,
   * otherwise, the newly generated StructField's name would be auto generated as
   * `col` with a suffix `index + 1`, i.e. col1, col2, col3, ...
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def struct(cols: Column*): Column = withExpr { CreateStruct.create(cols.map(_.expr)) }

  /**
   * Creates a new struct column that composes multiple input columns.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def struct(colName: String, colNames: String*): Column = {
    struct((colName +: colNames).map(col) : _*)
  }

  /**
   * Evaluates a list of conditions and returns one of multiple possible result expressions.
   * If otherwise is not defined at the end, null is returned for unmatched conditions.
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
   * @since 1.4.0
   */
  def when(condition: Column, value: Any): Column = withExpr {
    CaseWhen(Seq((condition.expr, lit(value).expr)))
  }

  /**
   * Computes bitwise NOT (~) of a number.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  @deprecated("Use bitwise_not", "3.2.0")
  def bitwiseNOT(e: Column): Column = bitwise_not(e)

  /**
   * Computes bitwise NOT (~) of a number.
   *
   * @group normal_funcs
   * @since 3.2.0
   */
  def bitwise_not(e: Column): Column = withExpr { BitwiseNot(e.expr) }

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
  def expr(expr: String): Column = {
    val parser = SparkSession.getActiveSession.map(_.sessionState.sqlParser).getOrElse {
      new SparkSqlParser()
    }
    Column(parser.parseExpression(expr))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Math Functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Computes the absolute value of a numeric value.
   *
   * @group math_funcs
   * @since 1.3.0
   */
  def abs(e: Column): Column = withExpr { Abs(e.expr) }

  /**
   * @return inverse cosine of `e` in radians, as if computed by `java.lang.Math.acos`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def acos(e: Column): Column = withExpr { Acos(e.expr) }

  /**
   * @return inverse cosine of `columnName`, as if computed by `java.lang.Math.acos`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def acos(columnName: String): Column = acos(Column(columnName))

  /**
   * @return inverse hyperbolic cosine of `e`
   *
   * @group math_funcs
   * @since 3.1.0
   */
  def acosh(e: Column): Column = withExpr { Acosh(e.expr) }

  /**
   * @return inverse hyperbolic cosine of `columnName`
   *
   * @group math_funcs
   * @since 3.1.0
   */
  def acosh(columnName: String): Column = acosh(Column(columnName))

  /**
   * @return inverse sine of `e` in radians, as if computed by `java.lang.Math.asin`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def asin(e: Column): Column = withExpr { Asin(e.expr) }

  /**
   * @return inverse sine of `columnName`, as if computed by `java.lang.Math.asin`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def asin(columnName: String): Column = asin(Column(columnName))

  /**
   * @return inverse hyperbolic sine of `e`
   *
   * @group math_funcs
   * @since 3.1.0
   */
  def asinh(e: Column): Column = withExpr { Asinh(e.expr) }

  /**
   * @return inverse hyperbolic sine of `columnName`
   *
   * @group math_funcs
   * @since 3.1.0
   */
  def asinh(columnName: String): Column = asinh(Column(columnName))

  /**
   * @return inverse tangent of `e` as if computed by `java.lang.Math.atan`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan(e: Column): Column = withExpr { Atan(e.expr) }

  /**
   * @return inverse tangent of `columnName`, as if computed by `java.lang.Math.atan`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan(columnName: String): Column = atan(Column(columnName))

  /**
   * @param y coordinate on y-axis
   * @param x coordinate on x-axis
   * @return the <i>theta</i> component of the point
   *         (<i>r</i>, <i>theta</i>)
   *         in polar coordinates that corresponds to the point
   *         (<i>x</i>, <i>y</i>) in Cartesian coordinates,
   *         as if computed by `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(y: Column, x: Column): Column = withExpr { Atan2(y.expr, x.expr) }

  /**
   * @param y coordinate on y-axis
   * @param xName coordinate on x-axis
   * @return the <i>theta</i> component of the point
   *         (<i>r</i>, <i>theta</i>)
   *         in polar coordinates that corresponds to the point
   *         (<i>x</i>, <i>y</i>) in Cartesian coordinates,
   *         as if computed by `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(y: Column, xName: String): Column = atan2(y, Column(xName))

  /**
   * @param yName coordinate on y-axis
   * @param x coordinate on x-axis
   * @return the <i>theta</i> component of the point
   *         (<i>r</i>, <i>theta</i>)
   *         in polar coordinates that corresponds to the point
   *         (<i>x</i>, <i>y</i>) in Cartesian coordinates,
   *         as if computed by `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(yName: String, x: Column): Column = atan2(Column(yName), x)

  /**
   * @param yName coordinate on y-axis
   * @param xName coordinate on x-axis
   * @return the <i>theta</i> component of the point
   *         (<i>r</i>, <i>theta</i>)
   *         in polar coordinates that corresponds to the point
   *         (<i>x</i>, <i>y</i>) in Cartesian coordinates,
   *         as if computed by `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(yName: String, xName: String): Column =
    atan2(Column(yName), Column(xName))

  /**
   * @param y coordinate on y-axis
   * @param xValue coordinate on x-axis
   * @return the <i>theta</i> component of the point
   *         (<i>r</i>, <i>theta</i>)
   *         in polar coordinates that corresponds to the point
   *         (<i>x</i>, <i>y</i>) in Cartesian coordinates,
   *         as if computed by `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(y: Column, xValue: Double): Column = atan2(y, lit(xValue))

  /**
   * @param yName coordinate on y-axis
   * @param xValue coordinate on x-axis
   * @return the <i>theta</i> component of the point
   *         (<i>r</i>, <i>theta</i>)
   *         in polar coordinates that corresponds to the point
   *         (<i>x</i>, <i>y</i>) in Cartesian coordinates,
   *         as if computed by `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(yName: String, xValue: Double): Column = atan2(Column(yName), xValue)

  /**
   * @param yValue coordinate on y-axis
   * @param x coordinate on x-axis
   * @return the <i>theta</i> component of the point
   *         (<i>r</i>, <i>theta</i>)
   *         in polar coordinates that corresponds to the point
   *         (<i>x</i>, <i>y</i>) in Cartesian coordinates,
   *         as if computed by `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(yValue: Double, x: Column): Column = atan2(lit(yValue), x)

  /**
   * @param yValue coordinate on y-axis
   * @param xName coordinate on x-axis
   * @return the <i>theta</i> component of the point
   *         (<i>r</i>, <i>theta</i>)
   *         in polar coordinates that corresponds to the point
   *         (<i>x</i>, <i>y</i>) in Cartesian coordinates,
   *         as if computed by `java.lang.Math.atan2`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(yValue: Double, xName: String): Column = atan2(yValue, Column(xName))

  /**
   * @return inverse hyperbolic tangent of `e`
   *
   * @group math_funcs
   * @since 3.1.0
   */
  def atanh(e: Column): Column = withExpr { Atanh(e.expr) }

  /**
   * @return inverse hyperbolic tangent of `columnName`
   *
   * @group math_funcs
   * @since 3.1.0
   */
  def atanh(columnName: String): Column = atanh(Column(columnName))

  /**
   * An expression that returns the string representation of the binary value of the given long
   * column. For example, bin("12") returns "1100".
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def bin(e: Column): Column = withExpr { Bin(e.expr) }

  /**
   * An expression that returns the string representation of the binary value of the given long
   * column. For example, bin("12") returns "1100".
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def bin(columnName: String): Column = bin(Column(columnName))

  /**
   * Computes the cube-root of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cbrt(e: Column): Column = withExpr { Cbrt(e.expr) }

  /**
   * Computes the cube-root of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cbrt(columnName: String): Column = cbrt(Column(columnName))

  /**
   * Computes the ceiling of the given value of `e` to `scale` decimal places.
   *
   * @group math_funcs
   * @since 3.3.0
   */
  def ceil(e: Column, scale: Column): Column = withExpr {
    UnresolvedFunction(Seq("ceil"), Seq(e.expr, scale.expr), isDistinct = false)
  }

  /**
   * Computes the ceiling of the given value of `e` to 0 decimal places.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def ceil(e: Column): Column = withExpr {
    UnresolvedFunction(Seq("ceil"), Seq(e.expr), isDistinct = false)
  }

  /**
   * Computes the ceiling of the given value of `e` to 0 decimal places.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def ceil(columnName: String): Column = ceil(Column(columnName))

  /**
   * Convert a number in a string column from one base to another.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def conv(num: Column, fromBase: Int, toBase: Int): Column = withExpr {
    Conv(num.expr, lit(fromBase).expr, lit(toBase).expr)
  }

  /**
   * @param e angle in radians
   * @return cosine of the angle, as if computed by `java.lang.Math.cos`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cos(e: Column): Column = withExpr { Cos(e.expr) }

  /**
   * @param columnName angle in radians
   * @return cosine of the angle, as if computed by `java.lang.Math.cos`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cos(columnName: String): Column = cos(Column(columnName))

  /**
   * @param e hyperbolic angle
   * @return hyperbolic cosine of the angle, as if computed by `java.lang.Math.cosh`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cosh(e: Column): Column = withExpr { Cosh(e.expr) }

  /**
   * @param columnName hyperbolic angle
   * @return hyperbolic cosine of the angle, as if computed by `java.lang.Math.cosh`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cosh(columnName: String): Column = cosh(Column(columnName))

  /**
   * @param e angle in radians
   * @return cotangent of the angle
   *
   * @group math_funcs
   * @since 3.3.0
   */
  def cot(e: Column): Column = withExpr { Cot(e.expr) }

  /**
   * @param e angle in radians
   * @return cosecant of the angle
   *
   * @group math_funcs
   * @since 3.3.0
   */
  def csc(e: Column): Column = withExpr { Csc(e.expr) }

  /**
   * Computes the exponential of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def exp(e: Column): Column = withExpr { Exp(e.expr) }

  /**
   * Computes the exponential of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def exp(columnName: String): Column = exp(Column(columnName))

  /**
   * Computes the exponential of the given value minus one.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def expm1(e: Column): Column = withExpr { Expm1(e.expr) }

  /**
   * Computes the exponential of the given column minus one.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def expm1(columnName: String): Column = expm1(Column(columnName))

  /**
   * Computes the factorial of the given value.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def factorial(e: Column): Column = withExpr { Factorial(e.expr) }

  /**
   * Computes the floor of the given value of `e` to `scale` decimal places.
   *
   * @group math_funcs
   * @since 3.3.0
   */
  def floor(e: Column, scale: Column): Column = withExpr {
    UnresolvedFunction(Seq("floor"), Seq(e.expr, scale.expr), isDistinct = false)
  }

  /**
   * Computes the floor of the given value of `e` to 0 decimal places.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def floor(e: Column): Column = withExpr {
    UnresolvedFunction(Seq("floor"), Seq(e.expr), isDistinct = false)
  }

  /**
   * Computes the floor of the given column value to 0 decimal places.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def floor(columnName: String): Column = floor(Column(columnName))

  /**
   * Returns the greatest value of the list of values, skipping null values.
   * This function takes at least 2 parameters. It will return null iff all parameters are null.
   *
   * @group normal_funcs
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def greatest(exprs: Column*): Column = withExpr { Greatest(exprs.map(_.expr)) }

  /**
   * Returns the greatest value of the list of column names, skipping null values.
   * This function takes at least 2 parameters. It will return null iff all parameters are null.
   *
   * @group normal_funcs
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def greatest(columnName: String, columnNames: String*): Column = {
    greatest((columnName +: columnNames).map(Column.apply): _*)
  }

  /**
   * Computes hex value of the given column.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def hex(column: Column): Column = withExpr { Hex(column.expr) }

  /**
   * Inverse of hex. Interprets each pair of characters as a hexadecimal number
   * and converts to the byte representation of number.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def unhex(column: Column): Column = withExpr { Unhex(column.expr) }

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(l: Column, r: Column): Column = withExpr { Hypot(l.expr, r.expr) }

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(l: Column, rightName: String): Column = hypot(l, Column(rightName))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(leftName: String, r: Column): Column = hypot(Column(leftName), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(leftName: String, rightName: String): Column =
    hypot(Column(leftName), Column(rightName))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(l: Column, r: Double): Column = hypot(l, lit(r))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(leftName: String, r: Double): Column = hypot(Column(leftName), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(l: Double, r: Column): Column = hypot(lit(l), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def hypot(l: Double, rightName: String): Column = hypot(l, Column(rightName))

  /**
   * Returns the least value of the list of values, skipping null values.
   * This function takes at least 2 parameters. It will return null iff all parameters are null.
   *
   * @group normal_funcs
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def least(exprs: Column*): Column = withExpr { Least(exprs.map(_.expr)) }

  /**
   * Returns the least value of the list of column names, skipping null values.
   * This function takes at least 2 parameters. It will return null iff all parameters are null.
   *
   * @group normal_funcs
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def least(columnName: String, columnNames: String*): Column = {
    least((columnName +: columnNames).map(Column.apply): _*)
  }

  /**
   * Computes the natural logarithm of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log(e: Column): Column = withExpr { Log(e.expr) }

  /**
   * Computes the natural logarithm of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log(columnName: String): Column = log(Column(columnName))

  /**
   * Returns the first argument-base logarithm of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log(base: Double, a: Column): Column = withExpr { Logarithm(lit(base).expr, a.expr) }

  /**
   * Returns the first argument-base logarithm of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log(base: Double, columnName: String): Column = log(base, Column(columnName))

  /**
   * Computes the logarithm of the given value in base 10.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log10(e: Column): Column = withExpr { Log10(e.expr) }

  /**
   * Computes the logarithm of the given value in base 10.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log10(columnName: String): Column = log10(Column(columnName))

  /**
   * Computes the natural logarithm of the given value plus one.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log1p(e: Column): Column = withExpr { Log1p(e.expr) }

  /**
   * Computes the natural logarithm of the given column plus one.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def log1p(columnName: String): Column = log1p(Column(columnName))

  /**
   * Computes the logarithm of the given column in base 2.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def log2(expr: Column): Column = withExpr { Log2(expr.expr) }

  /**
   * Computes the logarithm of the given value in base 2.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def log2(columnName: String): Column = log2(Column(columnName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(l: Column, r: Column): Column = withExpr { Pow(l.expr, r.expr) }

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(l: Column, rightName: String): Column = pow(l, Column(rightName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(leftName: String, r: Column): Column = pow(Column(leftName), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(leftName: String, rightName: String): Column = pow(Column(leftName), Column(rightName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(l: Column, r: Double): Column = pow(l, lit(r))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(leftName: String, r: Double): Column = pow(Column(leftName), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(l: Double, r: Column): Column = pow(lit(l), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def pow(l: Double, rightName: String): Column = pow(l, Column(rightName))

  /**
   * Returns the positive value of dividend mod divisor.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def pmod(dividend: Column, divisor: Column): Column = withExpr {
    Pmod(dividend.expr, divisor.expr)
  }

  /**
   * Returns the double value that is closest in value to the argument and
   * is equal to a mathematical integer.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def rint(e: Column): Column = withExpr { Rint(e.expr) }

  /**
   * Returns the double value that is closest in value to the argument and
   * is equal to a mathematical integer.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def rint(columnName: String): Column = rint(Column(columnName))

  /**
   * Returns the value of the column `e` rounded to 0 decimal places with HALF_UP round mode.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def round(e: Column): Column = round(e, 0)

  /**
   * Round the value of `e` to `scale` decimal places with HALF_UP round mode
   * if `scale` is greater than or equal to 0 or at integral part when `scale` is less than 0.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def round(e: Column, scale: Int): Column = withExpr { Round(e.expr, Literal(scale)) }

  /**
   * Returns the value of the column `e` rounded to 0 decimal places with HALF_EVEN round mode.
   *
   * @group math_funcs
   * @since 2.0.0
   */
  def bround(e: Column): Column = bround(e, 0)

  /**
   * Round the value of `e` to `scale` decimal places with HALF_EVEN round mode
   * if `scale` is greater than or equal to 0 or at integral part when `scale` is less than 0.
   *
   * @group math_funcs
   * @since 2.0.0
   */
  def bround(e: Column, scale: Int): Column = withExpr { BRound(e.expr, Literal(scale)) }

  /**
   * @param e angle in radians
   * @return secant of the angle
   *
   * @group math_funcs
   * @since 3.3.0
   */
  def sec(e: Column): Column = withExpr { Sec(e.expr) }

  /**
   * Shift the given value numBits left. If the given value is a long value, this function
   * will return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  @deprecated("Use shiftleft", "3.2.0")
  def shiftLeft(e: Column, numBits: Int): Column = shiftleft(e, numBits)

  /**
   * Shift the given value numBits left. If the given value is a long value, this function
   * will return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 3.2.0
   */
  def shiftleft(e: Column, numBits: Int): Column = withExpr { ShiftLeft(e.expr, lit(numBits).expr) }

  /**
   * (Signed) shift the given value numBits right. If the given value is a long value, it will
   * return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  @deprecated("Use shiftright", "3.2.0")
  def shiftRight(e: Column, numBits: Int): Column = shiftright(e, numBits)

  /**
   * (Signed) shift the given value numBits right. If the given value is a long value, it will
   * return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 3.2.0
   */
  def shiftright(e: Column, numBits: Int): Column = withExpr {
    ShiftRight(e.expr, lit(numBits).expr)
  }

  /**
   * Unsigned shift the given value numBits right. If the given value is a long value,
   * it will return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  @deprecated("Use shiftrightunsigned", "3.2.0")
  def shiftRightUnsigned(e: Column, numBits: Int): Column = shiftrightunsigned(e, numBits)

  /**
   * Unsigned shift the given value numBits right. If the given value is a long value,
   * it will return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 3.2.0
   */
  def shiftrightunsigned(e: Column, numBits: Int): Column = withExpr {
    ShiftRightUnsigned(e.expr, lit(numBits).expr)
  }

  /**
   * Computes the signum of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def signum(e: Column): Column = withExpr { Signum(e.expr) }

  /**
   * Computes the signum of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def signum(columnName: String): Column = signum(Column(columnName))

  /**
   * @param e angle in radians
   * @return sine of the angle, as if computed by `java.lang.Math.sin`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sin(e: Column): Column = withExpr { Sin(e.expr) }

  /**
   * @param columnName angle in radians
   * @return sine of the angle, as if computed by `java.lang.Math.sin`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sin(columnName: String): Column = sin(Column(columnName))

  /**
   * @param e hyperbolic angle
   * @return hyperbolic sine of the given value, as if computed by `java.lang.Math.sinh`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sinh(e: Column): Column = withExpr { Sinh(e.expr) }

  /**
   * @param columnName hyperbolic angle
   * @return hyperbolic sine of the given value, as if computed by `java.lang.Math.sinh`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sinh(columnName: String): Column = sinh(Column(columnName))

  /**
   * @param e angle in radians
   * @return tangent of the given value, as if computed by `java.lang.Math.tan`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tan(e: Column): Column = withExpr { Tan(e.expr) }

  /**
   * @param columnName angle in radians
   * @return tangent of the given value, as if computed by `java.lang.Math.tan`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tan(columnName: String): Column = tan(Column(columnName))

  /**
   * @param e hyperbolic angle
   * @return hyperbolic tangent of the given value, as if computed by `java.lang.Math.tanh`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tanh(e: Column): Column = withExpr { Tanh(e.expr) }

  /**
   * @param columnName hyperbolic angle
   * @return hyperbolic tangent of the given value, as if computed by `java.lang.Math.tanh`
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tanh(columnName: String): Column = tanh(Column(columnName))

  /**
   * @group math_funcs
   * @since 1.4.0
   */
  @deprecated("Use degrees", "2.1.0")
  def toDegrees(e: Column): Column = degrees(e)

  /**
   * @group math_funcs
   * @since 1.4.0
   */
  @deprecated("Use degrees", "2.1.0")
  def toDegrees(columnName: String): Column = degrees(Column(columnName))

  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   *
   * @param e angle in radians
   * @return angle in degrees, as if computed by `java.lang.Math.toDegrees`
   *
   * @group math_funcs
   * @since 2.1.0
   */
  def degrees(e: Column): Column = withExpr { ToDegrees(e.expr) }

  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   *
   * @param columnName angle in radians
   * @return angle in degrees, as if computed by `java.lang.Math.toDegrees`
   *
   * @group math_funcs
   * @since 2.1.0
   */
  def degrees(columnName: String): Column = degrees(Column(columnName))

  /**
   * @group math_funcs
   * @since 1.4.0
   */
  @deprecated("Use radians", "2.1.0")
  def toRadians(e: Column): Column = radians(e)

  /**
   * @group math_funcs
   * @since 1.4.0
   */
  @deprecated("Use radians", "2.1.0")
  def toRadians(columnName: String): Column = radians(Column(columnName))

  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
   *
   * @param e angle in degrees
   * @return angle in radians, as if computed by `java.lang.Math.toRadians`
   *
   * @group math_funcs
   * @since 2.1.0
   */
  def radians(e: Column): Column = withExpr { ToRadians(e.expr) }

  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
   *
   * @param columnName angle in degrees
   * @return angle in radians, as if computed by `java.lang.Math.toRadians`
   *
   * @group math_funcs
   * @since 2.1.0
   */
  def radians(columnName: String): Column = radians(Column(columnName))

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Misc functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Calculates the MD5 digest of a binary column and returns the value
   * as a 32 character hex string.
   *
   * @group misc_funcs
   * @since 1.5.0
   */
  def md5(e: Column): Column = withExpr { Md5(e.expr) }

  /**
   * Calculates the SHA-1 digest of a binary column and returns the value
   * as a 40 character hex string.
   *
   * @group misc_funcs
   * @since 1.5.0
   */
  def sha1(e: Column): Column = withExpr { Sha1(e.expr) }

  /**
   * Calculates the SHA-2 family of hash functions of a binary column and
   * returns the value as a hex string.
   *
   * @param e column to compute SHA-2 on.
   * @param numBits one of 224, 256, 384, or 512.
   *
   * @group misc_funcs
   * @since 1.5.0
   */
  def sha2(e: Column, numBits: Int): Column = {
    require(Seq(0, 224, 256, 384, 512).contains(numBits),
      s"numBits $numBits is not in the permitted values (0, 224, 256, 384, 512)")
    withExpr { Sha2(e.expr, lit(numBits).expr) }
  }

  /**
   * Calculates the cyclic redundancy check value  (CRC32) of a binary column and
   * returns the value as a bigint.
   *
   * @group misc_funcs
   * @since 1.5.0
   */
  def crc32(e: Column): Column = withExpr { Crc32(e.expr) }

  /**
   * Calculates the hash code of given columns, and returns the result as an int column.
   *
   * @group misc_funcs
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def hash(cols: Column*): Column = withExpr {
    new Murmur3Hash(cols.map(_.expr))
  }

  /**
   * Calculates the hash code of given columns using the 64-bit
   * variant of the xxHash algorithm, and returns the result as a long
   * column.
   *
   * @group misc_funcs
   * @since 3.0.0
   */
  @scala.annotation.varargs
  def xxhash64(cols: Column*): Column = withExpr {
    new XxHash64(cols.map(_.expr))
  }

  /**
   * Returns null if the condition is true, and throws an exception otherwise.
   *
   * @group misc_funcs
   * @since 3.1.0
   */
  def assert_true(c: Column): Column = withExpr {
    new AssertTrue(c.expr)
  }

  /**
   * Returns null if the condition is true; throws an exception with the error message otherwise.
   *
   * @group misc_funcs
   * @since 3.1.0
   */
  def assert_true(c: Column, e: Column): Column = withExpr {
    new AssertTrue(c.expr, e.expr)
  }

  /**
   * Throws an exception with the provided error message.
   *
   * @group misc_funcs
   * @since 3.1.0
   */
  def raise_error(c: Column): Column = withExpr {
    RaiseError(c.expr)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // String functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Computes the numeric value of the first character of the string column, and returns the
   * result as an int column.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def ascii(e: Column): Column = withExpr { Ascii(e.expr) }

  /**
   * Computes the BASE64 encoding of a binary column and returns it as a string column.
   * This is the reverse of unbase64.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def base64(e: Column): Column = withExpr { Base64(e.expr) }

  /**
   * Calculates the bit length for the specified string column.
   *
   * @group string_funcs
   * @since 3.3.0
   */
  def bit_length(e: Column): Column = withExpr { BitLength(e.expr) }

  /**
   * Concatenates multiple input string columns together into a single string column,
   * using the given separator.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def concat_ws(sep: String, exprs: Column*): Column = withExpr {
    ConcatWs(Literal.create(sep, StringType) +: exprs.map(_.expr))
  }

  /**
   * Computes the first argument into a string from a binary using the provided character set
   * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
   * If either argument is null, the result will also be null.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def decode(value: Column, charset: String): Column = withExpr {
    StringDecode(value.expr, lit(charset).expr)
  }

  /**
   * Computes the first argument into a binary from a string using the provided character set
   * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
   * If either argument is null, the result will also be null.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def encode(value: Column, charset: String): Column = withExpr {
    Encode(value.expr, lit(charset).expr)
  }

  /**
   * Formats numeric column x to a format like '#,###,###.##', rounded to d decimal places
   * with HALF_EVEN round mode, and returns the result as a string column.
   *
   * If d is 0, the result has no decimal point or fractional part.
   * If d is less than 0, the result will be null.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def format_number(x: Column, d: Int): Column = withExpr {
    FormatNumber(x.expr, lit(d).expr)
  }

  /**
   * Formats the arguments in printf-style and returns the result as a string column.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def format_string(format: String, arguments: Column*): Column = withExpr {
    FormatString((lit(format) +: arguments).map(_.expr): _*)
  }

  /**
   * Returns a new string column by converting the first letter of each word to uppercase.
   * Words are delimited by whitespace.
   *
   * For example, "hello world" will become "Hello World".
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def initcap(e: Column): Column = withExpr { InitCap(e.expr) }

  /**
   * Locate the position of the first occurrence of substr column in the given string.
   * Returns null if either of the arguments are null.
   *
   * @note The position is not zero based, but 1 based index. Returns 0 if substr
   * could not be found in str.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def instr(str: Column, substring: String): Column = withExpr {
    StringInstr(str.expr, lit(substring).expr)
  }

  /**
   * Computes the character length of a given string or number of bytes of a binary string.
   * The length of character strings include the trailing spaces. The length of binary strings
   * includes binary zeros.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def length(e: Column): Column = withExpr { Length(e.expr) }

  /**
   * Converts a string column to lower case.
   *
   * @group string_funcs
   * @since 1.3.0
   */
  def lower(e: Column): Column = withExpr { Lower(e.expr) }

  /**
   * Computes the Levenshtein distance of the two given string columns.
   * @group string_funcs
   * @since 1.5.0
   */
  def levenshtein(l: Column, r: Column): Column = withExpr { Levenshtein(l.expr, r.expr) }

  /**
   * Locate the position of the first occurrence of substr.
   *
   * @note The position is not zero based, but 1 based index. Returns 0 if substr
   * could not be found in str.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def locate(substr: String, str: Column): Column = withExpr {
    new StringLocate(lit(substr).expr, str.expr)
  }

  /**
   * Locate the position of the first occurrence of substr in a string column, after position pos.
   *
   * @note The position is not zero based, but 1 based index. returns 0 if substr
   * could not be found in str.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def locate(substr: String, str: Column, pos: Int): Column = withExpr {
    StringLocate(lit(substr).expr, str.expr, lit(pos).expr)
  }

  /**
   * Left-pad the string column with pad to a length of len. If the string column is longer
   * than len, the return value is shortened to len characters.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def lpad(str: Column, len: Int, pad: String): Column = withExpr {
    StringLPad(str.expr, lit(len).expr, lit(pad).expr)
  }

  /**
   * Left-pad the binary column with pad to a byte length of len. If the binary column is longer
   * than len, the return value is shortened to len bytes.
   *
   * @group string_funcs
   * @since 3.3.0
   */
  def lpad(str: Column, len: Int, pad: Array[Byte]): Column = withExpr {
    UnresolvedFunction("lpad", Seq(str.expr, lit(len).expr, lit(pad).expr), isDistinct = false)
  }

  /**
   * Trim the spaces from left end for the specified string value.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def ltrim(e: Column): Column = withExpr {StringTrimLeft(e.expr) }

  /**
   * Trim the specified character string from left end for the specified string column.
   * @group string_funcs
   * @since 2.3.0
   */
  def ltrim(e: Column, trimString: String): Column = withExpr {
    StringTrimLeft(e.expr, Literal(trimString))
  }

  /**
   * Calculates the byte length for the specified string column.
   *
   * @group string_funcs
   * @since 3.3.0
   */
  def octet_length(e: Column): Column = withExpr { OctetLength(e.expr) }

  /**
   * Extract a specific group matched by a Java regex, from the specified string column.
   * If the regex did not match, or the specified group did not match, an empty string is returned.
   * if the specified group index exceeds the group count of regex, an IllegalArgumentException
   * will be thrown.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def regexp_extract(e: Column, exp: String, groupIdx: Int): Column = withExpr {
    RegExpExtract(e.expr, lit(exp).expr, lit(groupIdx).expr)
  }

  /**
   * Replace all substrings of the specified string value that match regexp with rep.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def regexp_replace(e: Column, pattern: String, replacement: String): Column = withExpr {
    RegExpReplace(e.expr, lit(pattern).expr, lit(replacement).expr)
  }

  /**
   * Replace all substrings of the specified string value that match regexp with rep.
   *
   * @group string_funcs
   * @since 2.1.0
   */
  def regexp_replace(e: Column, pattern: Column, replacement: Column): Column = withExpr {
    RegExpReplace(e.expr, pattern.expr, replacement.expr)
  }

  /**
   * Decodes a BASE64 encoded string column and returns it as a binary column.
   * This is the reverse of base64.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def unbase64(e: Column): Column = withExpr { UnBase64(e.expr) }

  /**
   * Right-pad the string column with pad to a length of len. If the string column is longer
   * than len, the return value is shortened to len characters.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def rpad(str: Column, len: Int, pad: String): Column = withExpr {
    StringRPad(str.expr, lit(len).expr, lit(pad).expr)
  }

  /**
   * Right-pad the binary column with pad to a byte length of len. If the binary column is longer
   * than len, the return value is shortened to len bytes.
   *
   * @group string_funcs
   * @since 3.3.0
   */
  def rpad(str: Column, len: Int, pad: Array[Byte]): Column = withExpr {
    UnresolvedFunction("rpad", Seq(str.expr, lit(len).expr, lit(pad).expr), isDistinct = false)
  }

  /**
   * Repeats a string column n times, and returns it as a new string column.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def repeat(str: Column, n: Int): Column = withExpr {
    StringRepeat(str.expr, lit(n).expr)
  }

  /**
   * Trim the spaces from right end for the specified string value.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def rtrim(e: Column): Column = withExpr { StringTrimRight(e.expr) }

  /**
   * Trim the specified character string from right end for the specified string column.
   * @group string_funcs
   * @since 2.3.0
   */
  def rtrim(e: Column, trimString: String): Column = withExpr {
    StringTrimRight(e.expr, Literal(trimString))
  }

  /**
   * Returns the soundex code for the specified expression.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def soundex(e: Column): Column = withExpr { SoundEx(e.expr) }

  /**
   * Splits str around matches of the given pattern.
   *
   * @param str a string expression to split
   * @param pattern a string representing a regular expression. The regex string should be
   *                a Java regular expression.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def split(str: Column, pattern: String): Column = withExpr {
    StringSplit(str.expr, Literal(pattern), Literal(-1))
  }

  /**
   * Splits str around matches of the given pattern.
   *
   * @param str a string expression to split
   * @param pattern a string representing a regular expression. The regex string should be
   *                a Java regular expression.
   * @param limit an integer expression which controls the number of times the regex is applied.
   *        <ul>
   *          <li>limit greater than 0: The resulting array's length will not be more than limit,
   *          and the resulting array's last entry will contain all input beyond the last
   *          matched regex.</li>
   *          <li>limit less than or equal to 0: `regex` will be applied as many times as
   *          possible, and the resulting array can be of any size.</li>
   *        </ul>
   *
   * @group string_funcs
   * @since 3.0.0
   */
  def split(str: Column, pattern: String, limit: Int): Column = withExpr {
    StringSplit(str.expr, Literal(pattern), Literal(limit))
  }

  /**
   * Substring starts at `pos` and is of length `len` when str is String type or
   * returns the slice of byte array that starts at `pos` in byte and is of length `len`
   * when str is Binary type
   *
   * @note The position is not zero based, but 1 based index.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def substring(str: Column, pos: Int, len: Int): Column = withExpr {
    Substring(str.expr, lit(pos).expr, lit(len).expr)
  }

  /**
   * Returns the substring from string str before count occurrences of the delimiter delim.
   * If count is positive, everything the left of the final delimiter (counting from left) is
   * returned. If count is negative, every to the right of the final delimiter (counting from the
   * right) is returned. substring_index performs a case-sensitive match when searching for delim.
   *
   * @group string_funcs
   */
  def substring_index(str: Column, delim: String, count: Int): Column = withExpr {
    SubstringIndex(str.expr, lit(delim).expr, lit(count).expr)
  }

  /**
   * Overlay the specified portion of `src` with `replace`,
   *  starting from byte position `pos` of `src` and proceeding for `len` bytes.
   *
   * @group string_funcs
   * @since 3.0.0
   */
  def overlay(src: Column, replace: Column, pos: Column, len: Column): Column = withExpr {
    Overlay(src.expr, replace.expr, pos.expr, len.expr)
  }

  /**
   * Overlay the specified portion of `src` with `replace`,
   *  starting from byte position `pos` of `src`.
   *
   * @group string_funcs
   * @since 3.0.0
   */
  def overlay(src: Column, replace: Column, pos: Column): Column = withExpr {
    new Overlay(src.expr, replace.expr, pos.expr)
  }

  /**
   * Splits a string into arrays of sentences, where each sentence is an array of words.
   * @group string_funcs
   * @since 3.2.0
   */
  def sentences(string: Column, language: Column, country: Column): Column = withExpr {
    Sentences(string.expr, language.expr, country.expr)
  }

  /**
   * Splits a string into arrays of sentences, where each sentence is an array of words.
   * The default locale is used.
   * @group string_funcs
   * @since 3.2.0
   */
  def sentences(string: Column): Column = withExpr {
    Sentences(string.expr)
  }

  /**
   * Translate any character in the src by a character in replaceString.
   * The characters in replaceString correspond to the characters in matchingString.
   * The translate will happen when any character in the string matches the character
   * in the `matchingString`.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def translate(src: Column, matchingString: String, replaceString: String): Column = withExpr {
    StringTranslate(src.expr, lit(matchingString).expr, lit(replaceString).expr)
  }

  /**
   * Trim the spaces from both ends for the specified string column.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def trim(e: Column): Column = withExpr { StringTrim(e.expr) }

  /**
   * Trim the specified character from both ends for the specified string column.
   * @group string_funcs
   * @since 2.3.0
   */
  def trim(e: Column, trimString: String): Column = withExpr {
    StringTrim(e.expr, Literal(trimString))
  }

  /**
   * Converts a string column to upper case.
   *
   * @group string_funcs
   * @since 1.3.0
   */
  def upper(e: Column): Column = withExpr { Upper(e.expr) }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // DateTime functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the date that is `numMonths` after `startDate`.
   *
   * @param startDate A date, timestamp or string. If a string, the data must be in a format that
   *                  can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param numMonths The number of months to add to `startDate`, can be negative to subtract months
   * @return A date, or null if `startDate` was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 1.5.0
   */
  def add_months(startDate: Column, numMonths: Int): Column = add_months(startDate, lit(numMonths))

  /**
   * Returns the date that is `numMonths` after `startDate`.
   *
   * @param startDate A date, timestamp or string. If a string, the data must be in a format that
   *                  can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param numMonths A column of the number of months to add to `startDate`, can be negative to
   *                  subtract months
   * @return A date, or null if `startDate` was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 3.0.0
   */
  def add_months(startDate: Column, numMonths: Column): Column = withExpr {
    AddMonths(startDate.expr, numMonths.expr)
  }

  /**
   * Returns the current date at the start of query evaluation as a date column.
   * All calls of current_date within the same query return the same value.
   *
   * @group datetime_funcs
   * @since 1.5.0
   */
  def current_date(): Column = withExpr { CurrentDate() }

  /**
   * Returns the current timestamp at the start of query evaluation as a timestamp column.
   * All calls of current_timestamp within the same query return the same value.
   *
   * @group datetime_funcs
   * @since 1.5.0
   */
  def current_timestamp(): Column = withExpr { CurrentTimestamp() }

  /**
   * Returns the current timestamp without time zone at the start of query evaluation
   * as a timestamp without time zone column.
   * All calls of localtimestamp within the same query return the same value.
   *
   * @group datetime_funcs
   * @since 3.3.0
   */
  def localtimestamp(): Column = withExpr { LocalTimestamp() }

  /**
   * Converts a date/timestamp/string to a value of string in the format specified by the date
   * format given by the second argument.
   *
   * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">
   *   Datetime Patterns</a>
   * for valid date and time format patterns
   *
   * @param dateExpr A date, timestamp or string. If a string, the data must be in a format that
   *                 can be cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param format A pattern `dd.MM.yyyy` would return a string like `18.03.1993`
   * @return A string, or null if `dateExpr` was a string that could not be cast to a timestamp
   * @note Use specialized functions like [[year]] whenever possible as they benefit from a
   * specialized implementation.
   * @throws IllegalArgumentException if the `format` pattern is invalid
   * @group datetime_funcs
   * @since 1.5.0
   */
  def date_format(dateExpr: Column, format: String): Column = withExpr {
    DateFormatClass(dateExpr.expr, Literal(format))
  }

  /**
   * Returns the date that is `days` days after `start`
   *
   * @param start A date, timestamp or string. If a string, the data must be in a format that
   *              can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param days  The number of days to add to `start`, can be negative to subtract days
   * @return A date, or null if `start` was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 1.5.0
   */
  def date_add(start: Column, days: Int): Column = date_add(start, lit(days))

  /**
   * Returns the date that is `days` days after `start`
   *
   * @param start A date, timestamp or string. If a string, the data must be in a format that
   *              can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param days  A column of the number of days to add to `start`, can be negative to subtract days
   * @return A date, or null if `start` was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 3.0.0
   */
  def date_add(start: Column, days: Column): Column = withExpr { DateAdd(start.expr, days.expr) }

  /**
   * Returns the date that is `days` days before `start`
   *
   * @param start A date, timestamp or string. If a string, the data must be in a format that
   *              can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param days  The number of days to subtract from `start`, can be negative to add days
   * @return A date, or null if `start` was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 1.5.0
   */
  def date_sub(start: Column, days: Int): Column = date_sub(start, lit(days))

  /**
   * Returns the date that is `days` days before `start`
   *
   * @param start A date, timestamp or string. If a string, the data must be in a format that
   *              can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param days  A column of the number of days to subtract from `start`, can be negative to add
   *              days
   * @return A date, or null if `start` was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 3.0.0
   */
  def date_sub(start: Column, days: Column): Column = withExpr { DateSub(start.expr, days.expr) }

  /**
   * Returns the number of days from `start` to `end`.
   *
   * Only considers the date part of the input. For example:
   * {{{
   * dateddiff("2018-01-10 00:00:00", "2018-01-09 23:59:59")
   * // returns 1
   * }}}
   *
   * @param end A date, timestamp or string. If a string, the data must be in a format that
   *            can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param start A date, timestamp or string. If a string, the data must be in a format that
   *              can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @return An integer, or null if either `end` or `start` were strings that could not be cast to
   *         a date. Negative if `end` is before `start`
   * @group datetime_funcs
   * @since 1.5.0
   */
  def datediff(end: Column, start: Column): Column = withExpr { DateDiff(end.expr, start.expr) }

  /**
   * Extracts the year as an integer from a given date/timestamp/string.
   * @return An integer, or null if the input was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 1.5.0
   */
  def year(e: Column): Column = withExpr { Year(e.expr) }

  /**
   * Extracts the quarter as an integer from a given date/timestamp/string.
   * @return An integer, or null if the input was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 1.5.0
   */
  def quarter(e: Column): Column = withExpr { Quarter(e.expr) }

  /**
   * Extracts the month as an integer from a given date/timestamp/string.
   * @return An integer, or null if the input was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 1.5.0
   */
  def month(e: Column): Column = withExpr { Month(e.expr) }

  /**
   * Extracts the day of the week as an integer from a given date/timestamp/string.
   * Ranges from 1 for a Sunday through to 7 for a Saturday
   * @return An integer, or null if the input was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 2.3.0
   */
  def dayofweek(e: Column): Column = withExpr { DayOfWeek(e.expr) }

  /**
   * Extracts the day of the month as an integer from a given date/timestamp/string.
   * @return An integer, or null if the input was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 1.5.0
   */
  def dayofmonth(e: Column): Column = withExpr { DayOfMonth(e.expr) }

  /**
   * Extracts the day of the year as an integer from a given date/timestamp/string.
   * @return An integer, or null if the input was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 1.5.0
   */
  def dayofyear(e: Column): Column = withExpr { DayOfYear(e.expr) }

  /**
   * Extracts the hours as an integer from a given date/timestamp/string.
   * @return An integer, or null if the input was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 1.5.0
   */
  def hour(e: Column): Column = withExpr { Hour(e.expr) }

  /**
   * Returns the last day of the month which the given date belongs to.
   * For example, input "2015-07-27" returns "2015-07-31" since July 31 is the last day of the
   * month in July 2015.
   *
   * @param e A date, timestamp or string. If a string, the data must be in a format that can be
   *          cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @return A date, or null if the input was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 1.5.0
   */
  def last_day(e: Column): Column = withExpr { LastDay(e.expr) }

  /**
   * Extracts the minutes as an integer from a given date/timestamp/string.
   * @return An integer, or null if the input was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 1.5.0
   */
  def minute(e: Column): Column = withExpr { Minute(e.expr) }

  /**
   * @return A date created from year, month and day fields.
   * @group datetime_funcs
   * @since 3.3.0
   */
  def make_date(year: Column, month: Column, day: Column): Column = withExpr {
    MakeDate(year.expr, month.expr, day.expr)
  }

  /**
   * Returns number of months between dates `start` and `end`.
   *
   * A whole number is returned if both inputs have the same day of month or both are the last day
   * of their respective months. Otherwise, the difference is calculated assuming 31 days per month.
   *
   * For example:
   * {{{
   * months_between("2017-11-14", "2017-07-14")  // returns 4.0
   * months_between("2017-01-01", "2017-01-10")  // returns 0.29032258
   * months_between("2017-06-01", "2017-06-16 12:00:00")  // returns -0.5
   * }}}
   *
   * @param end   A date, timestamp or string. If a string, the data must be in a format that can
   *              be cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param start A date, timestamp or string. If a string, the data must be in a format that can
   *              cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @return A double, or null if either `end` or `start` were strings that could not be cast to a
   *         timestamp. Negative if `end` is before `start`
   * @group datetime_funcs
   * @since 1.5.0
   */
  def months_between(end: Column, start: Column): Column = withExpr {
    new MonthsBetween(end.expr, start.expr)
  }

  /**
   * Returns number of months between dates `end` and `start`. If `roundOff` is set to true, the
   * result is rounded off to 8 digits; it is not rounded otherwise.
   * @group datetime_funcs
   * @since 2.4.0
   */
  def months_between(end: Column, start: Column, roundOff: Boolean): Column = withExpr {
    MonthsBetween(end.expr, start.expr, lit(roundOff).expr)
  }

  /**
   * Returns the first date which is later than the value of the `date` column that is on the
   * specified day of the week.
   *
   * For example, `next_day('2015-07-27', "Sunday")` returns 2015-08-02 because that is the first
   * Sunday after 2015-07-27.
   *
   * @param date      A date, timestamp or string. If a string, the data must be in a format that
   *                  can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param dayOfWeek Case insensitive, and accepts: "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"
   * @return A date, or null if `date` was a string that could not be cast to a date or if
   *         `dayOfWeek` was an invalid value
   * @group datetime_funcs
   * @since 1.5.0
   */
  def next_day(date: Column, dayOfWeek: String): Column = next_day(date, lit(dayOfWeek))

  /**
   * Returns the first date which is later than the value of the `date` column that is on the
   * specified day of the week.
   *
   * For example, `next_day('2015-07-27', "Sunday")` returns 2015-08-02 because that is the first
   * Sunday after 2015-07-27.
   *
   * @param date      A date, timestamp or string. If a string, the data must be in a format that
   *                  can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param dayOfWeek A column of the day of week. Case insensitive, and accepts: "Mon", "Tue",
   *                  "Wed", "Thu", "Fri", "Sat", "Sun"
   * @return A date, or null if `date` was a string that could not be cast to a date or if
   *         `dayOfWeek` was an invalid value
   * @group datetime_funcs
   * @since 3.2.0
   */
  def next_day(date: Column, dayOfWeek: Column): Column = withExpr {
    NextDay(date.expr, dayOfWeek.expr)
  }

  /**
   * Extracts the seconds as an integer from a given date/timestamp/string.
   * @return An integer, or null if the input was a string that could not be cast to a timestamp
   * @group datetime_funcs
   * @since 1.5.0
   */
  def second(e: Column): Column = withExpr { Second(e.expr) }

  /**
   * Extracts the week number as an integer from a given date/timestamp/string.
   *
   * A week is considered to start on a Monday and week 1 is the first week with more than 3 days,
   * as defined by ISO 8601
   *
   * @return An integer, or null if the input was a string that could not be cast to a date
   * @group datetime_funcs
   * @since 1.5.0
   */
  def weekofyear(e: Column): Column = withExpr { WeekOfYear(e.expr) }

  /**
   * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
   * representing the timestamp of that moment in the current system time zone in the
   * yyyy-MM-dd HH:mm:ss format.
   *
   * @param ut A number of a type that is castable to a long, such as string or integer. Can be
   *           negative for timestamps before the unix epoch
   * @return A string, or null if the input was a string that could not be cast to a long
   * @group datetime_funcs
   * @since 1.5.0
   */
  def from_unixtime(ut: Column): Column = withExpr {
    FromUnixTime(ut.expr, Literal(TimestampFormatter.defaultPattern))
  }

  /**
   * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
   * representing the timestamp of that moment in the current system time zone in the given
   * format.
   *
   * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">
   *   Datetime Patterns</a>
   * for valid date and time format patterns
   *
   * @param ut A number of a type that is castable to a long, such as string or integer. Can be
   *           negative for timestamps before the unix epoch
   * @param f  A date time pattern that the input will be formatted to
   * @return A string, or null if `ut` was a string that could not be cast to a long or `f` was
   *         an invalid date time pattern
   * @group datetime_funcs
   * @since 1.5.0
   */
  def from_unixtime(ut: Column, f: String): Column = withExpr {
    FromUnixTime(ut.expr, Literal(f))
  }

  /**
   * Returns the current Unix timestamp (in seconds) as a long.
   *
   * @note All calls of `unix_timestamp` within the same query return the same value
   * (i.e. the current timestamp is calculated at the start of query evaluation).
   *
   * @group datetime_funcs
   * @since 1.5.0
   */
  def unix_timestamp(): Column = withExpr {
    UnixTimestamp(CurrentTimestamp(), Literal(TimestampFormatter.defaultPattern))
  }

  /**
   * Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds),
   * using the default timezone and the default locale.
   *
   * @param s A date, timestamp or string. If a string, the data must be in the
   *          `yyyy-MM-dd HH:mm:ss` format
   * @return A long, or null if the input was a string not of the correct format
   * @group datetime_funcs
   * @since 1.5.0
   */
  def unix_timestamp(s: Column): Column = withExpr {
    UnixTimestamp(s.expr, Literal(TimestampFormatter.defaultPattern))
  }

  /**
   * Converts time string with given pattern to Unix timestamp (in seconds).
   *
   * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">
   *   Datetime Patterns</a>
   * for valid date and time format patterns
   *
   * @param s A date, timestamp or string. If a string, the data must be in a format that can be
   *          cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param p A date time pattern detailing the format of `s` when `s` is a string
   * @return A long, or null if `s` was a string that could not be cast to a date or `p` was
   *         an invalid format
   * @group datetime_funcs
   * @since 1.5.0
   */
  def unix_timestamp(s: Column, p: String): Column = withExpr { UnixTimestamp(s.expr, Literal(p)) }

  /**
   * Converts to a timestamp by casting rules to `TimestampType`.
   *
   * @param s A date, timestamp or string. If a string, the data must be in a format that can be
   *          cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @return A timestamp, or null if the input was a string that could not be cast to a timestamp
   * @group datetime_funcs
   * @since 2.2.0
   */
  def to_timestamp(s: Column): Column = withExpr {
    new ParseToTimestamp(s.expr)
  }

  /**
   * Converts time string with the given pattern to timestamp.
   *
   * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">
   *   Datetime Patterns</a>
   * for valid date and time format patterns
   *
   * @param s   A date, timestamp or string. If a string, the data must be in a format that can be
   *            cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param fmt A date time pattern detailing the format of `s` when `s` is a string
   * @return A timestamp, or null if `s` was a string that could not be cast to a timestamp or
   *         `fmt` was an invalid format
   * @group datetime_funcs
   * @since 2.2.0
   */
  def to_timestamp(s: Column, fmt: String): Column = withExpr {
    new ParseToTimestamp(s.expr, Literal(fmt))
  }

  /**
   * Converts the column into `DateType` by casting rules to `DateType`.
   *
   * @group datetime_funcs
   * @since 1.5.0
   */
  def to_date(e: Column): Column = withExpr { new ParseToDate(e.expr) }

  /**
   * Converts the column into a `DateType` with a specified format
   *
   * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">
   *   Datetime Patterns</a>
   * for valid date and time format patterns
   *
   * @param e   A date, timestamp or string. If a string, the data must be in a format that can be
   *            cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param fmt A date time pattern detailing the format of `e` when `e`is a string
   * @return A date, or null if `e` was a string that could not be cast to a date or `fmt` was an
   *         invalid format
   * @group datetime_funcs
   * @since 2.2.0
   */
  def to_date(e: Column, fmt: String): Column = withExpr {
    new ParseToDate(e.expr, Literal(fmt))
  }

  /**
   * Returns date truncated to the unit specified by the format.
   *
   * For example, `trunc("2018-11-19 12:01:19", "year")` returns 2018-01-01
   *
   * @param date A date, timestamp or string. If a string, the data must be in a format that can be
   *             cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param format: 'year', 'yyyy', 'yy' to truncate by year,
   *               or 'month', 'mon', 'mm' to truncate by month
   *               Other options are: 'week', 'quarter'
   *
   * @return A date, or null if `date` was a string that could not be cast to a date or `format`
   *         was an invalid value
   * @group datetime_funcs
   * @since 1.5.0
   */
  def trunc(date: Column, format: String): Column = withExpr {
    TruncDate(date.expr, Literal(format))
  }

  /**
   * Returns timestamp truncated to the unit specified by the format.
   *
   * For example, `date_trunc("year", "2018-11-19 12:01:19")` returns 2018-01-01 00:00:00
   *
   * @param format: 'year', 'yyyy', 'yy' to truncate by year,
   *                'month', 'mon', 'mm' to truncate by month,
   *                'day', 'dd' to truncate by day,
   *                Other options are:
   *                'microsecond', 'millisecond', 'second', 'minute', 'hour', 'week', 'quarter'
   * @param timestamp A date, timestamp or string. If a string, the data must be in a format that
   *                  can be cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @return A timestamp, or null if `timestamp` was a string that could not be cast to a timestamp
   *         or `format` was an invalid value
   * @group datetime_funcs
   * @since 2.3.0
   */
  def date_trunc(format: String, timestamp: Column): Column = withExpr {
    TruncTimestamp(Literal(format), timestamp.expr)
  }

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC, and renders
   * that time as a timestamp in the given time zone. For example, 'GMT+1' would yield
   * '2017-07-14 03:40:00.0'.
   *
   * @param ts A date, timestamp or string. If a string, the data must be in a format that can be
   *           cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param tz A string detailing the time zone ID that the input should be adjusted to. It should
   *           be in the format of either region-based zone IDs or zone offsets. Region IDs must
   *           have the form 'area/city', such as 'America/Los_Angeles'. Zone offsets must be in
   *           the format '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are
   *           supported as aliases of '+00:00'. Other short names are not recommended to use
   *           because they can be ambiguous.
   * @return A timestamp, or null if `ts` was a string that could not be cast to a timestamp or
   *         `tz` was an invalid value
   * @group datetime_funcs
   * @since 1.5.0
   */
  def from_utc_timestamp(ts: Column, tz: String): Column = withExpr {
    FromUTCTimestamp(ts.expr, Literal(tz))
  }

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC, and renders
   * that time as a timestamp in the given time zone. For example, 'GMT+1' would yield
   * '2017-07-14 03:40:00.0'.
   * @group datetime_funcs
   * @since 2.4.0
   */
  def from_utc_timestamp(ts: Column, tz: Column): Column = withExpr {
    FromUTCTimestamp(ts.expr, tz.expr)
  }

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in the given time
   * zone, and renders that time as a timestamp in UTC. For example, 'GMT+1' would yield
   * '2017-07-14 01:40:00.0'.
   *
   * @param ts A date, timestamp or string. If a string, the data must be in a format that can be
   *           cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param tz A string detailing the time zone ID that the input should be adjusted to. It should
   *           be in the format of either region-based zone IDs or zone offsets. Region IDs must
   *           have the form 'area/city', such as 'America/Los_Angeles'. Zone offsets must be in
   *           the format '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are
   *           supported as aliases of '+00:00'. Other short names are not recommended to use
   *           because they can be ambiguous.
   * @return A timestamp, or null if `ts` was a string that could not be cast to a timestamp or
   *         `tz` was an invalid value
   * @group datetime_funcs
   * @since 1.5.0
   */
  def to_utc_timestamp(ts: Column, tz: String): Column = withExpr {
    ToUTCTimestamp(ts.expr, Literal(tz))
  }

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in the given time
   * zone, and renders that time as a timestamp in UTC. For example, 'GMT+1' would yield
   * '2017-07-14 01:40:00.0'.
   * @group datetime_funcs
   * @since 2.4.0
   */
  def to_utc_timestamp(ts: Column, tz: Column): Column = withExpr {
    ToUTCTimestamp(ts.expr, tz.expr)
  }

  /**
   * Bucketize rows into one or more time windows given a timestamp specifying column. Window
   * starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
   * [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
   * the order of months are not supported. The following example takes the average stock price for
   * a one minute window every 10 seconds starting 5 seconds after the hour:
   *
   * {{{
   *   val df = ... // schema => timestamp: TimestampType, stockId: StringType, price: DoubleType
   *   df.groupBy(window($"timestamp", "1 minute", "10 seconds", "5 seconds"), $"stockId")
   *     .agg(mean("price"))
   * }}}
   *
   * The windows will look like:
   *
   * {{{
   *   09:00:05-09:01:05
   *   09:00:15-09:01:15
   *   09:00:25-09:01:25 ...
   * }}}
   *
   * For a streaming query, you may use the function `current_timestamp` to generate windows on
   * processing time.
   *
   * @param timeColumn The column or the expression to use as the timestamp for windowing by time.
   *                   The time column must be of TimestampType or TimestampNTZType.
   * @param windowDuration A string specifying the width of the window, e.g. `10 minutes`,
   *                       `1 second`. Check `org.apache.spark.unsafe.types.CalendarInterval` for
   *                       valid duration identifiers. Note that the duration is a fixed length of
   *                       time, and does not vary over time according to a calendar. For example,
   *                       `1 day` always means 86,400,000 milliseconds, not a calendar day.
   * @param slideDuration A string specifying the sliding interval of the window, e.g. `1 minute`.
   *                      A new window will be generated every `slideDuration`. Must be less than
   *                      or equal to the `windowDuration`. Check
   *                      `org.apache.spark.unsafe.types.CalendarInterval` for valid duration
   *                      identifiers. This duration is likewise absolute, and does not vary
   *                      according to a calendar.
   * @param startTime The offset with respect to 1970-01-01 00:00:00 UTC with which to start
   *                  window intervals. For example, in order to have hourly tumbling windows that
   *                  start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide
   *                  `startTime` as `15 minutes`.
   *
   * @group datetime_funcs
   * @since 2.0.0
   */
  def window(
      timeColumn: Column,
      windowDuration: String,
      slideDuration: String,
      startTime: String): Column = {
    withExpr {
      TimeWindow(timeColumn.expr, windowDuration, slideDuration, startTime)
    }.as("window")
  }


  /**
   * Bucketize rows into one or more time windows given a timestamp specifying column. Window
   * starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
   * [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
   * the order of months are not supported. The windows start beginning at 1970-01-01 00:00:00 UTC.
   * The following example takes the average stock price for a one minute window every 10 seconds:
   *
   * {{{
   *   val df = ... // schema => timestamp: TimestampType, stockId: StringType, price: DoubleType
   *   df.groupBy(window($"timestamp", "1 minute", "10 seconds"), $"stockId")
   *     .agg(mean("price"))
   * }}}
   *
   * The windows will look like:
   *
   * {{{
   *   09:00:00-09:01:00
   *   09:00:10-09:01:10
   *   09:00:20-09:01:20 ...
   * }}}
   *
   * For a streaming query, you may use the function `current_timestamp` to generate windows on
   * processing time.
   *
   * @param timeColumn The column or the expression to use as the timestamp for windowing by time.
   *                   The time column must be of TimestampType or TimestampNTZType.
   * @param windowDuration A string specifying the width of the window, e.g. `10 minutes`,
   *                       `1 second`. Check `org.apache.spark.unsafe.types.CalendarInterval` for
   *                       valid duration identifiers. Note that the duration is a fixed length of
   *                       time, and does not vary over time according to a calendar. For example,
   *                       `1 day` always means 86,400,000 milliseconds, not a calendar day.
   * @param slideDuration A string specifying the sliding interval of the window, e.g. `1 minute`.
   *                      A new window will be generated every `slideDuration`. Must be less than
   *                      or equal to the `windowDuration`. Check
   *                      `org.apache.spark.unsafe.types.CalendarInterval` for valid duration
   *                      identifiers. This duration is likewise absolute, and does not vary
   *                      according to a calendar.
   *
   * @group datetime_funcs
   * @since 2.0.0
   */
  def window(timeColumn: Column, windowDuration: String, slideDuration: String): Column = {
    window(timeColumn, windowDuration, slideDuration, "0 second")
  }

  /**
   * Generates tumbling time windows given a timestamp specifying column. Window
   * starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
   * [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
   * the order of months are not supported. The windows start beginning at 1970-01-01 00:00:00 UTC.
   * The following example takes the average stock price for a one minute tumbling window:
   *
   * {{{
   *   val df = ... // schema => timestamp: TimestampType, stockId: StringType, price: DoubleType
   *   df.groupBy(window($"timestamp", "1 minute"), $"stockId")
   *     .agg(mean("price"))
   * }}}
   *
   * The windows will look like:
   *
   * {{{
   *   09:00:00-09:01:00
   *   09:01:00-09:02:00
   *   09:02:00-09:03:00 ...
   * }}}
   *
   * For a streaming query, you may use the function `current_timestamp` to generate windows on
   * processing time.
   *
   * @param timeColumn The column or the expression to use as the timestamp for windowing by time.
   *                   The time column must be of TimestampType or TimestampNTZType.
   * @param windowDuration A string specifying the width of the window, e.g. `10 minutes`,
   *                       `1 second`. Check `org.apache.spark.unsafe.types.CalendarInterval` for
   *                       valid duration identifiers.
   *
   * @group datetime_funcs
   * @since 2.0.0
   */
  def window(timeColumn: Column, windowDuration: String): Column = {
    window(timeColumn, windowDuration, windowDuration, "0 second")
  }

  /**
   * Generates session window given a timestamp specifying column.
   *
   * Session window is one of dynamic windows, which means the length of window is varying
   * according to the given inputs. The length of session window is defined as "the timestamp
   * of latest input of the session + gap duration", so when the new inputs are bound to the
   * current session window, the end time of session window can be expanded according to the new
   * inputs.
   *
   * Windows can support microsecond precision. gapDuration in the order of months are not
   * supported.
   *
   * For a streaming query, you may use the function `current_timestamp` to generate windows on
   * processing time.
   *
   * @param timeColumn The column or the expression to use as the timestamp for windowing by time.
   *                   The time column must be of TimestampType or TimestampNTZType.
   * @param gapDuration A string specifying the timeout of the session, e.g. `10 minutes`,
   *                    `1 second`. Check `org.apache.spark.unsafe.types.CalendarInterval` for
   *                    valid duration identifiers.
   *
   * @group datetime_funcs
   * @since 3.2.0
   */
  def session_window(timeColumn: Column, gapDuration: String): Column = {
    withExpr {
      SessionWindow(timeColumn.expr, gapDuration)
    }.as("session_window")
  }

  /**
   * Generates session window given a timestamp specifying column.
   *
   * Session window is one of dynamic windows, which means the length of window is varying
   * according to the given inputs. For static gap duration, the length of session window
   * is defined as "the timestamp of latest input of the session + gap duration", so when
   * the new inputs are bound to the current session window, the end time of session window
   * can be expanded according to the new inputs.
   *
   * Besides a static gap duration value, users can also provide an expression to specify
   * gap duration dynamically based on the input row. With dynamic gap duration, the closing
   * of a session window does not depend on the latest input anymore. A session window's range
   * is the union of all events' ranges which are determined by event start time and evaluated
   * gap duration during the query execution. Note that the rows with negative or zero gap
   * duration will be filtered out from the aggregation.
   *
   * Windows can support microsecond precision. gapDuration in the order of months are not
   * supported.
   *
   * For a streaming query, you may use the function `current_timestamp` to generate windows on
   * processing time.
   *
   * @param timeColumn The column or the expression to use as the timestamp for windowing by time.
   *                   The time column must be of TimestampType or TimestampNTZType.
   * @param gapDuration A column specifying the timeout of the session. It could be static value,
   *                    e.g. `10 minutes`, `1 second`, or an expression/UDF that specifies gap
   *                    duration dynamically based on the input row.
   *
   * @group datetime_funcs
   * @since 3.2.0
   */
  def session_window(timeColumn: Column, gapDuration: Column): Column = {
    withExpr {
      SessionWindow(timeColumn.expr, gapDuration.expr)
    }.as("session_window")
  }

  /**
   * Converts the number of seconds from the Unix epoch (1970-01-01T00:00:00Z)
   * to a timestamp.
   * @group datetime_funcs
   * @since 3.1.0
   */
  def timestamp_seconds(e: Column): Column = withExpr {
    SecondsToTimestamp(e.expr)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Collection functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns null if the array is null, true if the array contains `value`, and false otherwise.
   * @group collection_funcs
   * @since 1.5.0
   */
  def array_contains(column: Column, value: Any): Column = withExpr {
    ArrayContains(column.expr, lit(value).expr)
  }

  /**
   * Returns `true` if `a1` and `a2` have at least one non-null element in common. If not and both
   * the arrays are non-empty and any of them contains a `null`, it returns `null`. It returns
   * `false` otherwise.
   * @group collection_funcs
   * @since 2.4.0
   */
  def arrays_overlap(a1: Column, a2: Column): Column = withExpr {
    ArraysOverlap(a1.expr, a2.expr)
  }

  /**
   * Returns an array containing all the elements in `x` from index `start` (or starting from the
   * end if `start` is negative) with the specified `length`.
   *
   * @param x the array column to be sliced
   * @param start the starting index
   * @param length the length of the slice
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def slice(x: Column, start: Int, length: Int): Column =
    slice(x, lit(start), lit(length))

  /**
   * Returns an array containing all the elements in `x` from index `start` (or starting from the
   * end if `start` is negative) with the specified `length`.
   *
   * @param x the array column to be sliced
   * @param start the starting index
   * @param length the length of the slice
   *
   * @group collection_funcs
   * @since 3.1.0
   */
  def slice(x: Column, start: Column, length: Column): Column = withExpr {
    Slice(x.expr, start.expr, length.expr)
  }

  /**
   * Concatenates the elements of `column` using the `delimiter`. Null values are replaced with
   * `nullReplacement`.
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_join(column: Column, delimiter: String, nullReplacement: String): Column = withExpr {
    ArrayJoin(column.expr, Literal(delimiter), Some(Literal(nullReplacement)))
  }

  /**
   * Concatenates the elements of `column` using the `delimiter`.
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_join(column: Column, delimiter: String): Column = withExpr {
    ArrayJoin(column.expr, Literal(delimiter), None)
  }

  /**
   * Concatenates multiple input columns together into a single column.
   * The function works with strings, binary and compatible array columns.
   *
   * @group collection_funcs
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def concat(exprs: Column*): Column = withExpr { Concat(exprs.map(_.expr)) }

  /**
   * Locates the position of the first occurrence of the value in the given array as long.
   * Returns null if either of the arguments are null.
   *
   * @note The position is not zero based, but 1 based index. Returns 0 if value
   * could not be found in array.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_position(column: Column, value: Any): Column = withExpr {
    ArrayPosition(column.expr, lit(value).expr)
  }

  /**
   * Returns element of array at given index in value if column is array. Returns value for
   * the given key in value if column is map.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def element_at(column: Column, value: Any): Column = withExpr {
    ElementAt(column.expr, lit(value).expr)
  }

  /**
   * Returns element of array at given (0-based) index. If the index points
   * outside of the array boundaries, then this function returns NULL.
   *
   * @group collection_funcs
   * @since 3.4.0
   */
  def get(column: Column, index: Column): Column = withExpr {
    new Get(column.expr, index.expr)
  }

  /**
   * Sorts the input array in ascending order. The elements of the input array must be orderable.
   * NaN is greater than any non-NaN elements for double/float type.
   * Null elements will be placed at the end of the returned array.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_sort(e: Column): Column = withExpr { new ArraySort(e.expr) }

  /**
   * Sorts the input array based on the given comparator function. The comparator will take two
   * arguments representing two elements of the array. It returns a negative integer, 0, or a
   * positive integer as the first element is less than, equal to, or greater than the second
   * element. If the comparator function returns null, the function will fail and raise an error.
   *
   * @group collection_funcs
   * @since 3.4.0
   */
  def array_sort(e: Column, comparator: (Column, Column) => Column): Column = withExpr {
    new ArraySort(e.expr, createLambda(comparator))
  }

  /**
   * Remove all elements that equal to element from the given array.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_remove(column: Column, element: Any): Column = withExpr {
    ArrayRemove(column.expr, lit(element).expr)
  }

  /**
   * Removes duplicate values from the array.
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_distinct(e: Column): Column = withExpr { ArrayDistinct(e.expr) }

  /**
   * Returns an array of the elements in the intersection of the given two arrays,
   * without duplicates.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_intersect(col1: Column, col2: Column): Column = withExpr {
    ArrayIntersect(col1.expr, col2.expr)
  }

  /**
   * Returns an array of the elements in the union of the given two arrays, without duplicates.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_union(col1: Column, col2: Column): Column = withExpr {
    ArrayUnion(col1.expr, col2.expr)
  }

  /**
   * Returns an array of the elements in the first array but not in the second array,
   * without duplicates. The order of elements in the result is not determined
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_except(col1: Column, col2: Column): Column = withExpr {
    ArrayExcept(col1.expr, col2.expr)
  }

  private def createLambda(f: Column => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("x")))
    val function = f(Column(x)).expr
    LambdaFunction(function, Seq(x))
  }

  private def createLambda(f: (Column, Column) => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("y")))
    val function = f(Column(x), Column(y)).expr
    LambdaFunction(function, Seq(x, y))
  }

  private def createLambda(f: (Column, Column, Column) => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("y")))
    val z = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("z")))
    val function = f(Column(x), Column(y), Column(z)).expr
    LambdaFunction(function, Seq(x, y, z))
  }

  /**
   * Returns an array of elements after applying a transformation to each element
   * in the input array.
   * {{{
   *   df.select(transform(col("i"), x => x + 1))
   * }}}
   *
   * @param column the input array column
   * @param f col => transformed_col, the lambda function to transform the input column
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def transform(column: Column, f: Column => Column): Column = withExpr {
    ArrayTransform(column.expr, createLambda(f))
  }

  /**
   * Returns an array of elements after applying a transformation to each element
   * in the input array.
   * {{{
   *   df.select(transform(col("i"), (x, i) => x + i))
   * }}}
   *
   * @param column the input array column
   * @param f (col, index) => transformed_col, the lambda function to filter the input column
   *           given the index. Indices start at 0.
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def transform(column: Column, f: (Column, Column) => Column): Column = withExpr {
    ArrayTransform(column.expr, createLambda(f))
  }

  /**
   * Returns whether a predicate holds for one or more elements in the array.
   * {{{
   *   df.select(exists(col("i"), _ % 2 === 0))
   * }}}
   *
   * @param column the input array column
   * @param f col => predicate, the Boolean predicate to check the input column
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def exists(column: Column, f: Column => Column): Column = withExpr {
    ArrayExists(column.expr, createLambda(f))
  }

  /**
   * Returns whether a predicate holds for every element in the array.
   * {{{
   *   df.select(forall(col("i"), x => x % 2 === 0))
   * }}}
   *
   * @param column the input array column
   * @param f col => predicate, the Boolean predicate to check the input column
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def forall(column: Column, f: Column => Column): Column = withExpr {
    ArrayForAll(column.expr, createLambda(f))
  }

  /**
   * Returns an array of elements for which a predicate holds in a given array.
   * {{{
   *   df.select(filter(col("s"), x => x % 2 === 0))
   * }}}
   *
   * @param column the input array column
   * @param f col => predicate, the Boolean predicate to filter the input column
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def filter(column: Column, f: Column => Column): Column = withExpr {
    ArrayFilter(column.expr, createLambda(f))
  }

  /**
   * Returns an array of elements for which a predicate holds in a given array.
   * {{{
   *   df.select(filter(col("s"), (x, i) => i % 2 === 0))
   * }}}
   *
   * @param column the input array column
   * @param f (col, index) => predicate, the Boolean predicate to filter the input column
   *           given the index. Indices start at 0.
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def filter(column: Column, f: (Column, Column) => Column): Column = withExpr {
    ArrayFilter(column.expr, createLambda(f))
  }

  /**
   * Applies a binary operator to an initial state and all elements in the array,
   * and reduces this to a single state. The final state is converted into the final result
   * by applying a finish function.
   * {{{
   *   df.select(aggregate(col("i"), lit(0), (acc, x) => acc + x, _ * 10))
   * }}}
   *
   * @param expr the input array column
   * @param initialValue the initial value
   * @param merge (combined_value, input_value) => combined_value, the merge function to merge
   *              an input value to the combined_value
   * @param finish combined_value => final_value, the lambda function to convert the combined value
   *               of all inputs to final result
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def aggregate(
      expr: Column,
      initialValue: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column): Column = withExpr {
    ArrayAggregate(
      expr.expr,
      initialValue.expr,
      createLambda(merge),
      createLambda(finish)
    )
  }

  /**
   * Applies a binary operator to an initial state and all elements in the array,
   * and reduces this to a single state.
   * {{{
   *   df.select(aggregate(col("i"), lit(0), (acc, x) => acc + x))
   * }}}
   *
   * @param expr the input array column
   * @param initialValue the initial value
   * @param merge (combined_value, input_value) => combined_value, the merge function to merge
   *              an input value to the combined_value
   * @group collection_funcs
   * @since 3.0.0
   */
  def aggregate(expr: Column, initialValue: Column, merge: (Column, Column) => Column): Column =
    aggregate(expr, initialValue, merge, c => c)

  /**
   * Merge two given arrays, element-wise, into a single array using a function.
   * If one array is shorter, nulls are appended at the end to match the length of the longer
   * array, before applying the function.
   * {{{
   *   df.select(zip_with(df1("val1"), df1("val2"), (x, y) => x + y))
   * }}}
   *
   * @param left the left input array column
   * @param right the right input array column
   * @param f (lCol, rCol) => col, the lambda function to merge two input columns into one column
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def zip_with(left: Column, right: Column, f: (Column, Column) => Column): Column = withExpr {
    ZipWith(left.expr, right.expr, createLambda(f))
  }

  /**
   * Applies a function to every key-value pair in a map and returns
   * a map with the results of those applications as the new keys for the pairs.
   * {{{
   *   df.select(transform_keys(col("i"), (k, v) => k + v))
   * }}}
   *
   * @param expr the input map column
   * @param f (key, value) => new_key, the lambda function to transform the key of input map column
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def transform_keys(expr: Column, f: (Column, Column) => Column): Column = withExpr {
    TransformKeys(expr.expr, createLambda(f))
  }

  /**
   * Applies a function to every key-value pair in a map and returns
   * a map with the results of those applications as the new values for the pairs.
   * {{{
   *   df.select(transform_values(col("i"), (k, v) => k + v))
   * }}}
   *
   * @param expr the input map column
   * @param f (key, value) => new_value, the lambda function to transform the value of input map
   *          column
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def transform_values(expr: Column, f: (Column, Column) => Column): Column = withExpr {
    TransformValues(expr.expr, createLambda(f))
  }

  /**
   * Returns a map whose key-value pairs satisfy a predicate.
   * {{{
   *   df.select(map_filter(col("m"), (k, v) => k * 10 === v))
   * }}}
   *
   * @param expr the input map column
   * @param f (key, value) => predicate, the Boolean predicate to filter the input map column
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def map_filter(expr: Column, f: (Column, Column) => Column): Column = withExpr {
    MapFilter(expr.expr, createLambda(f))
  }

  /**
   * Merge two given maps, key-wise into a single map using a function.
   * {{{
   *   df.select(map_zip_with(df("m1"), df("m2"), (k, v1, v2) => k === v1 + v2))
   * }}}
   *
   * @param left the left input map column
   * @param right the right input map column
   * @param f (key, value1, value2) => new_value, the lambda function to merge the map values
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def map_zip_with(
      left: Column,
      right: Column,
      f: (Column, Column, Column) => Column): Column = withExpr {
    MapZipWith(left.expr, right.expr, createLambda(f))
  }

  /**
   * Creates a new row for each element in the given array or map column.
   * Uses the default column name `col` for elements in the array and
   * `key` and `value` for elements in the map unless specified otherwise.
   *
   * @group collection_funcs
   * @since 1.3.0
   */
  def explode(e: Column): Column = withExpr { Explode(e.expr) }

  /**
   * Creates a new row for each element in the given array or map column.
   * Uses the default column name `col` for elements in the array and
   * `key` and `value` for elements in the map unless specified otherwise.
   * Unlike explode, if the array/map is null or empty then null is produced.
   *
   * @group collection_funcs
   * @since 2.2.0
   */
  def explode_outer(e: Column): Column = withExpr { GeneratorOuter(Explode(e.expr)) }

  /**
   * Creates a new row for each element with position in the given array or map column.
   * Uses the default column name `pos` for position, and `col` for elements in the array
   * and `key` and `value` for elements in the map unless specified otherwise.
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  def posexplode(e: Column): Column = withExpr { PosExplode(e.expr) }

  /**
   * Creates a new row for each element with position in the given array or map column.
   * Uses the default column name `pos` for position, and `col` for elements in the array
   * and `key` and `value` for elements in the map unless specified otherwise.
   * Unlike posexplode, if the array/map is null or empty then the row (null, null) is produced.
   *
   * @group collection_funcs
   * @since 2.2.0
   */
  def posexplode_outer(e: Column): Column = withExpr { GeneratorOuter(PosExplode(e.expr)) }

  /**
   * Extracts json object from a json string based on json path specified, and returns json string
   * of the extracted json object. It will return null if the input json string is invalid.
   *
   * @group collection_funcs
   * @since 1.6.0
   */
  def get_json_object(e: Column, path: String): Column = withExpr {
    GetJsonObject(e.expr, lit(path).expr)
  }

  /**
   * Creates a new row for a json column according to the given field names.
   *
   * @group collection_funcs
   * @since 1.6.0
   */
  @scala.annotation.varargs
  def json_tuple(json: Column, fields: String*): Column = withExpr {
    require(fields.nonEmpty, "at least 1 field name should be given.")
    JsonTuple(json.expr +: fields.map(Literal.apply))
  }

  // scalastyle:off line.size.limit
  /**
   * (Scala-specific) Parses a column containing a JSON string into a `StructType` with the
   * specified schema. Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing JSON data.
   * @param schema the schema to use when parsing the json string
   * @param options options to control how the json is parsed. Accepts the same options as the
   *                json data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: StructType, options: Map[String, String]): Column =
    from_json(e, schema.asInstanceOf[DataType], options)

  // scalastyle:off line.size.limit
  /**
   * (Scala-specific) Parses a column containing a JSON string into a `MapType` with `StringType`
   * as keys type, `StructType` or `ArrayType` with the specified schema.
   * Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing JSON data.
   * @param schema the schema to use when parsing the json string
   * @param options options to control how the json is parsed. accepts the same options and the
   *                json data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *
   * @group collection_funcs
   * @since 2.2.0
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: DataType, options: Map[String, String]): Column = withExpr {
    JsonToStructs(CharVarcharUtils.failIfHasCharVarchar(schema), options, e.expr)
  }

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a JSON string into a `StructType` with the
   * specified schema. Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing JSON data.
   * @param schema the schema to use when parsing the json string
   * @param options options to control how the json is parsed. accepts the same options and the
   *                json data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: StructType, options: java.util.Map[String, String]): Column =
    from_json(e, schema, options.asScala.toMap)

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a JSON string into a `MapType` with `StringType`
   * as keys type, `StructType` or `ArrayType` with the specified schema.
   * Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing JSON data.
   * @param schema the schema to use when parsing the json string
   * @param options options to control how the json is parsed. accepts the same options and the
   *                json data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *
   * @group collection_funcs
   * @since 2.2.0
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: DataType, options: java.util.Map[String, String]): Column = {
    from_json(e, CharVarcharUtils.failIfHasCharVarchar(schema), options.asScala.toMap)
  }

  /**
   * Parses a column containing a JSON string into a `StructType` with the specified schema.
   * Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing JSON data.
   * @param schema the schema to use when parsing the json string
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  def from_json(e: Column, schema: StructType): Column =
    from_json(e, schema, Map.empty[String, String])

  /**
   * Parses a column containing a JSON string into a `MapType` with `StringType` as keys type,
   * `StructType` or `ArrayType` with the specified schema.
   * Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing JSON data.
   * @param schema the schema to use when parsing the json string
   *
   * @group collection_funcs
   * @since 2.2.0
   */
  def from_json(e: Column, schema: DataType): Column =
    from_json(e, schema, Map.empty[String, String])

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a JSON string into a `MapType` with `StringType`
   * as keys type, `StructType` or `ArrayType` with the specified schema.
   * Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing JSON data.
   * @param schema the schema as a DDL-formatted string.
   * @param options options to control how the json is parsed. accepts the same options and the
   *                json data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: String, options: java.util.Map[String, String]): Column = {
    from_json(e, schema, options.asScala.toMap)
  }

  // scalastyle:off line.size.limit
  /**
   * (Scala-specific) Parses a column containing a JSON string into a `MapType` with `StringType`
   * as keys type, `StructType` or `ArrayType` with the specified schema.
   * Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing JSON data.
   * @param schema the schema as a DDL-formatted string.
   * @param options options to control how the json is parsed. accepts the same options and the
   *                json data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *
   * @group collection_funcs
   * @since 2.3.0
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: String, options: Map[String, String]): Column = {
    val dataType = parseTypeWithFallback(
      schema,
      DataType.fromJson,
      "Cannot parse the schema in JSON format: ",
      fallbackParser = DataType.fromDDL)
    from_json(e, dataType, options)
  }

  /**
   * (Scala-specific) Parses a column containing a JSON string into a `MapType` with `StringType`
   * as keys type, `StructType` or `ArrayType` of `StructType`s with the specified schema.
   * Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing JSON data.
   * @param schema the schema to use when parsing the json string
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def from_json(e: Column, schema: Column): Column = {
    from_json(e, schema, Map.empty[String, String].asJava)
  }

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a JSON string into a `MapType` with `StringType`
   * as keys type, `StructType` or `ArrayType` of `StructType`s with the specified schema.
   * Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing JSON data.
   * @param schema the schema to use when parsing the json string
   * @param options options to control how the json is parsed. accepts the same options and the
   *                json data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: Column, options: java.util.Map[String, String]): Column = {
    withExpr(new JsonToStructs(e.expr, schema.expr, options.asScala.toMap))
  }

  /**
   * Parses a JSON string and infers its schema in DDL format.
   *
   * @param json a JSON string.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def schema_of_json(json: String): Column = schema_of_json(lit(json))

  /**
   * Parses a JSON string and infers its schema in DDL format.
   *
   * @param json a foldable string column containing a JSON string.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def schema_of_json(json: Column): Column = withExpr(new SchemaOfJson(json.expr))

  // scalastyle:off line.size.limit
  /**
   * Parses a JSON string and infers its schema in DDL format using options.
   *
   * @param json a foldable string column containing JSON data.
   * @param options options to control how the json is parsed. accepts the same options and the
   *                json data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   * @return a column with string literal containing schema in DDL format.
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  // scalastyle:on line.size.limit
  def schema_of_json(json: Column, options: java.util.Map[String, String]): Column = {
    withExpr(SchemaOfJson(json.expr, options.asScala.toMap))
  }

  // scalastyle:off line.size.limit
  /**
   * (Scala-specific) Converts a column containing a `StructType`, `ArrayType` or
   * a `MapType` into a JSON string with the specified schema.
   * Throws an exception, in the case of an unsupported type.
   *
   * @param e a column containing a struct, an array or a map.
   * @param options options to control how the struct column is converted into a json string.
   *                accepts the same options and the json data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *                Additionally the function supports the `pretty` option which enables
   *                pretty JSON generation.
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  // scalastyle:on line.size.limit
  def to_json(e: Column, options: Map[String, String]): Column = withExpr {
    StructsToJson(options, e.expr)
  }

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Converts a column containing a `StructType`, `ArrayType` or
   * a `MapType` into a JSON string with the specified schema.
   * Throws an exception, in the case of an unsupported type.
   *
   * @param e a column containing a struct, an array or a map.
   * @param options options to control how the struct column is converted into a json string.
   *                accepts the same options and the json data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *                Additionally the function supports the `pretty` option which enables
   *                pretty JSON generation.
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  // scalastyle:on line.size.limit
  def to_json(e: Column, options: java.util.Map[String, String]): Column =
    to_json(e, options.asScala.toMap)

  /**
   * Converts a column containing a `StructType`, `ArrayType` or
   * a `MapType` into a JSON string with the specified schema.
   * Throws an exception, in the case of an unsupported type.
   *
   * @param e a column containing a struct, an array or a map.
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  def to_json(e: Column): Column =
    to_json(e, Map.empty[String, String])

  /**
   * Returns length of array or map.
   *
   * The function returns null for null input if spark.sql.legacy.sizeOfNull is set to false or
   * spark.sql.ansi.enabled is set to true. Otherwise, the function returns -1 for null input.
   * With the default settings, the function returns -1 for null input.
   *
   * @group collection_funcs
   * @since 1.5.0
   */
  def size(e: Column): Column = withExpr { Size(e.expr) }

  /**
   * Sorts the input array for the given column in ascending order,
   * according to the natural ordering of the array elements.
   * Null elements will be placed at the beginning of the returned array.
   *
   * @group collection_funcs
   * @since 1.5.0
   */
  def sort_array(e: Column): Column = sort_array(e, asc = true)

  /**
   * Sorts the input array for the given column in ascending or descending order,
   * according to the natural ordering of the array elements. NaN is greater than any non-NaN
   * elements for double/float type. Null elements will be placed at the beginning of the returned
   * array in ascending order or
   * at the end of the returned array in descending order.
   *
   * @group collection_funcs
   * @since 1.5.0
   */
  def sort_array(e: Column, asc: Boolean): Column = withExpr { SortArray(e.expr, lit(asc).expr) }

  /**
   * Returns the minimum value in the array. NaN is greater than any non-NaN elements for
   * double/float type. NULL elements are skipped.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_min(e: Column): Column = withExpr { ArrayMin(e.expr) }

  /**
   * Returns the maximum value in the array. NaN is greater than any non-NaN elements for
   * double/float type. NULL elements are skipped.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_max(e: Column): Column = withExpr { ArrayMax(e.expr) }

  /**
   * Returns a random permutation of the given array.
   *
   * @note The function is non-deterministic.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def shuffle(e: Column): Column = withExpr { Shuffle(e.expr) }

  /**
   * Returns a reversed string or an array with reverse order of elements.
   * @group collection_funcs
   * @since 1.5.0
   */
  def reverse(e: Column): Column = withExpr { Reverse(e.expr) }

  /**
   * Creates a single array from an array of arrays. If a structure of nested arrays is deeper than
   * two levels, only one level of nesting is removed.
   * @group collection_funcs
   * @since 2.4.0
   */
  def flatten(e: Column): Column = withExpr { Flatten(e.expr) }

  /**
   * Generate a sequence of integers from start to stop, incrementing by step.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def sequence(start: Column, stop: Column, step: Column): Column = withExpr {
    new Sequence(start.expr, stop.expr, step.expr)
  }

  /**
   * Generate a sequence of integers from start to stop,
   * incrementing by 1 if start is less than or equal to stop, otherwise -1.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def sequence(start: Column, stop: Column): Column = withExpr {
    new Sequence(start.expr, stop.expr)
  }

  /**
   * Creates an array containing the left argument repeated the number of times given by the
   * right argument.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_repeat(left: Column, right: Column): Column = withExpr {
    ArrayRepeat(left.expr, right.expr)
  }

  /**
   * Creates an array containing the left argument repeated the number of times given by the
   * right argument.
   *
   * @group collection_funcs
   * @since 2.4.0
   */
  def array_repeat(e: Column, count: Int): Column = array_repeat(e, lit(count))

  /**
   * Returns true if the map contains the key.
   * @group collection_funcs
   * @since 3.3.0
   */
  def map_contains_key(column: Column, key: Any): Column = withExpr {
    ArrayContains(MapKeys(column.expr), lit(key).expr)
  }

  /**
   * Returns an unordered array containing the keys of the map.
   * @group collection_funcs
   * @since 2.3.0
   */
  def map_keys(e: Column): Column = withExpr { MapKeys(e.expr) }

  /**
   * Returns an unordered array containing the values of the map.
   * @group collection_funcs
   * @since 2.3.0
   */
  def map_values(e: Column): Column = withExpr { MapValues(e.expr) }

  /**
   * Returns an unordered array of all entries in the given map.
   * @group collection_funcs
   * @since 3.0.0
   */
  def map_entries(e: Column): Column = withExpr { MapEntries(e.expr) }

  /**
   * Returns a map created from the given array of entries.
   * @group collection_funcs
   * @since 2.4.0
   */
  def map_from_entries(e: Column): Column = withExpr { MapFromEntries(e.expr) }

  /**
   * Returns a merged array of structs in which the N-th struct contains all N-th values of input
   * arrays.
   * @group collection_funcs
   * @since 2.4.0
   */
  @scala.annotation.varargs
  def arrays_zip(e: Column*): Column = withExpr { ArraysZip(e.map(_.expr)) }

  /**
   * Returns the union of all the given maps.
   * @group collection_funcs
   * @since 2.4.0
   */
  @scala.annotation.varargs
  def map_concat(cols: Column*): Column = withExpr { MapConcat(cols.map(_.expr)) }

  // scalastyle:off line.size.limit
  /**
   * Parses a column containing a CSV string into a `StructType` with the specified schema.
   * Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing CSV data.
   * @param schema the schema to use when parsing the CSV string
   * @param options options to control how the CSV is parsed. accepts the same options and the
   *                CSV data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  // scalastyle:on line.size.limit
  def from_csv(e: Column, schema: StructType, options: Map[String, String]): Column = withExpr {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
    CsvToStructs(replaced, options, e.expr)
  }

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a CSV string into a `StructType`
   * with the specified schema. Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing CSV data.
   * @param schema the schema to use when parsing the CSV string
   * @param options options to control how the CSV is parsed. accepts the same options and the
   *                CSV data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  // scalastyle:on line.size.limit
  def from_csv(e: Column, schema: Column, options: java.util.Map[String, String]): Column = {
    withExpr(new CsvToStructs(e.expr, schema.expr, options.asScala.toMap))
  }

  /**
   * Parses a CSV string and infers its schema in DDL format.
   *
   * @param csv a CSV string.
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def schema_of_csv(csv: String): Column = schema_of_csv(lit(csv))

  /**
   * Parses a CSV string and infers its schema in DDL format.
   *
   * @param csv a foldable string column containing a CSV string.
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def schema_of_csv(csv: Column): Column = withExpr(new SchemaOfCsv(csv.expr))

  // scalastyle:off line.size.limit
  /**
   * Parses a CSV string and infers its schema in DDL format using options.
   *
   * @param csv a foldable string column containing a CSV string.
   * @param options options to control how the CSV is parsed. accepts the same options and the
   *                CSV data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   * @return a column with string literal containing schema in DDL format.
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  // scalastyle:on line.size.limit
  def schema_of_csv(csv: Column, options: java.util.Map[String, String]): Column = {
    withExpr(SchemaOfCsv(csv.expr, options.asScala.toMap))
  }

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Converts a column containing a `StructType` into a CSV string with
   * the specified schema. Throws an exception, in the case of an unsupported type.
   *
   * @param e a column containing a struct.
   * @param options options to control how the struct column is converted into a CSV string.
   *                It accepts the same options and the CSV data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  // scalastyle:on line.size.limit
  def to_csv(e: Column, options: java.util.Map[String, String]): Column = withExpr {
    StructsToCsv(options.asScala.toMap, e.expr)
  }

  /**
   * Converts a column containing a `StructType` into a CSV string with the specified schema.
   * Throws an exception, in the case of an unsupported type.
   *
   * @param e a column containing a struct.
   *
   * @group collection_funcs
   * @since 3.0.0
   */
  def to_csv(e: Column): Column = to_csv(e, Map.empty[String, String].asJava)

  /**
   * A transform for timestamps and dates to partition data into years.
   *
   * @group partition_transforms
   * @since 3.0.0
   */
  def years(e: Column): Column = withExpr { Years(e.expr) }

  /**
   * A transform for timestamps and dates to partition data into months.
   *
   * @group partition_transforms
   * @since 3.0.0
   */
  def months(e: Column): Column = withExpr { Months(e.expr) }

  /**
   * A transform for timestamps and dates to partition data into days.
   *
   * @group partition_transforms
   * @since 3.0.0
   */
  def days(e: Column): Column = withExpr { Days(e.expr) }

  /**
   * A transform for timestamps to partition data into hours.
   *
   * @group partition_transforms
   * @since 3.0.0
   */
  def hours(e: Column): Column = withExpr { Hours(e.expr) }

  /**
   * A transform for any type that partitions by a hash of the input column.
   *
   * @group partition_transforms
   * @since 3.0.0
   */
  def bucket(numBuckets: Column, e: Column): Column = withExpr {
    numBuckets.expr match {
      case lit @ Literal(_, IntegerType) =>
        Bucket(lit, e.expr)
      case _ =>
        throw QueryCompilationErrors.invalidBucketsNumberError(numBuckets.toString, e.toString)
    }
  }

  /**
   * A transform for any type that partitions by a hash of the input column.
   *
   * @group partition_transforms
   * @since 3.0.0
   */
  def bucket(numBuckets: Int, e: Column): Column = withExpr {
    Bucket(Literal(numBuckets), e.expr)
  }

  // scalastyle:off line.size.limit
  // scalastyle:off parameter.number

  /* Use the following code to generate:

  (0 to 10).foreach { x =>
    val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
    val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
    val inputEncoders = (1 to x).foldRight("Nil")((i, s) => {s"Try(ExpressionEncoder[A$i]()).toOption :: $s"})
    println(s"""
      |/**
      | * Defines a Scala closure of $x arguments as user-defined function (UDF).
      | * The data types are automatically inferred based on the Scala closure's
      | * signature. By default the returned UDF is deterministic. To change it to
      | * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
      | *
      | * @group udf_funcs
      | * @since 1.3.0
      | */
      |def udf[$typeTags](f: Function$x[$types]): UserDefinedFunction = {
      |  val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
      |  val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(UDFRegistration.outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
      |  val inputEncoders = $inputEncoders
      |  val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder)
      |  if (nullable) udf else udf.asNonNullable()
      |}""".stripMargin)
  }

  (0 to 10).foreach { i =>
    val extTypeArgs = (0 to i).map(_ => "_").mkString(", ")
    val anyTypeArgs = (0 to i).map(_ => "Any").mkString(", ")
    val anyCast = s".asInstanceOf[UDF$i[$anyTypeArgs]]"
    val anyParams = (1 to i).map(_ => "_: Any").mkString(", ")
    val funcCall = if (i == 0) s"() => f$anyCast.call($anyParams)" else s"f$anyCast.call($anyParams)"
    println(s"""
      |/**
      | * Defines a Java UDF$i instance as user-defined function (UDF).
      | * The caller must specify the output data type, and there is no automatic input type coercion.
      | * By default the returned UDF is deterministic. To change it to nondeterministic, call the
      | * API `UserDefinedFunction.asNondeterministic()`.
      | *
      | * @group udf_funcs
      | * @since 2.3.0
      | */
      |def udf(f: UDF$i[$extTypeArgs], returnType: DataType): UserDefinedFunction = {
      |  val func = $funcCall
      |  SparkUserDefinedFunction(func, returnType, inputEncoders = Seq.fill($i)(None))
      |}""".stripMargin)
  }

  */

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Scala UDF functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Obtains a `UserDefinedFunction` that wraps the given `Aggregator`
   * so that it may be used with untyped Data Frames.
   * {{{
   *   val agg = // Aggregator[IN, BUF, OUT]
   *
   *   // declare a UDF based on agg
   *   val aggUDF = udaf(agg)
   *   val aggData = df.agg(aggUDF($"colname"))
   *
   *   // register agg as a named function
   *   spark.udf.register("myAggName", udaf(agg))
   * }}}
   *
   * @tparam IN the aggregator input type
   * @tparam BUF the aggregating buffer type
   * @tparam OUT the finalized output type
   *
   * @param agg the typed Aggregator
   *
   * @return a UserDefinedFunction that can be used as an aggregating expression.
   *
   * @note The input encoder is inferred from the input type IN.
   */
  def udaf[IN: TypeTag, BUF, OUT](agg: Aggregator[IN, BUF, OUT]): UserDefinedFunction = {
    udaf(agg, ExpressionEncoder[IN]())
  }

  /**
   * Obtains a `UserDefinedFunction` that wraps the given `Aggregator`
   * so that it may be used with untyped Data Frames.
   * {{{
   *   Aggregator<IN, BUF, OUT> agg = // custom Aggregator
   *   Encoder<IN> enc = // input encoder
   *
   *   // declare a UDF based on agg
   *   UserDefinedFunction aggUDF = udaf(agg, enc)
   *   DataFrame aggData = df.agg(aggUDF($"colname"))
   *
   *   // register agg as a named function
   *   spark.udf.register("myAggName", udaf(agg, enc))
   * }}}
   *
   * @tparam IN the aggregator input type
   * @tparam BUF the aggregating buffer type
   * @tparam OUT the finalized output type
   *
   * @param agg the typed Aggregator
   * @param inputEncoder a specific input encoder to use
   *
   * @return a UserDefinedFunction that can be used as an aggregating expression
   *
   * @note This overloading takes an explicit input encoder, to support UDAF
   * declarations in Java.
   */
  def udaf[IN, BUF, OUT](
      agg: Aggregator[IN, BUF, OUT],
      inputEncoder: Encoder[IN]): UserDefinedFunction = {
    UserDefinedAggregator(agg, inputEncoder)
  }

  /**
   * Defines a Scala closure of 0 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the Scala closure's
   * signature. By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag](f: Function0[RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(UDFRegistration.outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders = Nil
    val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder)
    if (nullable) udf else udf.asNonNullable()
  }

  /**
   * Defines a Scala closure of 1 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the Scala closure's
   * signature. By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag](f: Function1[A1, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(UDFRegistration.outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders = Try(ExpressionEncoder[A1]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder)
    if (nullable) udf else udf.asNonNullable()
  }

  /**
   * Defines a Scala closure of 2 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the Scala closure's
   * signature. By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag](f: Function2[A1, A2, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(UDFRegistration.outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder)
    if (nullable) udf else udf.asNonNullable()
  }

  /**
   * Defines a Scala closure of 3 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the Scala closure's
   * signature. By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](f: Function3[A1, A2, A3, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(UDFRegistration.outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder)
    if (nullable) udf else udf.asNonNullable()
  }

  /**
   * Defines a Scala closure of 4 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the Scala closure's
   * signature. By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](f: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(UDFRegistration.outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder)
    if (nullable) udf else udf.asNonNullable()
  }

  /**
   * Defines a Scala closure of 5 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the Scala closure's
   * signature. By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](f: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(UDFRegistration.outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder)
    if (nullable) udf else udf.asNonNullable()
  }

  /**
   * Defines a Scala closure of 6 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the Scala closure's
   * signature. By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag](f: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(UDFRegistration.outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder)
    if (nullable) udf else udf.asNonNullable()
  }

  /**
   * Defines a Scala closure of 7 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the Scala closure's
   * signature. By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag](f: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(UDFRegistration.outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder)
    if (nullable) udf else udf.asNonNullable()
  }

  /**
   * Defines a Scala closure of 8 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the Scala closure's
   * signature. By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag](f: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(UDFRegistration.outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder)
    if (nullable) udf else udf.asNonNullable()
  }

  /**
   * Defines a Scala closure of 9 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the Scala closure's
   * signature. By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag](f: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(UDFRegistration.outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder)
    if (nullable) udf else udf.asNonNullable()
  }

  /**
   * Defines a Scala closure of 10 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the Scala closure's
   * signature. By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag](f: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(UDFRegistration.outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder)
    if (nullable) udf else udf.asNonNullable()
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Java UDF functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Defines a Java UDF0 instance as user-defined function (UDF).
   * The caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF0[_], returnType: DataType): UserDefinedFunction = {
    val func = () => f.asInstanceOf[UDF0[Any]].call()
    SparkUserDefinedFunction(func, returnType, inputEncoders = Seq.fill(0)(None))
  }

  /**
   * Defines a Java UDF1 instance as user-defined function (UDF).
   * The caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF1[_, _], returnType: DataType): UserDefinedFunction = {
    val func = f.asInstanceOf[UDF1[Any, Any]].call(_: Any)
    SparkUserDefinedFunction(func, returnType, inputEncoders = Seq.fill(1)(None))
  }

  /**
   * Defines a Java UDF2 instance as user-defined function (UDF).
   * The caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF2[_, _, _], returnType: DataType): UserDefinedFunction = {
    val func = f.asInstanceOf[UDF2[Any, Any, Any]].call(_: Any, _: Any)
    SparkUserDefinedFunction(func, returnType, inputEncoders = Seq.fill(2)(None))
  }

  /**
   * Defines a Java UDF3 instance as user-defined function (UDF).
   * The caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF3[_, _, _, _], returnType: DataType): UserDefinedFunction = {
    val func = f.asInstanceOf[UDF3[Any, Any, Any, Any]].call(_: Any, _: Any, _: Any)
    SparkUserDefinedFunction(func, returnType, inputEncoders = Seq.fill(3)(None))
  }

  /**
   * Defines a Java UDF4 instance as user-defined function (UDF).
   * The caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF4[_, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    val func = f.asInstanceOf[UDF4[Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any)
    SparkUserDefinedFunction(func, returnType, inputEncoders = Seq.fill(4)(None))
  }

  /**
   * Defines a Java UDF5 instance as user-defined function (UDF).
   * The caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF5[_, _, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    val func = f.asInstanceOf[UDF5[Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any)
    SparkUserDefinedFunction(func, returnType, inputEncoders = Seq.fill(5)(None))
  }

  /**
   * Defines a Java UDF6 instance as user-defined function (UDF).
   * The caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF6[_, _, _, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    val func = f.asInstanceOf[UDF6[Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    SparkUserDefinedFunction(func, returnType, inputEncoders = Seq.fill(6)(None))
  }

  /**
   * Defines a Java UDF7 instance as user-defined function (UDF).
   * The caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF7[_, _, _, _, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    val func = f.asInstanceOf[UDF7[Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    SparkUserDefinedFunction(func, returnType, inputEncoders = Seq.fill(7)(None))
  }

  /**
   * Defines a Java UDF8 instance as user-defined function (UDF).
   * The caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF8[_, _, _, _, _, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    val func = f.asInstanceOf[UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    SparkUserDefinedFunction(func, returnType, inputEncoders = Seq.fill(8)(None))
  }

  /**
   * Defines a Java UDF9 instance as user-defined function (UDF).
   * The caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF9[_, _, _, _, _, _, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    val func = f.asInstanceOf[UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    SparkUserDefinedFunction(func, returnType, inputEncoders = Seq.fill(9)(None))
  }

  /**
   * Defines a Java UDF10 instance as user-defined function (UDF).
   * The caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF10[_, _, _, _, _, _, _, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    val func = f.asInstanceOf[UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    SparkUserDefinedFunction(func, returnType, inputEncoders = Seq.fill(10)(None))
  }

  // scalastyle:on parameter.number
  // scalastyle:on line.size.limit

  /**
   * Defines a deterministic user-defined function (UDF) using a Scala closure. For this variant,
   * the caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   *
   * Note that, although the Scala closure can have primitive-type function argument, it doesn't
   * work well with null values. Because the Scala closure is passed in as Any type, there is no
   * type information for the function arguments. Without the type information, Spark may blindly
   * pass null to the Scala closure with primitive-type argument, and the closure will see the
   * default value of the Java type for the null argument, e.g. `udf((x: Int) => x, IntegerType)`,
   * the result is 0 for null input.
   *
   * @param f  A closure in Scala
   * @param dataType  The output data type of the UDF
   *
   * @group udf_funcs
   * @since 2.0.0
   */
  @deprecated("Scala `udf` method with return type parameter is deprecated. " +
    "Please use Scala `udf` method without return type parameter.", "3.0.0")
  def udf(f: AnyRef, dataType: DataType): UserDefinedFunction = {
    if (!SQLConf.get.getConf(SQLConf.LEGACY_ALLOW_UNTYPED_SCALA_UDF)) {
      throw QueryCompilationErrors.usingUntypedScalaUDFError()
    }
    SparkUserDefinedFunction(f, dataType, inputEncoders = Nil)
  }

  /**
   * Call an user-defined function.
   *
   * @group udf_funcs
   * @since 1.5.0
   */
  @scala.annotation.varargs
  @deprecated("Use call_udf")
  def callUDF(udfName: String, cols: Column*): Column =
    call_udf(udfName, cols: _*)

  /**
   * Call an user-defined function.
   * Example:
   * {{{
   *  import org.apache.spark.sql._
   *
   *  val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
   *  val spark = df.sparkSession
   *  spark.udf.register("simpleUDF", (v: Int) => v * v)
   *  df.select($"id", call_udf("simpleUDF", $"value"))
   * }}}
   *
   * @group udf_funcs
   * @since 3.2.0
   */
  @scala.annotation.varargs
  def call_udf(udfName: String, cols: Column*): Column = withExpr {
    UnresolvedFunction(udfName, cols.map(_.expr), isDistinct = false)
  }

  /**
   * Unwrap UDT data type column into its underlying type.
   *
   * @since 3.4.0
   */
  def unwrap_udt(column: Column): Column = withExpr {
    UnwrapUDT(column.expr)
  }
}
