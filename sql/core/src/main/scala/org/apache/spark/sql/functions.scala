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
import scala.language.implicitConversions
import scala.reflect.runtime.universe.{typeTag, TypeTag}
import scala.util.Try

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{Star, UnresolvedFunction}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.BroadcastHint
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * :: Experimental ::
 * Functions available for [[DataFrame]].
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
 * @groupname Ungrouped Support functions for DataFrames
 * @since 1.3.0
 */
@Experimental
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
  def lit(literal: Any): Column = {
    literal match {
      case c: Column => return c
      case s: Symbol => return new ColumnName(literal.asInstanceOf[Symbol].name)
      case _ =>  // continue
    }

    val literalExpr = Literal(literal)
    Column(literalExpr)
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
   *   df.sort(asc_nulls_last("dept"), desc("age"))
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
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def approxCountDistinct(e: Column): Column = withAggregateFunction {
    HyperLogLogPlusPlus(e.expr)
  }

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def approxCountDistinct(columnName: String): Column = approxCountDistinct(column(columnName))

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @param rsd maximum estimation error allowed (default = 0.05)
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def approxCountDistinct(e: Column, rsd: Double): Column = withAggregateFunction {
    HyperLogLogPlusPlus(e.expr, rsd, 0, 0)
  }

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @param rsd maximum estimation error allowed (default = 0.05)
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def approxCountDistinct(columnName: String, rsd: Double): Column = {
    approxCountDistinct(Column(columnName), rsd)
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
   * @group agg_funcs
   * @since 1.6.0
   */
  def collect_list(e: Column): Column = withAggregateFunction { CollectList(e.expr) }

  /**
   * Aggregate function: returns a list of objects with duplicates.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def collect_list(columnName: String): Column = collect_list(Column(columnName))

  /**
   * Aggregate function: returns a set of objects with duplicate elements eliminated.
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def collect_set(e: Column): Column = withAggregateFunction { CollectSet(e.expr) }

  /**
   * Aggregate function: returns a set of objects with duplicate elements eliminated.
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
   * @group agg_funcs
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def countDistinct(expr: Column, exprs: Column*): Column = {
    withAggregateFunction(Count.apply((expr +: exprs).map(_.expr)), isDistinct = true)
  }

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def countDistinct(columnName: String, columnNames: String*): Column =
    countDistinct(Column(columnName), columnNames.map(Column.apply) : _*)

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
   * @group agg_funcs
   * @since 2.0.0
   */
  def first(e: Column, ignoreNulls: Boolean): Column = withAggregateFunction {
    new First(e.expr, Literal(ignoreNulls))
  }

  /**
   * Aggregate function: returns the first value of a column in a group.
   *
   * The function by default returns the first values it sees. It will return the first non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
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
   *   (grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)
   *
   * Note: the list of columns should match with grouping columns exactly, or empty (means all the
   * grouping columns).
   *
   * @group agg_funcs
   * @since 2.0.0
   */
  def grouping_id(cols: Column*): Column = Column(GroupingID(cols.map(_.expr)))

  /**
   * Aggregate function: returns the level of grouping, equals to
   *
   *   (grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)
   *
   * Note: the list of columns should match with grouping columns exactly.
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
   * @group agg_funcs
   * @since 2.0.0
   */
  def last(e: Column, ignoreNulls: Boolean): Column = withAggregateFunction {
    new Last(e.expr, Literal(ignoreNulls))
  }

  /**
   * Aggregate function: returns the last value of the column in a group.
   *
   * The function by default returns the last values it sees. It will return the last non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
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
   * @group agg_funcs
   * @since 1.3.0
   */
  def last(columnName: String): Column = last(Column(columnName), ignoreNulls = false)

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
   * Aggregate function: alias for [[stddev_samp]].
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def stddev(e: Column): Column = withAggregateFunction { StddevSamp(e.expr) }

  /**
   * Aggregate function: alias for [[stddev_samp]].
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
  def sumDistinct(e: Column): Column = withAggregateFunction(Sum(e.expr), isDistinct = true)

  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def sumDistinct(columnName: String): Column = sumDistinct(Column(columnName))

  /**
   * Aggregate function: alias for [[var_samp]].
   *
   * @group agg_funcs
   * @since 1.6.0
   */
  def variance(e: Column): Column = withAggregateFunction { VarianceSamp(e.expr) }

  /**
   * Aggregate function: alias for [[var_samp]].
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
   * The difference between rank and denseRank is that denseRank leaves no gaps in ranking
   * sequence when there are ties. That is, if you were ranking a competition using denseRank
   * and had three people tie for second place, you would say that all three were in second
   * place and that the next person came in third.
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
  def lag(e: Column, offset: Int, defaultValue: Any): Column = withExpr {
    Lag(e.expr, Literal(offset), Literal(defaultValue))
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
  def lead(e: Column, offset: Int, defaultValue: Any): Column = withExpr {
    Lead(e.expr, Literal(offset), Literal(defaultValue))
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
   * The difference between rank and denseRank is that denseRank leaves no gaps in ranking
   * sequence when there are ties. That is, if you were ranking a competition using denseRank
   * and had three people tie for second place, you would say that all three were in second
   * place and that the next person came in third.
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
   * Computes the absolute value.
   *
   * @group normal_funcs
   * @since 1.3.0
   */
  def abs(e: Column): Column = withExpr { Abs(e.expr) }

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
    Dataset[T](df.sparkSession, BroadcastHint(df.logicalPlan))(df.exprEnc)
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
   * As an example, consider a [[DataFrame]] with two partitions, each with 3 records.
   * This expression would return the following IDs:
   * 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
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
   * As an example, consider a [[DataFrame]] with two partitions, each with 3 records.
   * This expression would return the following IDs:
   * 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
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
   * Generate a random column with i.i.d. samples from U[0.0, 1.0].
   *
   * Note that this is indeterministic when data partitions are not fixed.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def rand(seed: Long): Column = withExpr { Rand(seed) }

  /**
   * Generate a random column with i.i.d. samples from U[0.0, 1.0].
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def rand(): Column = rand(Utils.random.nextLong)

  /**
   * Generate a column with i.i.d. samples from the standard normal distribution.
   *
   * Note that this is indeterministic when data partitions are not fixed.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def randn(seed: Long): Column = withExpr { Randn(seed) }

  /**
   * Generate a column with i.i.d. samples from the standard normal distribution.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def randn(): Column = randn(Utils.random.nextLong)

  /**
   * Partition ID of the Spark task.
   *
   * Note that this is indeterministic because it depends on data partitioning and task scheduling.
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
   * If the input column is a column in a [[DataFrame]], or a derived column expression
   * that is named (i.e. aliased), its name would be remained as the StructField's name,
   * otherwise, the newly generated StructField's name would be auto generated as col${index + 1},
   * i.e. col1, col2, col3, ...
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def struct(cols: Column*): Column = withExpr { CreateStruct(cols.map(_.expr)) }

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
   * Computes bitwise NOT.
   *
   * @group normal_funcs
   * @since 1.4.0
   */
  def bitwiseNOT(e: Column): Column = withExpr { BitwiseNot(e.expr) }

  /**
   * Parses the expression string into the column that it represents, similar to
   * DataFrame.selectExpr
   * {{{
   *   // get the number of words of each length
   *   df.groupBy(expr("length(word)")).count()
   * }}}
   *
   * @group normal_funcs
   */
  def expr(expr: String): Column = {
    val parser = SparkSession.getActiveSession.map(_.sessionState.sqlParser).getOrElse {
      new SparkSqlParser(new SQLConf)
    }
    Column(parser.parseExpression(expr))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Math Functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Computes the cosine inverse of the given value; the returned angle is in the range
   * 0.0 through pi.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def acos(e: Column): Column = withExpr { Acos(e.expr) }

  /**
   * Computes the cosine inverse of the given column; the returned angle is in the range
   * 0.0 through pi.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def acos(columnName: String): Column = acos(Column(columnName))

  /**
   * Computes the sine inverse of the given value; the returned angle is in the range
   * -pi/2 through pi/2.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def asin(e: Column): Column = withExpr { Asin(e.expr) }

  /**
   * Computes the sine inverse of the given column; the returned angle is in the range
   * -pi/2 through pi/2.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def asin(columnName: String): Column = asin(Column(columnName))

  /**
   * Computes the tangent inverse of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan(e: Column): Column = withExpr { Atan(e.expr) }

  /**
   * Computes the tangent inverse of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan(columnName: String): Column = atan(Column(columnName))

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(l: Column, r: Column): Column = withExpr { Atan2(l.expr, r.expr) }

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(l: Column, rightName: String): Column = atan2(l, Column(rightName))

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(leftName: String, r: Column): Column = atan2(Column(leftName), r)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(leftName: String, rightName: String): Column =
    atan2(Column(leftName), Column(rightName))

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(l: Column, r: Double): Column = atan2(l, lit(r))

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(leftName: String, r: Double): Column = atan2(Column(leftName), r)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(l: Double, r: Column): Column = atan2(lit(l), r)

  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(l: Double, rightName: String): Column = atan2(l, Column(rightName))

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
   * Computes the ceiling of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def ceil(e: Column): Column = withExpr { Ceil(e.expr) }

  /**
   * Computes the ceiling of the given column.
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
   * Computes the cosine of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cos(e: Column): Column = withExpr { Cos(e.expr) }

  /**
   * Computes the cosine of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cos(columnName: String): Column = cos(Column(columnName))

  /**
   * Computes the hyperbolic cosine of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cosh(e: Column): Column = withExpr { Cosh(e.expr) }

  /**
   * Computes the hyperbolic cosine of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cosh(columnName: String): Column = cosh(Column(columnName))

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
   * Computes the exponential of the given column.
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
   * Computes the floor of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def floor(e: Column): Column = withExpr { Floor(e.expr) }

  /**
   * Computes the floor of the given column.
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
  def greatest(exprs: Column*): Column = withExpr {
    require(exprs.length > 1, "greatest requires at least 2 arguments.")
    Greatest(exprs.map(_.expr))
  }

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
  def least(exprs: Column*): Column = withExpr {
    require(exprs.length > 1, "least requires at least 2 arguments.")
    Least(exprs.map(_.expr))
  }

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
   * Returns the value of the column `e` rounded to 0 decimal places.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def round(e: Column): Column = round(e, 0)

  /**
   * Round the value of `e` to `scale` decimal places if `scale` >= 0
   * or at integral part when `scale` < 0.
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
   * if `scale` >= 0 or at integral part when `scale` < 0.
   *
   * @group math_funcs
   * @since 2.0.0
   */
  def bround(e: Column, scale: Int): Column = withExpr { BRound(e.expr, Literal(scale)) }

  /**
   * Shift the given value numBits left. If the given value is a long value, this function
   * will return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def shiftLeft(e: Column, numBits: Int): Column = withExpr { ShiftLeft(e.expr, lit(numBits).expr) }

  /**
   * Shift the given value numBits right. If the given value is a long value, it will return
   * a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def shiftRight(e: Column, numBits: Int): Column = withExpr {
    ShiftRight(e.expr, lit(numBits).expr)
  }

  /**
   * Unsigned shift the given value numBits right. If the given value is a long value,
   * it will return a long value else it will return an integer value.
   *
   * @group math_funcs
   * @since 1.5.0
   */
  def shiftRightUnsigned(e: Column, numBits: Int): Column = withExpr {
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
   * Computes the sine of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sin(e: Column): Column = withExpr { Sin(e.expr) }

  /**
   * Computes the sine of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sin(columnName: String): Column = sin(Column(columnName))

  /**
   * Computes the hyperbolic sine of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sinh(e: Column): Column = withExpr { Sinh(e.expr) }

  /**
   * Computes the hyperbolic sine of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sinh(columnName: String): Column = sinh(Column(columnName))

  /**
   * Computes the tangent of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tan(e: Column): Column = withExpr { Tan(e.expr) }

  /**
   * Computes the tangent of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tan(columnName: String): Column = tan(Column(columnName))

  /**
   * Computes the hyperbolic tangent of the given value.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tanh(e: Column): Column = withExpr { Tanh(e.expr) }

  /**
   * Computes the hyperbolic tangent of the given column.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tanh(columnName: String): Column = tanh(Column(columnName))

  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def toDegrees(e: Column): Column = withExpr { ToDegrees(e.expr) }

  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def toDegrees(columnName: String): Column = toDegrees(Column(columnName))

  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def toRadians(e: Column): Column = withExpr { ToRadians(e.expr) }

  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def toRadians(columnName: String): Column = toRadians(Column(columnName))

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
   * @since 2.0
   */
  @scala.annotation.varargs
  def hash(cols: Column*): Column = withExpr {
    new Murmur3Hash(cols.map(_.expr))
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
   * Concatenates multiple input string columns together into a single string column.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def concat(exprs: Column*): Column = withExpr { Concat(exprs.map(_.expr)) }

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
    Decode(value.expr, lit(charset).expr)
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
   * Formats numeric column x to a format like '#,###,###.##', rounded to d decimal places,
   * and returns the result as a string column.
   *
   * If d is 0, the result has no decimal point or fractional part.
   * If d < 0, the result will be null.
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
   * NOTE: The position is not zero based, but 1 based index, returns 0 if substr
   * could not be found in str.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def instr(str: Column, substring: String): Column = withExpr {
    StringInstr(str.expr, lit(substring).expr)
  }

  /**
   * Computes the length of a given string or binary column.
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
   * NOTE: The position is not zero based, but 1 based index, returns 0 if substr
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
   * NOTE: The position is not zero based, but 1 based index. returns 0 if substr
   * could not be found in str.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def locate(substr: String, str: Column, pos: Int): Column = withExpr {
    StringLocate(lit(substr).expr, str.expr, lit(pos).expr)
  }

  /**
   * Left-pad the string column with
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def lpad(str: Column, len: Int, pad: String): Column = withExpr {
    StringLPad(str.expr, lit(len).expr, lit(pad).expr)
  }

  /**
   * Trim the spaces from left end for the specified string value.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def ltrim(e: Column): Column = withExpr {StringTrimLeft(e.expr) }

  /**
   * Extract a specific group matched by a Java regex, from the specified string column.
   * If the regex did not match, or the specified group did not match, an empty string is returned.
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
   * Right-padded with pad to a length of len.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def rpad(str: Column, len: Int, pad: String): Column = withExpr {
    StringRPad(str.expr, lit(len).expr, lit(pad).expr)
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
   * Reverses the string column and returns it as a new string column.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def reverse(str: Column): Column = withExpr { StringReverse(str.expr) }

  /**
   * Trim the spaces from right end for the specified string value.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def rtrim(e: Column): Column = withExpr { StringTrimRight(e.expr) }

  /**
   * * Return the soundex code for the specified expression.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def soundex(e: Column): Column = withExpr { SoundEx(e.expr) }

  /**
   * Splits str around pattern (pattern is a regular expression).
   * NOTE: pattern is a string representation of the regular expression.
   *
   * @group string_funcs
   * @since 1.5.0
   */
  def split(str: Column, pattern: String): Column = withExpr {
    StringSplit(str.expr, lit(pattern).expr)
  }

  /**
   * Substring starts at `pos` and is of length `len` when str is String type or
   * returns the slice of byte array that starts at `pos` in byte and is of length `len`
   * when str is Binary type
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
   * Returns the date that is numMonths after startDate.
   *
   * @group datetime_funcs
   * @since 1.5.0
   */
  def add_months(startDate: Column, numMonths: Int): Column = withExpr {
    AddMonths(startDate.expr, Literal(numMonths))
  }

  /**
   * Returns the current date as a date column.
   *
   * @group datetime_funcs
   * @since 1.5.0
   */
  def current_date(): Column = withExpr { CurrentDate() }

  /**
   * Returns the current timestamp as a timestamp column.
   *
   * @group datetime_funcs
   * @since 1.5.0
   */
  def current_timestamp(): Column = withExpr { CurrentTimestamp() }

  /**
   * Converts a date/timestamp/string to a value of string in the format specified by the date
   * format given by the second argument.
   *
   * A pattern could be for instance `dd.MM.yyyy` and could return a string like '18.03.1993'. All
   * pattern letters of [[java.text.SimpleDateFormat]] can be used.
   *
   * NOTE: Use when ever possible specialized functions like [[year]]. These benefit from a
   * specialized implementation.
   *
   * @group datetime_funcs
   * @since 1.5.0
   */
  def date_format(dateExpr: Column, format: String): Column = withExpr {
    DateFormatClass(dateExpr.expr, Literal(format))
  }

  /**
   * Returns the date that is `days` days after `start`
   * @group datetime_funcs
   * @since 1.5.0
   */
  def date_add(start: Column, days: Int): Column = withExpr { DateAdd(start.expr, Literal(days)) }

  /**
   * Returns the date that is `days` days before `start`
   * @group datetime_funcs
   * @since 1.5.0
   */
  def date_sub(start: Column, days: Int): Column = withExpr { DateSub(start.expr, Literal(days)) }

  /**
   * Returns the number of days from `start` to `end`.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def datediff(end: Column, start: Column): Column = withExpr { DateDiff(end.expr, start.expr) }

  /**
   * Extracts the year as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def year(e: Column): Column = withExpr { Year(e.expr) }

  /**
   * Extracts the quarter as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def quarter(e: Column): Column = withExpr { Quarter(e.expr) }

  /**
   * Extracts the month as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def month(e: Column): Column = withExpr { Month(e.expr) }

  /**
   * Extracts the day of the month as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def dayofmonth(e: Column): Column = withExpr { DayOfMonth(e.expr) }

  /**
   * Extracts the day of the year as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def dayofyear(e: Column): Column = withExpr { DayOfYear(e.expr) }

  /**
   * Extracts the hours as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def hour(e: Column): Column = withExpr { Hour(e.expr) }

  /**
   * Given a date column, returns the last day of the month which the given date belongs to.
   * For example, input "2015-07-27" returns "2015-07-31" since July 31 is the last day of the
   * month in July 2015.
   *
   * @group datetime_funcs
   * @since 1.5.0
   */
  def last_day(e: Column): Column = withExpr { LastDay(e.expr) }

  /**
   * Extracts the minutes as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def minute(e: Column): Column = withExpr { Minute(e.expr) }

  /**
   * Returns number of months between dates `date1` and `date2`.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def months_between(date1: Column, date2: Column): Column = withExpr {
    MonthsBetween(date1.expr, date2.expr)
  }

  /**
   * Given a date column, returns the first date which is later than the value of the date column
   * that is on the specified day of the week.
   *
   * For example, `next_day('2015-07-27', "Sunday")` returns 2015-08-02 because that is the first
   * Sunday after 2015-07-27.
   *
   * Day of the week parameter is case insensitive, and accepts:
   * "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun".
   *
   * @group datetime_funcs
   * @since 1.5.0
   */
  def next_day(date: Column, dayOfWeek: String): Column = withExpr {
    NextDay(date.expr, lit(dayOfWeek).expr)
  }

  /**
   * Extracts the seconds as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def second(e: Column): Column = withExpr { Second(e.expr) }

  /**
   * Extracts the week number as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def weekofyear(e: Column): Column = withExpr { WeekOfYear(e.expr) }

  /**
   * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
   * representing the timestamp of that moment in the current system time zone in the given
   * format.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def from_unixtime(ut: Column): Column = withExpr {
    FromUnixTime(ut.expr, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  /**
   * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
   * representing the timestamp of that moment in the current system time zone in the given
   * format.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def from_unixtime(ut: Column, f: String): Column = withExpr {
    FromUnixTime(ut.expr, Literal(f))
  }

  /**
   * Gets current Unix timestamp in seconds.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def unix_timestamp(): Column = withExpr {
    UnixTimestamp(CurrentTimestamp(), Literal("yyyy-MM-dd HH:mm:ss"))
  }

  /**
   * Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds),
   * using the default timezone and the default locale, return null if fail.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def unix_timestamp(s: Column): Column = withExpr {
    UnixTimestamp(s.expr, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  /**
   * Convert time string with given pattern
   * (see [http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html])
   * to Unix time stamp (in seconds), return null if fail.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def unix_timestamp(s: Column, p: String): Column = withExpr {UnixTimestamp(s.expr, Literal(p)) }

  /**
   * Converts the column into DateType.
   *
   * @group datetime_funcs
   * @since 1.5.0
   */
  def to_date(e: Column): Column = withExpr { ToDate(e.expr) }

  /**
   * Returns date truncated to the unit specified by the format.
   *
   * @param format: 'year', 'yyyy', 'yy' for truncate by year,
   *               or 'month', 'mon', 'mm' for truncate by month
   *
   * @group datetime_funcs
   * @since 1.5.0
   */
  def trunc(date: Column, format: String): Column = withExpr {
    TruncDate(date.expr, Literal(format))
  }

  /**
   * Assumes given timestamp is UTC and converts to given timezone.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def from_utc_timestamp(ts: Column, tz: String): Column = withExpr {
    FromUTCTimestamp(ts.expr, Literal(tz))
  }

  /**
   * Assumes given timestamp is in given timezone and converts to UTC.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def to_utc_timestamp(ts: Column, tz: String): Column = withExpr {
    ToUTCTimestamp(ts.expr, Literal(tz))
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
   *   df.groupBy(window($"time", "1 minute", "10 seconds", "5 seconds"), $"stockId")
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
   *                   The time column must be of TimestampType.
   * @param windowDuration A string specifying the width of the window, e.g. `10 minutes`,
   *                       `1 second`. Check [[org.apache.spark.unsafe.types.CalendarInterval]] for
   *                       valid duration identifiers. Note that the duration is a fixed length of
   *                       time, and does not vary over time according to a calendar. For example,
   *                       `1 day` always means 86,400,000 milliseconds, not a calendar day.
   * @param slideDuration A string specifying the sliding interval of the window, e.g. `1 minute`.
   *                      A new window will be generated every `slideDuration`. Must be less than
   *                      or equal to the `windowDuration`. Check
   *                      [[org.apache.spark.unsafe.types.CalendarInterval]] for valid duration
   *                      identifiers. This duration is likewise absolute, and does not vary
    *                     according to a calendar.
   * @param startTime The offset with respect to 1970-01-01 00:00:00 UTC with which to start
   *                  window intervals. For example, in order to have hourly tumbling windows that
   *                  start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide
   *                  `startTime` as `15 minutes`.
   *
   * @group datetime_funcs
   * @since 2.0.0
   */
  @Experimental
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
   *   df.groupBy(window($"time", "1 minute", "10 seconds"), $"stockId")
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
   *                   The time column must be of TimestampType.
   * @param windowDuration A string specifying the width of the window, e.g. `10 minutes`,
   *                       `1 second`. Check [[org.apache.spark.unsafe.types.CalendarInterval]] for
   *                       valid duration identifiers. Note that the duration is a fixed length of
   *                       time, and does not vary over time according to a calendar. For example,
   *                       `1 day` always means 86,400,000 milliseconds, not a calendar day.
   * @param slideDuration A string specifying the sliding interval of the window, e.g. `1 minute`.
   *                      A new window will be generated every `slideDuration`. Must be less than
   *                      or equal to the `windowDuration`. Check
   *                      [[org.apache.spark.unsafe.types.CalendarInterval]] for valid duration
   *                      identifiers. This duration is likewise absolute, and does not vary
   *                     according to a calendar.
   *
   * @group datetime_funcs
   * @since 2.0.0
   */
  @Experimental
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
   *   df.groupBy(window($"time", "1 minute"), $"stockId")
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
   *                   The time column must be of TimestampType.
   * @param windowDuration A string specifying the width of the window, e.g. `10 minutes`,
   *                       `1 second`. Check [[org.apache.spark.unsafe.types.CalendarInterval]] for
   *                       valid duration identifiers.
   *
   * @group datetime_funcs
   * @since 2.0.0
   */
  @Experimental
  def window(timeColumn: Column, windowDuration: String): Column = {
    window(timeColumn, windowDuration, windowDuration, "0 second")
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Collection functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns true if the array contains `value`
   * @group collection_funcs
   * @since 1.5.0
   */
  def array_contains(column: Column, value: Any): Column = withExpr {
    ArrayContains(column.expr, Literal(value))
  }

  /**
   * Creates a new row for each element in the given array or map column.
   *
   * @group collection_funcs
   * @since 1.3.0
   */
  def explode(e: Column): Column = withExpr { Explode(e.expr) }

  /**
   * Creates a new row for each element with position in the given array or map column.
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  def posexplode(e: Column): Column = withExpr { PosExplode(e.expr) }

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

  /**
   * (Scala-specific) Parses a column containing a JSON string into a [[StructType]] with the
   * specified schema. Returns `null`, in the case of an unparseable string.
   *
   * @param schema the schema to use when parsing the json string
   * @param options options to control how the json is parsed. accepts the same options and the
   *                json data source.
   * @param e a string column containing JSON data.
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  def from_json(e: Column, schema: StructType, options: Map[String, String]): Column = withExpr {
    JsonToStruct(schema, options, e.expr)
  }

  /**
   * (Java-specific) Parses a column containing a JSON string into a [[StructType]] with the
   * specified schema. Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing JSON data.
   * @param schema the schema to use when parsing the json string
   * @param options options to control how the json is parsed. accepts the same options and the
   *                json data source.
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  def from_json(e: Column, schema: StructType, options: java.util.Map[String, String]): Column =
    from_json(e, schema, options.asScala.toMap)

  /**
   * Parses a column containing a JSON string into a [[StructType]] with the specified schema.
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
   * Parses a column containing a JSON string into a [[StructType]] with the specified schema.
   * Returns `null`, in the case of an unparseable string.
   *
   * @param e a string column containing JSON data.
   * @param schema the schema to use when parsing the json string as a json string
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  def from_json(e: Column, schema: String, options: java.util.Map[String, String]): Column =
    from_json(e, DataType.fromJson(schema).asInstanceOf[StructType], options)

  /**
   * Returns length of array or map.
   *
   * @group collection_funcs
   * @since 1.5.0
   */
  def size(e: Column): Column = withExpr { Size(e.expr) }

  /**
   * Sorts the input array for the given column in ascending order,
   * according to the natural ordering of the array elements.
   *
   * @group collection_funcs
   * @since 1.5.0
   */
  def sort_array(e: Column): Column = sort_array(e, asc = true)

  /**
   * Sorts the input array for the given column in ascending / descending order,
   * according to the natural ordering of the array elements.
   *
   * @group collection_funcs
   * @since 1.5.0
   */
  def sort_array(e: Column, asc: Boolean): Column = withExpr { SortArray(e.expr, lit(asc).expr) }

  //////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////

  // scalastyle:off line.size.limit
  // scalastyle:off parameter.number

  /* Use the following code to generate:
  (0 to 10).map { x =>
    val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
    val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
    val inputTypes = (1 to x).foldRight("Nil")((i, s) => {s"ScalaReflection.schemaFor(typeTag[A$i]).dataType :: $s"})
    println(s"""
    /**
     * Defines a user-defined function of ${x} arguments as user-defined function (UDF).
     * The data types are automatically inferred based on the function's signature.
     *
     * @group udf_funcs
     * @since 1.3.0
     */
    def udf[$typeTags](f: Function$x[$types]): UserDefinedFunction = {
      val inputTypes = Try($inputTypes).toOption
      UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, inputTypes)
    }""")
  }

  */
  /**
   * Defines a user-defined function of 0 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag](f: Function0[RT]): UserDefinedFunction = {
    val inputTypes = Try(Nil).toOption
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, inputTypes)
  }

  /**
   * Defines a user-defined function of 1 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag](f: Function1[A1, RT]): UserDefinedFunction = {
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: Nil).toOption
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, inputTypes)
  }

  /**
   * Defines a user-defined function of 2 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag](f: Function2[A1, A2, RT]): UserDefinedFunction = {
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: ScalaReflection.schemaFor(typeTag[A2]).dataType :: Nil).toOption
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, inputTypes)
  }

  /**
   * Defines a user-defined function of 3 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](f: Function3[A1, A2, A3, RT]): UserDefinedFunction = {
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: ScalaReflection.schemaFor(typeTag[A2]).dataType :: ScalaReflection.schemaFor(typeTag[A3]).dataType :: Nil).toOption
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, inputTypes)
  }

  /**
   * Defines a user-defined function of 4 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](f: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction = {
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: ScalaReflection.schemaFor(typeTag[A2]).dataType :: ScalaReflection.schemaFor(typeTag[A3]).dataType :: ScalaReflection.schemaFor(typeTag[A4]).dataType :: Nil).toOption
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, inputTypes)
  }

  /**
   * Defines a user-defined function of 5 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](f: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction = {
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: ScalaReflection.schemaFor(typeTag[A2]).dataType :: ScalaReflection.schemaFor(typeTag[A3]).dataType :: ScalaReflection.schemaFor(typeTag[A4]).dataType :: ScalaReflection.schemaFor(typeTag[A5]).dataType :: Nil).toOption
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, inputTypes)
  }

  /**
   * Defines a user-defined function of 6 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag](f: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction = {
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: ScalaReflection.schemaFor(typeTag[A2]).dataType :: ScalaReflection.schemaFor(typeTag[A3]).dataType :: ScalaReflection.schemaFor(typeTag[A4]).dataType :: ScalaReflection.schemaFor(typeTag[A5]).dataType :: ScalaReflection.schemaFor(typeTag[A6]).dataType :: Nil).toOption
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, inputTypes)
  }

  /**
   * Defines a user-defined function of 7 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag](f: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction = {
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: ScalaReflection.schemaFor(typeTag[A2]).dataType :: ScalaReflection.schemaFor(typeTag[A3]).dataType :: ScalaReflection.schemaFor(typeTag[A4]).dataType :: ScalaReflection.schemaFor(typeTag[A5]).dataType :: ScalaReflection.schemaFor(typeTag[A6]).dataType :: ScalaReflection.schemaFor(typeTag[A7]).dataType :: Nil).toOption
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, inputTypes)
  }

  /**
   * Defines a user-defined function of 8 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag](f: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction = {
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: ScalaReflection.schemaFor(typeTag[A2]).dataType :: ScalaReflection.schemaFor(typeTag[A3]).dataType :: ScalaReflection.schemaFor(typeTag[A4]).dataType :: ScalaReflection.schemaFor(typeTag[A5]).dataType :: ScalaReflection.schemaFor(typeTag[A6]).dataType :: ScalaReflection.schemaFor(typeTag[A7]).dataType :: ScalaReflection.schemaFor(typeTag[A8]).dataType :: Nil).toOption
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, inputTypes)
  }

  /**
   * Defines a user-defined function of 9 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag](f: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction = {
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: ScalaReflection.schemaFor(typeTag[A2]).dataType :: ScalaReflection.schemaFor(typeTag[A3]).dataType :: ScalaReflection.schemaFor(typeTag[A4]).dataType :: ScalaReflection.schemaFor(typeTag[A5]).dataType :: ScalaReflection.schemaFor(typeTag[A6]).dataType :: ScalaReflection.schemaFor(typeTag[A7]).dataType :: ScalaReflection.schemaFor(typeTag[A8]).dataType :: ScalaReflection.schemaFor(typeTag[A9]).dataType :: Nil).toOption
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, inputTypes)
  }

  /**
   * Defines a user-defined function of 10 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag](f: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction = {
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: ScalaReflection.schemaFor(typeTag[A2]).dataType :: ScalaReflection.schemaFor(typeTag[A3]).dataType :: ScalaReflection.schemaFor(typeTag[A4]).dataType :: ScalaReflection.schemaFor(typeTag[A5]).dataType :: ScalaReflection.schemaFor(typeTag[A6]).dataType :: ScalaReflection.schemaFor(typeTag[A7]).dataType :: ScalaReflection.schemaFor(typeTag[A8]).dataType :: ScalaReflection.schemaFor(typeTag[A9]).dataType :: ScalaReflection.schemaFor(typeTag[A10]).dataType :: Nil).toOption
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, inputTypes)
  }

  // scalastyle:on parameter.number
  // scalastyle:on line.size.limit

  /**
   * Defines a user-defined function (UDF) using a Scala closure. For this variant, the caller must
   * specify the output data type, and there is no automatic input type coercion.
   *
   * @param f  A closure in Scala
   * @param dataType  The output data type of the UDF
   *
   * @group udf_funcs
   * @since 2.0.0
   */
  def udf(f: AnyRef, dataType: DataType): UserDefinedFunction = {
    UserDefinedFunction(f, dataType, None)
  }

  /**
   * Call an user-defined function.
   * Example:
   * {{{
   *  import org.apache.spark.sql._
   *
   *  val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
   *  val spark = df.sparkSession
   *  spark.udf.register("simpleUDF", (v: Int) => v * v)
   *  df.select($"id", callUDF("simpleUDF", $"value"))
   * }}}
   *
   * @group udf_funcs
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def callUDF(udfName: String, cols: Column*): Column = withExpr {
    UnresolvedFunction(udfName, cols.map(_.expr), isDistinct = false)
  }

}
