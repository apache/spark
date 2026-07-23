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

import java.util.Collections

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.PrimitiveLongEncoder
import org.apache.spark.sql.errors.CompilationErrors
import org.apache.spark.sql.expressions.{Aggregator, SparkUserDefinedFunction, UserDefinedAggregator, UserDefinedFunction}
import org.apache.spark.sql.internal.{SqlApiConf, ToScalaUDF}
import org.apache.spark.sql.types._
import org.apache.spark.util.SparkClassUtils

/**
 * Commonly used functions available for DataFrame operations. Using functions defined here
 * provides a little bit more compile-time safety to make sure the function exists.
 *
 * You can call the functions defined here by two ways: `_FUNC_(...)` and
 * `functions.expr("_FUNC_(...)")`.
 *
 * As an example, `regr_count` is a function that is defined here. You can use
 * `regr_count(col("yCol", col("xCol")))` to invoke the `regr_count` function. This way the
 * programming language's compiler ensures `regr_count` exists and is of the proper form. You can
 * also use `expr("regr_count(yCol, xCol)")` function to invoke the same function. In this case,
 * Spark itself will ensure `regr_count` exists when it analyzes the query.
 *
 * You can find the entire list of functions at SQL API documentation of your Spark version, see
 * also <a href="https://spark.apache.org/docs/latest/api/sql/index.html">the latest list</a>
 *
 * This function APIs usually have methods with `Column` signature only because it can support not
 * only `Column` but also other types such as a native string. The other variants currently exist
 * for historical reasons.
 *
 * @groupname udf_funcs UDF, UDAF and UDT
 * @groupname agg_funcs Aggregate functions
 * @groupname datetime_funcs Date and Timestamp functions
 * @groupname sort_funcs Sort functions
 * @groupname normal_funcs Normal functions
 * @groupname math_funcs Mathematical functions
 * @groupname bitwise_funcs Bitwise functions
 * @groupname predicate_funcs Predicate functions
 * @groupname conditional_funcs Conditional functions
 * @groupname hash_funcs Hash functions
 * @groupname misc_funcs Misc functions
 * @groupname sketch_funcs Datasketch functions
 * @groupname window_funcs Window functions
 * @groupname generator_funcs Generator functions
 * @groupname string_funcs String functions
 * @groupname collection_funcs Collection functions
 * @groupname array_funcs Array functions
 * @groupname map_funcs Map functions
 * @groupname struct_funcs Struct functions
 * @groupname st_funcs ST geospatial functions
 * @groupname csv_funcs CSV functions
 * @groupname json_funcs JSON functions
 * @groupname variant_funcs VARIANT functions
 * @groupname vector_funcs Vector functions
 * @groupname xml_funcs XML functions
 * @groupname url_funcs URL functions
 * @groupname partition_transforms Partition transform functions
 * @groupname Ungrouped Support functions for DataFrames
 * @since 1.3.0
 */
@Stable
// scalastyle:off
object functions {
// scalastyle:on

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
   * The passed in object is returned directly if it is already a [[Column]]. If the object is a
   * Scala Symbol, it is converted into a [[Column]] also. Otherwise, a new [[Column]] is created
   * to represent the literal value.
   *
   * @group normal_funcs
   * @since 1.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lit(literal: Any): Column = {
    literal match {
      case c: Column => c
      case s: Symbol => new ColumnName(s.name)
      case _ =>
        // This is different from `typedlit`. `typedlit` calls `Literal.create` to use
        // `ScalaReflection` to get the type of `literal`. However, since we use `Any` in this
        // method, `typedLit[Any](literal)` will always fail and fallback to `Literal.apply`. Hence,
        // we can just manually call `Literal.apply` to skip the expensive `ScalaReflection` code.
        // This is significantly better when there are many threads calling `lit` concurrently.
        Column(internal.Literal(literal))
    }
  }

  /**
   * Creates a [[Column]] of literal value.
   *
   * An alias of `typedlit`, and it is encouraged to use `typedlit` directly.
   *
   * @group normal_funcs
   * @since 2.2.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def typedLit[T: TypeTag](literal: T): Column = {
    typedlit(literal)
  }

  /**
   * Creates a [[Column]] of literal value.
   *
   * The passed in object is returned directly if it is already a [[Column]]. If the object is a
   * Scala Symbol, it is converted into a [[Column]] also. Otherwise, a new [[Column]] is created
   * to represent the literal value. The difference between this function and [[lit]] is that this
   * function can handle parameterized scala types e.g.: List, Seq and Map.
   *
   * @note
   *   `typedlit` will call expensive Scala reflection APIs. `lit` is preferred if parameterized
   *   Scala types are not used.
   *
   * @group normal_funcs
   * @since 3.2.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def typedlit[T: TypeTag](literal: T): Column = {
    literal match {
      case c: Column => c
      case s: Symbol => new ColumnName(s.name)
      case _ =>
        val dataType = Try(ScalaReflection.schemaFor[T].dataType).toOption
        Column(internal.Literal(literal, dataType))
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
   * @since 1.3.0
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
   * @since 2.1.0
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
   * Returns a sort expression based on the descending order of the column, and null values appear
   * before non-null values.
   * {{{
   *   df.sort(asc("dept"), desc_nulls_first("age"))
   * }}}
   *
   * @group sort_funcs
   * @since 2.1.0
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
   * @since 2.1.0
   */
  def desc_nulls_last(columnName: String): Column = Column(columnName).desc_nulls_last

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Aggregate functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(e: Column): Column = approx_count_distinct(e)

  /**
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(columnName: String): Column = approx_count_distinct(columnName)

  /**
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(e: Column, rsd: Double): Column = approx_count_distinct(e, rsd)

  /**
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(columnName: String, rsd: Double): Column = {
    approx_count_distinct(Column(columnName), rsd)
  }

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @param e
   *   The column to count distinct values in. A column of any type.
   * @group agg_funcs
   * @since 2.1.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def approx_count_distinct(e: Column): Column = Column.fn("approx_count_distinct", e)

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @param columnName
   *   The name of the column to count distinct values in. A column of any type.
   * @group agg_funcs
   * @since 2.1.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def approx_count_distinct(columnName: String): Column = approx_count_distinct(
    column(columnName))

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @param rsd
   *   maximum relative standard deviation allowed (default = 0.05). A column that evaluates to a
   *   double. Must be a constant.
   *
   * @group agg_funcs
   * @since 2.1.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def approx_count_distinct(e: Column, rsd: Double): Column = {
    Column.fn("approx_count_distinct", e, lit(rsd))
  }

  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   *
   * @param rsd
   *   maximum relative standard deviation allowed (default = 0.05). A column that evaluates to a
   *   double. Must be a constant.
   *
   * @group agg_funcs
   * @since 2.1.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def approx_count_distinct(columnName: String, rsd: Double): Column = {
    approx_count_distinct(Column(columnName), rsd)
  }

  /**
   * Aggregate function: returns the average of the values in a group.
   *
   * @param e
   *   The column to average. A column that evaluates to a numeric or interval.
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a numeric.
   */
  def avg(e: Column): Column = Column.fn("avg", e)

  /**
   * Aggregate function: returns the average of the values in a group.
   *
   * @param columnName
   *   The name of the column to average. A column that evaluates to a numeric or interval.
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a numeric.
   */
  def avg(columnName: String): Column = avg(Column(columnName))

  /**
   * Aggregate function: returns a list of objects with duplicates.
   *
   * @param e
   *   The column to collect. A column of any type.
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def collect_list(e: Column): Column = Column.fn("collect_list", e)

  /**
   * Aggregate function: returns a list of objects with duplicates.
   *
   * @param columnName
   *   The name of the column to collect. A column of any type.
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def collect_list(columnName: String): Column = collect_list(Column(columnName))

  /**
   * Aggregate function: returns a set of objects with duplicate elements eliminated.
   *
   * @param e
   *   The column to collect. A column of any type.
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def collect_set(e: Column): Column = Column.fn("collect_set", e)

  /**
   * Aggregate function: returns a set of objects with duplicate elements eliminated.
   *
   * @param columnName
   *   The name of the column to collect. A column of any type.
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def collect_set(columnName: String): Column = collect_set(Column(columnName))

  /**
   * Returns a count-min sketch of a column with the given esp, confidence and seed. The result is
   * an array of bytes, which can be deserialized to a `CountMinSketch` before usage. Count-min
   * sketch is a probabilistic data structure used for cardinality estimation using sub-linear
   * space.
   *
   * @param e
   *   The column to compute the sketch on. A column that evaluates to an integral, string or
   *   binary.
   * @param eps
   *   The relative error, must be positive. A column that evaluates to a numeric. Must be a
   *   constant.
   * @param confidence
   *   The confidence, must be positive and less than 1.0. A column that evaluates to a numeric.
   *   Must be a constant.
   * @param seed
   *   The random seed. A column that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def count_min_sketch(e: Column, eps: Column, confidence: Column, seed: Column): Column =
    Column.fn("count_min_sketch", e, eps, confidence, seed)

  /**
   * Returns a count-min sketch of a column with the given esp, confidence and seed. The result is
   * an array of bytes, which can be deserialized to a `CountMinSketch` before usage. Count-min
   * sketch is a probabilistic data structure used for cardinality estimation using sub-linear
   * space.
   *
   * @param e
   *   The column to compute the sketch on. A column that evaluates to an integral, string or
   *   binary.
   * @param eps
   *   The relative error, must be positive. A column that evaluates to a numeric. Must be a
   *   constant.
   * @param confidence
   *   The confidence, must be positive and less than 1.0. A column that evaluates to a numeric.
   *   Must be a constant.
   * @group agg_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def count_min_sketch(e: Column, eps: Column, confidence: Column): Column =
    count_min_sketch(e, eps, confidence, lit(SparkClassUtils.random.nextLong))

  /**
   * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
   *
   * @param column1
   *   The first column. A column that evaluates to a numeric.
   * @param column2
   *   The second column. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def corr(column1: Column, column2: Column): Column = Column.fn("corr", column1, column2)

  /**
   * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
   *
   * @param columnName1
   *   The name of the first column. A column that evaluates to a numeric.
   * @param columnName2
   *   The name of the second column. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def corr(columnName1: String, columnName2: String): Column = {
    corr(Column(columnName1), Column(columnName2))
  }

  /**
   * Aggregate function: returns the number of items in a group.
   *
   * @param e
   *   The column to count. A column of any type.
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def count(e: Column): Column =
    Column.fn("count", e)

  /**
   * Aggregate function: returns the number of items in a group.
   *
   * @param columnName
   *   The name of the column to count. A column of any type.
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def count(columnName: String): TypedColumn[Any, Long] =
    count(Column(columnName)).as(PrimitiveLongEncoder)

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * An alias of `count_distinct`, and it is encouraged to use `count_distinct` directly.
   *
   * @param expr
   *   The first column. A column of any type.
   * @param exprs
   *   Additional columns. A column of any type.
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  @scala.annotation.varargs
  def countDistinct(expr: Column, exprs: Column*): Column = count_distinct(expr, exprs: _*)

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * An alias of `count_distinct`, and it is encouraged to use `count_distinct` directly.
   *
   * @param columnName
   *   first column to compute on. A column of any type.
   * @param columnNames
   *   additional columns to compute on. Columns of any type.
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  @scala.annotation.varargs
  def countDistinct(columnName: String, columnNames: String*): Column =
    count_distinct(Column(columnName), columnNames.map(Column.apply): _*)

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * @param expr
   *   first column to compute on. A column of any type.
   * @param exprs
   *   additional columns to compute on. Columns of any type.
   * @group agg_funcs
   * @since 3.2.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  @scala.annotation.varargs
  def count_distinct(expr: Column, exprs: Column*): Column =
    Column.fn("count", isDistinct = true, expr +: exprs: _*)

  /**
   * Aggregate function: returns the population covariance for two columns.
   *
   * @param column1
   *   first column to calculate covariance. A column that evaluates to a numeric.
   * @param column2
   *   second column to calculate covariance. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 2.0.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def covar_pop(column1: Column, column2: Column): Column =
    Column.fn("covar_pop", column1, column2)

  /**
   * Aggregate function: returns the population covariance for two columns.
   *
   * @param columnName1
   *   first column to calculate covariance. A column that evaluates to a numeric.
   * @param columnName2
   *   second column to calculate covariance. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 2.0.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def covar_pop(columnName1: String, columnName2: String): Column = {
    covar_pop(Column(columnName1), Column(columnName2))
  }

  /**
   * Aggregate function: returns the sample covariance for two columns.
   *
   * @param column1
   *   first column to calculate covariance. A column that evaluates to a numeric.
   * @param column2
   *   second column to calculate covariance. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 2.0.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def covar_samp(column1: Column, column2: Column): Column =
    Column.fn("covar_samp", column1, column2)

  /**
   * Aggregate function: returns the sample covariance for two columns.
   *
   * @param columnName1
   *   first column to calculate covariance. A column that evaluates to a numeric.
   * @param columnName2
   *   second column to calculate covariance. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 2.0.0
   * @return
   *   Returns a column that evaluates to a double.
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
   * @param e
   *   column to fetch the first value for. A column of any type.
   * @param ignoreNulls
   *   if first value is null then look for first non-null value. A column that evaluates to a
   *   boolean. Must be a constant.
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 2.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def first(e: Column, ignoreNulls: Boolean): Column =
    Column.fn("first", false, e, lit(ignoreNulls))

  /**
   * Aggregate function: returns the first value of a column in a group.
   *
   * The function by default returns the first values it sees. It will return the first non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @param columnName
   *   column to fetch the first value for. A column of any type.
   * @param ignoreNulls
   *   if first value is null then look for first non-null value. A column that evaluates to a
   *   boolean. Must be a constant.
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 2.0.0
   * @return
   *   Returns a column of the same type as the input.
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
   * @param e
   *   column to fetch the first value for. A column of any type.
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def first(e: Column): Column = first(e, ignoreNulls = false)

  /**
   * Aggregate function: returns the first value of a column in a group.
   *
   * The function by default returns the first values it sees. It will return the first non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @param columnName
   *   column to fetch the first value for. A column of any type.
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def first(columnName: String): Column = first(Column(columnName))

  /**
   * Aggregate function: returns the first value in a group.
   *
   * @param e
   *   column to fetch the first value for. A column of any type.
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def first_value(e: Column): Column = Column.fn("first_value", e)

  /**
   * Aggregate function: returns the first value in a group.
   *
   * The function by default returns the first values it sees. It will return the first non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @param e
   *   column to fetch the first value for. A column of any type.
   * @param ignoreNulls
   *   if first value is null then look for first non-null value. A column that evaluates to a
   *   boolean. Must be a constant.
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def first_value(e: Column, ignoreNulls: Column): Column =
    Column.fn("first_value", e, ignoreNulls)

  /**
   * Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated or
   * not, returns 1 for aggregated or 0 for not aggregated in the result set.
   *
   * @param e
   *   column to check if it is aggregated. A column of any type.
   * @group agg_funcs
   * @since 2.0.0
   * @return
   *   Returns a column that evaluates to a byte.
   */
  def grouping(e: Column): Column = Column.fn("grouping", e)

  /**
   * Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated or
   * not, returns 1 for aggregated or 0 for not aggregated in the result set.
   *
   * @param columnName
   *   column to check if it is aggregated. A column of any type.
   * @group agg_funcs
   * @since 2.0.0
   * @return
   *   Returns a column that evaluates to a byte.
   */
  def grouping(columnName: String): Column = grouping(Column(columnName))

  /**
   * Aggregate function: returns the level of grouping, equals to
   *
   * {{{
   *   (grouping(c1) <<; (n-1)) + (grouping(c2) <<; (n-2)) + ... + grouping(cn)
   * }}}
   *
   * @param cols
   *   columns to check for. Columns of any type.
   * @note
   *   The list of columns should match with grouping columns exactly, or empty (means all the
   *   grouping columns).
   *
   * @group agg_funcs
   * @since 2.0.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  @scala.annotation.varargs
  def grouping_id(cols: Column*): Column = Column.fn("grouping_id", cols: _*)

  /**
   * Aggregate function: returns the level of grouping, equals to
   *
   * {{{
   *   (grouping(c1) <<; (n-1)) + (grouping(c2) <<; (n-2)) + ... + grouping(cn)
   * }}}
   *
   * @param colName
   *   the name of the first grouping column. A column of any type.
   * @param colNames
   *   the names of the remaining grouping columns. Columns of any type.
   * @note
   *   The list of columns should match with grouping columns exactly.
   *
   * @group agg_funcs
   * @since 2.0.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  @scala.annotation.varargs
  def grouping_id(colName: String, colNames: String*): Column = {
    grouping_id((Seq(colName) ++ colNames).map(n => Column(n)): _*)
  }

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch
   * configured with lgConfigK arg.
   *
   * @param e
   *   the column to compute the sketch on. A column that evaluates to an integral, a string or a
   *   binary.
   * @param lgConfigK
   *   the log-base-2 of K, where K is the number of buckets or slots for the HllSketch. A column
   *   that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_sketch_agg(e: Column, lgConfigK: Column): Column =
    Column.fn("hll_sketch_agg", e, lgConfigK)

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch
   * configured with lgConfigK arg.
   *
   * @param e
   *   the column to compute the sketch on. A column that evaluates to an integral, a string or a
   *   binary.
   * @param lgConfigK
   *   the log-base-2 of K, where K is the number of buckets or slots for the HllSketch. A column
   *   that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_sketch_agg(e: Column, lgConfigK: Int): Column =
    Column.fn("hll_sketch_agg", e, lit(lgConfigK))

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch
   * configured with lgConfigK arg.
   *
   * @param columnName
   *   the name of the column to compute the sketch on. A column that evaluates to an integral, a
   *   string or a binary.
   * @param lgConfigK
   *   the log-base-2 of K, where K is the number of buckets or slots for the HllSketch. A column
   *   that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_sketch_agg(columnName: String, lgConfigK: Int): Column = {
    hll_sketch_agg(Column(columnName), lgConfigK)
  }

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch
   * configured with default lgConfigK value.
   *
   * @param e
   *   the column to compute the sketch on. A column that evaluates to an integral, a string or a
   *   binary.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_sketch_agg(e: Column): Column =
    Column.fn("hll_sketch_agg", e)

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch
   * configured with default lgConfigK value.
   *
   * @param columnName
   *   the name of the column to compute the sketch on. A column that evaluates to an integral, a
   *   string or a binary.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_sketch_agg(columnName: String): Column = {
    hll_sketch_agg(Column(columnName))
  }

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches
   * HllSketch, generated by merging previously created Datasketches HllSketch instances via a
   * Datasketches Union instance. Throws an exception if sketches have different lgConfigK values
   * and allowDifferentLgConfigK is set to false.
   *
   * @param e
   *   the column containing the HllSketch instances to merge. A column that evaluates to a
   *   binary.
   * @param allowDifferentLgConfigK
   *   allow sketches with different lgConfigK values to be merged. A column that evaluates to a
   *   boolean. Must be a constant.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_union_agg(e: Column, allowDifferentLgConfigK: Column): Column =
    Column.fn("hll_union_agg", e, allowDifferentLgConfigK)

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches
   * HllSketch, generated by merging previously created Datasketches HllSketch instances via a
   * Datasketches Union instance. Throws an exception if sketches have different lgConfigK values
   * and allowDifferentLgConfigK is set to false.
   *
   * @param e
   *   the column containing the HllSketch instances to merge. A column that evaluates to a
   *   binary.
   * @param allowDifferentLgConfigK
   *   allow sketches with different lgConfigK values to be merged. A column that evaluates to a
   *   boolean. Must be a constant.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_union_agg(e: Column, allowDifferentLgConfigK: Boolean): Column =
    Column.fn("hll_union_agg", e, lit(allowDifferentLgConfigK))

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches
   * HllSketch, generated by merging previously created Datasketches HllSketch instances via a
   * Datasketches Union instance. Throws an exception if sketches have different lgConfigK values
   * and allowDifferentLgConfigK is set to false.
   *
   * @param columnName
   *   the name of the column containing the HllSketch instances to merge. A column that evaluates
   *   to a binary.
   * @param allowDifferentLgConfigK
   *   allow sketches with different lgConfigK values to be merged. A column that evaluates to a
   *   boolean. Must be a constant.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_union_agg(columnName: String, allowDifferentLgConfigK: Boolean): Column = {
    hll_union_agg(Column(columnName), allowDifferentLgConfigK)
  }

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches
   * HllSketch, generated by merging previously created Datasketches HllSketch instances via a
   * Datasketches Union instance. Throws an exception if sketches have different lgConfigK values.
   *
   * @param e
   *   the column containing the HllSketch instances to merge. A column that evaluates to a
   *   binary.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_union_agg(e: Column): Column =
    Column.fn("hll_union_agg", e)

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches
   * HllSketch, generated by merging previously created Datasketches HllSketch instances via a
   * Datasketches Union instance. Throws an exception if sketches have different lgConfigK values.
   *
   * @param columnName
   *   the name of the column containing the HllSketch instances to merge. A column that evaluates
   *   to a binary.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_union_agg(columnName: String): Column = {
    hll_union_agg(Column(columnName))
  }

  /**
   * Aggregate function: returns the kurtosis of the values in a group.
   *
   * @param e
   *   the column to compute the kurtosis on. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def kurtosis(e: Column): Column = Column.fn("kurtosis", e)

  /**
   * Aggregate function: returns the kurtosis of the values in a group.
   *
   * @param columnName
   *   the name of the column to compute the kurtosis on. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def kurtosis(columnName: String): Column = kurtosis(Column(columnName))

  /**
   * Aggregate function: returns the last value in a group.
   *
   * The function by default returns the last values it sees. It will return the last non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @param e
   *   the column to take the last value from. A column of any type.
   * @param ignoreNulls
   *   if true, returns the last non-null value; if all values are null, null is returned. A
   *   column that evaluates to a boolean. Must be a constant.
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 2.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def last(e: Column, ignoreNulls: Boolean): Column =
    Column.fn("last", false, e, lit(ignoreNulls))

  /**
   * Aggregate function: returns the last value of the column in a group.
   *
   * The function by default returns the last values it sees. It will return the last non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @param columnName
   *   the name of the column to take the last value from. A column of any type.
   * @param ignoreNulls
   *   if true, returns the last non-null value; if all values are null, null is returned. A
   *   column that evaluates to a boolean. Must be a constant.
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 2.0.0
   * @return
   *   Returns a column of the same type as the input.
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
   * @param e
   *   column to fetch the last value for. A column of any type.
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column of the same type as the input.
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
   * @since 1.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def last(columnName: String): Column = last(Column(columnName), ignoreNulls = false)

  /**
   * Aggregate function: returns the last value in a group.
   *
   * @param e
   *   column to fetch the last value for. A column of any type.
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def last_value(e: Column): Column = Column.fn("last_value", e)

  /**
   * Aggregate function: returns the last value in a group.
   *
   * The function by default returns the last values it sees. It will return the last non-null
   * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
   *
   * @param e
   *   column to fetch the last value for. A column of any type.
   * @param ignoreNulls
   *   whether to skip null values. A column that evaluates to a boolean.
   * @note
   *   The function is non-deterministic because its results depends on the order of the rows
   *   which may be non-deterministic after a shuffle.
   *
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def last_value(e: Column, ignoreNulls: Column): Column =
    Column.fn("last_value", e, ignoreNulls)

  /**
   * Create time from hour, minute and second fields. For invalid inputs it will throw an error.
   *
   * @param hour
   *   the hour to represent, from 0 to 23. A column that evaluates to an integer.
   * @param minute
   *   the minute to represent, from 0 to 59. A column that evaluates to an integer.
   * @param second
   *   the second to represent, from 0 to 59.999999. A column that evaluates to a decimal.
   * @group datetime_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a time.
   */
  def make_time(hour: Column, minute: Column, second: Column): Column = {
    Column.fn("make_time", hour, minute, second)
  }

  /**
   * Aggregate function: returns the most frequent value in a group.
   *
   * @param e
   *   target column to compute on. A column of any type.
   * @group agg_funcs
   * @since 3.4.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def mode(e: Column): Column = Column.fn("mode", e)

  /**
   * Aggregate function: returns the most frequent value in a group.
   *
   * When multiple values have the same greatest frequency then either any of values is returned
   * if deterministic is false or is not defined, or the lowest value is returned if deterministic
   * is true.
   *
   * @param e
   *   target column to compute on. A column of any type.
   * @param deterministic
   *   if there are multiple equally-frequent results then return the lowest. A boolean. Must be a
   *   constant.
   * @group agg_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def mode(e: Column, deterministic: Boolean): Column = Column.fn("mode", e, lit(deterministic))

  /**
   * Aggregate function: returns the maximum value of the expression in a group.
   *
   * @param e
   *   the target column on which the maximum value is computed. A column of any type.
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def max(e: Column): Column = Column.fn("max", e)

  /**
   * Aggregate function: returns the maximum value of the column in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def max(columnName: String): Column = max(Column(columnName))

  /**
   * Aggregate function: returns the value associated with the maximum value of ord.
   *
   * @param e
   *   the column representing the values to be returned. A column of any type.
   * @param ord
   *   the column that needs to be maximized. A column of any orderable type.
   * @note
   *   The function is non-deterministic so the output order can be different for those associated
   *   the same values of `e`.
   *
   * @group agg_funcs
   * @since 3.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def max_by(e: Column, ord: Column): Column = Column.fn("max_by", e, ord)

  /**
   * Aggregate function: returns an array of values associated with the top `k` values of `ord`.
   *
   * The result array contains values in descending order by their associated ordering values.
   * Returns null if there are no non-null ordering values.
   *
   * @param e
   *   the column representing the values to be returned. A column of any type.
   * @param ord
   *   the column that needs to be maximized. A column of any orderable type.
   * @param k
   *   the number of top values to return. An integer. Must be a constant.
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle when there are ties in the
   *   ordering expression.
   * @note
   *   The maximum value of `k` is 100000.
   *
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def max_by(e: Column, ord: Column, k: Int): Column = Column.fn("max_by", e, ord, lit(k))

  /**
   * Aggregate function: returns an array of values associated with the top `k` values of `ord`.
   *
   * The result array contains values in descending order by their associated ordering values.
   * Returns null if there are no non-null ordering values.
   *
   * @param e
   *   the column representing the values to be returned. A column of any type.
   * @param ord
   *   the column that needs to be maximized. A column of any orderable type.
   * @param k
   *   the number of top values to return. A column that evaluates to an integer.
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle when there are ties in the
   *   ordering expression.
   * @note
   *   The maximum value of `k` is 100000.
   *
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def max_by(e: Column, ord: Column, k: Column): Column = Column.fn("max_by", e, ord, k)

  /**
   * Aggregate function: returns the average of the values in a group. Alias for avg.
   *
   * @param e
   *   target column to compute on. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def mean(e: Column): Column = avg(e)

  /**
   * Aggregate function: returns the average of the values in a group. Alias for avg.
   *
   * @group agg_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def mean(columnName: String): Column = avg(columnName)

  /**
   * Aggregate function: returns the median of the values in a group.
   *
   * @param e
   *   target column to compute on. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 3.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def median(e: Column): Column = Column.fn("median", e)

  /**
   * Aggregate function: returns the minimum value of the expression in a group.
   *
   * @param e
   *   the target column on which the minimum value is computed. A column of any type.
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def min(e: Column): Column = Column.fn("min", e)

  /**
   * Aggregate function: returns the minimum value of the column in a group.
   *
   * @param columnName
   *   the name of the column on which the minimum value is computed. A column of an orderable
   *   type.
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def min(columnName: String): Column = min(Column(columnName))

  /**
   * Aggregate function: returns the value associated with the minimum value of ord.
   *
   * @param e
   *   the column representing the values that will be returned. A column of any type.
   * @param ord
   *   the column that needs to be minimized. A column of an orderable type.
   * @note
   *   The function is non-deterministic so the output order can be different for those associated
   *   the same values of `e`.
   *
   * @group agg_funcs
   * @since 3.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def min_by(e: Column, ord: Column): Column = Column.fn("min_by", e, ord)

  /**
   * Aggregate function: returns an array of values associated with the bottom `k` values of
   * `ord`.
   *
   * The result array contains values in ascending order by their associated ordering values.
   * Returns null if there are no non-null ordering values.
   *
   * @param e
   *   the column representing the values that will be returned. A column of any type.
   * @param ord
   *   the column that needs to be minimized. A column of an orderable type.
   * @param k
   *   the number of bottom values to return. An integer. Must be a constant.
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle when there are ties in the
   *   ordering expression.
   * @note
   *   The maximum value of `k` is 100000.
   *
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def min_by(e: Column, ord: Column, k: Int): Column = Column.fn("min_by", e, ord, lit(k))

  /**
   * Aggregate function: returns an array of values associated with the bottom `k` values of
   * `ord`.
   *
   * The result array contains values in ascending order by their associated ordering values.
   * Returns null if there are no non-null ordering values.
   *
   * @param e
   *   the column representing the values that will be returned. A column of any type.
   * @param ord
   *   the column that needs to be minimized. A column of an orderable type.
   * @param k
   *   the number of bottom values to return. A column that evaluates to an integral. Must be a
   *   constant.
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle when there are ties in the
   *   ordering expression.
   * @note
   *   The maximum value of `k` is 100000.
   *
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def min_by(e: Column, ord: Column, k: Column): Column = Column.fn("min_by", e, ord, k)

  /**
   * Aggregate function: returns the exact percentile(s) of numeric column `expr` at the given
   * percentage(s) with value range in [0.0, 1.0].
   *
   * @param e
   *   the column to compute the percentile on. A column that evaluates to a numeric or interval.
   * @param percentage
   *   the percentage in decimal, between 0.0 and 1.0. A column that evaluates to a numeric or an
   *   array. Must be a constant.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def percentile(e: Column, percentage: Column): Column = Column.fn("percentile", e, percentage)

  /**
   * Aggregate function: returns the exact percentile(s) of numeric column `expr` at the given
   * percentage(s) with value range in [0.0, 1.0].
   *
   * @param e
   *   the column to compute the percentile on. A column that evaluates to a numeric or interval.
   * @param percentage
   *   the percentage in decimal, between 0.0 and 1.0. A column that evaluates to a numeric or an
   *   array. Must be a constant.
   * @param frequency
   *   the positive frequency with which to weight each value. A column that evaluates to an
   *   integral.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def percentile(e: Column, percentage: Column, frequency: Column): Column =
    Column.fn("percentile", e, percentage, frequency)

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
   * @param e
   *   the column to compute the approximate percentile on. A column that evaluates to a numeric
   *   or interval.
   * @param percentage
   *   the percentage in decimal, between 0.0 and 1.0. A column that evaluates to a numeric or an
   *   array. Must be a constant.
   * @param accuracy
   *   a positive numeric literal that controls approximation accuracy at the cost of memory. A
   *   column that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 3.1.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def percentile_approx(e: Column, percentage: Column, accuracy: Column): Column =
    Column.fn("percentile_approx", e, percentage, accuracy)

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
   * @param e
   *   the column to compute the approximate percentile on. A column that evaluates to a numeric
   *   or interval.
   * @param percentage
   *   the percentage in decimal, between 0.0 and 1.0. A column that evaluates to a numeric or an
   *   array. Must be a constant.
   * @param accuracy
   *   a positive numeric literal that controls approximation accuracy at the cost of memory. A
   *   column that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def approx_percentile(e: Column, percentage: Column, accuracy: Column): Column = {
    Column.fn("approx_percentile", e, percentage, accuracy)
  }

  /**
   * Aggregate function: returns the product of all numerical elements in a group.
   *
   * @param e
   *   the column to compute the product on. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 3.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def product(e: Column): Column = Column.internalFn("product", e)

  /**
   * Aggregate function: returns the skewness of the values in a group.
   *
   * @param e
   *   the column to compute the skewness on. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def skewness(e: Column): Column = Column.fn("skewness", e)

  /**
   * Aggregate function: returns the skewness of the values in a group.
   *
   * @param columnName
   *   the name of the column to compute the skewness on. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def skewness(columnName: String): Column = skewness(Column(columnName))

  /**
   * Aggregate function: alias for `stddev_samp`.
   *
   * @param e
   *   the column to compute the standard deviation on. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def std(e: Column): Column = Column.fn("std", e)

  /**
   * Aggregate function: alias for `stddev_samp`.
   *
   * @param e
   *   the column to compute the standard deviation on. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def stddev(e: Column): Column = Column.fn("stddev", e)

  /**
   * Aggregate function: alias for `stddev_samp`.
   *
   * @param columnName
   *   the name of the column to compute the standard deviation on. A column that evaluates to a
   *   numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def stddev(columnName: String): Column = stddev(Column(columnName))

  /**
   * Aggregate function: returns the sample standard deviation of the expression in a group.
   *
   * @param e
   *   the column to compute the sample standard deviation on. A column that evaluates to a
   *   numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def stddev_samp(e: Column): Column = Column.fn("stddev_samp", e)

  /**
   * Aggregate function: returns the sample standard deviation of the expression in a group.
   *
   * @param columnName
   *   Name of the column to compute the sample standard deviation on. A column that evaluates to
   *   a numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def stddev_samp(columnName: String): Column = stddev_samp(Column(columnName))

  /**
   * Aggregate function: returns the population standard deviation of the expression in a group.
   *
   * @param e
   *   The column to compute the population standard deviation on. A column that evaluates to a
   *   numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def stddev_pop(e: Column): Column = Column.fn("stddev_pop", e)

  /**
   * Aggregate function: returns the population standard deviation of the expression in a group.
   *
   * @param columnName
   *   Name of the column to compute the population standard deviation on. A column that evaluates
   *   to a numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def stddev_pop(columnName: String): Column = stddev_pop(Column(columnName))

  /**
   * Aggregate function: returns the sum of all values in the expression.
   *
   * @param e
   *   The column to sum. A column that evaluates to a numeric or interval.
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a numeric or interval.
   */
  def sum(e: Column): Column = Column.fn("sum", e)

  /**
   * Aggregate function: returns the sum of all values in the given column.
   *
   * @param columnName
   *   Name of the column to sum. A column that evaluates to a numeric or interval.
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a numeric or interval.
   */
  def sum(columnName: String): Column = sum(Column(columnName))

  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   *
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a numeric or interval.
   */
  @deprecated("Use sum_distinct", "3.2.0")
  def sumDistinct(e: Column): Column = sum_distinct(e)

  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   *
   * @group agg_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a numeric or interval.
   */
  @deprecated("Use sum_distinct", "3.2.0")
  def sumDistinct(columnName: String): Column = sum_distinct(Column(columnName))

  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   *
   * @param e
   *   The column to sum distinct values of. A column that evaluates to a numeric or interval.
   * @group agg_funcs
   * @since 3.2.0
   * @return
   *   Returns a column that evaluates to a numeric or interval.
   */
  def sum_distinct(e: Column): Column = Column.fn("sum", isDistinct = true, e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * ThetaSketch, generated by intersecting the Datasketches ThetaSketch instances in the input
   * column via a Datasketches Intersection instance.
   *
   * @param e
   *   The column of Datasketches ThetaSketch instances to intersect. A column that evaluates to a
   *   binary.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_intersection_agg(e: Column): Column =
    Column.fn("theta_intersection_agg", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * ThetaSketch, generated by intersecting the Datasketches ThetaSketch instances in the input
   * volumn via a Datasketches Intersection instance.
   *
   * @param columnName
   *   Name of the column of Datasketches ThetaSketch instances to intersect. A column that
   *   evaluates to a binary.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_intersection_agg(columnName: String): Column =
    theta_intersection_agg(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch
   * built with the values in the input column and configured with the `lgNomEntries` nominal
   * entries.
   *
   * @param e
   *   The column to build the ThetaSketch from. A column that evaluates to a numeric, string,
   *   binary or array.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries, which is the size of the sketch (must be between 4 and
   *   26). A column that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_sketch_agg(e: Column, lgNomEntries: Column): Column =
    Column.fn("theta_sketch_agg", e, lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch
   * built with the values in the input column and configured with the `lgNomEntries` nominal
   * entries.
   *
   * @param e
   *   The column to build the ThetaSketch from. A column that evaluates to a numeric, string,
   *   binary or array.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries, which is the size of the sketch (must be between 4 and
   *   26). A column that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_sketch_agg(e: Column, lgNomEntries: Int): Column =
    Column.fn("theta_sketch_agg", e, lit(lgNomEntries))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch
   * built with the values in the input column and configured with the `lgNomEntries` nominal
   * entries.
   *
   * @param columnName
   *   Name of the column to build the ThetaSketch from. A column that evaluates to a numeric,
   *   string, binary or array.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries, which is the size of the sketch (must be between 4 and
   *   26). A column that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_sketch_agg(columnName: String, lgNomEntries: Int): Column =
    theta_sketch_agg(Column(columnName), lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch
   * built with the values in the input column and configured with the default value of 12 for
   * `lgNomEntries`.
   *
   * @param e
   *   The column to build the ThetaSketch from. A column that evaluates to a numeric, string,
   *   binary or array.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_sketch_agg(e: Column): Column =
    Column.fn("theta_sketch_agg", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch
   * built with the values in the input column and configured with the default value of 12 for
   * `lgNomEntries`.
   *
   * @param columnName
   *   Name of the column to build the ThetaSketch from. A column that evaluates to a numeric,
   *   string, binary or array.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_sketch_agg(columnName: String): Column =
    theta_sketch_agg(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * ThetaSketch, generated by the union of Datasketches ThetaSketch instances in the input column
   * via a Datasketches Union instance. It allows the configuration of `lgNomEntries` log nominal
   * entries for the union buffer.
   *
   * @param e
   *   The column containing binary ThetaSketch representations. A column that evaluates to a
   *   binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union operation (must be between 4 and 26,
   *   defaults to 12). A column that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_union_agg(e: Column, lgNomEntries: Column): Column =
    Column.fn("theta_union_agg", e, lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * ThetaSketch, generated by the union of Datasketches ThetaSketch instances in the input column
   * via a Datasketches Union instance. It allows the configuration of `lgNomEntries` log nominal
   * entries for the union buffer.
   *
   * @param e
   *   The column containing binary ThetaSketch representations. A column that evaluates to a
   *   binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union operation (must be between 4 and 26,
   *   defaults to 12). A column that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_union_agg(e: Column, lgNomEntries: Int): Column =
    Column.fn("theta_union_agg", e, lit(lgNomEntries))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * ThetaSketch, generated by the union of Datasketches ThetaSketch instances in the input column
   * via a Datasketches Union instance. It allows the configuration of `lgNomEntries` log nominal
   * entries for the union buffer.
   *
   * @param columnName
   *   The name of the column containing binary ThetaSketch representations. A column that
   *   evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union operation (must be between 4 and 26,
   *   defaults to 12). A column that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_union_agg(columnName: String, lgNomEntries: Int): Column =
    theta_union_agg(Column(columnName), lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * ThetaSketch, generated by the union of Datasketches ThetaSketch instances in the input column
   * via a Datasketches Union instance. It is configured with the default value of 12 for
   * `lgNomEntries`.
   *
   * @param e
   *   The column containing binary ThetaSketch representations. A column that evaluates to a
   *   binary.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_union_agg(e: Column): Column =
    Column.fn("theta_union_agg", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * ThetaSketch, generated by the union of Datasketches ThetaSketch instances in the input column
   * via a Datasketches Union instance. It is configured with the default value of 12 for
   * `lgNomEntries`.
   *
   * @param columnName
   *   The name of the column containing binary ThetaSketch representations. A column that
   *   evaluates to a binary.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_union_agg(columnName: String): Column =
    theta_union_agg(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by intersecting the Datasketches TupleSketch instances
   * in the input column via a Datasketches Intersection instance. The mode parameter specifies
   * the aggregation mode for numeric summaries during intersection (sum, min, max, alwaysone).
   *
   * @param e
   *   The column containing binary TupleSketch representations. A column that evaluates to a
   *   binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_agg_double(e: Column, mode: Column): Column =
    Column.fn("tuple_intersection_agg_double", e, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by intersecting the Datasketches TupleSketch instances
   * in the input column via a Datasketches Intersection instance. The mode parameter specifies
   * the aggregation mode for numeric summaries during intersection (sum, min, max, alwaysone).
   *
   * @param e
   *   The column containing binary TupleSketch representations. A column that evaluates to a
   *   binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_agg_double(e: Column, mode: String): Column =
    Column.fn("tuple_intersection_agg_double", e, lit(mode))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by intersecting the Datasketches TupleSketch instances
   * in the input column via a Datasketches Intersection instance. The mode parameter specifies
   * the aggregation mode for numeric summaries during intersection (sum, min, max, alwaysone).
   *
   * @param columnName
   *   The name of the column containing binary TupleSketch representations. A column that
   *   evaluates to a binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_agg_double(columnName: String, mode: String): Column =
    tuple_intersection_agg_double(Column(columnName), mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by intersecting the Datasketches TupleSketch instances
   * in the input column via a Datasketches Intersection instance. It is configured with the
   * default mode of 'sum'.
   *
   * @param e
   *   The column containing binary TupleSketch representations. A column that evaluates to a
   *   binary.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_agg_double(e: Column): Column =
    Column.fn("tuple_intersection_agg_double", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by intersecting the Datasketches TupleSketch instances
   * in the input column via a Datasketches Intersection instance. It is configured with the
   * default mode of 'sum'.
   *
   * @param columnName
   *   The name of the column containing binary TupleSketch representations. A column that
   *   evaluates to a binary.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_agg_double(columnName: String): Column =
    tuple_intersection_agg_double(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by intersecting the Datasketches TupleSketch
   * instances in the input column via a Datasketches Intersection instance. The mode parameter
   * specifies the aggregation mode for numeric summaries during intersection (sum, min, max,
   * alwaysone).
   *
   * @param e
   *   The column containing binary TupleSketch representations. A column that evaluates to a
   *   binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_agg_integer(e: Column, mode: Column): Column =
    Column.fn("tuple_intersection_agg_integer", e, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by intersecting the Datasketches TupleSketch
   * instances in the input column via a Datasketches Intersection instance. The mode parameter
   * specifies the aggregation mode for numeric summaries during intersection (sum, min, max,
   * alwaysone).
   *
   * @param e
   *   The column containing binary TupleSketch representations. A column that evaluates to a
   *   binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_agg_integer(e: Column, mode: String): Column =
    Column.fn("tuple_intersection_agg_integer", e, lit(mode))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by intersecting the Datasketches TupleSketch
   * instances in the input column via a Datasketches Intersection instance. The mode parameter
   * specifies the aggregation mode for numeric summaries during intersection (sum, min, max,
   * alwaysone).
   *
   * @param columnName
   *   The name of the column containing binary TupleSketch representations. A column that
   *   evaluates to a binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_agg_integer(columnName: String, mode: String): Column =
    tuple_intersection_agg_integer(Column(columnName), mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by intersecting the Datasketches TupleSketch
   * instances in the input column via a Datasketches Intersection instance. It is configured with
   * the default mode of 'sum'.
   *
   * @param e
   *   The column containing binary TupleSketch representations. A column that evaluates to a
   *   binary.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_agg_integer(e: Column): Column =
    Column.fn("tuple_intersection_agg_integer", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by intersecting the Datasketches TupleSketch
   * instances in the input column via a Datasketches Intersection instance. It is configured with
   * the default mode of 'sum'.
   *
   * @param columnName
   *   The name of the column containing binary TupleSketch representations. A column that
   *   evaluates to a binary.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_agg_integer(columnName: String): Column =
    tuple_intersection_agg_integer(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries and aggregation mode. The mode parameter
   * specifies the aggregation mode for numeric summaries (sum, min, max, alwaysone).
   *
   * @param key
   *   the key values against which unique counting occurs. A column that evaluates to an array, a
   *   binary, a numeric, or a string.
   * @param summary
   *   the summary values against which mode aggregations occur. A column that evaluates to a
   *   numeric.
   * @param lgNomEntries
   *   the log-base-2 of nominal entries (must be between 4 and 26). A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   the summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   *   Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_double(
      key: Column,
      summary: Column,
      lgNomEntries: Column,
      mode: Column): Column =
    Column.fn("tuple_sketch_agg_double", key, summary, lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries and aggregation mode. The mode parameter
   * specifies the aggregation mode for numeric summaries (sum, min, max, alwaysone).
   *
   * @param key
   *   the key values against which unique counting occurs. A column that evaluates to an array, a
   *   binary, a numeric, or a string.
   * @param summary
   *   the summary values against which mode aggregations occur. A column that evaluates to a
   *   numeric.
   * @param lgNomEntries
   *   the log-base-2 of nominal entries (must be between 4 and 26). A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   the summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   *   Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_double(
      key: Column,
      summary: Column,
      lgNomEntries: Int,
      mode: String): Column =
    Column.fn("tuple_sketch_agg_double", key, summary, lit(lgNomEntries), lit(mode))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries and aggregation mode. The mode parameter
   * specifies the aggregation mode for numeric summaries (sum, min, max, alwaysone).
   *
   * @param keyColumnName
   *   the name of the column containing the key values against which unique counting occurs. A
   *   column that evaluates to an array, a binary, a numeric, or a string.
   * @param summaryColumnName
   *   the name of the column containing the summary values against which mode aggregations occur.
   *   A column that evaluates to a numeric.
   * @param lgNomEntries
   *   the log-base-2 of nominal entries (must be between 4 and 26). A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   the summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   *   Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_double(
      keyColumnName: String,
      summaryColumnName: String,
      lgNomEntries: Int,
      mode: String): Column =
    tuple_sketch_agg_double(Column(keyColumnName), Column(summaryColumnName), lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries. It uses the default mode of 'sum'.
   *
   * @param key
   *   the key values against which unique counting occurs. A column that evaluates to an array, a
   *   binary, a numeric, or a string.
   * @param summary
   *   the summary values against which mode aggregations occur. A column that evaluates to a
   *   numeric.
   * @param lgNomEntries
   *   the log-base-2 of nominal entries (must be between 4 and 26). A column that evaluates to an
   *   integral. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_double(key: Column, summary: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_sketch_agg_double", key, summary, lit(lgNomEntries))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries. It uses the default mode of 'sum'.
   *
   * @param keyColumnName
   *   the name of the column containing the key values against which unique counting occurs. A
   *   column that evaluates to an array, a binary, a numeric, or a string.
   * @param summaryColumnName
   *   the name of the column containing the summary values against which mode aggregations occur.
   *   A column that evaluates to a numeric.
   * @param lgNomEntries
   *   the log-base-2 of nominal entries (must be between 4 and 26). A column that evaluates to an
   *   integral. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_double(
      keyColumnName: String,
      summaryColumnName: String,
      lgNomEntries: Int): Column =
    tuple_sketch_agg_double(Column(keyColumnName), Column(summaryColumnName), lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns. It
   * uses the default values of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param key
   *   the key values against which unique counting occurs. A column that evaluates to an array, a
   *   binary, a numeric, or a string.
   * @param summary
   *   the summary values against which mode aggregations occur. A column that evaluates to a
   *   numeric.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_double(key: Column, summary: Column): Column =
    Column.fn("tuple_sketch_agg_double", key, summary)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns. It
   * uses the default values of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param keyColumnName
   *   the name of the column containing the key values against which unique counting occurs. A
   *   column that evaluates to an array, a binary, a numeric, or a string.
   * @param summaryColumnName
   *   the name of the column containing the summary values against which mode aggregations occur.
   *   A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_double(keyColumnName: String, summaryColumnName: String): Column =
    tuple_sketch_agg_double(Column(keyColumnName), Column(summaryColumnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries and aggregation mode. The mode parameter
   * specifies the aggregation mode for numeric summaries (sum, min, max, alwaysone).
   *
   * @param key
   *   the key values against which unique counting occurs. A column that evaluates to an array, a
   *   binary, a numeric, or a string.
   * @param summary
   *   the summary values against which mode aggregations occur. A column that evaluates to an
   *   integral.
   * @param lgNomEntries
   *   the log-base-2 of nominal entries (must be between 4 and 26). A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   the summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   *   Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_integer(
      key: Column,
      summary: Column,
      lgNomEntries: Column,
      mode: Column): Column =
    Column.fn("tuple_sketch_agg_integer", key, summary, lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries and aggregation mode. The mode parameter
   * specifies the aggregation mode for numeric summaries (sum, min, max, alwaysone).
   *
   * @param key
   *   the key values against which unique counting occurs. A column that evaluates to an array, a
   *   binary, a numeric, or a string.
   * @param summary
   *   the summary values against which mode aggregations occur. A column that evaluates to an
   *   integral.
   * @param lgNomEntries
   *   the log-base-2 of nominal entries (must be between 4 and 26). A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   the summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   *   Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_integer(
      key: Column,
      summary: Column,
      lgNomEntries: Int,
      mode: String): Column =
    Column.fn("tuple_sketch_agg_integer", key, summary, lit(lgNomEntries), lit(mode))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries and aggregation mode. The mode parameter
   * specifies the aggregation mode for numeric summaries (sum, min, max, alwaysone).
   *
   * @param keyColumnName
   *   the name of the column containing the key values against which unique counting occurs. A
   *   column that evaluates to an array, a binary, a numeric, or a string.
   * @param summaryColumnName
   *   the name of the column containing the summary values against which mode aggregations occur.
   *   A column that evaluates to an integral.
   * @param lgNomEntries
   *   the log-base-2 of nominal entries (must be between 4 and 26). A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   the summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   *   Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_integer(
      keyColumnName: String,
      summaryColumnName: String,
      lgNomEntries: Int,
      mode: String): Column =
    tuple_sketch_agg_integer(Column(keyColumnName), Column(summaryColumnName), lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries. It uses the default mode of 'sum'.
   *
   * @param key
   *   the key values against which unique counting occurs. A column that evaluates to an array, a
   *   binary, a numeric, or a string.
   * @param summary
   *   the summary values against which mode aggregations occur. A column that evaluates to an
   *   integral.
   * @param lgNomEntries
   *   the log-base-2 of nominal entries (must be between 4 and 26). A column that evaluates to an
   *   integral. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_integer(key: Column, summary: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_sketch_agg_integer", key, summary, lit(lgNomEntries))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries. It uses the default mode of 'sum'.
   *
   * @param keyColumnName
   *   the name of the column containing the key values against which unique counting occurs. A
   *   column that evaluates to an array, a binary, a numeric, or a string.
   * @param summaryColumnName
   *   the name of the column containing the summary values against which mode aggregations occur.
   *   A column that evaluates to an integral.
   * @param lgNomEntries
   *   the log-base-2 of nominal entries (must be between 4 and 26). A column that evaluates to an
   *   integral. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_integer(
      keyColumnName: String,
      summaryColumnName: String,
      lgNomEntries: Int): Column =
    tuple_sketch_agg_integer(Column(keyColumnName), Column(summaryColumnName), lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns. It
   * uses the default values of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param key
   *   the key values against which unique counting occurs. A column that evaluates to an array, a
   *   binary, a numeric, or a string.
   * @param summary
   *   the summary values against which mode aggregations occur. A column that evaluates to an
   *   integral.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_integer(key: Column, summary: Column): Column =
    Column.fn("tuple_sketch_agg_integer", key, summary)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns. It
   * uses the default values of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param keyColumnName
   *   the name of the column containing the key values against which unique counting occurs. A
   *   column that evaluates to an array, a binary, a numeric, or a string.
   * @param summaryColumnName
   *   the name of the column containing the summary values against which mode aggregations occur.
   *   A column that evaluates to an integral.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_sketch_agg_integer(keyColumnName: String, summaryColumnName: String): Column =
    tuple_sketch_agg_integer(Column(keyColumnName), Column(summaryColumnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It allows the configuration of
   * `lgNomEntries` log nominal entries for the union buffer and the aggregation mode for numeric
   * summaries (sum, min, max, alwaysone).
   *
   * @param e
   *   the column containing binary TupleSketch representations to union. A column that evaluates
   *   to a binary.
   * @param lgNomEntries
   *   the log-base-2 of nominal entries for the union buffer (must be between 4 and 26). A column
   *   that evaluates to an integral. Must be a constant.
   * @param mode
   *   the summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   *   Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_double(e: Column, lgNomEntries: Column, mode: Column): Column =
    Column.fn("tuple_union_agg_double", e, lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It allows the configuration of
   * `lgNomEntries` log nominal entries for the union buffer and the aggregation mode for numeric
   * summaries (sum, min, max, alwaysone).
   *
   * @param e
   *   The input column containing binary TupleSketch representations. A column that evaluates to
   *   a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   The summary mode: one of "sum", "min", "max", or "alwaysone". A column that evaluates to a
   *   string. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_double(e: Column, lgNomEntries: Int, mode: String): Column =
    Column.fn("tuple_union_agg_double", e, lit(lgNomEntries), lit(mode))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It allows the configuration of
   * `lgNomEntries` log nominal entries for the union buffer and the aggregation mode for numeric
   * summaries (sum, min, max, alwaysone).
   *
   * @param columnName
   *   The name of the input column containing binary TupleSketch representations. A column that
   *   evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   The summary mode: one of "sum", "min", "max", or "alwaysone". A column that evaluates to a
   *   string. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_double(columnName: String, lgNomEntries: Int, mode: String): Column =
    tuple_union_agg_double(Column(columnName), lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It allows the configuration of
   * `lgNomEntries` log nominal entries for the union buffer. It uses the default mode of 'sum'.
   *
   * @param e
   *   The input column containing binary TupleSketch representations. A column that evaluates to
   *   a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_double(e: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_union_agg_double", e, lit(lgNomEntries))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It allows the configuration of
   * `lgNomEntries` log nominal entries for the union buffer. It uses the default mode of 'sum'.
   *
   * @param columnName
   *   The name of the input column containing binary TupleSketch representations. A column that
   *   evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_double(columnName: String, lgNomEntries: Int): Column =
    tuple_union_agg_double(Column(columnName), lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It is configured with the default values
   * of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param e
   *   The input column containing binary TupleSketch representations. A column that evaluates to
   *   a binary.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_double(e: Column): Column =
    Column.fn("tuple_union_agg_double", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It is configured with the default values
   * of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param columnName
   *   The name of the input column containing binary TupleSketch representations. A column that
   *   evaluates to a binary.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_double(columnName: String): Column =
    tuple_union_agg_double(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It allows the configuration of
   * `lgNomEntries` log nominal entries for the union buffer and the aggregation mode for numeric
   * summaries (sum, min, max, alwaysone).
   *
   * @param e
   *   The input column containing binary TupleSketch representations. A column that evaluates to
   *   a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   The summary mode: one of "sum", "min", "max", or "alwaysone". A column that evaluates to a
   *   string. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_integer(e: Column, lgNomEntries: Column, mode: Column): Column =
    Column.fn("tuple_union_agg_integer", e, lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It allows the configuration of
   * `lgNomEntries` log nominal entries for the union buffer and the aggregation mode for numeric
   * summaries (sum, min, max, alwaysone).
   *
   * @param e
   *   The input column containing binary TupleSketch representations. A column that evaluates to
   *   a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   The summary mode: one of "sum", "min", "max", or "alwaysone". A column that evaluates to a
   *   string. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_integer(e: Column, lgNomEntries: Int, mode: String): Column =
    Column.fn("tuple_union_agg_integer", e, lit(lgNomEntries), lit(mode))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It allows the configuration of
   * `lgNomEntries` log nominal entries for the union buffer and the aggregation mode for numeric
   * summaries (sum, min, max, alwaysone).
   *
   * @param columnName
   *   The name of the input column containing binary TupleSketch representations. A column that
   *   evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   The summary mode: one of "sum", "min", "max", or "alwaysone". A column that evaluates to a
   *   string. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_integer(columnName: String, lgNomEntries: Int, mode: String): Column =
    tuple_union_agg_integer(Column(columnName), lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It allows the configuration of
   * `lgNomEntries` log nominal entries for the union buffer. It uses the default mode of 'sum'.
   *
   * @param e
   *   The input column containing binary TupleSketch representations. A column that evaluates to
   *   a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_integer(e: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_union_agg_integer", e, lit(lgNomEntries))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It allows the configuration of
   * `lgNomEntries` log nominal entries for the union buffer. It uses the default mode of 'sum'.
   *
   * @param columnName
   *   The name of the input column containing binary TupleSketch representations. A column that
   *   evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_integer(columnName: String, lgNomEntries: Int): Column =
    tuple_union_agg_integer(Column(columnName), lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It is configured with the default values
   * of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param e
   *   The input column containing binary TupleSketch representations. A column that evaluates to
   *   a binary.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_integer(e: Column): Column =
    Column.fn("tuple_union_agg_integer", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance. It is configured with the default values
   * of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param columnName
   *   The name of the input column containing binary TupleSketch representations. A column that
   *   evaluates to a binary.
   * @group agg_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_agg_integer(columnName: String): Column =
    tuple_union_agg_integer(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllLongsSketch built with the values in the input column. The optional k parameter controls
   * the size and accuracy of the sketch (default 200, range 8-65535).
   *
   * @param e
   *   The input column containing the values to aggregate. A column that evaluates to an
   *   integral.
   * @param k
   *   The parameter that controls the size and accuracy of the sketch. A column that evaluates to
   *   an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_bigint(e: Column, k: Column): Column =
    Column.fn("kll_sketch_agg_bigint", e, k)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllLongsSketch built with the values in the input column. The optional k parameter controls
   * the size and accuracy of the sketch (default 200, range 8-65535).
   *
   * @param e
   *   The input column containing the values to aggregate. A column that evaluates to an
   *   integral.
   * @param k
   *   The parameter that controls the size and accuracy of the sketch. A column that evaluates to
   *   an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_bigint(e: Column, k: Int): Column =
    Column.fn("kll_sketch_agg_bigint", e, lit(k))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllLongsSketch built with the values in the input column. The optional k parameter controls
   * the size and accuracy of the sketch (default 200, range 8-65535).
   *
   * @param columnName
   *   The column containing bigint values to aggregate. A column that evaluates to an integral.
   * @param k
   *   The k parameter that controls size and accuracy (default 200, range 8-65535). A column that
   *   evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_bigint(columnName: String, k: Int): Column =
    kll_sketch_agg_bigint(Column(columnName), k)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllLongsSketch built with the values in the input column with default k value of 200.
   *
   * @param e
   *   The column containing bigint values to aggregate. A column that evaluates to an integral.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_bigint(e: Column): Column =
    Column.fn("kll_sketch_agg_bigint", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllLongsSketch built with the values in the input column with default k value of 200.
   *
   * @param columnName
   *   The column containing bigint values to aggregate. A column that evaluates to an integral.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_bigint(columnName: String): Column =
    kll_sketch_agg_bigint(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllFloatsSketch built with the values in the input column. The optional k parameter controls
   * the size and accuracy of the sketch (default 200, range 8-65535).
   *
   * @param e
   *   The column containing float values to aggregate. A column that evaluates to a float.
   * @param k
   *   The k parameter that controls size and accuracy (default 200, range 8-65535). A column that
   *   evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_float(e: Column, k: Column): Column =
    Column.fn("kll_sketch_agg_float", e, k)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllFloatsSketch built with the values in the input column. The optional k parameter controls
   * the size and accuracy of the sketch (default 200, range 8-65535).
   *
   * @param e
   *   The column containing float values to aggregate. A column that evaluates to a numeric.
   * @param k
   *   The k parameter that controls size and accuracy (default 200, range 8-65535). A column that
   *   evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_float(e: Column, k: Int): Column =
    Column.fn("kll_sketch_agg_float", e, lit(k))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllFloatsSketch built with the values in the input column. The optional k parameter controls
   * the size and accuracy of the sketch (default 200, range 8-65535).
   *
   * @param columnName
   *   The column containing float values to aggregate. A column that evaluates to a numeric.
   * @param k
   *   The k parameter that controls size and accuracy (default 200, range 8-65535). A column that
   *   evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_float(columnName: String, k: Int): Column =
    kll_sketch_agg_float(Column(columnName), k)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllFloatsSketch built with the values in the input column with default k value of 200.
   *
   * @param e
   *   The column containing float values to aggregate. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_float(e: Column): Column =
    Column.fn("kll_sketch_agg_float", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllFloatsSketch built with the values in the input column with default k value of 200.
   *
   * @param columnName
   *   The column containing float values to aggregate. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_float(columnName: String): Column =
    kll_sketch_agg_float(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllDoublesSketch built with the values in the input column. The optional k parameter controls
   * the size and accuracy of the sketch (default 200, range 8-65535).
   *
   * @param e
   *   The column containing double values to aggregate. A column that evaluates to a float or
   *   double.
   * @param k
   *   The k parameter that controls size and accuracy (default 200, range 8-65535). A column that
   *   evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_double(e: Column, k: Column): Column =
    Column.fn("kll_sketch_agg_double", e, k)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllDoublesSketch built with the values in the input column. The optional k parameter controls
   * the size and accuracy of the sketch (default 200, range 8-65535).
   *
   * @param e
   *   The column containing double values to aggregate. A column that evaluates to a numeric.
   * @param k
   *   The k parameter that controls size and accuracy (default 200, range 8-65535). A column that
   *   evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_double(e: Column, k: Int): Column =
    Column.fn("kll_sketch_agg_double", e, lit(k))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllDoublesSketch built with the values in the input column. The optional k parameter controls
   * the size and accuracy of the sketch (default 200, range 8-65535).
   *
   * @param columnName
   *   The column containing double values to aggregate. A column that evaluates to a numeric.
   * @param k
   *   The k parameter that controls size and accuracy (default 200, range 8-65535). A column that
   *   evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_double(columnName: String, k: Int): Column =
    kll_sketch_agg_double(Column(columnName), k)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllDoublesSketch built with the values in the input column with default k value of 200.
   *
   * @param e
   *   The column containing double values to aggregate. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_double(e: Column): Column =
    Column.fn("kll_sketch_agg_double", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllDoublesSketch built with the values in the input column with default k value of 200.
   *
   * @param columnName
   *   The column containing double values to aggregate. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_agg_double(columnName: String): Column =
    kll_sketch_agg_double(Column(columnName))

  /**
   * Aggregate function: merges binary KllLongsSketch representations and returns the merged
   * sketch. The optional k parameter controls the size and accuracy of the merged sketch (range
   * 8-65535). If k is not specified, the merged sketch adopts the k value from the first input
   * sketch.
   *
   * @param e
   *   The column containing binary KllLongsSketch representations to merge. A column that
   *   evaluates to a binary.
   * @param k
   *   The k parameter that controls size and accuracy of the merged sketch (range 8-65535). A
   *   column that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_bigint(e: Column, k: Column): Column =
    Column.fn("kll_merge_agg_bigint", e, k)

  /**
   * Aggregate function: merges binary KllLongsSketch representations and returns the merged
   * sketch. The optional k parameter controls the size and accuracy of the merged sketch (range
   * 8-65535). If k is not specified, the merged sketch adopts the k value from the first input
   * sketch.
   *
   * @param e
   *   The column containing binary KllLongsSketch representations to merge. A column that
   *   evaluates to a binary.
   * @param k
   *   The k parameter that controls size and accuracy of the merged sketch (range 8-65535). A
   *   column that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_bigint(e: Column, k: Int): Column =
    Column.fn("kll_merge_agg_bigint", e, lit(k))

  /**
   * Aggregate function: merges binary KllLongsSketch representations and returns the merged
   * sketch. The optional k parameter controls the size and accuracy of the merged sketch (range
   * 8-65535). If k is not specified, the merged sketch adopts the k value from the first input
   * sketch.
   *
   * @param columnName
   *   The column containing binary KllLongsSketch representations. A column that evaluates to a
   *   binary.
   * @param k
   *   The k parameter that controls size and accuracy (range 8-65535). A column that evaluates to
   *   an integral.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_bigint(columnName: String, k: Int): Column =
    kll_merge_agg_bigint(Column(columnName), k)

  /**
   * Aggregate function: merges binary KllLongsSketch representations and returns the merged
   * sketch. If k is not specified, the merged sketch adopts the k value from the first input
   * sketch.
   *
   * @param e
   *   The column containing binary KllLongsSketch representations. A column that evaluates to a
   *   binary.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_bigint(e: Column): Column =
    Column.fn("kll_merge_agg_bigint", e)

  /**
   * Aggregate function: merges binary KllLongsSketch representations and returns the merged
   * sketch. If k is not specified, the merged sketch adopts the k value from the first input
   * sketch.
   *
   * @param columnName
   *   The column containing binary KllLongsSketch representations. A column that evaluates to a
   *   binary.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_bigint(columnName: String): Column =
    kll_merge_agg_bigint(Column(columnName))

  /**
   * Aggregate function: merges binary KllFloatsSketch representations and returns merged sketch.
   * The optional k parameter controls the size and accuracy of the merged sketch (range 8-65535).
   * If k is not specified, the merged sketch adopts the k value from the first input sketch.
   *
   * @param e
   *   The column containing binary KllFloatsSketch representations. A column that evaluates to a
   *   binary.
   * @param k
   *   The k parameter that controls size and accuracy (range 8-65535). A column that evaluates to
   *   an integral.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_float(e: Column, k: Column): Column =
    Column.fn("kll_merge_agg_float", e, k)

  /**
   * Aggregate function: merges binary KllFloatsSketch representations and returns merged sketch.
   * The optional k parameter controls the size and accuracy of the merged sketch (range 8-65535).
   * If k is not specified, the merged sketch adopts the k value from the first input sketch.
   *
   * @param e
   *   The column containing binary KllFloatsSketch representations. A column that evaluates to a
   *   binary.
   * @param k
   *   The k parameter that controls size and accuracy (range 8-65535). A column that evaluates to
   *   an integer. Must be a constant.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_float(e: Column, k: Int): Column =
    Column.fn("kll_merge_agg_float", e, lit(k))

  /**
   * Aggregate function: merges binary KllFloatsSketch representations and returns merged sketch.
   * The optional k parameter controls the size and accuracy of the merged sketch (range 8-65535).
   * If k is not specified, the merged sketch adopts the k value from the first input sketch.
   *
   * @param columnName
   *   The column containing binary KllFloatsSketch representations. A column that evaluates to a
   *   binary.
   * @param k
   *   The k parameter that controls size and accuracy (range 8-65535). A column that evaluates to
   *   an integer. Must be a constant.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_float(columnName: String, k: Int): Column =
    kll_merge_agg_float(Column(columnName), k)

  /**
   * Aggregate function: merges binary KllFloatsSketch representations and returns merged sketch.
   * If k is not specified, the merged sketch adopts the k value from the first input sketch.
   *
   * @param e
   *   The column containing binary KllFloatsSketch representations. A column that evaluates to a
   *   binary.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_float(e: Column): Column =
    Column.fn("kll_merge_agg_float", e)

  /**
   * Aggregate function: merges binary KllFloatsSketch representations and returns merged sketch.
   * If k is not specified, the merged sketch adopts the k value from the first input sketch.
   *
   * @param columnName
   *   The column containing binary KllFloatsSketch representations. A column that evaluates to a
   *   binary.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_float(columnName: String): Column =
    kll_merge_agg_float(Column(columnName))

  /**
   * Aggregate function: merges binary KllDoublesSketch representations and returns merged sketch.
   * The optional k parameter controls the size and accuracy of the merged sketch (range 8-65535).
   * If k is not specified, the merged sketch adopts the k value from the first input sketch.
   *
   * @param e
   *   The column containing binary KllDoublesSketch representations. A column that evaluates to a
   *   binary.
   * @param k
   *   The k parameter that controls size and accuracy (range 8-65535). A column that evaluates to
   *   an integer. Must be a constant.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_double(e: Column, k: Column): Column =
    Column.fn("kll_merge_agg_double", e, k)

  /**
   * Aggregate function: merges binary KllDoublesSketch representations and returns merged sketch.
   * The optional k parameter controls the size and accuracy of the merged sketch (range 8-65535).
   * If k is not specified, the merged sketch adopts the k value from the first input sketch.
   *
   * @param e
   *   The column containing binary KllDoublesSketch representations. A column that evaluates to a
   *   binary.
   * @param k
   *   The k parameter that controls size and accuracy (range 8-65535). A column that evaluates to
   *   an integer. Must be a constant.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_double(e: Column, k: Int): Column =
    Column.fn("kll_merge_agg_double", e, lit(k))

  /**
   * Aggregate function: merges binary KllDoublesSketch representations and returns merged sketch.
   * The optional k parameter controls the size and accuracy of the merged sketch (range 8-65535).
   * If k is not specified, the merged sketch adopts the k value from the first input sketch.
   *
   * @param columnName
   *   The column containing binary KllDoublesSketch representations. A column that evaluates to a
   *   binary.
   * @param k
   *   The k parameter that controls size and accuracy (range 8-65535). A column that evaluates to
   *   an integer. Must be a constant.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_double(columnName: String, k: Int): Column =
    kll_merge_agg_double(Column(columnName), k)

  /**
   * Aggregate function: merges binary KllDoublesSketch representations and returns merged sketch.
   * If k is not specified, the merged sketch adopts the k value from the first input sketch.
   *
   * @param e
   *   The column containing binary KllDoublesSketch representations. A column that evaluates to a
   *   binary.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_double(e: Column): Column =
    Column.fn("kll_merge_agg_double", e)

  /**
   * Aggregate function: merges binary KllDoublesSketch representations and returns merged sketch.
   * If k is not specified, the merged sketch adopts the k value from the first input sketch.
   *
   * @param columnName
   *   The column containing binary KllDoublesSketch representations. A column that evaluates to a
   *   binary.
   * @group agg_funcs
   * @since 4.1.2
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_merge_agg_double(columnName: String): Column =
    kll_merge_agg_double(Column(columnName))

  /**
   * Aggregate function: returns the concatenation of non-null input values.
   *
   * @param e
   *   The target column to compute on. A column that evaluates to a string or binary.
   * @group agg_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def listagg(e: Column): Column = Column.fn("listagg", e)

  /**
   * Aggregate function: returns the concatenation of non-null input values, separated by the
   * delimiter.
   *
   * @param e
   *   The target column to compute on. A column that evaluates to a string or binary.
   * @param delimiter
   *   The delimiter used to separate the values. A column that evaluates to a string or binary.
   *   Must be a constant.
   * @group agg_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def listagg(e: Column, delimiter: Column): Column = Column.fn("listagg", e, delimiter)

  /**
   * Aggregate function: returns the concatenation of distinct non-null input values.
   *
   * @param e
   *   the column to compute on. A column that evaluates to a string or binary.
   * @group agg_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def listagg_distinct(e: Column): Column = Column.fn("listagg", isDistinct = true, e)

  /**
   * Aggregate function: returns the concatenation of distinct non-null input values, separated by
   * the delimiter.
   *
   * @param e
   *   the column to compute on. A column that evaluates to a string or binary.
   * @param delimiter
   *   the delimiter to separate the values. A column that evaluates to a string or binary. Must
   *   be a constant.
   * @group agg_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def listagg_distinct(e: Column, delimiter: Column): Column =
    Column.fn("listagg", isDistinct = true, e, delimiter)

  /**
   * Aggregate function: returns the concatenation of non-null input values. Alias for `listagg`.
   *
   * @param e
   *   the column to compute on. A column that evaluates to a string or binary.
   * @group agg_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def string_agg(e: Column): Column = Column.fn("string_agg", e)

  /**
   * Aggregate function: returns the concatenation of non-null input values, separated by the
   * delimiter. Alias for `listagg`.
   *
   * @param e
   *   the column to compute on. A column that evaluates to a string or binary.
   * @param delimiter
   *   the delimiter to separate the values. A column that evaluates to a string or binary. Must
   *   be a constant.
   * @group agg_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def string_agg(e: Column, delimiter: Column): Column = Column.fn("string_agg", e, delimiter)

  /**
   * Aggregate function: returns the concatenation of distinct non-null input values. Alias for
   * `listagg`.
   *
   * @param e
   *   the column to compute on. A column that evaluates to a string or binary.
   * @group agg_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def string_agg_distinct(e: Column): Column = Column.fn("string_agg", isDistinct = true, e)

  /**
   * Aggregate function: returns the concatenation of distinct non-null input values, separated by
   * the delimiter. Alias for `listagg`.
   *
   * @param e
   *   the column to compute on. A column that evaluates to a string or binary.
   * @param delimiter
   *   the delimiter to separate the values. A column that evaluates to a string or binary. Must
   *   be a constant.
   * @group agg_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def string_agg_distinct(e: Column, delimiter: Column): Column =
    Column.fn("string_agg", isDistinct = true, e, delimiter)

  /**
   * Aggregate function: alias for `var_samp`.
   *
   * @param e
   *   the column to compute on. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def variance(e: Column): Column = Column.fn("variance", e)

  /**
   * Aggregate function: alias for `var_samp`.
   *
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def variance(columnName: String): Column = variance(Column(columnName))

  /**
   * Aggregate function: returns the unbiased variance of the values in a group.
   *
   * @param e
   *   the column to compute on. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def var_samp(e: Column): Column = Column.fn("var_samp", e)

  /**
   * Aggregate function: returns the unbiased variance of the values in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def var_samp(columnName: String): Column = var_samp(Column(columnName))

  /**
   * Aggregate function: returns the population variance of the values in a group.
   *
   * @param e
   *   the column to compute on. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def var_pop(e: Column): Column = Column.fn("var_pop", e)

  /**
   * Aggregate function: returns the population variance of the values in a group.
   *
   * @group agg_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def var_pop(columnName: String): Column = var_pop(Column(columnName))

  /**
   * Aggregate function: returns the average of the independent variable for non-null pairs in a
   * group, where `y` is the dependent variable and `x` is the independent variable.
   *
   * @param y
   *   the dependent variable. A column that evaluates to a numeric.
   * @param x
   *   the independent variable. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def regr_avgx(y: Column, x: Column): Column = Column.fn("regr_avgx", y, x)

  /**
   * Aggregate function: returns the average of the dependent variable for non-null pairs in a
   * group, where `y` is the dependent variable and `x` is the independent variable.
   *
   * @param y
   *   the dependent variable. A column that evaluates to a numeric.
   * @param x
   *   the independent variable. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def regr_avgy(y: Column, x: Column): Column = Column.fn("regr_avgy", y, x)

  /**
   * Aggregate function: returns the number of non-null number pairs in a group, where `y` is the
   * dependent variable and `x` is the independent variable.
   *
   * @param y
   *   the dependent variable. A column that evaluates to a numeric.
   * @param x
   *   the independent variable. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def regr_count(y: Column, x: Column): Column = Column.fn("regr_count", y, x)

  /**
   * Aggregate function: returns the intercept of the univariate linear regression line for
   * non-null pairs in a group, where `y` is the dependent variable and `x` is the independent
   * variable.
   *
   * @param y
   *   The dependent variable. A column that evaluates to a numeric.
   * @param x
   *   The independent variable. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def regr_intercept(y: Column, x: Column): Column = Column.fn("regr_intercept", y, x)

  /**
   * Aggregate function: returns the coefficient of determination for non-null pairs in a group,
   * where `y` is the dependent variable and `x` is the independent variable.
   *
   * @param y
   *   The dependent variable. A column that evaluates to a numeric.
   * @param x
   *   The independent variable. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def regr_r2(y: Column, x: Column): Column = Column.fn("regr_r2", y, x)

  /**
   * Aggregate function: returns the slope of the linear regression line for non-null pairs in a
   * group, where `y` is the dependent variable and `x` is the independent variable.
   *
   * @param y
   *   The dependent variable. A column that evaluates to a numeric.
   * @param x
   *   The independent variable. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def regr_slope(y: Column, x: Column): Column = Column.fn("regr_slope", y, x)

  /**
   * Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs in a group,
   * where `y` is the dependent variable and `x` is the independent variable.
   *
   * @param y
   *   The dependent variable. A column that evaluates to a numeric.
   * @param x
   *   The independent variable. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def regr_sxx(y: Column, x: Column): Column = Column.fn("regr_sxx", y, x)

  /**
   * Aggregate function: returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs in a group,
   * where `y` is the dependent variable and `x` is the independent variable.
   *
   * @param y
   *   The dependent variable. A column that evaluates to a numeric.
   * @param x
   *   The independent variable. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def regr_sxy(y: Column, x: Column): Column = Column.fn("regr_sxy", y, x)

  /**
   * Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs in a group,
   * where `y` is the dependent variable and `x` is the independent variable.
   *
   * @param y
   *   The dependent variable. A column that evaluates to a numeric.
   * @param x
   *   The independent variable. A column that evaluates to a numeric.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def regr_syy(y: Column, x: Column): Column = Column.fn("regr_syy", y, x)

  /**
   * Aggregate function: returns some value of `e` for a group of rows.
   *
   * @param e
   *   The column to return some value from. A column of any type.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def any_value(e: Column): Column = Column.fn("any_value", e)

  /**
   * Aggregate function: returns some value of `e` for a group of rows. If `ignoreNulls` is true,
   * returns only non-null values.
   *
   * @param e
   *   The column to return some value from. A column of any type.
   * @param ignoreNulls
   *   If true, returns only non-null values. A column that evaluates to a boolean. Must be a
   *   constant.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def any_value(e: Column, ignoreNulls: Column): Column =
    Column.fn("any_value", e, ignoreNulls)

  /**
   * Aggregate function: returns the number of `TRUE` values for the expression.
   *
   * @param e
   *   The expression to count TRUE values of. A column that evaluates to a boolean.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def count_if(e: Column): Column = Column.fn("count_if", e)

  /**
   * Returns the current time at the start of query evaluation. Note that the result will contain
   * 6 fractional digits of seconds.
   *
   * @return
   *   A time. Returns a column that evaluates to a time.
   * @group datetime_funcs
   * @since 4.1.0
   */
  def current_time(): Column = {
    Column.fn("current_time")
  }

  /**
   * Returns the current time at the start of query evaluation.
   *
   * @param precision
   *   An integer literal in the range [0..6], indicating how many fractional digits of seconds to
   *   include in the result. A column that evaluates to an integer. Must be a constant.
   * @return
   *   A time. Returns a column that evaluates to a time.
   * @group datetime_funcs
   * @since 4.1.0
   */
  def current_time(precision: Int): Column = {
    Column.fn("current_time", lit(precision))
  }

  /**
   * Aggregate function: computes a histogram on numeric 'expr' using nb bins. The return value is
   * an array of (x,y) pairs representing the centers of the histogram's bins. As the value of
   * 'nb' is increased, the histogram approximation gets finer-grained, but may yield artifacts
   * around outliers. In practice, 20-40 histogram bins appear to work well, with more bins being
   * required for skewed or smaller datasets. Note that this function creates a histogram with
   * non-uniform bin widths. It offers no guarantees in terms of the mean-squared-error of the
   * histogram, but in practice is comparable to the histograms produced by the R/S-Plus
   * statistical computing packages. Note: the output type of the 'x' field in the return value is
   * propagated from the input value consumed in the aggregate function.
   *
   * @param e
   *   The column to compute the histogram on. A column that evaluates to a numeric.
   * @param nBins
   *   The number of histogram bins. A column that evaluates to an integral. Must be a constant.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def histogram_numeric(e: Column, nBins: Column): Column =
    Column.fn("histogram_numeric", e, nBins)

  /**
   * Aggregate function: returns true if all values of `e` are true.
   *
   * @param e
   *   The expression to evaluate. A column that evaluates to a boolean.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def every(e: Column): Column = Column.fn("every", e)

  /**
   * Aggregate function: returns true if all values of `e` are true.
   *
   * @param e
   *   The expression to evaluate. A column that evaluates to a boolean.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def bool_and(e: Column): Column = Column.fn("bool_and", e)

  /**
   * Aggregate function: returns true if at least one value of `e` is true.
   *
   * @param e
   *   The expression to evaluate. A column that evaluates to a boolean.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def some(e: Column): Column = Column.fn("some", e)

  /**
   * Aggregate function: returns true if at least one value of `e` is true.
   *
   * @param e
   *   The expression to evaluate. A column that evaluates to a boolean.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def any(e: Column): Column = Column.fn("any", e)

  /**
   * Aggregate function: returns true if at least one value of `e` is true.
   *
   * @param e
   *   column to check if at least one value is true. A column that evaluates to a boolean.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def bool_or(e: Column): Column = Column.fn("bool_or", e)

  /**
   * Aggregate function: returns the bitwise AND of all non-null input values, or null if none.
   *
   * @param e
   *   target column to compute on. A column that evaluates to an integral.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def bit_and(e: Column): Column = Column.fn("bit_and", e)

  /**
   * Aggregate function: returns the bitwise OR of all non-null input values, or null if none.
   *
   * @param e
   *   target column to compute on. A column that evaluates to an integral.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def bit_or(e: Column): Column = Column.fn("bit_or", e)

  /**
   * Aggregate function: returns the bitwise XOR of all non-null input values, or null if none.
   *
   * @param e
   *   target column to compute on. A column that evaluates to an integral.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def bit_xor(e: Column): Column = Column.fn("bit_xor", e)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Window functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Window function: computes the differences between consecutive cumulative counter values in a
   * time series, thereby converting the counter from the cumulative to the delta format.
   *
   * Gracefully handles counter resets by returning NULL. Counter resets are detected when the
   * counter value decreases.
   *
   * Use the PARTITION BY clause of the window to separate independent counters. This is done by
   * specifying all columns which uniquely identify a time series. These are typically the counter
   * name and any attributes tied to the counter.
   *
   * Use the ORDER BY clause of the window to order the observations by the associated timestamp
   * in ascending order.
   *
   * @param value
   *   A cumulative counter. Must be a numeric data type. Must be non-negative.
   *
   * @return
   *   The difference between the current and previous counter value within the window partition,
   *   according to the order defined by the window's ORDER BY clause. Returns a column of the
   *   same type as the input.
   * @group window_funcs
   * @since 4.3.0
   */
  def counter_diff(value: Column): Column = Column.fn("counter_diff", value)

  /**
   * Window function: computes the differences between consecutive cumulative counter values in a
   * time series, thereby converting the counter from the cumulative to the delta format.
   *
   * Gracefully handles counter resets by returning NULL. Counter resets are detected when the
   * counter value decreases, or when the start time advances between rows.
   *
   * Use the PARTITION BY clause of the window to separate independent counters. This is done by
   * specifying all columns which uniquely identify a time series. These are typically the counter
   * name and any attributes tied to the counter.
   *
   * Use the ORDER BY clause of the window to order the observations by the associated timestamp
   * in ascending order.
   *
   * @param value
   *   A cumulative counter. Must be a numeric data type. Must be non-negative.
   *
   * @param startTime
   *   A timestamp indicating when the counter was last set to zero. Used to signal counter
   *   resets.
   *
   * @return
   *   The difference between the current and previous counter value within the window partition,
   *   according to the order defined by the window's ORDER BY clause. Returns a column of the
   *   same type as the input.
   * @group window_funcs
   * @since 4.3.0
   */
  def counter_diff(value: Column, startTime: Column): Column =
    Column.fn("counter_diff", value, startTime)

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
   * @return
   *   Returns a column that evaluates to a double.
   */
  def cume_dist(): Column = Column.fn("cume_dist")

  /**
   * Window function: returns the rank of rows within a window partition, without any gaps.
   *
   * The difference between rank and dense_rank is that denseRank leaves no gaps in ranking
   * sequence when there are ties. That is, if you were ranking a competition using dense_rank and
   * had three people tie for second place, you would say that all three were in second place and
   * that the next person came in third. Rank would give me sequential numbers, making the person
   * that came in third place (after the ties) would register as coming in fifth.
   *
   * This is equivalent to the DENSE_RANK function in SQL.
   *
   * @group window_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def dense_rank(): Column = Column.fn("dense_rank")

  /**
   * Window function: returns the value that is `offset` rows before the current row, and `null`
   * if there is less than `offset` rows before the current row. For example, an `offset` of one
   * will return the previous row at any given point in the window partition.
   *
   * This is equivalent to the LAG function in SQL.
   *
   * @param e
   *   the column to compute on. A column of any type.
   * @param offset
   *   number of rows to extend. A column that evaluates to an integer. Must be a constant.
   * @group window_funcs
   * @since 1.4.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lag(e: Column, offset: Int): Column = lag(e, offset, null)

  /**
   * Window function: returns the value that is `offset` rows before the current row, and `null`
   * if there is less than `offset` rows before the current row. For example, an `offset` of one
   * will return the previous row at any given point in the window partition.
   *
   * This is equivalent to the LAG function in SQL.
   *
   * @param columnName
   *   name of column or expression.
   * @param offset
   *   number of rows to extend. A column that evaluates to an integer. Must be a constant.
   * @group window_funcs
   * @since 1.4.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lag(columnName: String, offset: Int): Column = lag(columnName, offset, null)

  /**
   * Window function: returns the value that is `offset` rows before the current row, and
   * `defaultValue` if there is less than `offset` rows before the current row. For example, an
   * `offset` of one will return the previous row at any given point in the window partition.
   *
   * This is equivalent to the LAG function in SQL.
   *
   * @param columnName
   *   name of column or expression.
   * @param offset
   *   number of rows to extend. A column that evaluates to an integer. Must be a constant.
   * @param defaultValue
   *   default value. A column of any type.
   * @group window_funcs
   * @since 1.4.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lag(columnName: String, offset: Int, defaultValue: Any): Column = {
    lag(Column(columnName), offset, defaultValue)
  }

  /**
   * Window function: returns the value that is `offset` rows before the current row, and
   * `defaultValue` if there is less than `offset` rows before the current row. For example, an
   * `offset` of one will return the previous row at any given point in the window partition.
   *
   * This is equivalent to the LAG function in SQL.
   *
   * @param e
   *   the column to compute on. A column of any type.
   * @param offset
   *   number of rows to extend. A column that evaluates to an integer. Must be a constant.
   * @param defaultValue
   *   default value. A column of any type.
   * @group window_funcs
   * @since 1.4.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lag(e: Column, offset: Int, defaultValue: Any): Column = {
    lag(e, offset, defaultValue, false)
  }

  /**
   * Window function: returns the value that is `offset` rows before the current row, and
   * `defaultValue` if there is less than `offset` rows before the current row. `ignoreNulls`
   * determines whether null values of row are included in or eliminated from the calculation. For
   * example, an `offset` of one will return the previous row at any given point in the window
   * partition.
   *
   * This is equivalent to the LAG function in SQL.
   *
   * @param e
   *   the column to compute on. A column of any type.
   * @param offset
   *   number of rows to extend. A column that evaluates to an integer. Must be a constant.
   * @param defaultValue
   *   default value. A column of any type.
   * @param ignoreNulls
   *   whether to ignore null values. A column that evaluates to a boolean. Must be a constant.
   * @group window_funcs
   * @since 3.2.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lag(e: Column, offset: Int, defaultValue: Any, ignoreNulls: Boolean): Column =
    Column.fn("lag", false, e, lit(offset), lit(defaultValue), lit(ignoreNulls))

  /**
   * Window function: returns the value that is `offset` rows after the current row, and `null` if
   * there is less than `offset` rows after the current row. For example, an `offset` of one will
   * return the next row at any given point in the window partition.
   *
   * This is equivalent to the LEAD function in SQL.
   *
   * @param columnName
   *   name of column or expression.
   * @param offset
   *   number of rows to extend. A column that evaluates to an integer. Must be a constant.
   * @group window_funcs
   * @since 1.4.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lead(columnName: String, offset: Int): Column = { lead(columnName, offset, null) }

  /**
   * Window function: returns the value that is `offset` rows after the current row, and `null` if
   * there is less than `offset` rows after the current row. For example, an `offset` of one will
   * return the next row at any given point in the window partition.
   *
   * This is equivalent to the LEAD function in SQL.
   *
   * @param e
   *   the column to compute on. A column of any type.
   * @param offset
   *   number of rows to extend. A column that evaluates to an integer. Must be a constant.
   * @group window_funcs
   * @since 1.4.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lead(e: Column, offset: Int): Column = { lead(e, offset, null) }

  /**
   * Window function: returns the value that is `offset` rows after the current row, and
   * `defaultValue` if there is less than `offset` rows after the current row. For example, an
   * `offset` of one will return the next row at any given point in the window partition.
   *
   * This is equivalent to the LEAD function in SQL.
   *
   * @param columnName
   *   name of column or expression.
   * @param offset
   *   number of rows to extend. A column that evaluates to an integer. Must be a constant.
   * @param defaultValue
   *   default value. A column of any type.
   * @group window_funcs
   * @since 1.4.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lead(columnName: String, offset: Int, defaultValue: Any): Column = {
    lead(Column(columnName), offset, defaultValue)
  }

  /**
   * Window function: returns the value that is `offset` rows after the current row, and
   * `defaultValue` if there is less than `offset` rows after the current row. For example, an
   * `offset` of one will return the next row at any given point in the window partition.
   *
   * This is equivalent to the LEAD function in SQL.
   *
   * @param e
   *   the column to compute on. A column of any type.
   * @param offset
   *   number of rows to extend. A column that evaluates to an integer. Must be a constant.
   * @param defaultValue
   *   default value. A column of any type.
   * @group window_funcs
   * @since 1.4.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lead(e: Column, offset: Int, defaultValue: Any): Column = {
    lead(e, offset, defaultValue, false)
  }

  /**
   * Window function: returns the value that is `offset` rows after the current row, and
   * `defaultValue` if there is less than `offset` rows after the current row. `ignoreNulls`
   * determines whether null values of row are included in or eliminated from the calculation. The
   * default value of `ignoreNulls` is false. For example, an `offset` of one will return the next
   * row at any given point in the window partition.
   *
   * This is equivalent to the LEAD function in SQL.
   *
   * @param e
   *   The column to compute the lead value for. A column of any type.
   * @param offset
   *   Number of rows after the current row to look ahead. A column that evaluates to an integral.
   *   Must be a constant.
   * @param defaultValue
   *   Value to return when there are fewer than `offset` rows after the current row. A column of
   *   any type. Must be a constant.
   * @param ignoreNulls
   *   Whether to skip null values when computing the result. A column that evaluates to a
   *   boolean. Must be a constant.
   * @group window_funcs
   * @since 3.2.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lead(e: Column, offset: Int, defaultValue: Any, ignoreNulls: Boolean): Column =
    Column.fn("lead", false, e, lit(offset), lit(defaultValue), lit(ignoreNulls))

  /**
   * Window function: returns the value that is the `offset`th row of the window frame (counting
   * from 1), and `null` if the size of window frame is less than `offset` rows.
   *
   * It will return the `offset`th non-null value it sees when ignoreNulls is set to true. If all
   * values are null, then null is returned.
   *
   * This is equivalent to the nth_value function in SQL.
   *
   * @param e
   *   The column to extract the value from. A column of any type.
   * @param offset
   *   The 1-based row number within the window frame to use as the value. A column that evaluates
   *   to an integral. Must be a constant.
   * @param ignoreNulls
   *   Whether the nth value should skip nulls when determining which row to use. A column that
   *   evaluates to a boolean. Must be a constant.
   * @group window_funcs
   * @since 3.1.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def nth_value(e: Column, offset: Int, ignoreNulls: Boolean): Column =
    Column.fn("nth_value", false, e, lit(offset), lit(ignoreNulls))

  /**
   * Window function: returns the value that is the `offset`th row of the window frame (counting
   * from 1), and `null` if the size of window frame is less than `offset` rows.
   *
   * This is equivalent to the nth_value function in SQL.
   *
   * @param e
   *   The column to extract the value from. A column of any type.
   * @param offset
   *   The 1-based row number within the window frame to use as the value. A column that evaluates
   *   to an integral. Must be a constant.
   * @group window_funcs
   * @since 3.1.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def nth_value(e: Column, offset: Int): Column = nth_value(e, offset, false)

  /**
   * Window function: returns the ntile group id (from 1 to `n` inclusive) in an ordered window
   * partition. For example, if `n` is 4, the first quarter of the rows will get value 1, the
   * second quarter will get 2, the third quarter will get 3, and the last quarter will get 4.
   *
   * This is equivalent to the NTILE function in SQL.
   *
   * @param n
   *   The number of groups to divide the window partition into. A column that evaluates to an
   *   integral. Must be a constant.
   * @group window_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def ntile(n: Int): Column = Column.fn("ntile", lit(n))

  /**
   * Window function: returns the relative rank (i.e. percentile) of rows within a window
   * partition.
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
   * @return
   *   Returns a column that evaluates to a double.
   */
  def percent_rank(): Column = Column.fn("percent_rank")

  /**
   * Window function: returns the rank of rows within a window partition.
   *
   * The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
   * sequence when there are ties. That is, if you were ranking a competition using dense_rank and
   * had three people tie for second place, you would say that all three were in second place and
   * that the next person came in third. Rank would give me sequential numbers, making the person
   * that came in third place (after the ties) would register as coming in fifth.
   *
   * This is equivalent to the RANK function in SQL.
   *
   * @group window_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def rank(): Column = Column.fn("rank")

  /**
   * Window function: returns a sequential number starting at 1 within a window partition.
   *
   * @group window_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def row_number(): Column = Column.fn("row_number")

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Non-aggregate functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new array column. The input columns must all have the same data type.
   *
   * @param cols
   *   The columns to combine into an array. Each is a column of any type, and all must share the
   *   same data type.
   * @group array_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  @scala.annotation.varargs
  def array(cols: Column*): Column = Column.fn("array", cols: _*)

  /**
   * Creates a new array column. The input columns must all have the same data type.
   *
   * @group array_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to an array.
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
   * @param cols
   *   The columns grouped as key-value pairs (key1, value1, key2, value2, ...). Each is a column
   *   of any type; key columns must share a type and value columns must share a type.
   * @group map_funcs
   * @since 2.0
   * @return
   *   Returns a column that evaluates to a map.
   */
  @scala.annotation.varargs
  def map(cols: Column*): Column = Column.fn("map", cols: _*)

  /**
   * Creates a struct with the given field names and values.
   *
   * @param cols
   *   The field names and values grouped as pairs (name1, value1, name2, value2, ...). Names are
   *   columns that evaluate to a string; values are columns of any type.
   * @group struct_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  @scala.annotation.varargs
  def named_struct(cols: Column*): Column = Column.fn("named_struct", cols: _*)

  /**
   * Creates a new map column. The array in the first column is used for keys. The array in the
   * second column is used for values. All elements in the array for key should not be null.
   *
   * @param keys
   *   The array of keys for the map; elements must not be null. A column that evaluates to an
   *   array.
   * @param values
   *   The array of values for the map. A column that evaluates to an array.
   * @group map_funcs
   * @since 2.4
   * @return
   *   Returns a column that evaluates to a map.
   */
  def map_from_arrays(keys: Column, values: Column): Column =
    Column.fn("map_from_arrays", keys, values)

  /**
   * Creates a map after splitting the text into key/value pairs using delimiters. Both
   * `pairDelim` and `keyValueDelim` are treated as regular expressions.
   *
   * @param text
   *   The text to split into key/value pairs. A column that evaluates to a string.
   * @param pairDelim
   *   Delimiter used to split pairs, treated as a regular expression. A column that evaluates to
   *   a string.
   * @param keyValueDelim
   *   Delimiter used to split key and value, treated as a regular expression. A column that
   *   evaluates to a string.
   * @group map_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a map.
   */
  def str_to_map(text: Column, pairDelim: Column, keyValueDelim: Column): Column =
    Column.fn("str_to_map", text, pairDelim, keyValueDelim)

  /**
   * Creates a map after splitting the text into key/value pairs using delimiters. The `pairDelim`
   * is treated as regular expressions.
   *
   * @param text
   *   The text to split into key/value pairs. A column that evaluates to a string.
   * @param pairDelim
   *   Delimiter used to split pairs, treated as a regular expression. A column that evaluates to
   *   a string.
   * @group map_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a map.
   */
  def str_to_map(text: Column, pairDelim: Column): Column =
    Column.fn("str_to_map", text, pairDelim)

  /**
   * Creates a map after splitting the text into key/value pairs using delimiters.
   *
   * @param text
   *   The text to split into key/value pairs. A column that evaluates to a string.
   * @group map_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a map.
   */
  def str_to_map(text: Column): Column = Column.fn("str_to_map", text)

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
  def broadcast[U](df: Dataset[U]): df.type = {
    df.hint("broadcast").asInstanceOf[df.type]
  }

  /**
   * Returns the first column that is not null, or null if all inputs are null.
   *
   * For example, `coalesce(a, b, c)` will return a if a is not null, or b if a is null and b is
   * not null, or c if both a and b are null but c is not null.
   *
   * @param e
   *   the columns to work on. A column that evaluates to any type.
   * @group conditional_funcs
   * @since 1.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  @scala.annotation.varargs
  def coalesce(e: Column*): Column = Column.fn("coalesce", e: _*)

  /**
   * Creates a string column for the file name of the current Spark task.
   *
   * @group misc_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def input_file_name(): Column = Column.fn("input_file_name")

  /**
   * Return true iff the column is NaN.
   *
   * @param e
   *   the column to check. A column that evaluates to a numeric.
   * @group predicate_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def isnan(e: Column): Column = e.isNaN

  /**
   * Return true iff the column is null.
   *
   * @param e
   *   the column to check. A column that evaluates to any type.
   * @group predicate_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a boolean.
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
   * @group misc_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a long.
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
   * @group misc_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def monotonically_increasing_id(): Column = Column.fn("monotonically_increasing_id")

  /**
   * Returns col1 if it is not NaN, or col2 if col1 is NaN.
   *
   * Both inputs should be floating point columns (DoubleType or FloatType).
   *
   * @param col1
   *   the first column to check. A column that evaluates to a numeric.
   * @param col2
   *   the column to return if the first is NaN. A column that evaluates to a numeric.
   * @group conditional_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the first input.
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
   * @param e
   *   the column to negate. A column that evaluates to a numeric or interval.
   * @group math_funcs
   * @since 1.3.0
   * @return
   *   Returns a column of the same type as the input.
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
   * @param e
   *   the column to invert. A column that evaluates to a boolean.
   * @group predicate_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def not(e: Column): Column = !e

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [0.0, 1.0).
   *
   * @param seed
   *   the seed for the random generator.
   * @note
   *   The function is non-deterministic in general case.
   *
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def rand(seed: Long): Column = Column.fn("rand", lit(seed))

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [0.0, 1.0).
   *
   * @note
   *   The function is non-deterministic in general case.
   *
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def rand(): Column = rand(SparkClassUtils.random.nextLong)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples from the
   * standard normal distribution.
   *
   * @param seed
   *   the seed for the random generator.
   * @note
   *   The function is non-deterministic in general case.
   *
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def randn(seed: Long): Column = Column.fn("randn", lit(seed))

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples from the
   * standard normal distribution.
   *
   * @note
   *   The function is non-deterministic in general case.
   *
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def randn(): Column = randn(SparkClassUtils.random.nextLong)

  /**
   * Returns a string of the specified length whose characters are chosen uniformly at random from
   * the following pool of characters: 0-9, a-z, A-Z. The string length must be a constant
   * two-byte or four-byte integer (SMALLINT or INT, respectively).
   *
   * @param length
   *   the number of characters in the string to generate. A column that evaluates to an integral.
   *   Must be a constant.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def randstr(length: Column): Column =
    randstr(length, lit(SparkClassUtils.random.nextLong))

  /**
   * Returns a string of the specified length whose characters are chosen uniformly at random from
   * the following pool of characters: 0-9, a-z, A-Z, with the chosen random seed. The string
   * length must be a constant two-byte or four-byte integer (SMALLINT or INT, respectively).
   *
   * @param length
   *   the number of characters in the string to generate. A column that evaluates to an integral.
   *   Must be a constant.
   * @param seed
   *   the random seed to use. A column that evaluates to an integral.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def randstr(length: Column, seed: Column): Column = Column.fn("randstr", length, seed)

  /**
   * Partition ID.
   *
   * @note
   *   This is non-deterministic because it depends on data partitioning and task scheduling.
   *
   * @group misc_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def spark_partition_id(): Column = Column.fn("spark_partition_id")

  /**
   * Computes the square root of the specified float value.
   *
   * @param e
   *   the value to compute the square root of. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def sqrt(e: Column): Column = Column.fn("sqrt", e)

  /**
   * Computes the square root of the specified float value.
   *
   * @param colName
   *   the name of a numeric column to compute the square root of.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def sqrt(colName: String): Column = sqrt(Column(colName))

  /**
   * Returns the sum of `left` and `right` and the result is null on overflow. The acceptable
   * input types are the same with the `+` operator.
   *
   * @param left
   *   the left operand. A column that evaluates to a numeric or interval.
   * @param right
   *   the right operand. A column that evaluates to a numeric or interval.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def try_add(left: Column, right: Column): Column = Column.fn("try_add", left, right)

  /**
   * Returns the mean calculated from values of a group and the result is null on overflow.
   *
   * @param e
   *   the value to compute the mean of. A column that evaluates to a numeric or interval.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def try_avg(e: Column): Column = Column.fn("try_avg", e)

  /**
   * Returns `dividend``/``divisor`. It always performs floating point division. Its result is
   * always null if `divisor` is 0.
   *
   * @param left
   *   the dividend. A column that evaluates to a numeric or interval.
   * @param right
   *   the divisor. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def try_divide(left: Column, right: Column): Column = Column.fn("try_divide", left, right)

  /**
   * Returns the remainder of `dividend``/``divisor`. Its result is always null if `divisor` is 0.
   *
   * @param left
   *   the dividend. A column that evaluates to a numeric.
   * @param right
   *   the divisor. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def try_mod(left: Column, right: Column): Column = Column.fn("try_mod", left, right)

  /**
   * Returns `left``*``right` and the result is null on overflow. The acceptable input types are
   * the same with the `*` operator.
   *
   * @param left
   *   the multiplicand. A column that evaluates to a numeric or interval.
   * @param right
   *   the multiplier. A column that evaluates to a numeric or interval.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def try_multiply(left: Column, right: Column): Column = Column.fn("try_multiply", left, right)

  /**
   * Returns `left``-``right` and the result is null on overflow. The acceptable input types are
   * the same with the `-` operator.
   *
   * @param left
   *   the left operand. A column that evaluates to a numeric or interval.
   * @param right
   *   the right operand. A column that evaluates to a numeric or interval.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def try_subtract(left: Column, right: Column): Column = Column.fn("try_subtract", left, right)

  /**
   * Returns the sum calculated from values of a group and the result is null on overflow.
   *
   * @param e
   *   the value to compute the sum of. A column that evaluates to a numeric or interval.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a numeric.
   */
  def try_sum(e: Column): Column = Column.fn("try_sum", e)

  /**
   * Creates a new struct column. If the input column is a column in a `DataFrame`, or a derived
   * column expression that is named (i.e. aliased), its name would be retained as the
   * StructField's name, otherwise, the newly generated StructField's name would be auto generated
   * as `col` with a suffix `index + 1`, i.e. col1, col2, col3, ...
   *
   * @param cols
   *   the columns to contain in the output struct. A column of any type.
   * @group struct_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  @scala.annotation.varargs
  def struct(cols: Column*): Column = Column.fn("struct", cols: _*)

  /**
   * Creates a new struct column that composes multiple input columns.
   *
   * @param colName
   *   the name of the first column to contain in the output struct.
   * @param colNames
   *   the names of the remaining columns to contain in the output struct.
   * @group struct_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a struct.
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
   * @param condition
   *   the condition to evaluate. A column that evaluates to a boolean.
   * @param value
   *   the value to return when the condition is true. A literal value, or a column expression.
   * @group conditional_funcs
   * @since 1.4.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def when(condition: Column, value: Any): Column =
    Column(internal.CaseWhenOtherwise(Seq(condition.node -> lit(value).node)))

  /**
   * Computes bitwise NOT (~) of a number.
   *
   * @group bitwise_funcs
   * @since 1.4.0
   * @return
   *   Returns a column of the same type as the input.
   */
  @deprecated("Use bitwise_not", "3.2.0")
  def bitwiseNOT(e: Column): Column = bitwise_not(e)

  /**
   * Computes bitwise NOT (~) of a number.
   *
   * @param e
   *   the target column to compute on. A column that evaluates to an integral.
   * @group bitwise_funcs
   * @since 3.2.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def bitwise_not(e: Column): Column = Column.fn("~", e)

  /**
   * Returns the number of bits that are set in the argument expr as an unsigned 64-bit integer,
   * or NULL if the argument is NULL.
   *
   * @param e
   *   the target column to compute on. A column that evaluates to an integral or boolean.
   * @group bitwise_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def bit_count(e: Column): Column = Column.fn("bit_count", e)

  /**
   * Returns the value of the bit (0 or 1) at the specified position. The positions are numbered
   * from right to left, starting at zero. The position argument cannot be negative.
   *
   * @param e
   *   the target column to compute on. A column that evaluates to an integral.
   * @param pos
   *   the bit position, numbered from right to left starting at zero. A column that evaluates to
   *   an integer.
   * @group bitwise_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a byte.
   */
  def bit_get(e: Column, pos: Column): Column = Column.fn("bit_get", e, pos)

  /**
   * Returns the value of the bit (0 or 1) at the specified position. The positions are numbered
   * from right to left, starting at zero. The position argument cannot be negative.
   *
   * @param e
   *   the target column to compute on. A column that evaluates to an integral.
   * @param pos
   *   the bit position, numbered from right to left starting at zero. A column that evaluates to
   *   an integer.
   * @group bitwise_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a byte.
   */
  def getbit(e: Column, pos: Column): Column = Column.fn("getbit", e, pos)

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
  def expr(expr: String): Column = Column(internal.SqlExpression(expr))

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Math Functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Computes the absolute value of a numeric value.
   *
   * @param e
   *   the value to compute the absolute value of. A column that evaluates to a numeric or
   *   interval.
   * @group math_funcs
   * @since 1.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def abs(e: Column): Column = Column.fn("abs", e)

  /**
   * @param e
   *   the value to compute the inverse cosine of. A column that evaluates to a double.
   * @return
   *   inverse cosine of `e` in radians, as if computed by `java.lang.Math.acos`. Returns a column
   *   that evaluates to a double.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def acos(e: Column): Column = Column.fn("acos", e)

  /**
   * @param columnName
   *   the value to compute the inverse cosine of.
   * @return
   *   inverse cosine of `columnName`, as if computed by `java.lang.Math.acos`. Returns a column
   *   that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   */
  def acos(columnName: String): Column = acos(Column(columnName))

  /**
   * @param e
   *   the value to compute the inverse hyperbolic cosine of. A column that evaluates to a double.
   * @return
   *   inverse hyperbolic cosine of `e`. Returns a column that evaluates to a double.
   *
   * @group math_funcs
   * @since 3.1.0
   */
  def acosh(e: Column): Column = Column.fn("acosh", e)

  /**
   * @param columnName
   *   the value to compute the inverse hyperbolic cosine of.
   * @return
   *   inverse hyperbolic cosine of `columnName`. Returns a column that evaluates to a double.
   * @group math_funcs
   * @since 3.1.0
   */
  def acosh(columnName: String): Column = acosh(Column(columnName))

  /**
   * @param e
   *   the value to compute the inverse sine of. A column that evaluates to a double.
   * @return
   *   inverse sine of `e` in radians, as if computed by `java.lang.Math.asin`. Returns a column
   *   that evaluates to a double.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def asin(e: Column): Column = Column.fn("asin", e)

  /**
   * @param columnName
   *   the value to compute the inverse sine of.
   * @return
   *   inverse sine of `columnName`, as if computed by `java.lang.Math.asin`. Returns a column
   *   that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   */
  def asin(columnName: String): Column = asin(Column(columnName))

  /**
   * @param e
   *   the value to compute the inverse hyperbolic sine of. A column that evaluates to a double.
   * @return
   *   inverse hyperbolic sine of `e`. Returns a column that evaluates to a double.
   *
   * @group math_funcs
   * @since 3.1.0
   */
  def asinh(e: Column): Column = Column.fn("asinh", e)

  /**
   * @param columnName
   *   the value to compute the inverse hyperbolic sine of.
   * @return
   *   inverse hyperbolic sine of `columnName`. Returns a column that evaluates to a double.
   * @group math_funcs
   * @since 3.1.0
   */
  def asinh(columnName: String): Column = asinh(Column(columnName))

  /**
   * @param e
   *   the value to compute the inverse tangent of. A column that evaluates to a double.
   * @return
   *   inverse tangent of `e` as if computed by `java.lang.Math.atan`. Returns a column that
   *   evaluates to a double.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def atan(e: Column): Column = Column.fn("atan", e)

  /**
   * @param columnName
   *   the value to compute the inverse tangent of.
   * @return
   *   inverse tangent of `columnName`, as if computed by `java.lang.Math.atan`. Returns a column
   *   that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   */
  def atan(columnName: String): Column = atan(Column(columnName))

  /**
   * @param y
   *   coordinate on y-axis. A column that evaluates to a double.
   * @param x
   *   coordinate on x-axis. A column that evaluates to a double.
   * @return
   *   the <i>theta</i> component of the point (<i>r</i>, <i>theta</i>) in polar coordinates that
   *   corresponds to the point (<i>x</i>, <i>y</i>) in Cartesian coordinates, as if computed by
   *   `java.lang.Math.atan2`. Returns a column that evaluates to a double.
   *
   * @group math_funcs
   * @since 1.4.0
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
   *   `java.lang.Math.atan2`. Returns a column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
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
   *   `java.lang.Math.atan2`. Returns a column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
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
   *   `java.lang.Math.atan2`. Returns a column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
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
   *   `java.lang.Math.atan2`. Returns a column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
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
   *   `java.lang.Math.atan2`. Returns a column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
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
   *   `java.lang.Math.atan2`. Returns a column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
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
   *   `java.lang.Math.atan2`. Returns a column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   */
  def atan2(yValue: Double, xName: String): Column = atan2(yValue, Column(xName))

  /**
   * @param e
   *   target column to compute on. A column that evaluates to a numeric.
   * @return
   *   inverse hyperbolic tangent of `e`. Returns a column that evaluates to a double.
   *
   * @group math_funcs
   * @since 3.1.0
   */
  def atanh(e: Column): Column = Column.fn("atanh", e)

  /**
   * @param columnName
   *   target column to compute on.
   * @return
   *   inverse hyperbolic tangent of `columnName`. Returns a column that evaluates to a double.
   * @group math_funcs
   * @since 3.1.0
   */
  def atanh(columnName: String): Column = atanh(Column(columnName))

  /**
   * An expression that returns the string representation of the binary value of the given long
   * column. For example, bin("12") returns "1100".
   *
   * @param e
   *   target column to work on. A column that evaluates to an integral.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def bin(e: Column): Column = Column.fn("bin", e)

  /**
   * An expression that returns the string representation of the binary value of the given long
   * column. For example, bin("12") returns "1100".
   *
   * @param columnName
   *   target column to work on.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def bin(columnName: String): Column = bin(Column(columnName))

  /**
   * Computes the cube-root of the given value.
   *
   * @param e
   *   target column to compute on. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def cbrt(e: Column): Column = Column.fn("cbrt", e)

  /**
   * Computes the cube-root of the given column.
   *
   * @param columnName
   *   target column to compute on.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def cbrt(columnName: String): Column = cbrt(Column(columnName))

  /**
   * Computes the ceiling of the given value of `e` to `scale` decimal places.
   *
   * @param e
   *   the value to compute the ceiling on. A column that evaluates to a numeric.
   * @param scale
   *   parameter to control the rounding behavior. A column that evaluates to an integral. Must be
   *   a constant.
   * @group math_funcs
   * @since 3.3.0
   * @return
   *   Returns a column that evaluates to a long or decimal.
   */
  def ceil(e: Column, scale: Column): Column = Column.fn("ceil", e, scale)

  /**
   * Computes the ceiling of the given value of `e` to 0 decimal places.
   *
   * @param e
   *   the value to compute the ceiling on. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a long or decimal.
   */
  def ceil(e: Column): Column = Column.fn("ceil", e)

  /**
   * Computes the ceiling of the given value of `columnName` to 0 decimal places.
   *
   * @param columnName
   *   the value to compute the ceiling on.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a long or decimal.
   */
  def ceil(columnName: String): Column = ceil(Column(columnName))

  /**
   * Computes the ceiling of the given value of `e` to `scale` decimal places.
   *
   * @param e
   *   the value to compute the ceiling on. A column that evaluates to a numeric.
   * @param scale
   *   parameter to control the rounding behavior. A column that evaluates to an integer. Must be
   *   a constant.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long or decimal.
   */
  def ceiling(e: Column, scale: Column): Column = Column.fn("ceiling", e, scale)

  /**
   * Computes the ceiling of the given value of `e` to 0 decimal places.
   *
   * @param e
   *   the value to compute the ceiling on. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long or decimal.
   */
  def ceiling(e: Column): Column = Column.fn("ceiling", e)

  /**
   * Convert a number in a string column from one base to another.
   *
   * @param num
   *   a column to convert base for. A column that evaluates to a string.
   * @param fromBase
   *   from base number. A column that evaluates to an integer.
   * @param toBase
   *   to base number. A column that evaluates to an integer.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def conv(num: Column, fromBase: Int, toBase: Int): Column =
    Column.fn("conv", num, lit(fromBase), lit(toBase))

  /**
   * @param e
   *   angle in radians. A column that evaluates to a double.
   * @return
   *   cosine of the angle, as if computed by `java.lang.Math.cos`. Returns a column that
   *   evaluates to a double.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cos(e: Column): Column = Column.fn("cos", e)

  /**
   * @param columnName
   *   angle in radians. A column that evaluates to a double.
   * @return
   *   cosine of the angle, as if computed by `java.lang.Math.cos`. Returns a column that
   *   evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   */
  def cos(columnName: String): Column = cos(Column(columnName))

  /**
   * @param e
   *   hyperbolic angle. A column that evaluates to a double.
   * @return
   *   hyperbolic cosine of the angle, as if computed by `java.lang.Math.cosh`. Returns a column
   *   that evaluates to a double.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def cosh(e: Column): Column = Column.fn("cosh", e)

  /**
   * @param columnName
   *   hyperbolic angle
   * @return
   *   hyperbolic cosine of the angle, as if computed by `java.lang.Math.cosh`. Returns a column
   *   that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   */
  def cosh(columnName: String): Column = cosh(Column(columnName))

  /**
   * @param e
   *   angle in radians. A column that evaluates to a double.
   * @return
   *   cotangent of the angle. Returns a column that evaluates to a double.
   *
   * @group math_funcs
   * @since 3.3.0
   */
  def cot(e: Column): Column = Column.fn("cot", e)

  /**
   * @param e
   *   angle in radians. A column that evaluates to a double.
   * @return
   *   cosecant of the angle. Returns a column that evaluates to a double.
   *
   * @group math_funcs
   * @since 3.3.0
   */
  def csc(e: Column): Column = Column.fn("csc", e)

  /**
   * Returns Euler's number.
   *
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def e(): Column = Column.fn("e")

  /**
   * Computes the exponential of the given value.
   *
   * @param e
   *   target column to compute on. A column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def exp(e: Column): Column = Column.fn("exp", e)

  /**
   * Computes the exponential of the given column.
   *
   * @param columnName
   *   target column to compute on.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def exp(columnName: String): Column = exp(Column(columnName))

  /**
   * Computes the exponential of the given value minus one.
   *
   * @param e
   *   column to calculate exponential for. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def expm1(e: Column): Column = Column.fn("expm1", e)

  /**
   * Computes the exponential of the given column minus one.
   *
   * @param columnName
   *   column name to calculate exponential for. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def expm1(columnName: String): Column = expm1(Column(columnName))

  /**
   * Computes the factorial of the given value.
   *
   * @param e
   *   a column to calculate factorial for. A column that evaluates to an integral.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def factorial(e: Column): Column = Column.fn("factorial", e)

  /**
   * Computes the floor of the given value of `e` to `scale` decimal places.
   *
   * @param e
   *   the target column to compute the floor on. A column that evaluates to a numeric.
   * @param scale
   *   the number of decimal places to control the rounding behavior. A column that evaluates to
   *   an integral.
   * @group math_funcs
   * @since 3.3.0
   * @return
   *   Returns a column that evaluates to a long or decimal.
   */
  def floor(e: Column, scale: Column): Column = Column.fn("floor", e, scale)

  /**
   * Computes the floor of the given value of `e` to 0 decimal places.
   *
   * @param e
   *   the target column to compute the floor on. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a long or decimal.
   */
  def floor(e: Column): Column = Column.fn("floor", e)

  /**
   * Computes the floor of the given column value to 0 decimal places.
   *
   * @param columnName
   *   the target column name to compute the floor on. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a long or decimal.
   */
  def floor(columnName: String): Column = floor(Column(columnName))

  /**
   * Returns the greatest value of the list of values, skipping null values. This function takes
   * at least 2 parameters. It will return null iff all parameters are null.
   *
   * @param exprs
   *   columns to check for greatest value. A column that evaluates to any type.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  @scala.annotation.varargs
  def greatest(exprs: Column*): Column = Column.fn("greatest", exprs: _*)

  /**
   * Returns the greatest value of the list of column names, skipping null values. This function
   * takes at least 2 parameters. It will return null iff all parameters are null.
   *
   * @param columnName
   *   the first column name to check for greatest value. A column of a comparable type.
   * @param columnNames
   *   the remaining column names to check for greatest value. Columns of a comparable type.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  @scala.annotation.varargs
  def greatest(columnName: String, columnNames: String*): Column = {
    greatest((columnName +: columnNames).map(Column.apply): _*)
  }

  /**
   * Computes hex value of the given column.
   *
   * @param column
   *   target column to work on. A column that evaluates to an integral, string or binary.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def hex(column: Column): Column = Column.fn("hex", column)

  /**
   * Inverse of hex. Interprets each pair of characters as a hexadecimal number and converts to
   * the byte representation of number.
   *
   * @param column
   *   target column to work on. A column that evaluates to a string.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def unhex(column: Column): Column = Column.fn("unhex", column)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @param l
   *   a leg. A column that evaluates to a numeric.
   * @param r
   *   b leg. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def hypot(l: Column, r: Column): Column = Column.fn("hypot", l, r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @param l
   *   a leg. A column that evaluates to a numeric.
   * @param rightName
   *   b leg. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def hypot(l: Column, rightName: String): Column = hypot(l, Column(rightName))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @param leftName
   *   a leg. A column that evaluates to a numeric.
   * @param r
   *   b leg. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def hypot(leftName: String, r: Column): Column = hypot(Column(leftName), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @param leftName
   *   a leg. A column that evaluates to a numeric.
   * @param rightName
   *   b leg. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def hypot(leftName: String, rightName: String): Column =
    hypot(Column(leftName), Column(rightName))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @param l
   *   a leg. A column that evaluates to a numeric.
   * @param r
   *   b leg. A column that evaluates to a numeric. Must be a constant.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def hypot(l: Column, r: Double): Column = hypot(l, lit(r))

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @param leftName
   *   The a leg of the triangle. A column that evaluates to a numeric.
   * @param r
   *   The b leg of the triangle. A column that evaluates to a numeric. Must be a constant.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def hypot(leftName: String, r: Double): Column = hypot(Column(leftName), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @param l
   *   The a leg of the triangle. A column that evaluates to a numeric. Must be a constant.
   * @param r
   *   The b leg of the triangle. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def hypot(l: Double, r: Column): Column = hypot(lit(l), r)

  /**
   * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
   *
   * @param l
   *   The a leg of the triangle. A column that evaluates to a numeric. Must be a constant.
   * @param rightName
   *   The b leg of the triangle. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def hypot(l: Double, rightName: String): Column = hypot(l, Column(rightName))

  /**
   * Returns the least value of the list of values, skipping null values. This function takes at
   * least 2 parameters. It will return null iff all parameters are null.
   *
   * @param exprs
   *   The values to be compared. Columns that evaluate to a comparable type.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  @scala.annotation.varargs
  def least(exprs: Column*): Column = Column.fn("least", exprs: _*)

  /**
   * Returns the least value of the list of column names, skipping null values. This function
   * takes at least 2 parameters. It will return null iff all parameters are null.
   *
   * @param columnName
   *   The name of the first column to be compared. A column of a comparable type.
   * @param columnNames
   *   The names of the remaining columns to be compared. Columns of a comparable type.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  @scala.annotation.varargs
  def least(columnName: String, columnNames: String*): Column = {
    least((columnName +: columnNames).map(Column.apply): _*)
  }

  /**
   * Computes the natural logarithm of the given value.
   *
   * @param e
   *   The value to compute the natural logarithm of. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def ln(e: Column): Column = Column.fn("ln", e)

  /**
   * Computes the natural logarithm of the given value.
   *
   * @param e
   *   The value to compute the natural logarithm of. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def log(e: Column): Column = ln(e)

  /**
   * Computes the natural logarithm of the given column.
   *
   * @param columnName
   *   The name of the column to compute the natural logarithm of. A column that evaluates to a
   *   numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def log(columnName: String): Column = log(Column(columnName))

  /**
   * Returns the first argument-base logarithm of the second argument.
   *
   * @param base
   *   The base of the logarithm. A column that evaluates to a numeric. Must be a constant.
   * @param a
   *   The value to compute the logarithm of. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def log(base: Double, a: Column): Column = Column.fn("log", lit(base), a)

  /**
   * Returns the first argument-base logarithm of the second argument.
   *
   * @param base
   *   The base of the logarithm. A column that evaluates to a numeric. Must be a constant.
   * @param columnName
   *   The name of the column to compute the logarithm of. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def log(base: Double, columnName: String): Column = log(base, Column(columnName))

  /**
   * Computes the logarithm of the given value in base 10.
   *
   * @param e
   *   The value to compute the base-10 logarithm of. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def log10(e: Column): Column = Column.fn("log10", e)

  /**
   * Computes the logarithm of the given value in base 10.
   *
   * @param columnName
   *   The name of the column to compute the base-10 logarithm of. A column that evaluates to a
   *   numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def log10(columnName: String): Column = log10(Column(columnName))

  /**
   * Computes the natural logarithm of the given value plus one.
   *
   * @param e
   *   The value to compute the natural logarithm of the value plus one. A column that evaluates
   *   to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def log1p(e: Column): Column = Column.fn("log1p", e)

  /**
   * Computes the natural logarithm of the given column plus one.
   *
   * @param columnName
   *   The name of the column to compute the natural logarithm of the value plus one. A column
   *   that evaluates to a numeric.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def log1p(columnName: String): Column = log1p(Column(columnName))

  /**
   * Computes the logarithm of the given column in base 2.
   *
   * @param expr
   *   The value to compute the base-2 logarithm of. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def log2(expr: Column): Column = Column.fn("log2", expr)

  /**
   * Computes the logarithm of the given value in base 2.
   *
   * @param columnName
   *   a column to calculate logarithm for. A column that evaluates to a double.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def log2(columnName: String): Column = log2(Column(columnName))

  /**
   * Returns the negated value.
   *
   * @param e
   *   column to calculate negative value for. A column that evaluates to a numeric or interval.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def negative(e: Column): Column = Column.fn("negative", e)

  /**
   * Returns Pi.
   *
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def pi(): Column = Column.fn("pi")

  /**
   * Returns the value.
   *
   * @param e
   *   input value column. A column that evaluates to a numeric or interval.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def positive(e: Column): Column = Column.fn("positive", e)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @param l
   *   the base number. A column that evaluates to a double.
   * @param r
   *   the exponent number. A column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def pow(l: Column, r: Column): Column = Column.fn("power", l, r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @param l
   *   the base number. A column that evaluates to a double.
   * @param rightName
   *   the exponent number. A column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def pow(l: Column, rightName: String): Column = pow(l, Column(rightName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @param leftName
   *   the base number. A column that evaluates to a double.
   * @param r
   *   the exponent number. A column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def pow(leftName: String, r: Column): Column = pow(Column(leftName), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @param leftName
   *   the base number.
   * @param rightName
   *   the exponent number.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def pow(leftName: String, rightName: String): Column = pow(Column(leftName), Column(rightName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @param l
   *   the base number. A column that evaluates to a double.
   * @param r
   *   the exponent number. A column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def pow(l: Column, r: Double): Column = pow(l, lit(r))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @param leftName
   *   the base number.
   * @param r
   *   the exponent number.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def pow(leftName: String, r: Double): Column = pow(Column(leftName), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @param l
   *   the base number. A column that evaluates to a double.
   * @param r
   *   the exponent number. A column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def pow(l: Double, r: Column): Column = pow(lit(l), r)

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @param l
   *   the base number.
   * @param rightName
   *   the exponent number.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def pow(l: Double, rightName: String): Column = pow(l, Column(rightName))

  /**
   * Returns the value of the first argument raised to the power of the second argument.
   *
   * @param l
   *   the base number. A column that evaluates to a double.
   * @param r
   *   the exponent number. A column that evaluates to a double.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def power(l: Column, r: Column): Column = Column.fn("power", l, r)

  /**
   * Returns the positive value of dividend mod divisor.
   *
   * @param dividend
   *   the column that contains dividend, or the specified dividend value. A column that evaluates
   *   to a numeric.
   * @param divisor
   *   the column that contains divisor, or the specified divisor value. A column that evaluates
   *   to a numeric.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def pmod(dividend: Column, divisor: Column): Column = Column.fn("pmod", dividend, divisor)

  /**
   * Returns the double value that is closest in value to the argument and is equal to a
   * mathematical integer.
   *
   * @param e
   *   target column to compute on. A column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def rint(e: Column): Column = Column.fn("rint", e)

  /**
   * Returns the double value that is closest in value to the argument and is equal to a
   * mathematical integer.
   *
   * @param columnName
   *   the numeric column name to round to the closest integer.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def rint(columnName: String): Column = rint(Column(columnName))

  /**
   * Returns the value of the column `e` rounded to 0 decimal places with HALF_UP round mode.
   *
   * @param e
   *   the value to round. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def round(e: Column): Column = round(e, 0)

  /**
   * Round the value of `e` to `scale` decimal places with HALF_UP round mode if `scale` is
   * greater than or equal to 0 or at integral part when `scale` is less than 0.
   *
   * @param e
   *   the value to round. A column that evaluates to a numeric.
   * @param scale
   *   the number of decimal places to round to. A column that evaluates to an integral. Must be a
   *   constant.
   * @group math_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def round(e: Column, scale: Int): Column = Column.fn("round", e, lit(scale))

  /**
   * Round the value of `e` to `scale` decimal places with HALF_UP round mode if `scale` is
   * greater than or equal to 0 or at integral part when `scale` is less than 0.
   *
   * @param e
   *   the value to round. A column that evaluates to a numeric.
   * @param scale
   *   the number of decimal places to round to. A column that evaluates to an integral. Must be a
   *   constant.
   * @group math_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def round(e: Column, scale: Column): Column = Column.fn("round", e, scale)

  /**
   * Returns the value of the column `e` rounded to 0 decimal places with HALF_EVEN round mode.
   *
   * @param e
   *   the value to round. A column that evaluates to a numeric.
   * @group math_funcs
   * @since 2.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def bround(e: Column): Column = bround(e, 0)

  /**
   * Round the value of `e` to `scale` decimal places with HALF_EVEN round mode if `scale` is
   * greater than or equal to 0 or at integral part when `scale` is less than 0.
   *
   * @param e
   *   the value to round. A column that evaluates to a numeric.
   * @param scale
   *   the number of decimal places to round to. A column that evaluates to an integral. Must be a
   *   constant.
   * @group math_funcs
   * @since 2.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def bround(e: Column, scale: Int): Column = Column.fn("bround", e, lit(scale))

  /**
   * Round the value of `e` to `scale` decimal places with HALF_EVEN round mode if `scale` is
   * greater than or equal to 0 or at integral part when `scale` is less than 0.
   *
   * @param e
   *   the value to round. A column that evaluates to a numeric.
   * @param scale
   *   the number of decimal places to round to. A column that evaluates to an integral. Must be a
   *   constant.
   * @group math_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def bround(e: Column, scale: Column): Column = Column.fn("bround", e, scale)

  /**
   * @param e
   *   angle in radians. A column that evaluates to a double.
   * @return
   *   secant of the angle. Returns a column that evaluates to a double.
   *
   * @group math_funcs
   * @since 3.3.0
   */
  def sec(e: Column): Column = Column.fn("sec", e)

  /**
   * Shift the given value numBits left. If the given value is a long value, this function will
   * return a long value else it will return an integer value.
   *
   * @group bitwise_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  @deprecated("Use shiftleft", "3.2.0")
  def shiftLeft(e: Column, numBits: Int): Column = shiftleft(e, numBits)

  /**
   * Shift the given value numBits left. If the given value is a long value, this function will
   * return a long value else it will return an integer value.
   *
   * @param e
   *   the value to shift. A column that evaluates to an integral.
   * @param numBits
   *   the number of bits to shift left. A column that evaluates to an integral. Must be a
   *   constant.
   * @group bitwise_funcs
   * @since 3.2.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def shiftleft(e: Column, numBits: Int): Column = Column.fn("shiftleft", e, lit(numBits))

  /**
   * (Signed) shift the given value numBits right. If the given value is a long value, it will
   * return a long value else it will return an integer value.
   *
   * @group bitwise_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  @deprecated("Use shiftright", "3.2.0")
  def shiftRight(e: Column, numBits: Int): Column = shiftright(e, numBits)

  /**
   * (Signed) shift the given value numBits right. If the given value is a long value, it will
   * return a long value else it will return an integer value.
   *
   * @param e
   *   the value to shift. A column that evaluates to an integral.
   * @param numBits
   *   the number of bits to shift right. A column that evaluates to an integral. Must be a
   *   constant.
   * @group bitwise_funcs
   * @since 3.2.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def shiftright(e: Column, numBits: Int): Column = Column.fn("shiftright", e, lit(numBits))

  /**
   * Unsigned shift the given value numBits right. If the given value is a long value, it will
   * return a long value else it will return an integer value.
   *
   * @group bitwise_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  @deprecated("Use shiftrightunsigned", "3.2.0")
  def shiftRightUnsigned(e: Column, numBits: Int): Column = shiftrightunsigned(e, numBits)

  /**
   * Unsigned shift the given value numBits right. If the given value is a long value, it will
   * return a long value else it will return an integer value.
   *
   * @param e
   *   the value to shift. A column that evaluates to an integral.
   * @param numBits
   *   the number of bits to shift right. A column that evaluates to an integral. Must be a
   *   constant.
   * @group bitwise_funcs
   * @since 3.2.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def shiftrightunsigned(e: Column, numBits: Int): Column =
    Column.fn("shiftrightunsigned", e, lit(numBits))

  /**
   * Computes the signum of the given value.
   *
   * @param e
   *   the value to compute the signum of. A column that evaluates to a numeric or interval.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def sign(e: Column): Column = Column.fn("sign", e)

  /**
   * Computes the signum of the given value.
   *
   * @param e
   *   the value to compute the signum of. A column that evaluates to a numeric or interval.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def signum(e: Column): Column = Column.fn("signum", e)

  /**
   * Computes the signum of the given column.
   *
   * @param columnName
   *   column to compute the signum on. A column that evaluates to a numeric or interval.
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def signum(columnName: String): Column = signum(Column(columnName))

  /**
   * @param e
   *   angle in radians. A column that evaluates to a double.
   * @return
   *   sine of the angle, as if computed by `java.lang.Math.sin`. Returns a column that evaluates
   *   to a double.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def sin(e: Column): Column = Column.fn("sin", e)

  /**
   * @param columnName
   *   angle in radians. A column that evaluates to a double.
   * @return
   *   sine of the angle, as if computed by `java.lang.Math.sin`. Returns a column that evaluates
   *   to a double.
   * @group math_funcs
   * @since 1.4.0
   */
  def sin(columnName: String): Column = sin(Column(columnName))

  /**
   * @param e
   *   hyperbolic angle. A column that evaluates to a double.
   * @return
   *   hyperbolic sine of the given value, as if computed by `java.lang.Math.sinh`. Returns a
   *   column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   */
  def sinh(e: Column): Column = Column.fn("sinh", e)

  /**
   * @param columnName
   *   hyperbolic angle. A column that evaluates to a double.
   * @return
   *   hyperbolic sine of the given value, as if computed by `java.lang.Math.sinh`. Returns a
   *   column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   */
  def sinh(columnName: String): Column = sinh(Column(columnName))

  /**
   * @param e
   *   angle in radians. A column that evaluates to a double.
   * @return
   *   tangent of the given value, as if computed by `java.lang.Math.tan`. Returns a column that
   *   evaluates to a double.
   *
   * @group math_funcs
   * @since 1.4.0
   */
  def tan(e: Column): Column = Column.fn("tan", e)

  /**
   * @param columnName
   *   angle in radians. A column that evaluates to a double.
   * @return
   *   tangent of the given value, as if computed by `java.lang.Math.tan`. Returns a column that
   *   evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   */
  def tan(columnName: String): Column = tan(Column(columnName))

  /**
   * @param e
   *   hyperbolic angle. A column that evaluates to a double.
   * @return
   *   hyperbolic tangent of the given value, as if computed by `java.lang.Math.tanh`. Returns a
   *   column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   */
  def tanh(e: Column): Column = Column.fn("tanh", e)

  /**
   * @param columnName
   *   hyperbolic angle. A column that evaluates to a double.
   * @return
   *   hyperbolic tangent of the given value, as if computed by `java.lang.Math.tanh`. Returns a
   *   column that evaluates to a double.
   * @group math_funcs
   * @since 1.4.0
   */
  def tanh(columnName: String): Column = tanh(Column(columnName))

  /**
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  @deprecated("Use degrees", "2.1.0")
  def toDegrees(e: Column): Column = degrees(e)

  /**
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  @deprecated("Use degrees", "2.1.0")
  def toDegrees(columnName: String): Column = degrees(Column(columnName))

  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in
   * degrees.
   *
   * @param e
   *   angle in radians. A column that evaluates to a double.
   * @return
   *   angle in degrees, as if computed by `java.lang.Math.toDegrees`. Returns a column that
   *   evaluates to a double.
   *
   * @group math_funcs
   * @since 2.1.0
   */
  def degrees(e: Column): Column = Column.fn("degrees", e)

  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in
   * degrees.
   *
   * @param columnName
   *   angle in radians. A column that evaluates to a double.
   * @return
   *   angle in degrees, as if computed by `java.lang.Math.toDegrees`. Returns a column that
   *   evaluates to a double.
   * @group math_funcs
   * @since 2.1.0
   */
  def degrees(columnName: String): Column = degrees(Column(columnName))

  /**
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  @deprecated("Use radians", "2.1.0")
  def toRadians(e: Column): Column = radians(e)

  /**
   * @group math_funcs
   * @since 1.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  @deprecated("Use radians", "2.1.0")
  def toRadians(columnName: String): Column = radians(Column(columnName))

  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in
   * radians.
   *
   * @param e
   *   angle in degrees. A column that evaluates to a double.
   * @return
   *   angle in radians, as if computed by `java.lang.Math.toRadians`. Returns a column that
   *   evaluates to a double.
   *
   * @group math_funcs
   * @since 2.1.0
   */
  def radians(e: Column): Column = Column.fn("radians", e)

  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in
   * radians.
   *
   * @param columnName
   *   angle in degrees. A column that evaluates to a double.
   * @return
   *   angle in radians, as if computed by `java.lang.Math.toRadians`. Returns a column that
   *   evaluates to a double.
   * @group math_funcs
   * @since 2.1.0
   */
  def radians(columnName: String): Column = radians(Column(columnName))

  /**
   * Returns the bucket number into which the value of this expression would fall after being
   * evaluated. Note that input arguments must follow conditions listed below; otherwise, the
   * method will return null.
   *
   * @param v
   *   value to compute a bucket number in the histogram. A column that evaluates to a double or
   *   interval.
   * @param min
   *   minimum value of the histogram. A column that evaluates to a double or interval.
   * @param max
   *   maximum value of the histogram. A column that evaluates to a double or interval.
   * @param numBucket
   *   the number of buckets. A column that evaluates to a long.
   * @return
   *   the bucket number into which the value would fall after being evaluated. Returns a column
   *   that evaluates to a long.
   * @group math_funcs
   * @since 3.5.0
   */
  def width_bucket(v: Column, min: Column, max: Column, numBucket: Column): Column =
    Column.fn("width_bucket", v, min, max, numBucket)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Misc functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the current catalog.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def current_catalog(): Column = Column.fn("current_catalog")

  /**
   * Returns the current database.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def current_database(): Column = Column.fn("current_database")

  /**
   * Returns the current schema.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def current_schema(): Column = Column.fn("current_schema")

  /**
   * Returns the current SQL path as a comma-separated list of qualified schema names.
   *
   * @group misc_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def current_path(): Column = Column.fn("current_path")

  /**
   * Returns the user name of current execution context.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def current_user(): Column = Column.fn("current_user")

  /**
   * Calculates the MD5 digest of a binary column and returns the value as a 32 character hex
   * string.
   *
   * @param e
   *   target column to compute on. A column that evaluates to a binary.
   * @group hash_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def md5(e: Column): Column = Column.fn("md5", e)

  /**
   * Calculates the SHA-1 digest of a binary column and returns the value as a 40 character hex
   * string.
   *
   * @param e
   *   target column to compute on. A column that evaluates to a binary.
   * @group hash_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def sha1(e: Column): Column = Column.fn("sha1", e)

  /**
   * Calculates the SHA-2 family of hash functions of a binary column and returns the value as a
   * hex string.
   *
   * @param e
   *   column to compute SHA-2 on. A column that evaluates to a binary.
   * @param numBits
   *   one of 224, 256, 384, or 512. A column that evaluates to an integer.
   *
   * @group hash_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def sha2(e: Column, numBits: Int): Column = {
    require(
      Seq(0, 224, 256, 384, 512).contains(numBits),
      s"numBits $numBits is not in the permitted values (0, 224, 256, 384, 512)")
    Column.fn("sha2", e, lit(numBits))
  }

  /**
   * Calculates the cyclic redundancy check value (CRC32) of a binary column and returns the value
   * as a bigint.
   *
   * @param e
   *   target column to compute on. A column that evaluates to a binary.
   * @group hash_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def crc32(e: Column): Column = Column.fn("crc32", e)

  /**
   * Calculates the hash code of given columns, and returns the result as an int column.
   *
   * @param cols
   *   one or more columns to compute on. A column of any type.
   * @group hash_funcs
   * @since 2.0.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  @scala.annotation.varargs
  def hash(cols: Column*): Column = Column.fn("hash", cols: _*)

  /**
   * Calculates the hash code of given columns using the 64-bit variant of the xxHash algorithm,
   * and returns the result as a long column. The hash computation uses an initial seed of 42.
   *
   * @param cols
   *   one or more columns to compute on. A column of any type.
   * @group hash_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  @scala.annotation.varargs
  def xxhash64(cols: Column*): Column = Column.fn("xxhash64", cols: _*)

  /**
   * Returns null if the condition is true, and throws an exception otherwise.
   *
   * @param c
   *   The condition to check. A column that evaluates to a boolean.
   * @group misc_funcs
   * @since 3.1.0
   * @return
   *   Returns a column that always evaluates to NULL.
   */
  def assert_true(c: Column): Column = Column.fn("assert_true", c)

  /**
   * Returns null if the condition is true; throws an exception with the error message otherwise.
   *
   * @param c
   *   The condition to check. A column that evaluates to a boolean.
   * @param e
   *   The error message to throw. A column that evaluates to a string.
   * @group misc_funcs
   * @since 3.1.0
   * @return
   *   Returns a column that always evaluates to NULL.
   */
  def assert_true(c: Column, e: Column): Column = Column.fn("assert_true", c, e)

  /**
   * Throws an exception with the provided error message.
   *
   * @param c
   *   The error message to throw. A column that evaluates to a string.
   * @group misc_funcs
   * @since 3.1.0
   * @return
   *   Returns a column that always evaluates to NULL.
   */
  def raise_error(c: Column): Column = Column.fn("raise_error", c)

  /**
   * Returns the user name of current execution context.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def user(): Column = Column.fn("user")

  /**
   * Returns the user name of current execution context.
   *
   * @group misc_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def session_user(): Column = Column.fn("session_user")

  /**
   * Returns an universally unique identifier (UUID) string. The value is returned as a canonical
   * UUID 36-character string.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def uuid(): Column = Column.fn("uuid", lit(SparkClassUtils.random.nextLong))

  /**
   * Returns an universally unique identifier (UUID) string. The value is returned as a canonical
   * UUID 36-character string.
   *
   * @param seed
   *   The random number seed to use. A column that evaluates to an integral. Must be a constant.
   * @group misc_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def uuid(seed: Column): Column = Column.fn("uuid", seed)

  /**
   * Returns the keyed-hash message authentication code (HMAC) of `message` using `key` and the
   * given hash `algorithm`. The result is returned as raw MAC bytes; wrap it with `hex` or
   * `base64` for a textual value.
   *
   * @param key
   *   The secret key, as a binary value.
   * @param message
   *   The message to authenticate, as a binary value.
   * @param algorithm
   *   The hash algorithm. Valid values: SHA-224, SHA-256, SHA-384, SHA-512, SHA-1, MD5.
   *
   * @group misc_funcs
   * @since 4.3.0
   */
  def hmac(key: Column, message: Column, algorithm: Column): Column =
    Column.fn("hmac", key, message, algorithm)

  /**
   * Returns the keyed-hash message authentication code (HMAC) of `message` using `key` and
   * SHA-256. The result is returned as raw MAC bytes; wrap it with `hex` or `base64` for a
   * textual value. To use a different algorithm, call the three-argument overload.
   *
   * @param key
   *   The secret key, as a binary value.
   * @param message
   *   The message to authenticate, as a binary value.
   *
   * @group misc_funcs
   * @since 4.3.0
   */
  def hmac(key: Column, message: Column): Column =
    Column.fn("hmac", key, message)

  /**
   * Returns an encrypted value of `input` using AES in given `mode` with the specified `padding`.
   * Key lengths of 16, 24 and 32 bits are supported. Supported combinations of (`mode`,
   * `padding`) are ('ECB', 'PKCS'), ('GCM', 'NONE') and ('CBC', 'PKCS'). Optional initialization
   * vectors (IVs) are only supported for CBC and GCM modes. These must be 16 bytes for CBC and 12
   * bytes for GCM. If not provided, a random vector will be generated and prepended to the
   * output. Optional additional authenticated data (AAD) is only supported for GCM. If provided
   * for encryption, the identical AAD value must be provided for decryption. The default mode is
   * GCM.
   *
   * @param input
   *   The binary value to encrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to encrypt the data. A column that evaluates to a binary.
   * @param mode
   *   Specifies which block cipher mode should be used to encrypt messages. Valid modes: ECB,
   *   GCM, CBC. A column that evaluates to a string.
   * @param padding
   *   Specifies how to pad messages whose length is not a multiple of the block size. Valid
   *   values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB, NONE for GCM and PKCS
   *   for CBC. A column that evaluates to a string.
   * @param iv
   *   Optional initialization vector. Only supported for CBC and GCM modes. Valid values: None or
   *   "". 16-byte array for CBC mode. 12-byte array for GCM mode. A column that evaluates to a
   *   binary.
   * @param aad
   *   Optional additional authenticated data. Only supported for GCM mode. This can be any
   *   free-form input and must be provided for both encryption and decryption. A column that
   *   evaluates to a binary.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def aes_encrypt(
      input: Column,
      key: Column,
      mode: Column,
      padding: Column,
      iv: Column,
      aad: Column): Column = Column.fn("aes_encrypt", input, key, mode, padding, iv, aad)

  /**
   * Returns an encrypted value of `input`.
   *
   * @param input
   *   The binary value to encrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to encrypt the data. A column that evaluates to a binary.
   * @param mode
   *   Specifies which block cipher mode should be used to encrypt messages. Valid modes: ECB,
   *   GCM, CBC. A column that evaluates to a string.
   * @param padding
   *   Specifies how to pad messages whose length is not a multiple of the block size. Valid
   *   values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB, NONE for GCM and PKCS
   *   for CBC. A column that evaluates to a string.
   * @param iv
   *   Optional initialization vector. Only supported for CBC and GCM modes. Valid values: None or
   *   "". 16-byte array for CBC mode. 12-byte array for GCM mode. A column that evaluates to a
   *   binary.
   * @see
   *   `org.apache.spark.sql.functions.aes_encrypt(Column, Column, Column, Column, Column,
   *   Column)`
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def aes_encrypt(input: Column, key: Column, mode: Column, padding: Column, iv: Column): Column =
    Column.fn("aes_encrypt", input, key, mode, padding, iv)

  /**
   * Returns an encrypted value of `input`.
   *
   * @param input
   *   The binary value to encrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to encrypt the data. A column that evaluates to a binary.
   * @param mode
   *   Specifies which block cipher mode should be used to encrypt messages. Valid modes: ECB,
   *   GCM, CBC. A column that evaluates to a string.
   * @param padding
   *   Specifies how to pad messages whose length is not a multiple of the block size. Valid
   *   values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB, NONE for GCM and PKCS
   *   for CBC. A column that evaluates to a string.
   * @see
   *   `org.apache.spark.sql.functions.aes_encrypt(Column, Column, Column, Column, Column,
   *   Column)`
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def aes_encrypt(input: Column, key: Column, mode: Column, padding: Column): Column =
    Column.fn("aes_encrypt", input, key, mode, padding)

  /**
   * Returns an encrypted value of `input`.
   *
   * @param input
   *   The binary value to encrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to encrypt the data. A column that evaluates to a binary.
   * @param mode
   *   Specifies which block cipher mode should be used to encrypt messages. Valid modes: ECB,
   *   GCM, CBC. A column that evaluates to a string.
   * @see
   *   `org.apache.spark.sql.functions.aes_encrypt(Column, Column, Column, Column, Column,
   *   Column)`
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def aes_encrypt(input: Column, key: Column, mode: Column): Column =
    Column.fn("aes_encrypt", input, key, mode)

  /**
   * Returns an encrypted value of `input`.
   *
   * @param input
   *   The binary value to encrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to encrypt the data. A column that evaluates to a binary.
   * @see
   *   `org.apache.spark.sql.functions.aes_encrypt(Column, Column, Column, Column, Column,
   *   Column)`
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def aes_encrypt(input: Column, key: Column): Column =
    Column.fn("aes_encrypt", input, key)

  /**
   * Returns a decrypted value of `input` using AES in `mode` with `padding`. Key lengths of 16,
   * 24 and 32 bits are supported. Supported combinations of (`mode`, `padding`) are ('ECB',
   * 'PKCS'), ('GCM', 'NONE') and ('CBC', 'PKCS'). Optional additional authenticated data (AAD) is
   * only supported for GCM. If provided for encryption, the identical AAD value must be provided
   * for decryption. The default mode is GCM.
   *
   * @param input
   *   The binary value to decrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to decrypt the data. A column that evaluates to a binary.
   * @param mode
   *   Specifies which block cipher mode should be used to decrypt messages. Valid modes: ECB,
   *   GCM, CBC. A column that evaluates to a string.
   * @param padding
   *   Specifies how to pad messages whose length is not a multiple of the block size. Valid
   *   values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB, NONE for GCM and PKCS
   *   for CBC. A column that evaluates to a string.
   * @param aad
   *   Optional additional authenticated data. Only supported for GCM mode. This can be any
   *   free-form input and must be provided for both encryption and decryption. A column that
   *   evaluates to a binary.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def aes_decrypt(
      input: Column,
      key: Column,
      mode: Column,
      padding: Column,
      aad: Column): Column =
    Column.fn("aes_decrypt", input, key, mode, padding, aad)

  /**
   * Returns a decrypted value of `input`.
   *
   * @param input
   *   The binary value to decrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to decrypt the data. A column that evaluates to a binary.
   * @param mode
   *   Specifies which block cipher mode should be used to decrypt messages. Valid modes: ECB,
   *   GCM, CBC. A column that evaluates to a string.
   * @param padding
   *   Specifies how to pad messages whose length is not a multiple of the block size. Valid
   *   values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB, NONE for GCM and PKCS
   *   for CBC. A column that evaluates to a string.
   * @see
   *   `org.apache.spark.sql.functions.aes_decrypt(Column, Column, Column, Column, Column)`
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def aes_decrypt(input: Column, key: Column, mode: Column, padding: Column): Column =
    Column.fn("aes_decrypt", input, key, mode, padding)

  /**
   * Returns a decrypted value of `input`.
   *
   * @param input
   *   The binary value to decrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to decrypt the data. A column that evaluates to a binary.
   * @param mode
   *   Specifies which block cipher mode should be used to decrypt messages. Valid modes: ECB,
   *   GCM, CBC. A column that evaluates to a string.
   * @see
   *   `org.apache.spark.sql.functions.aes_decrypt(Column, Column, Column, Column, Column)`
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def aes_decrypt(input: Column, key: Column, mode: Column): Column =
    Column.fn("aes_decrypt", input, key, mode)

  /**
   * Returns a decrypted value of `input`.
   *
   * @param input
   *   The binary value to decrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to decrypt the data. A column that evaluates to a binary.
   * @see
   *   `org.apache.spark.sql.functions.aes_decrypt(Column, Column, Column, Column, Column)`
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def aes_decrypt(input: Column, key: Column): Column =
    Column.fn("aes_decrypt", input, key)

  /**
   * This is a special version of `aes_decrypt` that performs the same operation, but returns a
   * NULL value instead of raising an error if the decryption cannot be performed.
   *
   * @param input
   *   The binary value to decrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to decrypt the data. A column that evaluates to a binary.
   * @param mode
   *   Specifies which block cipher mode should be used to decrypt messages. Valid modes: ECB,
   *   GCM, CBC. A column that evaluates to a string.
   * @param padding
   *   Specifies how to pad messages whose length is not a multiple of the block size. Valid
   *   values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB, NONE for GCM and PKCS
   *   for CBC. A column that evaluates to a string.
   * @param aad
   *   Optional additional authenticated data. Only supported for GCM mode. This can be any
   *   free-form input and must be provided for both encryption and decryption. A column that
   *   evaluates to a binary.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def try_aes_decrypt(
      input: Column,
      key: Column,
      mode: Column,
      padding: Column,
      aad: Column): Column =
    Column.fn("try_aes_decrypt", input, key, mode, padding, aad)

  /**
   * Returns a decrypted value of `input`.
   *
   * @param input
   *   The binary value to decrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to decrypt the data. A column that evaluates to a binary.
   * @param mode
   *   Specifies which block cipher mode should be used to decrypt messages. Valid modes: ECB,
   *   GCM, CBC. A column that evaluates to a string.
   * @param padding
   *   Specifies how to pad messages whose length is not a multiple of the block size. Valid
   *   values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB, NONE for GCM and PKCS
   *   for CBC. A column that evaluates to a string.
   * @see
   *   `org.apache.spark.sql.functions.try_aes_decrypt(Column, Column, Column, Column, Column)`
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def try_aes_decrypt(input: Column, key: Column, mode: Column, padding: Column): Column =
    Column.fn("try_aes_decrypt", input, key, mode, padding)

  /**
   * Returns a decrypted value of `input`.
   *
   * @param input
   *   The binary value to decrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to decrypt the data. A column that evaluates to a binary.
   * @param mode
   *   Specifies which block cipher mode should be used to decrypt messages. Valid modes: ECB,
   *   GCM, CBC. A column that evaluates to a string.
   * @see
   *   `org.apache.spark.sql.functions.try_aes_decrypt(Column, Column, Column, Column, Column)`
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def try_aes_decrypt(input: Column, key: Column, mode: Column): Column =
    Column.fn("try_aes_decrypt", input, key, mode)

  /**
   * Returns a decrypted value of `input`.
   *
   * @param input
   *   The binary value to decrypt. A column that evaluates to a binary.
   * @param key
   *   The passphrase to use to decrypt the data. A column that evaluates to a binary.
   * @see
   *   `org.apache.spark.sql.functions.try_aes_decrypt(Column, Column, Column, Column, Column)`
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def try_aes_decrypt(input: Column, key: Column): Column =
    Column.fn("try_aes_decrypt", input, key)

  /**
   * Returns a sha1 hash value as a hex string of the `col`.
   *
   * @param col
   *   The value to hash. A column that evaluates to a string or binary.
   * @group hash_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def sha(col: Column): Column = Column.fn("sha", col)

  /**
   * Returns the length of the block being read, or -1 if not available.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def input_file_block_length(): Column = Column.fn("input_file_block_length")

  /**
   * Returns the start offset of the block being read, or -1 if not available.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def input_file_block_start(): Column = Column.fn("input_file_block_start")

  /**
   * Calls a method with reflection.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  @scala.annotation.varargs
  def reflect(cols: Column*): Column = Column.fn("reflect", cols: _*)

  /**
   * Calls a method with reflection.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  @scala.annotation.varargs
  def java_method(cols: Column*): Column = Column.fn("java_method", cols: _*)

  /**
   * This is a special version of `reflect` that performs the same operation, but returns a NULL
   * value instead of raising an error if the invoke method thrown exception.
   *
   * @group misc_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  @scala.annotation.varargs
  def try_reflect(cols: Column*): Column = Column.fn("try_reflect", cols: _*)

  /**
   * Returns the Spark version. The string contains 2 fields, the first being a release version
   * and the second being a git revision.
   *
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def version(): Column = Column.fn("version")

  /**
   * Return DDL-formatted type string for the data type of the input.
   *
   * @param col
   *   The value whose data type is returned. A column of any type.
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def typeof(col: Column): Column = Column.fn("typeof", col)

  /**
   * Separates `col1`, ..., `colk` into `n` rows. Uses column names col0, col1, etc. by default
   * unless specified otherwise.
   *
   * @param cols
   *   The first column must be a constant integer for the number of rows, and the remaining
   *   columns are the input elements to be separated into rows.
   * @group generator_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  @scala.annotation.varargs
  def stack(cols: Column*): Column = Column.fn("stack", cols: _*)

  /**
   * Returns a random value with independent and identically distributed (i.i.d.) values with the
   * specified range of numbers. The provided numbers specifying the minimum and maximum values of
   * the range must be constant. If both of these numbers are integers, then the result will also
   * be an integer. Otherwise if one or both of these are floating-point numbers, then the result
   * will also be a floating-point number.
   *
   * @param min
   *   Minimum value in the range. A column that evaluates to a numeric. Must be a constant.
   * @param max
   *   Maximum value in the range. A column that evaluates to a numeric. Must be a constant.
   * @group math_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def uniform(min: Column, max: Column): Column =
    uniform(min, max, lit(SparkClassUtils.random.nextLong))

  /**
   * Returns a random value with independent and identically distributed (i.i.d.) values with the
   * specified range of numbers, with the chosen random seed. The provided numbers specifying the
   * minimum and maximum values of the range must be constant. If both of these numbers are
   * integers, then the result will also be an integer. Otherwise if one or both of these are
   * floating-point numbers, then the result will also be a floating-point number.
   *
   * @param min
   *   Minimum value in the range. A column that evaluates to a numeric. Must be a constant.
   * @param max
   *   Maximum value in the range. A column that evaluates to a numeric. Must be a constant.
   * @param seed
   *   Random number seed to use. A column that evaluates to an integral. Must be a constant.
   * @group math_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def uniform(min: Column, max: Column, seed: Column): Column =
    Column.fn("uniform", min, max, seed)

  /**
   * Returns a random value with independent and identically distributed (i.i.d.) uniformly
   * distributed values in [0, 1).
   *
   * @param seed
   *   Random number seed to use. A column that evaluates to an integral. Must be a constant.
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def random(seed: Column): Column = Column.fn("random", seed)

  /**
   * Returns a random value with independent and identically distributed (i.i.d.) uniformly
   * distributed values in [0, 1).
   *
   * @group math_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def random(): Column = random(lit(SparkClassUtils.random.nextLong))

  /**
   * Returns the bit position for the given input column.
   *
   * @param col
   *   The input column. A column that evaluates to an integral.
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def bitmap_bit_position(col: Column): Column =
    Column.fn("bitmap_bit_position", col)

  /**
   * Returns the bucket number for the given input column.
   *
   * @param col
   *   The input column. A column that evaluates to an integral.
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def bitmap_bucket_number(col: Column): Column =
    Column.fn("bitmap_bucket_number", col)

  /**
   * Returns a bitmap with the positions of the bits set from all the values from the input
   * column. The input column will most likely be bitmap_bit_position().
   *
   * @param col
   *   The input column will most likely be bitmap_bit_position(). A column that evaluates to an
   *   integral.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def bitmap_construct_agg(col: Column): Column =
    Column.fn("bitmap_construct_agg", col)

  /**
   * Returns the number of set bits in the input bitmap.
   *
   * @param col
   *   The input bitmap. A column that evaluates to a binary.
   * @group misc_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def bitmap_count(col: Column): Column = Column.fn("bitmap_count", col)

  /**
   * Returns a bitmap that is the bitwise OR of all of the bitmaps from the input column. The
   * input column should be bitmaps created from bitmap_construct_agg().
   *
   * @param col
   *   The input column should be bitmaps created from bitmap_construct_agg(). A column that
   *   evaluates to a binary.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def bitmap_or_agg(col: Column): Column = Column.fn("bitmap_or_agg", col)

  /**
   * Returns a bitmap that is the bitwise AND of all of the bitmaps from the input column. The
   * input column should be bitmaps created from bitmap_construct_agg().
   *
   * @param col
   *   The input column should be bitmaps created from bitmap_construct_agg(). A column that
   *   evaluates to a binary.
   * @group agg_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def bitmap_and_agg(col: Column): Column = Column.fn("bitmap_and_agg", col)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // String functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Computes the numeric value of the first character of the string column, and returns the
   * result as an int column.
   *
   * @param e
   *   The target column to work on. A column that evaluates to a string.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def ascii(e: Column): Column = Column.fn("ascii", e)

  /**
   * Computes the BASE64 encoding of a binary column and returns it as a string column. This is
   * the reverse of unbase64.
   *
   * @param e
   *   The target column to work on. A column that evaluates to a binary.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def base64(e: Column): Column = Column.fn("base64", e)

  /**
   * Calculates the bit length for the specified string column.
   *
   * @param e
   *   The source column or strings. A column that evaluates to a string or binary.
   * @group string_funcs
   * @since 3.3.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def bit_length(e: Column): Column = Column.fn("bit_length", e)

  /**
   * Concatenates multiple input string columns together into a single string column, using the
   * given separator.
   *
   * @param sep
   *   The words separator. A column that evaluates to a string. Must be a constant.
   * @param exprs
   *   The list of columns to work on. Each a column that evaluates to a string or an array of
   *   strings.
   * @note
   *   Input strings which are null are skipped.
   *
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  @scala.annotation.varargs
  def concat_ws(sep: String, exprs: Column*): Column =
    Column.fn("concat_ws", lit(sep) +: exprs: _*)

  /**
   * Computes the first argument into a string from a binary using the provided character set (one
   * of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16', 'UTF-32'). If either
   * argument is null, the result will also be null.
   *
   * @param value
   *   The target column to work on. A column that evaluates to a binary.
   * @param charset
   *   The charset to use to decode to. A column that evaluates to a string. Must be a constant.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def decode(value: Column, charset: String): Column =
    Column.fn("decode", value, lit(charset))

  /**
   * Computes the first argument into a binary from a string using the provided character set (one
   * of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16', 'UTF-32'). If either
   * argument is null, the result will also be null.
   *
   * @param value
   *   The target column to work on. A column that evaluates to a string.
   * @param charset
   *   The charset to use to encode. A column that evaluates to a string. Must be a constant.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def encode(value: Column, charset: String): Column =
    Column.fn("encode", value, lit(charset))

  /**
   * Returns true if the input is a valid UTF-8 string, otherwise returns false.
   *
   * @param str
   *   A column of strings, each representing a UTF-8 byte sequence. A column that evaluates to a
   *   string.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def is_valid_utf8(str: Column): Column =
    Column.fn("is_valid_utf8", str)

  /**
   * Returns a new string in which all invalid UTF-8 byte sequences, if any, are replaced by the
   * Unicode replacement character (U+FFFD).
   *
   * @param str
   *   A column of strings, each representing a UTF-8 byte sequence. A column that evaluates to a
   *   string.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def make_valid_utf8(str: Column): Column =
    Column.fn("make_valid_utf8", str)

  /**
   * Returns the input value if it corresponds to a valid UTF-8 string, or emits a
   * SparkIllegalArgumentException exception otherwise.
   *
   * @param str
   *   A column of strings, each representing a UTF-8 byte sequence. A column that evaluates to a
   *   string.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def validate_utf8(str: Column): Column =
    Column.fn("validate_utf8", str)

  /**
   * Returns the input value if it corresponds to a valid UTF-8 string, or NULL otherwise.
   *
   * @param str
   *   the input value. A column that evaluates to a string.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def try_validate_utf8(str: Column): Column =
    Column.fn("try_validate_utf8", str)

  /**
   * Formats numeric column x to a format like '#,###,###.##', rounded to d decimal places with
   * HALF_EVEN round mode, and returns the result as a string column.
   *
   * If d is 0, the result has no decimal point or fractional part. If d is less than 0, the
   * result will be null.
   *
   * @param x
   *   the numeric value to be formatted. A column that evaluates to a numeric.
   * @param d
   *   the number of decimal places. A column that evaluates to an integral. Must be a constant.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def format_number(x: Column, d: Int): Column = Column.fn("format_number", x, lit(d))

  /**
   * Formats the arguments in printf-style and returns the result as a string column.
   *
   * @param format
   *   the format string that can contain embedded format tags. A column that evaluates to a
   *   string. Must be a constant.
   * @param arguments
   *   the values to be used in formatting. Columns that evaluate to any type.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  @scala.annotation.varargs
  def format_string(format: String, arguments: Column*): Column =
    Column.fn("format_string", lit(format) +: arguments: _*)

  /**
   * Returns a new string column by converting the first letter of each word to uppercase. Words
   * are delimited by whitespace.
   *
   * For example, "hello world" will become "Hello World".
   *
   * @param e
   *   the target column to work on. A column that evaluates to a string.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def initcap(e: Column): Column = Column.fn("initcap", e)

  /**
   * Locate the position of the first occurrence of substr column in the given string. Returns
   * null if either of the arguments are null.
   *
   * @param str
   *   the string to search in. A column that evaluates to a string.
   * @param substring
   *   the substring to search for. A column that evaluates to a string. Must be a constant.
   * @note
   *   The position is not zero based, but 1 based index. Returns 0 if substr could not be found
   *   in str.
   *
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def instr(str: Column, substring: String): Column = instr(str, lit(substring))

  /**
   * Locate the position of the first occurrence of substr column in the given string. Returns
   * null if either of the arguments are null.
   *
   * @param str
   *   the string to search in. A column that evaluates to a string.
   * @param substring
   *   the substring to search for. A column that evaluates to a string.
   * @note
   *   The position is not zero based, but 1 based index. Returns 0 if substr could not be found
   *   in str.
   *
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def instr(str: Column, substring: Column): Column = Column.fn("instr", str, substring)

  /**
   * Locate the position of the first occurrence of `substring` in `str`, starting the search from
   * position `start`. Returns null if either of the arguments are null.
   *
   * @param str
   *   the string to search in. A column that evaluates to a string.
   * @param substring
   *   the substring to search for. A column that evaluates to a string.
   * @param start
   *   the position to start the search from. A column that evaluates to an integral. Must be a
   *   constant.
   * @note
   *   The position is not zero based, but 1 based index. Returns 0 if substr could not be found
   *   in str.
   * @note
   *   If `start` is positive, the search proceeds forward. If `start` is negative, the search
   *   proceeds backward from the end of the string. If `start` is 0, returns 0.
   *
   * @group string_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def instr(str: Column, substring: Column, start: Int): Column =
    Column.fn("instr", str, substring, lit(start))

  /**
   * Locate the position of the first occurrence of `substring` in `str`, starting the search from
   * position `start`. Returns null if either of the arguments are null.
   *
   * @param str
   *   the string to search in. A column that evaluates to a string.
   * @param substring
   *   the substring to search for. A column that evaluates to a string.
   * @param start
   *   the position to start the search from. A column that evaluates to an integral.
   * @note
   *   The position is not zero based, but 1 based index. Returns 0 if substr could not be found
   *   in str.
   * @note
   *   If `start` is positive, the search proceeds forward. If `start` is negative, the search
   *   proceeds backward from the end of the string. If `start` is 0, returns 0.
   *
   * @group string_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def instr(str: Column, substring: Column, start: Column): Column =
    Column.fn("instr", str, substring, start)

  /**
   * Locate the position of the `occurrence`-th occurrence of `substring` in `str`, starting the
   * search from position `start`. Returns null if either of the arguments are null.
   *
   * @param str
   *   the string to search in. A column that evaluates to a string.
   * @param substring
   *   the substring to search for. A column that evaluates to a string.
   * @param start
   *   the position to start the search from. A column that evaluates to an integral. Must be a
   *   constant.
   * @param occurrence
   *   which occurrence of the substring to locate. A column that evaluates to an integral. Must
   *   be a constant.
   * @note
   *   The position is not zero based, but 1 based index. Returns 0 if substr could not be found
   *   in str.
   * @note
   *   If `start` is positive, the search proceeds forward. If `start` is negative, the search
   *   proceeds backward from the end of the string. If `start` is 0, returns 0.
   * @note
   *   The `occurrence` parameter must be a positive integer.
   *
   * @group string_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def instr(str: Column, substring: Column, start: Int, occurrence: Int): Column =
    Column.fn("instr", str, substring, lit(start), lit(occurrence))

  /**
   * Locate the position of the `occurrence`-th occurrence of `substring` in `str`, starting the
   * search from position `start`. Returns null if either of the arguments are null.
   *
   * @param str
   *   the string to search in. A column that evaluates to a string.
   * @param substring
   *   the substring to search for. A column that evaluates to a string.
   * @param start
   *   the position to start the search from. A column that evaluates to an integral.
   * @param occurrence
   *   which occurrence of the substring to locate. A column that evaluates to an integral.
   * @note
   *   The position is not zero based, but 1 based index. Returns 0 if substr could not be found
   *   in str.
   * @note
   *   If `start` is positive, the search proceeds forward. If `start` is negative, the search
   *   proceeds backward from the end of the string. If `start` is 0, returns 0.
   * @note
   *   The `occurrence` parameter must be a positive integer.
   *
   * @group string_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def instr(str: Column, substring: Column, start: Column, occurrence: Column): Column =
    Column.fn("instr", str, substring, start, occurrence)

  /**
   * Computes the character length of a given string or number of bytes of a binary string. The
   * length of character strings include the trailing spaces. The length of binary strings
   * includes binary zeros.
   *
   * @param e
   *   the target column to work on. A column that evaluates to a string or binary.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def length(e: Column): Column = Column.fn("length", e)

  /**
   * Computes the character length of a given string or number of bytes of a binary string. The
   * length of character strings include the trailing spaces. The length of binary strings
   * includes binary zeros.
   *
   * @param e
   *   the target column to work on. A column that evaluates to a string or binary.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def len(e: Column): Column = Column.fn("len", e)

  /**
   * Converts a string column to lower case.
   *
   * @param e
   *   the target column to work on. A column that evaluates to a string.
   * @group string_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def lower(e: Column): Column = Column.fn("lower", e)

  /**
   * Computes the Levenshtein distance of the two given string columns if it's less than or equal
   * to a given threshold.
   * @param l
   *   the first input column. A column that evaluates to a string.
   * @param r
   *   the second input column. A column that evaluates to a string.
   * @param threshold
   *   the maximum distance to compute. A column that evaluates to an integral. Must be a
   *   constant.
   * @return
   *   result distance, or -1. Returns a column that evaluates to an integer.
   * @group string_funcs
   * @since 3.5.0
   */
  def levenshtein(l: Column, r: Column, threshold: Int): Column =
    Column.fn("levenshtein", l, r, lit(threshold))

  /**
   * Computes the Levenshtein distance of the two given string columns.
   * @param l
   *   the first input column. A column that evaluates to a string.
   * @param r
   *   the second input column. A column that evaluates to a string.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def levenshtein(l: Column, r: Column): Column = Column.fn("levenshtein", l, r)

  /**
   * Computes the Jaro-Winkler similarity between the two given string columns. The result is a
   * double between 0.0 (no similarity) and 1.0 (identical).
   * @param l
   *   A column that evaluates to a string.
   * @param r
   *   A column that evaluates to a string.
   * @group string_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def jaro_winkler_similarity(l: Column, r: Column): Column =
    Column.fn("jaro_winkler_similarity", l, r)

  /**
   * Locate the position of the first occurrence of substr.
   *
   * @param substr
   *   The substring to find. A column that evaluates to a string.
   * @param str
   *   A column that evaluates to a string.
   * @note
   *   The position is not zero based, but 1 based index. Returns 0 if substr could not be found
   *   in str.
   *
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def locate(substr: String, str: Column): Column = Column.fn("locate", lit(substr), str)

  /**
   * Locate the position of the first occurrence of substr in a string column, after position pos.
   *
   * @param substr
   *   The substring to find. A column that evaluates to a string.
   * @param str
   *   A column that evaluates to a string.
   * @param pos
   *   The starting position. A column that evaluates to an integer.
   * @note
   *   The position is not zero based, but 1 based index. returns 0 if substr could not be found
   *   in str.
   *
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def locate(substr: String, str: Column, pos: Int): Column =
    Column.fn("locate", lit(substr), str, lit(pos))

  /**
   * Left-pad the string column with pad to a length of len. If the string column is longer than
   * len, the return value is shortened to len characters.
   *
   * @param str
   *   A column that evaluates to a string.
   * @param len
   *   The length of the padded result. A column that evaluates to an integer.
   * @param pad
   *   The padding string. A column that evaluates to a string.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lpad(str: Column, len: Int, pad: String): Column = lpad(str, lit(len), lit(pad))

  /**
   * Left-pad the binary column with pad to a byte length of len. If the binary column is longer
   * than len, the return value is shortened to len bytes.
   *
   * @param str
   *   A column that evaluates to a binary.
   * @param len
   *   The byte length of the padded result. A column that evaluates to an integer.
   * @param pad
   *   The padding bytes. A column that evaluates to a binary.
   * @group string_funcs
   * @since 3.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lpad(str: Column, len: Int, pad: Array[Byte]): Column = lpad(str, lit(len), lit(pad))

  /**
   * Left-pad the string column with pad to a length of len. If the string column is longer than
   * len, the return value is shortened to len characters.
   *
   * @param str
   *   A column that evaluates to a string.
   * @param len
   *   The length of the padded result. A column that evaluates to an integer.
   * @param pad
   *   The padding string. A column that evaluates to a string.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def lpad(str: Column, len: Column, pad: Column): Column = Column.fn("lpad", str, len, pad)

  /**
   * Trim the spaces from left end for the specified string value.
   *
   * @param e
   *   A column that evaluates to a string.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def ltrim(e: Column): Column = Column.fn("ltrim", e)

  /**
   * Trim the specified character string from left end for the specified string column.
   * @param e
   *   A column that evaluates to a string.
   * @param trimString
   *   The trim string. A column that evaluates to a string.
   * @group string_funcs
   * @since 2.3.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def ltrim(e: Column, trimString: String): Column = ltrim(e, lit(trimString))

  /**
   * Trim the specified character string from left end for the specified string column.
   * @param e
   *   A column that evaluates to a string.
   * @param trim
   *   The trim string. A column that evaluates to a string.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def ltrim(e: Column, trim: Column): Column = Column.fn("ltrim", trim, e)

  /**
   * Calculates the byte length for the specified string column.
   *
   * @param e
   *   A column that evaluates to a string or binary.
   * @group string_funcs
   * @since 3.3.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def octet_length(e: Column): Column = Column.fn("octet_length", e)

  /**
   * Marks a given column with specified collation.
   *
   * @param e
   *   A column that evaluates to a string.
   * @param collation
   *   The collation name. A column that evaluates to a string. Must be a constant.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def collate(e: Column, collation: String): Column = Column.fn("collate", e, lit(collation))

  /**
   * Returns the collation name of a given column.
   *
   * @param e
   *   A column that evaluates to a string.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def collation(e: Column): Column = Column.fn("collation", e)

  /**
   * Returns true if `str` matches `regexp`, or false otherwise.
   *
   * @param str
   *   A column that evaluates to a string.
   * @param regexp
   *   The regular expression pattern. A column that evaluates to a string.
   * @group predicate_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def rlike(str: Column, regexp: Column): Column = Column.fn("rlike", str, regexp)

  /**
   * Returns true if `str` matches `regexp`, or false otherwise.
   *
   * @param str
   *   A column that evaluates to a string.
   * @param regexp
   *   The regular expression pattern. A column that evaluates to a string.
   * @group predicate_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def regexp(str: Column, regexp: Column): Column = Column.fn("regexp", str, regexp)

  /**
   * Returns true if `str` matches `regexp`, or false otherwise.
   *
   * @param str
   *   A column that evaluates to a string.
   * @param regexp
   *   The regular expression pattern. A column that evaluates to a string.
   * @group predicate_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def regexp_like(str: Column, regexp: Column): Column = Column.fn("regexp_like", str, regexp)

  /**
   * Returns a count of the number of times that the regular expression pattern `regexp` is
   * matched in the string `str`.
   *
   * @param str
   *   target column to work on. A column that evaluates to a string.
   * @param regexp
   *   regex pattern to apply. A column that evaluates to a string.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def regexp_count(str: Column, regexp: Column): Column = Column.fn("regexp_count", str, regexp)

  /**
   * Extract a specific group matched by a Java regex, from the specified string column. If the
   * regex did not match, or the specified group did not match, an empty string is returned. if
   * the specified group index exceeds the group count of regex, an IllegalArgumentException will
   * be thrown.
   *
   * @param e
   *   target column to work on. A column that evaluates to a string.
   * @param exp
   *   regex pattern to apply. A string. Must be a constant.
   * @param groupIdx
   *   matched group id. An integer. Must be a constant.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def regexp_extract(e: Column, exp: String, groupIdx: Int): Column =
    Column.fn("regexp_extract", e, lit(exp), lit(groupIdx))

  /**
   * Extract all strings in the `str` that match the `regexp` expression and corresponding to the
   * first regex group index.
   *
   * @param str
   *   target column to work on. A column that evaluates to a string.
   * @param regexp
   *   regex pattern to apply. A column that evaluates to a string.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def regexp_extract_all(str: Column, regexp: Column): Column =
    Column.fn("regexp_extract_all", str, regexp)

  /**
   * Extract all strings in the `str` that match the `regexp` expression and corresponding to the
   * regex group index.
   *
   * @param str
   *   target column to work on. A column that evaluates to a string.
   * @param regexp
   *   regex pattern to apply. A column that evaluates to a string.
   * @param idx
   *   matched group id. A column that evaluates to an integer.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def regexp_extract_all(str: Column, regexp: Column, idx: Column): Column =
    Column.fn("regexp_extract_all", str, regexp, idx)

  /**
   * Replace all substrings of the specified string value that match regexp with rep.
   *
   * @param e
   *   target column to work on. A column that evaluates to a string.
   * @param pattern
   *   regex pattern to apply. A string. Must be a constant.
   * @param replacement
   *   replacement string. A string. Must be a constant.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def regexp_replace(e: Column, pattern: String, replacement: String): Column =
    regexp_replace(e, lit(pattern), lit(replacement))

  /**
   * Replace all substrings of the specified string value that match regexp with rep, starting at
   * the specified position `pos`.
   *
   * @param e
   *   target column to work on. A column that evaluates to a string.
   * @param pattern
   *   regex pattern to apply. A string. Must be a constant.
   * @param replacement
   *   replacement string. A string. Must be a constant.
   * @param pos
   *   position to start replacement. The first position is 1. An integer. Must be a constant.
   * @group string_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def regexp_replace(e: Column, pattern: String, replacement: String, pos: Int): Column =
    regexp_replace(e, lit(pattern), lit(replacement), lit(pos))

  /**
   * Replace all substrings of the specified string value that match regexp with rep.
   *
   * @param e
   *   target column to work on. A column that evaluates to a string.
   * @param pattern
   *   regex pattern to apply. A column that evaluates to a string.
   * @param replacement
   *   replacement string. A column that evaluates to a string.
   * @group string_funcs
   * @since 2.1.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def regexp_replace(e: Column, pattern: Column, replacement: Column): Column =
    Column.fn("regexp_replace", e, pattern, replacement)

  /**
   * Replace all substrings of the specified string value that match regexp with rep, starting at
   * the specified position `pos`.
   *
   * @param e
   *   target column to work on. A column that evaluates to a string.
   * @param pattern
   *   regex pattern to apply. A column that evaluates to a string.
   * @param replacement
   *   replacement string. A column that evaluates to a string.
   * @param pos
   *   position to start replacement. The first position is 1. A column that evaluates to an
   *   integer.
   * @group string_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def regexp_replace(e: Column, pattern: Column, replacement: Column, pos: Column): Column =
    Column.fn("regexp_replace", e, pattern, replacement, pos)

  /**
   * Returns the substring that matches the regular expression `regexp` within the string `str`.
   * If the regular expression is not found, the result is null.
   *
   * @param str
   *   target column to work on. A column that evaluates to a string.
   * @param regexp
   *   regex pattern to apply. A column that evaluates to a string.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def regexp_substr(str: Column, regexp: Column): Column = Column.fn("regexp_substr", str, regexp)

  /**
   * Searches a string for a regular expression and returns an integer that indicates the
   * beginning position of the matched substring. Positions are 1-based, not 0-based. If no match
   * is found, returns 0.
   *
   * @param str
   *   target column to work on. A column that evaluates to a string.
   * @param regexp
   *   regex pattern to apply. A column that evaluates to a string.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def regexp_instr(str: Column, regexp: Column): Column = Column.fn("regexp_instr", str, regexp)

  /**
   * Searches a string for a regular expression and returns an integer that indicates the
   * beginning position of the matched substring. Positions are 1-based, not 0-based. If no match
   * is found, returns 0.
   *
   * @param str
   *   target column to work on. A column that evaluates to a string.
   * @param regexp
   *   regex pattern to apply. A column that evaluates to a string.
   * @param idx
   *   matched group id. A column that evaluates to an integer.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def regexp_instr(str: Column, regexp: Column, idx: Column): Column =
    Column.fn("regexp_instr", str, regexp, idx)

  /**
   * Decodes a BASE64 encoded string column and returns it as a binary column. This is the reverse
   * of base64.
   *
   * @param e
   *   target column to work on. A column that evaluates to a string.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def unbase64(e: Column): Column = Column.fn("unbase64", e)

  /**
   * Right-pad the string column with pad to a length of len. If the string column is longer than
   * len, the return value is shortened to len characters.
   *
   * @param str
   *   target column to work on. A column that evaluates to a string.
   * @param len
   *   length of the final string. An integer. Must be a constant.
   * @param pad
   *   chars to append. A string. Must be a constant.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def rpad(str: Column, len: Int, pad: String): Column = rpad(str, lit(len), lit(pad))

  /**
   * Right-pad the binary column with pad to a byte length of len. If the binary column is longer
   * than len, the return value is shortened to len bytes.
   *
   * @param str
   *   target column to work on. A column that evaluates to a binary.
   * @param len
   *   byte length of the final binary. An integer. Must be a constant.
   * @param pad
   *   bytes to append. A binary. Must be a constant.
   * @group string_funcs
   * @since 3.3.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def rpad(str: Column, len: Int, pad: Array[Byte]): Column = rpad(str, lit(len), lit(pad))

  /**
   * Right-pad the string column with pad to a length of len. If the string column is longer than
   * len, the return value is shortened to len characters.
   *
   * @param str
   *   target column to work on. A column that evaluates to a string or binary.
   * @param len
   *   length of the final result. A column that evaluates to an integer.
   * @param pad
   *   chars or bytes to append. A column that evaluates to a string or binary.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def rpad(str: Column, len: Column, pad: Column): Column = Column.fn("rpad", str, len, pad)

  /**
   * Repeats a string column n times, and returns it as a new string column.
   *
   * @param str
   *   target column to work on. A column that evaluates to a string.
   * @param n
   *   number of times to repeat value. A column that evaluates to an integral. Must be a
   *   constant.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def repeat(str: Column, n: Int): Column = Column.fn("repeat", str, lit(n))

  /**
   * Repeats a string column n times, and returns it as a new string column.
   *
   * @param str
   *   target column to work on. A column that evaluates to a string.
   * @param n
   *   number of times to repeat value. A column that evaluates to an integral.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def repeat(str: Column, n: Column): Column = Column.fn("repeat", str, n)

  /**
   * Trim the spaces from right end for the specified string value.
   *
   * @param e
   *   target column to work on. A column that evaluates to a string.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def rtrim(e: Column): Column = Column.fn("rtrim", e)

  /**
   * Trim the specified character string from right end for the specified string column.
   * @param e
   *   target column to work on. A column that evaluates to a string.
   * @param trimString
   *   the trim string characters to trim. A column that evaluates to a string.
   * @group string_funcs
   * @since 2.3.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def rtrim(e: Column, trimString: String): Column = rtrim(e, lit(trimString))

  /**
   * Trim the specified character string from right end for the specified string column.
   * @param e
   *   target column to work on. A column that evaluates to a string.
   * @param trim
   *   the trim string characters to trim. A column that evaluates to a string.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def rtrim(e: Column, trim: Column): Column = Column.fn("rtrim", trim, e)

  /**
   * Returns the soundex code for the specified expression.
   *
   * @param e
   *   target column to work on. A column that evaluates to a string.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def soundex(e: Column): Column = Column.fn("soundex", e)

  /**
   * Splits str around matches of the given pattern.
   *
   * @param str
   *   a string expression to split. A column that evaluates to a string.
   * @param pattern
   *   a string representing a regular expression. The regex string should be a Java regular
   *   expression. A column that evaluates to a string.
   *
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def split(str: Column, pattern: String): Column = Column.fn("split", str, lit(pattern))

  /**
   * Splits str around matches of the given pattern.
   *
   * @param str
   *   a string expression to split. A column that evaluates to a string.
   * @param pattern
   *   a column of string representing a regular expression. The regex string should be a Java
   *   regular expression. A column that evaluates to a string.
   *
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def split(str: Column, pattern: Column): Column = Column.fn("split", str, pattern)

  /**
   * Splits str around matches of the given pattern.
   *
   * @param str
   *   a string expression to split. A column that evaluates to a string.
   * @param pattern
   *   a string representing a regular expression. The regex string should be a Java regular
   *   expression. A column that evaluates to a string.
   * @param limit
   *   an integer expression which controls the number of times the regex is applied. <ul>
   *   <li>limit greater than 0: The resulting array's length will not be more than limit, and the
   *   resulting array's last entry will contain all input beyond the last matched regex.</li>
   *   <li>limit less than or equal to 0: `regex` will be applied as many times as possible, and
   *   the resulting array can be of any size.</li> </ul> A column that evaluates to an integer.
   *
   * @group string_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def split(str: Column, pattern: String, limit: Int): Column =
    Column.fn("split", str, lit(pattern), lit(limit))

  /**
   * Splits str around matches of the given pattern.
   *
   * @param str
   *   a string expression to split. A column that evaluates to a string.
   * @param pattern
   *   a column of string representing a regular expression. The regex string should be a Java
   *   regular expression. A column that evaluates to a string.
   * @param limit
   *   a column of integer expression which controls the number of times the regex is applied.
   *   <ul> <li>limit greater than 0: The resulting array's length will not be more than limit,
   *   and the resulting array's last entry will contain all input beyond the last matched
   *   regex.</li> <li>limit less than or equal to 0: `regex` will be applied as many times as
   *   possible, and the resulting array can be of any size.</li> </ul> A column that evaluates to
   *   an integer.
   *
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def split(str: Column, pattern: Column, limit: Column): Column =
    Column.fn("split", str, pattern, limit)

  /**
   * Substring starts at `pos` and is of length `len` when str is String type or returns the slice
   * of byte array that starts at `pos` in byte and is of length `len` when str is Binary type
   *
   * @param str
   *   target column to work on. A column that evaluates to a string or binary.
   * @param pos
   *   starting position in str. A column that evaluates to an integral. Must be a constant.
   * @param len
   *   length of chars. A column that evaluates to an integral. Must be a constant.
   * @note
   *   The position is not zero based, but 1 based index.
   *
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def substring(str: Column, pos: Int, len: Int): Column =
    Column.fn("substring", str, lit(pos), lit(len))

  /**
   * Substring starts at `pos` and is of length `len` when str is String type or returns the slice
   * of byte array that starts at `pos` in byte and is of length `len` when str is Binary type
   *
   * @param str
   *   target column to work on. A column that evaluates to a string or binary.
   * @param pos
   *   starting position in str. A column that evaluates to an integral.
   * @param len
   *   length of chars. A column that evaluates to an integral.
   * @note
   *   The position is not zero based, but 1 based index.
   *
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def substring(str: Column, pos: Column, len: Column): Column =
    Column.fn("substring", str, pos, len)

  /**
   * Returns the substring from string str before count occurrences of the delimiter delim. If
   * count is positive, everything the left of the final delimiter (counting from left) is
   * returned. If count is negative, every to the right of the final delimiter (counting from the
   * right) is returned. substring_index performs a case-sensitive match when searching for delim.
   *
   * @param str
   *   target column to work on. A column that evaluates to a string.
   * @param delim
   *   delimiter of values. A column that evaluates to a string. Must be a constant.
   * @param count
   *   number of occurrences. A column that evaluates to an integral. Must be a constant.
   * @group string_funcs
   * @return
   *   Returns a column that evaluates to a string.
   */
  def substring_index(str: Column, delim: String, count: Int): Column =
    Column.fn("substring_index", str, lit(delim), lit(count))

  /**
   * Overlay the specified portion of `src` with `replace`, starting from byte position `pos` of
   * `src` and proceeding for `len` bytes.
   *
   * @param src
   *   the string that will be replaced. A column that evaluates to a string or binary.
   * @param replace
   *   the substitution string. A column that evaluates to a string or binary.
   * @param pos
   *   the starting position in src. A column that evaluates to an integral.
   * @param len
   *   the number of bytes to replace in src. A column that evaluates to an integral.
   * @group string_funcs
   * @since 3.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def overlay(src: Column, replace: Column, pos: Column, len: Column): Column =
    Column.fn("overlay", src, replace, pos, len)

  /**
   * Overlay the specified portion of `src` with `replace`, starting from byte position `pos` of
   * `src`.
   *
   * @param src
   *   the string that will be replaced. A column that evaluates to a string or binary.
   * @param replace
   *   the substitution string. A column that evaluates to a string or binary.
   * @param pos
   *   the starting position in src. A column that evaluates to an integral.
   * @group string_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def overlay(src: Column, replace: Column, pos: Column): Column =
    Column.fn("overlay", src, replace, pos)

  /**
   * Splits a string into arrays of sentences, where each sentence is an array of words.
   * @param string
   *   a string to be split. A column that evaluates to a string.
   * @param language
   *   a language of the locale. A column that evaluates to a string.
   * @param country
   *   a country of the locale. A column that evaluates to a string.
   * @group string_funcs
   * @since 3.2.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def sentences(string: Column, language: Column, country: Column): Column =
    Column.fn("sentences", string, language, country)

  /**
   * Splits a string into arrays of sentences, where each sentence is an array of words. The
   * default `country`('') is used.
   * @param string
   *   a string to be split. A column that evaluates to a string.
   * @param language
   *   a language of the locale. A column that evaluates to a string.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def sentences(string: Column, language: Column): Column =
    Column.fn("sentences", string, language)

  /**
   * Splits a string into arrays of sentences, where each sentence is an array of words. The
   * default locale is used.
   * @param string
   *   a string to be split. A column that evaluates to a string.
   * @group string_funcs
   * @since 3.2.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def sentences(string: Column): Column = Column.fn("sentences", string)

  /**
   * Translate any character in the src by a character in replaceString. The characters in
   * replaceString correspond to the characters in matchingString. The translate will happen when
   * any character in the string matches the character in the `matchingString`.
   *
   * @param src
   *   source column to work on. A column that evaluates to a string.
   * @param matchingString
   *   matching characters. A column that evaluates to a string. Must be a constant.
   * @param replaceString
   *   characters for replacement. A column that evaluates to a string. Must be a constant.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def translate(src: Column, matchingString: String, replaceString: String): Column =
    Column.fn("translate", src, lit(matchingString), lit(replaceString))

  /**
   * Trim the spaces from both ends for the specified string column.
   *
   * @param e
   *   The string column to trim. A column that evaluates to a string.
   * @group string_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def trim(e: Column): Column = Column.fn("trim", e)

  /**
   * Trim the specified character from both ends for the specified string column.
   * @param e
   *   The string column to trim. A column that evaluates to a string.
   * @param trimString
   *   The trim string characters to trim. A column that evaluates to a string.
   * @group string_funcs
   * @since 2.3.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def trim(e: Column, trimString: String): Column = trim(e, lit(trimString))

  /**
   * Trim the specified character from both ends for the specified string column.
   * @param e
   *   The string column to trim. A column that evaluates to a string.
   * @param trim
   *   The trim string characters to trim. A column that evaluates to a string.
   * @group string_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def trim(e: Column, trim: Column): Column = Column.fn("trim", trim, e)

  /**
   * Converts a string column to upper case.
   *
   * @param e
   *   The input column to convert to upper case. A column that evaluates to a string.
   * @group string_funcs
   * @since 1.3.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def upper(e: Column): Column = Column.fn("upper", e)

  /**
   * Converts the input `e` to a binary value based on the supplied `format`. The `format` can be
   * a case-insensitive string literal of "hex", "utf-8", "utf8", or "base64". By default, the
   * binary format for conversion is "hex" if `format` is omitted. The function returns NULL if at
   * least one of the input parameters is NULL.
   *
   * @param e
   *   The input value to convert. A column that evaluates to a string.
   * @param f
   *   The format to use to convert the value. A column that evaluates to a string. Must be a
   *   constant.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def to_binary(e: Column, f: Column): Column = Column.fn("to_binary", e, f)

  /**
   * Converts the input `e` to a binary value based on the default format "hex". The function
   * returns NULL if at least one of the input parameters is NULL.
   *
   * @param e
   *   The input value to convert. A column that evaluates to a string.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def to_binary(e: Column): Column = Column.fn("to_binary", e)

  // scalastyle:off line.size.limit
  /**
   * Convert `e` to a string based on the `format`. Throws an exception if the conversion fails.
   * The format can consist of the following characters, case insensitive: '0' or '9': Specifies
   * an expected digit between 0 and 9. A sequence of 0 or 9 in the format string matches a
   * sequence of digits in the input value, generating a result string of the same length as the
   * corresponding sequence in the format string. The result string is left-padded with zeros if
   * the 0/9 sequence comprises more digits than the matching part of the decimal value, starts
   * with 0, and is before the decimal point. Otherwise, it is padded with spaces. '.' or 'D':
   * Specifies the position of the decimal point (optional, only allowed once). ',' or 'G':
   * Specifies the position of the grouping (thousands) separator (,). There must be a 0 or 9 to
   * the left and right of each grouping separator. '$': Specifies the location of the $ currency
   * sign. This character may only be specified once. 'S' or 'MI': Specifies the position of a '-'
   * or '+' sign (optional, only allowed once at the beginning or end of the format string). Note
   * that 'S' prints '+' for positive values but 'MI' prints a space. 'PR': Only allowed at the
   * end of the format string; specifies that the result string will be wrapped by angle brackets
   * if the input value is negative.
   *
   * If `e` is a datetime, `format` shall be a valid datetime pattern, see <a
   * href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime
   * Patterns</a>. If `e` is a binary, it is converted to a string in one of the formats:
   * 'base64': a base 64 string. 'hex': a string in the hexadecimal format. 'utf-8': the input
   * binary is decoded to UTF-8 string.
   *
   * @param e
   *   The input value to convert. A column that evaluates to a numeric, date, timestamp or
   *   binary.
   * @param format
   *   The format to use to convert the value. A column that evaluates to a string. Must be a
   *   constant when `e` is a numeric or binary value.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  // scalastyle:on line.size.limit
  def to_char(e: Column, format: Column): Column = Column.fn("to_char", e, format)

  // scalastyle:off line.size.limit
  /**
   * Convert `e` to a string based on the `format`. Throws an exception if the conversion fails.
   * The format can consist of the following characters, case insensitive: '0' or '9': Specifies
   * an expected digit between 0 and 9. A sequence of 0 or 9 in the format string matches a
   * sequence of digits in the input value, generating a result string of the same length as the
   * corresponding sequence in the format string. The result string is left-padded with zeros if
   * the 0/9 sequence comprises more digits than the matching part of the decimal value, starts
   * with 0, and is before the decimal point. Otherwise, it is padded with spaces. '.' or 'D':
   * Specifies the position of the decimal point (optional, only allowed once). ',' or 'G':
   * Specifies the position of the grouping (thousands) separator (,). There must be a 0 or 9 to
   * the left and right of each grouping separator. '$': Specifies the location of the $ currency
   * sign. This character may only be specified once. 'S' or 'MI': Specifies the position of a '-'
   * or '+' sign (optional, only allowed once at the beginning or end of the format string). Note
   * that 'S' prints '+' for positive values but 'MI' prints a space. 'PR': Only allowed at the
   * end of the format string; specifies that the result string will be wrapped by angle brackets
   * if the input value is negative.
   *
   * If `e` is a datetime, `format` shall be a valid datetime pattern, see <a
   * href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime
   * Patterns</a>. If `e` is a binary, it is converted to a string in one of the formats:
   * 'base64': a base 64 string. 'hex': a string in the hexadecimal format. 'utf-8': the input
   * binary is decoded to UTF-8 string.
   *
   * @param e
   *   The input value to convert. A column that evaluates to a numeric, date, timestamp or
   *   binary.
   * @param format
   *   The format to use to convert the value. A column that evaluates to a string. Must be a
   *   constant when `e` is a numeric or binary value.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  // scalastyle:on line.size.limit
  def to_varchar(e: Column, format: Column): Column = Column.fn("to_varchar", e, format)

  /**
   * Convert string 'e' to a number based on the string format 'format'. Throws an exception if
   * the conversion fails. The format can consist of the following characters, case insensitive:
   * '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the format
   * string matches a sequence of digits in the input string. If the 0/9 sequence starts with 0
   * and is before the decimal point, it can only match a digit sequence of the same size.
   * Otherwise, if the sequence starts with 9 or is after the decimal point, it can match a digit
   * sequence that has the same or smaller size. '.' or 'D': Specifies the position of the decimal
   * point (optional, only allowed once). ',' or 'G': Specifies the position of the grouping
   * (thousands) separator (,). There must be a 0 or 9 to the left and right of each grouping
   * separator. 'expr' must match the grouping separator relevant for the size of the number. '$':
   * Specifies the location of the $ currency sign. This character may only be specified once. 'S'
   * or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed once at the
   * beginning or end of the format string). Note that 'S' allows '-' but 'MI' does not. 'PR':
   * Only allowed at the end of the format string; specifies that 'expr' indicates a negative
   * number with wrapping angled brackets.
   *
   * @param e
   *   The input string to convert to a number. A column that evaluates to a string.
   * @param format
   *   The format to use to convert the value. A column that evaluates to a string. Must be a
   *   constant.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a decimal.
   */
  def to_number(e: Column, format: Column): Column = Column.fn("to_number", e, format)

  /**
   * Replaces all occurrences of `search` with `replace`.
   *
   * @param src
   *   A column of strings to be replaced. A column that evaluates to a string.
   * @param search
   *   A column of strings. If `search` is not found in `str`, `str` is returned unchanged. A
   *   column that evaluates to a string.
   * @param replace
   *   A column of strings. If `replace` is not specified or is an empty string, nothing replaces
   *   the string that is removed from `str`. A column that evaluates to a string.
   *
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def replace(src: Column, search: Column, replace: Column): Column =
    Column.fn("replace", src, search, replace)

  /**
   * Replaces all occurrences of `search` with `replace`.
   *
   * @param src
   *   A column of strings to be replaced. A column that evaluates to a string.
   * @param search
   *   A column of strings. If `search` is not found in `src`, `src` is returned unchanged. A
   *   column that evaluates to a string.
   *
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def replace(src: Column, search: Column): Column = Column.fn("replace", src, search)

  /**
   * Splits `str` by delimiter and return requested part of the split (1-based). If any input is
   * null, returns null. if `partNum` is out of range of split parts, returns empty string. If
   * `partNum` is 0, throws an error. If `partNum` is negative, the parts are counted backward
   * from the end of the string. If the `delimiter` is an empty string, the `str` is not split.
   *
   * @param str
   *   A column of strings to be split. A column that evaluates to a string.
   * @param delimiter
   *   The delimiter used for split. A column that evaluates to a string.
   * @param partNum
   *   The requested part of the split (1-based). A column that evaluates to an integral.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def split_part(str: Column, delimiter: Column, partNum: Column): Column =
    Column.fn("split_part", str, delimiter, partNum)

  /**
   * Returns the substring of `str` that starts at `pos` and is of length `len`, or the slice of
   * byte array that starts at `pos` and is of length `len`.
   *
   * @param str
   *   The input from which to take the substring. A column that evaluates to a string or binary.
   * @param pos
   *   The starting position of the substring. A column that evaluates to an integral.
   * @param len
   *   The length of the substring. A column that evaluates to an integral.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def substr(str: Column, pos: Column, len: Column): Column =
    Column.fn("substr", str, pos, len)

  /**
   * Returns the substring of `str` that starts at `pos`, or the slice of byte array that starts
   * at `pos`.
   *
   * @param str
   *   The input from which to take the substring. A column that evaluates to a string or binary.
   * @param pos
   *   The starting position of the substring. A column that evaluates to an integral.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def substr(str: Column, pos: Column): Column = Column.fn("substr", str, pos)

  /**
   * Extracts a part from a URL.
   *
   * @param url
   *   A column of strings, each representing a URL. A column that evaluates to a string.
   * @param partToExtract
   *   The part to extract from the URL. A column that evaluates to a string.
   * @param key
   *   The key of a query parameter in the URL. A column that evaluates to a string.
   * @group url_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def try_parse_url(url: Column, partToExtract: Column, key: Column): Column =
    Column.fn("try_parse_url", url, partToExtract, key)

  /**
   * Extracts a part from a URL.
   *
   * @param url
   *   A column of strings, each representing a URL. A column that evaluates to a string.
   * @param partToExtract
   *   The part to extract from the URL. A column that evaluates to a string.
   * @group url_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def try_parse_url(url: Column, partToExtract: Column): Column =
    Column.fn("try_parse_url", url, partToExtract)

  /**
   * Extracts a part from a URL.
   *
   * @param url
   *   A column of strings, each representing a URL. A column that evaluates to a string.
   * @param partToExtract
   *   The part to extract from the URL. A column that evaluates to a string.
   * @param key
   *   The key of a query parameter in the URL. A column that evaluates to a string.
   * @group url_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def parse_url(url: Column, partToExtract: Column, key: Column): Column =
    Column.fn("parse_url", url, partToExtract, key)

  /**
   * Extracts a part from a URL.
   *
   * @param url
   *   A column representing a URL. A column that evaluates to a string.
   * @param partToExtract
   *   The part to extract from the URL. A column that evaluates to a string.
   * @group url_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def parse_url(url: Column, partToExtract: Column): Column =
    Column.fn("parse_url", url, partToExtract)

  /**
   * Formats the arguments in printf-style and returns the result as a string column.
   *
   * @param format
   *   A format string that can contain embedded format tags. A column that evaluates to a string.
   * @param arguments
   *   The values to be used in formatting. Columns that evaluate to any type.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  @scala.annotation.varargs
  def printf(format: Column, arguments: Column*): Column =
    Column.fn("printf", (format +: arguments): _*)

  /**
   * Decodes a `str` in 'application/x-www-form-urlencoded' format using a specific encoding
   * scheme.
   *
   * @param str
   *   A URL-encoded string. A column that evaluates to a string.
   * @group url_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def url_decode(str: Column): Column = Column.fn("url_decode", str)

  /**
   * This is a special version of `url_decode` that performs the same operation, but returns a
   * NULL value instead of raising an error if the decoding cannot be performed.
   *
   * @param str
   *   A URL-encoded string. A column that evaluates to a string.
   * @group url_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def try_url_decode(str: Column): Column = Column.fn("try_url_decode", str)

  /**
   * Translates a string into 'application/x-www-form-urlencoded' format using a specific encoding
   * scheme.
   *
   * @param str
   *   A string to encode. A column that evaluates to a string.
   * @group url_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def url_encode(str: Column): Column = Column.fn("url_encode", str)

  /**
   * Returns the position of the first occurrence of `substr` in `str` after position `start`. The
   * given `start` and return value are 1-based.
   *
   * @param substr
   *   The substring to search for. A column that evaluates to a string.
   * @param str
   *   The string to search in. A column that evaluates to a string.
   * @param start
   *   The 1-based position to start the search from. A column that evaluates to an integral.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def position(substr: Column, str: Column, start: Column): Column =
    Column.fn("position", substr, str, start)

  /**
   * Returns the position of the first occurrence of `substr` in `str` after position `1`. The
   * return value are 1-based.
   *
   * @param substr
   *   The substring to search for. A column that evaluates to a string.
   * @param str
   *   The string to search in. A column that evaluates to a string.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def position(substr: Column, str: Column): Column =
    Column.fn("position", substr, str)

  /**
   * Returns a boolean. The value is True if str ends with suffix. Returns NULL if either input
   * expression is NULL. Otherwise, returns False. Both str or suffix must be of STRING or BINARY
   * type.
   *
   * @param str
   *   The string to test. A column that evaluates to a string or binary.
   * @param suffix
   *   The suffix to test for. A column that evaluates to a string or binary.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def endswith(str: Column, suffix: Column): Column =
    Column.fn("endswith", str, suffix)

  /**
   * Returns a boolean. The value is True if str starts with prefix. Returns NULL if either input
   * expression is NULL. Otherwise, returns False. Both str or prefix must be of STRING or BINARY
   * type.
   *
   * @param str
   *   The string to test. A column that evaluates to a string or binary.
   * @param prefix
   *   The prefix to test for. A column that evaluates to a string or binary.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def startswith(str: Column, prefix: Column): Column =
    Column.fn("startswith", str, prefix)

  /**
   * Returns the ASCII character having the binary equivalent to `n`. If n is larger than 256 the
   * result is equivalent to char(n % 256)
   *
   * @param n
   *   The code point value. A column that evaluates to an integral.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def char(n: Column): Column = Column.fn("char", n)

  /**
   * Removes the leading and trailing space characters from `str`.
   *
   * @param str
   *   The string to trim. A column that evaluates to a string.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def btrim(str: Column): Column = Column.fn("btrim", str)

  /**
   * Remove the leading and trailing `trim` characters from `str`.
   *
   * @param str
   *   The string to trim. A column that evaluates to a string.
   * @param trim
   *   The trim string characters to trim. A column that evaluates to a string.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def btrim(str: Column, trim: Column): Column = Column.fn("btrim", str, trim)

  /**
   * This is a special version of `to_binary` that performs the same operation, but returns a NULL
   * value instead of raising an error if the conversion cannot be performed.
   *
   * @param e
   *   The string to convert. A column that evaluates to a string.
   * @param f
   *   The format to use for the conversion. A column that evaluates to a string. Must be a
   *   constant.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def try_to_binary(e: Column, f: Column): Column = Column.fn("try_to_binary", e, f)

  /**
   * This is a special version of `to_binary` that performs the same operation, but returns a NULL
   * value instead of raising an error if the conversion cannot be performed.
   *
   * @param e
   *   The string to convert. A column that evaluates to a string.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def try_to_binary(e: Column): Column = Column.fn("try_to_binary", e)

  /**
   * Convert string `e` to a number based on the string format `format`. Returns NULL if the
   * string `e` does not match the expected format. The format follows the same semantics as the
   * to_number function.
   *
   * @param e
   *   The string to convert. A column that evaluates to a string.
   * @param format
   *   The format used to convert the string to a number. A column that evaluates to a string.
   *   Must be a constant.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a decimal.
   */
  def try_to_number(e: Column, format: Column): Column = Column.fn("try_to_number", e, format)

  /**
   * Returns the character length of string data or number of bytes of binary data. The length of
   * string data includes the trailing spaces. The length of binary data includes binary zeros.
   *
   * @param str
   *   Input column or strings. A column that evaluates to a string or binary.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def char_length(str: Column): Column = Column.fn("char_length", str)

  /**
   * Returns the character length of string data or number of bytes of binary data. The length of
   * string data includes the trailing spaces. The length of binary data includes binary zeros.
   *
   * @param str
   *   Input column or strings. A column that evaluates to a string or binary.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def character_length(str: Column): Column = Column.fn("character_length", str)

  /**
   * Returns the ASCII character having the binary equivalent to `n`. If n is larger than 256 the
   * result is equivalent to chr(n % 256)
   *
   * @param n
   *   The code point. A column that evaluates to an integral.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def chr(n: Column): Column = Column.fn("chr", n)

  /**
   * Returns a boolean. The value is True if right is found inside left. Returns NULL if either
   * input expression is NULL. Otherwise, returns False. Both left or right must be of STRING or
   * BINARY type.
   *
   * @param left
   *   The input to check, may be NULL. A column that evaluates to a string or binary.
   * @param right
   *   The input to find, may be NULL. A column that evaluates to a string or binary.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def contains(left: Column, right: Column): Column = Column.fn("contains", left, right)

  /**
   * Returns the `n`-th input, e.g., returns `input2` when `n` is 2. The function returns NULL if
   * the index exceeds the length of the array and `spark.sql.ansi.enabled` is set to false. If
   * `spark.sql.ansi.enabled` is set to true, it throws ArrayIndexOutOfBoundsException for invalid
   * indices.
   *
   * @param inputs
   *   The index followed by the inputs to select from. Columns where the first evaluates to an
   *   integral and the rest evaluate to strings or binaries.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  @scala.annotation.varargs
  def elt(inputs: Column*): Column = Column.fn("elt", inputs: _*)

  /**
   * Returns the index (1-based) of the given string (`str`) in the comma-delimited list
   * (`strArray`). Returns 0, if the string was not found or if the given string (`str`) contains
   * a comma.
   *
   * @param str
   *   The given string to be found. A column that evaluates to a string.
   * @param strArray
   *   The comma-delimited list. A column that evaluates to a string.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def find_in_set(str: Column, strArray: Column): Column = Column.fn("find_in_set", str, strArray)

  /**
   * Returns true if str matches `pattern` with `escapeChar`, null if any arguments are null,
   * false otherwise.
   *
   * @param str
   *   A column that evaluates to a string.
   * @param pattern
   *   The pattern to match. A column that evaluates to a string.
   * @param escapeChar
   *   The escape character. A column that evaluates to a string. Must be a constant.
   * @group predicate_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def like(str: Column, pattern: Column, escapeChar: Column): Column =
    Column.fn("like", str, pattern, escapeChar)

  /**
   * Returns true if str matches `pattern` with `escapeChar`('\'), null if any arguments are null,
   * false otherwise.
   *
   * @param str
   *   A column that evaluates to a string.
   * @param pattern
   *   The pattern to match. A column that evaluates to a string.
   * @group predicate_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def like(str: Column, pattern: Column): Column = Column.fn("like", str, pattern)

  /**
   * Returns true if str matches `pattern` with `escapeChar` case-insensitively, null if any
   * arguments are null, false otherwise.
   *
   * @param str
   *   A column that evaluates to a string.
   * @param pattern
   *   The pattern to match. A column that evaluates to a string.
   * @param escapeChar
   *   The escape character. A column that evaluates to a string. Must be a constant.
   * @group predicate_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def ilike(str: Column, pattern: Column, escapeChar: Column): Column =
    Column.fn("ilike", str, pattern, escapeChar)

  /**
   * Returns true if str matches `pattern` with `escapeChar`('\') case-insensitively, null if any
   * arguments are null, false otherwise.
   *
   * @param str
   *   A column that evaluates to a string.
   * @param pattern
   *   The pattern to match. A column that evaluates to a string.
   * @group predicate_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def ilike(str: Column, pattern: Column): Column = Column.fn("ilike", str, pattern)

  /**
   * Returns `str` with all characters changed to lowercase.
   *
   * @param str
   *   A column that evaluates to a string.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def lcase(str: Column): Column = Column.fn("lcase", str)

  /**
   * Returns `str` with all characters changed to uppercase.
   *
   * @param str
   *   A column that evaluates to a string.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def ucase(str: Column): Column = Column.fn("ucase", str)

  /**
   * Returns the leftmost `len`(`len` can be string type) characters from the string `str`, if
   * `len` is less or equal than 0 the result is an empty string.
   *
   * @param str
   *   Input column or strings. A column that evaluates to a string or binary.
   * @param len
   *   The number of leftmost characters. A column that evaluates to an integral.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def left(str: Column, len: Column): Column = Column.fn("left", str, len)

  /**
   * Returns the rightmost `len`(`len` can be string type) characters from the string `str`, if
   * `len` is less or equal than 0 the result is an empty string.
   *
   * @param str
   *   Input column or strings. A column that evaluates to a string.
   * @param len
   *   The number of rightmost characters. A column that evaluates to an integral.
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def right(str: Column, len: Column): Column = Column.fn("right", str, len)

  /**
   * Returns `str` enclosed by single quotes and each instance of single quote in it is preceded
   * by a backslash.
   *
   * @param str
   *   A column that evaluates to a string.
   * @group string_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def quote(str: Column): Column = Column.fn("quote", str)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Datasketch functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the estimated number of unique values given the binary representation of a
   * Datasketches HllSketch.
   *
   * @param c
   *   The binary representation of a Datasketches HllSketch. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def hll_sketch_estimate(c: Column): Column = Column.fn("hll_sketch_estimate", c)

  /**
   * Returns the estimated number of unique values given the binary representation of a
   * Datasketches HllSketch.
   *
   * @param columnName
   *   Name of the column containing the binary representation of a Datasketches HllSketch. A
   *   column that evaluates to a binary.
   * @group sketch_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def hll_sketch_estimate(columnName: String): Column = {
    hll_sketch_estimate(Column(columnName))
  }

  /**
   * Merges two binary representations of Datasketches HllSketch objects, using a Datasketches
   * Union object. Throws an exception if sketches have different lgConfigK values.
   *
   * @param c1
   *   The first binary representation of a Datasketches HllSketch. A column that evaluates to a
   *   binary.
   * @param c2
   *   The second binary representation of a Datasketches HllSketch. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_union(c1: Column, c2: Column): Column =
    Column.fn("hll_union", c1, c2)

  /**
   * Merges two binary representations of Datasketches HllSketch objects, using a Datasketches
   * Union object. Throws an exception if sketches have different lgConfigK values.
   *
   * @param columnName1
   *   Name of the column containing the first binary representation of a Datasketches HllSketch.
   *   A column that evaluates to a binary.
   * @param columnName2
   *   Name of the column containing the second binary representation of a Datasketches HllSketch.
   *   A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_union(columnName1: String, columnName2: String): Column = {
    hll_union(Column(columnName1), Column(columnName2))
  }

  /**
   * Merges two binary representations of Datasketches HllSketch objects, using a Datasketches
   * Union object. Throws an exception if sketches have different lgConfigK values and
   * allowDifferentLgConfigK is set to false.
   *
   * @param c1
   *   The first binary representation of a Datasketches HllSketch. A column that evaluates to a
   *   binary.
   * @param c2
   *   The second binary representation of a Datasketches HllSketch. A column that evaluates to a
   *   binary.
   * @param allowDifferentLgConfigK
   *   Allow sketches with different lgConfigK values to be merged (defaults to false). A column
   *   that evaluates to a boolean. Must be a constant.
   * @group sketch_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_union(c1: Column, c2: Column, allowDifferentLgConfigK: Boolean): Column =
    Column.fn("hll_union", c1, c2, lit(allowDifferentLgConfigK))

  /**
   * Merges two binary representations of Datasketches HllSketch objects, using a Datasketches
   * Union object. Throws an exception if sketches have different lgConfigK values and
   * allowDifferentLgConfigK is set to false.
   *
   * @param columnName1
   *   Name of the column containing the first binary representation of a Datasketches HllSketch.
   *   A column that evaluates to a binary.
   * @param columnName2
   *   Name of the column containing the second binary representation of a Datasketches HllSketch.
   *   A column that evaluates to a binary.
   * @param allowDifferentLgConfigK
   *   Allow sketches with different lgConfigK values to be merged (defaults to false). A column
   *   that evaluates to a boolean. Must be a constant.
   * @group sketch_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def hll_union(
      columnName1: String,
      columnName2: String,
      allowDifferentLgConfigK: Boolean): Column = {
    hll_union(Column(columnName1), Column(columnName2), allowDifferentLgConfigK)
  }

  /**
   * Subtracts two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches AnotB object
   *
   * @param c1
   *   The first binary representation of a Datasketches ThetaSketch. A column that evaluates to a
   *   binary.
   * @param c2
   *   The second binary representation of a Datasketches ThetaSketch. A column that evaluates to
   *   a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_difference(c1: Column, c2: Column): Column =
    Column.fn("theta_difference", c1, c2)

  /**
   * Subtracts two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches AnotB object
   *
   * @param columnName1
   *   Name of the column containing the first binary representation of a Datasketches
   *   ThetaSketch. A column that evaluates to a binary.
   * @param columnName2
   *   Name of the column containing the second binary representation of a Datasketches
   *   ThetaSketch. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_difference(columnName1: String, columnName2: String): Column = {
    theta_difference(Column(columnName1), Column(columnName2))
  }

  /**
   * Intersects two binary representations of Datasketches ThetaSketch objects in the input
   * columns using a Datasketches Intersection object
   *
   * @param c1
   *   The first binary representation of a Datasketches ThetaSketch. A column that evaluates to a
   *   binary.
   * @param c2
   *   The second binary representation of a Datasketches ThetaSketch. A column that evaluates to
   *   a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_intersection(c1: Column, c2: Column): Column =
    Column.fn("theta_intersection", c1, c2)

  /**
   * Intersects two binary representations of Datasketches ThetaSketch objects in the input
   * columns using a Datasketches Intersection object
   *
   * @param columnName1
   *   Name of the column containing the first binary representation of a Datasketches
   *   ThetaSketch. A column that evaluates to a binary.
   * @param columnName2
   *   Name of the column containing the second binary representation of a Datasketches
   *   ThetaSketch. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_intersection(columnName1: String, columnName2: String): Column = {
    theta_intersection(Column(columnName1), Column(columnName2))
  }

  /**
   * Returns the estimated number of unique values given the binary representation of a
   * Datasketches ThetaSketch.
   *
   * @param c
   *   The binary representation of a Datasketches ThetaSketch. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def theta_sketch_estimate(c: Column): Column = Column.fn("theta_sketch_estimate", c)

  /**
   * Returns the estimated number of unique values given the binary representation of a
   * Datasketches ThetaSketch.
   *
   * @param columnName
   *   Name of the column containing the binary representation of a Datasketches ThetaSketch.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def theta_sketch_estimate(columnName: String): Column = {
    theta_sketch_estimate(Column(columnName))
  }

  /**
   * Unions two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches Union object. It is configured with the default value of 12 for
   * `lgNomEntries`.
   *
   * @param c1
   *   The first binary representation of a Datasketches ThetaSketch. A column that evaluates to a
   *   binary.
   * @param c2
   *   The second binary representation of a Datasketches ThetaSketch. A column that evaluates to
   *   a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_union(c1: Column, c2: Column): Column =
    Column.fn("theta_union", c1, c2)

  /**
   * Unions two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches Union object. It is configured with the default value of 12 for
   * `lgNomEntries`.
   *
   * @param columnName1
   *   Name of the column containing the first binary representation of a Datasketches
   *   ThetaSketch. A column that evaluates to a binary.
   * @param columnName2
   *   Name of the column containing the second binary representation of a Datasketches
   *   ThetaSketch. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_union(columnName1: String, columnName2: String): Column = {
    theta_union(Column(columnName1), Column(columnName2))
  }

  /**
   * Unions two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches Union object. It allows the configuration of `lgNomEntries` log nominal
   * entries for the union buffer.
   *
   * @param c1
   *   The first binary representation of a Datasketches ThetaSketch. A column that evaluates to a
   *   binary.
   * @param c2
   *   The second binary representation of a Datasketches ThetaSketch. A column that evaluates to
   *   a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union operation (must be between 4 and 26,
   *   defaults to 12). A column that evaluates to an integral. Must be a constant.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_union(c1: Column, c2: Column, lgNomEntries: Int): Column =
    Column.fn("theta_union", c1, c2, lit(lgNomEntries))

  /**
   * Unions two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches Union object. It allows the configuration of `lgNomEntries` log nominal
   * entries for the union buffer.
   *
   * @param columnName1
   *   The first ThetaSketch column to union. A column that evaluates to a binary.
   * @param columnName2
   *   The second ThetaSketch column to union. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_union(columnName1: String, columnName2: String, lgNomEntries: Int): Column = {
    theta_union(Column(columnName1), Column(columnName2), lgNomEntries)
  }

  /**
   * Unions two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches Union object. It allows the configuration of `lgNomEntries` log nominal
   * entries for the union buffer.
   *
   * @param c1
   *   The first ThetaSketch column to union. A column that evaluates to a binary.
   * @param c2
   *   The second ThetaSketch column to union. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def theta_union(c1: Column, c2: Column, lgNomEntries: Column): Column =
    Column.fn("theta_union", c1, c2, lgNomEntries)

  /**
   * Subtracts two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches AnotB object. Returns elements in the
   * first sketch that are not in the second sketch.
   *
   * @param c1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column to subtract. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_difference_double(c1: Column, c2: Column): Column =
    Column.fn("tuple_difference_double", c1, c2)

  /**
   * Subtracts two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches AnotB object. Returns elements in the
   * first sketch that are not in the second sketch.
   *
   * @param columnName1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The second TupleSketch column to subtract. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_difference_double(columnName1: String, columnName2: String): Column =
    tuple_difference_double(Column(columnName1), Column(columnName2))

  /**
   * Subtracts two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches AnotB object. Returns elements in the
   * first sketch that are not in the second sketch.
   *
   * @param c1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column to subtract. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_difference_integer(c1: Column, c2: Column): Column =
    Column.fn("tuple_difference_integer", c1, c2)

  /**
   * Subtracts two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches AnotB object. Returns elements in the
   * first sketch that are not in the second sketch.
   *
   * @param columnName1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The second TupleSketch column to subtract. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_difference_integer(columnName1: String, columnName2: String): Column =
    tuple_difference_integer(Column(columnName1), Column(columnName2))

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Intersection object. The mode parameter
   * specifies the aggregation mode for numeric summaries during intersection (sum, min, max,
   * alwaysone). It is configured with the default mode of 'sum'.
   *
   * @param c1
   *   The first TupleSketch column to intersect. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column to intersect. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_double(c1: Column, c2: Column): Column =
    Column.fn("tuple_intersection_double", c1, c2)

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Intersection object. The mode parameter
   * specifies the aggregation mode for numeric summaries during intersection (sum, min, max,
   * alwaysone). It is configured with the default mode of 'sum'.
   *
   * @param columnName1
   *   The first TupleSketch column to intersect. A column that evaluates to a binary.
   * @param columnName2
   *   The second TupleSketch column to intersect. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_double(columnName1: String, columnName2: String): Column =
    tuple_intersection_double(Column(columnName1), Column(columnName2))

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Intersection object. The mode parameter
   * specifies the aggregation mode for numeric summaries during intersection (sum, min, max,
   * alwaysone).
   *
   * @param c1
   *   The first TupleSketch column to intersect. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column to intersect. A column that evaluates to a binary.
   * @param mode
   *   The aggregation mode for numeric summaries (sum, min, max, alwaysone). A column that
   *   evaluates to a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_double(c1: Column, c2: Column, mode: String): Column =
    Column.fn("tuple_intersection_double", c1, c2, lit(mode))

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Intersection object. The mode parameter
   * specifies the aggregation mode for numeric summaries during intersection (sum, min, max,
   * alwaysone).
   *
   * @param columnName1
   *   The first TupleSketch column to intersect. A column that evaluates to a binary.
   * @param columnName2
   *   The second TupleSketch column to intersect. A column that evaluates to a binary.
   * @param mode
   *   The aggregation mode for numeric summaries (sum, min, max, alwaysone). A column that
   *   evaluates to a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_double(columnName1: String, columnName2: String, mode: String): Column =
    tuple_intersection_double(Column(columnName1), Column(columnName2), mode)

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Intersection object. The mode parameter
   * specifies the aggregation mode for numeric summaries during intersection (sum, min, max,
   * alwaysone).
   *
   * @param c1
   *   The first TupleSketch column to intersect. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column to intersect. A column that evaluates to a binary.
   * @param mode
   *   The aggregation mode for numeric summaries (sum, min, max, alwaysone). A column that
   *   evaluates to a string.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_double(c1: Column, c2: Column, mode: Column): Column =
    Column.fn("tuple_intersection_double", c1, c2, mode)

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with integer
   * summary data type in the input columns using a Datasketches Intersection object. The mode
   * parameter specifies the aggregation mode for numeric summaries during intersection (sum, min,
   * max, alwaysone). It is configured with the default mode of 'sum'.
   *
   * @param c1
   *   The first TupleSketch column to intersect. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column to intersect. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_integer(c1: Column, c2: Column): Column =
    Column.fn("tuple_intersection_integer", c1, c2)

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with integer
   * summary data type in the input columns using a Datasketches Intersection object. The mode
   * parameter specifies the aggregation mode for numeric summaries during intersection (sum, min,
   * max, alwaysone). It is configured with the default mode of 'sum'.
   *
   * @param columnName1
   *   The first TupleSketch column to intersect. A column that evaluates to a binary.
   * @param columnName2
   *   The second TupleSketch column to intersect. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_integer(columnName1: String, columnName2: String): Column =
    tuple_intersection_integer(Column(columnName1), Column(columnName2))

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with integer
   * summary data type in the input columns using a Datasketches Intersection object. The mode
   * parameter specifies the aggregation mode for numeric summaries during intersection (sum, min,
   * max, alwaysone).
   *
   * @param c1
   *   The first TupleSketch column to intersect. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column to intersect. A column that evaluates to a binary.
   * @param mode
   *   The aggregation mode for numeric summaries (sum, min, max, alwaysone). A column that
   *   evaluates to a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_integer(c1: Column, c2: Column, mode: String): Column =
    Column.fn("tuple_intersection_integer", c1, c2, lit(mode))

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with integer
   * summary data type in the input columns using a Datasketches Intersection object. The mode
   * parameter specifies the aggregation mode for numeric summaries during intersection (sum, min,
   * max, alwaysone).
   *
   * @param columnName1
   *   The first TupleSketch column to intersect. A column that evaluates to a binary.
   * @param columnName2
   *   The second TupleSketch column to intersect. A column that evaluates to a binary.
   * @param mode
   *   The aggregation mode for numeric summaries (sum, min, max, alwaysone). A column that
   *   evaluates to a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_integer(columnName1: String, columnName2: String, mode: String): Column =
    tuple_intersection_integer(Column(columnName1), Column(columnName2), mode)

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with integer
   * summary data type in the input columns using a Datasketches Intersection object. The mode
   * parameter specifies the aggregation mode for numeric summaries during intersection (sum, min,
   * max, alwaysone).
   *
   * @param c1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_integer(c1: Column, c2: Column, mode: Column): Column =
    Column.fn("tuple_intersection_integer", c1, c2, mode)

  /**
   * Returns the estimated number of unique values given the binary representation of a
   * Datasketches TupleSketch with double summary data type.
   *
   * @param c
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_estimate_double(c: Column): Column =
    Column.fn("tuple_sketch_estimate_double", c)

  /**
   * Returns the estimated number of unique values given the binary representation of a
   * Datasketches TupleSketch with double summary data type.
   *
   * @param columnName
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_estimate_double(columnName: String): Column =
    tuple_sketch_estimate_double(Column(columnName))

  /**
   * Returns the estimated number of unique values given the binary representation of a
   * Datasketches TupleSketch with integer summary data type.
   *
   * @param c
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_estimate_integer(c: Column): Column =
    Column.fn("tuple_sketch_estimate_integer", c)

  /**
   * Returns the estimated number of unique values given the binary representation of a
   * Datasketches TupleSketch with integer summary data type.
   *
   * @param columnName
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_estimate_integer(columnName: String): Column =
    tuple_sketch_estimate_integer(Column(columnName))

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with double summary data type.
   * The mode parameter specifies the aggregation mode (sum, min, max, alwaysone). It is
   * configured with the default mode of 'sum'.
   *
   * @param c
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_summary_double(c: Column): Column =
    Column.fn("tuple_sketch_summary_double", c)

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with double summary data type.
   * The mode parameter specifies the aggregation mode (sum, min, max, alwaysone). It is
   * configured with the default mode of 'sum'.
   *
   * @param columnName
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_summary_double(columnName: String): Column =
    tuple_sketch_summary_double(Column(columnName))

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with double summary data type.
   * The mode parameter specifies the aggregation mode (sum, min, max, alwaysone).
   *
   * @param c
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_summary_double(c: Column, mode: String): Column =
    Column.fn("tuple_sketch_summary_double", c, lit(mode))

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with double summary data type.
   * The mode parameter specifies the aggregation mode (sum, min, max, alwaysone).
   *
   * @param columnName
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_summary_double(columnName: String, mode: String): Column =
    tuple_sketch_summary_double(Column(columnName), mode)

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with double summary data type.
   * The mode parameter specifies the aggregation mode (sum, min, max, alwaysone).
   *
   * @param c
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_summary_double(c: Column, mode: Column): Column =
    Column.fn("tuple_sketch_summary_double", c, mode)

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with integer summary data type.
   * The mode parameter specifies the aggregation mode (sum, min, max, alwaysone). It is
   * configured with the default mode of 'sum'.
   *
   * @param c
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def tuple_sketch_summary_integer(c: Column): Column =
    Column.fn("tuple_sketch_summary_integer", c)

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with integer summary data type.
   * The mode parameter specifies the aggregation mode (sum, min, max, alwaysone). It is
   * configured with the default mode of 'sum'.
   *
   * @param columnName
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def tuple_sketch_summary_integer(columnName: String): Column =
    tuple_sketch_summary_integer(Column(columnName))

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with integer summary data type.
   * The mode parameter specifies the aggregation mode (sum, min, max, alwaysone).
   *
   * @param c
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def tuple_sketch_summary_integer(c: Column, mode: String): Column =
    Column.fn("tuple_sketch_summary_integer", c, lit(mode))

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with integer summary data type.
   * The mode parameter specifies the aggregation mode (sum, min, max, alwaysone).
   *
   * @param columnName
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def tuple_sketch_summary_integer(columnName: String, mode: String): Column =
    tuple_sketch_summary_integer(Column(columnName), mode)

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with integer summary data type.
   * The mode parameter specifies the aggregation mode (sum, min, max, alwaysone).
   *
   * @param c
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def tuple_sketch_summary_integer(c: Column, mode: Column): Column =
    Column.fn("tuple_sketch_summary_integer", c, mode)

  /**
   * Returns the theta value (sampling rate) from a Datasketches TupleSketch with double summary
   * data type. The theta value represents the effective sampling rate of the sketch, between 0.0
   * and 1.0.
   *
   * @param c
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_theta_double(c: Column): Column =
    Column.fn("tuple_sketch_theta_double", c)

  /**
   * Returns the theta value (sampling rate) from a Datasketches TupleSketch with double summary
   * data type. The theta value represents the effective sampling rate of the sketch, between 0.0
   * and 1.0.
   *
   * @param columnName
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_theta_double(columnName: String): Column =
    tuple_sketch_theta_double(Column(columnName))

  /**
   * Returns the theta value (sampling rate) from a Datasketches TupleSketch with integer summary
   * data type. The theta value represents the effective sampling rate of the sketch, between 0.0
   * and 1.0.
   *
   * @param c
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_theta_integer(c: Column): Column =
    Column.fn("tuple_sketch_theta_integer", c)

  /**
   * Returns the theta value (sampling rate) from a Datasketches TupleSketch with integer summary
   * data type. The theta value represents the effective sampling rate of the sketch, between 0.0
   * and 1.0.
   *
   * @param columnName
   *   The column containing a binary TupleSketch representation. A column that evaluates to a
   *   binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def tuple_sketch_theta_integer(columnName: String): Column =
    tuple_sketch_theta_integer(Column(columnName))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Union object. It is configured with the
   * default values of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param c1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_double(c1: Column, c2: Column): Column =
    Column.fn("tuple_union_double", c1, c2)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Union object. It is configured with the
   * default values of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param columnName1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_double(columnName1: String, columnName2: String): Column =
    tuple_union_double(Column(columnName1), Column(columnName2))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Union object. It allows the configuration
   * of `lgNomEntries` log nominal entries for the union buffer. It uses the default mode of
   * 'sum'.
   *
   * @param c1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_double(c1: Column, c2: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_union_double", c1, c2, lit(lgNomEntries))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Union object. It allows the configuration
   * of `lgNomEntries` log nominal entries for the union buffer. It uses the default mode of
   * 'sum'.
   *
   * @param columnName1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_double(columnName1: String, columnName2: String, lgNomEntries: Int): Column =
    tuple_union_double(Column(columnName1), Column(columnName2), lgNomEntries)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Union object. It allows the configuration
   * of `lgNomEntries` log nominal entries for the union buffer and the aggregation mode for
   * numeric summaries (sum, min, max, alwaysone).
   *
   * @param c1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   The aggregation mode for numeric summaries (sum, min, max, alwaysone). A column that
   *   evaluates to a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_double(c1: Column, c2: Column, lgNomEntries: Int, mode: String): Column =
    Column.fn("tuple_union_double", c1, c2, lit(lgNomEntries), lit(mode))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Union object. It allows the configuration
   * of `lgNomEntries` log nominal entries for the union buffer and the aggregation mode for
   * numeric summaries (sum, min, max, alwaysone).
   *
   * @param columnName1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @param mode
   *   The aggregation mode for numeric summaries (sum, min, max, alwaysone). A column that
   *   evaluates to a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_double(
      columnName1: String,
      columnName2: String,
      lgNomEntries: Int,
      mode: String): Column =
    tuple_union_double(Column(columnName1), Column(columnName2), lgNomEntries, mode)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Union object. It allows the configuration
   * of `lgNomEntries` log nominal entries for the union buffer and the aggregation mode for
   * numeric summaries (sum, min, max, alwaysone).
   *
   * @param c1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral.
   * @param mode
   *   The aggregation mode for numeric summaries (sum, min, max, alwaysone). A column that
   *   evaluates to a string.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_double(c1: Column, c2: Column, lgNomEntries: Column, mode: Column): Column =
    Column.fn("tuple_union_double", c1, c2, lgNomEntries, mode)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches Union object. It is configured with the
   * default values of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param c1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_integer(c1: Column, c2: Column): Column =
    Column.fn("tuple_union_integer", c1, c2)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches Union object. It is configured with the
   * default values of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param columnName1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_integer(columnName1: String, columnName2: String): Column =
    tuple_union_integer(Column(columnName1), Column(columnName2))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches Union object. It allows the configuration
   * of `lgNomEntries` log nominal entries for the union buffer. It uses the default mode of
   * 'sum'.
   *
   * @param c1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_integer(c1: Column, c2: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_union_integer", c1, c2, lit(lgNomEntries))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches Union object. It allows the configuration
   * of `lgNomEntries` log nominal entries for the union buffer. It uses the default mode of
   * 'sum'.
   *
   * @param columnName1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries for the union buffer. A column that evaluates to an
   *   integral. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_integer(columnName1: String, columnName2: String, lgNomEntries: Int): Column =
    tuple_union_integer(Column(columnName1), Column(columnName2), lgNomEntries)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches Union object. It allows the configuration
   * of `lgNomEntries` log nominal entries for the union buffer and the aggregation mode for
   * numeric summaries (sum, min, max, alwaysone).
   *
   * @param c1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries. A column that evaluates to an integral. Must be a
   *   constant.
   * @param mode
   *   The summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   *   Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_integer(c1: Column, c2: Column, lgNomEntries: Int, mode: String): Column =
    Column.fn("tuple_union_integer", c1, c2, lit(lgNomEntries), lit(mode))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches Union object. It allows the configuration
   * of `lgNomEntries` log nominal entries for the union buffer and the aggregation mode for
   * numeric summaries (sum, min, max, alwaysone).
   *
   * @param columnName1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries. A column that evaluates to an integral. Must be a
   *   constant.
   * @param mode
   *   The summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   *   Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_integer(
      columnName1: String,
      columnName2: String,
      lgNomEntries: Int,
      mode: String): Column =
    tuple_union_integer(Column(columnName1), Column(columnName2), lgNomEntries, mode)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches Union object. It allows the configuration
   * of `lgNomEntries` log nominal entries for the union buffer and the aggregation mode for
   * numeric summaries (sum, min, max, alwaysone).
   *
   * @param c1
   *   The first TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The second TupleSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries. A column that evaluates to an integral.
   * @param mode
   *   The summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_integer(c1: Column, c2: Column, lgNomEntries: Column, mode: Column): Column =
    Column.fn("tuple_union_integer", c1, c2, lgNomEntries, mode)

  /**
   * Subtracts the binary representation of a Datasketches ThetaSketch from a TupleSketch with
   * double summary data type in the input columns using a Datasketches AnotB object. Returns
   * elements in the TupleSketch that are not in the ThetaSketch.
   *
   * @param c1
   *   The TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_difference_theta_double(c1: Column, c2: Column): Column =
    Column.fn("tuple_difference_theta_double", c1, c2)

  /**
   * Subtracts the binary representation of a Datasketches ThetaSketch from a TupleSketch with
   * double summary data type in the input columns using a Datasketches AnotB object. Returns
   * elements in the TupleSketch that are not in the ThetaSketch.
   *
   * @param columnName1
   *   The TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_difference_theta_double(columnName1: String, columnName2: String): Column =
    tuple_difference_theta_double(Column(columnName1), Column(columnName2))

  /**
   * Subtracts the binary representation of a Datasketches ThetaSketch from a TupleSketch with
   * integer summary data type in the input columns using a Datasketches AnotB object. Returns
   * elements in the TupleSketch that are not in the ThetaSketch.
   *
   * @param c1
   *   The TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_difference_theta_integer(c1: Column, c2: Column): Column =
    Column.fn("tuple_difference_theta_integer", c1, c2)

  /**
   * Subtracts the binary representation of a Datasketches ThetaSketch from a TupleSketch with
   * integer summary data type in the input columns using a Datasketches AnotB object. Returns
   * elements in the TupleSketch that are not in the ThetaSketch.
   *
   * @param columnName1
   *   The TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_difference_theta_integer(columnName1: String, columnName2: String): Column =
    tuple_difference_theta_integer(Column(columnName1), Column(columnName2))

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with double summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object. The mode parameter specifies the aggregation mode for numeric summaries during
   * intersection (sum, min, max, alwaysone). It is configured with the default mode of 'sum'.
   *
   * @param c1
   *   The TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_theta_double(c1: Column, c2: Column): Column =
    Column.fn("tuple_intersection_theta_double", c1, c2)

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with double summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object. The mode parameter specifies the aggregation mode for numeric summaries during
   * intersection (sum, min, max, alwaysone). It is configured with the default mode of 'sum'.
   *
   * @param columnName1
   *   The TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_theta_double(columnName1: String, columnName2: String): Column =
    tuple_intersection_theta_double(Column(columnName1), Column(columnName2))

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with double summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object. The mode parameter specifies the aggregation mode for numeric summaries during
   * intersection (sum, min, max, alwaysone).
   *
   * @param c1
   *   The TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param mode
   *   The summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   *   Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_theta_double(c1: Column, c2: Column, mode: String): Column =
    Column.fn("tuple_intersection_theta_double", c1, c2, lit(mode))

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with double summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object. The mode parameter specifies the aggregation mode for numeric summaries during
   * intersection (sum, min, max, alwaysone).
   *
   * @param columnName1
   *   The TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param mode
   *   The summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   *   Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_theta_double(
      columnName1: String,
      columnName2: String,
      mode: String): Column =
    tuple_intersection_theta_double(Column(columnName1), Column(columnName2), mode)

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with double summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object. The mode parameter specifies the aggregation mode for numeric summaries during
   * intersection (sum, min, max, alwaysone).
   *
   * @param c1
   *   The TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param mode
   *   The summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_theta_double(c1: Column, c2: Column, mode: Column): Column =
    Column.fn("tuple_intersection_theta_double", c1, c2, mode)

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with integer summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object. The mode parameter specifies the aggregation mode for numeric summaries during
   * intersection (sum, min, max, alwaysone). It is configured with the default mode of 'sum'.
   *
   * @param c1
   *   The TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_theta_integer(c1: Column, c2: Column): Column =
    Column.fn("tuple_intersection_theta_integer", c1, c2)

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with integer summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object. The mode parameter specifies the aggregation mode for numeric summaries during
   * intersection (sum, min, max, alwaysone). It is configured with the default mode of 'sum'.
   *
   * @param columnName1
   *   The TupleSketch column. A column that evaluates to a binary.
   * @param columnName2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_theta_integer(columnName1: String, columnName2: String): Column =
    tuple_intersection_theta_integer(Column(columnName1), Column(columnName2))

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with integer summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object. The mode parameter specifies the aggregation mode for numeric summaries during
   * intersection (sum, min, max, alwaysone).
   *
   * @param c1
   *   The TupleSketch column. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param mode
   *   The summary mode: "sum", "min", "max", or "alwaysone". A column that evaluates to a string.
   *   Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_theta_integer(c1: Column, c2: Column, mode: String): Column =
    Column.fn("tuple_intersection_theta_integer", c1, c2, lit(mode))

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with integer summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object. The mode parameter specifies the aggregation mode for numeric summaries during
   * intersection (sum, min, max, alwaysone).
   *
   * @param columnName1
   *   The TupleSketch column with integer summaries. A column that evaluates to a binary.
   * @param columnName2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_theta_integer(
      columnName1: String,
      columnName2: String,
      mode: String): Column =
    tuple_intersection_theta_integer(Column(columnName1), Column(columnName2), mode)

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with integer summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object. The mode parameter specifies the aggregation mode for numeric summaries during
   * intersection (sum, min, max, alwaysone).
   *
   * @param c1
   *   The TupleSketch column with integer summaries. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_intersection_theta_integer(c1: Column, c2: Column, mode: Column): Column =
    Column.fn("tuple_intersection_theta_integer", c1, c2, mode)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It is
   * configured with the default values of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param c1
   *   The TupleSketch column with double summaries. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_double(c1: Column, c2: Column): Column =
    Column.fn("tuple_union_theta_double", c1, c2)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It is
   * configured with the default values of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param columnName1
   *   The TupleSketch column with double summaries. A column that evaluates to a binary.
   * @param columnName2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_double(columnName1: String, columnName2: String): Column =
    tuple_union_theta_double(Column(columnName1), Column(columnName2))

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It
   * allows the configuration of `lgNomEntries` log nominal entries for the union buffer. It uses
   * the default mode of 'sum'.
   *
   * @param c1
   *   The TupleSketch column with double summaries. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries (between 4 and 26, defaults to 12). A column that
   *   evaluates to an integral. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_double(c1: Column, c2: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_union_theta_double", c1, c2, lit(lgNomEntries))

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It
   * allows the configuration of `lgNomEntries` log nominal entries for the union buffer. It uses
   * the default mode of 'sum'.
   *
   * @param columnName1
   *   The TupleSketch column with double summaries. A column that evaluates to a binary.
   * @param columnName2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries (between 4 and 26, defaults to 12). A column that
   *   evaluates to an integral. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_double(
      columnName1: String,
      columnName2: String,
      lgNomEntries: Int): Column =
    tuple_union_theta_double(Column(columnName1), Column(columnName2), lgNomEntries)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It
   * allows the configuration of `lgNomEntries` log nominal entries for the union buffer and the
   * aggregation mode for numeric summaries (sum, min, max, alwaysone).
   *
   * @param c1
   *   The TupleSketch column with double summaries. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries (between 4 and 26, defaults to 12). A column that
   *   evaluates to an integral. Must be a constant.
   * @param mode
   *   The summary mode: sum (default), min, max, or alwaysone. A column that evaluates to a
   *   string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_double(c1: Column, c2: Column, lgNomEntries: Int, mode: String): Column =
    Column.fn("tuple_union_theta_double", c1, c2, lit(lgNomEntries), lit(mode))

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It
   * allows the configuration of `lgNomEntries` log nominal entries for the union buffer and the
   * aggregation mode for numeric summaries (sum, min, max, alwaysone).
   *
   * @param columnName1
   *   The TupleSketch column with double summaries. A column that evaluates to a binary.
   * @param columnName2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries (between 4 and 26, defaults to 12). A column that
   *   evaluates to an integral. Must be a constant.
   * @param mode
   *   The summary mode: sum (default), min, max, or alwaysone. A column that evaluates to a
   *   string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_double(
      columnName1: String,
      columnName2: String,
      lgNomEntries: Int,
      mode: String): Column =
    tuple_union_theta_double(Column(columnName1), Column(columnName2), lgNomEntries, mode)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It
   * allows the configuration of `lgNomEntries` log nominal entries for the union buffer and the
   * aggregation mode for numeric summaries (sum, min, max, alwaysone).
   *
   * @param c1
   *   The TupleSketch column with double summaries. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries (between 4 and 26, defaults to 12). A column that
   *   evaluates to an integral.
   * @param mode
   *   The summary mode: sum (default), min, max, or alwaysone. A column that evaluates to a
   *   string.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_double(
      c1: Column,
      c2: Column,
      lgNomEntries: Column,
      mode: Column): Column =
    Column.fn("tuple_union_theta_double", c1, c2, lgNomEntries, mode)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It is
   * configured with the default values of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param c1
   *   The TupleSketch column with integer summaries. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_integer(c1: Column, c2: Column): Column =
    Column.fn("tuple_union_theta_integer", c1, c2)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It is
   * configured with the default values of 12 for `lgNomEntries` and 'sum' for mode.
   *
   * @param columnName1
   *   The TupleSketch column with integer summaries. A column that evaluates to a binary.
   * @param columnName2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_integer(columnName1: String, columnName2: String): Column =
    tuple_union_theta_integer(Column(columnName1), Column(columnName2))

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It
   * allows the configuration of `lgNomEntries` log nominal entries for the union buffer. It uses
   * the default mode of 'sum'.
   *
   * @param c1
   *   The TupleSketch column with integer summaries. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries (between 4 and 26, defaults to 12). A column that
   *   evaluates to an integral. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_integer(c1: Column, c2: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_union_theta_integer", c1, c2, lit(lgNomEntries))

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It
   * allows the configuration of `lgNomEntries` log nominal entries for the union buffer. It uses
   * the default mode of 'sum'.
   *
   * @param columnName1
   *   The TupleSketch column with integer summaries. A column that evaluates to a binary.
   * @param columnName2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries (between 4 and 26, defaults to 12). A column that
   *   evaluates to an integral. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_integer(
      columnName1: String,
      columnName2: String,
      lgNomEntries: Int): Column =
    tuple_union_theta_integer(Column(columnName1), Column(columnName2), lgNomEntries)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It
   * allows the configuration of `lgNomEntries` log nominal entries for the union buffer and the
   * aggregation mode for numeric summaries (sum, min, max, alwaysone).
   *
   * @param c1
   *   The TupleSketch column with integer summaries. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries (between 4 and 26, defaults to 12). A column that
   *   evaluates to an integral. Must be a constant.
   * @param mode
   *   The summary mode: sum (default), min, max, or alwaysone. A column that evaluates to a
   *   string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_integer(c1: Column, c2: Column, lgNomEntries: Int, mode: String): Column =
    Column.fn("tuple_union_theta_integer", c1, c2, lit(lgNomEntries), lit(mode))

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It
   * allows the configuration of `lgNomEntries` log nominal entries for the union buffer and the
   * aggregation mode for numeric summaries (sum, min, max, alwaysone).
   *
   * @param columnName1
   *   The TupleSketch column with integer summaries. A column that evaluates to a binary.
   * @param columnName2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries (between 4 and 26, defaults to 12). A column that
   *   evaluates to an integral. Must be a constant.
   * @param mode
   *   The summary mode: sum (default), min, max, or alwaysone. A column that evaluates to a
   *   string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_integer(
      columnName1: String,
      columnName2: String,
      lgNomEntries: Int,
      mode: String): Column =
    tuple_union_theta_integer(Column(columnName1), Column(columnName2), lgNomEntries, mode)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object. It
   * allows the configuration of `lgNomEntries` log nominal entries for the union buffer and the
   * aggregation mode for numeric summaries (sum, min, max, alwaysone).
   *
   * @param c1
   *   The TupleSketch column with integer summaries. A column that evaluates to a binary.
   * @param c2
   *   The ThetaSketch column. A column that evaluates to a binary.
   * @param lgNomEntries
   *   The log-base-2 of nominal entries (must be between 4 and 26, defaults to 12). A column that
   *   evaluates to an integral. Must be a constant.
   * @param mode
   *   The summary mode: "sum" (default), "min", "max", or "alwaysone". A column that evaluates to
   *   a string. Must be a constant.
   * @group sketch_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def tuple_union_theta_integer(
      c1: Column,
      c2: Column,
      lgNomEntries: Column,
      mode: Column): Column =
    Column.fn("tuple_union_theta_integer", c1, c2, lgNomEntries, mode)

  /**
   * Returns a string with human readable summary information about the KLL bigint sketch.
   *
   * @param e
   *   The KLL bigint sketch binary representation. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def kll_sketch_to_string_bigint(e: Column): Column =
    Column.fn("kll_sketch_to_string_bigint", e)

  /**
   * Returns a string with human readable summary information about the KLL float sketch.
   *
   * @param e
   *   The KLL float sketch binary representation. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def kll_sketch_to_string_float(e: Column): Column =
    Column.fn("kll_sketch_to_string_float", e)

  /**
   * Returns a string with human readable summary information about the KLL double sketch.
   *
   * @param e
   *   The KLL double sketch binary representation. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def kll_sketch_to_string_double(e: Column): Column =
    Column.fn("kll_sketch_to_string_double", e)

  /**
   * Returns the number of items collected in the KLL bigint sketch.
   *
   * @param e
   *   The KLL bigint sketch binary representation. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def kll_sketch_get_n_bigint(e: Column): Column =
    Column.fn("kll_sketch_get_n_bigint", e)

  /**
   * Returns the number of items collected in the KLL float sketch.
   *
   * @param e
   *   The KLL float sketch binary representation. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def kll_sketch_get_n_float(e: Column): Column =
    Column.fn("kll_sketch_get_n_float", e)

  /**
   * Returns the number of items collected in the KLL double sketch.
   *
   * @param e
   *   The KLL double sketch binary representation. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def kll_sketch_get_n_double(e: Column): Column =
    Column.fn("kll_sketch_get_n_double", e)

  /**
   * Merges two KLL bigint sketch buffers together into one.
   *
   * @param left
   *   The first KLL bigint sketch. A column that evaluates to a binary.
   * @param right
   *   The second KLL bigint sketch. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_merge_bigint(left: Column, right: Column): Column =
    Column.fn("kll_sketch_merge_bigint", left, right)

  /**
   * Merges two KLL float sketch buffers together into one.
   *
   * @param left
   *   The first KLL float sketch. A column that evaluates to a binary.
   * @param right
   *   The second KLL float sketch. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_merge_float(left: Column, right: Column): Column =
    Column.fn("kll_sketch_merge_float", left, right)

  /**
   * Merges two KLL double sketch buffers together into one.
   *
   * @param left
   *   The first KLL double sketch. A column that evaluates to a binary.
   * @param right
   *   The second KLL double sketch. A column that evaluates to a binary.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def kll_sketch_merge_double(left: Column, right: Column): Column =
    Column.fn("kll_sketch_merge_double", left, right)

  /**
   * Extracts a quantile value from a KLL bigint sketch given an input rank value. The rank can be
   * a single value or an array.
   *
   * @param sketch
   *   The KLL bigint sketch binary representation. A column that evaluates to a binary.
   * @param rank
   *   The rank value(s) to extract (between 0.0 and 1.0). A column that evaluates to a numeric or
   *   an array. Must be a constant.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a long, or an array of longs when `rank` is an array.
   */
  def kll_sketch_get_quantile_bigint(sketch: Column, rank: Column): Column =
    Column.fn("kll_sketch_get_quantile_bigint", sketch, rank)

  /**
   * Extracts a quantile value from a KLL float sketch given an input rank value. The rank can be
   * a single value or an array.
   *
   * @param sketch
   *   The KLL float sketch binary representation. A column that evaluates to a binary.
   * @param rank
   *   The rank value(s) to extract (between 0.0 and 1.0). A column that evaluates to a numeric or
   *   an array.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a float, or an array of floats when `rank` is an array.
   */
  def kll_sketch_get_quantile_float(sketch: Column, rank: Column): Column =
    Column.fn("kll_sketch_get_quantile_float", sketch, rank)

  /**
   * Extracts a quantile value from a KLL double sketch given an input rank value. The rank can be
   * a single value or an array.
   *
   * @param sketch
   *   The KLL double sketch binary representation. A column that evaluates to a binary.
   * @param rank
   *   The rank value(s) to extract (between 0.0 and 1.0). A column that evaluates to a numeric or
   *   an array.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a double, or an array of doubles when `rank` is an
   *   array.
   */
  def kll_sketch_get_quantile_double(sketch: Column, rank: Column): Column =
    Column.fn("kll_sketch_get_quantile_double", sketch, rank)

  /**
   * Extracts a rank value from a KLL bigint sketch given an input quantile value. The quantile
   * can be a single value or an array.
   *
   * @param sketch
   *   The KLL bigint sketch binary representation. A column that evaluates to a binary.
   * @param quantile
   *   The quantile value(s) to lookup. A column that evaluates to an integral or an array.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a double, or an array of doubles when `quantile` is an
   *   array.
   */
  def kll_sketch_get_rank_bigint(sketch: Column, quantile: Column): Column =
    Column.fn("kll_sketch_get_rank_bigint", sketch, quantile)

  /**
   * Extracts a rank value from a KLL float sketch given an input quantile value. The quantile can
   * be a single value or an array.
   *
   * @param sketch
   *   The KLL float sketch binary representation. A column that evaluates to a binary.
   * @param quantile
   *   The quantile value(s) to lookup. A column that evaluates to a numeric or an array.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a double, or an array of doubles when `quantile` is an
   *   array.
   */
  def kll_sketch_get_rank_float(sketch: Column, quantile: Column): Column =
    Column.fn("kll_sketch_get_rank_float", sketch, quantile)

  /**
   * Extracts a rank value from a KLL double sketch given an input quantile value. The quantile
   * can be a single value or an array.
   *
   * @param sketch
   *   The KLL double sketch binary representation. A column that evaluates to a binary.
   * @param quantile
   *   The quantile value(s) to look up. A column that evaluates to a numeric or an array. Must be
   *   a constant.
   * @group sketch_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a double, or an array of doubles when `quantile` is an
   *   array.
   */
  def kll_sketch_get_rank_double(sketch: Column, quantile: Column): Column =
    Column.fn("kll_sketch_get_rank_double", sketch, quantile)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // DateTime functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the date that is `numMonths` after `startDate`.
   *
   * @param startDate
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @param numMonths
   *   The number of months to add to `startDate`, can be negative to subtract months. A column
   *   that evaluates to an integer.
   * @return
   *   A date, or null if `startDate` was a string that could not be cast to a date. Returns a
   *   column that evaluates to a date.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def add_months(startDate: Column, numMonths: Int): Column =
    add_months(startDate, lit(numMonths))

  /**
   * Returns the date that is `numMonths` after `startDate`.
   *
   * @param startDate
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @param numMonths
   *   A column of the number of months to add to `startDate`, can be negative to subtract months.
   *   A column that evaluates to an integer.
   * @return
   *   A date, or null if `startDate` was a string that could not be cast to a date. Returns a
   *   column that evaluates to a date.
   * @group datetime_funcs
   * @since 3.0.0
   */
  def add_months(startDate: Column, numMonths: Column): Column =
    Column.fn("add_months", startDate, numMonths)

  /**
   * Returns the current date at the start of query evaluation as a date column. All calls of
   * current_date within the same query return the same value.
   *
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a date.
   */
  def curdate(): Column = Column.fn("curdate")

  /**
   * Returns the current date at the start of query evaluation as a date column. All calls of
   * current_date within the same query return the same value.
   *
   * @group datetime_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a date.
   */
  def current_date(): Column = Column.fn("current_date")

  /**
   * Returns the current session local timezone.
   *
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def current_timezone(): Column = Column.fn("current_timezone")

  /**
   * Returns the current timestamp at the start of query evaluation as a timestamp column. All
   * calls of current_timestamp within the same query return the same value.
   *
   * @group datetime_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def current_timestamp(): Column = Column.fn("current_timestamp")

  /**
   * Returns the current timestamp at the start of query evaluation.
   *
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def now(): Column = Column.fn("now")

  /**
   * Returns the current timestamp without time zone at the start of query evaluation as a
   * timestamp without time zone column. All calls of localtimestamp within the same query return
   * the same value.
   *
   * @group datetime_funcs
   * @since 3.3.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def localtimestamp(): Column = Column.fn("localtimestamp")

  /**
   * Converts a date/timestamp/string to a value of string in the format specified by the date
   * format given by the second argument.
   *
   * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html"> Datetime
   * Patterns</a> for valid date and time format patterns
   *
   * @param dateExpr
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to
   *   a timestamp or time.
   * @param format
   *   A pattern `dd.MM.yyyy` would return a string like `18.03.1993`. A column that evaluates to
   *   a string.
   * @return
   *   A string, or null if `dateExpr` was a string that could not be cast to a timestamp. Returns
   *   a column that evaluates to a string.
   * @note
   *   Use specialized functions like [[year]] whenever possible as they benefit from a
   *   specialized implementation.
   * @throws IllegalArgumentException
   *   if the `format` pattern is invalid
   * @group datetime_funcs
   * @since 1.5.0
   */
  def date_format(dateExpr: Column, format: String): Column =
    Column.fn("date_format", dateExpr, lit(format))

  /**
   * Returns the date that is `days` days after `start`
   *
   * @param start
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @param days
   *   The number of days to add to `start`, can be negative to subtract days. A column that
   *   evaluates to an integer, short, or byte.
   * @return
   *   A date, or null if `start` was a string that could not be cast to a date. Returns a column
   *   that evaluates to a date.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def date_add(start: Column, days: Int): Column = date_add(start, lit(days))

  /**
   * Returns the date that is `days` days after `start`
   *
   * @param start
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @param days
   *   A column of the number of days to add to `start`, can be negative to subtract days. A
   *   column that evaluates to an integer, short, or byte.
   * @return
   *   A date, or null if `start` was a string that could not be cast to a date. Returns a column
   *   that evaluates to a date.
   * @group datetime_funcs
   * @since 3.0.0
   */
  def date_add(start: Column, days: Column): Column = Column.fn("date_add", start, days)

  /**
   * Returns the date that is `days` days after `start`
   *
   * @param start
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @param days
   *   A column of the number of days to add to `start`, can be negative to subtract days. A
   *   column that evaluates to an integer, short, or byte.
   * @return
   *   A date, or null if `start` was a string that could not be cast to a date. Returns a column
   *   that evaluates to a date.
   * @group datetime_funcs
   * @since 3.5.0
   */
  def dateadd(start: Column, days: Column): Column = Column.fn("dateadd", start, days)

  /**
   * Returns the date that is `days` days before `start`
   *
   * @param start
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @param days
   *   The number of days to subtract from `start`, can be negative to add days. A column that
   *   evaluates to an integer, short, or byte.
   * @return
   *   A date, or null if `start` was a string that could not be cast to a date. Returns a column
   *   that evaluates to a date.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def date_sub(start: Column, days: Int): Column = date_sub(start, lit(days))

  /**
   * Returns the date that is `days` days before `start`
   *
   * @param start
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @param days
   *   A column of the number of days to subtract from `start`, can be negative to add days. A
   *   column that evaluates to an integer, short, or byte.
   * @return
   *   A date, or null if `start` was a string that could not be cast to a date. Returns a column
   *   that evaluates to a date.
   * @group datetime_funcs
   * @since 3.0.0
   */
  def date_sub(start: Column, days: Column): Column =
    Column.fn("date_sub", start, days)

  /**
   * Returns the number of days from `start` to `end`.
   *
   * Only considers the date part of the input. For example:
   * {{{
   * datediff("2018-01-10 00:00:00", "2018-01-09 23:59:59")
   * // returns 1
   * }}}
   *
   * @param end
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @param start
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @return
   *   An integer, or null if either `end` or `start` were strings that could not be cast to a
   *   date. Negative if `end` is before `start`. Returns a column that evaluates to an integer.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def datediff(end: Column, start: Column): Column = Column.fn("datediff", end, start)

  /**
   * Returns the number of days from `start` to `end`.
   *
   * Only considers the date part of the input. For example:
   * {{{
   * date_diff("2018-01-10 00:00:00", "2018-01-09 23:59:59")
   * // returns 1
   * }}}
   *
   * @param end
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @param start
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @return
   *   An integer, or null if either `end` or `start` were strings that could not be cast to a
   *   date. Negative if `end` is before `start`. Returns a column that evaluates to an integer.
   * @group datetime_funcs
   * @since 3.5.0
   */
  def date_diff(end: Column, start: Column): Column = Column.fn("date_diff", end, start)

  /**
   * Create date from the number of `days` since 1970-01-01.
   *
   * @param days
   *   The number of days since 1970-01-01. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a date.
   */
  def date_from_unix_date(days: Column): Column = Column.fn("date_from_unix_date", days)

  /**
   * Extracts the year as an integer from a given date/timestamp/string.
   * @param e
   *   The date, timestamp or string to extract the year from. A column that evaluates to a date,
   *   timestamp or string.
   * @return
   *   An integer, or null if the input was a string that could not be cast to a date. Returns a
   *   column that evaluates to an integer.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def year(e: Column): Column = Column.fn("year", e)

  /**
   * Extracts the quarter as an integer from a given date/timestamp/string.
   * @param e
   *   The date, timestamp or string to extract the quarter from. A column that evaluates to a
   *   date, timestamp or string.
   * @return
   *   An integer, or null if the input was a string that could not be cast to a date. Returns a
   *   column that evaluates to an integer.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def quarter(e: Column): Column = Column.fn("quarter", e)

  /**
   * Extracts the month as an integer from a given date/timestamp/string.
   * @param e
   *   The date, timestamp or string to extract the month from. A column that evaluates to a date,
   *   timestamp or string.
   * @return
   *   An integer, or null if the input was a string that could not be cast to a date. Returns a
   *   column that evaluates to an integer.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def month(e: Column): Column = Column.fn("month", e)

  /**
   * Extracts the day of the week as an integer from a given date/timestamp/string. Ranges from 1
   * for a Sunday through to 7 for a Saturday
   * @param e
   *   The date, timestamp or string to extract the day of the week from. A column that evaluates
   *   to a date, timestamp or string.
   * @return
   *   An integer, or null if the input was a string that could not be cast to a date. Returns a
   *   column that evaluates to an integer.
   * @group datetime_funcs
   * @since 2.3.0
   */
  def dayofweek(e: Column): Column = Column.fn("dayofweek", e)

  /**
   * Extracts the day of the month as an integer from a given date/timestamp/string.
   * @param e
   *   The date, timestamp or string to extract the day of the month from. A column that evaluates
   *   to a date, timestamp or string.
   * @return
   *   An integer, or null if the input was a string that could not be cast to a date. Returns a
   *   column that evaluates to an integer.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def dayofmonth(e: Column): Column = Column.fn("dayofmonth", e)

  /**
   * Extracts the day of the month as an integer from a given date/timestamp/string.
   * @param e
   *   The date, timestamp or string to extract the day of the month from. A column that evaluates
   *   to a date, timestamp or string.
   * @return
   *   An integer, or null if the input was a string that could not be cast to a date. Returns a
   *   column that evaluates to an integer.
   * @group datetime_funcs
   * @since 3.5.0
   */
  def day(e: Column): Column = Column.fn("day", e)

  /**
   * Extracts the day of the year as an integer from a given date/timestamp/string.
   * @param e
   *   The date, timestamp or string to extract the day of the year from. A column that evaluates
   *   to a date, timestamp or string.
   * @return
   *   An integer, or null if the input was a string that could not be cast to a date. Returns a
   *   column that evaluates to an integer.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def dayofyear(e: Column): Column = Column.fn("dayofyear", e)

  /**
   * Extracts the hours as an integer from a given date/time/timestamp/string. The input may also
   * be a nanosecond-precision timestamp `TIMESTAMP_NTZ(p)` or `TIMESTAMP_LTZ(p)` (`p` in
   * `[7, 9]`, since 4.3.0), in which case the sub-microsecond digits are ignored.
   * @param e
   *   The column to extract the hours from. A column that evaluates to a date, time, timestamp or
   *   string.
   * @return
   *   An integer, or null if the input was a string that could not be cast to a date. Returns a
   *   column that evaluates to an integer.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def hour(e: Column): Column = Column.fn("hour", e)

  /**
   * Extracts a part of the date/timestamp or interval source.
   *
   * @param field
   *   selects which part of the source should be extracted.
   * @param source
   *   a date, time, timestamp or interval column from where `field` should be extracted.
   * @return
   *   a part of the date/timestamp or interval source. Returns a column whose type depends on the
   *   field to extract, e.g. an integer for `YEAR` and a decimal for `SECOND`.
   * @group datetime_funcs
   * @since 3.5.0
   */
  def extract(field: Column, source: Column): Column = {
    Column.fn("extract", field, source)
  }

  /**
   * Extracts a part of the date/timestamp or interval source.
   *
   * @param field
   *   selects which part of the source should be extracted, and supported string values are as
   *   same as the fields of the equivalent function `extract`.
   * @param source
   *   a date/timestamp or time or interval column from where `field` should be extracted.
   * @return
   *   a part of the date/timestamp or interval source. Returns a column whose type depends on the
   *   field to extract, e.g. an integer for `YEAR` and a decimal for `SECOND`.
   * @group datetime_funcs
   * @since 3.5.0
   */
  def date_part(field: Column, source: Column): Column = {
    Column.fn("date_part", field, source)
  }

  /**
   * Extracts a part of the date/timestamp or interval source.
   *
   * @param field
   *   selects which part of the source should be extracted, and supported string values are as
   *   same as the fields of the equivalent function `EXTRACT`.
   * @param source
   *   a date/timestamp or interval column from where `field` should be extracted.
   * @return
   *   a part of the date/timestamp or interval source. Returns a column whose type depends on the
   *   field to extract, e.g. an integer for `YEAR` and a decimal for `SECOND`.
   * @group datetime_funcs
   * @since 3.5.0
   */
  def datepart(field: Column, source: Column): Column = {
    Column.fn("datepart", field, source)
  }

  /**
   * Returns the last day of the month which the given date belongs to. For example, input
   * "2015-07-27" returns "2015-07-31" since July 31 is the last day of the month in July 2015.
   *
   * @param e
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @return
   *   A date, or null if the input was a string that could not be cast to a date. Returns a
   *   column that evaluates to a date.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def last_day(e: Column): Column = Column.fn("last_day", e)

  /**
   * Extracts the minutes as an integer from a given date/time/timestamp/string. The input may
   * also be a nanosecond-precision timestamp `TIMESTAMP_NTZ(p)` or `TIMESTAMP_LTZ(p)` (`p` in
   * `[7, 9]`, since 4.3.0), in which case the sub-microsecond digits are ignored.
   * @param e
   *   The column to extract the minutes from. A column that evaluates to a date, time, timestamp
   *   or string.
   * @return
   *   An integer, or null if the input was a string that could not be cast to a date. Returns a
   *   column that evaluates to an integer.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def minute(e: Column): Column = Column.fn("minute", e)

  /**
   * Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).
   *
   * @param e
   *   The column to extract the day of the week from. A column that evaluates to a date,
   *   timestamp or string.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def weekday(e: Column): Column = Column.fn("weekday", e)

  /**
   * @param year
   *   The year to build the date. A column that evaluates to an integral.
   * @param month
   *   The month to build the date. A column that evaluates to an integral.
   * @param day
   *   The day to build the date. A column that evaluates to an integral.
   * @return
   *   A date created from year, month and day fields. Returns a column that evaluates to a date.
   * @group datetime_funcs
   * @since 3.3.0
   */
  def make_date(year: Column, month: Column, day: Column): Column =
    Column.fn("make_date", year, month, day)

  /**
   * Returns number of months between dates `start` and `end`.
   *
   * A whole number is returned if both inputs have the same day of month or both are the last day
   * of their respective months. Otherwise, the difference is calculated assuming 31 days per
   * month.
   *
   * For example:
   * {{{
   * months_between("2017-11-14", "2017-07-14")  // returns 4.0
   * months_between("2017-01-01", "2017-01-10")  // returns 0.29032258
   * months_between("2017-06-01", "2017-06-16 12:00:00")  // returns -0.5
   * }}}
   *
   * @param end
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to
   *   a timestamp.
   * @param start
   *   A date, timestamp or string. If a string, the data must be in a format that can cast to a
   *   timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   timestamp.
   * @return
   *   A double, or null if either `end` or `start` were strings that could not be cast to a
   *   timestamp. Negative if `end` is before `start`. Returns a column that evaluates to a
   *   double.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def months_between(end: Column, start: Column): Column =
    Column.fn("months_between", end, start)

  /**
   * Returns number of months between dates `end` and `start`. If `roundOff` is set to true, the
   * result is rounded off to 8 digits; it is not rounded otherwise.
   * @param end
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to
   *   a timestamp.
   * @param start
   *   A date, timestamp or string. If a string, the data must be in a format that can cast to a
   *   timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   timestamp.
   * @param roundOff
   *   Whether to round off the result to 8 digits. A column that evaluates to a boolean. Must be
   *   a constant.
   * @group datetime_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def months_between(end: Column, start: Column, roundOff: Boolean): Column =
    Column.fn("months_between", end, start, lit(roundOff))

  /**
   * Returns the first date which is later than the value of the `date` column that is on the
   * specified day of the week.
   *
   * For example, `next_day('2015-07-27', "Sunday")` returns 2015-08-02 because that is the first
   * Sunday after 2015-07-27.
   *
   * @param date
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @param dayOfWeek
   *   Case insensitive, and accepts: "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun". A column
   *   that evaluates to a string.
   * @return
   *   A date, or null if `date` was a string that could not be cast to a date or if `dayOfWeek`
   *   was an invalid value. Returns a column that evaluates to a date.
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
   * @param date
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @param dayOfWeek
   *   A column of the day of week. Case insensitive, and accepts: "Mon", "Tue", "Wed", "Thu",
   *   "Fri", "Sat", "Sun". A column that evaluates to a string.
   * @return
   *   A date, or null if `date` was a string that could not be cast to a date or if `dayOfWeek`
   *   was an invalid value. Returns a column that evaluates to a date.
   * @group datetime_funcs
   * @since 3.2.0
   */
  def next_day(date: Column, dayOfWeek: Column): Column =
    Column.fn("next_day", date, dayOfWeek)

  /**
   * Extracts the seconds as an integer from a given date/time/timestamp/string. The input may
   * also be a nanosecond-precision timestamp `TIMESTAMP_NTZ(p)` or `TIMESTAMP_LTZ(p)` (`p` in
   * `[7, 9]`, since 4.3.0), in which case the sub-microsecond digits are ignored.
   * @param e
   *   The column to extract the seconds from. A column that evaluates to a date, time, timestamp
   *   or string.
   * @return
   *   An integer, or null if the input was a string that could not be cast to a timestamp.
   *   Returns a column that evaluates to an integer.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def second(e: Column): Column = Column.fn("second", e)

  /**
   * Extracts the week number as an integer from a given date/timestamp/string.
   *
   * A week is considered to start on a Monday and week 1 is the first week with more than 3 days,
   * as defined by ISO 8601
   *
   * @param e
   *   The column to extract the week number from. A column that evaluates to a date, timestamp or
   *   string.
   * @return
   *   An integer, or null if the input was a string that could not be cast to a date. Returns a
   *   column that evaluates to an integer.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def weekofyear(e: Column): Column = Column.fn("weekofyear", e)

  /**
   * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
   * representing the timestamp of that moment in the current system time zone in the yyyy-MM-dd
   * HH:mm:ss format.
   *
   * @param ut
   *   A number of a type that is castable to a long, such as string or integer. Can be negative
   *   for timestamps before the unix epoch
   * @return
   *   A string, or null if the input was a string that could not be cast to a long. Returns a
   *   column that evaluates to a string.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def from_unixtime(ut: Column): Column = Column.fn("from_unixtime", ut)

  /**
   * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
   * representing the timestamp of that moment in the current system time zone in the given
   * format.
   *
   * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html"> Datetime
   * Patterns</a> for valid date and time format patterns
   *
   * @param ut
   *   A number of a type that is castable to a long, such as string or integer. Can be negative
   *   for timestamps before the unix epoch
   * @param f
   *   A date time pattern that the input will be formatted to
   * @return
   *   A string, or null if `ut` was a string that could not be cast to a long or `f` was an
   *   invalid date time pattern. Returns a column that evaluates to a string.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def from_unixtime(ut: Column, f: String): Column =
    Column.fn("from_unixtime", ut, lit(f))

  /**
   * Returns the current Unix timestamp (in seconds) as a long.
   *
   * @note
   *   All calls of `unix_timestamp` within the same query return the same value (i.e. the current
   *   timestamp is calculated at the start of query evaluation).
   *
   * @group datetime_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def unix_timestamp(): Column = unix_timestamp(current_timestamp())

  /**
   * Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds), using the
   * default timezone and the default locale.
   *
   * @param s
   *   A date, timestamp or string. If a string, the data must be in the `yyyy-MM-dd HH:mm:ss`
   *   format
   * @return
   *   A long, or null if the input was a string not of the correct format. Returns a column that
   *   evaluates to a long.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def unix_timestamp(s: Column): Column = Column.fn("unix_timestamp", s)

  /**
   * Converts time string with given pattern to Unix timestamp (in seconds).
   *
   * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html"> Datetime
   * Patterns</a> for valid date and time format patterns
   *
   * @param s
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   string, date, or timestamp.
   * @param p
   *   A date time pattern detailing the format of `s` when `s` is a string. A column that
   *   evaluates to a string.
   * @return
   *   A long, or null if `s` was a string that could not be cast to a date or `p` was an invalid
   *   format. Returns a column that evaluates to a long.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def unix_timestamp(s: Column, p: String): Column =
    Column.fn("unix_timestamp", s, lit(p))

  /**
   * Parses a string value to a time value.
   *
   * @param str
   *   A string to be parsed to time. A column that evaluates to a string.
   * @return
   *   A time, or raises an error if the input is malformed. Returns a column that evaluates to a
   *   time.
   *
   * @group datetime_funcs
   * @since 4.1.0
   */
  def to_time(str: Column): Column = {
    Column.fn("to_time", str)
  }

  /**
   * Parses a string value to a time value.
   *
   * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html"> Datetime
   * Patterns</a> for valid time format patterns.
   *
   * @param str
   *   A string to be parsed to time.
   * @param format
   *   A time format pattern to follow. A column that evaluates to a string.
   * @return
   *   A time, or raises an error if the input is malformed. Returns a column that evaluates to a
   *   time.
   * @group datetime_funcs
   * @since 4.1.0
   */
  def to_time(str: Column, format: Column): Column = {
    Column.fn("to_time", str, format)
  }

  /**
   * Converts to a timestamp by casting rules to `TimestampType`.
   *
   * @param s
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to
   *   a string, date, timestamp, or numeric.
   * @return
   *   A timestamp, or null if the input was a string that could not be cast to a timestamp.
   *   Returns a column that evaluates to a timestamp.
   * @group datetime_funcs
   * @since 2.2.0
   */
  def to_timestamp(s: Column): Column = Column.fn("to_timestamp", s)

  /**
   * Converts time string with the given pattern to timestamp.
   *
   * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html"> Datetime
   * Patterns</a> for valid date and time format patterns
   *
   * @param s
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to
   *   a string, date, timestamp, or numeric.
   * @param fmt
   *   A date time pattern detailing the format of `s` when `s` is a string. A column that
   *   evaluates to a string.
   * @return
   *   A timestamp, or null if `s` was a string that could not be cast to a timestamp or `fmt` was
   *   an invalid format. Returns a column that evaluates to a timestamp.
   * @group datetime_funcs
   * @since 2.2.0
   */
  def to_timestamp(s: Column, fmt: String): Column = Column.fn("to_timestamp", s, lit(fmt))

  /**
   * Parses a string value to a time value.
   *
   * @param str
   *   A string to be parsed to time. A column that evaluates to a string.
   * @return
   *   A time, or null if the input is malformed. Returns a column that evaluates to a time.
   *
   * @group datetime_funcs
   * @since 4.1.0
   */
  def try_to_time(str: Column): Column = {
    Column.fn("try_to_time", str)
  }

  /**
   * Parses a string value to a time value.
   *
   * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html"> Datetime
   * Patterns</a> for valid time format patterns.
   *
   * @param str
   *   A string to be parsed to time.
   * @param format
   *   A time format pattern to follow. A column that evaluates to a string.
   * @return
   *   A time, or null if the input is malformed. Returns a column that evaluates to a time.
   * @group datetime_funcs
   * @since 4.1.0
   */
  def try_to_time(str: Column, format: Column): Column = {
    Column.fn("try_to_time", str, format)
  }

  /**
   * Parses the `s` with the `format` to a timestamp. The function always returns null on an
   * invalid input with`/`without ANSI SQL mode enabled. The result data type is consistent with
   * the value of configuration `spark.sql.timestampType`.
   *
   * @param s
   *   Column values to convert. A column that evaluates to a string, date, timestamp, or numeric.
   * @param format
   *   Format to use to convert timestamp values. A column that evaluates to a string.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def try_to_timestamp(s: Column, format: Column): Column =
    Column.fn("try_to_timestamp", s, format)

  /**
   * Parses the `s` to a timestamp. The function always returns null on an invalid input
   * with`/`without ANSI SQL mode enabled. It follows casting rules to a timestamp. The result
   * data type is consistent with the value of configuration `spark.sql.timestampType`.
   *
   * @param s
   *   Column values to convert. A column that evaluates to a string, date, timestamp, or numeric.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def try_to_timestamp(s: Column): Column = Column.fn("try_to_timestamp", s)

  /**
   * Converts the column into `DateType` by casting rules to `DateType`.
   *
   * @param e
   *   Input column of values to convert. A column that evaluates to a string, date, or timestamp.
   * @group datetime_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a date.
   */
  def to_date(e: Column): Column = Column.fn("to_date", e)

  /**
   * Converts the column into a `DateType` with a specified format
   *
   * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html"> Datetime
   * Patterns</a> for valid date and time format patterns
   *
   * @param e
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   string, date, or timestamp.
   * @param fmt
   *   A date time pattern detailing the format of `e` when `e`is a string. A column that
   *   evaluates to a string.
   * @return
   *   A date, or null if `e` was a string that could not be cast to a date or `fmt` was an
   *   invalid format. Returns a column that evaluates to a date.
   * @group datetime_funcs
   * @since 2.2.0
   */
  def to_date(e: Column, fmt: String): Column = Column.fn("to_date", e, lit(fmt))

  /**
   * This is a special version of `to_date` that performs the same operation, but returns a NULL
   * value instead of raising an error if date cannot be created.
   *
   * @param e
   *   Input column of values to convert. A column that evaluates to a string, date, or timestamp.
   * @group datetime_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a date.
   */
  def try_to_date(e: Column): Column = Column.fn("try_to_date", e)

  /**
   * This is a special version of `to_date` that performs the same operation, but returns a NULL
   * value instead of raising an error if date cannot be created.
   *
   * @param e
   *   Input column of values to convert. A column that evaluates to a string, date, or timestamp.
   * @param fmt
   *   Format to use to convert date values. A column that evaluates to a string. Must be a
   *   constant.
   * @group datetime_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a date.
   */
  def try_to_date(e: Column, fmt: String): Column = Column.fn("try_to_date", e, lit(fmt))

  /**
   * Returns the number of days since 1970-01-01.
   *
   * @param e
   *   Input column of values to convert. A column that evaluates to a date.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def unix_date(e: Column): Column = Column.fn("unix_date", e)

  /**
   * Returns the number of microseconds since 1970-01-01 00:00:00 UTC.
   *
   * @param e
   *   Input column of values to convert. A column that evaluates to a timestamp.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def unix_micros(e: Column): Column = Column.fn("unix_micros", e)

  /**
   * Returns the number of nanoseconds since 1970-01-01 00:00:00 UTC for a nanosecond-precision
   * timestamp (`TIMESTAMP_LTZ(p)` / `TIMESTAMP_NTZ(p)`, `p` in `[7, 9]`). The result is a
   * lossless `DECIMAL(21, 0)`.
   *
   * @param e
   *   input column of nanosecond-precision timestamp values to convert. A column that evaluates
   *   to a timestamp.
   * @group datetime_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to a decimal.
   */
  def unix_nanos(e: Column): Column = Column.fn("unix_nanos", e)

  /**
   * Returns the number of milliseconds since 1970-01-01 00:00:00 UTC. Truncates higher levels of
   * precision.
   *
   * @param e
   *   input column of values to convert. A column that evaluates to a timestamp.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def unix_millis(e: Column): Column = Column.fn("unix_millis", e)

  /**
   * Returns the number of seconds since 1970-01-01 00:00:00 UTC. Truncates higher levels of
   * precision.
   *
   * @param e
   *   input column of values to convert. A column that evaluates to a timestamp.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def unix_seconds(e: Column): Column = Column.fn("unix_seconds", e)

  /**
   * Returns date truncated to the unit specified by the format.
   *
   * For example, `trunc("2018-11-19 12:01:19", "year")` returns 2018-01-01
   *
   * @param date
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to a
   *   date.
   * @param format:
   *   'year', 'yyyy', 'yy' to truncate by year, or 'month', 'mon', 'mm' to truncate by month
   *   Other options are: 'week', 'quarter'. A column that evaluates to a string.
   *
   * @return
   *   A date, or null if `date` was a string that could not be cast to a date or `format` was an
   *   invalid value. Returns a column that evaluates to a date.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def trunc(date: Column, format: String): Column = Column.fn("trunc", date, lit(format))

  /**
   * Returns timestamp truncated to the unit specified by the format.
   *
   * For example, `date_trunc("year", "2018-11-19 12:01:19")` returns 2018-01-01 00:00:00
   *
   * @param format:
   *   'year', 'yyyy', 'yy' to truncate by year, 'month', 'mon', 'mm' to truncate by month, 'day',
   *   'dd' to truncate by day, Other options are: 'microsecond', 'millisecond', 'second',
   *   'minute', 'hour', 'week', 'quarter'. A column that evaluates to a string.
   * @param timestamp
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to
   *   a timestamp.
   * @return
   *   A timestamp, or null if `timestamp` was a string that could not be cast to a timestamp or
   *   `format` was an invalid value. Returns a column that evaluates to a timestamp.
   * @group datetime_funcs
   * @since 2.3.0
   */
  def date_trunc(format: String, timestamp: Column): Column =
    Column.fn("date_trunc", lit(format), timestamp)

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC, and renders
   * that time as a timestamp in the given time zone. For example, 'GMT+1' would yield '2017-07-14
   * 03:40:00.0'.
   *
   * @param ts
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to
   *   a timestamp.
   * @param tz
   *   A string detailing the time zone ID that the input should be adjusted to. It should be in
   *   the format of either region-based zone IDs or zone offsets. Region IDs must have the form
   *   'area/city', such as 'America/Los_Angeles'. Zone offsets must be in the format
   *   '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are supported as aliases
   *   of '+00:00'. Other short names are not recommended to use because they can be ambiguous. A
   *   column that evaluates to a string.
   * @return
   *   A timestamp, or null if `ts` was a string that could not be cast to a timestamp or `tz` was
   *   an invalid value. Returns a column that evaluates to a timestamp.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def from_utc_timestamp(ts: Column, tz: String): Column = from_utc_timestamp(ts, lit(tz))

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC, and renders
   * that time as a timestamp in the given time zone. For example, 'GMT+1' would yield '2017-07-14
   * 03:40:00.0'.
   * @param ts
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to
   *   a timestamp.
   * @param tz
   *   A string detailing the time zone ID that the input should be adjusted to. A column that
   *   evaluates to a string.
   * @group datetime_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def from_utc_timestamp(ts: Column, tz: Column): Column =
    Column.fn("from_utc_timestamp", ts, tz)

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in the given time
   * zone, and renders that time as a timestamp in UTC. For example, 'GMT+1' would yield
   * '2017-07-14 01:40:00.0'.
   *
   * @param ts
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to
   *   a timestamp.
   * @param tz
   *   A string detailing the time zone ID that the input should be adjusted to. It should be in
   *   the format of either region-based zone IDs or zone offsets. Region IDs must have the form
   *   'area/city', such as 'America/Los_Angeles'. Zone offsets must be in the format
   *   '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are supported as aliases
   *   of '+00:00'. Other short names are not recommended to use because they can be ambiguous. A
   *   column that evaluates to a string.
   * @return
   *   A timestamp, or null if `ts` was a string that could not be cast to a timestamp or `tz` was
   *   an invalid value. Returns a column that evaluates to a timestamp.
   * @group datetime_funcs
   * @since 1.5.0
   */
  def to_utc_timestamp(ts: Column, tz: String): Column = to_utc_timestamp(ts, lit(tz))

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in the given time
   * zone, and renders that time as a timestamp in UTC. For example, 'GMT+1' would yield
   * '2017-07-14 01:40:00.0'.
   * @param ts
   *   A date, timestamp or string. If a string, the data must be in a format that can be cast to
   *   a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`. A column that evaluates to
   *   a timestamp.
   * @param tz
   *   A string detailing the time zone ID that the input should be adjusted to. A column that
   *   evaluates to a string.
   * @group datetime_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def to_utc_timestamp(ts: Column, tz: Column): Column = Column.fn("to_utc_timestamp", ts, tz)

  /**
   * Bucketize rows into one or more time windows given a timestamp specifying column. Window
   * starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
   * [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
   * the order of months are not supported. The following example takes the average stock price
   * for a one minute window every 10 seconds starting 5 seconds after the hour:
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
   * @param timeColumn
   *   The column or the expression to use as the timestamp for windowing by time. The time column
   *   must be of TimestampType or TimestampNTZType. A column that evaluates to a timestamp.
   * @param windowDuration
   *   A string specifying the width of the window, e.g. `10 minutes`, `1 second`. Check
   *   `org.apache.spark.unsafe.types.CalendarInterval` for valid duration identifiers. Note that
   *   the duration is a fixed length of time, and does not vary over time according to a
   *   calendar. For example, `1 day` always means 86,400,000 milliseconds, not a calendar day. A
   *   column that evaluates to a string.
   * @param slideDuration
   *   A string specifying the sliding interval of the window, e.g. `1 minute`. A new window will
   *   be generated every `slideDuration`. Must be less than or equal to the `windowDuration`.
   *   Check `org.apache.spark.unsafe.types.CalendarInterval` for valid duration identifiers. This
   *   duration is likewise absolute, and does not vary according to a calendar. A column that
   *   evaluates to a string.
   * @param startTime
   *   The offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals.
   *   For example, in order to have hourly tumbling windows that start 15 minutes past the hour,
   *   e.g. 12:15-13:15, 13:15-14:15... provide `startTime` as `15 minutes`. A column that
   *   evaluates to a string.
   *
   * @group datetime_funcs
   * @since 2.0.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  def window(
      timeColumn: Column,
      windowDuration: String,
      slideDuration: String,
      startTime: String): Column =
    Column.fn("window", timeColumn, lit(windowDuration), lit(slideDuration), lit(startTime))

  /**
   * Bucketize rows into one or more time windows given a timestamp specifying column. Window
   * starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
   * [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
   * the order of months are not supported. The windows start beginning at 1970-01-01 00:00:00
   * UTC. The following example takes the average stock price for a one minute window every 10
   * seconds:
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
   * @param timeColumn
   *   The column or the expression to use as the timestamp for windowing by time. The time column
   *   must be of TimestampType or TimestampNTZType. A column that evaluates to a timestamp.
   * @param windowDuration
   *   A string specifying the width of the window, e.g. `10 minutes`, `1 second`. Check
   *   `org.apache.spark.unsafe.types.CalendarInterval` for valid duration identifiers. Note that
   *   the duration is a fixed length of time, and does not vary over time according to a
   *   calendar. For example, `1 day` always means 86,400,000 milliseconds, not a calendar day. A
   *   column that evaluates to a string.
   * @param slideDuration
   *   A string specifying the sliding interval of the window, e.g. `1 minute`. A new window will
   *   be generated every `slideDuration`. Must be less than or equal to the `windowDuration`.
   *   Check `org.apache.spark.unsafe.types.CalendarInterval` for valid duration identifiers. This
   *   duration is likewise absolute, and does not vary according to a calendar. A column that
   *   evaluates to a string.
   *
   * @group datetime_funcs
   * @since 2.0.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  def window(timeColumn: Column, windowDuration: String, slideDuration: String): Column = {
    window(timeColumn, windowDuration, slideDuration, "0 second")
  }

  /**
   * Generates tumbling time windows given a timestamp specifying column. Window starts are
   * inclusive but the window ends are exclusive, e.g. 12:05 will be in the window [12:05,12:10)
   * but not in [12:00,12:05). Windows can support microsecond precision. Windows in the order of
   * months are not supported. The windows start beginning at 1970-01-01 00:00:00 UTC. The
   * following example takes the average stock price for a one minute tumbling window:
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
   * @param timeColumn
   *   The column or the expression to use as the timestamp for windowing by time. The time column
   *   must be of TimestampType or TimestampNTZType. A column that evaluates to a timestamp.
   * @param windowDuration
   *   A string specifying the width of the window, e.g. `10 minutes`, `1 second`. Check
   *   `org.apache.spark.unsafe.types.CalendarInterval` for valid duration identifiers. A column
   *   that evaluates to a string.
   *
   * @group datetime_funcs
   * @since 2.0.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  def window(timeColumn: Column, windowDuration: String): Column = {
    window(timeColumn, windowDuration, windowDuration, "0 second")
  }

  /**
   * Extracts the event time from the window column.
   *
   * The window column is of StructType { start: Timestamp, end: Timestamp } where start is
   * inclusive and end is exclusive. Since event time can support microsecond precision,
   * window_time(window) = window.end - 1 microsecond.
   *
   * @param windowColumn
   *   The window column (typically produced by window aggregation) of type StructType { start:
   *   Timestamp, end: Timestamp }. A column that evaluates to a struct.
   *
   * @group datetime_funcs
   * @since 3.4.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def window_time(windowColumn: Column): Column = Column.fn("window_time", windowColumn)

  /**
   * Generates session window given a timestamp specifying column.
   *
   * Session window is one of dynamic windows, which means the length of window is varying
   * according to the given inputs. The length of session window is defined as "the timestamp of
   * latest input of the session + gap duration", so when the new inputs are bound to the current
   * session window, the end time of session window can be expanded according to the new inputs.
   *
   * Windows can support microsecond precision. gapDuration in the order of months are not
   * supported.
   *
   * For a streaming query, you may use the function `current_timestamp` to generate windows on
   * processing time.
   *
   * @param timeColumn
   *   The column or the expression to use as the timestamp for windowing by time. The time column
   *   must be of TimestampType or TimestampNTZType. A column that evaluates to a timestamp.
   * @param gapDuration
   *   A string specifying the timeout of the session, e.g. `10 minutes`, `1 second`. Check
   *   `org.apache.spark.unsafe.types.CalendarInterval` for valid duration identifiers. A column
   *   that evaluates to a string.
   *
   * @group datetime_funcs
   * @since 3.2.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  def session_window(timeColumn: Column, gapDuration: String): Column =
    session_window(timeColumn, lit(gapDuration))

  /**
   * Generates session window given a timestamp specifying column.
   *
   * Session window is one of dynamic windows, which means the length of window is varying
   * according to the given inputs. For static gap duration, the length of session window is
   * defined as "the timestamp of latest input of the session + gap duration", so when the new
   * inputs are bound to the current session window, the end time of session window can be
   * expanded according to the new inputs.
   *
   * Besides a static gap duration value, users can also provide an expression to specify gap
   * duration dynamically based on the input row. With dynamic gap duration, the closing of a
   * session window does not depend on the latest input anymore. A session window's range is the
   * union of all events' ranges which are determined by event start time and evaluated gap
   * duration during the query execution. Note that the rows with negative or zero gap duration
   * will be filtered out from the aggregation.
   *
   * Windows can support microsecond precision. gapDuration in the order of months are not
   * supported.
   *
   * For a streaming query, you may use the function `current_timestamp` to generate windows on
   * processing time.
   *
   * @param timeColumn
   *   The column or the expression to use as the timestamp for windowing by time. The time column
   *   must be of TimestampType or TimestampNTZType. A column that evaluates to a timestamp.
   * @param gapDuration
   *   A column specifying the timeout of the session. It could be static value, e.g. `10
   *   minutes`, `1 second`, or an expression/UDF that specifies gap duration dynamically based on
   *   the input row. A column that evaluates to a string or interval.
   *
   * @group datetime_funcs
   * @since 3.2.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  def session_window(timeColumn: Column, gapDuration: Column): Column =
    Column.fn("session_window", timeColumn, gapDuration)

  /**
   * Converts the number of seconds from the Unix epoch (1970-01-01T00:00:00Z) to a timestamp.
   * @param e
   *   unix time values. A column that evaluates to a numeric.
   * @group datetime_funcs
   * @since 3.1.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def timestamp_seconds(e: Column): Column = Column.fn("timestamp_seconds", e)

  /**
   * Creates timestamp from the number of milliseconds since UTC epoch.
   *
   * @param e
   *   unix time values. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def timestamp_millis(e: Column): Column = Column.fn("timestamp_millis", e)

  /**
   * Creates timestamp from the number of microseconds since UTC epoch.
   *
   * @param e
   *   unix time values. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def timestamp_micros(e: Column): Column = Column.fn("timestamp_micros", e)

  /**
   * Creates a timestamp with the local time zone and nanosecond precision (TIMESTAMP_LTZ(9)) from
   * the number of nanoseconds since UTC epoch.
   *
   * @param e
   *   nanosecond values since the UTC epoch. A column that evaluates to an integral or decimal.
   * @group datetime_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def timestamp_nanos(e: Column): Column = Column.fn("timestamp_nanos", e)

  /**
   * Gets the difference between the timestamps in the specified units by truncating the fraction
   * part.
   *
   * @param unit
   *   the units of the difference between the given timestamps, e.g. 'YEAR', 'MONTH', 'DAY',
   *   'HOUR'. A column that evaluates to a string. Must be a constant.
   * @param start
   *   A timestamp which the expression subtracts from `end`. A column that evaluates to a
   *   timestamp.
   * @param end
   *   A timestamp from which the expression subtracts `start`. A column that evaluates to a
   *   timestamp.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def timestamp_diff(unit: String, start: Column, end: Column): Column =
    Column.internalFn("timestampdiff", lit(unit), start, end)

  /**
   * Adds the specified number of units to the given timestamp.
   *
   * @param unit
   *   the units of datetime to add, e.g. 'YEAR', 'MONTH', 'DAY', 'HOUR'. A column that evaluates
   *   to a string. Must be a constant.
   * @param quantity
   *   the number of units of time to add. A column that evaluates to an integral.
   * @param ts
   *   A timestamp to which to add. A column that evaluates to a timestamp.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def timestamp_add(unit: String, quantity: Column, ts: Column): Column =
    Column.internalFn("timestampadd", lit(unit), quantity, ts)

  /**
   * Returns the start of the fixed-size bucket of `bucketSize` that contains `ts`, with buckets
   * aligned to the default origin (1970-01-01 00:00:00). For `TIMESTAMP_NTZ`, bucketing is
   * performed in UTC. For `TIMESTAMP`, year-month interval buckets and calendar-day components of
   * day-time interval buckets align to the session time zone.
   *
   * @param bucketSize
   *   A day-time or year-month interval defining the bucket size. Must be positive and foldable.
   * @param ts
   *   A TIMESTAMP or TIMESTAMP_NTZ value to bucket.
   * @group datetime_funcs
   * @since 4.2.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def time_bucket(bucketSize: Column, ts: Column): Column =
    Column.fn("time_bucket", bucketSize, ts)

  /**
   * Returns the start of the fixed-size bucket of `bucketSize` that contains `ts`, with buckets
   * aligned to `origin`. For `TIMESTAMP_NTZ`, bucketing is performed in UTC. For `TIMESTAMP`,
   * year-month interval buckets and calendar-day components of day-time interval buckets align to
   * the session time zone.
   *
   * @param bucketSize
   *   A day-time or year-month interval defining the bucket size. Must be positive and foldable.
   * @param ts
   *   A TIMESTAMP or TIMESTAMP_NTZ value to bucket.
   * @param origin
   *   Alignment anchor. Must be the same type as `ts` and must be foldable.
   * @group datetime_funcs
   * @since 4.2.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def time_bucket(bucketSize: Column, ts: Column, origin: Column): Column =
    Column.fn("time_bucket", bucketSize, ts, origin)

  /**
   * Returns the difference between two times, measured in specified units. Throws a
   * SparkIllegalArgumentException, in case the specified unit is not supported.
   *
   * @param unit
   *   A STRING representing the unit of the time difference. Supported units are: "HOUR",
   *   "MINUTE", "SECOND", "MILLISECOND", and "MICROSECOND". The unit is case-insensitive. A
   *   column that evaluates to a string.
   * @param start
   *   A starting TIME. A column that evaluates to a time.
   * @param end
   *   An ending TIME. A column that evaluates to a time.
   * @return
   *   The difference between `end` and `start` times, measured in specified units. Returns a
   *   column that evaluates to a long.
   * @note
   *   If any of the inputs is `NULL`, the result is `NULL`.
   * @group datetime_funcs
   * @since 4.1.0
   */
  def time_diff(unit: Column, start: Column, end: Column): Column = {
    Column.fn("time_diff", unit, start, end)
  }

  /**
   * Returns `time` truncated to the `unit`.
   *
   * @param unit
   *   A STRING representing the unit to truncate the time to. Supported units are: "HOUR",
   *   "MINUTE", "SECOND", "MILLISECOND", and "MICROSECOND". The unit is case-insensitive. A
   *   column that evaluates to a string.
   * @param time
   *   A TIME to truncate. A column that evaluates to a time.
   * @return
   *   A TIME truncated to the specified unit. Returns a column that evaluates to a time.
   * @note
   *   If any of the inputs is `NULL`, the result is `NULL`.
   * @throws IllegalArgumentException
   *   If the `unit` is not supported.
   * @group datetime_funcs
   * @since 4.1.0
   */
  def time_trunc(unit: Column, time: Column): Column = {
    Column.fn("time_trunc", unit, time)
  }

  /**
   * Creates a TIME from the number of seconds since midnight.
   *
   * @param e
   *   seconds since midnight (0 to 86399.999999). A column that evaluates to a numeric.
   * @group datetime_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a time.
   */
  def time_from_seconds(e: Column): Column = Column.fn("time_from_seconds", e)

  /**
   * Creates a TIME from the number of milliseconds since midnight.
   *
   * @param e
   *   milliseconds since midnight (0 to 86399999). A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a time.
   */
  def time_from_millis(e: Column): Column = Column.fn("time_from_millis", e)

  /**
   * Creates a TIME from the number of microseconds since midnight.
   *
   * @param e
   *   microseconds since midnight (0 to 86399999999). A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a time.
   */
  def time_from_micros(e: Column): Column = Column.fn("time_from_micros", e)

  /**
   * Extracts the number of seconds (including fractional seconds) from a TIME value. Returns a
   * DECIMAL(14,6) to preserve microsecond precision.
   *
   * @param e
   *   TIME value to convert. A column that evaluates to a time.
   * @group datetime_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a decimal.
   */
  def time_to_seconds(e: Column): Column = Column.fn("time_to_seconds", e)

  /**
   * Extracts the number of milliseconds since midnight from a TIME value.
   *
   * @param e
   *   the TIME value to convert. A column that evaluates to a time.
   * @group datetime_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def time_to_millis(e: Column): Column = Column.fn("time_to_millis", e)

  /**
   * Extracts the number of microseconds since midnight from a TIME value.
   *
   * @param e
   *   the TIME value to convert. A column that evaluates to a time.
   * @group datetime_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def time_to_micros(e: Column): Column = Column.fn("time_to_micros", e)

  /**
   * Parses the `timestamp` expression with the `format` expression to a timestamp with local time
   * zone. Returns null with invalid input.
   *
   * @param timestamp
   *   the input column or strings. A column that evaluates to a date, timestamp or string.
   * @param format
   *   the format used to parse the timestamp values. A column that evaluates to a string.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def to_timestamp_ltz(timestamp: Column, format: Column): Column =
    Column.fn("to_timestamp_ltz", timestamp, format)

  /**
   * Parses the `timestamp` expression with the default format to a timestamp with local time
   * zone. The default format follows casting rules to a timestamp. Returns null with invalid
   * input.
   *
   * @param timestamp
   *   the input column or strings. A column that evaluates to a date, timestamp or string.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def to_timestamp_ltz(timestamp: Column): Column =
    Column.fn("to_timestamp_ltz", timestamp)

  /**
   * Parses the `timestamp_str` expression with the `format` expression to a timestamp without
   * time zone. Returns null with invalid input.
   *
   * @param timestamp
   *   the input column or strings. A column that evaluates to a date, timestamp or string.
   * @param format
   *   the format used to parse the timestamp values. A column that evaluates to a string.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def to_timestamp_ntz(timestamp: Column, format: Column): Column =
    Column.fn("to_timestamp_ntz", timestamp, format)

  /**
   * Parses the `timestamp` expression with the default format to a timestamp without time zone.
   * The default format follows casting rules to a timestamp. Returns null with invalid input.
   *
   * @param timestamp
   *   the input column or strings. A column that evaluates to a date, timestamp or string.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def to_timestamp_ntz(timestamp: Column): Column =
    Column.fn("to_timestamp_ntz", timestamp)

  /**
   * Returns the UNIX timestamp of the given time.
   *
   * @param timeExp
   *   the input column or strings. A column that evaluates to a date, timestamp or string.
   * @param format
   *   the format used to convert the time values. A column that evaluates to a string.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def to_unix_timestamp(timeExp: Column, format: Column): Column =
    Column.fn("to_unix_timestamp", timeExp, format)

  /**
   * Returns the UNIX timestamp of the given time.
   *
   * @param timeExp
   *   the input column or strings. A column that evaluates to a date, timestamp or string.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def to_unix_timestamp(timeExp: Column): Column =
    Column.fn("to_unix_timestamp", timeExp)

  /**
   * Extracts the three-letter abbreviated month name from a given date/timestamp/string.
   *
   * @param timeExp
   *   the target date/timestamp to work on. A column that evaluates to a date, timestamp or
   *   string.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def monthname(timeExp: Column): Column =
    Column.fn("monthname", timeExp)

  /**
   * Extracts the three-letter abbreviated day name from a given date/timestamp/string.
   *
   * @param timeExp
   *   the target date/timestamp to work on. A column that evaluates to a date, timestamp or
   *   string.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def dayname(timeExp: Column): Column =
    Column.fn("dayname", timeExp)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Collection functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns null if the array is null, true if the array contains `value`, and false otherwise.
   * @param column
   *   the target column containing the arrays. A column that evaluates to an array.
   * @param value
   *   the value to check for in the array. A column that evaluates to a value matching the
   *   array's element type.
   * @group array_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def array_contains(column: Column, value: Any): Column =
    Column.fn("array_contains", column, lit(value))

  /**
   * Returns an ARRAY containing all elements from the source ARRAY as well as the new element.
   * The new element/column is located at end of the ARRAY.
   *
   * @param column
   *   the source column containing the array. A column that evaluates to an array.
   * @param element
   *   the value to append to the array. A column that evaluates to a value matching the array's
   *   element type.
   * @group array_funcs
   * @since 3.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_append(column: Column, element: Any): Column =
    Column.fn("array_append", column, lit(element))

  /**
   * Returns `true` if `a1` and `a2` have at least one non-null element in common. If not and both
   * the arrays are non-empty and any of them contains a `null`, it returns `null`. It returns
   * `false` otherwise.
   * @param a1
   *   the first input array. A column that evaluates to an array.
   * @param a2
   *   the second input array. A column that evaluates to an array.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def arrays_overlap(a1: Column, a2: Column): Column = Column.fn("arrays_overlap", a1, a2)

  /**
   * Returns an array containing all the elements in `x` from index `start` (or starting from the
   * end if `start` is negative) with the specified `length`.
   *
   * @param x
   *   the array column to be sliced. A column that evaluates to an array.
   * @param start
   *   the starting index. A column that evaluates to an integer.
   * @param length
   *   the length of the slice. A column that evaluates to an integer.
   *
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def slice(x: Column, start: Int, length: Int): Column =
    slice(x, lit(start), lit(length))

  /**
   * Returns an array containing all the elements in `x` from index `start` (or starting from the
   * end if `start` is negative) with the specified `length`.
   *
   * @param x
   *   the array column to be sliced. A column that evaluates to an array.
   * @param start
   *   the starting index. A column that evaluates to an integer.
   * @param length
   *   the length of the slice. A column that evaluates to an integer.
   *
   * @group array_funcs
   * @since 3.1.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def slice(x: Column, start: Column, length: Column): Column =
    Column.fn("slice", x, start, length)

  /**
   * Concatenates the elements of `column` using the `delimiter`. Null values are replaced with
   * `nullReplacement`.
   * @param column
   *   the input column containing the array. A column that evaluates to an array.
   * @param delimiter
   *   the string used to join the array elements. A column that evaluates to a string.
   * @param nullReplacement
   *   the string used to replace null values. A column that evaluates to a string.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def array_join(column: Column, delimiter: String, nullReplacement: String): Column =
    Column.fn("array_join", column, lit(delimiter), lit(nullReplacement))

  /**
   * Concatenates the elements of `column` using the `delimiter`.
   * @param column
   *   the input column containing the array. A column that evaluates to an array.
   * @param delimiter
   *   the string used to join the array elements. A column that evaluates to a string.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def array_join(column: Column, delimiter: String): Column =
    Column.fn("array_join", column, lit(delimiter))

  /**
   * Concatenates multiple input columns together into a single column. The function works with
   * strings, binary and compatible array columns.
   *
   * @param exprs
   *   Input columns to concatenate. A column that evaluates to a string, binary or an array.
   * @note
   *   Returns null if any of the input columns are null.
   *
   * @group collection_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  @scala.annotation.varargs
  def concat(exprs: Column*): Column = Column.fn("concat", exprs: _*)

  /**
   * Locates the position of the first occurrence of the value in the given array as long. Returns
   * null if either of the arguments are null.
   *
   * @param column
   *   The array to search. A column that evaluates to an array.
   * @param value
   *   The value to locate. A column.
   * @note
   *   The position is not zero based, but 1 based index. Returns 0 if value could not be found in
   *   array.
   *
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def array_position(column: Column, value: Any): Column =
    Column.fn("array_position", column, lit(value))

  /**
   * Returns element of array at given index in value if column is array. Returns value for the
   * given key in value if column is map.
   *
   * @param column
   *   The array or map to extract from. A column that evaluates to an array or a map.
   * @param value
   *   The 1-based index for arrays, or the key for maps. A column.
   * @group collection_funcs
   * @since 2.4.0
   * @return
   *   Returns a column of the element type of the input array, or the value type of the input
   *   map.
   */
  def element_at(column: Column, value: Any): Column = Column.fn("element_at", column, lit(value))

  /**
   * (array, index) - Returns element of array at given (1-based) index. If Index is 0, Spark will
   * throw an error. If index &lt; 0, accesses elements from the last to the first. The function
   * always returns NULL if the index exceeds the length of the array.
   *
   * (map, key) - Returns value for given key. The function always returns NULL if the key is not
   * contained in the map.
   *
   * @param column
   *   The array or map to extract from. A column that evaluates to an array or a map.
   * @param value
   *   The 1-based index for arrays, or the key for maps. A column.
   * @group collection_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the element type of the input array, or the value type of the input
   *   map.
   */
  def try_element_at(column: Column, value: Column): Column =
    Column.fn("try_element_at", column, value)

  /**
   * Returns element of array at given (0-based) index. If the index points outside of the array
   * boundaries, then this function returns NULL.
   *
   * @param column
   *   The array to extract from. A column that evaluates to an array.
   * @param index
   *   The 0-based index. A column that evaluates to an integral.
   * @group array_funcs
   * @since 3.4.0
   * @return
   *   Returns a column of the element type of the input array.
   */
  def get(column: Column, index: Column): Column = Column.fn("get", column, index)

  /**
   * Sorts the input array in ascending order. Null elements will be placed at the end of the
   * returned array. NaN is greater than any non-NaN elements for double/float type.
   *
   * The elements of the input array must be orderable. For example, when the array elements are
   * structs, the default comparator compares the struct fields in schema order. Therefore, all
   * fields in the struct must be orderable. If the default comparator does not support the input
   * type, you can specify a custom comparator.
   *
   * @param e
   *   The array to sort. A column that evaluates to an array.
   * @group collection_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_sort(e: Column): Column = Column.fn("array_sort", e)

  /**
   * Sorts the input array based on the given comparator function. The comparator will take two
   * arguments representing two elements of the array. It returns a negative integer, 0, or a
   * positive integer as the first element is less than, equal to, or greater than the second
   * element. If the comparator function returns null, the function will fail and raise an error.
   *
   * @param e
   *   The array to sort. A column that evaluates to an array.
   * @param comparator
   *   A binary comparator function that returns a negative integer, 0, or a positive integer as
   *   the first element is less than, equal to, or greater than the second element.
   * @group collection_funcs
   * @since 3.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_sort(e: Column, comparator: (Column, Column) => Column): Column =
    Column.fn("array_sort", e, createLambda(comparator))

  /**
   * Remove all elements that equal to element from the given array.
   *
   * @param column
   *   The array to remove from. A column that evaluates to an array.
   * @param element
   *   The element to remove. A column.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_remove(column: Column, element: Any): Column =
    Column.fn("array_remove", column, lit(element))

  /**
   * Remove all null elements from the given array.
   *
   * @param column
   *   The array to compact. A column that evaluates to an array.
   * @group array_funcs
   * @since 3.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_compact(column: Column): Column = Column.fn("array_compact", column)

  /**
   * Returns an array containing value as well as all elements from array. The new element is
   * positioned at the beginning of the array.
   *
   * @param column
   *   The array to prepend to. A column that evaluates to an array.
   * @param element
   *   The element to prepend. A column.
   * @group array_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_prepend(column: Column, element: Any): Column =
    Column.fn("array_prepend", column, lit(element))

  /**
   * Removes duplicate values from the array.
   * @param e
   *   The array to deduplicate. A column that evaluates to an array.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_distinct(e: Column): Column = Column.fn("array_distinct", e)

  /**
   * Returns an array of the elements in the intersection of the given two arrays, without
   * duplicates.
   *
   * @param col1
   *   The first array. A column that evaluates to an array.
   * @param col2
   *   The second array. A column that evaluates to an array.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_intersect(col1: Column, col2: Column): Column =
    Column.fn("array_intersect", col1, col2)

  /**
   * Adds an item into a given array at a specified position
   *
   * @param arr
   *   The array to insert into. A column that evaluates to an array.
   * @param pos
   *   The 1-based position at which to insert (negative counts from the end). A column that
   *   evaluates to an integral.
   * @param value
   *   The value to insert. A column.
   * @group array_funcs
   * @since 3.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_insert(arr: Column, pos: Column, value: Column): Column =
    Column.fn("array_insert", arr, pos, value)

  /**
   * Returns an array of the elements in the union of the given two arrays, without duplicates.
   *
   * @param col1
   *   The first array. A column that evaluates to an array.
   * @param col2
   *   The second array. A column that evaluates to an array.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_union(col1: Column, col2: Column): Column =
    Column.fn("array_union", col1, col2)

  /**
   * Returns an array of the elements in the first array but not in the second array, without
   * duplicates. The order of elements in the result is not determined
   *
   * @param col1
   *   The first array. A column that evaluates to an array.
   * @param col2
   *   The second array. A column that evaluates to an array.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_except(col1: Column, col2: Column): Column =
    Column.fn("array_except", col1, col2)

  private def createLambda(f: Column => Column) = {
    val x = internal.UnresolvedNamedLambdaVariable("x")
    val function = f(Column(x)).node
    Column(internal.LambdaFunction(function, Seq(x)))
  }

  private def createLambda(f: (Column, Column) => Column) = {
    val x = internal.UnresolvedNamedLambdaVariable("x")
    val y = internal.UnresolvedNamedLambdaVariable("y")
    val function = f(Column(x), Column(y)).node
    Column(internal.LambdaFunction(function, Seq(x, y)))
  }

  private def createLambda(f: (Column, Column, Column) => Column) = {
    val x = internal.UnresolvedNamedLambdaVariable("x")
    val y = internal.UnresolvedNamedLambdaVariable("y")
    val z = internal.UnresolvedNamedLambdaVariable("z")
    val function = f(Column(x), Column(y), Column(z)).node
    Column(internal.LambdaFunction(function, Seq(x, y, z)))
  }

  /**
   * Returns an array of elements after applying a transformation to each element in the input
   * array.
   * {{{
   *   df.select(transform(col("i"), x => x + 1))
   * }}}
   *
   * @param column
   *   the input array column. A column that evaluates to an array.
   * @param f
   *   col => transformed_col, the lambda function to transform the input column.
   *
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def transform(column: Column, f: Column => Column): Column =
    Column.fn("transform", column, createLambda(f))

  /**
   * Returns an array of elements after applying a transformation to each element in the input
   * array.
   * {{{
   *   df.select(transform(col("i"), (x, i) => x + i))
   * }}}
   *
   * @param column
   *   the input array column. A column that evaluates to an array.
   * @param f
   *   (col, index) => transformed_col, the lambda function to transform the input column given
   *   the index. Indices start at 0.
   *
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def transform(column: Column, f: (Column, Column) => Column): Column =
    Column.fn("transform", column, createLambda(f))

  /**
   * Returns whether a predicate holds for one or more elements in the array.
   * {{{
   *   df.select(exists(col("i"), _ % 2 === 0))
   * }}}
   *
   * @param column
   *   the input array column. A column that evaluates to an array.
   * @param f
   *   col => predicate, the Boolean predicate to check the input column.
   *
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def exists(column: Column, f: Column => Column): Column =
    Column.fn("exists", column, createLambda(f))

  /**
   * Returns whether a predicate holds for every element in the array.
   * {{{
   *   df.select(forall(col("i"), x => x % 2 === 0))
   * }}}
   *
   * @param column
   *   the input array column. A column that evaluates to an array.
   * @param f
   *   col => predicate, the Boolean predicate to check the input column.
   *
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def forall(column: Column, f: Column => Column): Column =
    Column.fn("forall", column, createLambda(f))

  /**
   * Returns an array of elements for which a predicate holds in a given array.
   * {{{
   *   df.select(filter(col("s"), x => x % 2 === 0))
   * }}}
   *
   * @param column
   *   the input array column. A column that evaluates to an array.
   * @param f
   *   col => predicate, the Boolean predicate to filter the input column.
   *
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def filter(column: Column, f: Column => Column): Column =
    Column.fn("filter", column, createLambda(f))

  /**
   * Returns an array of elements for which a predicate holds in a given array.
   * {{{
   *   df.select(filter(col("s"), (x, i) => i % 2 === 0))
   * }}}
   *
   * @param column
   *   the input array column. A column that evaluates to an array.
   * @param f
   *   (col, index) => predicate, the Boolean predicate to filter the input column given the
   *   index. Indices start at 0.
   *
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def filter(column: Column, f: (Column, Column) => Column): Column =
    Column.fn("filter", column, createLambda(f))

  /**
   * Applies a binary operator to an initial state and all elements in the array, and reduces this
   * to a single state. The final state is converted into the final result by applying a finish
   * function.
   * {{{
   *   df.select(aggregate(col("i"), lit(0), (acc, x) => acc + x, _ * 10))
   * }}}
   *
   * @param expr
   *   the input array column. A column that evaluates to an array.
   * @param initialValue
   *   the initial value. A column of any type.
   * @param merge
   *   (combined_value, input_value) => combined_value, the merge function to merge an input value
   *   to the combined_value.
   * @param finish
   *   combined_value => final_value, the lambda function to convert the combined value of all
   *   inputs to final result.
   *
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column of the same type as the initial value.
   */
  def aggregate(
      expr: Column,
      initialValue: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column): Column =
    Column.fn("aggregate", expr, initialValue, createLambda(merge), createLambda(finish))

  /**
   * Applies a binary operator to an initial state and all elements in the array, and reduces this
   * to a single state.
   * {{{
   *   df.select(aggregate(col("i"), lit(0), (acc, x) => acc + x))
   * }}}
   *
   * @param expr
   *   the input array column. A column that evaluates to an array.
   * @param initialValue
   *   the initial value. A column of any type.
   * @param merge
   *   (combined_value, input_value) => combined_value, the merge function to merge an input value
   *   to the combined_value
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column of the same type as the initial value.
   */
  def aggregate(expr: Column, initialValue: Column, merge: (Column, Column) => Column): Column =
    aggregate(expr, initialValue, merge, c => c)

  /**
   * Applies a binary operator to an initial state and all elements in the array, and reduces this
   * to a single state. The final state is converted into the final result by applying a finish
   * function.
   * {{{
   *   df.select(reduce(col("i"), lit(0), (acc, x) => acc + x, _ * 10))
   * }}}
   *
   * @param expr
   *   the input array column. A column that evaluates to an array.
   * @param initialValue
   *   the initial value. A column of any type.
   * @param merge
   *   (combined_value, input_value) => combined_value, the merge function to merge an input value
   *   to the combined_value.
   * @param finish
   *   combined_value => final_value, the lambda function to convert the combined value of all
   *   inputs to final result.
   *
   * @group collection_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the initial value.
   */
  def reduce(
      expr: Column,
      initialValue: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column): Column =
    Column.fn("reduce", expr, initialValue, createLambda(merge), createLambda(finish))

  /**
   * Applies a binary operator to an initial state and all elements in the array, and reduces this
   * to a single state.
   * {{{
   *   df.select(reduce(col("i"), lit(0), (acc, x) => acc + x))
   * }}}
   *
   * @param expr
   *   the input array column. A column that evaluates to an array.
   * @param initialValue
   *   the initial value. A column of any type.
   * @param merge
   *   (combined_value, input_value) => combined_value, the merge function to merge an input value
   *   to the combined_value
   * @group collection_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the initial value.
   */
  def reduce(expr: Column, initialValue: Column, merge: (Column, Column) => Column): Column =
    reduce(expr, initialValue, merge, c => c)

  /**
   * Merge two given arrays, element-wise, into a single array using a function. If one array is
   * shorter, nulls are appended at the end to match the length of the longer array, before
   * applying the function.
   * {{{
   *   df.select(zip_with(df1("val1"), df1("val2"), (x, y) => x + y))
   * }}}
   *
   * @param left
   *   the left input array column. A column that evaluates to an array.
   * @param right
   *   the right input array column. A column that evaluates to an array.
   * @param f
   *   (lCol, rCol) => col, the lambda function to merge two input columns into one column.
   *
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def zip_with(left: Column, right: Column, f: (Column, Column) => Column): Column =
    Column.fn("zip_with", left, right, createLambda(f))

  /**
   * Applies a function to every key-value pair in a map and returns a map with the results of
   * those applications as the new keys for the pairs.
   * {{{
   *   df.select(transform_keys(col("i"), (k, v) => k + v))
   * }}}
   *
   * @param expr
   *   the input map column. A column that evaluates to a map.
   * @param f
   *   (key, value) => new_key, the lambda function to transform the key of input map column
   *
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a map.
   */
  def transform_keys(expr: Column, f: (Column, Column) => Column): Column =
    Column.fn("transform_keys", expr, createLambda(f))

  /**
   * Applies a function to every key-value pair in a map and returns a map with the results of
   * those applications as the new values for the pairs.
   * {{{
   *   df.select(transform_values(col("i"), (k, v) => k + v))
   * }}}
   *
   * @param expr
   *   the input map column. A column that evaluates to a map.
   * @param f
   *   (key, value) => new_value, the lambda function to transform the value of input map column
   *
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a map.
   */
  def transform_values(expr: Column, f: (Column, Column) => Column): Column =
    Column.fn("transform_values", expr, createLambda(f))

  /**
   * Returns a map whose key-value pairs satisfy a predicate.
   * {{{
   *   df.select(map_filter(col("m"), (k, v) => k * 10 === v))
   * }}}
   *
   * @param expr
   *   the input map column. A column that evaluates to a map.
   * @param f
   *   (key, value) => predicate, the Boolean predicate to filter the input map column
   *
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a map.
   */
  def map_filter(expr: Column, f: (Column, Column) => Column): Column =
    Column.fn("map_filter", expr, createLambda(f))

  /**
   * Merge two given maps, key-wise into a single map using a function.
   * {{{
   *   df.select(map_zip_with(df("m1"), df("m2"), (k, v1, v2) => k === v1 + v2))
   * }}}
   *
   * @param left
   *   the left input map column. A column that evaluates to a map.
   * @param right
   *   the right input map column. A column that evaluates to a map.
   * @param f
   *   (key, value1, value2) => new_value, the lambda function to merge the map values
   *
   * @group collection_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a map.
   */
  def map_zip_with(left: Column, right: Column, f: (Column, Column, Column) => Column): Column =
    Column.fn("map_zip_with", left, right, createLambda(f))

  /**
   * Creates a new row for each element in the given array or map column. Uses the default column
   * name `col` for elements in the array and `key` and `value` for elements in the map unless
   * specified otherwise.
   *
   * @param e
   *   the target column to explode. A column that evaluates to an array or a map.
   * @group generator_funcs
   * @since 1.3.0
   * @return
   *   Returns a column of the element type of the input array, or the key and value columns of
   *   the input map.
   */
  def explode(e: Column): Column = Column.fn("explode", e)

  /**
   * Creates a new row for each element in the given array or map column. Uses the default column
   * name `col` for elements in the array and `key` and `value` for elements in the map unless
   * specified otherwise. Unlike explode, if the array/map is null or empty then null is produced.
   *
   * @param e
   *   the target column to explode. A column that evaluates to an array or a map.
   * @group generator_funcs
   * @since 2.2.0
   * @return
   *   Returns a column of the element type of the input array, or the key and value columns of
   *   the input map.
   */
  def explode_outer(e: Column): Column = Column.fn("explode_outer", e)

  /**
   * Creates a new row for each element with position in the given array or map column. Uses the
   * default column name `pos` for position, and `col` for elements in the array and `key` and
   * `value` for elements in the map unless specified otherwise.
   *
   * @param e
   *   the target column to explode. A column that evaluates to an array or a map.
   * @group generator_funcs
   * @since 2.1.0
   * @return
   *   Returns the position column and a column of the element type of the input array, or the
   *   position column and the key and value columns of the input map.
   */
  def posexplode(e: Column): Column = Column.fn("posexplode", e)

  /**
   * Creates a new row for each element with position in the given array or map column. Uses the
   * default column name `pos` for position, and `col` for elements in the array and `key` and
   * `value` for elements in the map unless specified otherwise. Unlike posexplode, if the
   * array/map is null or empty then the row (null, null) is produced.
   *
   * @param e
   *   the target column to explode. A column that evaluates to an array or a map.
   * @group generator_funcs
   * @since 2.2.0
   * @return
   *   Returns the position column and a column of the element type of the input array, or the
   *   position column and the key and value columns of the input map.
   */
  def posexplode_outer(e: Column): Column = Column.fn("posexplode_outer", e)

  /**
   * Creates a new row for each element in the given array of structs.
   *
   * @param e
   *   the target column to explode. A column that evaluates to an array of structs.
   * @group generator_funcs
   * @since 3.4.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  def inline(e: Column): Column = Column.fn("inline", e)

  /**
   * Creates a new row for each element in the given array of structs. Unlike inline, if the array
   * is null or empty then null is produced for each nested column.
   *
   * @param e
   *   the target column to explode. A column that evaluates to an array of structs.
   * @group generator_funcs
   * @since 3.4.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  def inline_outer(e: Column): Column = Column.fn("inline_outer", e)

  /**
   * Extracts json object from a json string based on json path specified, and returns json string
   * of the extracted json object. It will return null if the input json string is invalid.
   *
   * @param e
   *   the JSON string column. A column that evaluates to a string.
   * @param path
   *   the JSON path to extract. A column that evaluates to a string. Must be a constant.
   * @group json_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def get_json_object(e: Column, path: String): Column =
    Column.fn("get_json_object", e, lit(path))

  /**
   * Creates a new row for a json column according to the given field names.
   *
   * @param json
   *   the JSON string column. A column that evaluates to a string.
   * @param fields
   *   the field names to extract. A column that evaluates to a string. Must be a constant.
   * @group json_funcs
   * @since 1.6.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  @scala.annotation.varargs
  def json_tuple(json: Column, fields: String*): Column = {
    require(fields.nonEmpty, "at least 1 field name should be given.")
    Column.fn("json_tuple", json +: fields.map(lit): _*)
  }

  // scalastyle:off line.size.limit
  /**
   * (Scala-specific) Parses a column containing a JSON string into a `StructType` with the
   * specified schema. Returns `null`, in the case of an unparseable string.
   *
   * @param e
   *   a string column containing JSON data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the json string. A string, StructType or DataType. Must be a
   *   constant.
   * @param options
   *   options to control how the json is parsed. Accepts the same options as the json data
   *   source. See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   *
   * @group json_funcs
   * @since 2.1.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: StructType, options: Map[String, String]): Column =
    from_json(e, schema.asInstanceOf[DataType], options)

  // scalastyle:off line.size.limit
  /**
   * (Scala-specific) Parses a column containing a JSON string into a `MapType` with `StringType`
   * as keys type, `StructType` or `ArrayType` with the specified schema. Returns `null`, in the
   * case of an unparseable string.
   *
   * @param e
   *   a string column containing JSON data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the json string. A string, StructType or DataType. Must be a
   *   constant.
   * @param options
   *   options to control how the json is parsed. accepts the same options and the json data
   *   source. See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   *
   * @group json_funcs
   * @since 2.2.0
   * @return
   *   Returns a column of the type given by the schema (a struct, array, or map).
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: DataType, options: Map[String, String]): Column = {
    from_json(e, lit(schema.sql), options.iterator)
  }

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a JSON string into a `StructType` with the
   * specified schema. Returns `null`, in the case of an unparseable string.
   *
   * @param e
   *   a string column containing JSON data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the json string. A string, StructType or DataType. Must be a
   *   constant.
   * @param options
   *   options to control how the json is parsed. accepts the same options and the json data
   *   source. See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   *
   * @group json_funcs
   * @since 2.1.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: StructType, options: java.util.Map[String, String]): Column =
    from_json(e, schema, options.asScala.toMap)

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a JSON string into a `MapType` with `StringType`
   * as keys type, `StructType` or `ArrayType` with the specified schema. Returns `null`, in the
   * case of an unparseable string.
   *
   * @param e
   *   a string column containing JSON data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the json string. A string, StructType or DataType. Must be a
   *   constant.
   * @param options
   *   options to control how the json is parsed. accepts the same options and the json data
   *   source. See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   *
   * @group json_funcs
   * @since 2.2.0
   * @return
   *   Returns a column of the type given by the schema (a struct, array, or map).
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: DataType, options: java.util.Map[String, String]): Column = {
    from_json(e, schema, options.asScala.toMap)
  }

  /**
   * Parses a column containing a JSON string into a `StructType` with the specified schema.
   * Returns `null`, in the case of an unparseable string.
   *
   * @param e
   *   a string column containing JSON data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the json string. A string, StructType or DataType. Must be a
   *   constant.
   *
   * @group json_funcs
   * @since 2.1.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  def from_json(e: Column, schema: StructType): Column =
    from_json(e, schema, Map.empty[String, String])

  /**
   * Parses a column containing a JSON string into a `MapType` with `StringType` as keys type,
   * `StructType` or `ArrayType` with the specified schema. Returns `null`, in the case of an
   * unparseable string.
   *
   * @param e
   *   a string column containing JSON data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the json string. A string, StructType or DataType. Must be a
   *   constant.
   *
   * @group json_funcs
   * @since 2.2.0
   * @return
   *   Returns a column of the type given by the schema (a struct, array, or map).
   */
  def from_json(e: Column, schema: DataType): Column =
    from_json(e, schema, Map.empty[String, String])

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a JSON string into a `MapType` with `StringType`
   * as keys type, `StructType` or `ArrayType` with the specified schema. Returns `null`, in the
   * case of an unparseable string.
   *
   * @param e
   *   a string column containing JSON data. A column that evaluates to a string.
   * @param schema
   *   the schema as a DDL-formatted string. A string, StructType or DataType. Must be a constant.
   * @param options
   *   options to control how the json is parsed. accepts the same options and the json data
   *   source. See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   *
   * @group json_funcs
   * @since 2.1.0
   * @return
   *   Returns a column of the type given by the schema (a struct, array, or map).
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: String, options: java.util.Map[String, String]): Column = {
    from_json(e, schema, options.asScala.toMap)
  }

  // scalastyle:off line.size.limit
  /**
   * (Scala-specific) Parses a column containing a JSON string into a `MapType` with `StringType`
   * as keys type, `StructType` or `ArrayType` with the specified schema. Returns `null`, in the
   * case of an unparseable string.
   *
   * @param e
   *   a string column containing JSON data. A column that evaluates to a string.
   * @param schema
   *   the schema as a DDL-formatted string. A string, StructType or DataType. Must be a constant.
   * @param options
   *   options to control how the json is parsed. accepts the same options and the json data
   *   source. See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   *
   * @group json_funcs
   * @since 2.3.0
   * @return
   *   Returns a column of the type given by the schema (a struct, array, or map).
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: String, options: Map[String, String]): Column = {
    from_json(e, lit(schema), options.asJava)
  }

  /**
   * (Scala-specific) Parses a column containing a JSON string into a `MapType` with `StringType`
   * as keys type, `StructType` or `ArrayType` of `StructType`s with the specified schema. Returns
   * `null`, in the case of an unparseable string.
   *
   * @param e
   *   a string column containing JSON data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the json string. A column that evaluates to a string.
   *
   * @group json_funcs
   * @since 2.4.0
   * @return
   *   Returns a column of the type given by the schema (a struct, array, or map).
   */
  def from_json(e: Column, schema: Column): Column = {
    from_json(e, schema, Map.empty[String, String].asJava)
  }

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a JSON string into a `MapType` with `StringType`
   * as keys type, `StructType` or `ArrayType` of `StructType`s with the specified schema. Returns
   * `null`, in the case of an unparseable string.
   *
   * @param e
   *   a string column containing JSON data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the json string. A column that evaluates to a string.
   * @param options
   *   options to control how the json is parsed. accepts the same options and the json data
   *   source. See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   *
   * @group json_funcs
   * @since 2.4.0
   * @return
   *   Returns a column of the type given by the schema (a struct, array, or map).
   */
  // scalastyle:on line.size.limit
  def from_json(e: Column, schema: Column, options: java.util.Map[String, String]): Column = {
    from_json(e, schema, options.asScala.iterator)
  }

  private def from_json(
      e: Column,
      schema: Column,
      options: Iterator[(String, String)]): Column = {
    Column.fnWithOptions("from_json", options, e, schema)
  }

  /**
   * Parses a JSON string and constructs a Variant value. Returns null if the input string is not
   * a valid JSON value.
   *
   * @param json
   *   a string column that contains JSON data. A column that evaluates to a string.
   *
   * @group variant_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a variant.
   */
  def try_parse_json(json: Column): Column = Column.fn("try_parse_json", json)

  /**
   * Parses a JSON string and constructs a Variant value.
   *
   * @param json
   *   a string column that contains JSON data. A column that evaluates to a string.
   * @group variant_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a variant.
   */
  def parse_json(json: Column): Column = Column.fn("parse_json", json)

  /**
   * Converts a column containing nested inputs (array/map/struct) into a variants where maps and
   * structs are converted to variant objects which are unordered unlike SQL structs. Input maps
   * can only have string keys.
   *
   * @param col
   *   a column with a nested schema or column name. A column that evaluates to a struct, array,
   *   map, or variant.
   * @group variant_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a variant.
   */
  def to_variant_object(col: Column): Column = Column.fn("to_variant_object", col)

  /**
   * Check if a variant value is a variant null. Returns true if and only if the input is a
   * variant null and false otherwise (including in the case of SQL NULL).
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @group variant_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def is_variant_null(v: Column): Column = Column.fn("is_variant_null", v)

  /**
   * Check if a variant value is valid. Returns true if the variant is valid, false if it is
   * malformed, and NULL if the input is NULL.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @group variant_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def is_valid_variant(v: Column): Column = Column.fn("is_valid_variant", v)

  /**
   * Removes fields or array elements from a variant at the given JSONPath locations. Multiple
   * paths are applied left to right. Returns NULL if `v` is NULL; NULL paths are skipped.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the column containing the first JSONPath string. A valid path should start with `$` and is
   *   followed by one or more segments like `[123]`, `.name`, `['name']`, or `["name"]`. The root
   *   path `$` is not allowed. A column that evaluates to a string.
   * @param paths
   *   additional JSONPath arguments, applied after `path` in order. A column that evaluates to a
   *   string.
   * @group variant_funcs
   * @since 5.0.0
   * @return
   *   Returns a column that evaluates to a variant.
   */
  @scala.annotation.varargs
  def variant_delete(v: Column, path: Column, paths: Column*): Column =
    Column.fn("variant_delete", (v +: path +: paths): _*)

  /**
   * Removes fields or array elements from a variant at the given JSONPath locations. Multiple
   * paths are applied left to right. Returns NULL if `v` is NULL; NULL paths are skipped.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the first JSONPath identifying a deletion target. A valid path should start with `$` and is
   *   followed by one or more segments like `[123]`, `.name`, `['name']`, or `["name"]`. The root
   *   path `$` is not allowed. A string. Must be a constant.
   * @param paths
   *   additional JSONPath strings, applied after `path` in order. A string. Must be a constant.
   * @group variant_funcs
   * @since 5.0.0
   * @return
   *   Returns a column that evaluates to a variant.
   */
  @scala.annotation.varargs
  def variant_delete(v: Column, path: String, paths: String*): Column =
    Column.fn("variant_delete", (v +: lit(path) +: paths.map(lit)): _*)

  /**
   * Inserts a value into a variant at the given JSONPath location. An object path adds a new
   * field (error if it already exists); an array path inserts at the index, shifting later
   * elements right. Missing intermediate keys are created. Throws an error if a path segment hits
   * a value of an incompatible type. Returns NULL if any argument is NULL.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the column containing the JSONPath string identifying the insertion target. A valid path
   *   should start with `$` and is followed by one or more segments like `[123]`, `.name`,
   *   `['name']`, or `["name"]`. The root path `$` is not allowed. A column that evaluates to a
   *   string.
   * @param value
   *   the value to insert. Any expression castable to variant.
   * @group variant_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to a variant.
   */
  def variant_insert(v: Column, path: Column, value: Column): Column =
    Column.fn("variant_insert", v, path, value)

  /**
   * Inserts a value into a variant at the given JSONPath location. An object path adds a new
   * field (error if it already exists); an array path inserts at the index, shifting later
   * elements right. Missing intermediate keys are created. Throws an error if a path segment hits
   * a value of an incompatible type. Returns NULL if any argument is NULL.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the JSONPath identifying the insertion target. A valid path should start with `$` and is
   *   followed by one or more segments like `[123]`, `.name`, `['name']`, or `["name"]`. The root
   *   path `$` is not allowed. A string. Must be a constant.
   * @param value
   *   the value to insert. Any expression castable to variant.
   * @group variant_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to a variant.
   */
  def variant_insert(v: Column, path: String, value: Column): Column =
    Column.fn("variant_insert", v, lit(path), value)

  /**
   * Inserts a value into a variant at the given JSONPath location. An object path adds a new
   * field; an array path inserts at the index, shifting later elements right. Missing
   * intermediate keys are created. Returns NULL if the field already exists or a path segment
   * hits a value of an incompatible type, or if any argument is NULL.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the column containing the JSONPath string identifying the insertion target. A valid path
   *   should start with `$` and is followed by one or more segments like `[123]`, `.name`,
   *   `['name']`, or `["name"]`. The root path `$` is not allowed. A column that evaluates to a
   *   string.
   * @param value
   *   the value to insert. Any expression castable to variant.
   * @group variant_funcs
   * @since 4.3.0
   */
  def try_variant_insert(v: Column, path: Column, value: Column): Column =
    Column.fn("try_variant_insert", v, path, value)

  /**
   * Inserts a value into a variant at the given JSONPath location. An object path adds a new
   * field; an array path inserts at the index, shifting later elements right. Missing
   * intermediate keys are created. Returns NULL if the field already exists or a path segment
   * hits a value of an incompatible type, or if any argument is NULL.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the JSONPath identifying the insertion target. A valid path should start with `$` and is
   *   followed by one or more segments like `[123]`, `.name`, `['name']`, or `["name"]`. The root
   *   path `$` is not allowed. A string. Must be a constant.
   * @param value
   *   the value to insert. Any expression castable to variant.
   * @group variant_funcs
   * @since 4.3.0
   */
  def try_variant_insert(v: Column, path: String, value: Column): Column =
    Column.fn("try_variant_insert", v, lit(path), value)

  /**
   * Sets or upserts a value in a variant at the given JSONPath location. An existing object field
   * or array element at the target is replaced. A missing field, array index, or intermediate
   * path is created. Throws an error if a path segment hits a value of an incompatible type.
   * Returns NULL if any argument is NULL.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the column containing the JSONPath string identifying the set target. A valid path should
   *   start with `$` and is followed by one or more segments like `[123]`, `.name`, `['name']`,
   *   or `["name"]`. The root path `$` is not allowed. A column that evaluates to a string.
   * @param value
   *   the value to set. Any expression castable to variant.
   * @group variant_funcs
   * @since 4.3.0
   */
  def variant_set(v: Column, path: Column, value: Column): Column =
    Column.fn("variant_set", v, path, value)

  /**
   * Sets or upserts a value in a variant at the given JSONPath location. An existing object field
   * or array element at the target is replaced. A missing field, array index, or intermediate
   * path is created. Throws an error if a path segment hits a value of an incompatible type.
   * Returns NULL if any argument is NULL.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the JSONPath identifying the set target. A valid path should start with `$` and is followed
   *   by one or more segments like `[123]`, `.name`, `['name']`, or `["name"]`. The root path `$`
   *   is not allowed. A string. Must be a constant.
   * @param value
   *   the value to set. Any expression castable to variant.
   * @group variant_funcs
   * @since 4.3.0
   */
  def variant_set(v: Column, path: String, value: Column): Column =
    Column.fn("variant_set", v, lit(path), value)

  /**
   * Sets or upserts a value in a variant at the given JSONPath location. An existing object field
   * or array element at the target is replaced. A missing field, array index, or intermediate
   * path is created, unless `createIfMissing` is false, in which case the variant is left
   * unchanged. Throws an error if a path segment hits a value of an incompatible type. Returns
   * NULL if any argument is NULL.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the column containing the JSONPath string identifying the set target. A valid path should
   *   start with `$` and is followed by one or more segments like `[123]`, `.name`, `['name']`,
   *   or `["name"]`. The root path `$` is not allowed. A column that evaluates to a string.
   * @param value
   *   the value to set. Any expression castable to variant.
   * @param createIfMissing
   *   whether to create missing keys or out-of-range array indices. A boolean. Must be a
   *   constant.
   * @group variant_funcs
   * @since 4.3.0
   */
  def variant_set(v: Column, path: Column, value: Column, createIfMissing: Boolean): Column =
    Column.fn("variant_set", v, path, value, lit(createIfMissing))

  /**
   * Sets or upserts a value in a variant at the given JSONPath location. An existing object field
   * or array element at the target is replaced. A missing field, array index, or intermediate
   * path is created, unless `createIfMissing` is false, in which case the variant is left
   * unchanged. Throws an error if a path segment hits a value of an incompatible type. Returns
   * NULL if any argument is NULL.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the JSONPath identifying the set target. A valid path should start with `$` and is followed
   *   by one or more segments like `[123]`, `.name`, `['name']`, or `["name"]`. The root path `$`
   *   is not allowed. A string. Must be a constant.
   * @param value
   *   the value to set. Any expression castable to variant.
   * @param createIfMissing
   *   whether to create missing keys or out-of-range array indices. A boolean. Must be a
   *   constant.
   * @group variant_funcs
   * @since 4.3.0
   */
  def variant_set(v: Column, path: String, value: Column, createIfMissing: Boolean): Column =
    Column.fn("variant_set", v, lit(path), value, lit(createIfMissing))

  /**
   * Appends a value to the array in a variant at the given JSONPath location. Returns the variant
   * unchanged if a path key or index is absent. Throws an error if a path segment hits a value of
   * an incompatible type or the target is not an array. Returns NULL if any argument is NULL.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the column containing the JSONPath string identifying the target array. A valid path should
   *   start with `$` and is followed by zero or more segments like `[123]`, `.name`, `['name']`,
   *   or `["name"]`. A column that evaluates to a string.
   * @param value
   *   the value to append. Any expression castable to variant.
   * @group variant_funcs
   * @since 4.3.0
   */
  def variant_array_append(v: Column, path: Column, value: Column): Column =
    Column.fn("variant_array_append", v, path, value)

  /**
   * Appends a value to the array in a variant at the given JSONPath location. Returns the variant
   * unchanged if a path key or index is absent. Throws an error if a path segment hits a value of
   * an incompatible type or the target is not an array. Returns NULL if any argument is NULL.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the JSONPath identifying the target array. A valid path should start with `$` and is
   *   followed by zero or more segments like `[123]`, `.name`, `['name']`, or `["name"]`. A
   *   string. Must be a constant.
   * @param value
   *   the value to append. Any expression castable to variant.
   * @group variant_funcs
   * @since 4.3.0
   */
  def variant_array_append(v: Column, path: String, value: Column): Column =
    Column.fn("variant_array_append", v, lit(path), value)

  /**
   * Appends a value to the array in a variant at the given JSONPath location. Returns the variant
   * unchanged if a path key or index is absent. Returns NULL if a path segment hits a value of an
   * incompatible type, the target is not an array, or if any argument is NULL.
   *
   * @param v
   *   a variant column.
   * @param path
   *   the column containing the JSONPath string identifying the target array. A valid path should
   *   start with `$` and is followed by zero or more segments like `[123]`, `.name`, `['name']`,
   *   or `["name"]`.
   * @param value
   *   the value to append. Any expression castable to variant.
   * @group variant_funcs
   * @since 4.3.0
   */
  def try_variant_array_append(v: Column, path: Column, value: Column): Column =
    Column.fn("try_variant_array_append", v, path, value)

  /**
   * Appends a value to the array in a variant at the given JSONPath location. Returns the variant
   * unchanged if a path key or index is absent. Returns NULL if a path segment hits a value of an
   * incompatible type, the target is not an array, or if any argument is NULL.
   *
   * @param v
   *   a variant column.
   * @param path
   *   the JSONPath identifying the target array. A valid path should start with `$` and is
   *   followed by zero or more segments like `[123]`, `.name`, `['name']`, or `["name"]`.
   * @param value
   *   the value to append. Any expression castable to variant.
   * @group variant_funcs
   * @since 4.3.0
   */
  def try_variant_array_append(v: Column, path: String, value: Column): Column =
    Column.fn("try_variant_array_append", v, lit(path), value)

  /**
   * Extracts a sub-variant from `v` according to `path` string, and then cast the sub-variant to
   * `targetType`. Returns null if the path does not exist. Throws an exception if the cast fails.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the extraction path. A valid path should start with `$` and is followed by zero or more
   *   segments like `[123]`, `.name`, `['name']`, or `["name"]`. A string. Must be a constant.
   * @param targetType
   *   the target data type to cast into, in a DDL-formatted string. A string. Must be a constant.
   * @group variant_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the type specified by the `targetType` argument.
   */
  def variant_get(v: Column, path: String, targetType: String): Column =
    Column.fn("variant_get", v, lit(path), lit(targetType))

  /**
   * Extracts a sub-variant from `v` according to `path` column, and then cast the sub-variant to
   * `targetType`. Returns null if the path does not exist. Throws an exception if the cast fails.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the column containing the extraction path strings. A valid path string should start with
   *   `$` and is followed by zero or more segments like `[123]`, `.name`, `['name']`, or
   *   `["name"]`. A column that evaluates to a string.
   * @param targetType
   *   the target data type to cast into, in a DDL-formatted string. A string. Must be a constant.
   * @group variant_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the type specified by the `targetType` argument.
   */
  def variant_get(v: Column, path: Column, targetType: String): Column =
    Column.fn("variant_get", v, path, lit(targetType))

  /**
   * Extracts a sub-variant from `v` according to `path` string, and then cast the sub-variant to
   * `targetType`. Returns null if the path does not exist or the cast fails..
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the extraction path. A valid path should start with `$` and is followed by zero or more
   *   segments like `[123]`, `.name`, `['name']`, or `["name"]`. A string. Must be a constant.
   * @param targetType
   *   the target data type to cast into, in a DDL-formatted string. A string. Must be a constant.
   * @group variant_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the type specified by the `targetType` argument.
   */
  def try_variant_get(v: Column, path: String, targetType: String): Column =
    Column.fn("try_variant_get", v, lit(path), lit(targetType))

  /**
   * Extracts a sub-variant from `v` according to `path` column, and then cast the sub-variant to
   * `targetType`. Returns null if the path does not exist or the cast fails..
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @param path
   *   the column containing the extraction path strings. A valid path string should start with
   *   `$` and is followed by zero or more segments like `[123]`, `.name`, `['name']`, or
   *   `["name"]`. A column that evaluates to a string.
   * @param targetType
   *   the target data type to cast into, in a DDL-formatted string. A string. Must be a constant.
   * @group variant_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the type specified by the `targetType` argument.
   */
  def try_variant_get(v: Column, path: Column, targetType: String): Column =
    Column.fn("try_variant_get", v, lit(path), lit(targetType))

  /**
   * Returns schema in the SQL format of a variant.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @group variant_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def schema_of_variant(v: Column): Column = Column.fn("schema_of_variant", v)

  /**
   * Returns the merged schema in the SQL format of a variant column.
   *
   * @param v
   *   a variant column. A column that evaluates to a variant.
   * @group variant_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def schema_of_variant_agg(v: Column): Column = Column.fn("schema_of_variant_agg", v)

  /**
   * Parses a JSON string and infers its schema in DDL format.
   *
   * @param json
   *   a JSON string. A string. Must be a constant.
   *
   * @group json_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def schema_of_json(json: String): Column = schema_of_json(lit(json))

  /**
   * Parses a JSON string and infers its schema in DDL format.
   *
   * @param json
   *   a foldable string column containing a JSON string. A column that evaluates to a string.
   *
   * @group json_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def schema_of_json(json: Column): Column = Column.fn("schema_of_json", json)

  // scalastyle:off line.size.limit
  /**
   * Parses a JSON string and infers its schema in DDL format using options.
   *
   * @param json
   *   a foldable string column containing JSON data. A column that evaluates to a string.
   * @param options
   *   options to control how the json is parsed. accepts the same options and the json data
   *   source. See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   * @return
   *   a column with string literal containing schema in DDL format. Returns a column that
   *   evaluates to a string.
   *
   * @group json_funcs
   * @since 3.0.0
   */
  // scalastyle:on line.size.limit
  def schema_of_json(json: Column, options: java.util.Map[String, String]): Column =
    Column.fnWithOptions("schema_of_json", options.asScala.iterator, json)

  /**
   * Returns the number of elements in the outermost JSON array. `NULL` is returned in case of any
   * other valid JSON string, `NULL` or an invalid JSON.
   *
   * @param e
   *   the JSON array string column. A column that evaluates to a string.
   * @group json_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def json_array_length(e: Column): Column = Column.fn("json_array_length", e)

  /**
   * Returns all the keys of the outermost JSON object as an array. If a valid JSON object is
   * given, all the keys of the outermost object will be returned as an array. If it is any other
   * valid JSON string, an invalid JSON string or an empty string, the function returns null.
   *
   * @param e
   *   the JSON object string column. A column that evaluates to a string.
   * @group json_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def json_object_keys(e: Column): Column = Column.fn("json_object_keys", e)

  // scalastyle:off line.size.limit
  /**
   * (Scala-specific) Converts a column containing a `StructType`, `ArrayType` or a `MapType` into
   * a JSON string with the specified schema. Throws an exception, in the case of an unsupported
   * type.
   *
   * @param e
   *   a column containing a struct, an array, a map, or a variant. A column that evaluates to a
   *   struct, array, map, or variant.
   * @param options
   *   options to control how the struct column is converted into a json string. accepts the same
   *   options and the json data source. See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option"> Data
   *   Source Option</a> in the version you use. Additionally the function supports the `pretty`
   *   option which enables pretty JSON generation. A map of string options. Must be a constant.
   *
   * @group json_funcs
   * @since 2.1.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  // scalastyle:on line.size.limit
  def to_json(e: Column, options: Map[String, String]): Column =
    Column.fnWithOptions("to_json", options.iterator, e)

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Converts a column containing a `StructType`, `ArrayType` or a `MapType` into
   * a JSON string with the specified schema. Throws an exception, in the case of an unsupported
   * type.
   *
   * @param e
   *   a column containing a struct, an array, a map, or a variant. A column that evaluates to a
   *   struct, array, map, or variant.
   * @param options
   *   options to control how the struct column is converted into a json string. accepts the same
   *   options and the json data source. See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option"> Data
   *   Source Option</a> in the version you use. Additionally the function supports the `pretty`
   *   option which enables pretty JSON generation. A map of string options. Must be a constant.
   *
   * @group json_funcs
   * @since 2.1.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  // scalastyle:on line.size.limit
  def to_json(e: Column, options: java.util.Map[String, String]): Column =
    to_json(e, options.asScala.toMap)

  /**
   * Converts a column containing a `StructType`, `ArrayType` or a `MapType` into a JSON string
   * with the specified schema. Throws an exception, in the case of an unsupported type.
   *
   * @param e
   *   a column containing a struct, an array, a map, or a variant. A column that evaluates to a
   *   struct, array, map, or variant.
   *
   * @group json_funcs
   * @since 2.1.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def to_json(e: Column): Column =
    to_json(e, Map.empty[String, String])

  /**
   * Masks the given string value. The function replaces characters with 'X' or 'x', and numbers
   * with 'n'. This can be useful for creating copies of tables with sensitive information
   * removed.
   *
   * @param input
   *   string value to mask. Supported types: STRING, VARCHAR, CHAR. A column that evaluates to a
   *   string.
   *
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def mask(input: Column): Column = Column.fn("mask", input)

  /**
   * Masks the given string value. The function replaces upper-case characters with specific
   * character, lower-case characters with 'x', and numbers with 'n'. This can be useful for
   * creating copies of tables with sensitive information removed.
   *
   * @param input
   *   string value to mask. Supported types: STRING, VARCHAR, CHAR. A column that evaluates to a
   *   string.
   * @param upperChar
   *   character to replace upper-case characters with. Specify NULL to retain original character.
   *   A column that evaluates to a string.
   *
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def mask(input: Column, upperChar: Column): Column =
    Column.fn("mask", input, upperChar)

  /**
   * Masks the given string value. The function replaces upper-case and lower-case characters with
   * the characters specified respectively, and numbers with 'n'. This can be useful for creating
   * copies of tables with sensitive information removed.
   *
   * @param input
   *   string value to mask. Supported types: STRING, VARCHAR, CHAR. A column that evaluates to a
   *   string.
   * @param upperChar
   *   character to replace upper-case characters with. Specify NULL to retain original character.
   *   A column that evaluates to a string.
   * @param lowerChar
   *   character to replace lower-case characters with. Specify NULL to retain original character.
   *   A column that evaluates to a string.
   *
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def mask(input: Column, upperChar: Column, lowerChar: Column): Column =
    Column.fn("mask", input, upperChar, lowerChar)

  /**
   * Masks the given string value. The function replaces upper-case, lower-case characters and
   * numbers with the characters specified respectively. This can be useful for creating copies of
   * tables with sensitive information removed.
   *
   * @param input
   *   string value to mask. Supported types: STRING, VARCHAR, CHAR. A column that evaluates to a
   *   string.
   * @param upperChar
   *   character to replace upper-case characters with. Specify NULL to retain original character.
   *   A column that evaluates to a string.
   * @param lowerChar
   *   character to replace lower-case characters with. Specify NULL to retain original character.
   *   A column that evaluates to a string.
   * @param digitChar
   *   character to replace digit characters with. Specify NULL to retain original character. A
   *   column that evaluates to a string.
   *
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def mask(input: Column, upperChar: Column, lowerChar: Column, digitChar: Column): Column =
    Column.fn("mask", input, upperChar, lowerChar, digitChar)

  /**
   * Masks the given string value. This can be useful for creating copies of tables with sensitive
   * information removed.
   *
   * @param input
   *   string value to mask. Supported types: STRING, VARCHAR, CHAR. A column that evaluates to a
   *   string.
   * @param upperChar
   *   character to replace upper-case characters with. Specify NULL to retain original character.
   *   A column that evaluates to a string.
   * @param lowerChar
   *   character to replace lower-case characters with. Specify NULL to retain original character.
   *   A column that evaluates to a string.
   * @param digitChar
   *   character to replace digit characters with. Specify NULL to retain original character. A
   *   column that evaluates to a string.
   * @param otherChar
   *   character to replace all other characters with. Specify NULL to retain original character.
   *   A column that evaluates to a string.
   *
   * @group string_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def mask(
      input: Column,
      upperChar: Column,
      lowerChar: Column,
      digitChar: Column,
      otherChar: Column): Column =
    Column.fn("mask", input, upperChar, lowerChar, digitChar, otherChar)

  /**
   * Returns length of array or map.
   *
   * This function returns -1 for null input only if spark.sql.ansi.enabled is false and
   * spark.sql.legacy.sizeOfNull is true. Otherwise, it returns null for null input. With the
   * default settings, the function returns null for null input.
   *
   * @param e
   *   the target column. A column that evaluates to an array or a map.
   * @group collection_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def size(e: Column): Column = Column.fn("size", e)

  /**
   * Returns length of array or map. This is an alias of `size` function.
   *
   * This function returns -1 for null input only if spark.sql.ansi.enabled is false and
   * spark.sql.legacy.sizeOfNull is true. Otherwise, it returns null for null input. With the
   * default settings, the function returns null for null input.
   *
   * @param e
   *   the target column. A column that evaluates to an array or a map.
   * @group collection_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def cardinality(e: Column): Column = Column.fn("cardinality", e)

  /**
   * Sorts the input array for the given column in ascending order, according to the natural
   * ordering of the array elements. Null elements will be placed at the beginning of the returned
   * array.
   *
   * @param e
   *   the array column to sort. A column that evaluates to an array.
   * @group array_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def sort_array(e: Column): Column = sort_array(e, asc = true)

  /**
   * Sorts the input array for the given column in ascending or descending order, according to the
   * natural ordering of the array elements. NaN is greater than any non-NaN elements for
   * double/float type. Null elements will be placed at the beginning of the returned array in
   * ascending order or at the end of the returned array in descending order.
   *
   * @param e
   *   the array column to sort. A column that evaluates to an array.
   * @param asc
   *   whether to sort in ascending order. A column that evaluates to a boolean. Must be a
   *   constant.
   * @group array_funcs
   * @since 1.5.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def sort_array(e: Column, asc: Boolean): Column = Column.fn("sort_array", e, lit(asc))

  /**
   * Returns the minimum value in the array. NaN is greater than any non-NaN elements for
   * double/float type. NULL elements are skipped.
   *
   * @param e
   *   the array column. A column that evaluates to an array.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column of the element type of the input array.
   */
  def array_min(e: Column): Column = Column.fn("array_min", e)

  /**
   * Returns the maximum value in the array. NaN is greater than any non-NaN elements for
   * double/float type. NULL elements are skipped.
   *
   * @param e
   *   the input column. A column that evaluates to an array.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column of the element type of the input array.
   */
  def array_max(e: Column): Column = Column.fn("array_max", e)

  /**
   * Returns the total number of elements in the array. The function returns null for null input.
   *
   * @param e
   *   the input column. A column that evaluates to an array.
   * @group array_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def array_size(e: Column): Column = Column.fn("array_size", e)

  /**
   * Aggregate function: returns a list of objects with duplicates.
   *
   * @param e
   *   the input column. A column that evaluates to any type.
   * @note
   *   The function is non-deterministic because the order of collected results depends on the
   *   order of the rows which may be non-deterministic after a shuffle.
   * @group agg_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_agg(e: Column): Column = Column.fn("array_agg", e)

  /**
   * Returns a random permutation of the given array.
   *
   * @param e
   *   the input column. A column that evaluates to an array.
   * @note
   *   The function is non-deterministic.
   *
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def shuffle(e: Column): Column = shuffle(e, lit(SparkClassUtils.random.nextLong))

  /**
   * Returns a random permutation of the given array.
   *
   * @param e
   *   the input column. A column that evaluates to an array.
   * @param seed
   *   the seed for the random generator. A column that evaluates to an integral. Must be a
   *   constant.
   * @note
   *   The function is non-deterministic.
   *
   * @group array_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def shuffle(e: Column, seed: Column): Column = Column.fn("shuffle", e, seed)

  /**
   * Returns a reversed string or an array with reverse order of elements.
   * @param e
   *   the input column. A column that evaluates to a string, a binary, or an array.
   * @group collection_funcs
   * @since 1.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def reverse(e: Column): Column = Column.fn("reverse", e)

  /**
   * Creates a single array from an array of arrays. If a structure of nested arrays is deeper
   * than two levels, only one level of nesting is removed.
   * @param e
   *   the input column. A column that evaluates to an array of arrays.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def flatten(e: Column): Column = Column.fn("flatten", e)

  /**
   * Generate a sequence of integers from start to stop, incrementing by step.
   *
   * @param start
   *   the starting value (inclusive) of the sequence. A column that evaluates to an integral, a
   *   date, or a timestamp.
   * @param stop
   *   the last value (inclusive) of the sequence. A column that evaluates to an integral, a date,
   *   or a timestamp.
   * @param step
   *   the value to add to the current element to get the next element. A column that evaluates to
   *   an integral or interval.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def sequence(start: Column, stop: Column, step: Column): Column =
    Column.fn("sequence", start, stop, step)

  /**
   * Generate a sequence of integers from start to stop, incrementing by 1 if start is less than
   * or equal to stop, otherwise -1.
   *
   * @param start
   *   the starting value (inclusive) of the sequence. A column that evaluates to an integral, a
   *   date, or a timestamp.
   * @param stop
   *   the last value (inclusive) of the sequence. A column that evaluates to an integral, a date,
   *   or a timestamp.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def sequence(start: Column, stop: Column): Column = Column.fn("sequence", start, stop)

  /**
   * Creates an array containing the left argument repeated the number of times given by the right
   * argument.
   *
   * @param left
   *   the value to repeat. A column that evaluates to any type.
   * @param right
   *   the number of times to repeat the value. A column that evaluates to an integral.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_repeat(left: Column, right: Column): Column = Column.fn("array_repeat", left, right)

  /**
   * Creates an array containing the left argument repeated the number of times given by the right
   * argument.
   *
   * @param e
   *   the value to repeat. A column that evaluates to any type.
   * @param count
   *   the number of times to repeat the value. A column that evaluates to an integral. Must be a
   *   constant.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def array_repeat(e: Column, count: Int): Column = array_repeat(e, lit(count))

  /**
   * Returns true if the map contains the key.
   * @param column
   *   the input column. A column that evaluates to a map.
   * @param key
   *   the key to check for. A column that evaluates to the map's key type. Must be a constant.
   * @group map_funcs
   * @since 3.3.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def map_contains_key(column: Column, key: Any): Column =
    Column.fn("map_contains_key", column, lit(key))

  /**
   * Returns an unordered array containing the keys of the map.
   * @param e
   *   the input column. A column that evaluates to a map.
   * @group map_funcs
   * @since 2.3.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def map_keys(e: Column): Column = Column.fn("map_keys", e)

  /**
   * Returns an unordered array containing the values of the map.
   * @param e
   *   the input column. A column that evaluates to a map.
   * @group map_funcs
   * @since 2.3.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def map_values(e: Column): Column = Column.fn("map_values", e)

  /**
   * Returns an unordered array of all entries in the given map.
   * @param e
   *   the input column. A column that evaluates to a map.
   * @group map_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def map_entries(e: Column): Column = Column.fn("map_entries", e)

  /**
   * Returns a map created from the given array of entries.
   * @param e
   *   the array of entries to convert. A column that evaluates to an array of structs, each with
   *   a key and value field.
   * @group map_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to a map.
   */
  def map_from_entries(e: Column): Column = Column.fn("map_from_entries", e)

  /**
   * Returns a merged array of structs in which the N-th struct contains all N-th values of input
   * arrays.
   * @param e
   *   the columns of arrays to be merged. Each is a column that evaluates to an array.
   * @group array_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  @scala.annotation.varargs
  def arrays_zip(e: Column*): Column = Column.fn("arrays_zip", e: _*)

  /**
   * Returns the union of all the given maps.
   * @param cols
   *   the maps to merge. Each is a column that evaluates to a map.
   * @group map_funcs
   * @since 2.4.0
   * @return
   *   Returns a column that evaluates to a map.
   */
  @scala.annotation.varargs
  def map_concat(cols: Column*): Column = Column.fn("map_concat", cols: _*)

  // scalastyle:off line.size.limit
  /**
   * Parses a column containing a CSV string into a `StructType` with the specified schema.
   * Returns `null`, in the case of an unparseable string.
   *
   * @param e
   *   a string column containing CSV data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the CSV string. A string, StructType or DataType. Must be a
   *   constant.
   * @param options
   *   options to control how the CSV is parsed. accepts the same options and the CSV data source.
   *   See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   *
   * @group csv_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  // scalastyle:on line.size.limit
  def from_csv(e: Column, schema: StructType, options: Map[String, String]): Column =
    from_csv(e, lit(schema.toDDL), options.iterator)

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a CSV string into a `StructType` with the
   * specified schema. Returns `null`, in the case of an unparseable string.
   *
   * @param e
   *   a string column containing CSV data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the CSV string. A column that evaluates to a string.
   * @param options
   *   options to control how the CSV is parsed. accepts the same options and the CSV data source.
   *   See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   *
   * @group csv_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  // scalastyle:on line.size.limit
  def from_csv(e: Column, schema: Column, options: java.util.Map[String, String]): Column =
    from_csv(e, schema, options.asScala.iterator)

  private def from_csv(e: Column, schema: Column, options: Iterator[(String, String)]): Column =
    Column.fnWithOptions("from_csv", options, e, schema)

  /**
   * Parses a CSV string and infers its schema in DDL format.
   *
   * @param csv
   *   a CSV string. A string. Must be a constant.
   *
   * @group csv_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def schema_of_csv(csv: String): Column = schema_of_csv(lit(csv))

  /**
   * Parses a CSV string and infers its schema in DDL format.
   *
   * @param csv
   *   a foldable string column containing a CSV string. A column that evaluates to a string.
   *
   * @group csv_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def schema_of_csv(csv: Column): Column = schema_of_csv(csv, Collections.emptyMap())

  // scalastyle:off line.size.limit
  /**
   * Parses a CSV string and infers its schema in DDL format using options.
   *
   * @param csv
   *   a foldable string column containing a CSV string. A column that evaluates to a string.
   * @param options
   *   options to control how the CSV is parsed. accepts the same options and the CSV data source.
   *   See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   * @return
   *   a column with string literal containing schema in DDL format. Returns a column that
   *   evaluates to a string.
   * @group csv_funcs
   * @since 3.0.0
   */
  // scalastyle:on line.size.limit
  def schema_of_csv(csv: Column, options: java.util.Map[String, String]): Column =
    Column.fnWithOptions("schema_of_csv", options.asScala.iterator, csv)

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Converts a column containing a `StructType` into a CSV string with the
   * specified schema. Throws an exception, in the case of an unsupported type.
   *
   * @param e
   *   a column containing a struct. A column that evaluates to a string.
   * @param options
   *   options to control how the struct column is converted into a CSV string. It accepts the
   *   same options and the CSV data source. See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   *
   * @group csv_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  // scalastyle:on line.size.limit
  def to_csv(e: Column, options: java.util.Map[String, String]): Column =
    Column.fnWithOptions("to_csv", options.asScala.iterator, e)

  /**
   * Converts a column containing a `StructType` into a CSV string with the specified schema.
   * Throws an exception, in the case of an unsupported type.
   *
   * @param e
   *   a column containing a struct. A column that evaluates to a string.
   *
   * @group csv_funcs
   * @since 3.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def to_csv(e: Column): Column = to_csv(e, Map.empty[String, String].asJava)

  // scalastyle:off line.size.limit
  /**
   * Parses a column containing a XML string into the data type corresponding to the specified
   * schema. Returns `null`, in the case of an unparseable string.
   *
   * @param e
   *   a string column containing XML data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the XML string. A string, StructType or DataType. Must be a
   *   constant.
   * @param options
   *   options to control how the XML is parsed. accepts the same options and the XML data source.
   *   See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   * @group xml_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  // scalastyle:on line.size.limit
  def from_xml(e: Column, schema: StructType, options: java.util.Map[String, String]): Column =
    from_xml(e, lit(schema.sql), options.asScala.iterator)

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a XML string into a `StructType` with the
   * specified schema. Returns `null`, in the case of an unparseable string.
   *
   * @param e
   *   a string column containing XML data. A column that evaluates to a string.
   * @param schema
   *   the schema as a DDL-formatted string. A string, StructType or DataType. Must be a constant.
   * @param options
   *   options to control how the XML is parsed. accepts the same options and the xml data source.
   *   See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   * @group xml_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  // scalastyle:on line.size.limit
  def from_xml(e: Column, schema: String, options: java.util.Map[String, String]): Column = {
    from_xml(e, lit(schema), options)
  }

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a XML string into a `StructType` with the
   * specified schema. Returns `null`, in the case of an unparseable string.
   *
   * @param e
   *   a string column containing XML data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the XML string. A column that evaluates to a string.
   * @group xml_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  // scalastyle:on line.size.limit
  def from_xml(e: Column, schema: Column): Column = {
    from_xml(e, schema, Iterator.empty)
  }

  // scalastyle:off line.size.limit
  /**
   * (Java-specific) Parses a column containing a XML string into a `StructType` with the
   * specified schema. Returns `null`, in the case of an unparseable string.
   *
   * @param e
   *   a string column containing XML data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the XML string. A column that evaluates to a string.
   * @param options
   *   options to control how the XML is parsed. accepts the same options and the XML data source.
   *   See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   * @group xml_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  // scalastyle:on line.size.limit
  def from_xml(e: Column, schema: Column, options: java.util.Map[String, String]): Column =
    from_xml(e, schema, options.asScala.iterator)

  /**
   * Parses a column containing a XML string into the data type corresponding to the specified
   * schema. Returns `null`, in the case of an unparseable string.
   *
   * @param e
   *   a string column containing XML data. A column that evaluates to a string.
   * @param schema
   *   the schema to use when parsing the XML string. A string, StructType or DataType. Must be a
   *   constant.
   *
   * @group xml_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a struct.
   */
  def from_xml(e: Column, schema: StructType): Column =
    from_xml(e, schema, Map.empty[String, String].asJava)

  private def from_xml(e: Column, schema: Column, options: Iterator[(String, String)]): Column = {
    Column.fnWithOptions("from_xml", options, e, schema)
  }

  /**
   * Parses a XML string and infers its schema in DDL format.
   *
   * @param xml
   *   a XML string. A string. Must be a constant.
   * @group xml_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def schema_of_xml(xml: String): Column = schema_of_xml(lit(xml))

  /**
   * Parses a XML string and infers its schema in DDL format.
   *
   * @param xml
   *   a foldable string column containing a XML string. A column that evaluates to a string.
   * @group xml_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def schema_of_xml(xml: Column): Column = Column.fn("schema_of_xml", xml)

  // scalastyle:off line.size.limit

  /**
   * Parses a XML string and infers its schema in DDL format using options.
   *
   * @param xml
   *   a foldable string column containing XML data. A column that evaluates to a string.
   * @param options
   *   options to control how the xml is parsed. accepts the same options and the XML data source.
   *   See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   * @return
   *   a column with string literal containing schema in DDL format. Returns a column that
   *   evaluates to a string.
   * @group xml_funcs
   * @since 4.0.0
   */
  // scalastyle:on line.size.limit
  def schema_of_xml(xml: Column, options: java.util.Map[String, String]): Column =
    Column.fnWithOptions("schema_of_xml", options.asScala.iterator, xml)

  // scalastyle:off line.size.limit

  /**
   * (Java-specific) Converts a column containing a `StructType` into a XML string with the
   * specified schema. Throws an exception, in the case of an unsupported type.
   *
   * @param e
   *   a column containing a struct. A column that evaluates to a string.
   * @param options
   *   options to control how the struct column is converted into a XML string. It accepts the
   *   same options as the XML data source. See <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option"> Data
   *   Source Option</a> in the version you use. A map of string options. Must be a constant.
   * @group xml_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  // scalastyle:on line.size.limit
  def to_xml(e: Column, options: java.util.Map[String, String]): Column =
    Column.fnWithOptions("to_xml", options.asScala.iterator, e)

  /**
   * Converts a column containing a `StructType` into a XML string with the specified schema.
   * Throws an exception, in the case of an unsupported type.
   *
   * @param e
   *   a column containing a struct. A column that evaluates to a string.
   * @group xml_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def to_xml(e: Column): Column = to_xml(e, Map.empty[String, String].asJava)

  /**
   * (Java-specific) A transform for timestamps and dates to partition data into years.
   *
   * @param e
   *   the target column to transform. A column that evaluates to a date or a timestamp.
   * @group partition_transforms
   * @since 3.0.0
   */
  def years(e: Column): Column = partitioning.years(e)

  /**
   * (Java-specific) A transform for timestamps and dates to partition data into months.
   *
   * @param e
   *   the target column to transform. A column that evaluates to a date or a timestamp.
   * @group partition_transforms
   * @since 3.0.0
   */
  def months(e: Column): Column = partitioning.months(e)

  /**
   * (Java-specific) A transform for timestamps and dates to partition data into days.
   *
   * @param e
   *   the target column to transform. A column that evaluates to a date or a timestamp.
   * @group partition_transforms
   * @since 3.0.0
   */
  def days(e: Column): Column = partitioning.days(e)

  /**
   * Returns a string array of values within the nodes of xml that match the XPath expression.
   *
   * @param xml
   *   the XML column to evaluate. A column that evaluates to a string.
   * @param path
   *   the XPath expression to match. A column that evaluates to a string. Must be a constant.
   * @group xml_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def xpath(xml: Column, path: Column): Column =
    Column.fn("xpath", xml, path)

  /**
   * Returns true if the XPath expression evaluates to true, or if a matching node is found.
   *
   * @param xml
   *   the XML column to evaluate. A column that evaluates to a string.
   * @param path
   *   the XPath expression to match. A column that evaluates to a string. Must be a constant.
   * @group xml_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def xpath_boolean(xml: Column, path: Column): Column =
    Column.fn("xpath_boolean", xml, path)

  /**
   * Returns a double value, the value zero if no match is found, or NaN if a match is found but
   * the value is non-numeric.
   *
   * @param xml
   *   the XML column to evaluate. A column that evaluates to a string.
   * @param path
   *   the XPath expression to match. A column that evaluates to a string. Must be a constant.
   * @group xml_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def xpath_double(xml: Column, path: Column): Column =
    Column.fn("xpath_double", xml, path)

  /**
   * Returns a double value, the value zero if no match is found, or NaN if a match is found but
   * the value is non-numeric.
   *
   * @param xml
   *   the XML column to evaluate. A column that evaluates to a string.
   * @param path
   *   the XPath expression to match. A column that evaluates to a string. Must be a constant.
   * @group xml_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a double.
   */
  def xpath_number(xml: Column, path: Column): Column =
    Column.fn("xpath_number", xml, path)

  /**
   * Returns a float value, the value zero if no match is found, or NaN if a match is found but
   * the value is non-numeric.
   *
   * @param xml
   *   the XML column to evaluate. A column that evaluates to a string.
   * @param path
   *   the XPath expression to match. A column that evaluates to a string. Must be a constant.
   * @group xml_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a float.
   */
  def xpath_float(xml: Column, path: Column): Column =
    Column.fn("xpath_float", xml, path)

  /**
   * Returns an integer value, or the value zero if no match is found, or a match is found but the
   * value is non-numeric.
   *
   * @param xml
   *   the XML column to evaluate. A column that evaluates to a string.
   * @param path
   *   the XPath expression to match. A column that evaluates to a string. Must be a constant.
   * @group xml_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def xpath_int(xml: Column, path: Column): Column =
    Column.fn("xpath_int", xml, path)

  /**
   * Returns a long integer value, or the value zero if no match is found, or a match is found but
   * the value is non-numeric.
   *
   * @param xml
   *   the XML column to evaluate. A column that evaluates to a string.
   * @param path
   *   the XPath expression to match. A column that evaluates to a string. Must be a constant.
   * @group xml_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a long.
   */
  def xpath_long(xml: Column, path: Column): Column =
    Column.fn("xpath_long", xml, path)

  /**
   * Returns a short integer value, or the value zero if no match is found, or a match is found
   * but the value is non-numeric.
   *
   * @param xml
   *   the XML column to evaluate. A column that evaluates to a string.
   * @param path
   *   the XPath expression to match. A column that evaluates to a string. Must be a constant.
   * @group xml_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a short.
   */
  def xpath_short(xml: Column, path: Column): Column =
    Column.fn("xpath_short", xml, path)

  /**
   * Returns the text contents of the first xml node that matches the XPath expression.
   *
   * @param xml
   *   the XML column to evaluate. A column that evaluates to a string.
   * @param path
   *   the XPath expression to match. A column that evaluates to a string. Must be a constant.
   * @group xml_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a string.
   */
  def xpath_string(xml: Column, path: Column): Column =
    Column.fn("xpath_string", xml, path)

  /**
   * (Java-specific) A transform for timestamps to partition data into hours.
   *
   * @param e
   *   target date or timestamp column to work on. A column that evaluates to a date or timestamp.
   * @group partition_transforms
   * @since 3.0.0
   */
  def hours(e: Column): Column = partitioning.hours(e)

  /**
   * Converts the timestamp without time zone `sourceTs` from the `sourceTz` time zone to
   * `targetTz`.
   *
   * @param sourceTz
   *   the time zone for the input timestamp. If it is missed, the current session time zone is
   *   used as the source time zone. A column that evaluates to a string.
   * @param targetTz
   *   the time zone to which the input timestamp should be converted. A column that evaluates to
   *   a string.
   * @param sourceTs
   *   a timestamp without time zone. A column that evaluates to a timestamp.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def convert_timezone(sourceTz: Column, targetTz: Column, sourceTs: Column): Column =
    Column.fn("convert_timezone", sourceTz, targetTz, sourceTs)

  /**
   * Converts the timestamp without time zone `sourceTs` from the current time zone to `targetTz`.
   *
   * @param targetTz
   *   the time zone to which the input timestamp should be converted. A column that evaluates to
   *   a string.
   * @param sourceTs
   *   a timestamp without time zone. A column that evaluates to a timestamp.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def convert_timezone(targetTz: Column, sourceTs: Column): Column =
    Column.fn("convert_timezone", targetTz, sourceTs)

  /**
   * Make DayTimeIntervalType duration from days, hours, mins and secs.
   *
   * @param days
   *   the number of days, positive or negative. A column that evaluates to an integral.
   * @param hours
   *   the number of hours, positive or negative. A column that evaluates to an integral.
   * @param mins
   *   the number of minutes, positive or negative. A column that evaluates to an integral.
   * @param secs
   *   the number of seconds with the fractional part in microsecond precision. A column that
   *   evaluates to a numeric.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_dt_interval(days: Column, hours: Column, mins: Column, secs: Column): Column =
    Column.fn("make_dt_interval", days, hours, mins, secs)

  /**
   * Make DayTimeIntervalType duration from days, hours and mins.
   *
   * @param days
   *   the number of days, positive or negative. A column that evaluates to an integral.
   * @param hours
   *   the number of hours, positive or negative. A column that evaluates to an integral.
   * @param mins
   *   the number of minutes, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_dt_interval(days: Column, hours: Column, mins: Column): Column =
    Column.fn("make_dt_interval", days, hours, mins)

  /**
   * Make DayTimeIntervalType duration from days and hours.
   *
   * @param days
   *   the number of days, positive or negative. A column that evaluates to an integral.
   * @param hours
   *   the number of hours, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_dt_interval(days: Column, hours: Column): Column =
    Column.fn("make_dt_interval", days, hours)

  /**
   * Make DayTimeIntervalType duration from days.
   *
   * @param days
   *   the number of days, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_dt_interval(days: Column): Column =
    Column.fn("make_dt_interval", days)

  /**
   * Make DayTimeIntervalType duration.
   *
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_dt_interval(): Column =
    Column.fn("make_dt_interval")

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   *
   * @param years
   *   the number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   the number of months, positive or negative. A column that evaluates to an integral.
   * @param weeks
   *   the number of weeks, positive or negative. A column that evaluates to an integral.
   * @param days
   *   the number of days, positive or negative. A column that evaluates to an integral.
   * @param hours
   *   the number of hours, positive or negative. A column that evaluates to an integral.
   * @param mins
   *   the number of minutes, positive or negative. A column that evaluates to an integral.
   * @param secs
   *   the number of seconds with the fractional part in microsecond precision. A column that
   *   evaluates to a numeric.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def try_make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column): Column =
    Column.fn("try_make_interval", years, months, weeks, days, hours, mins, secs)

  /**
   * Make interval from years, months, weeks, days, hours, mins and secs.
   *
   * @param years
   *   the number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   the number of months, positive or negative. A column that evaluates to an integral.
   * @param weeks
   *   the number of weeks, positive or negative. A column that evaluates to an integral.
   * @param days
   *   the number of days, positive or negative. A column that evaluates to an integral.
   * @param hours
   *   the number of hours, positive or negative. A column that evaluates to an integral.
   * @param mins
   *   the number of minutes, positive or negative. A column that evaluates to an integral.
   * @param secs
   *   the number of seconds with the fractional part in microsecond precision. A column that
   *   evaluates to a numeric.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column): Column =
    Column.fn("make_interval", years, months, weeks, days, hours, mins, secs)

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   *
   * @param years
   *   the number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   the number of months, positive or negative. A column that evaluates to an integral.
   * @param weeks
   *   the number of weeks, positive or negative. A column that evaluates to an integral.
   * @param days
   *   the number of days, positive or negative. A column that evaluates to an integral.
   * @param hours
   *   the number of hours, positive or negative. A column that evaluates to an integral.
   * @param mins
   *   the number of minutes, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def try_make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column,
      mins: Column): Column =
    Column.fn("try_make_interval", years, months, weeks, days, hours, mins)

  /**
   * Make interval from years, months, weeks, days, hours and mins.
   *
   * @param years
   *   the number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   the number of months, positive or negative. A column that evaluates to an integral.
   * @param weeks
   *   the number of weeks, positive or negative. A column that evaluates to an integral.
   * @param days
   *   the number of days, positive or negative. A column that evaluates to an integral.
   * @param hours
   *   the number of hours, positive or negative. A column that evaluates to an integral.
   * @param mins
   *   the number of minutes, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column,
      mins: Column): Column =
    Column.fn("make_interval", years, months, weeks, days, hours, mins)

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   *
   * @param years
   *   the number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   the number of months, positive or negative. A column that evaluates to an integral.
   * @param weeks
   *   the number of weeks, positive or negative. A column that evaluates to an integral.
   * @param days
   *   the number of days, positive or negative. A column that evaluates to an integral.
   * @param hours
   *   the number of hours, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def try_make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column): Column =
    Column.fn("try_make_interval", years, months, weeks, days, hours)

  /**
   * Make interval from years, months, weeks, days and hours.
   *
   * @param years
   *   the number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   the number of months, positive or negative. A column that evaluates to an integral.
   * @param weeks
   *   the number of weeks, positive or negative. A column that evaluates to an integral.
   * @param days
   *   the number of days, positive or negative. A column that evaluates to an integral.
   * @param hours
   *   the number of hours, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column): Column =
    Column.fn("make_interval", years, months, weeks, days, hours)

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   *
   * @param years
   *   the number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   the number of months, positive or negative. A column that evaluates to an integral.
   * @param weeks
   *   the number of weeks, positive or negative. A column that evaluates to an integral.
   * @param days
   *   the number of days, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def try_make_interval(years: Column, months: Column, weeks: Column, days: Column): Column =
    Column.fn("try_make_interval", years, months, weeks, days)

  /**
   * Make interval from years, months, weeks and days.
   *
   * @param years
   *   the number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   the number of months, positive or negative. A column that evaluates to an integral.
   * @param weeks
   *   the number of weeks, positive or negative. A column that evaluates to an integral.
   * @param days
   *   the number of days, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_interval(years: Column, months: Column, weeks: Column, days: Column): Column =
    Column.fn("make_interval", years, months, weeks, days)

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   *
   * @param years
   *   the number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   the number of months, positive or negative. A column that evaluates to an integral.
   * @param weeks
   *   the number of weeks, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def try_make_interval(years: Column, months: Column, weeks: Column): Column =
    Column.fn("try_make_interval", years, months, weeks)

  /**
   * Make interval from years, months and weeks.
   *
   * @param years
   *   The number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   The number of months, positive or negative. A column that evaluates to an integral.
   * @param weeks
   *   The number of weeks, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_interval(years: Column, months: Column, weeks: Column): Column =
    Column.fn("make_interval", years, months, weeks)

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   *
   * @param years
   *   The number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   The number of months, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def try_make_interval(years: Column, months: Column): Column =
    Column.fn("try_make_interval", years, months)

  /**
   * Make interval from years and months.
   *
   * @param years
   *   The number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   The number of months, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_interval(years: Column, months: Column): Column =
    Column.fn("make_interval", years, months)

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   *
   * @param years
   *   The number of years, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def try_make_interval(years: Column): Column =
    Column.fn("try_make_interval", years)

  /**
   * Make interval from years.
   *
   * @param years
   *   The number of years, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_interval(years: Column): Column =
    Column.fn("make_interval", years)

  /**
   * Make interval.
   *
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_interval(): Column =
    Column.fn("make_interval")

  /**
   * Create timestamp from years, months, days, hours, mins, secs and timezone fields. The result
   * data type is consistent with the value of configuration `spark.sql.timestampType`. If the
   * configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs.
   * Otherwise, it will throw an error instead.
   *
   * @param years
   *   The year to represent, from 1 to 9999. A column that evaluates to an integral.
   * @param months
   *   The month-of-year to represent, from 1 to 12. A column that evaluates to an integral.
   * @param days
   *   The day-of-month to represent, from 1 to 31. A column that evaluates to an integral.
   * @param hours
   *   The hour-of-day to represent, from 0 to 23. A column that evaluates to an integral.
   * @param mins
   *   The minute-of-hour to represent, from 0 to 59. A column that evaluates to an integral.
   * @param secs
   *   The second-of-minute and its micro-fraction to represent, from 0 to 60. A column that
   *   evaluates to a numeric.
   * @param timezone
   *   The time zone identifier. A column that evaluates to a string.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def make_timestamp(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column,
      timezone: Column): Column =
    Column.fn("make_timestamp", years, months, days, hours, mins, secs, timezone)

  /**
   * Create timestamp from years, months, days, hours, mins and secs fields. The result data type
   * is consistent with the value of configuration `spark.sql.timestampType`. If the configuration
   * `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs. Otherwise, it
   * will throw an error instead.
   *
   * @param years
   *   The year to represent, from 1 to 9999. A column that evaluates to an integral.
   * @param months
   *   The month-of-year to represent, from 1 to 12. A column that evaluates to an integral.
   * @param days
   *   The day-of-month to represent, from 1 to 31. A column that evaluates to an integral.
   * @param hours
   *   The hour-of-day to represent, from 0 to 23. A column that evaluates to an integral.
   * @param mins
   *   The minute-of-hour to represent, from 0 to 59. A column that evaluates to an integral.
   * @param secs
   *   The second-of-minute and its micro-fraction to represent, from 0 to 60. A column that
   *   evaluates to a numeric.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def make_timestamp(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column): Column =
    Column.fn("make_timestamp", years, months, days, hours, mins, secs)

  /**
   * Create a local date-time from date, time, and timezone fields.
   *
   * @param date
   *   The date to represent, in valid DATE format. A column that evaluates to a date.
   * @param time
   *   The time to represent, in valid TIME format. A column that evaluates to a time.
   * @param timezone
   *   The time zone identifier. A column that evaluates to a string.
   * @group datetime_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def make_timestamp(date: Column, time: Column, timezone: Column): Column =
    Column.fn("make_timestamp", date, time, timezone)

  /**
   * Create a local date-time from date and time fields.
   *
   * @param date
   *   The date to represent, in valid DATE format. A column that evaluates to a date.
   * @param time
   *   The time to represent, in valid TIME format. A column that evaluates to a time.
   * @group datetime_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def make_timestamp(date: Column, time: Column): Column =
    Column.fn("make_timestamp", date, time)

  /**
   * Try to create a timestamp from years, months, days, hours, mins, secs and timezone fields.
   * The result data type is consistent with the value of configuration `spark.sql.timestampType`.
   * The function returns NULL on invalid inputs.
   *
   * @param years
   *   The year to represent, from 1 to 9999. A column that evaluates to an integral.
   * @param months
   *   The month-of-year to represent, from 1 to 12. A column that evaluates to an integral.
   * @param days
   *   The day-of-month to represent, from 1 to 31. A column that evaluates to an integral.
   * @param hours
   *   The hour-of-day to represent, from 0 to 23. A column that evaluates to an integral.
   * @param mins
   *   The minute-of-hour to represent, from 0 to 59. A column that evaluates to an integral.
   * @param secs
   *   The second-of-minute and its micro-fraction to represent, from 0 to 60. A column that
   *   evaluates to a numeric.
   * @param timezone
   *   The time zone identifier. A column that evaluates to a string.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def try_make_timestamp(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column,
      timezone: Column): Column =
    Column.fn("try_make_timestamp", years, months, days, hours, mins, secs, timezone)

  /**
   * Try to create a timestamp from years, months, days, hours, mins, and secs fields. The result
   * data type is consistent with the value of configuration `spark.sql.timestampType`. The
   * function returns NULL on invalid inputs.
   *
   * @param years
   *   The year to represent, from 1 to 9999. A column that evaluates to an integral.
   * @param months
   *   The month-of-year to represent, from 1 to 12. A column that evaluates to an integral.
   * @param days
   *   The day-of-month to represent, from 1 to 31. A column that evaluates to an integral.
   * @param hours
   *   The hour-of-day to represent, from 0 to 23. A column that evaluates to an integral.
   * @param mins
   *   The minute-of-hour to represent, from 0 to 59. A column that evaluates to an integral.
   * @param secs
   *   The second-of-minute and its micro-fraction to represent, from 0 to 60. A column that
   *   evaluates to a numeric.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def try_make_timestamp(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column): Column =
    Column.fn("try_make_timestamp", years, months, days, hours, mins, secs)

  /**
   * Try to create a local date-time from date, time, and timezone fields.
   *
   * @param date
   *   The date to represent, in valid DATE format. A column that evaluates to a date.
   * @param time
   *   The time to represent, in valid TIME format. A column that evaluates to a time.
   * @param timezone
   *   The time zone identifier. A column that evaluates to a string.
   * @group datetime_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def try_make_timestamp(date: Column, time: Column, timezone: Column): Column =
    Column.fn("try_make_timestamp", date, time, timezone)

  /**
   * Try to create a local date-time from date and time fields.
   *
   * @param date
   *   The date to represent, in valid DATE format. A column that evaluates to a date.
   * @param time
   *   The time to represent, in valid TIME format. A column that evaluates to a time.
   * @group datetime_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def try_make_timestamp(date: Column, time: Column): Column =
    Column.fn("try_make_timestamp", date, time)

  /**
   * Create the current timestamp with local time zone from years, months, days, hours, mins, secs
   * and timezone fields. If the configuration `spark.sql.ansi.enabled` is false, the function
   * returns NULL on invalid inputs. Otherwise, it will throw an error instead.
   *
   * @param years
   *   The year to represent, from 1 to 9999. A column that evaluates to an integral.
   * @param months
   *   The month-of-year to represent, from 1 to 12. A column that evaluates to an integral.
   * @param days
   *   The day-of-month to represent, from 1 to 31. A column that evaluates to an integral.
   * @param hours
   *   The hour-of-day to represent, from 0 to 23. A column that evaluates to an integral.
   * @param mins
   *   The minute-of-hour to represent, from 0 to 59. A column that evaluates to an integral.
   * @param secs
   *   The second-of-minute and its micro-fraction to represent, from 0 to 60. A column that
   *   evaluates to a numeric.
   * @param timezone
   *   The time zone identifier. A column that evaluates to a string.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def make_timestamp_ltz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column,
      timezone: Column): Column =
    Column.fn("make_timestamp_ltz", years, months, days, hours, mins, secs, timezone)

  /**
   * Create the current timestamp with local time zone from years, months, days, hours, mins and
   * secs fields. If the configuration `spark.sql.ansi.enabled` is false, the function returns
   * NULL on invalid inputs. Otherwise, it will throw an error instead.
   *
   * @param years
   *   The year to represent, from 1 to 9999. A column that evaluates to an integral.
   * @param months
   *   The month-of-year to represent, from 1 (January) to 12 (December). A column that evaluates
   *   to an integral.
   * @param days
   *   The day-of-month to represent, from 1 to 31. A column that evaluates to an integral.
   * @param hours
   *   The hour-of-day to represent, from 0 to 23. A column that evaluates to an integral.
   * @param mins
   *   The minute-of-hour to represent, from 0 to 59. A column that evaluates to an integral.
   * @param secs
   *   The second-of-minute and its micro-fraction to represent, from 0 to 60. A column that
   *   evaluates to a numeric.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def make_timestamp_ltz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column): Column =
    Column.fn("make_timestamp_ltz", years, months, days, hours, mins, secs)

  /**
   * Try to create the current timestamp with local time zone from years, months, days, hours,
   * mins, secs and timezone fields. The function returns NULL on invalid inputs.
   *
   * @param years
   *   The year to represent, from 1 to 9999. A column that evaluates to an integral.
   * @param months
   *   The month-of-year to represent, from 1 (January) to 12 (December). A column that evaluates
   *   to an integral.
   * @param days
   *   The day-of-month to represent, from 1 to 31. A column that evaluates to an integral.
   * @param hours
   *   The hour-of-day to represent, from 0 to 23. A column that evaluates to an integral.
   * @param mins
   *   The minute-of-hour to represent, from 0 to 59. A column that evaluates to an integral.
   * @param secs
   *   The second-of-minute and its micro-fraction to represent, from 0 to 60. A column that
   *   evaluates to a numeric.
   * @param timezone
   *   The time zone identifier. A column that evaluates to a string.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def try_make_timestamp_ltz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column,
      timezone: Column): Column =
    Column.fn("try_make_timestamp_ltz", years, months, days, hours, mins, secs, timezone)

  /**
   * Try to create the current timestamp with local time zone from years, months, days, hours,
   * mins and secs fields. The function returns NULL on invalid inputs.
   *
   * @param years
   *   The year to represent, from 1 to 9999. A column that evaluates to an integral.
   * @param months
   *   The month-of-year to represent, from 1 (January) to 12 (December). A column that evaluates
   *   to an integral.
   * @param days
   *   The day-of-month to represent, from 1 to 31. A column that evaluates to an integral.
   * @param hours
   *   The hour-of-day to represent, from 0 to 23. A column that evaluates to an integral.
   * @param mins
   *   The minute-of-hour to represent, from 0 to 59. A column that evaluates to an integral.
   * @param secs
   *   The second-of-minute and its micro-fraction to represent, from 0 to 60. A column that
   *   evaluates to a numeric.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def try_make_timestamp_ltz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column): Column =
    Column.fn("try_make_timestamp_ltz", years, months, days, hours, mins, secs)

  /**
   * Create local date-time from years, months, days, hours, mins, secs fields. If the
   * configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs.
   * Otherwise, it will throw an error instead.
   *
   * @param years
   *   The year to represent, from 1 to 9999. A column that evaluates to an integral.
   * @param months
   *   The month-of-year to represent, from 1 (January) to 12 (December). A column that evaluates
   *   to an integral.
   * @param days
   *   The day-of-month to represent, from 1 to 31. A column that evaluates to an integral.
   * @param hours
   *   The hour-of-day to represent, from 0 to 23. A column that evaluates to an integral.
   * @param mins
   *   The minute-of-hour to represent, from 0 to 59. A column that evaluates to an integral.
   * @param secs
   *   The second-of-minute and its micro-fraction to represent, from 0 to 60. A column that
   *   evaluates to a numeric.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def make_timestamp_ntz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column): Column =
    Column.fn("make_timestamp_ntz", years, months, days, hours, mins, secs)

  /**
   * Create a local date-time from date and time fields.
   *
   * @param date
   *   The date to represent, in valid DATE format. A column that evaluates to a date.
   * @param time
   *   The time to represent, in valid TIME format. A column that evaluates to a time.
   * @group datetime_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def make_timestamp_ntz(date: Column, time: Column): Column =
    Column.fn("make_timestamp_ntz", date, time)

  /**
   * Try to create a local date-time from years, months, days, hours, mins, secs fields. The
   * function returns NULL on invalid inputs.
   *
   * @param years
   *   The year to represent, from 1 to 9999. A column that evaluates to an integral.
   * @param months
   *   The month-of-year to represent, from 1 (January) to 12 (December). A column that evaluates
   *   to an integral.
   * @param days
   *   The day-of-month to represent, from 1 to 31. A column that evaluates to an integral.
   * @param hours
   *   The hour-of-day to represent, from 0 to 23. A column that evaluates to an integral.
   * @param mins
   *   The minute-of-hour to represent, from 0 to 59. A column that evaluates to an integral.
   * @param secs
   *   The second-of-minute and its micro-fraction to represent, from 0 to 60. A column that
   *   evaluates to a numeric.
   * @group datetime_funcs
   * @since 4.0.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def try_make_timestamp_ntz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column): Column =
    Column.fn("try_make_timestamp_ntz", years, months, days, hours, mins, secs)

  /**
   * Try to create a local date-time from date and time fields.
   *
   * @param date
   *   The date to represent, in valid DATE format. A column that evaluates to a date.
   * @param time
   *   The time to represent, in valid TIME format. A column that evaluates to a time.
   * @group datetime_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a timestamp.
   */
  def try_make_timestamp_ntz(date: Column, time: Column): Column =
    Column.fn("try_make_timestamp_ntz", date, time)

  /**
   * Make year-month interval from years, months.
   *
   * @param years
   *   The number of years, positive or negative. A column that evaluates to an integral.
   * @param months
   *   The number of months, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_ym_interval(years: Column, months: Column): Column =
    Column.fn("make_ym_interval", years, months)

  /**
   * Make year-month interval from years.
   *
   * @param years
   *   The number of years, positive or negative. A column that evaluates to an integral.
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_ym_interval(years: Column): Column = Column.fn("make_ym_interval", years)

  /**
   * Make year-month interval.
   *
   * @group datetime_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to an interval.
   */
  def make_ym_interval(): Column = Column.fn("make_ym_interval")

  /**
   * (Java-specific) A transform for any type that partitions by a hash of the input column.
   *
   * @param numBuckets
   *   The number of buckets. A column that evaluates to an integral. Must be a constant.
   * @param e
   *   The input column to partition. A column of any type.
   * @group partition_transforms
   * @since 3.0.0
   */
  def bucket(numBuckets: Column, e: Column): Column = partitioning.bucket(numBuckets, e)

  /**
   * (Java-specific) A transform for any type that partitions by a hash of the input column.
   *
   * @param numBuckets
   *   The number of buckets. Must be a constant.
   * @param e
   *   The input column to partition. A column of any type.
   * @group partition_transforms
   * @since 3.0.0
   */
  def bucket(numBuckets: Int, e: Column): Column = partitioning.bucket(numBuckets, e)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Predicates functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns `col2` if `col1` is null, or `col1` otherwise.
   *
   * @param col1
   *   The column to test for null. A column of any type.
   * @param col2
   *   The column to return when col1 is null. A column of any type.
   * @group conditional_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def ifnull(col1: Column, col2: Column): Column = Column.fn("ifnull", col1, col2)

  /**
   * Returns true if `col` is not null, or false otherwise.
   *
   * @param col
   *   The column to check. A column of any type.
   * @group predicate_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def isnotnull(col: Column): Column = Column.fn("isnotnull", col)

  /**
   * Returns same result as the EQUAL(=) operator for non-null operands, but returns true if both
   * are null, false if one of the them is null.
   *
   * @param col1
   *   The first column to compare. A column of any type.
   * @param col2
   *   The second column to compare. A column of any type.
   * @group predicate_funcs
   * @since 3.5.0
   * @return
   *   Returns a column that evaluates to a boolean.
   */
  def equal_null(col1: Column, col2: Column): Column = Column.fn("equal_null", col1, col2)

  /**
   * Returns null if `col1` equals to `col2`, or `col1` otherwise.
   *
   * @param col1
   *   The value to return if it is not equal to `col2`. A column of any type.
   * @param col2
   *   The value compared with `col1`. A column of any type.
   * @group conditional_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def nullif(col1: Column, col2: Column): Column = Column.fn("nullif", col1, col2)

  /**
   * Returns null if `col` is equal to zero, or `col` otherwise.
   *
   * @param col
   *   The input value. A column that evaluates to a numeric.
   * @group conditional_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def nullifzero(col: Column): Column = Column.fn("nullifzero", col)

  /**
   * Returns `col2` if `col1` is null, or `col1` otherwise.
   *
   * @param col1
   *   The value to return if it is not null. A column of any type.
   * @param col2
   *   The value to return if `col1` is null. A column of any type.
   * @group conditional_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def nvl(col1: Column, col2: Column): Column = Column.fn("nvl", col1, col2)

  /**
   * Returns `col2` if `col1` is not null, or `col3` otherwise.
   *
   * @param col1
   *   The value that determines which branch to return. A column of any type.
   * @param col2
   *   The value to return if `col1` is not null. A column of any type.
   * @param col3
   *   The value to return if `col1` is null. A column of any type.
   * @group conditional_funcs
   * @since 3.5.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def nvl2(col1: Column, col2: Column, col3: Column): Column = Column.fn("nvl2", col1, col2, col3)

  /**
   * Returns zero if `col` is null, or `col` otherwise.
   *
   * @param col
   *   The input value. A column that evaluates to a numeric.
   * @group conditional_funcs
   * @since 4.0.0
   * @return
   *   Returns a column of the same type as the input.
   */
  def zeroifnull(col: Column): Column = Column.fn("zeroifnull", col)

  // scalastyle:off line.size.limit
  // scalastyle:off parameter.number

  /* Use the following code to generate:

  (0 to 10).foreach { x =>
    val types = (1 to x).foldRight("RT")((i, s) => s"A$i, $s")
    val typeSeq = "RT" +: (1 to x).map(i => s"A$i")
    val typeTags = typeSeq.map(t => s"$t: TypeTag").mkString(", ")
    val implicitTypeTags = typeSeq.map(t => s"implicitly[TypeTag[$t]]").mkString(", ")
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
      |  SparkUserDefinedFunction(f, $implicitTypeTags)
      |}""".stripMargin)
  }

  (0 to 10).foreach { i =>
    val extTypeArgs = (0 to i).map(_ => "_").mkString(", ")
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
      |  SparkUserDefinedFunction(ToScalaUDF(f), returnType, $i)
      |}""".stripMargin)
  }

   */

  //////////////////////////////////////////////////////////////////////////////////////////////
  // ST geospatial functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the input GEOGRAPHY or GEOMETRY value in WKB format.
   *
   * @param geo
   *   A geospatial value, either a GEOGRAPHY or a GEOMETRY. A column that evaluates to a
   *   geography or geometry.
   * @group st_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def st_asbinary(geo: Column): Column =
    Column.fn("st_asbinary", geo)

  /**
   * Returns the input GEOGRAPHY or GEOMETRY value in WKB format using the specified endianness.
   *
   * @param geo
   *   A geospatial value, either a GEOGRAPHY or a GEOMETRY. A column that evaluates to a
   *   geography or geometry.
   * @param endianness
   *   The endianness of the output WKB, 'NDR' for little-endian or 'XDR' for big-endian. A column
   *   that evaluates to a string.
   * @group st_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def st_asbinary(geo: Column, endianness: Column): Column =
    Column.fn("st_asbinary", geo, endianness)

  /**
   * Returns the input GEOGRAPHY or GEOMETRY value in WKB format using the specified endianness.
   *
   * @param geo
   *   A geospatial value, either a GEOGRAPHY or a GEOMETRY. A column that evaluates to a
   *   geography or geometry.
   * @param endianness
   *   The endianness of the output WKB, 'NDR' for little-endian or 'XDR' for big-endian. A column
   *   that evaluates to a string.
   * @group st_funcs
   * @since 4.2.0
   * @return
   *   Returns a column that evaluates to a binary.
   */
  def st_asbinary(geo: Column, endianness: String): Column =
    Column.fn("st_asbinary", geo, lit(endianness))

  /**
   * Parses the WKB description of a geography and returns the corresponding GEOGRAPHY value.
   *
   * @param wkb
   *   A value in WKB format, representing a GEOGRAPHY value. A column that evaluates to a binary.
   * @group st_funcs
   * @since 4.1.0
   */
  def st_geogfromwkb(wkb: Column): Column =
    Column.fn("st_geogfromwkb", wkb)

  /**
   * Parses the WKB description of a geometry and returns the corresponding GEOMETRY value.
   *
   * @param wkb
   *   A value in WKB format, representing a GEOMETRY value. A column that evaluates to a binary.
   * @group st_funcs
   * @since 4.1.0
   */
  def st_geomfromwkb(wkb: Column): Column =
    Column.fn("st_geomfromwkb", wkb)

  /**
   * Parses the WKB description of a geometry and returns the corresponding GEOMETRY value.
   *
   * @param wkb
   *   A value in WKB format, representing a GEOMETRY value. A column that evaluates to a binary.
   * @param srid
   *   The SRID value of the geometry. A column that evaluates to an integer.
   * @group st_funcs
   * @since 4.2.0
   */
  def st_geomfromwkb(wkb: Column, srid: Column): Column =
    Column.fn("st_geomfromwkb", wkb, srid)

  /**
   * Parses the WKB description of a geometry and returns the corresponding GEOMETRY value.
   *
   * @param wkb
   *   A value in WKB format, representing a GEOMETRY value. A column that evaluates to a binary.
   * @param srid
   *   The SRID value of the geometry. A column that evaluates to an integer.
   * @group st_funcs
   * @since 4.2.0
   */
  def st_geomfromwkb(wkb: Column, srid: Int): Column =
    Column.fn("st_geomfromwkb", wkb, lit(srid))

  /**
   * Returns a new GEOGRAPHY or GEOMETRY value whose SRID is the specified SRID value.
   *
   * @param geo
   *   A geospatial value, either a GEOGRAPHY or a GEOMETRY. A column that evaluates to a
   *   geography or geometry.
   * @param srid
   *   The new SRID of the geospatial value. A column that evaluates to an integer.
   * @group st_funcs
   * @since 4.1.0
   */
  def st_setsrid(geo: Column, srid: Column): Column =
    Column.fn("st_setsrid", geo, srid)

  /**
   * Returns a new GEOGRAPHY or GEOMETRY value whose SRID is the specified SRID value.
   *
   * @param geo
   *   A geospatial value, either a GEOGRAPHY or a GEOMETRY. A column that evaluates to a
   *   geography or geometry.
   * @param srid
   *   The new SRID of the geospatial value. A column that evaluates to an integer.
   * @group st_funcs
   * @since 4.1.0
   */
  def st_setsrid(geo: Column, srid: Int): Column =
    Column.fn("st_setsrid", geo, lit(srid))

  /**
   * Returns the SRID of the input GEOGRAPHY or GEOMETRY value.
   *
   * @param geo
   *   A geospatial value, either a GEOGRAPHY or a GEOMETRY. A column that evaluates to a
   *   geography or geometry.
   * @group st_funcs
   * @since 4.1.0
   * @return
   *   Returns a column that evaluates to an integer.
   */
  def st_srid(geo: Column): Column =
    Column.fn("st_srid", geo)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Scala UDF functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Obtains a `UserDefinedFunction` that wraps the given `Aggregator` so that it may be used with
   * untyped Data Frames.
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
   * @tparam IN
   *   the aggregator input type
   * @tparam BUF
   *   the aggregating buffer type
   * @tparam OUT
   *   the finalized output type
   *
   * @param agg
   *   the typed Aggregator
   *
   * @return
   *   a UserDefinedFunction that can be used as an aggregating expression.
   *
   * @group udf_funcs
   * @note
   *   The input encoder is inferred from the input type IN.
   */
  def udaf[IN: TypeTag, BUF, OUT](agg: Aggregator[IN, BUF, OUT]): UserDefinedFunction = {
    udaf(agg, ScalaReflection.encoderFor[IN])
  }

  /**
   * Obtains a `UserDefinedFunction` that wraps the given `Aggregator` so that it may be used with
   * untyped Data Frames.
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
   * @tparam IN
   *   the aggregator input type
   * @tparam BUF
   *   the aggregating buffer type
   * @tparam OUT
   *   the finalized output type
   *
   * @param agg
   *   the typed Aggregator
   * @param inputEncoder
   *   a specific input encoder to use
   *
   * @return
   *   a UserDefinedFunction that can be used as an aggregating expression
   *
   * @group udf_funcs
   * @note
   *   This overloading takes an explicit input encoder, to support UDAF declarations in Java.
   */
  def udaf[IN, BUF, OUT](
      agg: Aggregator[IN, BUF, OUT],
      inputEncoder: Encoder[IN]): UserDefinedFunction = {
    UserDefinedAggregator(agg, inputEncoder)
  }

  /**
   * Defines a Scala closure of 0 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag](f: Function0[RT]): UserDefinedFunction = {
    SparkUserDefinedFunction(f, implicitly[TypeTag[RT]])
  }

  /**
   * Defines a Scala closure of 1 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag](f: Function1[A1, RT]): UserDefinedFunction = {
    SparkUserDefinedFunction(f, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]])
  }

  /**
   * Defines a Scala closure of 2 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag](
      f: Function2[A1, A2, RT]): UserDefinedFunction = {
    SparkUserDefinedFunction(
      f,
      implicitly[TypeTag[RT]],
      implicitly[TypeTag[A1]],
      implicitly[TypeTag[A2]])
  }

  /**
   * Defines a Scala closure of 3 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](
      f: Function3[A1, A2, A3, RT]): UserDefinedFunction = {
    SparkUserDefinedFunction(
      f,
      implicitly[TypeTag[RT]],
      implicitly[TypeTag[A1]],
      implicitly[TypeTag[A2]],
      implicitly[TypeTag[A3]])
  }

  /**
   * Defines a Scala closure of 4 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](
      f: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction = {
    SparkUserDefinedFunction(
      f,
      implicitly[TypeTag[RT]],
      implicitly[TypeTag[A1]],
      implicitly[TypeTag[A2]],
      implicitly[TypeTag[A3]],
      implicitly[TypeTag[A4]])
  }

  /**
   * Defines a Scala closure of 5 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](
      f: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction = {
    SparkUserDefinedFunction(
      f,
      implicitly[TypeTag[RT]],
      implicitly[TypeTag[A1]],
      implicitly[TypeTag[A2]],
      implicitly[TypeTag[A3]],
      implicitly[TypeTag[A4]],
      implicitly[TypeTag[A5]])
  }

  /**
   * Defines a Scala closure of 6 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag](f: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction = {
    SparkUserDefinedFunction(
      f,
      implicitly[TypeTag[RT]],
      implicitly[TypeTag[A1]],
      implicitly[TypeTag[A2]],
      implicitly[TypeTag[A3]],
      implicitly[TypeTag[A4]],
      implicitly[TypeTag[A5]],
      implicitly[TypeTag[A6]])
  }

  /**
   * Defines a Scala closure of 7 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag](f: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction = {
    SparkUserDefinedFunction(
      f,
      implicitly[TypeTag[RT]],
      implicitly[TypeTag[A1]],
      implicitly[TypeTag[A2]],
      implicitly[TypeTag[A3]],
      implicitly[TypeTag[A4]],
      implicitly[TypeTag[A5]],
      implicitly[TypeTag[A6]],
      implicitly[TypeTag[A7]])
  }

  /**
   * Defines a Scala closure of 8 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
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
      A8: TypeTag](f: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction = {
    SparkUserDefinedFunction(
      f,
      implicitly[TypeTag[RT]],
      implicitly[TypeTag[A1]],
      implicitly[TypeTag[A2]],
      implicitly[TypeTag[A3]],
      implicitly[TypeTag[A4]],
      implicitly[TypeTag[A5]],
      implicitly[TypeTag[A6]],
      implicitly[TypeTag[A7]],
      implicitly[TypeTag[A8]])
  }

  /**
   * Defines a Scala closure of 9 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
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
      A9: TypeTag](f: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction = {
    SparkUserDefinedFunction(
      f,
      implicitly[TypeTag[RT]],
      implicitly[TypeTag[A1]],
      implicitly[TypeTag[A2]],
      implicitly[TypeTag[A3]],
      implicitly[TypeTag[A4]],
      implicitly[TypeTag[A5]],
      implicitly[TypeTag[A6]],
      implicitly[TypeTag[A7]],
      implicitly[TypeTag[A8]],
      implicitly[TypeTag[A9]])
  }

  /**
   * Defines a Scala closure of 10 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 1.3.0
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
      A10: TypeTag](
      f: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction = {
    SparkUserDefinedFunction(
      f,
      implicitly[TypeTag[RT]],
      implicitly[TypeTag[A1]],
      implicitly[TypeTag[A2]],
      implicitly[TypeTag[A3]],
      implicitly[TypeTag[A4]],
      implicitly[TypeTag[A5]],
      implicitly[TypeTag[A6]],
      implicitly[TypeTag[A7]],
      implicitly[TypeTag[A8]],
      implicitly[TypeTag[A9]],
      implicitly[TypeTag[A10]])
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Java UDF functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Defines a Java UDF0 instance as user-defined function (UDF). The caller must specify the
   * output data type, and there is no automatic input type coercion. By default the returned UDF
   * is deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF0[_], returnType: DataType): UserDefinedFunction = {
    SparkUserDefinedFunction(ToScalaUDF(f), returnType, 0)
  }

  /**
   * Defines a Java UDF1 instance as user-defined function (UDF). The caller must specify the
   * output data type, and there is no automatic input type coercion. By default the returned UDF
   * is deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF1[_, _], returnType: DataType): UserDefinedFunction = {
    SparkUserDefinedFunction(ToScalaUDF(f), returnType, 1)
  }

  /**
   * Defines a Java UDF2 instance as user-defined function (UDF). The caller must specify the
   * output data type, and there is no automatic input type coercion. By default the returned UDF
   * is deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF2[_, _, _], returnType: DataType): UserDefinedFunction = {
    SparkUserDefinedFunction(ToScalaUDF(f), returnType, 2)
  }

  /**
   * Defines a Java UDF3 instance as user-defined function (UDF). The caller must specify the
   * output data type, and there is no automatic input type coercion. By default the returned UDF
   * is deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF3[_, _, _, _], returnType: DataType): UserDefinedFunction = {
    SparkUserDefinedFunction(ToScalaUDF(f), returnType, 3)
  }

  /**
   * Defines a Java UDF4 instance as user-defined function (UDF). The caller must specify the
   * output data type, and there is no automatic input type coercion. By default the returned UDF
   * is deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF4[_, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    SparkUserDefinedFunction(ToScalaUDF(f), returnType, 4)
  }

  /**
   * Defines a Java UDF5 instance as user-defined function (UDF). The caller must specify the
   * output data type, and there is no automatic input type coercion. By default the returned UDF
   * is deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF5[_, _, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    SparkUserDefinedFunction(ToScalaUDF(f), returnType, 5)
  }

  /**
   * Defines a Java UDF6 instance as user-defined function (UDF). The caller must specify the
   * output data type, and there is no automatic input type coercion. By default the returned UDF
   * is deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF6[_, _, _, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    SparkUserDefinedFunction(ToScalaUDF(f), returnType, 6)
  }

  /**
   * Defines a Java UDF7 instance as user-defined function (UDF). The caller must specify the
   * output data type, and there is no automatic input type coercion. By default the returned UDF
   * is deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF7[_, _, _, _, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    SparkUserDefinedFunction(ToScalaUDF(f), returnType, 7)
  }

  /**
   * Defines a Java UDF8 instance as user-defined function (UDF). The caller must specify the
   * output data type, and there is no automatic input type coercion. By default the returned UDF
   * is deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF8[_, _, _, _, _, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    SparkUserDefinedFunction(ToScalaUDF(f), returnType, 8)
  }

  /**
   * Defines a Java UDF9 instance as user-defined function (UDF). The caller must specify the
   * output data type, and there is no automatic input type coercion. By default the returned UDF
   * is deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(f: UDF9[_, _, _, _, _, _, _, _, _, _], returnType: DataType): UserDefinedFunction = {
    SparkUserDefinedFunction(ToScalaUDF(f), returnType, 9)
  }

  /**
   * Defines a Java UDF10 instance as user-defined function (UDF). The caller must specify the
   * output data type, and there is no automatic input type coercion. By default the returned UDF
   * is deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 2.3.0
   */
  def udf(
      f: UDF10[_, _, _, _, _, _, _, _, _, _, _],
      returnType: DataType): UserDefinedFunction = {
    SparkUserDefinedFunction(ToScalaUDF(f), returnType, 10)
  }

  // scalastyle:on parameter.number
  // scalastyle:on line.size.limit

  /**
   * Defines a deterministic user-defined function (UDF) using a Scala closure. For this variant,
   * the caller must specify the output data type, and there is no automatic input type coercion.
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * Note that, although the Scala closure can have primitive-type function argument, it doesn't
   * work well with null values. Because the Scala closure is passed in as Any type, there is no
   * type information for the function arguments. Without the type information, Spark may blindly
   * pass null to the Scala closure with primitive-type argument, and the closure will see the
   * default value of the Java type for the null argument, e.g. `udf((x: Int) => x, IntegerType)`,
   * the result is 0 for null input.
   *
   * @param f
   *   A closure in Scala
   * @param dataType
   *   The output data type of the UDF
   *
   * @group udf_funcs
   * @since 2.0.0
   */
  @deprecated(
    "Scala `udf` method with return type parameter is deprecated. " +
      "Please use Scala `udf` method without return type parameter.",
    "3.0.0")
  def udf(f: AnyRef, dataType: DataType): UserDefinedFunction = {
    if (!SqlApiConf.get.legacyAllowUntypedScalaUDFs) {
      throw CompilationErrors.usingUntypedScalaUDFError()
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
  def callUDF(udfName: String, cols: Column*): Column = call_function(udfName, cols: _*)

  /**
   * Call an user-defined function. Example:
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
  def call_udf(udfName: String, cols: Column*): Column = call_function(udfName, cols: _*)

  /**
   * Call a SQL function.
   *
   * @param funcName
   *   function name that follows the SQL identifier syntax (can be quoted, can be qualified)
   * @param cols
   *   the expression parameters of function
   * @group normal_funcs
   * @since 3.5.0
   */
  @scala.annotation.varargs
  def call_function(funcName: String, cols: Column*): Column = {
    Column(internal.UnresolvedFunction(funcName, cols.map(_.node), isUserDefinedFunction = true))
  }

  /**
   * Unwrap UDT data type column into its underlying type.
   * @param column
   *   the UDT column to unwrap. A column that evaluates to a user-defined type.
   * @group udf_funcs
   * @since 3.4.0
   */
  def unwrap_udt(column: Column): Column = Column.internalFn("unwrap_udt", column)

  // ---------------------- Vector Functions ----------------------

  /**
   * Returns the cosine similarity between two float vectors.
   * @param left
   *   first vector column. A column that evaluates to an array.
   * @param right
   *   second vector column. A column that evaluates to an array.
   * @group vector_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to a float.
   */
  def vector_cosine_similarity(left: Column, right: Column): Column =
    Column.fn("vector_cosine_similarity", left, right)

  /**
   * Returns the inner product (dot product) between two float vectors.
   * @param left
   *   first vector column. A column that evaluates to an array.
   * @param right
   *   second vector column. A column that evaluates to an array.
   * @group vector_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to a float.
   */
  def vector_inner_product(left: Column, right: Column): Column =
    Column.fn("vector_inner_product", left, right)

  /**
   * Returns the Euclidean (L2) distance between two float vectors.
   * @param left
   *   first vector column. A column that evaluates to an array.
   * @param right
   *   second vector column. A column that evaluates to an array.
   * @group vector_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to a float.
   */
  def vector_l2_distance(left: Column, right: Column): Column =
    Column.fn("vector_l2_distance", left, right)

  /**
   * Returns the Lp norm of a float vector. Degree defaults to 2.0 if unspecified.
   * @param vector
   *   input vector column. A column that evaluates to an array.
   * @param degree
   *   norm degree (1.0 for L1, 2.0 for L2, infinity norm). A column that evaluates to a float.
   * @group vector_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to a float.
   */
  def vector_norm(vector: Column, degree: Column): Column =
    Column.fn("vector_norm", vector, degree)

  /**
   * Returns the Lp norm of a float vector using degree 2.0 (Euclidean norm).
   * @param vector
   *   input vector column. A column that evaluates to an array.
   * @group vector_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to a float.
   */
  def vector_norm(vector: Column): Column =
    Column.fn("vector_norm", vector)

  /**
   * Normalizes a float vector to unit length. Degree defaults to 2.0 if unspecified.
   * @param vector
   *   input vector column. A column that evaluates to an array.
   * @param degree
   *   norm degree (1.0 for L1, 2.0 for L2, infinity norm). A column that evaluates to a float.
   * @group vector_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def vector_normalize(vector: Column, degree: Column): Column =
    Column.fn("vector_normalize", vector, degree)

  /**
   * Normalizes a float vector to unit length using degree 2.0 (Euclidean norm).
   * @param vector
   *   input vector column. A column that evaluates to an array.
   * @group vector_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def vector_normalize(vector: Column): Column =
    Column.fn("vector_normalize", vector)

  /**
   * Aggregate function: returns the element-wise mean of float vectors in a group.
   * @param col
   *   input vector column. A column that evaluates to an array.
   * @group vector_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def vector_avg(col: Column): Column = Column.fn("vector_avg", col)

  /**
   * Aggregate function: returns the element-wise sum of float vectors in a group.
   * @param col
   *   input vector column. A column that evaluates to an array.
   * @group vector_funcs
   * @since 4.3.0
   * @return
   *   Returns a column that evaluates to an array.
   */
  def vector_sum(col: Column): Column = Column.fn("vector_sum", col)

  // scalastyle:off
  // TODO(SPARK-45970): Use @static annotation so Java can access to those
  //   API in the same way. Once we land this fix, should deprecate
  //   functions.hours, days, months, years and bucket.
  object partitioning {
    // scalastyle:on
    /**
     * (Scala-specific) A transform for timestamps and dates to partition data into years.
     *
     * @group partition_transforms
     * @since 4.0.0
     */
    def years(e: Column): Column = Column.internalFn("years", e)

    /**
     * (Scala-specific) A transform for timestamps and dates to partition data into months.
     *
     * @group partition_transforms
     * @since 4.0.0
     */
    def months(e: Column): Column = Column.internalFn("months", e)

    /**
     * (Scala-specific) A transform for timestamps and dates to partition data into days.
     *
     * @group partition_transforms
     * @since 4.0.0
     */
    def days(e: Column): Column = Column.internalFn("days", e)

    /**
     * (Scala-specific) A transform for timestamps to partition data into hours.
     *
     * @group partition_transforms
     * @since 4.0.0
     */
    def hours(e: Column): Column = Column.internalFn("hours", e)

    /**
     * (Scala-specific) A transform for any type that partitions by a hash of the input column.
     *
     * @group partition_transforms
     * @since 4.0.0
     */
    def bucket(numBuckets: Column, e: Column): Column = Column.internalFn("bucket", numBuckets, e)

    /**
     * (Scala-specific) A transform for any type that partitions by a hash of the input column.
     *
     * @group partition_transforms
     * @since 4.0.0
     */
    def bucket(numBuckets: Int, e: Column): Column = bucket(lit(numBuckets), e)
  }
}
