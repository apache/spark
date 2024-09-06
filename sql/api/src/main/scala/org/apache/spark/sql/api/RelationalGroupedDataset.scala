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
package org.apache.spark.sql.api

import scala.jdk.CollectionConverters._

import _root_.java.util

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.{functions, Column, Encoder, Row}

/**
 * A set of methods for aggregations on a `DataFrame`, created by [[Dataset#groupBy groupBy]],
 * [[Dataset#cube cube]] or [[Dataset#rollup rollup]] (and also `pivot`).
 *
 * The main method is the `agg` function, which has multiple variants. This class also contains
 * some first-order statistics such as `mean`, `sum` for convenience.
 *
 * @note
 *   This class was named `GroupedData` in Spark 1.x.
 * @since 2.0.0
 */
@Stable
abstract class RelationalGroupedDataset[DS[U] <: Dataset[U, DS]] {
  type RGD <: RelationalGroupedDataset[DS]

  protected def df: DS[Row]

  /**
   * Create a aggregation based on the grouping column, the grouping type, and the aggregations.
   */
  protected def toDF(aggCols: Seq[Column]): DS[Row]

  protected def selectNumericColumns(colNames: Seq[String]): Seq[Column]

  /**
   * Convert a name method tuple into a Column.
   */
  private def toAggCol(colAndMethod: (String, String)): Column = {
    val col = df.col(colAndMethod._1)
    colAndMethod._2.toLowerCase(util.Locale.ROOT) match {
      case "avg" | "average" | "mean" => functions.avg(col)
      case "stddev" | "std" => functions.stddev(col)
      case "count" | "size" => functions.count(col)
      case name => Column.fn(name, col)
    }
  }

  private def aggregateNumericColumns(
      colNames: Seq[String],
      function: Column => Column): DS[Row] = {
    toDF(selectNumericColumns(colNames).map(function))
  }

  /**
   * Returns a `KeyValueGroupedDataset` where the data is grouped by the grouping expressions of
   * current `RelationalGroupedDataset`.
   *
   * @since 3.0.0
   */
  def as[K: Encoder, T: Encoder]: KeyValueGroupedDataset[K, T, DS]

  /**
   * (Scala-specific) Compute aggregates by specifying the column names and aggregate methods. The
   * resulting `DataFrame` will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(
   *     "age" -> "max",
   *     "expense" -> "sum"
   *   )
   * }}}
   *
   * @since 1.3.0
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DS[Row] =
    toDF((aggExpr +: aggExprs).map(toAggCol))

  /**
   * (Scala-specific) Compute aggregates by specifying a map from column name to aggregate
   * methods. The resulting `DataFrame` will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(Map(
   *     "age" -> "max",
   *     "expense" -> "sum"
   *   ))
   * }}}
   *
   * @since 1.3.0
   */
  def agg(exprs: Map[String, String]): DS[Row] = toDF(exprs.map(toAggCol).toSeq)

  /**
   * (Java-specific) Compute aggregates by specifying a map from column name to aggregate methods.
   * The resulting `DataFrame` will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   import com.google.common.collect.ImmutableMap;
   *   df.groupBy("department").agg(ImmutableMap.of("age", "max", "expense", "sum"));
   * }}}
   *
   * @since 1.3.0
   */
  def agg(exprs: util.Map[String, String]): DS[Row] = {
    agg(exprs.asScala.toMap)
  }

  /**
   * Compute aggregates by specifying a series of aggregate columns. Note that this function by
   * default retains the grouping columns in its output. To not retain grouping columns, set
   * `spark.sql.retainGroupColumns` to false.
   *
   * The available aggregate methods are defined in [[org.apache.spark.sql.functions]].
   *
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *
   *   // Scala:
   *   import org.apache.spark.sql.functions._
   *   df.groupBy("department").agg(max("age"), sum("expense"))
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.groupBy("department").agg(max("age"), sum("expense"));
   * }}}
   *
   * Note that before Spark 1.4, the default behavior is to NOT retain grouping columns. To change
   * to that behavior, set config variable `spark.sql.retainGroupColumns` to `false`.
   * {{{
   *   // Scala, 1.3.x:
   *   df.groupBy("department").agg($"department", max("age"), sum("expense"))
   *
   *   // Java, 1.3.x:
   *   df.groupBy("department").agg(col("department"), max("age"), sum("expense"));
   * }}}
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DS[Row] = toDF(expr +: exprs)

  /**
   * Count the number of rows for each group. The resulting `DataFrame` will also contain the
   * grouping columns.
   *
   * @since 1.3.0
   */
  def count(): DS[Row] = toDF(functions.count(functions.lit(1)).as("count") :: Nil)

  /**
   * Compute the average value for each numeric columns for each group. This is an alias for
   * `avg`. The resulting `DataFrame` will also contain the grouping columns. When specified
   * columns are given, only compute the average values for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def mean(colNames: String*): DS[Row] = aggregateNumericColumns(colNames, functions.avg)

  /**
   * Compute the max value for each numeric columns for each group. The resulting `DataFrame` will
   * also contain the grouping columns. When specified columns are given, only compute the max
   * values for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def max(colNames: String*): DS[Row] = aggregateNumericColumns(colNames, functions.max)

  /**
   * Compute the mean value for each numeric columns for each group. The resulting `DataFrame`
   * will also contain the grouping columns. When specified columns are given, only compute the
   * mean values for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def avg(colNames: String*): DS[Row] = aggregateNumericColumns(colNames, functions.avg)

  /**
   * Compute the min value for each numeric column for each group. The resulting `DataFrame` will
   * also contain the grouping columns. When specified columns are given, only compute the min
   * values for them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def min(colNames: String*): DS[Row] = aggregateNumericColumns(colNames, functions.min)

  /**
   * Compute the sum for each numeric columns for each group. The resulting `DataFrame` will also
   * contain the grouping columns. When specified columns are given, only compute the sum for
   * them.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def sum(colNames: String*): DS[Row] = aggregateNumericColumns(colNames, functions.sum)

  /**
   * Pivots a column of the current `DataFrame` and performs the specified aggregation.
   *
   * Spark will eagerly compute the distinct values in `pivotColumn` so it can determine the
   * resulting schema of the transformation. To avoid any eager computations, provide an explicit
   * list of values via `pivot(pivotColumn: String, values: Seq[Any])`.
   *
   * {{{
   *   // Compute the sum of earnings for each year by course with each course as a separate column
   *   df.groupBy("year").pivot("course").sum("earnings")
   * }}}
   *
   * @see
   *   `org.apache.spark.sql.Dataset.unpivot` for the reverse operation, except for the
   *   aggregation.
   * @param pivotColumn
   *   Name of the column to pivot.
   * @since 1.6.0
   */
  def pivot(pivotColumn: String): RGD = pivot(df.col(pivotColumn))

  /**
   * Pivots a column of the current `DataFrame` and performs the specified aggregation. There are
   * two versions of pivot function: one that requires the caller to specify the list of distinct
   * values to pivot on, and one that does not. The latter is more concise but less efficient,
   * because Spark needs to first compute the list of distinct values internally.
   *
   * {{{
   *   // Compute the sum of earnings for each year by course with each course as a separate column
   *   df.groupBy("year").pivot("course", Seq("dotNET", "Java")).sum("earnings")
   *
   *   // Or without specifying column values (less efficient)
   *   df.groupBy("year").pivot("course").sum("earnings")
   * }}}
   *
   * From Spark 3.0.0, values can be literal columns, for instance, struct. For pivoting by
   * multiple columns, use the `struct` function to combine the columns and values:
   *
   * {{{
   *   df.groupBy("year")
   *     .pivot("trainingCourse", Seq(struct(lit("java"), lit("Experts"))))
   *     .agg(sum($"earnings"))
   * }}}
   *
   * @see
   *   `org.apache.spark.sql.Dataset.unpivot` for the reverse operation, except for the
   *   aggregation.
   * @param pivotColumn
   *   Name of the column to pivot.
   * @param values
   *   List of values that will be translated to columns in the output DataFrame.
   * @since 1.6.0
   */
  def pivot(pivotColumn: String, values: Seq[Any]): RGD =
    pivot(df.col(pivotColumn), values)

  /**
   * (Java-specific) Pivots a column of the current `DataFrame` and performs the specified
   * aggregation.
   *
   * There are two versions of pivot function: one that requires the caller to specify the list of
   * distinct values to pivot on, and one that does not. The latter is more concise but less
   * efficient, because Spark needs to first compute the list of distinct values internally.
   *
   * {{{
   *   // Compute the sum of earnings for each year by course with each course as a separate column
   *   df.groupBy("year").pivot("course", Arrays.<Object>asList("dotNET", "Java")).sum("earnings");
   *
   *   // Or without specifying column values (less efficient)
   *   df.groupBy("year").pivot("course").sum("earnings");
   * }}}
   *
   * @see
   *   `org.apache.spark.sql.Dataset.unpivot` for the reverse operation, except for the
   *   aggregation.
   * @param pivotColumn
   *   Name of the column to pivot.
   * @param values
   *   List of values that will be translated to columns in the output DataFrame.
   * @since 1.6.0
   */
  def pivot(pivotColumn: String, values: util.List[Any]): RGD =
    pivot(df.col(pivotColumn), values)

  /**
   * (Java-specific) Pivots a column of the current `DataFrame` and performs the specified
   * aggregation. This is an overloaded version of the `pivot` method with `pivotColumn` of the
   * `String` type.
   *
   * @see
   *   `org.apache.spark.sql.Dataset.unpivot` for the reverse operation, except for the
   *   aggregation.
   * @param pivotColumn
   *   the column to pivot.
   * @param values
   *   List of values that will be translated to columns in the output DataFrame.
   * @since 2.4.0
   */
  def pivot(pivotColumn: Column, values: util.List[Any]): RGD =
    pivot(pivotColumn, values.asScala.toSeq)

  /**
   * Pivots a column of the current `DataFrame` and performs the specified aggregation.
   *
   * Spark will eagerly compute the distinct values in `pivotColumn` so it can determine the
   * resulting schema of the transformation. To avoid any eager computations, provide an explicit
   * list of values via `pivot(pivotColumn: Column, values: Seq[Any])`.
   *
   * {{{
   *   // Compute the sum of earnings for each year by course with each course as a separate column
   *   df.groupBy($"year").pivot($"course").sum($"earnings");
   * }}}
   *
   * @see
   *   `org.apache.spark.sql.Dataset.unpivot` for the reverse operation, except for the
   *   aggregation.
   * @param pivotColumn
   *   he column to pivot.
   * @since 2.4.0
   */
  def pivot(pivotColumn: Column): RGD

  /**
   * Pivots a column of the current `DataFrame` and performs the specified aggregation. This is an
   * overloaded version of the `pivot` method with `pivotColumn` of the `String` type.
   *
   * {{{
   *   // Compute the sum of earnings for each year by course with each course as a separate column
   *   df.groupBy($"year").pivot($"course", Seq("dotNET", "Java")).sum($"earnings")
   * }}}
   *
   * @see
   *   `org.apache.spark.sql.Dataset.unpivot` for the reverse operation, except for the
   *   aggregation.
   * @param pivotColumn
   *   the column to pivot.
   * @param values
   *   List of values that will be translated to columns in the output DataFrame.
   * @since 2.4.0
   */
  def pivot(pivotColumn: Column, values: Seq[Any]): RGD
}
