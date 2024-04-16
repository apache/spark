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

import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto

/**
 * A set of methods for aggregations on a `DataFrame`, created by [[Dataset#groupBy groupBy]],
 * [[Dataset#cube cube]] or [[Dataset#rollup rollup]] (and also `pivot`).
 *
 * The main method is the `agg` function, which has multiple variants. This class also contains
 * some first-order statistics such as `mean`, `sum` for convenience.
 *
 * @note
 *   This class was named `GroupedData` in Spark 1.x.
 *
 * @since 3.4.0
 */
class RelationalGroupedDataset private[sql] (
    private[sql] val df: DataFrame,
    private[sql] val groupingExprs: Seq[Column],
    groupType: proto.Aggregate.GroupType,
    pivot: Option[proto.Aggregate.Pivot] = None,
    groupingSets: Option[Seq[proto.Aggregate.GroupingSets]] = None) {

  private[this] def toDF(aggExprs: Seq[Column]): DataFrame = {
    df.sparkSession.newDataFrame { builder =>
      builder.getAggregateBuilder
        .setInput(df.plan.getRoot)
        .addAllGroupingExpressions(groupingExprs.map(_.expr).asJava)
        .addAllAggregateExpressions(aggExprs.map(e => e.expr).asJava)

      groupType match {
        case proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP =>
          builder.getAggregateBuilder.setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP)
        case proto.Aggregate.GroupType.GROUP_TYPE_CUBE =>
          builder.getAggregateBuilder.setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_CUBE)
        case proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY =>
          builder.getAggregateBuilder.setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
        case proto.Aggregate.GroupType.GROUP_TYPE_PIVOT =>
          assert(pivot.isDefined)
          builder.getAggregateBuilder
            .setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_PIVOT)
            .setPivot(pivot.get)
        case proto.Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS =>
          assert(groupingSets.isDefined)
          val aggBuilder = builder.getAggregateBuilder
            .setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS)
          groupingSets.get.foreach(aggBuilder.addGroupingSets)
        case g => throw new UnsupportedOperationException(g.toString)
      }
    }
  }

  /**
   * Returns a `KeyValueGroupedDataset` where the data is grouped by the grouping expressions of
   * current `RelationalGroupedDataset`.
   *
   * @since 3.5.0
   */
  def as[K: Encoder, T: Encoder]: KeyValueGroupedDataset[K, T] = {
    KeyValueGroupedDatasetImpl[K, T](df, encoderFor[K], encoderFor[T], groupingExprs)
  }

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
   * @since 3.4.0
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    toDF((aggExpr +: aggExprs).map { case (colName, expr) =>
      strToColumn(expr, df(colName))
    })
  }

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
   * @since 3.4.0
   */
  def agg(exprs: Map[String, String]): DataFrame = {
    toDF(exprs.map { case (colName, expr) =>
      strToColumn(expr, df(colName))
    }.toSeq)
  }

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
   * @since 3.4.0
   */
  def agg(exprs: java.util.Map[String, String]): DataFrame = {
    agg(exprs.asScala.toMap)
  }

  private[this] def strToColumn(expr: String, inputExpr: Column): Column = {
    expr.toLowerCase(Locale.ROOT) match {
      case "avg" | "average" | "mean" => functions.avg(inputExpr)
      case "stddev" | "std" => functions.stddev(inputExpr)
      case "count" | "size" => functions.count(inputExpr)
      case name => Column.fn(name, inputExpr)
    }
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
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = {
    toDF((expr +: exprs).map { case c =>
      c
    // TODO: deal with typed columns.
    })
  }

  /**
   * Count the number of rows for each group. The resulting `DataFrame` will also contain the
   * grouping columns.
   *
   * @since 3.4.0
   */
  def count(): DataFrame = toDF(Seq(functions.count(functions.lit(1)).alias("count")))

  /**
   * Compute the average value for each numeric columns for each group. This is an alias for
   * `avg`. The resulting `DataFrame` will also contain the grouping columns. When specified
   * columns are given, only compute the average values for them.
   *
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def mean(colNames: String*): DataFrame = {
    toDF(colNames.map(colName => functions.mean(colName)))
  }

  /**
   * Compute the max value for each numeric columns for each group. The resulting `DataFrame` will
   * also contain the grouping columns. When specified columns are given, only compute the max
   * values for them.
   *
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def max(colNames: String*): DataFrame = {
    toDF(colNames.map(colName => functions.max(colName)))
  }

  /**
   * Compute the mean value for each numeric columns for each group. The resulting `DataFrame`
   * will also contain the grouping columns. When specified columns are given, only compute the
   * mean values for them.
   *
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def avg(colNames: String*): DataFrame = {
    toDF(colNames.map(colName => functions.avg(colName)))
  }

  /**
   * Compute the min value for each numeric column for each group. The resulting `DataFrame` will
   * also contain the grouping columns. When specified columns are given, only compute the min
   * values for them.
   *
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def min(colNames: String*): DataFrame = {
    toDF(colNames.map(colName => functions.min(colName)))
  }

  /**
   * Compute the sum for each numeric columns for each group. The resulting `DataFrame` will also
   * contain the grouping columns. When specified columns are given, only compute the sum for
   * them.
   *
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def sum(colNames: String*): DataFrame = {
    toDF(colNames.map(colName => functions.sum(colName)))
  }

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
   *
   * @param pivotColumn
   *   Name of the column to pivot.
   * @since 3.4.0
   */
  def pivot(pivotColumn: String): RelationalGroupedDataset = pivot(Column(pivotColumn))

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
   *
   * @param pivotColumn
   *   Name of the column to pivot.
   * @param values
   *   List of values that will be translated to columns in the output DataFrame.
   * @since 3.4.0
   */
  def pivot(pivotColumn: String, values: Seq[Any]): RelationalGroupedDataset = {
    pivot(Column(pivotColumn), values)
  }

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
   *
   * @param pivotColumn
   *   Name of the column to pivot.
   * @param values
   *   List of values that will be translated to columns in the output DataFrame.
   * @since 3.4.0
   */
  def pivot(pivotColumn: String, values: java.util.List[Any]): RelationalGroupedDataset = {
    pivot(Column(pivotColumn), values)
  }

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
   *
   * @param pivotColumn
   *   the column to pivot.
   * @param values
   *   List of values that will be translated to columns in the output DataFrame.
   * @since 3.4.0
   */
  def pivot(pivotColumn: Column, values: Seq[Any]): RelationalGroupedDataset = {
    groupType match {
      case proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY =>
        val valueExprs = values.map(_ match {
          case c: Column if c.expr.hasLiteral => c.expr.getLiteral
          case c: Column if !c.expr.hasLiteral =>
            throw new IllegalArgumentException("values only accept literal Column")
          case v => functions.lit(v).expr.getLiteral
        })
        new RelationalGroupedDataset(
          df,
          groupingExprs,
          proto.Aggregate.GroupType.GROUP_TYPE_PIVOT,
          Some(
            proto.Aggregate.Pivot
              .newBuilder()
              .setCol(pivotColumn.expr)
              .addAllValues(valueExprs.asJava)
              .build()))
      case _ =>
        throw new UnsupportedOperationException()
    }
  }

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
   *
   * @param pivotColumn
   *   he column to pivot.
   * @since 3.4.0
   */
  def pivot(pivotColumn: Column): RelationalGroupedDataset = {
    pivot(pivotColumn, Seq())
  }

  /**
   * (Java-specific) Pivots a column of the current `DataFrame` and performs the specified
   * aggregation. This is an overloaded version of the `pivot` method with `pivotColumn` of the
   * `String` type.
   *
   * @see
   *   `org.apache.spark.sql.Dataset.unpivot` for the reverse operation, except for the
   *   aggregation.
   *
   * @param pivotColumn
   *   the column to pivot.
   * @param values
   *   List of values that will be translated to columns in the output DataFrame.
   * @since 3.4.0
   */
  def pivot(pivotColumn: Column, values: java.util.List[Any]): RelationalGroupedDataset = {
    pivot(pivotColumn, values.asScala.toSeq)
  }
}
