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

import scala.collection.JavaConverters._

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
class RelationalGroupedDataset protected[sql] (
    private[sql] val df: DataFrame,
    private[sql] val groupingExprs: Seq[proto.Expression]) {

  private[this] def toDF(aggExprs: Seq[proto.Expression]): DataFrame = {
    // TODO: support other GroupByType such as Rollup, Cube, Pivot.
    df.session.newDataset { builder =>
      builder.getAggregateBuilder
        .setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
        .setInput(df.plan.getRoot)
        .addAllGroupingExpressions(groupingExprs.asJava)
        .addAllAggregateExpressions(aggExprs.asJava)
    }
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
      strToExpr(expr, df(colName).expr)
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
      strToExpr(expr, df(colName).expr)
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

  private[this] def strToExpr(expr: String, inputExpr: proto.Expression): proto.Expression = {
    val builder = proto.Expression.newBuilder()

    expr.toLowerCase(Locale.ROOT) match {
      // We special handle a few cases that have alias that are not in function registry.
      case "avg" | "average" | "mean" =>
        builder.getUnresolvedFunctionBuilder
          .setFunctionName("avg")
          .addArguments(inputExpr)
          .setIsDistinct(false)
      case "stddev" | "std" =>
        builder.getUnresolvedFunctionBuilder
          .setFunctionName("stddev")
          .addArguments(inputExpr)
          .setIsDistinct(false)
      // Also special handle count because we need to take care count(*).
      case "count" | "size" =>
        // Turn count(*) into count(1)
        inputExpr match {
          case s if s.hasUnresolvedStar =>
            val exprBuilder = proto.Expression.newBuilder
            exprBuilder.getLiteralBuilder.setInteger(1)
            builder.getUnresolvedFunctionBuilder
              .setFunctionName("count")
              .addArguments(exprBuilder)
              .setIsDistinct(false)
          case _ =>
            builder.getUnresolvedFunctionBuilder
              .setFunctionName("count")
              .addArguments(inputExpr)
              .setIsDistinct(false)
        }
      case name =>
        builder.getUnresolvedFunctionBuilder
          .setFunctionName(name)
          .addArguments(inputExpr)
          .setIsDistinct(false)
    }
    builder.build()
  }
}
