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

import java.util.{List => JList}

import scala.language.implicitConversions
import scala.collection.JavaConversions._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.{Literal => LiteralExpr}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate


/**
 * A set of methods for aggregations on a [[DataFrame]], created by [[DataFrame.groupBy]].
 */
class GroupedDataFrame protected[sql](df: DataFrameImpl, groupingExprs: Seq[Expression]) {

  private[this] implicit def toDataFrame(aggExprs: Seq[NamedExpression]): DataFrame = {
    val namedGroupingExprs = groupingExprs.map {
      case expr: NamedExpression => expr
      case expr: Expression => Alias(expr, expr.toString)()
    }
    DataFrame(
      df.sqlContext, Aggregate(groupingExprs, namedGroupingExprs ++ aggExprs, df.logicalPlan))
  }

  private[this] def aggregateNumericColumns(f: Expression => Expression): Seq[NamedExpression] = {
    df.numericColumns.map { c =>
      val a = f(c)
      Alias(a, a.toString)()
    }
  }

  private[this] def strToExpr(expr: String): (Expression => Expression) = {
    expr.toLowerCase match {
      case "avg" | "average" | "mean" => Average
      case "max" => Max
      case "min" => Min
      case "sum" => Sum
      case "count" | "size" => Count
    }
  }

  /**
   * Compute aggregates by specifying a map from column name to aggregate methods. The resulting
   * [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(Map(
   *     "age" -> "max"
   *     "sum" -> "expense"
   *   ))
   * }}}
   */
  def agg(exprs: Map[String, String]): DataFrame = {
    exprs.map { case (colName, expr) =>
      val a = strToExpr(expr)(df(colName).expr)
      Alias(a, a.toString)()
    }.toSeq
  }

  /**
   * Compute aggregates by specifying a map from column name to aggregate methods. The resulting
   * [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(Map(
   *     "age" -> "max"
   *     "sum" -> "expense"
   *   ))
   * }}}
   */
  def agg(exprs: java.util.Map[String, String]): DataFrame = {
    agg(exprs.toMap)
  }

  /**
   * Compute aggregates by specifying a series of aggregate columns. Unlike other methods in this
   * class, the resulting [[DataFrame]] won't automatically include the grouping columns.
   *
   * The available aggregate methods are defined in [[org.apache.spark.sql.Dsl]].
   *
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   import org.apache.spark.sql.dsl._
   *   df.groupBy("department").agg($"department", max($"age"), sum($"expense"))
   * }}}
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = {
    val aggExprs = (expr +: exprs).map(_.expr).map {
      case expr: NamedExpression => expr
      case expr: Expression => Alias(expr, expr.toString)()
    }
    DataFrame(df.sqlContext, Aggregate(groupingExprs, aggExprs, df.logicalPlan))
  }

  /**
   * Count the number of rows for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   */
  def count(): DataFrame = Seq(Alias(Count(LiteralExpr(1)), "count")())

  /**
   * Compute the average value for each numeric columns for each group. This is an alias for `avg`.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   */
  def mean(): DataFrame = aggregateNumericColumns(Average)

  /**
   * Compute the max value for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   */
  def max(): DataFrame = aggregateNumericColumns(Max)

  /**
   * Compute the mean value for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   */
  def avg(): DataFrame = aggregateNumericColumns(Average)

  /**
   * Compute the min value for each numeric column for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   */
  def min(): DataFrame = aggregateNumericColumns(Min)

  /**
   * Compute the sum for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   */
  def sum(): DataFrame = aggregateNumericColumns(Sum)
}
