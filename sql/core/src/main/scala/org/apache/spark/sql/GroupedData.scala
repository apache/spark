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

import scala.collection.JavaConversions._
import scala.language.implicitConversions

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.types.NumericType


/**
 * :: Experimental ::
 * A set of methods for aggregations on a [[DataFrame]], created by [[DataFrame.groupBy]].
 */
@Experimental
class GroupedData protected[sql](df: DataFrameImpl, groupingExprs: Seq[Expression]) {

  private[this] implicit def toDF(aggExprs: Seq[NamedExpression]): DataFrame = {
    val namedGroupingExprs = groupingExprs.map {
      case expr: NamedExpression => expr
      case expr: Expression => Alias(expr, expr.toString)()
    }
    DataFrame(
      df.sqlContext, Aggregate(groupingExprs, namedGroupingExprs ++ aggExprs, df.logicalPlan))
  }

  private[this] def aggregateNumericColumns(colNames: String*)(f: Expression => Expression)
    : Seq[NamedExpression] = {

    val columnExprs = if (colNames.isEmpty) {
      // No columns specified. Use all numeric columns.
      df.numericColumns
    } else {
      // Make sure all specified columns are numeric.
      colNames.map { colName =>
        val namedExpr = df.resolve(colName)
        if (!namedExpr.dataType.isInstanceOf[NumericType]) {
          throw new AnalysisException(
            s""""$colName" is not a numeric column. """ +
            "Aggregation function can only be applied on a numeric column.")
        }
        namedExpr
      }
    }
    columnExprs.map { c =>
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
      case "count" | "size" =>
        // Turn count(*) into count(1)
        (inputExpr: Expression) => inputExpr match {
          case s: Star => Count(Literal(1))
          case _ => Count(inputExpr)
        }
    }
  }

  /**
   * (Scala-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(
   *     "age" -> "max",
   *     "expense" -> "sum"
   *   )
   * }}}
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    agg((aggExpr +: aggExprs).toMap)
  }

  /**
   * (Scala-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(Map(
   *     "age" -> "max",
   *     "expense" -> "sum"
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
   * (Java-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   import com.google.common.collect.ImmutableMap;
   *   df.groupBy("department").agg(ImmutableMap.<String, String>builder()
   *     .put("age", "max")
   *     .put("expense", "sum")
   *     .build());
   * }}}
   */
  def agg(exprs: java.util.Map[String, String]): DataFrame = {
    agg(exprs.toMap)
  }

  /**
   * Compute aggregates by specifying a series of aggregate columns. Unlike other methods in this
   * class, the resulting [[DataFrame]] won't automatically include the grouping columns.
   *
   * The available aggregate methods are defined in [[org.apache.spark.sql.functions]].
   *
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *
   *   // Scala:
   *   import org.apache.spark.sql.functions._
   *   df.groupBy("department").agg($"department", max($"age"), sum($"expense"))
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.groupBy("department").agg(col("department"), max(col("age")), sum(col("expense")));
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
  def count(): DataFrame = Seq(Alias(Count(Literal(1)), "count")())

  /**
   * Compute the average value for each numeric columns for each group. This is an alias for `avg`.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the average values for them.
   */
  @scala.annotation.varargs
  def mean(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames:_*)(Average)
  }
 
  /**
   * Compute the max value for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the max values for them.
   */
  @scala.annotation.varargs
  def max(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames:_*)(Max)
  }

  /**
   * Compute the mean value for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the mean values for them.
   */
  @scala.annotation.varargs
  def avg(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames:_*)(Average)
  }

  /**
   * Compute the min value for each numeric column for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the min values for them.
   */
  @scala.annotation.varargs
  def min(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames:_*)(Min)
  }

  /**
   * Compute the sum for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the sum for them.
   */
  @scala.annotation.varargs
  def sum(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames:_*)(Sum)
  }    
}
