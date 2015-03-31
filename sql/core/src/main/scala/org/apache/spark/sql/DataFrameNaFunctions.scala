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

import java.{lang => jl}

import scala.collection.JavaConversions._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
 * Functionality for working with missing data in [[DataFrame]]s.
 */
final class DataFrameNaFunctions private[sql](df: DataFrame) {

  /**
   * Returns a new [[DataFrame]] that drops rows containing any null values.
   */
  def drop(): DataFrame = drop("any", df.columns)

  /**
   * Returns a new [[DataFrame]] that drops rows containing null values.
   *
   * If `how` is "any", then drop rows containing any null values.
   * If `how` is "all", then drop rows only if every column is null for that row.
   */
  def drop(how: String): DataFrame = drop(how, df.columns)

  /**
   * Returns a new [[DataFrame]] that drops rows containing any null values
   * in the specified columns.
   */
  def drop(cols: Array[String]): DataFrame = drop(cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame ]] that drops rows containing any null values
   * in the specified columns.
   */
  def drop(cols: Seq[String]): DataFrame = drop(cols.size, cols)

  /**
   * Returns a new [[DataFrame]] that drops rows containing null values
   * in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null for that row.
   */
  def drop(how: String, cols: Array[String]): DataFrame = drop(how, cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] that drops rows containing null values
   * in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null for that row.
   */
  def drop(how: String, cols: Seq[String]): DataFrame = {
    how.toLowerCase match {
      case "any" => drop(cols.size, cols)
      case "all" => drop(1, cols)
      case _ => throw new IllegalArgumentException(s"how ($how) must be 'any' or 'all'")
    }
  }

  /**
   * Returns a new [[DataFrame]] that drops rows containing less than `minNonNulls` non-null values.
   */
  def drop(minNonNulls: Int): DataFrame = drop(minNonNulls, df.columns)

  /**
   * Returns a new [[DataFrame]] that drops rows containing less than `minNonNulls` non-null
   * values in the specified columns.
   */
  def drop(minNonNulls: Int, cols: Array[String]): DataFrame = drop(minNonNulls, cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] that drops rows containing less than
   * `minNonNulls` non-null values in the specified columns.
   */
  def drop(minNonNulls: Int, cols: Seq[String]): DataFrame = {
    // Filtering condition -- only keep the row if it has at least `minNonNulls` non-null values.
    val predicate = AtLeastNNonNulls(minNonNulls, cols.map(name => df.resolve(name)))
    df.filter(Column(predicate))
  }

  /**
   * Returns a new [[DataFrame]] that replaces null values in numeric columns with `value`.
   */
  def fill(value: Double): DataFrame = fill(value, df.columns)

  /**
   * Returns a new [[DataFrame ]] that replaces null values in string columns with `value`.
   */
  def fill(value: String): DataFrame = fill(value, df.columns)

  /**
   * Returns a new [[DataFrame]] that replaces null values in specified numeric columns.
   * If a specified column is not a numeric column, it is ignored.
   */
  def fill(value: Double, cols: Array[String]): DataFrame = fill(value, cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] that replaces null values in specified
   * numeric columns. If a specified column is not a numeric column, it is ignored.
   */
  def fill(value: Double, cols: Seq[String]): DataFrame = {
    val columnEquals = df.sqlContext.analyzer.resolver
    val projections = df.schema.fields.map { f =>
      // Only fill if the column is part of the cols list.
      if (f.dataType.isInstanceOf[NumericType] && cols.exists(col => columnEquals(f.name, col))) {
        fillCol[Double](f, value)
      } else {
        df.col(f.name)
      }
    }
    df.select(projections : _*)
  }

  /**
   * Returns a new [[DataFrame]] that replaces null values in specified string columns.
   * If a specified column is not a string column, it is ignored.
   */
  def fill(value: String, cols: Array[String]): DataFrame = fill(value, cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] that replaces null values in
   * specified string columns. If a specified column is not a string column, it is ignored.
   */
  def fill(value: String, cols: Seq[String]): DataFrame = {
    val columnEquals = df.sqlContext.analyzer.resolver
    val projections = df.schema.fields.map { f =>
      // Only fill if the column is part of the cols list.
      if (f.dataType.isInstanceOf[StringType] && cols.exists(col => columnEquals(f.name, col))) {
        fillCol[String](f, value)
      } else {
        df.col(f.name)
      }
    }
    df.select(projections : _*)
  }

  /**
   * Returns a new [[DataFrame]] that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is the replacement value.
   * The value must be of the following type: `Integer`, `Long`, `Float`, `Double`, `String`.
   *
   * For example, the following replaces null values in column "A" with string "unknown", and
   * null values in column "B" with numeric value 1.0.
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *   df.na.fill(ImmutableMap.of("A", "unknown", "B", 1.0));
   * }}}
   */
  def fill(valueMap: java.util.Map[String, Any]): DataFrame = fill0(valueMap.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is the replacement value.
   * The value must be of the following type: `Int`, `Long`, `Float`, `Double`, `String`.
   *
   * For example, the following replaces null values in column "A" with string "unknown", and
   * null values in column "B" with numeric value 1.0.
   * {{{
   *   df.na.fill(Map(
   *     "A" -> "unknown",
   *     "B" -> 1.0
   *   ))
   * }}}
   */
  def fill(valueMap: Map[String, Any]): DataFrame = fill0(valueMap.toSeq)

  private def fill0(values: Seq[(String, Any)]): DataFrame = {
    // Error handling
    values.foreach { case (colName, replaceValue) =>
      // Check column name exists
      df.resolve(colName)

      // Check data type
      replaceValue match {
        case _: jl.Double | _: jl.Float | _: jl.Integer | _: jl.Long | _: String =>
          // This is good
        case _ => throw new IllegalArgumentException(
          s"Unsupported value type ${replaceValue.getClass.getName} ($replaceValue).")
      }
    }

    val columnEquals = df.sqlContext.analyzer.resolver
    val projections = df.schema.fields.map { f =>
      values.find { case (k, _) => columnEquals(k, f.name) }.map { case (_, v) =>
        v match {
          case v: jl.Float => fillCol[Double](f, v.toDouble)
          case v: jl.Double => fillCol[Double](f, v)
          case v: jl.Long => fillCol[Double](f, v.toDouble)
          case v: jl.Integer => fillCol[Double](f, v.toDouble)
          case v: String => fillCol[String](f, v)
        }
      }.getOrElse(df.col(f.name))
    }
    df.select(projections : _*)
  }

  /**
   * Returns a [[Column]] expression that replaces null value in `col` with `replacement`.
   */
  private def fillCol[T](col: StructField, replacement: T): Column = {
    coalesce(df.col(col.name), lit(replacement).cast(col.dataType)).as(col.name)
  }
}
