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
   * Returns a new [[DataFrame ]] that drops rows containing any null values.
   */
  @scala.annotation.varargs
  def drop(cols: String*): DataFrame = {
    // If cols is empty, use all columns.
    val subset = if (cols.isEmpty) df.columns.toSeq else cols
    drop(subset.size, subset)
  }

  /**
   * Returns a new [[DataFrame ]] that drops rows containing less than `threshold` non-null values.
   */
  def drop(threshold: Int): DataFrame = drop(threshold, df.columns)

  /**
   * Returns a new [[DataFrame ]] that drops rows containing less than `threshold` non-null
   * values in the specified columns.
   */
  def drop(threshold: Int, cols: Array[String]): DataFrame = drop(threshold, cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame ]] that drops rows containing less than
   * `threshold` non-null values in the specified columns.
   */
  def drop(threshold: Int, cols: Seq[String]): DataFrame = {
    // Filtering condition -- drop rows that have less than `threshold` non-null,
    // i.e. at most (cols.size - threshold) null values.
    val predicate = AtLeastNNonNulls(threshold, cols.map(name => df.resolve(name)))
    df.filter(Column(predicate))
  }

  /**
   * Returns a new [[DataFrame ]] that replaces null values in numeric columns with `value`.
   */
  def fill(value: Double): DataFrame = fill(value, df.columns)

  /**
   * Returns a new [[DataFrame ]] that replaces null values in string columns with `value`.
   */
  def fill(value: String): DataFrame = fill(value, df.columns)

  /**
   * Returns a new [[DataFrame ]] that replaces null values in specified numeric columns.
   * If a specified column is not a numeric column, it is ignored.
   */
  def fill(value: Double, cols: Array[String]): DataFrame = fill(value, cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame ]] that replaces null values in specified
   * numeric columns. If a specified column is not a numeric column, it is ignored.
   */
  def fill(value: Double, cols: Seq[String]): DataFrame = {
    val columnEquals = df.sqlContext.analyzer.resolver
    val projections = df.schema.fields.map { f =>
      // Only fill if the column is part of the cols list.
      if (f.dataType.isInstanceOf[NumericType] && cols.exists(col => columnEquals(f.name, col))) {
        fillDouble(f, value)
      } else {
        df.col(f.name)
      }
    }
    df.select(projections : _*)
  }

  /**
   * Returns a new [[DataFrame ]] that replaces null values in specified string columns.
   * If a specified column is not a string column, it is ignored.
   */
  def fill(value: String, cols: Array[String]): DataFrame = fill(value, cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame ]] that replaces null values in
   * specified string columns. If a specified column is not a string column, it is ignored.
   */
  def fill(value: String, cols: Seq[String]): DataFrame = {
    val columnEquals = df.sqlContext.analyzer.resolver
    val projections = df.schema.fields.map { f =>
      // Only fill if the column is part of the cols list.
      if (f.dataType.isInstanceOf[StringType] && cols.exists(col => columnEquals(f.name, col))) {
        fillString(f, value)
      } else {
        df.col(f.name)
      }
    }
    df.select(projections : _*)
  }

  /**
   * Returns a new [[DataFrame ]] that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is the replacement value.
   * The value must be of the following type: `Integer`, `Long`, `Float`, `Double`, `String`.
   *
   * For example, the following replaces null values in column "A" with string "unknown", and
   * null values in column "B" with numeric value 1.0.
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *   df.na.fill(ImmutableMap.<String, Object>builder()
   *     .put("A", "unknown")
   *     .put("B", 1.0)
   *     .build());
   * }}}
   */
  def fill(valueMap: java.util.Map[String, Any]): DataFrame = fill(valueMap.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame ]] that replaces null values.
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
  def fill(valueMap: Map[String, Any]): DataFrame = fill(valueMap.toSeq)

  private def fill(valueMap: Seq[(String, Any)]): DataFrame = {
    // Error handling
    valueMap.foreach { case (colName, replaceValue) =>
      // Check column name exists
      df.resolve(colName)

      // Check data type
      replaceValue match {
        case _: jl.Double | _: jl.Float | _: jl.Integer | _: jl.Long | _: String =>
          // This is good
        case _ => throw new IllegalArgumentException(
          s"Does not support value type ${replaceValue.getClass.getName} ($replaceValue).")
      }
    }

    val columnEquals = df.sqlContext.analyzer.resolver
    val pairs = valueMap.toSeq

    val projections = df.schema.fields.map { f =>
      pairs.find { case (k, _) => columnEquals(k, f.name) }.map { case (_, v) =>
        v match {
          case v: jl.Float => fillDouble(f, v.toDouble)
          case v: jl.Double => fillDouble(f, v)
          case v: jl.Long => fillDouble(f, v.toDouble)
          case v: jl.Integer => fillDouble(f, v.toDouble)
          case v: String => fillString(f, v)
        }
      }.getOrElse(df.col(f.name))
    }
    df.select(projections : _*)
  }

  /**
   * Returns a [[Column]] expression that replaces null value in `col` with `replacement`.
   */
  private def fillDouble(col: StructField, replacement: Double): Column = {
    coalesce(df.col(col.name), lit(replacement).cast(col.dataType)).as(col.name)
  }

  /**
   * Returns a [[Column]] expression that replaces null value in `col` with `replacement`.
   */
  private def fillString(col: StructField, replacement: String): Column = {
    coalesce(df.col(col.name), lit(replacement).cast(col.dataType)).as(col.name)
  }
}
