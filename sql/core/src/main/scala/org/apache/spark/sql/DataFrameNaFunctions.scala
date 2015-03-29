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
   * Returns a new [[DataFrame ]] that drops rows containing less than `threshold` non-null
   * values in the specified columns.
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
   * Returns a new [[DataFrame ]] that replaces null values in specified numeric columns.
   * If a specified column is not a numeric column, it is ignored.
   */
  def fill(value: Double, cols: Seq[String]): DataFrame = {
    val columnEquals = df.sqlContext.analyzer.resolver
    val projections = df.schema.fields.map { f =>
      // Only fill if the column is part of the cols list.
      if (cols.exists(col => columnEquals(f.name, col))) {
        f.dataType match {
          case _: DoubleType =>
            coalesce(df.col(f.name), lit(value)).as(f.name)
          case typ: NumericType =>
            coalesce(df.col(f.name), lit(value).cast(typ)).as(f.name)
          case _ =>
            df.col(f.name)
        }
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
   * Returns a new [[DataFrame ]] that replaces null values in specified string columns.
   * If a specified column is not a string column, it is ignored.
   */
  def fill(value: String, cols: Seq[String]): DataFrame = {
    val columnEquals = df.sqlContext.analyzer.resolver
    val projections = df.schema.fields.map { f =>
      // Only fill if the column is part of the cols list.
      if (cols.exists(col => columnEquals(f.name, col))) {
        f.dataType match {
          case _: StringType =>
            coalesce(df.col(f.name), lit(value)).as(f.name)
          case _ =>
            df.col(f.name)
        }
      } else {
        df.col(f.name)
      }
    }
    df.select(projections : _*)
  }
}
