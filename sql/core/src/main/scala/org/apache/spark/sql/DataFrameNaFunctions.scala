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

import org.apache.spark.sql.catalyst.expressions.AtLeastNNonNulls

/**
 * Functionality for working with missing data in [[DataFrame]]s.
 */
final class DataFrameNaFunctions private[sql](df: DataFrame) {

  /**
   * Dropping rows that contain any null values.
   */
  @scala.annotation.varargs
  def drop(cols: String*): DataFrame = {
    // If cols is empty, use all columns.
    val subset = if (cols.isEmpty) df.columns.toSeq else cols
    drop(subset.size, subset)
  }

  /**
   * Dropping rows that contain less than `threshold` non-null values.
   */
  def drop(threshold: Int): DataFrame = drop(threshold, df.columns)

  /**
   * Dropping rows that contain less than `threshold` non-null values in the specified columns.
   */
  def drop(threshold: Int, cols: Array[String]): DataFrame = drop(threshold, cols.toSeq)

  /**
   * Dropping rows that contain less than `threshold` non-null values in the specified columns.
   */
  def drop(threshold: Int, cols: Seq[String]): DataFrame = {
    // Filtering condition -- drop rows that have less than `threshold` non-null,
    // i.e. at most (cols.size - threshold) null values.
    val predicate = AtLeastNNonNulls(threshold, cols.map(name => df.col(name).expr))
    df.filter(Column(predicate))
  }

  /** Replace all null values in numeric columns with `value` */
  def fill(value: Double): DataFrame = {
    ???
  }

  /** Replace all null values in numeric columns with `value` */
  def fill(value: Int): DataFrame = {
    ???
  }

  /** Replace all null values in numeric columns with `value` */
  def fill(value: Long): DataFrame = {
    ???
  }

  /** Replace all null values in string columns with `value` */
  def fill(value: String): DataFrame = {
    ???
  }

  /**
   * A map from the column name to the value in which we should
   * replace null values in that column with.
   *
   * Note that the caller should guarantee the types match the column.
   */
  def fill(values: Map[String, Any]): DataFrame = {
    ???
  }

  /** Replace `toReplace` with `value` in numeric columns. */
  def replace(toReplace: Double, value: Double): DataFrame = {
    ???
  }

  /** Replace `toReplace` with `value` in string columns. */
  def replace(toReplace: String, value: String): DataFrame = {
    ???
  }

  /** Replace cell matching toReplace[i] with value[i] for numeric columns. */
  def replace(toReplace: Array[Double], value: Array[Double]): DataFrame = {
    ???
  }

  /** Replace cell matching toReplace[i] with value[i] for string columns. */
  def replace(toReplace: Array[String], value: Array[String]): DataFrame = {
    ???
  }

  def replace(toReplace: Seq[Any], value: Seq[Any]) = {
    ???
  }

  /** Replace cell matching key with value for string columns. */
  def replace(replaceMap: Map[Any, Any]) = {
    ???
  }

  /** Replace cell matching `toReplace` with `value` for the given column. */
  def replace(colName: String, toReplace: Double, value: Double) = {
    ???
  }

  /** Replace cell matching `toReplace` with `value` for the given column. */
  def replace(colName: String, toReplace: String, value: String) = {
    ???
  }
}
