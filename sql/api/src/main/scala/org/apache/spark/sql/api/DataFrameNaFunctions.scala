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
import org.apache.spark.sql.Row
import org.apache.spark.util.ArrayImplicits._

/**
 * Functionality for working with missing data in `DataFrame`s.
 *
 * @since 1.3.1
 */
@Stable
abstract class DataFrameNaFunctions[DS[U] <: Dataset[U, DS]] {

  /**
   * Returns a new `DataFrame` that drops rows containing any null or NaN values.
   *
   * @since 1.3.1
   */
  def drop(): DS[Row] = drop("any")

  /**
   * Returns a new `DataFrame` that drops rows containing null or NaN values.
   *
   * If `how` is "any", then drop rows containing any null or NaN values.
   * If `how` is "all", then drop rows only if every column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  def drop(how: String): DS[Row] = drop(toMinNonNulls(how))

  /**
   * Returns a new `DataFrame` that drops rows containing any null or NaN values
   * in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(cols: Array[String]): DS[Row] = drop(cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows containing any null or NaN values
   * in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(cols: Seq[String]): DS[Row] = drop(cols.size, cols)

  /**
   * Returns a new `DataFrame` that drops rows containing null or NaN values
   * in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  def drop(how: String, cols: Array[String]): DS[Row] = drop(how, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows containing null or NaN values
   * in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  def drop(how: String, cols: Seq[String]): DS[Row] = drop(toMinNonNulls(how), cols)

  /**
   * Returns a new `DataFrame` that drops rows containing
   * less than `minNonNulls` non-null and non-NaN values.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int): DS[Row] = drop(Option(minNonNulls))

  /**
   * Returns a new `DataFrame` that drops rows containing
   * less than `minNonNulls` non-null and non-NaN values in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int, cols: Array[String]): DS[Row] =
    drop(minNonNulls, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows containing less than
   * `minNonNulls` non-null and non-NaN values in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int, cols: Seq[String]): DS[Row] = drop(Option(minNonNulls), cols)

  private def toMinNonNulls(how: String): Option[Int] = {
    how.toLowerCase(util.Locale.ROOT) match {
      case "any" => None // No-Op. Do nothing.
      case "all" => Some(1)
      case _ => throw new IllegalArgumentException(s"how ($how) must be 'any' or 'all'")
    }
  }

  protected def drop(minNonNulls: Option[Int]): DS[Row]

  protected def drop(minNonNulls: Option[Int], cols: Seq[String]): DS[Row]

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in numeric columns with `value`.
   *
   * @since 2.2.0
   */
  def fill(value: Long): DS[Row]

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in numeric columns with `value`.
   * @since 1.3.1
   */
  def fill(value: Double): DS[Row]

  /**
   * Returns a new `DataFrame` that replaces null values in string columns with `value`.
   *
   * @since 1.3.1
   */
  def fill(value: String): DS[Row]

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in specified numeric columns.
   * If a specified column is not a numeric column, it is ignored.
   *
   * @since 2.2.0
   */
  def fill(value: Long, cols: Array[String]): DS[Row] = fill(value, cols.toImmutableArraySeq)

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in specified numeric columns.
   * If a specified column is not a numeric column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: Double, cols: Array[String]): DS[Row] = fill(value, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null or NaN values in specified
   * numeric columns. If a specified column is not a numeric column, it is ignored.
   *
   * @since 2.2.0
   */
  def fill(value: Long, cols: Seq[String]): DS[Row]

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null or NaN values in specified
   * numeric columns. If a specified column is not a numeric column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: Double, cols: Seq[String]): DS[Row]


  /**
   * Returns a new `DataFrame` that replaces null values in specified string columns.
   * If a specified column is not a string column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: String, cols: Array[String]): DS[Row] = fill(value, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null values in
   * specified string columns. If a specified column is not a string column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: String, cols: Seq[String]): DS[Row]

  /**
   * Returns a new `DataFrame` that replaces null values in boolean columns with `value`.
   *
   * @since 2.3.0
   */
  def fill(value: Boolean): DS[Row]

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null values in specified
   * boolean columns. If a specified column is not a boolean column, it is ignored.
   *
   * @since 2.3.0
   */
  def fill(value: Boolean, cols: Seq[String]): DS[Row]

  /**
   * Returns a new `DataFrame` that replaces null values in specified boolean columns.
   * If a specified column is not a boolean column, it is ignored.
   *
   * @since 2.3.0
   */
  def fill(value: Boolean, cols: Array[String]): DS[Row] = fill(value, cols.toImmutableArraySeq)

  /**
   * Returns a new `DataFrame` that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is the replacement value.
   * The value must be of the following type:
   * `Integer`, `Long`, `Float`, `Double`, `String`, `Boolean`.
   * Replacement values are cast to the column data type.
   *
   * For example, the following replaces null values in column "A" with string "unknown", and
   * null values in column "B" with numeric value 1.0.
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *   df.na.fill(ImmutableMap.of("A", "unknown", "B", 1.0));
   * }}}
   *
   * @since 1.3.1
   */
  def fill(valueMap: util.Map[String, Any]): DS[Row] = fillMap(valueMap.asScala.toSeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is the replacement value.
   * The value must be of the following type: `Int`, `Long`, `Float`, `Double`, `String`, `Boolean`.
   * Replacement values are cast to the column data type.
   *
   * For example, the following replaces null values in column "A" with string "unknown", and
   * null values in column "B" with numeric value 1.0.
   * {{{
   *   df.na.fill(Map(
   *     "A" -> "unknown",
   *     "B" -> 1.0
   *   ))
   * }}}
   *
   * @since 1.3.1
   */
  def fill(valueMap: Map[String, Any]): DS[Row] = fillMap(valueMap.toSeq)

  protected def fillMap(values: Seq[(String, Any)]): DS[Row]

  /**
   * Replaces values matching keys in `replacement` map with the corresponding values.
   *
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height".
   *   df.na.replace("height", ImmutableMap.of(1.0, 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "name".
   *   df.na.replace("name", ImmutableMap.of("UNKNOWN", "unnamed"));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in all string columns.
   *   df.na.replace("*", ImmutableMap.of("UNKNOWN", "unnamed"));
   * }}}
   *
   * @param col name of the column to apply the value replacement. If `col` is "*",
   *            replacement is applied on all string, numeric or boolean columns.
   * @param replacement value replacement map. Key and value of `replacement` map must have
   *                    the same type, and can only be doubles, strings or booleans.
   *                    The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](col: String, replacement: util.Map[T, T]): DS[Row] = {
    replace[T](col, replacement.asScala.toMap)
  }

  /**
   * Replaces values matching keys in `replacement` map with the corresponding values.
   *
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height" and "weight".
   *   df.na.replace(new String[] {"height", "weight"}, ImmutableMap.of(1.0, 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "firstname" and "lastname".
   *   df.na.replace(new String[] {"firstname", "lastname"}, ImmutableMap.of("UNKNOWN", "unnamed"));
   * }}}
   *
   * @param cols list of columns to apply the value replacement. If `col` is "*",
   *             replacement is applied on all string, numeric or boolean columns.
   * @param replacement value replacement map. Key and value of `replacement` map must have
   *                    the same type, and can only be doubles, strings or booleans.
   *                    The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](cols: Array[String], replacement: util.Map[T, T]): DS[Row] = {
    replace(cols.toImmutableArraySeq, replacement.asScala.toMap)
  }

  /**
   * (Scala-specific) Replaces values matching keys in `replacement` map.
   *
   * {{{
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height".
   *   df.na.replace("height", Map(1.0 -> 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "name".
   *   df.na.replace("name", Map("UNKNOWN" -> "unnamed"));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in all string columns.
   *   df.na.replace("*", Map("UNKNOWN" -> "unnamed"));
   * }}}
   *
   * @param col name of the column to apply the value replacement. If `col` is "*",
   *            replacement is applied on all string, numeric or boolean columns.
   * @param replacement value replacement map. Key and value of `replacement` map must have
   *                    the same type, and can only be doubles, strings or booleans.
   *                    The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](col: String, replacement: Map[T, T]): DS[Row]

  /**
   * (Scala-specific) Replaces values matching keys in `replacement` map.
   *
   * {{{
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height" and "weight".
   *   df.na.replace("height" :: "weight" :: Nil, Map(1.0 -> 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "firstname" and "lastname".
   *   df.na.replace("firstname" :: "lastname" :: Nil, Map("UNKNOWN" -> "unnamed"));
   * }}}
   *
   * @param cols list of columns to apply the value replacement. If `col` is "*",
   *             replacement is applied on all string, numeric or boolean columns.
   * @param replacement value replacement map. Key and value of `replacement` map must have
   *                    the same type, and can only be doubles, strings or booleans.
   *                    The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](cols: Seq[String], replacement: Map[T, T]): DS[Row]
}
