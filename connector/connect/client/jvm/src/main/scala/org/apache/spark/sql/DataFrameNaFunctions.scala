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

import org.apache.spark.connect.proto.{NAReplace, Relation}
import org.apache.spark.connect.proto.Expression.{Literal => GLiteral}
import org.apache.spark.connect.proto.NAReplace.Replacement
import org.apache.spark.util.ArrayImplicits._

/**
 * Functionality for working with missing data in `DataFrame`s.
 *
 * @since 3.4.0
 */
final class DataFrameNaFunctions private[sql] (sparkSession: SparkSession, root: Relation) {
  import sparkSession.RichColumn

  /**
   * Returns a new `DataFrame` that drops rows containing any null or NaN values.
   *
   * @since 3.4.0
   */
  def drop(): DataFrame = buildDropDataFrame(None, None)

  /**
   * Returns a new `DataFrame` that drops rows containing null or NaN values.
   *
   * If `how` is "any", then drop rows containing any null or NaN values. If `how` is "all", then
   * drop rows only if every column is null or NaN for that row.
   *
   * @since 3.4.0
   */
  def drop(how: String): DataFrame = {
    buildDropDataFrame(None, buildMinNonNulls(how))
  }

  /**
   * Returns a new `DataFrame` that drops rows containing any null or NaN values in the specified
   * columns.
   *
   * @since 3.4.0
   */
  def drop(cols: Array[String]): DataFrame = drop(cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows containing any null or NaN values
   * in the specified columns.
   *
   * @since 3.4.0
   */
  def drop(cols: Seq[String]): DataFrame = buildDropDataFrame(Some(cols), None)

  /**
   * Returns a new `DataFrame` that drops rows containing null or NaN values in the specified
   * columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null or NaN for that row.
   *
   * @since 3.4.0
   */
  def drop(how: String, cols: Array[String]): DataFrame = drop(how, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows containing null or NaN values in
   * the specified columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null or NaN for that row.
   *
   * @since 3.4.0
   */
  def drop(how: String, cols: Seq[String]): DataFrame = {
    buildDropDataFrame(Some(cols), buildMinNonNulls(how))
  }

  /**
   * Returns a new `DataFrame` that drops rows containing less than `minNonNulls` non-null and
   * non-NaN values.
   *
   * @since 3.4.0
   */
  def drop(minNonNulls: Int): DataFrame = {
    buildDropDataFrame(None, Some(minNonNulls))
  }

  /**
   * Returns a new `DataFrame` that drops rows containing less than `minNonNulls` non-null and
   * non-NaN values in the specified columns.
   *
   * @since 3.4.0
   */
  def drop(minNonNulls: Int, cols: Array[String]): DataFrame =
    drop(minNonNulls, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows containing less than `minNonNulls`
   * non-null and non-NaN values in the specified columns.
   *
   * @since 3.4.0
   */
  def drop(minNonNulls: Int, cols: Seq[String]): DataFrame = {
    buildDropDataFrame(Some(cols), Some(minNonNulls))
  }

  private def buildMinNonNulls(how: String): Option[Int] = {
    how.toLowerCase(Locale.ROOT) match {
      case "any" => None // No-Op. Do nothing.
      case "all" => Some(1)
      case _ => throw new IllegalArgumentException(s"how ($how) must be 'any' or 'all'")
    }
  }

  private def buildDropDataFrame(
      cols: Option[Seq[String]],
      minNonNulls: Option[Int]): DataFrame = {
    sparkSession.newDataFrame { builder =>
      val dropNaBuilder = builder.getDropNaBuilder.setInput(root)
      cols.foreach(c => dropNaBuilder.addAllCols(c.asJava))
      minNonNulls.foreach(dropNaBuilder.setMinNonNulls)
    }
  }

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in numeric columns with `value`.
   *
   * @since 3.4.0
   */
  def fill(value: Long): DataFrame = {
    buildFillDataFrame(None, GLiteral.newBuilder().setLong(value).build())
  }

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in specified numeric columns. If a
   * specified column is not a numeric column, it is ignored.
   *
   * @since 3.4.0
   */
  def fill(value: Long, cols: Array[String]): DataFrame = fill(value, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null or NaN values in specified
   * numeric columns. If a specified column is not a numeric column, it is ignored.
   *
   * @since 3.4.0
   */
  def fill(value: Long, cols: Seq[String]): DataFrame = {
    buildFillDataFrame(Some(cols), GLiteral.newBuilder().setLong(value).build())
  }

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in numeric columns with `value`.
   *
   * @since 3.4.0
   */
  def fill(value: Double): DataFrame = {
    buildFillDataFrame(None, GLiteral.newBuilder().setDouble(value).build())
  }

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in specified numeric columns. If a
   * specified column is not a numeric column, it is ignored.
   *
   * @since 3.4.0
   */
  def fill(value: Double, cols: Array[String]): DataFrame = fill(value, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null or NaN values in specified
   * numeric columns. If a specified column is not a numeric column, it is ignored.
   *
   * @since 3.4.0
   */
  def fill(value: Double, cols: Seq[String]): DataFrame = {
    buildFillDataFrame(Some(cols), GLiteral.newBuilder().setDouble(value).build())
  }

  /**
   * Returns a new `DataFrame` that replaces null values in string columns with `value`.
   *
   * @since 3.4.0
   */
  def fill(value: String): DataFrame = {
    buildFillDataFrame(None, GLiteral.newBuilder().setString(value).build())
  }

  /**
   * Returns a new `DataFrame` that replaces null values in specified string columns. If a
   * specified column is not a string column, it is ignored.
   *
   * @since 3.4.0
   */
  def fill(value: String, cols: Array[String]): DataFrame = fill(value, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null values in specified string
   * columns. If a specified column is not a string column, it is ignored.
   *
   * @since 3.4.0
   */
  def fill(value: String, cols: Seq[String]): DataFrame = {
    buildFillDataFrame(Some(cols), GLiteral.newBuilder().setString(value).build())
  }

  /**
   * Returns a new `DataFrame` that replaces null values in boolean columns with `value`.
   *
   * @since 3.4.0
   */
  def fill(value: Boolean): DataFrame = {
    buildFillDataFrame(None, GLiteral.newBuilder().setBoolean(value).build())
  }

  /**
   * Returns a new `DataFrame` that replaces null values in specified boolean columns. If a
   * specified column is not a boolean column, it is ignored.
   *
   * @since 3.4.0
   */
  def fill(value: Boolean, cols: Array[String]): DataFrame = fill(value, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null values in specified boolean
   * columns. If a specified column is not a boolean column, it is ignored.
   *
   * @since 3.4.0
   */
  def fill(value: Boolean, cols: Seq[String]): DataFrame = {
    buildFillDataFrame(Some(cols), GLiteral.newBuilder().setBoolean(value).build())
  }

  private def buildFillDataFrame(cols: Option[Seq[String]], value: GLiteral): DataFrame = {
    sparkSession.newDataFrame { builder =>
      val fillNaBuilder = builder.getFillNaBuilder.setInput(root)
      fillNaBuilder.addValues(value)
      cols.foreach(c => fillNaBuilder.addAllCols(c.asJava))
    }
  }

  /**
   * Returns a new `DataFrame` that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is the replacement value. The
   * value must be of the following type: `Integer`, `Long`, `Float`, `Double`, `String`,
   * `Boolean`. Replacement values are cast to the column data type.
   *
   * For example, the following replaces null values in column "A" with string "unknown", and null
   * values in column "B" with numeric value 1.0.
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *   df.na.fill(ImmutableMap.of("A", "unknown", "B", 1.0));
   * }}}
   *
   * @since 3.4.0
   */
  def fill(valueMap: java.util.Map[String, Any]): DataFrame = fillMap(valueMap.asScala.toSeq)

  /**
   * Returns a new `DataFrame` that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is the replacement value. The
   * value must be of the following type: `Integer`, `Long`, `Float`, `Double`, `String`,
   * `Boolean`. Replacement values are cast to the column data type.
   *
   * For example, the following replaces null values in column "A" with string "unknown", and null
   * values in column "B" with numeric value 1.0.
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *   df.na.fill(ImmutableMap.of("A", "unknown", "B", 1.0));
   * }}}
   *
   * @since 3.4.0
   */
  def fill(valueMap: Map[String, Any]): DataFrame = fillMap(valueMap.toSeq)

  private def fillMap(values: Seq[(String, Any)]): DataFrame = {
    sparkSession.newDataFrame { builder =>
      val fillNaBuilder = builder.getFillNaBuilder.setInput(root)
      values.map { case (colName, replaceValue) =>
        fillNaBuilder.addCols(colName).addValues(functions.lit(replaceValue).expr.getLiteral)
      }
    }
  }

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
   * @param col
   *   name of the column to apply the value replacement. If `col` is "*", replacement is applied
   *   on all string, numeric or boolean columns.
   * @param replacement
   *   value replacement map. Key and value of `replacement` map must have the same type, and can
   *   only be doubles, strings or booleans. The map value can have nulls.
   * @since 3.4.0
   */
  def replace[T](col: String, replacement: java.util.Map[T, T]): DataFrame =
    replace(col, replacement.asScala.toMap)

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
   * @param col
   *   name of the column to apply the value replacement. If `col` is "*", replacement is applied
   *   on all string, numeric or boolean columns.
   * @param replacement
   *   value replacement map. Key and value of `replacement` map must have the same type, and can
   *   only be doubles, strings or booleans. The map value can have nulls.
   * @since 3.4.0
   */
  def replace[T](col: String, replacement: Map[T, T]): DataFrame = {
    val cols = if (col != "*") Some(Seq(col)) else None
    buildReplaceDataFrame(cols, buildReplacement(replacement))
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
   * @param cols
   *   list of columns to apply the value replacement. If `col` is "*", replacement is applied on
   *   all string, numeric or boolean columns.
   * @param replacement
   *   value replacement map. Key and value of `replacement` map must have the same type, and can
   *   only be doubles, strings or booleans. The map value can have nulls.
   * @since 3.4.0
   */
  def replace[T](cols: Array[String], replacement: java.util.Map[T, T]): DataFrame = {
    replace(cols.toImmutableArraySeq, replacement.asScala.toMap)
  }

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
   * @param cols
   *   list of columns to apply the value replacement. If `col` is "*", replacement is applied on
   *   all string, numeric or boolean columns.
   * @param replacement
   *   value replacement map. Key and value of `replacement` map must have the same type, and can
   *   only be doubles, strings or booleans. The map value can have nulls.
   * @since 3.4.0
   */
  def replace[T](cols: Seq[String], replacement: Map[T, T]): DataFrame = {
    buildReplaceDataFrame(Some(cols), buildReplacement(replacement))
  }

  private def buildReplaceDataFrame(
      cols: Option[Seq[String]],
      replacements: Iterable[NAReplace.Replacement]): DataFrame = {
    sparkSession.newDataFrame { builder =>
      val replaceBuilder = builder.getReplaceBuilder.setInput(root)
      replaceBuilder.addAllReplacements(replacements.asJava)
      cols.foreach(c => replaceBuilder.addAllCols(c.asJava))
    }
  }

  private def buildReplacement[T](replacement: Map[T, T]): Iterable[NAReplace.Replacement] = {
    // Convert the NumericType in replacement map to DoubleType,
    // while leaving StringType, BooleanType and null untouched.
    val replacementMap: Map[_, _] = replacement.map {
      case (k, v: String) => (k, v)
      case (k, v: Boolean) => (k, v)
      case (k: String, null) => (k, null)
      case (k: Boolean, null) => (k, null)
      case (k, null) => (convertToDouble(k), null)
      case (k, v) => (convertToDouble(k), convertToDouble(v))
    }
    replacementMap.map { case (oldValue, newValue) =>
      Replacement
        .newBuilder()
        .setOldValue(functions.lit(oldValue).expr.getLiteral)
        .setNewValue(functions.lit(newValue).expr.getLiteral)
        .build()
    }
  }

  private def convertToDouble(v: Any): Double = v match {
    case v: Float => v.toDouble
    case v: Double => v
    case v: Long => v.toDouble
    case v: Int => v.toDouble
    case v =>
      throw new IllegalArgumentException(s"Unsupported value type ${v.getClass.getName} ($v).")
  }
}
