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
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.ExpressionUtils.column
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * Functionality for working with missing data in `DataFrame`s.
 *
 * @since 1.3.1
 */
@Stable
final class DataFrameNaFunctions private[sql](df: DataFrame) {
  import df.sparkSession.RichColumn

  /**
   * Returns a new `DataFrame` that drops rows containing any null or NaN values.
   *
   * @since 1.3.1
   */
  def drop(): DataFrame = drop0("any", outputAttributes)

  /**
   * Returns a new `DataFrame` that drops rows containing null or NaN values.
   *
   * If `how` is "any", then drop rows containing any null or NaN values.
   * If `how` is "all", then drop rows only if every column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  def drop(how: String): DataFrame = drop0(how, outputAttributes)

  /**
   * Returns a new `DataFrame` that drops rows containing any null or NaN values
   * in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(cols: Array[String]): DataFrame = drop(cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows containing any null or NaN values
   * in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(cols: Seq[String]): DataFrame = drop(cols.size, cols)

  /**
   * Returns a new `DataFrame` that drops rows containing null or NaN values
   * in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  def drop(how: String, cols: Array[String]): DataFrame = drop(how, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows containing null or NaN values
   * in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  def drop(how: String, cols: Seq[String]): DataFrame = {
    drop0(how, cols.map(df.resolve(_)))
  }

  /**
   * Returns a new `DataFrame` that drops rows containing
   * less than `minNonNulls` non-null and non-NaN values.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int): DataFrame = drop(minNonNulls, df.columns)

  /**
   * Returns a new `DataFrame` that drops rows containing
   * less than `minNonNulls` non-null and non-NaN values in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int, cols: Array[String]): DataFrame =
    drop(minNonNulls, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows containing less than
   * `minNonNulls` non-null and non-NaN values in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int, cols: Seq[String]): DataFrame = {
    drop0(minNonNulls, cols.map(df.resolve(_)))
  }

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in numeric columns with `value`.
   *
   * @since 2.2.0
   */
  def fill(value: Long): DataFrame = fillValue(value, outputAttributes)

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in numeric columns with `value`.
   * @since 1.3.1
   */
  def fill(value: Double): DataFrame = fillValue(value, outputAttributes)

  /**
   * Returns a new `DataFrame` that replaces null values in string columns with `value`.
   *
   * @since 1.3.1
   */
  def fill(value: String): DataFrame = fillValue(value, outputAttributes)

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in specified numeric columns.
   * If a specified column is not a numeric column, it is ignored.
   *
   * @since 2.2.0
   */
  def fill(value: Long, cols: Array[String]): DataFrame = fill(value, cols.toImmutableArraySeq)

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in specified numeric columns.
   * If a specified column is not a numeric column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: Double, cols: Array[String]): DataFrame = fill(value, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null or NaN values in specified
   * numeric columns. If a specified column is not a numeric column, it is ignored.
   *
   * @since 2.2.0
   */
  def fill(value: Long, cols: Seq[String]): DataFrame = fillValue(value, toAttributes(cols))

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null or NaN values in specified
   * numeric columns. If a specified column is not a numeric column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: Double, cols: Seq[String]): DataFrame = fillValue(value, toAttributes(cols))


  /**
   * Returns a new `DataFrame` that replaces null values in specified string columns.
   * If a specified column is not a string column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: String, cols: Array[String]): DataFrame = fill(value, cols.toImmutableArraySeq)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null values in
   * specified string columns. If a specified column is not a string column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: String, cols: Seq[String]): DataFrame = fillValue(value, toAttributes(cols))

  /**
   * Returns a new `DataFrame` that replaces null values in boolean columns with `value`.
   *
   * @since 2.3.0
   */
  def fill(value: Boolean): DataFrame = fillValue(value, outputAttributes)

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null values in specified
   * boolean columns. If a specified column is not a boolean column, it is ignored.
   *
   * @since 2.3.0
   */
  def fill(value: Boolean, cols: Seq[String]): DataFrame = fillValue(value, toAttributes(cols))

  /**
   * Returns a new `DataFrame` that replaces null values in specified boolean columns.
   * If a specified column is not a boolean column, it is ignored.
   *
   * @since 2.3.0
   */
  def fill(value: Boolean, cols: Array[String]): DataFrame = fill(value, cols.toImmutableArraySeq)


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
  def fill(valueMap: java.util.Map[String, Any]): DataFrame = fillMap(valueMap.asScala.toSeq)

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
  def fill(valueMap: Map[String, Any]): DataFrame = fillMap(valueMap.toSeq)

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
  def replace[T](col: String, replacement: java.util.Map[T, T]): DataFrame = {
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
  def replace[T](cols: Array[String], replacement: java.util.Map[T, T]): DataFrame = {
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
  def replace[T](col: String, replacement: Map[T, T]): DataFrame = {
    if (col == "*") {
      replace0(df.logicalPlan.output, replacement)
    } else {
      replace(Seq(col), replacement)
    }
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
   * @param cols list of columns to apply the value replacement. If `col` is "*",
   *             replacement is applied on all string, numeric or boolean columns.
   * @param replacement value replacement map. Key and value of `replacement` map must have
   *                    the same type, and can only be doubles, strings or booleans.
   *                    The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](cols: Seq[String], replacement: Map[T, T]): DataFrame = {
    val attrs = cols.map { colName =>
      // Check column name exists
      val attr = df.resolve(colName) match {
        case a: Attribute => a
        case _ => throw QueryExecutionErrors.nestedFieldUnsupportedError(colName)
      }
      attr
    }
    replace0(attrs, replacement)
  }

  private def replace0[T](attrs: Seq[Attribute], replacement: Map[T, T]): DataFrame = {
    if (replacement.isEmpty || attrs.isEmpty) {
      return df
    }

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

    // targetColumnType is either DoubleType, StringType or BooleanType,
    // depending on the type of first key in replacement map.
    // Only fields of targetColumnType will perform replacement.
    val targetColumnType = replacement.head._1 match {
      case _: jl.Double | _: jl.Float | _: jl.Integer | _: jl.Long => DoubleType
      case _: jl.Boolean => BooleanType
      case _: String => StringType
    }

    val output = df.queryExecution.analyzed.output
    val projections = output.map { attr =>
      if (attrs.contains(attr) && (attr.dataType == targetColumnType ||
        (attr.dataType.isInstanceOf[NumericType] && targetColumnType == DoubleType))) {
        replaceCol(attr, replacementMap)
      } else {
        column(attr)
      }
    }
    df.select(projections : _*)
  }

  private def fillMap(values: Seq[(String, Any)]): DataFrame = {
    // Error handling
    val attrToValue = AttributeMap(values.map { case (colName, replaceValue) =>
      // Check column name exists
      val attr = df.resolve(colName) match {
        case a: Attribute => a
        case _ => throw QueryExecutionErrors.nestedFieldUnsupportedError(colName)
      }
      // Check data type
      replaceValue match {
        case _: jl.Double | _: jl.Float | _: jl.Integer | _: jl.Long | _: jl.Boolean | _: String =>
          // This is good
        case _ => throw new IllegalArgumentException(
          s"Unsupported value type ${replaceValue.getClass.getName} ($replaceValue).")
      }
      attr -> replaceValue
    })

    val output = df.queryExecution.analyzed.output
    val projections = output.map {
      attr => attrToValue.get(attr).map {
        case v: jl.Float => fillCol[Float](attr, v)
        case v: jl.Double => fillCol[Double](attr, v)
        case v: jl.Long => fillCol[Long](attr, v)
        case v: jl.Integer => fillCol[Integer](attr, v)
        case v: jl.Boolean => fillCol[Boolean](attr, v.booleanValue())
        case v: String => fillCol[String](attr, v)
      }.getOrElse(column(attr))
    }
    df.select(projections : _*)
  }

  /**
   * Returns a [[Column]] expression that replaces null value in column defined by `attr`
   * with `replacement`.
   */
  private def fillCol[T](attr: Attribute, replacement: T): Column = {
    fillCol(attr.dataType, attr.name, column(attr), replacement)
  }

  /**
   * Returns a [[Column]] expression that replaces null value in `expr` with `replacement`.
   * It uses the given `expr` as a column.
   */
  private def fillCol[T](dataType: DataType, name: String, expr: Column, replacement: T): Column = {
    val colValue = dataType match {
      case DoubleType | FloatType =>
        nanvl(expr, lit(null)) // nanvl only supports these types
      case _ => expr
    }
    coalesce(colValue, lit(replacement).cast(dataType)).as(name)
  }

  /**
   * Returns a [[Column]] expression that replaces value matching key in `replacementMap` with
   * value in `replacementMap`, using [[CaseWhen]].
   *
   * TODO: This can be optimized to use broadcast join when replacementMap is large.
   */
  private def replaceCol[K, V](attr: Attribute, replacementMap: Map[K, V]): Column = {
    def buildExpr(v: Any) = Cast(Literal(v), attr.dataType)
    val branches = replacementMap.flatMap { case (source, target) =>
      Seq(Literal(source), buildExpr(target))
    }.toSeq
    column(CaseKeyWhen(attr, branches :+ attr)).as(attr.name)
  }

  private def convertToDouble(v: Any): Double = v match {
    case v: Float => v.toDouble
    case v: Double => v
    case v: Long => v.toDouble
    case v: Int => v.toDouble
    case v => throw new IllegalArgumentException(
      s"Unsupported value type ${v.getClass.getName} ($v).")
  }

  private def toAttributes(cols: Seq[String]): Seq[Attribute] = {
    cols.map(name => df.col(name).expr).collect {
      case a: Attribute => a
    }
  }

  private def outputAttributes: Seq[Attribute] = {
    df.queryExecution.analyzed.output
  }

  private def drop0(how: String, cols: Seq[NamedExpression]): DataFrame = {
    how.toLowerCase(Locale.ROOT) match {
      case "any" => drop0(cols.size, cols)
      case "all" => drop0(1, cols)
      case _ => throw new IllegalArgumentException(s"how ($how) must be 'any' or 'all'")
    }
  }

  private def drop0(minNonNulls: Int, cols: Seq[NamedExpression]): DataFrame = {
    // Filtering condition:
    // only keep the row if it has at least `minNonNulls` non-null and non-NaN values.
    val predicate = AtLeastNNonNulls(minNonNulls, cols)
    df.filter(column(predicate))
  }

  private[sql] def fillValue(value: Any, cols: Option[Seq[String]]): DataFrame = {
    fillValue(value, cols.map(toAttributes).getOrElse(outputAttributes))
  }

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in the specified
   * columns. If a specified column is not a numeric, string or boolean column,
   * it is ignored.
   */
  private def fillValue[T](value: T, cols: Seq[Attribute]): DataFrame = {
    // the fill[T] which T is  Long/Double,
    // should apply on all the NumericType Column, for example:
    // val input = Seq[(java.lang.Integer, java.lang.Double)]((null, 164.3)).toDF("a","b")
    // input.na.fill(3.1)
    // the result is (3,164.3), not (null, 164.3)
    val targetType = value match {
      case _: Double | _: Long => NumericType
      case _: String => StringType
      case _: Boolean => BooleanType
      case _ => throw new IllegalArgumentException(
        s"Unsupported value type ${value.getClass.getName} ($value).")
    }

    val projections = outputAttributes.map { col =>
      val typeMatches = (targetType, col.dataType) match {
        case (NumericType, dt) => dt.isInstanceOf[NumericType]
        case (StringType, dt) => dt == StringType
        case (BooleanType, dt) => dt == BooleanType
        case _ =>
          throw new IllegalArgumentException(s"$targetType is not matched at fillValue")
      }
      // Only fill if the column is part of the cols list.
      if (typeMatches && cols.exists(_.semanticEquals(col))) {
        fillCol(col.dataType, col.name, column(col), value)
      } else {
        column(col)
      }
    }
    df.select(projections : _*)
  }
}
