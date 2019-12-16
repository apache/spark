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

import org.apache.spark.sql.functions.{coalesce, lit, not, when}
import org.apache.spark.sql.internal.SQLConf

/**
 * Differ class to diff two Datasets.
 * @param options options for the diffing process
 */
private[sql] class Diff(options: DiffOptions) {

  // column names case-sensitivity can be configured
  private def columnName(columnName: String): String =
    if (SQLConf.get.caseSensitiveAnalysis) columnName else columnName.toLowerCase(Locale.ROOT)

  def checkSchema[T](left: Dataset[T], right: Dataset[T], idColumns: String*): Unit = {
    require(left.columns.length == right.columns.length,
      "The number of columns doesn't match.\n" +
        s"Left column names (${left.columns.length}): ${left.columns.mkString(", ")}\n" +
        s"Right column names (${right.columns.length}): ${right.columns.mkString(", ")}")

    require(left.columns.length > 0, "The schema must not be empty")

    // column types must match but we ignore the nullability of columns
    val leftFields = left.schema.fields.map(f => columnName(f.name) -> f.dataType)
    val rightFields = right.schema.fields.map(f => columnName(f.name) -> f.dataType)
    val leftExtraSchema = leftFields.diff(rightFields)
    val rightExtraSchema = rightFields.diff(leftFields)
    require(leftExtraSchema.isEmpty && rightExtraSchema.isEmpty,
      "The datasets do not have the same schema.\n" +
        s"Left extra columns: ${leftExtraSchema.map(t => s"${t._1} (${t._2})").mkString(", ")}\n" +
        s"Right extra columns: ${rightExtraSchema.map(t => s"${t._1} (${t._2})").mkString(", ")}")

    val columns = left.columns.map(columnName)
    val pkColumns = if (idColumns.isEmpty) columns.toList else idColumns.map(columnName)
    val missingIdColumns = pkColumns.diff(columns)
    require(missingIdColumns.isEmpty,
      s"Some id columns do not exist: ${missingIdColumns.mkString(", ")}")

    require(!pkColumns.contains(options.diffColumn),
      s"The id columns must not contain the diff column name '${options.diffColumn}': " +
        s"${pkColumns.mkString(", ")}")

    val nonIdColumns = columns.diff(pkColumns)
    val diffValueColumns = getDiffValueColumns(nonIdColumns)

    require(!diffValueColumns.contains(options.diffColumn),
      s"The column prefixes '${options.leftColumnPrefix}' and '${options.rightColumnPrefix}', " +
        s"together with these non-id columns " +
        s"must not produce the diff column name '${options.diffColumn}': " +
        s"${nonIdColumns.mkString(", ")}")

    require(diffValueColumns.forall(column => !pkColumns.contains(column)),
      s"The column prefixes '${options.leftColumnPrefix}' and '${options.rightColumnPrefix}', " +
        s"together with these non-id columns " +
        s"must not produce any id column name '${pkColumns.mkString("', '")}': " +
        s"${nonIdColumns.mkString(", ")}")
  }

  def getDiffValueColumns(nonIdColumns: Seq[String]): Seq[String] =
    Seq(options.leftColumnPrefix, options.rightColumnPrefix)
      .flatMap(prefix => nonIdColumns.map(column => s"${prefix}_$column"))

  def of[T](left: Dataset[T], right: Dataset[T], idColumns: String*): DataFrame = {
    checkSchema(left, right, idColumns: _*)

    val pkColumns = if (idColumns.isEmpty) left.columns.toList else idColumns
    val pkColumnsCs = pkColumns.map(columnName).toSet
    val otherColumns = left.columns.filter(col => !pkColumnsCs.contains(columnName(col)))

    val existsColumnName = Diff.distinctStringNameFor(left.columns)
    val l = left.withColumn(existsColumnName, lit(1))
    val r = right.withColumn(existsColumnName, lit(1))
    val joinCondition = pkColumns.map(c => l(c) <=> r(c)).reduce(_ && _)
    val unChanged = otherColumns.map(c => l(c) <=> r(c)).reduceOption(_ && _)
    val changeCondition = not(unChanged.getOrElse(lit(true)))

    val diffCondition =
      when(l(existsColumnName).isNull, lit(options.insertDiffValue)).
        when(r(existsColumnName).isNull, lit(options.deleteDiffValue)).
        when(changeCondition, lit(options.changeDiffValue)).
        otherwise(lit(options.nochangeDiffValue))

    val diffColumns =
      pkColumns.map(c => coalesce(l(c), r(c)).as(c)) ++
        otherColumns.flatMap(c =>
          Seq(
            left(c).as(s"${options.leftColumnPrefix}_$c"),
            right(c).as(s"${options.rightColumnPrefix}_$c")
          )
        )

    l.join(r, joinCondition, "fullouter")
      .select(diffCondition.as(options.diffColumn) +: diffColumns: _*)
  }

  def ofAs[T, U](left: Dataset[T], right: Dataset[T], idColumns: String*)
               (implicit diffEncoder: Encoder[U]): Dataset[U] = {
    ofAs(left, right, diffEncoder, idColumns: _*)
  }

  def ofAs[T, U](left: Dataset[T], right: Dataset[T],
                 diffEncoder: Encoder[U], idColumns: String*): Dataset[U] = {
    val nonIdColumns = left.columns.diff(if (idColumns.isEmpty) left.columns.toList else idColumns)
    val encColumns = diffEncoder.schema.fields.map(_.name)
    val diffColumns = Seq(options.diffColumn) ++ idColumns ++ getDiffValueColumns(nonIdColumns)
    val extraColumns = encColumns.diff(diffColumns)

    require(extraColumns.isEmpty,
      s"Diff encoder's columns must be part of the diff result schema, " +
        s"these columns are unexpected: ${extraColumns.mkString(", ")}")

    of(left, right, idColumns: _*).as[U](diffEncoder)
  }

}

/**
 * Diffing singleton with default diffing options.
 */
private[sql] object Diff {
  val default = new Diff(DiffOptions.default)

  /**
   * Provides a string  that is distinct w.r.t. the given strings.
   * @param existing strings
   * @return distinct string w.r.t. existing
   */
  def distinctStringNameFor(existing: Seq[String]): String = {
    "_" * (existing.map(_.length).reduceOption(_ max _).getOrElse(0) + 1)
  }

  def of[T](left: Dataset[T], right: Dataset[T], idColumns: String*): DataFrame =
    default.of(left, right, idColumns: _*)

  def ofAs[T, U](left: Dataset[T], right: Dataset[T], idColumns: String*)
                (implicit diffEncoder: Encoder[U]): Dataset[U] =
    default.ofAs(left, right, idColumns: _*)

  def ofAs[T, U](left: Dataset[T], right: Dataset[T],
                 diffEncoder: Encoder[U], idColumns: String*): Dataset[U] =
    default.ofAs(left, right, diffEncoder, idColumns: _*)

}
