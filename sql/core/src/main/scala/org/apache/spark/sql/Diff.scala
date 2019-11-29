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

import org.apache.spark.sql.functions.{coalesce, lit, not, when}

/**
 * Differ class to diff two Datasets.
 * @param options options for the diffing process
 */
class Diff(options: DiffOptions) {

  def checkSchema[T](left: Dataset[T], right: Dataset[T], idColumns: String*): Unit = {
    require(left.columns.length == right.columns.length,
      "The number of columns doesn't match.\n" +
        s"Left column names (${left.columns.length}): ${left.columns.mkString(", ")}\n" +
        s"Right column names (${right.columns.length}): ${right.columns.mkString(", ")}")

    require(left.columns.length > 0, "The schema must not be empty")

    // we ignore the nullability of fields
    val leftFields = left.schema.fields.map(f => (f.name, f.dataType))
    val rightFields = right.schema.fields.map(f => (f.name, f.dataType))
    val leftExtraSchema = leftFields.diff(rightFields)
    val rightExtraSchema = rightFields.diff(leftFields)
    val extraSchema = leftExtraSchema.union(rightExtraSchema)
    require(extraSchema.isEmpty,
      "The datasets do not have the same schema.\n" +
        s"Left extra columns: ${leftExtraSchema.map(t => s"${t._1} (${t._2})").mkString(", ")}\n" +
        s"Right extra columns: ${rightExtraSchema.map(t => s"${t._1} (${t._2})").mkString(", ")}")

    val missingIdColumns = idColumns.diff(left.columns)
    require(missingIdColumns.isEmpty,
      s"Some id columns do not exist: ${missingIdColumns.mkString(", ")}")
  }

  def of[T](left: Dataset[T], right: Dataset[T], idColumns: String*): DataFrame = {
    checkSchema(left, right, idColumns: _*)

    val pkColumns = if (idColumns.isEmpty) left.columns.toList else idColumns
    val otherColumns = left.columns.diff(pkColumns)

    val existsColumnName = "exists" // make it not exist in schema T
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
    // TODO: require schema of encoder to be correct
    of(left, right, idColumns: _*).as[U](diffEncoder)
  }

}

/**
 * Diffing singleton with default diffing options.
 */
object Diff {
  val default = new Diff(DiffOptions.default)

  def of[T](left: Dataset[T], right: Dataset[T], idColumns: String*): DataFrame =
    default.of(left, right, idColumns: _*)

  def ofAs[T, U](left: Dataset[T], right: Dataset[T], idColumns: String*)
                (implicit diffEncoder: Encoder[U]): Dataset[U] =
    default.ofAs(left, right, idColumns: _*)

  def ofAs[T, U](left: Dataset[T], right: Dataset[T],
                 diffEncoder: Encoder[U], idColumns: String*): Dataset[U] =
    default.ofAs(left, right, diffEncoder, idColumns: _*)
}
