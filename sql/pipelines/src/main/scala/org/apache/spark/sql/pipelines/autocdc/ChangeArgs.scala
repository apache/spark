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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.sql.{AnalysisException, Column}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructType

/** A column reference that must be a single, unqualified identifier (no nested
  * field path and no table/alias qualifier). The constructor parses
  * [[columnName]] with the Spark SQL parser and throws an [[AnalysisException]]
  * if it does not resolve to exactly one name part.
  */
case class UnqualifiedColumnName(name: String) {
  UnqualifiedColumnName.validate(name)
}

object UnqualifiedColumnName {
  private def validate(columnName: String): Unit = {
    val nameParts = CatalystSqlParser.parseMultipartIdentifier(columnName)
    if (nameParts.length != 1) {
      throw multipartColumnIdentifierError(columnName, nameParts)
    }
  }

  private def multipartColumnIdentifierError(
      columnName: String,
      nameParts: Seq[String]
  ): AnalysisException =
    new AnalysisException(
      errorClass = "AUTOCDC_INVALID_COLUMN_SELECTION.MULTIPART_COLUMN_IDENTIFIER",
      messageParameters = Map(
        "columnName" -> columnName,
        "nameParts" -> nameParts.mkString(", ")
      )
    )
}

sealed trait ColumnSelection
object ColumnSelection {
  type ColumnList = Seq[UnqualifiedColumnName]

  case class IncludeColumns(columns: ColumnList) extends ColumnSelection
  case class ExcludeColumns(columns: ColumnList) extends ColumnSelection

  /**
   * Applies [[ColumnSelection]] to a [[StructType]] and returns the filtered schema.
   * Field names are matched exactly. Field order follows the original schema (filtered in place).
   */
  def applyToSchema(schema: StructType, columnSelection: Option[ColumnSelection]): StructType =
    columnSelection match {
      case None =>
        // A none column selection is interpreted as a no-op.
        schema
      case Some(IncludeColumns(includeColumns)) =>
        validateColumnsExistInSchema(includeColumns, schema)

        val includeColumnSet = includeColumns.map(_.name).toSet
        StructType(schema.fields.filter(f => includeColumnSet.contains(f.name)))
      case Some(ExcludeColumns(excludeColumns)) =>
        validateColumnsExistInSchema(excludeColumns, schema)

        val excludeColumnSet = excludeColumns.map(_.name).toSet
        StructType(schema.fields.filterNot(f => excludeColumnSet.contains(f.name)))
    }

  private def validateColumnsExistInSchema(columns: ColumnList, schema: StructType): Unit = {
    val schemaColumns = schema.fieldNames.toSet
    val missingColumns = columns.map(_.name).filterNot(schemaColumns.contains).distinct
    if (missingColumns.nonEmpty) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_INVALID_COLUMN_SELECTION.COLUMNS_NOT_FOUND",
        messageParameters = Map(
          "missingColumns" -> missingColumns.mkString(", "),
          "availableColumns" -> schema.fieldNames.mkString(", ")
        ))
    }
  }
}

/** The SCD (Slowly Changing Dimension) strategy for a CDC flow. */
sealed trait ScdType

object ScdType {
  /** Representation for the standard SCD1 strategy. */
  case object Type1 extends ScdType
  /** Representation for the standard SCD2 strategy. */
  case object Type2 extends ScdType
}

/**
 * Configuration for an AutoCDC flow.
 *
 * @param keys            The column(s) that uniquely identify a row in the source data.
 * @param sequencing      Expression ordering CDC events to correctly resolve out-of-order
 *                        arrivals. Must be a sortable type.
 * @param deleteCondition Expression that marks a source row as a DELETE. When None, all
 *                        rows are treated as upserts.
 * @param storedAsScdType The SCD strategy these args should be applied to.
 * @param columnSelection Which source columns to select in the target table. None means
 *                        all columns.
 */
case class ChangeArgs(
    keys: Seq[String],
    sequencing: Column,
    storedAsScdType: ScdType,
    deleteCondition: Option[Column] = None,
    columnSelection: Option[ColumnSelection] = None
)
