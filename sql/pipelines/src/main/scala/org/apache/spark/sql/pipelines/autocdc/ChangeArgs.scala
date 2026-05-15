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

import java.util.Locale

import org.apache.spark.sql.{AnalysisException, Column}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.types.StructType

/**
 * A single, unqualified column identifier (no nested path or table/alias qualifier). Backticks
 * are consumed: "`a.b`" is stored as "a.b" in [[name]]. Use [[name]] for direct schema-fieldName
 * comparison and [[quoted]] for APIs that re-parse identifier strings.
 */
case class UnqualifiedColumnName private (name: String) {
  def quoted: String = QuotingUtils.quoteIdentifier(name)
}

object UnqualifiedColumnName {
  def apply(input: String): UnqualifiedColumnName = {
    val nameParts = CatalystSqlParser.parseMultipartIdentifier(input)
    if (nameParts.length != 1) {
      throw multipartColumnIdentifierError(input, nameParts)
    }
    new UnqualifiedColumnName(nameParts.head)
  }

  private def multipartColumnIdentifierError(
      columnName: String,
      nameParts: Seq[String]
  ): AnalysisException =
    new AnalysisException(
      errorClass = "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER",
      messageParameters = Map(
        "columnName" -> columnName,
        "nameParts" -> nameParts.mkString(", ")
      )
    )
}

sealed trait ColumnSelection
object ColumnSelection {

  case class IncludeColumns(columns: Seq[UnqualifiedColumnName]) extends ColumnSelection
  case class ExcludeColumns(columns: Seq[UnqualifiedColumnName])
      extends ColumnSelection

  /**
   * Applies [[ColumnSelection]] to a [[StructType]] and returns the filtered schema. Field
   * order follows the original schema; filtering happens in place.
   */
  def applyToSchema(
      schemaName: String,
      schema: StructType,
      columnSelection: Option[ColumnSelection],
      ignoreCase: Boolean): StructType = columnSelection match {
    case None =>
      // A none column selection is interpreted as a no-op.
      schema
    case Some(IncludeColumns(cols)) =>
      val includeColumnNames = cols.map(_.name)
      validateColumnsExistInSchema(schemaName, schema, includeColumnNames, ignoreCase)

      val caseNormalizedIncludeColumnNames =
        includeColumnNames.map(normalizeCase(_, ignoreCase)).toSet

      StructType(
        schema.fields.filter(schemaField =>
          caseNormalizedIncludeColumnNames.contains(normalizeCase(schemaField.name, ignoreCase))
        )
      )
    case Some(ExcludeColumns(cols)) =>
      val excludeColumnNames = cols.map(_.name)
      validateColumnsExistInSchema(schemaName, schema, excludeColumnNames, ignoreCase)

      val caseNormalizedExcludeColumnNames =
        excludeColumnNames.map(normalizeCase(_, ignoreCase)).toSet

      StructType(
        schema.fields.filterNot(schemaField =>
          caseNormalizedExcludeColumnNames.contains(normalizeCase(schemaField.name, ignoreCase))
        )
      )
  }

  private def validateColumnsExistInSchema(
      schemaName: String,
      schema: StructType,
      columnNames: Seq[String],
      ignoreCase: Boolean): Unit = {
    val caseNormalizedSchemaColumns =
      schema.fieldNames.map(normalizeCase(_, ignoreCase)).toSet

    // Compare folded forms but report the missing and available columns using their original
    // casing so error messages reflect what the user actually wrote and what the schema holds.
    val columnsMissingInSchema = columnNames
      .filterNot(columnName =>
        caseNormalizedSchemaColumns.contains(normalizeCase(columnName, ignoreCase))
      )
      .distinct

    if (columnsMissingInSchema.nonEmpty) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA",
        messageParameters = Map(
          "caseSensitivity" -> CaseSensitivityLabels.of(ignoreCase),
          "schemaName" -> schemaName,
          "missingColumns" -> columnsMissingInSchema.mkString(", "),
          "availableColumns" -> schema.fieldNames.mkString(", ")
        ))
    }
  }

  /**
   * If ignoreCase, normalize all strings to lowercase for stable comparison.
   */
  private def normalizeCase(name: String, ignoreCase: Boolean): String = {
    if (ignoreCase) {
      name.toLowerCase(Locale.ROOT)
    } else {
      name
    }
  }
}

/** User-facing case-sensitivity labels surfaced in AutoCDC error messages. */
private[autocdc] object CaseSensitivityLabels {
  val CaseSensitive: String = "case-sensitive"
  val CaseInsensitive: String = "case-insensitive"

  def of(ignoreCase: Boolean): String =
    if (ignoreCase) CaseInsensitive else CaseSensitive
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
    keys: Seq[UnqualifiedColumnName],
    sequencing: Column,
    storedAsScdType: ScdType,
    deleteCondition: Option[Column] = None,
    columnSelection: Option[ColumnSelection] = None
)
