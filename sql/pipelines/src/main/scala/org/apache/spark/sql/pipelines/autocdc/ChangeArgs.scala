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
   * Applies [[ColumnSelection]] to a [[StructType]] and returns the filtered schema. Field order
   * follows the original schema; only matching fields are retained in the returned schema.
   *
   * @param schemaName      Logical name of the schema being filtered, surfaced in error messages
   *                        when columns are not found (e.g. "microbatch", "target").
   * @param schema          The schema to filter.
   * @param columnSelection The user-provided selection. `None` is a no-op and returns `schema`
   *                        unchanged.
   * @param caseSensitive   Whether to match column names case-sensitively against the schema.
   *                        Callers should derive this from the session, e.g.
   *                        `session.sessionState.conf.caseSensitiveAnalysis`, so column matching
   *                        stays consistent with `spark.sql.caseSensitive`.
   */
  def applyToSchema(
      schemaName: String,
      schema: StructType,
      columnSelection: Option[ColumnSelection],
      caseSensitive: Boolean): StructType = columnSelection match {
    case None =>
      // A None column selection is interpreted as a no-op.
      schema
    case Some(IncludeColumns(cols)) =>
      val keepIndices = lookupFieldIndices(schemaName, schema, cols, caseSensitive)
      StructType(schema.fields.zipWithIndex.collect {
        case (field, idx) if keepIndices.contains(idx) => field
      })
    case Some(ExcludeColumns(cols)) =>
      val dropIndices = lookupFieldIndices(schemaName, schema, cols, caseSensitive)
      StructType(schema.fields.zipWithIndex.collect {
        case (field, idx) if !dropIndices.contains(idx) => field
      })
  }

  private def lookupFieldIndices(
      schemaName: String,
      schema: StructType,
      fields: Seq[UnqualifiedColumnName],
      caseSensitive: Boolean): Set[Int] = {
    val caseAwareGetFieldIndex: String => Option[Int] =
      if (caseSensitive) schema.getFieldIndex else schema.getFieldIndexCaseInsensitive

    val fieldIndexResolutions = fields.map(f => f -> caseAwareGetFieldIndex(f.name))
    val missingFieldNames = fieldIndexResolutions.collect { case (f, None) => f.name }.distinct
    if (missingFieldNames.nonEmpty) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA",
        messageParameters = Map(
          "caseSensitivity" -> CaseSensitivityLabels.of(caseSensitive),
          "schemaName" -> schemaName,
          "missingColumns" -> missingFieldNames.mkString(", "),
          "availableColumns" -> schema.fieldNames.mkString(", ")
        )
      )
    }
    fieldIndexResolutions.flatMap { case (_, idx) => idx }.toSet
  }
}

/** User-facing case-sensitivity labels surfaced in AutoCDC error messages. */
private[pipelines] object CaseSensitivityLabels {
  val CaseSensitive: String = "case-sensitive"
  val CaseInsensitive: String = "case-insensitive"

  def of(caseSensitive: Boolean): String =
    if (caseSensitive) CaseSensitive else CaseInsensitive
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
) {
  ChangeArgs.validateNonEmptyKeys(keys)
}

object ChangeArgs {
  /**
   * Validates that [[ChangeArgs.keys]] is non-empty. Both SCD1 and SCD2 semantics require at
   * least one key column to identify rows; rejecting empty key sets at construction lets
   * downstream consumers rely on `keys.nonEmpty` without re-validating.
   */
  private def validateNonEmptyKeys(keys: Seq[UnqualifiedColumnName]): Unit = {
    if (keys.isEmpty) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_EMPTY_KEYS",
        messageParameters = Map.empty
      )
    }
  }
}
