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
import org.apache.spark.sql.types.StructType

sealed trait ColumnSelection
object ColumnSelection {
  type ColumnList = Seq[String]
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
        validateColumnsExistInSchema(columns = includeColumns, schema = schema)

        val includeColumnSet = includeColumns.toSet
        StructType(schema.fields.filter(f => includeColumnSet.contains(f.name)))
      case Some(ExcludeColumns(excludeColumns)) =>
        validateColumnsExistInSchema(columns = excludeColumns, schema = schema)

        val excludeColumnSet = excludeColumns.toSet
        StructType(schema.fields.filterNot(f => excludeColumnSet.contains(f.name)))
    }

  private def validateColumnsExistInSchema(columns: ColumnList, schema: StructType): Unit = {
    val schemaColumns = schema.fieldNames.toSet
    val missingColumns = columns.filterNot(schemaColumns.contains).distinct
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
  case object Type1 extends ScdType
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
 * @param columnSelection Which source columns to include in the target table. None means
 *                        all columns.
 */
case class ChangeArgs(
    keys: Seq[String],
    sequencing: Column,
    deleteCondition: Option[Column] = None,
    storedAsScdType: ScdType,
    columnSelection: Option[ColumnSelection] = None
)
