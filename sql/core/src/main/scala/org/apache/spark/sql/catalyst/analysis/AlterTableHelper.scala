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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.CreateTableHelper.constantFoldDefaultValue
import org.apache.spark.sql.catalyst.plans.logical.{AlterColumns, AlterColumnSpec, DefaultValueExpression, QualifiedColType}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, ResolveDefaultColumns => DefaultCols}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, V1Table}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsCommand, AlterTableChangeColumnCommand, AlterTableChangeColumnCommentsCommand}
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StringType, StructField}

/** Lowering of resolved V1 column additions, changes and comments. */
private[sql] object AlterTableHelper {

  /** Lowers column additions after name, position and default-value resolution. */
  def lowerAddColumns(
      ident: TableIdentifier,
      cols: Seq[QualifiedColType]): AlterTableAddColumnsCommand = {
    cols.foreach { c =>
      if (c.name.length > 1) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          ident, "ADD COLUMN with qualified column")
      }
      if (!c.nullable) {
        throw QueryCompilationErrors.addColumnWithV1TableCannotSpecifyNotNullError()
      }
    }
    // For V1 ALTER TABLE ADD COLUMNS command, the default value expression is hidden in the
    // StructField metadata, and won't be constant folded by the optimizer. Here we
    // manually constant fold it, as exist default needs to be a constant.
    val newCols = cols.map { col =>
      val builder = new MetadataBuilder
      col.comment.foreach(builder.putString("comment", _))
      col.default.foreach { defaultValue =>
        addDefaultValueMetadata(builder, defaultValue, col.colName, col.dataType,
          "ALTER TABLE ADD COLUMNS")
      }
      StructField(col.name.head, col.dataType, nullable = true, builder.build())
    }
    AlterTableAddColumnsCommand(ident, newCols)
  }

  /** Lowers a V1 column change after default-value type coercion. */
  def lowerAlterColumns(
      a: AlterColumns,
      catalog: TableCatalog,
      ident: Identifier,
      table: V1Table,
      resolver: => Resolver): AlterTableChangeColumnCommand = {
    if (a.specs.size > 1) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, "ALTER COLUMN in bulk")
    }
    val s = a.specs.head
    if (s.column.name.length > 1) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, "ALTER COLUMN with qualified column")
    }
    if (s.newNullability.isDefined) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, "ALTER COLUMN ... SET NOT NULL")
    }
    if (s.newPosition.isDefined) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, "ALTER COLUMN ... FIRST | AFTER")
    }
    val builder = new MetadataBuilder
    // Add comment to metadata
    s.newComment.foreach(c => builder.putString("comment", c))
    val colName = s.column.name.head
    val dataType = s.newDataType.getOrElse {
      table.schema.findNestedField(Seq(colName), resolver = resolver)
        .map {
          case (_, StructField(_, st: StringType, _, metadata)) =>
            CharVarcharUtils.getRawType(metadata).getOrElse(st)
          case (_, field) => field.dataType
        }
        .getOrElse {
          throw QueryCompilationErrors.unresolvedColumnError(
            toSQLId(s.column.name), table.schema.fieldNames)
        }
    }
    // For V1 ALTER TABLE ALTER COLUMN command, we only set the CURRENT_DEFAULT metadata.
    // The EXISTS_DEFAULT should be preserved from the original column, as it represents
    // the default value that was in effect when the column was added (used for backfilling).
    s.newDefaultExpression.foreach { defaultValue =>
      // Validate the default value expression
      constantFoldDefaultValue(defaultValue, colName, dataType, "ALTER TABLE ALTER COLUMN")
      builder.putExpression(
        DefaultCols.CURRENT_DEFAULT_COLUMN_METADATA_KEY, defaultValue.originalSQL,
        Some(defaultValue.child))
    }
    if (s.dropDefault) {
      // for legacy reasons, "" means clearing default value
      builder.putString(CURRENT_DEFAULT_COLUMN_METADATA_KEY, "")
    }
    val newColumn = StructField(
      colName,
      dataType,
      nullable = true,
      builder.build())
    AlterTableChangeColumnCommand(
      table.catalogTable.identifier, colName, newColumn, dropComment = s.dropComment)
  }

  /**
   * Lowers comment-only column changes, preserving duplicate-column error context.
   * The identifier is read after validation, and the display name only on failure.
   */
  def lowerColumnComments(
      a: AlterColumns,
      tableName: => String,
      identifier: => TableIdentifier): AlterTableChangeColumnCommentsCommand = {
    // ResolveSessionCatalog rewrites AlterColumns into a command before CheckAnalysis runs,
    // so repeat its duplicate-column check here. Use a.failAnalysis to preserve query context.
    AlterColumns.findRepeatedColumn(a.specs).foreach { name =>
      a.failAnalysis(
        errorClass = "NOT_SUPPORTED_CHANGE_SAME_COLUMN",
        messageParameters = Map(
          "table" -> toSQLId(tableName),
          "fieldName" -> toSQLId(name)))
    }
    AlterTableChangeColumnCommentsCommand(identifier, a.changes)
  }

  // True when the ALTER COLUMN specs only change comments (set or drop, no type / nullability /
  // position / default changes) and, taken together, touch multiple columns or a nested field.
  // Such changes come from `COMMENT ON TABLE ... COLUMN (...)` and cannot be expressed by the
  // single-column, top-level-only V1 `AlterTableChangeColumnCommand`, so they are handled by the
  // dedicated `AlterTableChangeColumnCommentsCommand` instead.
  def isMultiOrNestedCommentOnly(specs: Seq[AlterColumnSpec]): Boolean = {
    val commentOnly = specs.forall { s =>
      (s.newComment.isDefined || s.dropComment) &&
        s.newDataType.isEmpty && s.newNullability.isEmpty && s.newPosition.isEmpty &&
        s.newDefaultExpression.isEmpty && !s.dropDefault
    }
    commentOnly && (specs.size > 1 || specs.exists(_.column.name.length > 1))
  }

  /**
   * Validates and constant-folds a column default value expression for V1 tables.
   * Populates both CURRENT_DEFAULT and EXISTS_DEFAULT in the metadata builder.
   */
  private def addDefaultValueMetadata(
      builder: MetadataBuilder,
      defaultValue: DefaultValueExpression,
      colName: String,
      colType: DataType,
      statementType: String): Unit = {
    val existsDefaultSQL = constantFoldDefaultValue(defaultValue, colName, colType, statementType)
    builder.putExpression(
      DefaultCols.CURRENT_DEFAULT_COLUMN_METADATA_KEY, defaultValue.originalSQL,
      Some(defaultValue.child))
    builder.putString(DefaultCols.EXISTS_DEFAULT_COLUMN_METADATA_KEY, existsDefaultSQL)
  }
}
