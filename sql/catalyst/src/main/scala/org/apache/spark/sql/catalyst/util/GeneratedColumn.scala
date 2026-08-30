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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.ColumnDefinition
import org.apache.spark.sql.connector.catalog.{Column, Identifier, Table, TableCapability,
  TableCatalog, TableCatalogCapability}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField, StructType}

/**
 * This object contains utility methods and values for Generated Columns
 */
object GeneratedColumn {

  /**
   * The metadata key for saving a generation expression in a generated column's metadata. This is
   * only used internally and connectors should access generation expressions from the V2 columns.
   */
  val GENERATION_EXPRESSION_METADATA_KEY = "GENERATION_EXPRESSION"

  /**
   * The metadata key marking a generated column in a write target's output as one whose value
   * Spark computed from the generation expression, so that it is not validated against the
   * expression that produced it. Only set while resolving a write.
   */
  val AUTO_FILLED_GENERATED_COLUMN_METADATA_KEY = "__auto_filled_generated_column"

  /**
   * Whether the given `field` is a generated column
   */
  def isGeneratedColumn(field: StructField): Boolean = {
    isGeneratedColumn(field.metadata)
  }

  /**
   * Whether the given metadata indicates a generated column
   */
  def isGeneratedColumn(metadata: Metadata): Boolean = {
    metadata.contains(GENERATION_EXPRESSION_METADATA_KEY)
  }

  /**
   * Returns the generation expression stored in the column metadata if it exists
   */
  def getGenerationExpression(field: StructField): Option[String] = {
    getGenerationExpression(field.metadata)
  }

  /**
   * Returns the generation expression stored in the metadata if it exists
   */
  def getGenerationExpression(metadata: Metadata): Option[String] = {
    if (isGeneratedColumn(metadata)) {
      Some(metadata.getString(GENERATION_EXPRESSION_METADATA_KEY))
    } else {
      None
    }
  }

  /**
   * Whether the `schema` has one or more generated columns
   */
  def hasGeneratedColumns(schema: StructType): Boolean = {
    schema.exists(isGeneratedColumn)
  }

  /**
   * Check if the table catalog supports generated columns.
   * This is called from DataSourceV2Strategy for CREATE/REPLACE TABLE commands.
   */
  def validateCatalogForGeneratedColumn(
      columns: Seq[ColumnDefinition],
      catalog: TableCatalog,
      ident: Identifier): Unit = {
    if (columns.exists(_.generationExpression.isDefined)) {
      if (!catalog.capabilities().contains(
        TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_GENERATED_COLUMNS)) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          catalog, ident, "generated columns")
      }
    }
  }

  /**
   * Whether the table wants Spark to auto-fill generated column values and enforce generated
   * column constraints during writes (the
   * [[TableCapability.GENERATE_COLUMN_VALUES_ON_WRITE]] capability). Without it, the connector is
   * responsible for handling generated column values.
   */
  def supportsGeneratedColumnsOnWrite(table: Table): Boolean = {
    table.capabilities().contains(TableCapability.GENERATE_COLUMN_VALUES_ON_WRITE)
  }

  /**
   * Whether the table supports generated columns on write (see
   * [[supportsGeneratedColumnsOnWrite]]) and the given columns include at least one generated
   * column.
   */
  def supportsGeneratedColumnsOnWrite(
      table: Table,
      columns: Array[Column]): Boolean = {
    supportsGeneratedColumnsOnWrite(table) &&
      columns.exists(_.columnGenerationExpression() != null)
  }

  /**
   * Returns `relation`'s output with every generated column's generation expression recorded in
   * the attribute's metadata, which is where [[TableOutputResolver]] looks when it auto-fills the
   * generated columns a write does not provide a value for.
   *
   * Generation expressions are internal metadata: they should neither surface in a DataFrame's
   * schema nor propagate into tables created from it. A table's V2 columns are the persisted form
   * and the source of truth, so the write path copies the expression into plan attribute metadata
   * only for as long as resolving the write takes.
   *
   * The output is returned unchanged if the table does not ask Spark to handle generated columns.
   */
  def attachGenerationExpressions(relation: DataSourceV2Relation): Seq[AttributeReference] = {
    if (!supportsGeneratedColumnsOnWrite(relation.table)) {
      return relation.output
    }
    val genExprs = relation.table.columns()
      .flatMap(col => Option(col.generationExpression()).map(col.name -> _))
      .toMap
    relation.output.map { attr =>
      genExprs.get(attr.name) match {
        case Some(genExpr) =>
          withMetadataEntry(attr, GENERATION_EXPRESSION_METADATA_KEY, genExpr)
        case None => attr
      }
    }
  }

  /**
   * When a write supplies its own value for a generated column, that value must agree with the
   * generation expression, so ResolveTableConstraints validates it with a CheckInvariant, the same
   * way it enforces a table's CHECK constraints. In contrast, values computed by Spark from the
   * generation expression pass that check by construction, so this mark is what tells the rule to
   * skip them.
   *
   * Returns a write target's `output` with the generated columns named in `autoFilled` marked as
   * computed by Spark, so that it can skip CheckInvariant validation.
   *
   * Columns are left unmarked by default, so a value that did not come from the generation
   * expression is always validated.
   */
  def markAutoFilledGeneratedColumns(
      output: Seq[AttributeReference],
      autoFilled: Set[String]): Seq[AttributeReference] = {
    output.map { attr =>
      if (autoFilled.contains(attr.name)) {
        withMetadataEntry(attr, AUTO_FILLED_GENERATED_COLUMN_METADATA_KEY, "true")
      } else {
        attr
      }
    }
  }

  /**
   * Whether `attr` is a generated column whose value Spark computed from the generation
   * expression (see [[markAutoFilledGeneratedColumns]]).
   */
  def isAutoFilledGeneratedColumn(attr: Attribute): Boolean = {
    attr.metadata.contains(AUTO_FILLED_GENERATED_COLUMN_METADATA_KEY)
  }

  private def withMetadataEntry(
      attr: AttributeReference,
      key: String,
      value: String): AttributeReference = {
    val metadata = new MetadataBuilder()
      .withMetadata(attr.metadata)
      .putString(key, value)
      .build()
    attr.withMetadata(metadata).asInstanceOf[AttributeReference]
  }
}
