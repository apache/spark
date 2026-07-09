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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.ColumnDefinition
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Column, Identifier, TableCatalog,
  TableCatalogCapability}
import org.apache.spark.sql.errors.QueryCompilationErrors
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
   * Whether the table catalog supports Spark auto-filling generated column values and enforcing
   * generated column constraints during writes (the
   * [[TableCatalogCapability.SUPPORT_GENERATED_COLUMN_ON_WRITE]] capability). Without it, the
   * connector is responsible for handling generated column values.
   */
  def supportsGeneratedColumnsOnWrite(catalog: Option[CatalogPlugin]): Boolean = {
    catalog.exists {
      case tc: TableCatalog =>
        tc.capabilities().contains(TableCatalogCapability.SUPPORT_GENERATED_COLUMN_ON_WRITE)
      case _ => false
    }
  }

  /**
   * Whether the catalog supports generated columns on write (see
   * [[supportsGeneratedColumnsOnWrite]]) and the given columns include at least one generated
   * column.
   */
  def supportsGeneratedColumnsOnWrite(
      catalog: Option[CatalogPlugin],
      columns: Array[Column]): Boolean = {
    supportsGeneratedColumnsOnWrite(catalog) &&
      columns.exists(_.columnGenerationExpression() != null)
  }

  /**
   * Returns an attribute with the generation expression metadata removed.
   * Used when the catalog does not support auto-filling generated columns on write.
   */
  def removeGenerationExpressionMetadata(attr: Attribute): Attribute = {
    if (isGeneratedColumn(attr.metadata)) {
      val cleaned = new MetadataBuilder()
        .withMetadata(attr.metadata)
        .remove(GENERATION_EXPRESSION_METADATA_KEY)
        .build()
      attr.withMetadata(cleaned)
    } else {
      attr
    }
  }
}
