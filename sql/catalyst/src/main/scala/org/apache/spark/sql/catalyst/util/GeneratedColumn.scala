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

import org.apache.spark.sql.catalyst.plans.logical.ColumnDefinition
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableCatalogCapability}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{StructField, StructType}

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
    field.metadata.contains(GENERATION_EXPRESSION_METADATA_KEY)
  }

  /**
   * Returns the generation expression stored in the column metadata if it exists
   */
  def getGenerationExpression(field: StructField): Option[String] = {
    if (isGeneratedColumn(field)) {
      Some(field.metadata.getString(GENERATION_EXPRESSION_METADATA_KEY))
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
}
