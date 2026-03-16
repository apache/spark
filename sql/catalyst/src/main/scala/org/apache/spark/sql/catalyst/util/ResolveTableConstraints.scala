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

import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableCatalogCapability, TableChange}
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.errors.QueryCompilationErrors

object ResolveTableConstraints {
  // Validates that the catalog supports create/replace table with constraints.
  // Throws an exception if unsupported
  def validateCatalogForTableConstraint(
      constraints: Seq[Constraint],
      catalog: TableCatalog,
      ident: Identifier): Unit = {
    if (constraints.nonEmpty &&
      !catalog.capabilities().contains(TableCatalogCapability.SUPPORT_TABLE_CONSTRAINT)) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, "table constraint")
    }
  }

  // Validates that the catalog supports ALTER TABLE ADD/DROP CONSTRAINT operations.
  // Throws an exception if unsupported.
  def validateCatalogForTableChange(
      tableChanges: Seq[TableChange],
      catalog: TableCatalog,
      ident: Identifier): Unit = {
    // Check if the table changes contain table constraints.
    val hasTableConstraint = tableChanges.exists {
      case _: TableChange.AddConstraint => true
      case _: TableChange.DropConstraint => true
      case _ => false
    }
    if (hasTableConstraint &&
      !catalog.capabilities().contains(TableCatalogCapability.SUPPORT_TABLE_CONSTRAINT)) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, "table constraint")
    }
  }
}
