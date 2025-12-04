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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, V2TableUtil}
import org.apache.spark.sql.connector.catalog.CatalogV2Util
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.util.SchemaValidationMode
import org.apache.spark.sql.util.SchemaValidationMode.ALLOW_NEW_FIELDS
import org.apache.spark.sql.util.SchemaValidationMode.PROHIBIT_CHANGES

private[sql] object V2TableRefreshUtil extends SQLConfHelper with Logging {
  /**
   * Refreshes table metadata for tables in the plan.
   *
   * This method reloads table metadata from the catalog and validates:
   *  - Table identity: Ensures table ID has not changed
   *  - Data columns: Verifies captured columns align with the current schema
   *  - Metadata columns: Checks metadata column consistency
   *
   * Tables with time travel specifications are skipped as they reference a specific point
   * in time and don't have to be refreshed.
   *
   * Schema validation mode depends on the underlying plan. Commands, for instance,
   * prohibit any schema changes while queries permit adding columns.
   *
   * @param spark the currently active Spark session
   * @param plan the logical plan to refresh
   * @param versionedOnly indicates whether to refresh only versioned tables
   * @return plan with refreshed table metadata
   */
  def refresh(
      spark: SparkSession,
      plan: LogicalPlan,
      versionedOnly: Boolean = false): LogicalPlan = {
    refresh(spark, plan, versionedOnly, determineSchemaValidationMode(plan))
  }

  /**
   * Refreshes table metadata for tables in the plan.
   *
   * This method reloads table metadata from the catalog and validates:
   *  - Table identity: Ensures table ID has not changed
   *  - Data columns: Verifies captured columns align with the current schema
   *  - Metadata columns: Checks metadata column consistency
   *
   * Tables with time travel specifications are skipped as they reference a specific point
   * in time and don't have to be refreshed.
   *
   * @param spark the currently active Spark session
   * @param plan the logical plan to refresh
   * @param versionedOnly indicates whether to refresh only versioned tables
   * @param schemaValidationMode schema validation mode to use
   * @return plan with refreshed table metadata
   */
  def refresh(
      spark: SparkSession,
      plan: LogicalPlan,
      versionedOnly: Boolean,
      schemaValidationMode: SchemaValidationMode): LogicalPlan = {
    val currentTables = mutable.HashMap.empty[(TableCatalog, Identifier), Table]
    plan transformWithSubqueries {
      case r @ ExtractV2CatalogAndIdentifier(catalog, ident)
          if (r.isVersioned || !versionedOnly) && r.timeTravelSpec.isEmpty =>
        val currentTable = currentTables.getOrElseUpdate((catalog, ident), {
          val tableName = V2TableUtil.toQualifiedName(catalog, ident)
          lookupCachedRelation(spark, catalog, ident, r.table) match {
            case Some(cached) =>
              logDebug(s"Refreshing table metadata for $tableName using shared relation cache")
              cached.table
            case None =>
              logDebug(s"Refreshing table metadata for $tableName using catalog")
              catalog.loadTable(ident)
          }
        })
        validateTableIdentity(currentTable, r)
        validateDataColumns(currentTable, r, schemaValidationMode)
        validateMetadataColumns(currentTable, r, schemaValidationMode)
        r.copy(table = currentTable)
    }
  }

  private def lookupCachedRelation(
      spark: SparkSession,
      catalog: TableCatalog,
      ident: Identifier,
      table: Table): Option[DataSourceV2Relation] = {
    CatalogV2Util.lookupCachedRelation(spark.sharedState.relationCache, catalog, ident, table, conf)
  }

  // it is not safe to allow any schema changes in commands (e.g. CTAS, RTAS, MERGE)
  private def determineSchemaValidationMode(plan: LogicalPlan): SchemaValidationMode = {
    if (containsCommand(plan)) PROHIBIT_CHANGES else ALLOW_NEW_FIELDS
  }

  private def containsCommand(plan: LogicalPlan): Boolean = {
    plan.find(_.isInstanceOf[Command]).isDefined
  }

  private def validateTableIdentity(currentTable: Table, relation: DataSourceV2Relation): Unit = {
    if (relation.table.id != null && relation.table.id != currentTable.id) {
      throw QueryCompilationErrors.tableIdChangedAfterAnalysis(
        relation.name,
        capturedTableId = relation.table.id,
        currentTableId = currentTable.id)
    }
  }

  private def validateDataColumns(
      currentTable: Table,
      relation: DataSourceV2Relation,
      mode: SchemaValidationMode): Unit = {
    val errors = V2TableUtil.validateCapturedColumns(currentTable, relation, mode)
    if (errors.nonEmpty) {
      throw QueryCompilationErrors.columnsChangedAfterAnalysis(relation.name, errors)
    }
  }

  private def validateMetadataColumns(
      currentTable: Table,
      relation: DataSourceV2Relation,
      mode: SchemaValidationMode): Unit = {
    val errors = V2TableUtil.validateCapturedMetadataColumns(currentTable, relation, mode)
    if (errors.nonEmpty) {
      throw QueryCompilationErrors.metadataColumnsChangedAfterAnalysis(relation.name, errors)
    }
  }
}
