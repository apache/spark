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
import org.apache.spark.sql.catalyst.analysis.AsOfVersion
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, V2TableUtil}
import org.apache.spark.sql.errors.QueryCompilationErrors

private[sql] object V2TableRefreshUtil extends SQLConfHelper with Logging {
  /**
   * Pins table versions for all versioned tables in the plan.
   *
   * This method captures the current version of each versioned table by adding time travel
   * specifications. Tables that already have time travel specifications or are not versioned
   * are left unchanged.
   *
   * @param plan the logical plan to pin versions for
   * @return plan with pinned table versions
   */
  def pinVersions(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case r @ ExtractV2CatalogAndIdentifier(catalog, ident)
          if r.table.currentVersion != null && r.timeTravelSpec.isEmpty =>
        val tableName = V2TableUtil.toQualifiedName(catalog, ident)
        val version = r.table.currentVersion
        logDebug(s"Pinning table version for $tableName to $version")
        r.copy(timeTravelSpec = Some(AsOfVersion(version)))
    }
  }

  /**
   * Refreshes table metadata for all versioned tables in the plan.
   *
   * This method reloads table metadata from the catalog and validates:
   *  - Table identity: Ensures table ID has not changed
   *  - Data columns: Verifies captured columns match the current schema
   *  - Metadata columns: Checks metadata column consistency
   *
   * @param plan the logical plan to refresh
   * @return plan with refreshed table metadata
   */
  def refreshVersions(plan: LogicalPlan): LogicalPlan = {
    val cache = mutable.HashMap.empty[(TableCatalog, Identifier), Table]
    plan transform {
      case r @ ExtractV2CatalogAndIdentifier(catalog, ident)
          if r.table.currentVersion != null && r.timeTravelSpec.isEmpty =>
        val currentTable = cache.getOrElseUpdate((catalog, ident), {
          val tableName = V2TableUtil.toQualifiedName(catalog, ident)
          logDebug(s"Refreshing table metadata for $tableName")
          catalog.loadTable(ident)
        })
        validateTableIdentity(currentTable, r)
        validateDataColumns(currentTable, r)
        validateMetadataColumns(currentTable, r)
        r.copy(table = currentTable)
    }
  }

  private def validateTableIdentity(currentTable: Table, relation: DataSourceV2Relation): Unit = {
    if (relation.table.id != null && relation.table.id != currentTable.id) {
      throw QueryCompilationErrors.tableIdChangedAfterAnalysis(
        relation.name,
        capturedTableId = relation.table.id,
        currentTableId = currentTable.id)
    }
  }

  private def validateDataColumns(currentTable: Table, relation: DataSourceV2Relation): Unit = {
    val errors = V2TableUtil.validateCapturedColumns(currentTable, relation)
    if (errors.nonEmpty) {
      throw QueryCompilationErrors.columnsChangedAfterAnalysis(relation.name, errors)
    }
  }

  private def validateMetadataColumns(currentTable: Table, relation: DataSourceV2Relation): Unit = {
    val errors = V2TableUtil.validateCapturedMetadataColumns(currentTable, relation)
    if (errors.nonEmpty) {
      throw QueryCompilationErrors.metadataColumnsChangedAfterAnalysis(relation.name, errors)
    }
  }
}
