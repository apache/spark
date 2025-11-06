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

package org.apache.spark.sql.execution.analysis

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.V2TableUtil
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.ExtractV2CatalogAndIdentifier

private[sql] object TableRefreshUtil extends SQLConfHelper with Logging {
  /**
   * Checks whether table references captured by the plan are stale.
   *
   * This method analyzes all versioned tables and checks if a refresh is needed based on:
   *  - Inconsistent versions: the same table appears multiple times with different versions
   *  - Staleness: the latest table load time is older than the configured threshold
   *  - Unknown table load times: the load time information is completely unavailable
   *
   * @param plan the logical plan to analyze
   * @return true if the plan should be refreshed, false otherwise
   */
  def shouldRefresh(plan: LogicalPlan): Boolean = {
    val accessLedgers = mutable.HashMap[(TableCatalog, Identifier), TableAccessLedger]()

    plan foreach {
      case r @ ExtractV2CatalogAndIdentifier(catalog, ident)
          if r.table.currentVersion != null && r.timeTravelSpec.isEmpty =>
        val accessLedger = accessLedgers.getOrElseUpdate((catalog, ident), new TableAccessLedger)
        accessLedger.add(r.table.currentVersion, r.loadTimeNanos)
      case _ =>
        // ignore other nodes
    }

    // if no versioned tables found, no refresh needed
    if (accessLedgers.isEmpty) {
      return false
    }

    val currentTimeNanos = System.nanoTime()
    val maxMetadataAgeNanos = TimeUnit.MILLISECONDS.toNanos(conf.tableMetadataMaxAge)

    accessLedgers.exists { case ((catalog, ident), accessLedger) =>
      val tableName = V2TableUtil.toQualifiedName(catalog, ident)

      if (accessLedger.containsInconsistentVersions) {
        logDebug(s"Plan references inconsistent versions of table $tableName, refresh needed")
        return true
      }

      accessLedger.latestTableLoadTimeNanos match {
        case Some(latestTableLoadTimeNanos) =>
          val metadataAgeNanos = currentTimeNanos - latestTableLoadTimeNanos
          if (metadataAgeNanos >= maxMetadataAgeNanos) {
            val metadataAgeMillis = TimeUnit.NANOSECONDS.toMillis(metadataAgeNanos)
            logDebug(s"Table $tableName metadata is stale (${metadataAgeMillis}ms), refresh needed")
            true
          } else {
            false
          }
        case None =>
          logDebug(s"All table $tableName references have unknown load time, refresh needed")
          true
      }
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
  def refresh(plan: LogicalPlan): LogicalPlan = {
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

  private class TableAccessLedger {
    private val accesses = mutable.ArrayBuffer[(String, Option[Long])]()

    def add(version: String, tableLoadTimeNanos: Option[Long]): Unit = {
      accesses += version -> tableLoadTimeNanos
    }

    def containsInconsistentVersions: Boolean = {
      accesses.map(_._1).distinct.size > 1
    }

    def latestTableLoadTimeNanos: Option[Long] = {
      accesses.flatMap(_._2).maxOption
    }
  }
}
