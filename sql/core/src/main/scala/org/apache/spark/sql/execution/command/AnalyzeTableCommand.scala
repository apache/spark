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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTableType}


/**
 * Analyzes the given table to generate statistics, which will be used in query optimizations.
 */
case class AnalyzeTableCommand(
    tableIdent: TableIdentifier,
    noscan: Boolean = true) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionState = sparkSession.sessionState
    val db = tableIdent.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = TableIdentifier(tableIdent.table, Some(db))
    val tableMeta = sessionState.catalog.getTableMetadata(tableIdentWithDB)
    if (tableMeta.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException("ANALYZE TABLE is not supported on views.")
    }
    val newTotalSize = CommandUtils.calculateTotalSize(sessionState, tableMeta)

    val oldTotalSize = tableMeta.stats.map(_.sizeInBytes.toLong).getOrElse(-1L)
    val oldRowCount = tableMeta.stats.flatMap(_.rowCount.map(_.toLong)).getOrElse(-1L)
    var newStats: Option[CatalogStatistics] = None
    if (newTotalSize >= 0 && newTotalSize != oldTotalSize) {
      newStats = Some(CatalogStatistics(sizeInBytes = newTotalSize))
    }
    // We only set rowCount when noscan is false, because otherwise:
    // 1. when total size is not changed, we don't need to alter the table;
    // 2. when total size is changed, `oldRowCount` becomes invalid.
    // This is to make sure that we only record the right statistics.
    if (!noscan) {
      val newRowCount = sparkSession.table(tableIdentWithDB).count()
      if (newRowCount >= 0 && newRowCount != oldRowCount) {
        newStats = if (newStats.isDefined) {
          newStats.map(_.copy(rowCount = Some(BigInt(newRowCount))))
        } else {
          Some(CatalogStatistics(
            sizeInBytes = oldTotalSize, rowCount = Some(BigInt(newRowCount))))
        }
      }
    }
    // Update the metastore if the above statistics of the table are different from those
    // recorded in the metastore.
    if (newStats.isDefined) {
      sessionState.catalog.alterTableStats(tableIdentWithDB, newStats)
      // Refresh the cached data source table in the catalog.
      sessionState.catalog.refreshTable(tableIdentWithDB)
    }

    Seq.empty[Row]
  }
}
