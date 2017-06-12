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

import org.apache.spark.sql.{AnalysisException, Column, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTableType}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, Literal}


/**
 * Analyzes the given table or partition to generate statistics, which will be used in
 * query optimizations.
 */
case class AnalyzeTableCommand(
    tableIdent: TableIdentifier,
    noscan: Boolean = true,
    partitionSpec: Option[TablePartitionSpec] = None) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionState = sparkSession.sessionState
    val db = tableIdent.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = TableIdentifier(tableIdent.table, Some(db))
    val tableMeta = sessionState.catalog.getTableMetadata(tableIdentWithDB)
    if (tableMeta.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException("ANALYZE TABLE is not supported on views.")
    }

    val partitionMeta = partitionSpec.map(
      p => sessionState.catalog.getPartition(tableMeta.identifier, partitionSpec.get))

    val oldStats =
      if (partitionMeta.isDefined) {
        partitionMeta.get.stats
      } else {
        tableMeta.stats
      }

    def calculateTotalSize(): BigInt = {
      if (partitionMeta.isDefined) {
        CommandUtils.calculateTotalSize(sessionState, tableMeta, partitionMeta.get)
      } else {
        CommandUtils.calculateTotalSize(sessionState, tableMeta)
      }
    }

    def calculateRowCount(): Long = {
      if (partitionSpec.isDefined) {
        val filters = partitionSpec.get.map {
          case (columnName, value) => EqualTo(UnresolvedAttribute(columnName), Literal(value))
        }
        val filter = filters match {
          case head::tail =>
            if (tail.isEmpty) head
            else tail.foldLeft(head : Expression)((a, b) => And(a, b))
        }
        sparkSession.table(tableIdentWithDB).filter(Column(filter)).count()
      } else {
        sparkSession.table(tableIdentWithDB).count()
      }
    }

    def updateStats(newStats: CatalogStatistics): Unit = {
      if (partitionMeta.isDefined) {
        sessionState.catalog.alterPartitions(tableMeta.identifier,
          List(partitionMeta.get.copy(stats = Some(newStats))))
      } else {
        sessionState.catalog.alterTableStats(tableIdentWithDB, Some(newStats))
        // Refresh the cached data source table in the catalog.
        sessionState.catalog.refreshTable(tableIdentWithDB)
      }
    }

    val newTotalSize = calculateTotalSize()

    val oldTotalSize = oldStats.map(_.sizeInBytes.toLong).getOrElse(0L)
    val oldRowCount = oldStats.flatMap(_.rowCount.map(_.toLong)).getOrElse(-1L)

    var newStats: Option[CatalogStatistics] = None
    if (newTotalSize >= 0 && newTotalSize != oldTotalSize) {
      newStats = Some(CatalogStatistics(sizeInBytes = newTotalSize))
    }
    // We only set rowCount when noscan is false, because otherwise:
    // 1. when total size is not changed, we don't need to alter the table;
    // 2. when total size is changed, `oldRowCount` becomes invalid.
    // This is to make sure that we only record the right statistics.
    if (!noscan) {
      val newRowCount = calculateRowCount()
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
      updateStats(newStats.get)
    }
    Seq.empty[Row]
  }
}
