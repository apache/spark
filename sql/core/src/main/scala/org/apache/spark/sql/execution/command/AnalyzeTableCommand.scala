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
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, Literal}


/**
 * Analyzes the given table or partition to generate statistics, which will be used in
 * query optimizations.
 *
 * If certain partition specs are specified, then statistics are gathered for only those partitions.
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

    if (!partitionSpec.isDefined) {
      // Compute stats for the whole table
      val newTotalSize = CommandUtils.calculateTotalSize(sessionState, tableMeta)
      val newRowCount =
        if (noscan) {
          None
        } else {
          Some(BigInt(sparkSession.table(tableIdentWithDB).count()))
        }

      def updateStats(newStats: CatalogStatistics): Unit = {
        sessionState.catalog.alterTableStats(tableIdentWithDB, Some(newStats))
        // Refresh the cached data source table in the catalog.
        sessionState.catalog.refreshTable(tableIdentWithDB)
      }

      calculateAndUpdateStats(tableMeta.stats, newTotalSize, newRowCount, updateStats)
    } else {
      val partitions = sessionState.catalog.listPartitions(tableMeta.identifier, partitionSpec)

      if (partitionSpec.isDefined && partitions.isEmpty) {
        throw new NoSuchPartitionException(db, tableIdent.table, partitionSpec.get)
      }

      // Compute stats for individual partitions
      val rowCounts: Map[TablePartitionSpec, BigInt] =
        if (noscan) {
          Map.empty
        } else {
          calculateRowCountsPerPartition(sparkSession, tableMeta)
        }

      partitions.foreach { p =>
        val newTotalSize = CommandUtils.calculateLocationSize(sessionState,
          tableMeta.identifier, p.storage.locationUri)
        val newRowCount = rowCounts.get(p.spec)

        def updateStats(newStats: CatalogStatistics): Unit = {
          sessionState.catalog.alterPartitions(tableMeta.identifier,
            List(p.copy(stats = Some(newStats))))
        }

        calculateAndUpdateStats(p.stats, newTotalSize, newRowCount, updateStats)
      }
    }

    Seq.empty[Row]
  }

  private def calculateRowCountsPerPartition(
      sparkSession: SparkSession,
      tableMeta: CatalogTable): Map[TablePartitionSpec, BigInt] = {
    val filters = partitionSpec.get.map {
      case (columnName, value) => EqualTo(UnresolvedAttribute(columnName), Literal(value))
    }
    val filter = filters.reduce(And)

    val partitionColumns = tableMeta.partitionColumnNames.map(Column(_))

    val df = sparkSession.table(tableMeta.identifier).filter(Column(filter))
      .groupBy(partitionColumns: _*).count()

    val numPartitionColumns = partitionColumns.size

    df.collect().map { r =>
      val partitionColumnValues = partitionColumns.indices.map(r.get(_).toString)
      val spec: TablePartitionSpec =
        tableMeta.partitionColumnNames.zip(partitionColumnValues).toMap
      val count = BigInt(r.getLong(numPartitionColumns))
      (spec, count)
    }.toMap
  }

  private def calculateAndUpdateStats(
      oldStats: Option[CatalogStatistics],
      newTotalSize: BigInt,
      newRowCount: Option[BigInt],
      updateStats: CatalogStatistics => Unit): Unit = {

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
    if (newRowCount.isDefined) {
      if (newRowCount.get >= 0 && newRowCount.get != oldRowCount) {
        newStats = if (newStats.isDefined) {
          newStats.map(_.copy(rowCount = newRowCount))
        } else {
          Some(CatalogStatistics(sizeInBytes = oldTotalSize, rowCount = newRowCount))
        }
      }
    }
    // Update the metastore if the above statistics of the table are different from those
    // recorded in the metastore.
    if (newStats.isDefined) {
      updateStats(newStats.get)
    }
  }
}
