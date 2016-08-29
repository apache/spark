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

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, CatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.datasources.LogicalRelation


/**
 * Analyzes the given table in the current database to generate statistics, which will be
 * used in query optimizations.
 */
case class AnalyzeTableCommand(tableName: String, noscan: Boolean = true) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionState = sparkSession.sessionState
    val tableIdent = sessionState.sqlParser.parseTableIdentifier(tableName)
    val relation = EliminateSubqueryAliases(sessionState.catalog.lookupRelation(tableIdent))

    def updateTableStats(
        catalogTable: CatalogTable,
        oldTotalSize: Long,
        oldRowCount: Long,
        newTotalSize: Long): Unit = {

      var needUpdate = false
      val totalSize = if (newTotalSize > 0 && newTotalSize != oldTotalSize) {
        needUpdate = true
        newTotalSize
      } else {
        oldTotalSize
      }
      var numRows: Option[BigInt] = None
      if (!noscan) {
        val newRowCount = sparkSession.table(tableName).count()
        if (newRowCount >= 0 && newRowCount != oldRowCount) {
          numRows = Some(BigInt(newRowCount))
          needUpdate = true
        }
      }
      // Update the metastore if the above statistics of the table are different from those
      // recorded in the metastore.
      if (needUpdate) {
        sessionState.catalog.alterTable(
          catalogTable.copy(
            catalogStats = Some(Statistics(
              sizeInBytes = totalSize, rowCount = numRows))),
          fromAnalyze = true)

        // Refresh the cache of the table in the catalog.
        sessionState.catalog.refreshTable(tableIdent)
      }
    }

    relation match {
      case relation: CatalogRelation =>
        val catalogTable: CatalogTable = relation.catalogTable
        // This method is mainly based on
        // org.apache.hadoop.hive.ql.stats.StatsUtils.getFileSizeForTable(HiveConf, Table)
        // in Hive 0.13 (except that we do not use fs.getContentSummary).
        // TODO: Generalize statistics collection.
        // TODO: Why fs.getContentSummary returns wrong size on Jenkins?
        // Can we use fs.getContentSummary in future?
        // Seems fs.getContentSummary returns wrong table size on Jenkins. So we use
        // countFileSize to count the table size.
        val stagingDir = sessionState.conf.getConfString("hive.exec.stagingdir", ".hive-staging")

        def calculateTableSize(fs: FileSystem, path: Path): Long = {
          val fileStatus = fs.getFileStatus(path)
          val size = if (fileStatus.isDirectory) {
            fs.listStatus(path)
              .map { status =>
                if (!status.getPath.getName.startsWith(stagingDir)) {
                  calculateTableSize(fs, status.getPath)
                } else {
                  0L
                }
              }.sum
          } else {
            fileStatus.getLen
          }

          size
        }

        val newTotalSize =
          catalogTable.storage.locationUri.map { p =>
            val path = new Path(p)
            try {
              val fs = path.getFileSystem(sparkSession.sessionState.newHadoopConf())
              calculateTableSize(fs, path)
            } catch {
              case NonFatal(e) =>
                logWarning(
                  s"Failed to get the size of table ${catalogTable.identifier.table} in the " +
                    s"database ${catalogTable.identifier.database} because of ${e.toString}", e)
                0L
            }
          }.getOrElse(0L)

        updateTableStats(
          catalogTable,
          oldTotalSize = catalogTable.catalogStats.map(_.sizeInBytes.toLong).getOrElse(0L),
          oldRowCount = catalogTable.catalogStats.flatMap(_.rowCount.map(_.toLong)).getOrElse(-1L),
          newTotalSize = newTotalSize)

      // data source tables have been converted into LogicalRelations
      case logicalRel: LogicalRelation if logicalRel.metastoreTableIdentifier.isDefined =>
        val tableIdentifier = logicalRel.metastoreTableIdentifier.get
        val catalogTable = sessionState.catalog.getTableMetadata(tableIdentifier)
        updateTableStats(
          catalogTable,
          oldTotalSize = logicalRel.statistics.sizeInBytes.toLong,
          oldRowCount = logicalRel.statistics.rowCount.map(_.toLong).getOrElse(-1L),
          newTotalSize = logicalRel.relation.sizeInBytes)

      case otherRelation =>
        throw new AnalysisException(s"ANALYZE TABLE is only supported for Hive tables, " +
          s"but '${tableIdent.unquotedString}' is a ${otherRelation.nodeName}.")
    }
    Seq.empty[Row]
  }
}
