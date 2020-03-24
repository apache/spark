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

import java.net.URI

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, InMemoryFileIndex}
import org.apache.spark.sql.internal.SessionState


object CommandUtils extends Logging {

  /** Change statistics after changing data by commands. */
  def updateTableStats(sparkSession: SparkSession, table: CatalogTable): Unit = {
    val catalog = sparkSession.sessionState.catalog
    if (sparkSession.sessionState.conf.autoSizeUpdateEnabled) {
      val newTable = catalog.getTableMetadata(table.identifier)
      val newSize = CommandUtils.calculateTotalSize(sparkSession, newTable)
      val newStats = CatalogStatistics(sizeInBytes = newSize)
      catalog.alterTableStats(table.identifier, Some(newStats))
    } else if (table.stats.nonEmpty) {
      catalog.alterTableStats(table.identifier, None)
    }
  }

  def calculateTotalSize(spark: SparkSession, catalogTable: CatalogTable): BigInt = {
    val sessionState = spark.sessionState
    val startTime = System.nanoTime()
    val totalSize = if (catalogTable.partitionColumnNames.isEmpty) {
      calculateLocationSize(sessionState, catalogTable.identifier, catalogTable.storage.locationUri)
    } else {
      // Calculate table size as a sum of the visible partitions. See SPARK-21079
      val partitions = sessionState.catalog.listPartitions(catalogTable.identifier)
      logInfo(s"Starting to calculate sizes for ${partitions.length} partitions.")
      if (spark.sessionState.conf.parallelFileListingInStatsComputation) {
        val paths = partitions.map(x => new Path(x.storage.locationUri.get))
        val stagingDir = sessionState.conf.getConfString("hive.exec.stagingdir", ".hive-staging")
        val pathFilter = new PathFilter with Serializable {
          override def accept(path: Path): Boolean = isDataPath(path, stagingDir)
        }
        val fileStatusSeq = InMemoryFileIndex.bulkListLeafFiles(
          paths, sessionState.newHadoopConf(), pathFilter, spark)
        fileStatusSeq.flatMap(_._2.map(_.getLen)).sum
      } else {
        partitions.map { p =>
          calculateLocationSize(sessionState, catalogTable.identifier, p.storage.locationUri)
        }.sum
      }
    }
    logInfo(s"It took ${(System.nanoTime() - startTime) / (1000 * 1000)} ms to calculate" +
      s" the total size for table ${catalogTable.identifier}.")
    totalSize
  }

  def calculateLocationSize(
      sessionState: SessionState,
      identifier: TableIdentifier,
      locationUri: Option[URI]): Long = {
    // This method is mainly based on
    // org.apache.hadoop.hive.ql.stats.StatsUtils.getFileSizeForTable(HiveConf, Table)
    // in Hive 0.13 (except that we do not use fs.getContentSummary).
    // TODO: Generalize statistics collection.
    // TODO: Why fs.getContentSummary returns wrong size on Jenkins?
    // Can we use fs.getContentSummary in future?
    // Seems fs.getContentSummary returns wrong table size on Jenkins. So we use
    // countFileSize to count the table size.
    val stagingDir = sessionState.conf.getConfString("hive.exec.stagingdir", ".hive-staging")

    def getPathSize(fs: FileSystem, path: Path): Long = {
      val fileStatus = fs.getFileStatus(path)
      val size = if (fileStatus.isDirectory) {
        fs.listStatus(path)
          .map { status =>
            if (isDataPath(status.getPath, stagingDir)) {
              getPathSize(fs, status.getPath)
            } else {
              0L
            }
          }.sum
      } else {
        fileStatus.getLen
      }

      size
    }

    val startTime = System.nanoTime()
    val size = locationUri.map { p =>
      val path = new Path(p)
      try {
        val fs = path.getFileSystem(sessionState.newHadoopConf())
        getPathSize(fs, path)
      } catch {
        case NonFatal(e) =>
          logWarning(
            s"Failed to get the size of table ${identifier.table} in the " +
              s"database ${identifier.database} because of ${e.toString}", e)
          0L
      }
    }.getOrElse(0L)
    val durationInMs = (System.nanoTime() - startTime) / (1000 * 1000)
    logDebug(s"It took $durationInMs ms to calculate the total file size under path $locationUri.")

    size
  }

  def compareAndGetNewStats(
      oldStats: Option[CatalogStatistics],
      newTotalSize: BigInt,
      newRowCount: Option[BigInt]): Option[CatalogStatistics] = {
    val oldTotalSize = oldStats.map(_.sizeInBytes).getOrElse(BigInt(-1))
    val oldRowCount = oldStats.flatMap(_.rowCount).getOrElse(BigInt(-1))
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
    newStats
  }

  private def isDataPath(path: Path, stagingDir: String): Boolean = {
    !path.getName.startsWith(stagingDir) && DataSourceUtils.isDataPath(path)
  }

  def uncacheTableOrView(sparkSession: SparkSession, name: String): Unit = {
    try {
      sparkSession.catalog.uncacheTable(name)
    } catch {
      case NonFatal(e) => logWarning("Exception when attempting to uncache $name", e)
    }
  }
}
