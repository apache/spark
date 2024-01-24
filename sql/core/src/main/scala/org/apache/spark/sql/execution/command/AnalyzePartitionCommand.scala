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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.util.PartitioningUtils

/**
 * Analyzes a given set of partitions to generate per-partition statistics, which will be used in
 * query optimizations.
 *
 * When `partitionSpec` is empty, statistics for all partitions are collected and stored in
 * Metastore.
 *
 * When `partitionSpec` mentions only some of the partition columns, all partitions with
 * matching values for specified columns are processed.
 *
 * If `partitionSpec` mentions unknown partition column, an `AnalysisException` is raised.
 *
 * By default, total number of rows and total size in bytes are calculated. When `noscan`
 * is `true`, only total size in bytes is computed.
 */
case class AnalyzePartitionCommand(
    tableIdent: TableIdentifier,
    partitionSpec: Map[String, Option[String]],
    noscan: Boolean = true) extends LeafRunnableCommand {

  private def getPartitionSpec(table: CatalogTable): Option[TablePartitionSpec] = {
    val normalizedPartitionSpec =
      PartitioningUtils.normalizePartitionSpec(partitionSpec, table.partitionSchema,
        table.identifier.quotedString, conf.resolver)

    // Report an error if partition columns in partition specification do not form
    // a prefix of the list of partition columns defined in the table schema
    val isNotSpecified =
      table.partitionColumnNames.map(normalizedPartitionSpec.getOrElse(_, None).isEmpty)
    if (isNotSpecified.init.zip(isNotSpecified.tail).contains((true, false))) {
      val tableId = table.identifier
      val schemaColumns = table.partitionColumnNames.mkString(",")
      val specColumns = normalizedPartitionSpec.keys.mkString(",")
      throw QueryCompilationErrors.unexpectedPartitionColumnPrefixError(
        tableId.table, tableId.database.get, schemaColumns, specColumns)
    }

    val filteredSpec = normalizedPartitionSpec.filter(_._2.isDefined).transform((_, v) => v.get)
    if (filteredSpec.isEmpty) {
      None
    } else {
      Some(filteredSpec)
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionState = sparkSession.sessionState
    val db = tableIdent.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = TableIdentifier(tableIdent.table, Some(db))
    val tableMeta = sessionState.catalog.getTableMetadata(tableIdentWithDB)
    if (tableMeta.tableType == CatalogTableType.VIEW) {
      throw QueryCompilationErrors.analyzeTableNotSupportedOnViewsError()
    }

    val partitionValueSpec = getPartitionSpec(tableMeta)

    val partitions = sessionState.catalog.listPartitions(tableMeta.identifier, partitionValueSpec)

    if (partitions.isEmpty) {
      if (partitionValueSpec.isDefined) {
        throw QueryCompilationErrors.noSuchPartitionError(
          db, tableIdent.table, partitionValueSpec.get)
      } else {
        // the user requested to analyze all partitions for a table which has no partitions
        // return normally, since there is nothing to do
        return Seq.empty[Row]
      }
    }

    // Compute statistics for individual partitions
    val rowCounts: Map[TablePartitionSpec, BigInt] =
      if (noscan) {
        Map.empty
      } else {
        CommandUtils.calculateRowCountsPerPartition(sparkSession, tableMeta, partitionValueSpec)
      }

    // Update the metastore if newly computed statistics are different from those
    // recorded in the metastore.
    val (_, newPartitions) = CommandUtils.calculatePartitionStats(
      sparkSession, tableMeta, partitions, Some(rowCounts))
    if (newPartitions.nonEmpty) {
      sessionState.catalog.alterPartitions(tableMeta.identifier, newPartitions)
    }

    Seq.empty[Row]
  }


}
