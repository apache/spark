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
import org.apache.spark.sql.catalyst.catalog.CatalogTableType


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

    // Compute stats for the whole table
    val newTotalSize = CommandUtils.calculateTotalSize(sparkSession, tableMeta)
    val newRowCount =
      if (noscan) None else Some(BigInt(sparkSession.table(tableIdentWithDB).count()))

    // Update the metastore if the above statistics of the table are different from those
    // recorded in the metastore.
    val newStats = CommandUtils.compareAndGetNewStats(tableMeta.stats, newTotalSize, newRowCount)
    if (newStats.isDefined) {
      sessionState.catalog.alterTableStats(tableIdentWithDB, newStats)
    }

    Seq.empty[Row]
  }
}
