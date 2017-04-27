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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._


/**
 * Analyzes the given columns of the given table to generate statistics, which will be used in
 * query optimizations.
 */
case class AnalyzeColumnCommand(
    tableIdent: TableIdentifier,
    columnNames: Seq[String]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionState = sparkSession.sessionState
    val db = tableIdent.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = TableIdentifier(tableIdent.table, Some(db))
    val tableMeta = sessionState.catalog.getTableMetadata(tableIdentWithDB)
    if (tableMeta.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException("ANALYZE TABLE is not supported on views.")
    }
    val sizeInBytes = AnalyzeTableCommand.calculateTotalSize(sessionState, tableMeta)

    // Compute stats for each column
    val (rowCount, newColStats) = computeColumnStats(sparkSession, tableIdentWithDB, columnNames)

    // We also update table-level stats in order to keep them consistent with column-level stats.
    val statistics = CatalogStatistics(
      sizeInBytes = sizeInBytes,
      rowCount = Some(rowCount),
      // Newly computed column stats should override the existing ones.
      colStats = tableMeta.stats.map(_.colStats).getOrElse(Map.empty) ++ newColStats)

    sessionState.catalog.alterTable(tableMeta.copy(stats = Some(statistics)))

    // Refresh the cached data source table in the catalog.
    sessionState.catalog.refreshTable(tableIdentWithDB)

    Seq.empty[Row]
  }

  /**
   * Compute stats for the given columns.
   * @return (row count, map from column name to ColumnStats)
   */
  private def computeColumnStats(
      sparkSession: SparkSession,
      tableIdent: TableIdentifier,
      columnNames: Seq[String]): (Long, Map[String, ColumnStat]) = {

    val relation = sparkSession.table(tableIdent).logicalPlan
    // Resolve the column names and dedup using AttributeSet
    val resolver = sparkSession.sessionState.conf.resolver
    val attributesToAnalyze = columnNames.map { col =>
      val exprOption = relation.output.find(attr => resolver(attr.name, col))
      exprOption.getOrElse(throw new AnalysisException(s"Column $col does not exist."))
    }

    // Make sure the column types are supported for stats gathering.
    attributesToAnalyze.foreach { attr =>
      if (!ColumnStat.supportsType(attr.dataType)) {
        throw new AnalysisException(
          s"Column ${attr.name} in table $tableIdent is of type ${attr.dataType}, " +
            "and Spark does not support statistics collection on this column type.")
      }
    }

    // Collect statistics per column.
    // The first element in the result will be the overall row count, the following elements
    // will be structs containing all column stats.
    // The layout of each struct follows the layout of the ColumnStats.
    val ndvMaxErr = sparkSession.sessionState.conf.ndvMaxError
    val expressions = Count(Literal(1)).toAggregateExpression() +:
      attributesToAnalyze.map(ColumnStat.statExprs(_, ndvMaxErr))

    val namedExpressions = expressions.map(e => Alias(e, e.toString)())
    val statsRow = Dataset.ofRows(sparkSession, Aggregate(Nil, namedExpressions, relation)).head()

    val rowCount = statsRow.getLong(0)
    val columnStats = attributesToAnalyze.zipWithIndex.map { case (attr, i) =>
      (attr.name, ColumnStat.rowToColumnStat(statsRow.getStruct(i + 1), attr))
    }.toMap
    (rowCount, columnStats)
  }
}
