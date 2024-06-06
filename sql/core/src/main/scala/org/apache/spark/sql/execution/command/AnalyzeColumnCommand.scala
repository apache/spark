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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{DatetimeType, _}


/**
 * Analyzes the given columns of the given table to generate statistics, which will be used in
 * query optimizations. Parameter `allColumns` may be specified to generate statistics of all the
 * columns of a given table.
 */
case class AnalyzeColumnCommand(
    tableIdent: TableIdentifier,
    columnNames: Option[Seq[String]],
    allColumns: Boolean) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    require(columnNames.isDefined ^ allColumns, "Parameter `columnNames` or `allColumns` are " +
      "mutually exclusive. Only one of them should be specified.")
    val sessionState = sparkSession.sessionState

    tableIdent.database match {
      case Some(db) if db == sparkSession.sharedState.globalTempViewManager.database =>
        val plan = sessionState.catalog.getGlobalTempView(tableIdent.identifier).getOrElse {
          throw QueryCompilationErrors.noSuchTableError(db, tableIdent.identifier)
        }
        analyzeColumnInTempView(plan, sparkSession)
      case Some(_) =>
        analyzeColumnInCatalog(sparkSession)
      case None =>
        sessionState.catalog.getTempView(tableIdent.identifier) match {
          case Some(tempView) => analyzeColumnInTempView(tempView, sparkSession)
          case _ => analyzeColumnInCatalog(sparkSession)
        }
    }

    Seq.empty[Row]
  }

  private def analyzeColumnInCachedData(plan: LogicalPlan, sparkSession: SparkSession): Boolean = {
    val cacheManager = sparkSession.sharedState.cacheManager
    val df = Dataset.ofRows(sparkSession, plan)
    cacheManager.lookupCachedData(df).map { cachedData =>
      val columnsToAnalyze = getColumnsToAnalyze(
        tableIdent, cachedData.cachedRepresentation, columnNames, allColumns)
      cacheManager.analyzeColumnCacheQuery(sparkSession, cachedData, columnsToAnalyze)
      cachedData
    }.isDefined
  }

  private def analyzeColumnInTempView(plan: LogicalPlan, sparkSession: SparkSession): Unit = {
    if (!analyzeColumnInCachedData(plan, sparkSession)) {
      throw QueryCompilationErrors.tempViewNotCachedForAnalyzingColumnsError(tableIdent)
    }
  }

  private def getColumnsToAnalyze(
      tableIdent: TableIdentifier,
      relation: LogicalPlan,
      columnNames: Option[Seq[String]],
      allColumns: Boolean = false): Seq[Attribute] = {
    val columnsToAnalyze = if (allColumns) {
      relation.output
    } else {
      columnNames.get.map { col =>
        val exprOption = relation.output.find(attr => conf.resolver(attr.name, col))
        exprOption.getOrElse(throw QueryCompilationErrors.columnNotFoundError(col))
      }
    }
    // Make sure the column types are supported for stats gathering.
    columnsToAnalyze.foreach { attr =>
      if (!supportsType(attr.dataType)) {
        throw QueryCompilationErrors.columnTypeNotSupportStatisticsCollectionError(
          attr.name, tableIdent, attr.dataType)
      }
    }
    columnsToAnalyze
  }

  private def analyzeColumnInCatalog(sparkSession: SparkSession): Unit = {
    val sessionState = sparkSession.sessionState
    val tableMeta = sessionState.catalog.getTableMetadata(tableIdent)
    if (tableMeta.tableType == CatalogTableType.VIEW) {
      // Analyzes a catalog view if the view is cached
      val plan = sparkSession.table(tableIdent.quotedString).logicalPlan
      if (!analyzeColumnInCachedData(plan, sparkSession)) {
        throw QueryCompilationErrors.analyzeTableNotSupportedOnViewsError()
      }
    } else {
      val (sizeInBytes, _) = CommandUtils.calculateTotalSize(sparkSession, tableMeta)
      val relation = sparkSession.table(tableIdent).logicalPlan
      val columnsToAnalyze = getColumnsToAnalyze(tableIdent, relation, columnNames, allColumns)

      // Compute stats for the computed list of columns.
      val (rowCount, newColStats) =
        CommandUtils.computeColumnStats(sparkSession, relation, columnsToAnalyze)

      val newColCatalogStats = newColStats.map {
        case (attr, columnStat) =>
          attr.name -> columnStat.toCatalogColumnStat(attr.name, attr.dataType)
      }

      // We also update table-level stats in order to keep them consistent with column-level stats.
      val statistics = CatalogStatistics(
        sizeInBytes = sizeInBytes,
        rowCount = Some(rowCount),
        // Newly computed column stats should override the existing ones.
        colStats = tableMeta.stats.map(_.colStats).getOrElse(Map.empty) ++ newColCatalogStats)

      sessionState.catalog.alterTableStats(tableIdent, Some(statistics))
    }
  }

  /** Returns true iff the we support gathering column statistics on column of the given type. */
  private def supportsType(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case BooleanType => true
    case _: DatetimeType => true
    case BinaryType | StringType => true
    case _ => false
  }
}
