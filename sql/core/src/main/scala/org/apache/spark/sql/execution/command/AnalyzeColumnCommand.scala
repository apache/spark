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

import scala.collection.mutable

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.QueryExecution


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
    val sizeInBytes = CommandUtils.calculateTotalSize(sessionState, tableMeta)

    // Compute stats for each column
    val (rowCount, newColStats) = computeColumnStats(sparkSession, tableIdentWithDB, columnNames)

    // We also update table-level stats in order to keep them consistent with column-level stats.
    val statistics = CatalogStatistics(
      sizeInBytes = sizeInBytes,
      rowCount = Some(rowCount),
      // Newly computed column stats should override the existing ones.
      colStats = tableMeta.stats.map(_.colStats).getOrElse(Map.empty) ++ newColStats)

    sessionState.catalog.alterTableStats(tableIdentWithDB, Some(statistics))

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

    val conf = sparkSession.sessionState.conf
    val relation = sparkSession.table(tableIdent).logicalPlan
    // Resolve the column names and dedup using AttributeSet
    val attributesToAnalyze = columnNames.map { col =>
      val exprOption = relation.output.find(attr => conf.resolver(attr.name, col))
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
    // If no histogram is required, we run a job to compute basic column stats such as
    // min, max, ndv, etc. Otherwise, besides basic column stats, histogram will also be
    // generated. Currently we only support equi-height histogram.
    // To generate an equi-height histogram, we need two jobs:
    // 1. compute percentiles p(0), p(1/n) ... p((n-1)/n), p(1).
    // 2. use the percentiles as value intervals of bins, e.g. [p(0), p(1/n)],
    // [p(1/n), p(2/n)], ..., [p((n-1)/n), p(1)], and then count ndv in each bin.
    // Basic column stats will be computed together in the second job.
    val attributePercentiles = computePercentiles(attributesToAnalyze, sparkSession, relation)

    // The first element in the result will be the overall row count, the following elements
    // will be structs containing all column stats.
    // The layout of each struct follows the layout of the ColumnStats.
    val expressions = Count(Literal(1)).toAggregateExpression() +:
      attributesToAnalyze.map(ColumnStat.statExprs(_, conf, attributePercentiles))

    val namedExpressions = expressions.map(e => Alias(e, e.toString)())
    val statsRow = new QueryExecution(sparkSession, Aggregate(Nil, namedExpressions, relation))
      .executedPlan.executeTake(1).head

    val rowCount = statsRow.getLong(0)
    val columnStats = attributesToAnalyze.zipWithIndex.map { case (attr, i) =>
      // according to `ColumnStat.statExprs`, the stats struct always have 7 fields.
      (attr.name, ColumnStat.rowToColumnStat(statsRow.getStruct(i + 1, 7), attr, rowCount,
        attributePercentiles.get(attr)))
    }.toMap
    (rowCount, columnStats)
  }

  /** Computes percentiles for each attribute. */
  private def computePercentiles(
      attributesToAnalyze: Seq[Attribute],
      sparkSession: SparkSession,
      relation: LogicalPlan): AttributeMap[ArrayData] = {
    val attrsToGenHistogram = if (conf.histogramEnabled) {
      attributesToAnalyze.filter(a => ColumnStat.supportsHistogram(a.dataType))
    } else {
      Nil
    }
    val attributePercentiles = mutable.HashMap[Attribute, ArrayData]()
    if (attrsToGenHistogram.nonEmpty) {
      val percentiles = (0 to conf.histogramNumBins)
        .map(i => i.toDouble / conf.histogramNumBins).toArray

      val namedExprs = attrsToGenHistogram.map { attr =>
        val aggFunc =
          new ApproximatePercentile(attr, Literal(percentiles), Literal(conf.percentileAccuracy))
        val expr = aggFunc.toAggregateExpression()
        Alias(expr, expr.toString)()
      }

      val percentilesRow = new QueryExecution(sparkSession, Aggregate(Nil, namedExprs, relation))
        .executedPlan.executeTake(1).head
      attrsToGenHistogram.zipWithIndex.foreach { case (attr, i) =>
        attributePercentiles += attr -> percentilesRow.getArray(i)
      }
    }
    AttributeMap(attributePercentiles.toSeq)
  }

}
