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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{BasicColStats, Statistics}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
 * Analyzes the given columns of the given table in the current database to generate statistics,
 * which will be used in query optimizations.
 */
case class AnalyzeColumnCommand(
    tableName: String,
    columnNames: Seq[String]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionState = sparkSession.sessionState
    val tableIdent = sessionState.sqlParser.parseTableIdentifier(tableName)
    val relation = EliminateSubqueryAliases(sessionState.catalog.lookupRelation(tableIdent))

    // check correctness of column names
    val validColumns = mutable.HashSet[NamedExpression]()
    val resolver = sparkSession.sessionState.conf.resolver
    columnNames.foreach { col =>
      val exprOption = relation.resolve(col.split("\\."), resolver)
      if (exprOption.isEmpty) {
        throw new AnalysisException(s"Invalid column name: $col")
      }
      if (validColumns.map(_.exprId).contains(exprOption.get.exprId)) {
        throw new AnalysisException(s"Duplicate column name: $col")
      }
      validColumns += exprOption.get
    }

    relation match {
      case catalogRel: CatalogRelation =>
        updateStats(catalogRel.catalogTable,
          AnalyzeTableCommand.calculateTotalSize(sparkSession, catalogRel.catalogTable))

      case logicalRel: LogicalRelation if logicalRel.catalogTable.isDefined =>
        updateStats(logicalRel.catalogTable.get, logicalRel.relation.sizeInBytes)

      case otherRelation =>
        throw new AnalysisException("ANALYZE TABLE is not supported for " +
          s"${otherRelation.nodeName}.")
    }

    def updateStats(catalogTable: CatalogTable, newTotalSize: Long): Unit = {
      // collect column statistics
      val aggColumns = mutable.ArrayBuffer[Column](count(Column("*")))
      validColumns.foreach(entry => aggColumns ++= statsAgg(entry.name, entry.dataType))
      val statsRow: InternalRow = Dataset.ofRows(sparkSession, relation).select(aggColumns: _*)
        .queryExecution.toRdd.collect().head

      // We also update table-level stats to prevent inconsistency in case of table modification
      // between the two ANALYZE commands for collecting table-level stats and column-level stats.
      val rowCount = statsRow.getLong(0)
      var newStats: Statistics = if (catalogTable.stats.isDefined) {
        catalogTable.stats.get.copy(sizeInBytes = newTotalSize, rowCount = Some(rowCount))
      } else {
        Statistics(sizeInBytes = newTotalSize, rowCount = Some(rowCount))
      }

      var pos = 1
      val colStats = mutable.HashMap[String, BasicColStats]()
      validColumns.foreach { attr =>
        attr.dataType match {
          case n: NumericType =>
            colStats += attr.name -> BasicColStats(
              dataType = attr.dataType,
              numNulls = rowCount - statsRow.getLong(pos + NumericStatsAgg.numNotNullsIndex),
              max = Option(statsRow.get(pos + NumericStatsAgg.maxIndex, attr.dataType)),
              min = Option(statsRow.get(pos + NumericStatsAgg.minIndex, attr.dataType)),
              ndv = Some(statsRow.getLong(pos + NumericStatsAgg.ndvIndex)))
            pos += NumericStatsAgg.statsSeq.length
          case TimestampType | DateType =>
            colStats += attr.name -> BasicColStats(
              dataType = attr.dataType,
              numNulls = rowCount - statsRow.getLong(pos + NumericStatsAgg.numNotNullsIndex),
              max = Option(statsRow.get(pos + NumericStatsAgg.maxIndex, attr.dataType)),
              min = Option(statsRow.get(pos + NumericStatsAgg.minIndex, attr.dataType)),
              ndv = Some(statsRow.getLong(pos + NumericStatsAgg.ndvIndex)))
            pos += NumericStatsAgg.statsSeq.length
          case StringType =>
            colStats += attr.name -> BasicColStats(
              dataType = attr.dataType,
              numNulls = rowCount - statsRow.getLong(pos + StringStatsAgg.numNotNullsIndex),
              maxColLen = Some(statsRow.getLong(pos + StringStatsAgg.maxLenIndex)),
              avgColLen =
                Some(statsRow.getLong(pos + StringStatsAgg.sumLenIndex) / (1.0 * rowCount)),
              ndv = Some(statsRow.getLong(pos + StringStatsAgg.ndvIndex)))
            pos += StringStatsAgg.statsSeq.length
          case BinaryType =>
            colStats += attr.name -> BasicColStats(
              dataType = attr.dataType,
              numNulls = rowCount - statsRow.getLong(pos + BinaryStatsAgg.numNotNullsIndex),
              maxColLen = Some(statsRow.getLong(pos + BinaryStatsAgg.maxLenIndex)),
              avgColLen =
                Some(statsRow.getLong(pos + BinaryStatsAgg.sumLenIndex) / (1.0 * rowCount)))
            pos += BinaryStatsAgg.statsSeq.length
          case BooleanType =>
            val numOfNotNulls = statsRow.getLong(pos + BooleanStatsAgg.numNotNullsIndex)
            val numOfTrues = Some(statsRow.getLong(pos + BooleanStatsAgg.numTruesIndex))
            colStats += attr.name -> BasicColStats(
              dataType = attr.dataType,
              numNulls = rowCount - numOfNotNulls,
              numTrues = numOfTrues,
              numFalses = numOfTrues.map(i => numOfNotNulls - i),
              ndv = Some(2))
            pos += BooleanStatsAgg.statsSeq.length
        }
      }
      newStats = newStats.copy(basicColStats = colStats.toMap)
      sessionState.catalog.alterTable(catalogTable.copy(stats = Some(newStats)))
      // Refresh the cached data source table in the catalog.
      sessionState.catalog.refreshTable(tableIdent)
    }

    Seq.empty[Row]
  }

  private def statsAgg(name: String, dataType: DataType): Seq[Column] = dataType match {
    // Currently we only support stats generation for atomic types
    case n: NumericType => NumericStatsAgg(name)
    case TimestampType | DateType => NumericStatsAgg(name)
    case StringType => StringStatsAgg(name)
    case BinaryType => BinaryStatsAgg(name)
    case BooleanType => BooleanStatsAgg(name)
    case otherType =>
      throw new AnalysisException(s"Analyzing column $name of $otherType is not supported.")
  }
}

object ColumnStats extends Enumeration {
  val MAX, MIN, NDV, NUM_NOT_NULLS, MAX_LENGTH, SUM_LENGTH, NUM_TRUES = Value
}

trait StatsAggFunc {
  // This sequence is used to track the order of stats results when collecting.
  val statsSeq: Seq[ColumnStats.Value]

  def apply(name: String): Seq[Column] = {
    val col = Column(name)
    statsSeq.map {
      case ColumnStats.MAX => max(col)
      case ColumnStats.MIN => min(col)
      // count(distinct col) will have a shuffle, so we use an approximate ndv for efficiency
      case ColumnStats.NDV => approxCountDistinct(col)
      case ColumnStats.NUM_NOT_NULLS => count(col)
      case ColumnStats.MAX_LENGTH => max(length(col))
      case ColumnStats.SUM_LENGTH => sum(length(col))
      case ColumnStats.NUM_TRUES => sum(col.cast(IntegerType))
    }
  }

  // This is used to locate the needed stat in the sequence.
  def offset: Map[ColumnStats.Value, Int] = statsSeq.zipWithIndex.toMap

  def numNotNullsIndex: Int = offset(ColumnStats.NUM_NOT_NULLS)
}

object NumericStatsAgg extends StatsAggFunc {
  override val statsSeq = Seq(ColumnStats.MAX, ColumnStats.MIN, ColumnStats.NDV,
    ColumnStats.NUM_NOT_NULLS)
  def maxIndex: Int = offset(ColumnStats.MAX)
  def minIndex: Int = offset(ColumnStats.MIN)
  def ndvIndex: Int = offset(ColumnStats.NDV)
}

object StringStatsAgg extends StatsAggFunc {
  override val statsSeq = Seq(ColumnStats.MAX_LENGTH, ColumnStats.SUM_LENGTH, ColumnStats.NDV,
    ColumnStats.NUM_NOT_NULLS)
  def maxLenIndex: Int = offset(ColumnStats.MAX_LENGTH)
  def sumLenIndex: Int = offset(ColumnStats.SUM_LENGTH)
  def ndvIndex: Int = offset(ColumnStats.NDV)
}

object BinaryStatsAgg extends StatsAggFunc {
  override val statsSeq = Seq(ColumnStats.MAX_LENGTH, ColumnStats.SUM_LENGTH,
    ColumnStats.NUM_NOT_NULLS)
  def maxLenIndex: Int = offset(ColumnStats.MAX_LENGTH)
  def sumLenIndex: Int = offset(ColumnStats.SUM_LENGTH)
}

object BooleanStatsAgg extends StatsAggFunc {
  override val statsSeq = Seq(ColumnStats.NUM_TRUES, ColumnStats.NUM_NOT_NULLS)
  def numTruesIndex: Int = offset(ColumnStats.NUM_TRUES)
}
