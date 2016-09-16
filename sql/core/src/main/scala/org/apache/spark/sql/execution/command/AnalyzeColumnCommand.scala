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
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, CatalogTable}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, BasicColStats, Statistics}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._


/**
 * Analyzes the given columns of the given table in the current database to generate statistics,
 * which will be used in query optimizations.
 */
case class AnalyzeColumnCommand(
    tableIdent: TableIdentifier,
    columnNames: Seq[String]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionState = sparkSession.sessionState
    val relation = EliminateSubqueryAliases(sessionState.catalog.lookupRelation(tableIdent))

    // check correctness of column names
    val validColumns = mutable.MutableList[NamedExpression]()
    val resolver = sessionState.conf.resolver
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
      // Collect statistics per column.
      // The first element in the result will be the overall row count, the following elements
      // will be structs containing all column stats.
      // The layout of each struct follows the layout of the BasicColStats.
      val ndvMaxErr = sessionState.conf.ndvMaxError
      val expressions = Count(Literal(1)).toAggregateExpression() +:
        validColumns.map(ColumnStatsStruct(_, ndvMaxErr))
      val namedExpressions = expressions.map(e => Alias(e, e.toString)())
      val statsRow = Dataset.ofRows(sparkSession, Aggregate(Nil, namedExpressions, relation))
        .queryExecution.toRdd.collect().head

      // unwrap the result
      val rowCount = statsRow.getLong(0)
      val colStats = validColumns.zipWithIndex.map { case (expr, i) =>
        val colInfo = statsRow.getStruct(i + 1, ColumnStatsStruct.statsNumber)
        val colStats = ColumnStatsStruct.unwrapRow(expr, colInfo)
        (expr.name, colStats)
      }.toMap

      val statistics =
        Statistics(sizeInBytes = newTotalSize, rowCount = Some(rowCount), basicColStats = colStats)
      sessionState.catalog.alterTable(catalogTable.copy(stats = Some(statistics)))
      // Refresh the cached data source table in the catalog.
      sessionState.catalog.refreshTable(tableIdent)
    }

    Seq.empty[Row]
  }
}

object ColumnStatsStruct {
  val zero = Literal(0, LongType)
  val one = Literal(1, LongType)
  val two = Literal(2, LongType)
  val nullLong = Literal(null, LongType)
  val nullDouble = Literal(null, DoubleType)
  val nullString = Literal(null, StringType)
  val nullBinary = Literal(null, BinaryType)
  val nullBoolean = Literal(null, BooleanType)
  val statsNumber = 8

  def apply(e: Expression, relativeSD: Double): CreateStruct = {
    var statistics = e.dataType match {
      case n: NumericType =>
        Seq(Max(e), Min(e), HyperLogLogPlusPlus(e, relativeSD), nullDouble, nullLong, nullLong,
          nullLong)
      case TimestampType | DateType =>
        Seq(Max(e), Min(e), HyperLogLogPlusPlus(e, relativeSD), nullDouble, nullLong, nullLong,
          nullLong)
      case StringType =>
        Seq(nullString, nullString, HyperLogLogPlusPlus(e, relativeSD), Average(Length(e)),
          Max(Length(e)), nullLong, nullLong)
      case BinaryType =>
        Seq(nullBinary, nullBinary, nullLong, Average(Length(e)), Max(Length(e)), nullLong,
          nullLong)
      case BooleanType =>
        Seq(nullBoolean, nullBoolean, two, nullDouble, nullLong, Sum(If(e, one, zero)),
          Sum(If(e, zero, one)))
      case otherType =>
        throw new AnalysisException("ANALYZE command is not supported for data type: " +
          s"${e.dataType}")
    }
    statistics = if (e.nullable) {
      Sum(If(IsNull(e), one, zero)) +: statistics
    } else {
      zero +: statistics
    }
    assert(statistics.length == statsNumber)
    CreateStruct(statistics.map {
      case af: AggregateFunction => af.toAggregateExpression()
      case e: Expression => e
    })
  }

  def unwrapRow(e: Expression, row: InternalRow): BasicColStats = {
    BasicColStats(
      dataType = e.dataType,
      numNulls = row.getLong(0),
      max = if (row.isNullAt(1)) None else Some(row.get(1, e.dataType)),
      min = if (row.isNullAt(2)) None else Some(row.get(2, e.dataType)),
      ndv = if (row.isNullAt(3)) None else Some(row.getLong(3)),
      avgColLen = if (row.isNullAt(4)) None else Some(row.getDouble(4)),
      maxColLen = if (row.isNullAt(5)) None else Some(row.getLong(5)),
      numTrues = if (row.isNullAt(6)) None else Some(row.getLong(6)),
      numFalses = if (row.isNullAt(7)) None else Some(row.getLong(7) - row.getLong(0)))
  }
}
