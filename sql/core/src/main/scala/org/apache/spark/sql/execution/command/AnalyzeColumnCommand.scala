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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, ColumnStats, Statistics}
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
    val db = tableIdent.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = TableIdentifier(tableIdent.table, Some(db))
    val relation = EliminateSubqueryAliases(sessionState.catalog.lookupRelation(tableIdentWithDB))

    // check correctness of column names
    val attributesToAnalyze = mutable.MutableList[Attribute]()
    val caseSensitive = sessionState.conf.caseSensitiveAnalysis
    columnNames.foreach { col =>
      val exprOption = relation.output.find { attr =>
        if (caseSensitive) attr.name == col else attr.name.equalsIgnoreCase(col)
      }
      val expr = exprOption.getOrElse(throw new AnalysisException(s"Invalid column name: $col."))
      // do deduplication
      if (!attributesToAnalyze.contains(expr)) {
        attributesToAnalyze += expr
      }
    }

    relation match {
      case catalogRel: CatalogRelation =>
        updateStats(catalogRel.catalogTable,
          AnalyzeTableCommand.calculateTotalSize(sessionState, catalogRel.catalogTable))

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
      // The layout of each struct follows the layout of the ColumnStats.
      val ndvMaxErr = sessionState.conf.ndvMaxError
      val expressions = Count(Literal(1)).toAggregateExpression() +:
        attributesToAnalyze.map(ColumnStatsStruct(_, ndvMaxErr))
      val namedExpressions = expressions.map(e => Alias(e, e.toString)())
      val statsRow = Dataset.ofRows(sparkSession, Aggregate(Nil, namedExpressions, relation))
        .queryExecution.toRdd.collect().head

      // unwrap the result
      val rowCount = statsRow.getLong(0)
      val columnStats = attributesToAnalyze.zipWithIndex.map { case (expr, i) =>
        (expr.name, ColumnStatsStruct.unwrapStruct(statsRow, i + 1, expr))
      }.toMap

      val statistics = Statistics(
        sizeInBytes = newTotalSize,
        rowCount = Some(rowCount),
        colStats = columnStats ++ catalogTable.stats.map(_.colStats).getOrElse(Map()))
      sessionState.catalog.alterTable(catalogTable.copy(stats = Some(statistics)))
      // Refresh the cached data source table in the catalog.
      sessionState.catalog.refreshTable(tableIdentWithDB)
    }

    Seq.empty[Row]
  }
}

object ColumnStatsStruct {
  val zero = Literal(0, LongType)
  val one = Literal(1, LongType)
  val nullLong = Literal(null, LongType)
  val nullDouble = Literal(null, DoubleType)
  val nullString = Literal(null, StringType)
  val nullBinary = Literal(null, BinaryType)
  val nullBoolean = Literal(null, BooleanType)
  val statsNumber = 8

  def apply(e: NamedExpression, relativeSD: Double): CreateStruct = {
    // Use aggregate functions to compute statistics we need:
    // - number of nulls: Sum(If(IsNull(e), one, zero));
    // - maximum value: Max(e);
    // - minimum value: Min(e);
    // - ndv (number of distinct values): HyperLogLogPlusPlus(e, relativeSD);
    // - average length of values: Average(Length(e));
    // - maximum length of values: Max(Length(e));
    // - number of true values: Sum(If(e, one, zero));
    // - number of false values: Sum(If(Not(e), one, zero));
    // - If we don't need some statistic for the data type, use null literal.
    // Note that: the order of each sequence must be as follows:
    // numNulls, max, min, ndv, avgColLen, maxColLen, numTrues, numFalses
    var statistics = e.dataType match {
      case _: NumericType | TimestampType | DateType =>
        Seq(Max(e), Min(e), HyperLogLogPlusPlus(e, relativeSD), nullDouble, nullLong, nullLong,
          nullLong)
      case StringType =>
        Seq(nullString, nullString, HyperLogLogPlusPlus(e, relativeSD), Average(Length(e)),
          Max(Length(e)), nullLong, nullLong)
      case BinaryType =>
        Seq(nullBinary, nullBinary, nullLong, Average(Length(e)), Max(Length(e)), nullLong,
          nullLong)
      case BooleanType =>
        Seq(nullBoolean, nullBoolean, nullLong, nullDouble, nullLong, Sum(If(e, one, zero)),
          Sum(If(Not(e), one, zero)))
      case otherType =>
        throw new AnalysisException("Analyzing columns is not supported for column " +
          s"${e.name} of data type: ${e.dataType}.")
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

  def unwrapStruct(row: InternalRow, offset: Int, e: Expression): ColumnStats = {
    val struct = row.getStruct(offset, statsNumber)
    ColumnStats(
      dataType = e.dataType,
      numNulls = struct.getLong(0),
      max = getField(struct, 1, e.dataType),
      min = getField(struct, 2, e.dataType),
      ndv = getLongField(struct, 3),
      avgColLen = getDoubleField(struct, 4),
      maxColLen = getLongField(struct, 5),
      numTrues = getLongField(struct, 6),
      numFalses = getLongField(struct, 7))
  }

  private def getField(struct: InternalRow, index: Int, dataType: DataType): Option[Any] = {
    if (struct.isNullAt(index)) None else Some(struct.get(index, dataType))
  }

  private def getLongField(struct: InternalRow, index: Int): Option[Long] = {
    if (struct.isNullAt(index)) None else Some(struct.getLong(index))
  }

  private def getDoubleField(struct: InternalRow, index: Int): Option[Double] = {
    if (struct.isNullAt(index)) None else Some(struct.getDouble(index))
  }
}
