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
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


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
    val expressions = Count(Literal(1)).toAggregateExpression() +:
      attributesToAnalyze.map(statExprs(_, sparkSession.sessionState.conf))

    val namedExpressions = expressions.map(e => Alias(e, e.toString)())
    val statsRow = new QueryExecution(sparkSession, Aggregate(Nil, namedExpressions, relation))
      .executedPlan.executeTake(1).head

    val rowCount = statsRow.getLong(0)
    val colStats = rowToColumnStats(sparkSession, relation, attributesToAnalyze, statsRow, rowCount)
    (rowCount, colStats)
  }

  /**
   * Constructs an expression to compute column statistics for a given column.
   *
   * The expression should create a single struct column with the following schema:
   * distinctCount: Long, min: T, max: T, nullCount: Long, avgLen: Long, maxLen: Long,
   * percentiles: Array[T]
   *
   * Together with [[rowToColumnStats]], this function is used to create [[ColumnStat]] and
   * as a result should stay in sync with it.
   */
  private def statExprs(col: Attribute, conf: SQLConf): CreateNamedStruct = {
    def struct(exprs: Expression*): CreateNamedStruct = CreateStruct(exprs.map { expr =>
      expr.transformUp { case af: AggregateFunction => af.toAggregateExpression() }
    })
    val one = Literal(1, LongType)

    // the approximate ndv (num distinct value) should never be larger than the number of rows
    val numNonNulls = if (col.nullable) Count(col) else Count(one)
    val ndv = Least(Seq(HyperLogLogPlusPlus(col, conf.ndvMaxError), numNonNulls))
    val numNulls = Subtract(Count(one), numNonNulls)
    val defaultSize = Literal(col.dataType.defaultSize, LongType)
    val nullArray = Literal(null, ArrayType(DoubleType))

    def fixedLenTypeExprs(castType: DataType) = {
      // For fixed width types, avg size should be the same as max size.
      Seq(ndv, Cast(Min(col), castType), Cast(Max(col), castType), numNulls, defaultSize,
        defaultSize)
    }

    def fixedLenTypeStruct(castType: DataType, genHistogram: Boolean) = {
      val percentileExpr = if (genHistogram) {
        // To generate equi-height histogram, we need to:
        // 1. get percentiles p(1/n), p(2/n) ... p((n-1)/n),
        // 2. use min, max, and percentiles as range values of buckets, e.g. [min, p(1/n)],
        // [p(1/n), p(2/n)] ... [p((n-1)/n), max], and then count ndv in each bucket.
        // Step 2 will be performed in `rowToColumnStats`.
        val percentiles = (1 until conf.histogramBucketsNum)
          .map(i => i.toDouble / conf.histogramBucketsNum)
          .toArray
        new ApproximatePercentile(col, Literal(percentiles), Literal(conf.percentileAccuracy))
      } else {
        nullArray
      }
      struct(fixedLenTypeExprs(castType) :+ percentileExpr: _*)
    }

    col.dataType match {
      case dt: IntegralType => fixedLenTypeStruct(dt, conf.histogramEnabled)
      case _: DecimalType => fixedLenTypeStruct(col.dataType, conf.histogramEnabled)
      case dt @ (DoubleType | FloatType) => fixedLenTypeStruct(dt, conf.histogramEnabled)
      case BooleanType => fixedLenTypeStruct(col.dataType, false)
      case DateType => fixedLenTypeStruct(col.dataType, conf.histogramEnabled)
      case TimestampType => fixedLenTypeStruct(col.dataType, conf.histogramEnabled)
      case BinaryType | StringType =>
        // For string and binary type, we don't compute min, max or histogram
        val nullLit = Literal(null, col.dataType)
        struct(
          ndv, nullLit, nullLit, numNulls,
          // Set avg/max size to default size if all the values are null or there is no value.
          Coalesce(Seq(Ceil(Average(Length(col))), defaultSize)),
          Coalesce(Seq(Cast(Max(Length(col)), LongType), defaultSize)),
          nullArray)
      case _ =>
        throw new AnalysisException("Analyzing column statistics is not supported for column " +
            s"${col.name} of data type: ${col.dataType}.")
    }
  }

  /**
   * Convert the result in row into [[ColumnStat]]s.
   */
  private def rowToColumnStats(
      sparkSession: SparkSession,
      relation: LogicalPlan,
      attributes: Seq[Attribute],
      statsRow: InternalRow,
      rowCount: Long): Map[String, ColumnStat] = {
    val conf = sparkSession.sessionState.conf
    val columnStats = new mutable.HashMap[String, ColumnStat]
    val intervalNdvExprs = new mutable.ArrayBuffer[ApproxCountDistinctForIntervals]

    /**
     * For the given attribute, convert a struct for column stats (defined in `statExprs`) into
     * [[ColumnStat]].
     */
    def rowToColumnStat(row: InternalRow, attr: Attribute): ColumnStat = {
      // The first 6 fields are basic column stats, the 7th is percentiles, which will be used to
      // compute equi-height histogram.
      val cs = ColumnStat(
        distinctCount = BigInt(row.getLong(0)),
        // for string/binary min/max, get should return null
        min = Option(row.get(1, attr.dataType)),
        max = Option(row.get(2, attr.dataType)),
        nullCount = BigInt(row.getLong(3)),
        avgLen = row.getLong(4),
        maxLen = row.getLong(5)
      )
      if (!row.isNullAt(6)) {
        // Construct bucket endpoints by combining min/max and percentiles.
        val endpoints = cs.min.get +: row.getArray(6).toArray(attr.dataType) :+ cs.max.get
        intervalNdvExprs += ApproxCountDistinctForIntervals(attr,
          CreateArray(endpoints.map(Literal(_))), conf.ndvMaxError)
      }
      cs
    }

    // Get basic column stats in the first scan.
    attributes.zipWithIndex.foreach { case (attr, i) =>
      // according to `statExprs`, the stats struct always has 7 fields
      val basicColStat = rowToColumnStat(statsRow.getStruct(i + 1, 7), attr)
      columnStats.put(attr.name, basicColStat)
    }

    if (intervalNdvExprs.nonEmpty) {
      // To generate equi-height histogram, get bucket ndvs in the second scan.
      val namedExprs = intervalNdvExprs.map { aggFunc =>
        val expr = aggFunc.toAggregateExpression()
        Alias(expr, expr.toString)()
      }
      val ndvsRow = new QueryExecution(sparkSession, Aggregate(Nil, namedExprs, relation))
        .executedPlan.executeTake(1).head

      intervalNdvExprs.zipWithIndex.foreach { case (agg, i) =>
        val endpoints = agg.endpoints
        val ndvs = ndvsRow.getArray(i).toLongArray()
        assert(endpoints.length == ndvs.length + 1)

        // Construct equi-height histogram
        val buckets = ndvs.zipWithIndex.map { case (ndv, j) =>
          EquiHeightBucket(endpoints(j), endpoints(j + 1), ndv)
        }
        val colName = agg.child.asInstanceOf[Attribute].name
        val nonNullRows = rowCount - columnStats(colName).nullCount
        val ehHistogram =
          EquiHeightHistogram(nonNullRows.toDouble / conf.histogramBucketsNum, buckets)

        val colStatsWithHgm = columnStats(colName).copy(histogram = Some(ehHistogram))
        columnStats.update(colName, colStatsWithHgm)
      }
    }
    columnStats.toMap
  }

}
