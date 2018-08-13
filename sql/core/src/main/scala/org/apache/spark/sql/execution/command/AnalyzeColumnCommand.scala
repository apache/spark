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
import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.ArrayData
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
    val sizeInBytes = CommandUtils.calculateTotalSize(sparkSession, tableMeta)

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
   * @return (row count, map from column name to CatalogColumnStats)
   */
  private def computeColumnStats(
      sparkSession: SparkSession,
      tableIdent: TableIdentifier,
      columnNames: Seq[String]): (Long, Map[String, CatalogColumnStat]) = {

    val conf = sparkSession.sessionState.conf
    val relation = sparkSession.table(tableIdent).logicalPlan
    // Resolve the column names and dedup using AttributeSet
    val attributesToAnalyze = columnNames.map { col =>
      val exprOption = relation.output.find(attr => conf.resolver(attr.name, col))
      exprOption.getOrElse(throw new AnalysisException(s"Column $col does not exist."))
    }

    // Make sure the column types are supported for stats gathering.
    attributesToAnalyze.foreach { attr =>
      if (!supportsType(attr.dataType)) {
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
      attributesToAnalyze.map(statExprs(_, conf, attributePercentiles))

    val namedExpressions = expressions.map(e => Alias(e, e.toString)())
    val statsRow = new QueryExecution(sparkSession, Aggregate(Nil, namedExpressions, relation))
      .executedPlan.executeTake(1).head

    val rowCount = statsRow.getLong(0)
    val columnStats = attributesToAnalyze.zipWithIndex.map { case (attr, i) =>
      // according to `statExprs`, the stats struct always have 7 fields.
      (attr.name, rowToColumnStat(statsRow.getStruct(i + 1, 7), attr, rowCount,
        attributePercentiles.get(attr)).toCatalogColumnStat(attr.name, attr.dataType))
    }.toMap
    (rowCount, columnStats)
  }

  /** Computes percentiles for each attribute. */
  private def computePercentiles(
      attributesToAnalyze: Seq[Attribute],
      sparkSession: SparkSession,
      relation: LogicalPlan): AttributeMap[ArrayData] = {
    val attrsToGenHistogram = if (conf.histogramEnabled) {
      attributesToAnalyze.filter(a => supportsHistogram(a.dataType))
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
        val percentiles = percentilesRow.getArray(i)
        // When there is no non-null value, `percentiles` is null. In such case, there is no
        // need to generate histogram.
        if (percentiles != null) {
          attributePercentiles += attr -> percentiles
        }
      }
    }
    AttributeMap(attributePercentiles.toSeq)
  }

  /** Returns true iff the we support gathering column statistics on column of the given type. */
  private def supportsType(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case BooleanType => true
    case DateType => true
    case TimestampType => true
    case BinaryType | StringType => true
    case _ => false
  }

  /** Returns true iff the we support gathering histogram on column of the given type. */
  private def supportsHistogram(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case DateType => true
    case TimestampType => true
    case _ => false
  }

  /**
   * Constructs an expression to compute column statistics for a given column.
   *
   * The expression should create a single struct column with the following schema:
   * distinctCount: Long, min: T, max: T, nullCount: Long, avgLen: Long, maxLen: Long,
   * distinctCountsForIntervals: Array[Long]
   *
   * Together with [[rowToColumnStat]], this function is used to create [[ColumnStat]] and
   * as a result should stay in sync with it.
   */
  private def statExprs(
    col: Attribute,
    conf: SQLConf,
    colPercentiles: AttributeMap[ArrayData]): CreateNamedStruct = {
    def struct(exprs: Expression*): CreateNamedStruct = CreateStruct(exprs.map { expr =>
      expr.transformUp { case af: AggregateFunction => af.toAggregateExpression() }
    })
    val one = Literal(1, LongType)

    // the approximate ndv (num distinct value) should never be larger than the number of rows
    val numNonNulls = if (col.nullable) Count(col) else Count(one)
    val ndv = Least(Seq(HyperLogLogPlusPlus(col, conf.ndvMaxError), numNonNulls))
    val numNulls = Subtract(Count(one), numNonNulls)
    val defaultSize = Literal(col.dataType.defaultSize, LongType)
    val nullArray = Literal(null, ArrayType(LongType))

    def fixedLenTypeStruct: CreateNamedStruct = {
      val genHistogram =
        supportsHistogram(col.dataType) && colPercentiles.contains(col)
      val intervalNdvsExpr = if (genHistogram) {
        ApproxCountDistinctForIntervals(col,
          Literal(colPercentiles(col), ArrayType(col.dataType)), conf.ndvMaxError)
      } else {
        nullArray
      }
      // For fixed width types, avg size should be the same as max size.
      struct(ndv, Cast(Min(col), col.dataType), Cast(Max(col), col.dataType), numNulls,
        defaultSize, defaultSize, intervalNdvsExpr)
    }

    col.dataType match {
      case _: IntegralType => fixedLenTypeStruct
      case _: DecimalType => fixedLenTypeStruct
      case DoubleType | FloatType => fixedLenTypeStruct
      case BooleanType => fixedLenTypeStruct
      case DateType => fixedLenTypeStruct
      case TimestampType => fixedLenTypeStruct
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

  /** Convert a struct for column stats (defined in `statExprs`) into [[ColumnStat]]. */
  private def rowToColumnStat(
    row: InternalRow,
    attr: Attribute,
    rowCount: Long,
    percentiles: Option[ArrayData]): ColumnStat = {
    // The first 6 fields are basic column stats, the 7th is ndvs for histogram bins.
    val cs = ColumnStat(
      distinctCount = Option(BigInt(row.getLong(0))),
      // for string/binary min/max, get should return null
      min = Option(row.get(1, attr.dataType)),
      max = Option(row.get(2, attr.dataType)),
      nullCount = Option(BigInt(row.getLong(3))),
      avgLen = Option(row.getLong(4)),
      maxLen = Option(row.getLong(5))
    )
    if (row.isNullAt(6) || cs.nullCount.isEmpty) {
      cs
    } else {
      val ndvs = row.getArray(6).toLongArray()
      assert(percentiles.get.numElements() == ndvs.length + 1)
      val endpoints = percentiles.get.toArray[Any](attr.dataType).map(_.toString.toDouble)
      // Construct equi-height histogram
      val bins = ndvs.zipWithIndex.map { case (ndv, i) =>
        HistogramBin(endpoints(i), endpoints(i + 1), ndv)
      }
      val nonNullRows = rowCount - cs.nullCount.get
      val histogram = Histogram(nonNullRows.toDouble / ndvs.length, bins)
      cs.copy(histogram = Some(histogram))
    }
  }

}
