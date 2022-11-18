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

import java.net.URI

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTable, CatalogTablePartition, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, InMemoryFileIndex}
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.types._

/**
 * For the purpose of calculating total directory sizes, use this filter to
 * ignore some irrelevant files.
 * @param stagingDir hive staging dir
 */
class PathFilterIgnoreNonData(stagingDir: String) extends PathFilter with Serializable {
  override def accept(path: Path): Boolean = {
    val fileName = path.getName
    !fileName.startsWith(stagingDir) && DataSourceUtils.isDataFile(fileName)
  }
}

object CommandUtils extends Logging {

  /** Change statistics after changing data by commands. */
  def updateTableStats(sparkSession: SparkSession, table: CatalogTable): Unit = {
    val catalog = sparkSession.sessionState.catalog
    if (sparkSession.sessionState.conf.autoSizeUpdateEnabled) {
      val newTable = catalog.getTableMetadata(table.identifier)
      val (newSize, newPartitions) = CommandUtils.calculateTotalSize(sparkSession, newTable)
      val isNewStats = newTable.stats.map(newSize != _.sizeInBytes).getOrElse(true)
      if (isNewStats) {
        val newStats = CatalogStatistics(sizeInBytes = newSize)
        catalog.alterTableStats(table.identifier, Some(newStats))
      }
      if (newPartitions.nonEmpty) {
        catalog.alterPartitions(table.identifier, newPartitions)
      }
    } else if (table.stats.nonEmpty) {
      catalog.alterTableStats(table.identifier, None)
    } else {
      // In other cases, we still need to invalidate the table relation cache.
      catalog.refreshTable(table.identifier)
    }
  }

  def calculateTotalSize(
      spark: SparkSession,
      catalogTable: CatalogTable): (BigInt, Seq[CatalogTablePartition]) = {
    val sessionState = spark.sessionState
    val startTime = System.nanoTime()
    val (totalSize, newPartitions) = if (catalogTable.partitionColumnNames.isEmpty) {
      (calculateSingleLocationSize(sessionState, catalogTable.identifier,
        catalogTable.storage.locationUri), Seq())
    } else {
      // Calculate table size as a sum of the visible partitions. See SPARK-21079
      val partitions = sessionState.catalog.listPartitions(catalogTable.identifier)
      logInfo(s"Starting to calculate sizes for ${partitions.length} partitions.")
      val paths = partitions.map(_.storage.locationUri)
      val sizes = calculateMultipleLocationSizes(spark, catalogTable.identifier, paths)
      val newPartitions = partitions.zipWithIndex.flatMap { case (p, idx) =>
        val newStats = CommandUtils.compareAndGetNewStats(p.stats, sizes(idx), None)
        newStats.map(_ => p.copy(stats = newStats))
      }
      (sizes.sum, newPartitions)
    }
    logInfo(s"It took ${(System.nanoTime() - startTime) / (1000 * 1000)} ms to calculate" +
      s" the total size for table ${catalogTable.identifier}.")
    (totalSize, newPartitions)
  }

  def calculateSingleLocationSize(
      sessionState: SessionState,
      identifier: TableIdentifier,
      locationUri: Option[URI]): Long = {
    // This method is mainly based on
    // org.apache.hadoop.hive.ql.stats.StatsUtils.getFileSizeForTable(HiveConf, Table)
    // in Hive 0.13 (except that we do not use fs.getContentSummary).
    // TODO: Generalize statistics collection.
    // TODO: Why fs.getContentSummary returns wrong size on Jenkins?
    // Can we use fs.getContentSummary in future?
    // Seems fs.getContentSummary returns wrong table size on Jenkins. So we use
    // countFileSize to count the table size.
    val stagingDir = sessionState.conf.getConfString("hive.exec.stagingdir", ".hive-staging")

    def getPathSize(fs: FileSystem, fileStatus: FileStatus): Long = {
      val size = if (fileStatus.isDirectory) {
        fs.listStatus(fileStatus.getPath)
          .map { status =>
            if (isDataPath(status.getPath, stagingDir)) {
              getPathSize(fs, status)
            } else {
              0L
            }
          }.sum
      } else {
        fileStatus.getLen
      }

      size
    }

    val startTime = System.nanoTime()
    val size = locationUri.map { p =>
      val path = new Path(p)
      try {
        val fs = path.getFileSystem(sessionState.newHadoopConf())
        getPathSize(fs, fs.getFileStatus(path))
      } catch {
        case NonFatal(e) =>
          logWarning(
            s"Failed to get the size of table ${identifier.table} in the " +
              s"database ${identifier.database} because of ${e.toString}", e)
          0L
      }
    }.getOrElse(0L)
    val durationInMs = (System.nanoTime() - startTime) / (1000 * 1000)
    logDebug(s"It took $durationInMs ms to calculate the total file size under path $locationUri.")

    size
  }

  def calculateMultipleLocationSizes(
      sparkSession: SparkSession,
      tid: TableIdentifier,
      paths: Seq[Option[URI]]): Seq[Long] = {
    if (sparkSession.sessionState.conf.parallelFileListingInStatsComputation) {
      calculateMultipleLocationSizesInParallel(sparkSession, paths.map(_.map(new Path(_))))
    } else {
      paths.map(p => calculateSingleLocationSize(sparkSession.sessionState, tid, p))
    }
  }

  /**
   * Launch a Job to list all leaf files in `paths` and compute the total size
   * for each path.
   * @param sparkSession the [[SparkSession]]
   * @param paths the Seq of [[Option[Path]]]s
   * @return a Seq of same size as `paths` where i-th element is total size of `paths(i)` or 0
   *         if `paths(i)` is None
   */
  def calculateMultipleLocationSizesInParallel(
      sparkSession: SparkSession,
      paths: Seq[Option[Path]]): Seq[Long] = {
    val stagingDir = sparkSession.sessionState.conf
      .getConfString("hive.exec.stagingdir", ".hive-staging")
    val filter = new PathFilterIgnoreNonData(stagingDir)
    val sizes = InMemoryFileIndex.bulkListLeafFiles(paths.flatten,
      sparkSession.sessionState.newHadoopConf(), filter, sparkSession).map {
      case (_, files) => files.map(_.getLen).sum
    }
    // the size is 0 where paths(i) is not defined and sizes(i) where it is defined
    paths.zipWithIndex.map { case (p, idx) => p.map(_ => sizes(idx)).getOrElse(0L) }
  }

  def compareAndGetNewStats(
      oldStats: Option[CatalogStatistics],
      newTotalSize: BigInt,
      newRowCount: Option[BigInt]): Option[CatalogStatistics] = {
    val oldTotalSize = oldStats.map(_.sizeInBytes).getOrElse(BigInt(-1))
    val oldRowCount = oldStats.flatMap(_.rowCount).getOrElse(BigInt(-1))
    var newStats: Option[CatalogStatistics] = None
    if (newTotalSize >= 0 && newTotalSize != oldTotalSize) {
      newStats = Some(CatalogStatistics(sizeInBytes = newTotalSize))
    }
    // We only set rowCount when noscan is false, because otherwise:
    // 1. when total size is not changed, we don't need to alter the table;
    // 2. when total size is changed, `oldRowCount` becomes invalid.
    // This is to make sure that we only record the right statistics.
    if (newRowCount.isDefined) {
      if (newRowCount.get >= 0 && newRowCount.get != oldRowCount) {
        newStats = if (newStats.isDefined) {
          newStats.map(_.copy(rowCount = newRowCount))
        } else {
          Some(CatalogStatistics(sizeInBytes = oldTotalSize, rowCount = newRowCount))
        }
      }
    }
    newStats
  }

  def analyzeTable(
      sparkSession: SparkSession,
      tableIdent: TableIdentifier,
      noScan: Boolean): Unit = {
    val sessionState = sparkSession.sessionState
    val db = tableIdent.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = TableIdentifier(tableIdent.table, Some(db))
    val tableMeta = sessionState.catalog.getTableMetadata(tableIdentWithDB)
    if (tableMeta.tableType == CatalogTableType.VIEW) {
      // Analyzes a catalog view if the view is cached
      val table = sparkSession.table(tableIdent.quotedString)
      val cacheManager = sparkSession.sharedState.cacheManager
      if (cacheManager.lookupCachedData(table.logicalPlan).isDefined) {
        if (!noScan) {
          // To collect table stats, materializes an underlying columnar RDD
          table.count()
        }
      } else {
        throw QueryCompilationErrors.analyzeTableNotSupportedOnViewsError()
      }
    } else {
      // Compute stats for the whole table
      val (newTotalSize, _) = CommandUtils.calculateTotalSize(sparkSession, tableMeta)
      val newRowCount =
        if (noScan) None else Some(BigInt(sparkSession.table(tableIdentWithDB).count()))

      // Update the metastore if the above statistics of the table are different from those
      // recorded in the metastore.
      val newStats = CommandUtils.compareAndGetNewStats(tableMeta.stats, newTotalSize, newRowCount)
      if (newStats.isDefined) {
        sessionState.catalog.alterTableStats(tableIdentWithDB, newStats)
      }
    }
  }

  /**
   * Compute stats for the given columns.
   * @return (row count, map from column name to CatalogColumnStats)
   */
  private[sql] def computeColumnStats(
      sparkSession: SparkSession,
      relation: LogicalPlan,
      columns: Seq[Attribute]): (Long, Map[Attribute, ColumnStat]) = {
    val conf = sparkSession.sessionState.conf

    // Collect statistics per column.
    // If no histogram is required, we run a job to compute basic column stats such as
    // min, max, ndv, etc. Otherwise, besides basic column stats, histogram will also be
    // generated. Currently we only support equi-height histogram.
    // To generate an equi-height histogram, we need two jobs:
    // 1. compute percentiles p(0), p(1/n) ... p((n-1)/n), p(1).
    // 2. use the percentiles as value intervals of bins, e.g. [p(0), p(1/n)],
    // [p(1/n), p(2/n)], ..., [p((n-1)/n), p(1)], and then count ndv in each bin.
    // Basic column stats will be computed together in the second job.
    val attributePercentiles = computePercentiles(columns, sparkSession, relation)

    // The first element in the result will be the overall row count, the following elements
    // will be structs containing all column stats.
    // The layout of each struct follows the layout of the ColumnStats.
    val expressions = Count(Literal(1)).toAggregateExpression() +:
      columns.map(statExprs(_, conf, attributePercentiles))

    val namedExpressions = expressions.map(e => Alias(e, e.toString)())
    val statsRow = new QueryExecution(sparkSession, Aggregate(Nil, namedExpressions, relation))
      .executedPlan.executeTake(1).head

    val rowCount = statsRow.getLong(0)
    val columnStats = columns.zipWithIndex.map { case (attr, i) =>
      // according to `statExprs`, the stats struct always have 7 fields.
      (attr, rowToColumnStat(statsRow.getStruct(i + 1, 7), attr, rowCount,
        attributePercentiles.get(attr)))
    }.toMap
    (rowCount, columnStats)
  }

  /** Computes percentiles for each attribute. */
  private def computePercentiles(
      attributesToAnalyze: Seq[Attribute],
      sparkSession: SparkSession,
      relation: LogicalPlan): AttributeMap[ArrayData] = {
    val conf = sparkSession.sessionState.conf
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
          new ApproximatePercentile(attr,
            Literal(new GenericArrayData(percentiles), ArrayType(DoubleType, false)),
            Literal(conf.percentileAccuracy))
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
    AttributeMap(attributePercentiles)
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
    val one = Literal(1.toLong, LongType)

    // the approximate ndv (num distinct value) should never be larger than the number of rows
    val numNonNulls = if (col.nullable) Count(col) else Count(one)
    val ndv = Least(Seq(HyperLogLogPlusPlus(col, conf.ndvMaxError), numNonNulls))
    val numNulls = Subtract(Count(one), numNonNulls)
    val defaultSize = Literal(col.dataType.defaultSize.toLong, LongType)
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
        throw QueryCompilationErrors.analyzingColumnStatisticsNotSupportedForColumnTypeError(
          col.name, col.dataType)
    }
  }

  /**
   * Convert a struct for column stats (defined in `statExprs`) into
   * [[org.apache.spark.sql.catalyst.plans.logical.ColumnStat]].
   */
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

  private def isDataPath(path: Path, stagingDir: String): Boolean = {
    !path.getName.startsWith(stagingDir) && DataSourceUtils.isDataPath(path)
  }

  def uncacheTableOrView(sparkSession: SparkSession, name: String): Unit = {
    try {
      sparkSession.catalog.uncacheTable(name)
    } catch {
      case NonFatal(e) => logWarning(s"Exception when attempting to uncache $name", e)
    }
  }
}
