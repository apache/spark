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

package org.apache.spark.sql

import java.{lang => jl}
import java.io.File
import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.catalyst.{FullQualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics, CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Histogram, HistogramBin, HistogramSerializer, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestUtils

/**
 * The base for statistics test cases that we want to include in both the hive module (for
 * verifying behavior when using the Hive external catalog) as well as in the sql/core module.
 */
abstract class StatisticsCollectionTestBase extends QueryTest with SQLTestUtils {
  import testImplicits._

  private val dec1 = new java.math.BigDecimal("-1.756893400357024346")
  // BigDecimal is of course unlimited, but Spark decimals are limited to DECIMAL(38, 18). This
  // is one of the widest values that will fit.
  private val dec2 = new java.math.BigDecimal("8.4376864556435654231524645784930122085e19")
  private val d1Str = "2016-05-08"
  private val d1Internal = days(2016, 5, 8)
  private val d1 = Date.valueOf(d1Str)
  private val d2Str = "2016-05-09"
  private val d2Internal = days(2016, 5, 9)
  private val d2 = Date.valueOf(d2Str)

  // In the timestamps below, the parameter to date is micros but Timestamp takes setNanos.
  // Microsecond precision is the smallest supported by Spark.
  private val t1Str = "2016-05-08 00:00:01.123456"
  private val t1Internal = date(2016, 5, 8, 0, 0, 1, 123456)
  private val t1 = new Timestamp(DateTimeUtils.microsToMillis(t1Internal))
  t1.setNanos(123456000)
  private val tsNTZ1 = LocalDateTime.parse(t1Str.replace(" ", "T"))
  private val t2Str = "2016-05-09 00:00:02.987654"
  private val t2Internal = date(2016, 5, 9, 0, 0, 2, 987654)
  private val t2 = new Timestamp(DateTimeUtils.microsToMillis(t2Internal))
  t2.setNanos(987654000)
  private val tsNTZ2 = LocalDateTime.parse(t2Str.replace(" ", "T"))

  private val double1 = 1.123456789
  private val double2 = 6.987654321

  /**
   * Define a very simple 3 row table used for testing column serialization.
   * Note: last column is seq[int] which doesn't support stats collection.
   */
  protected val data = Seq[
    (jl.Boolean, jl.Byte, jl.Short, jl.Integer, jl.Long,
      jl.Double, jl.Float, java.math.BigDecimal,
      String, Array[Byte], Date, Timestamp, LocalDateTime,
      Seq[Int])](
    // scalastyle:off nonascii
    (false, 1.toByte, 1.toShort, 1, 1L, double1, 1.12345f,
      dec1, "string escrito en español", "b1".getBytes, d1, t1, tsNTZ1, null),
    (true, 2.toByte, 30000.toShort, 40000000, 5536453629L, double2, 7.54321f,
      dec2, "日本語で書かれたstring", "a string full of bytes".getBytes, d2, t2, tsNTZ2, null),
    (null, null, null, null, null, null, null, null, null, null, null, null, null, null)
    // scalastyle:on nonascii
  )

  /** A mapping from column to the stats collected. */
  protected val stats = mutable.LinkedHashMap(
    "cbool" -> CatalogColumnStat(Some(2), Some("false"), Some("true"), Some(1), Some(1), Some(1)),
    "cbyte" -> CatalogColumnStat(Some(2), Some("1"), Some("2"), Some(1), Some(1), Some(1)),
    "cshort" -> CatalogColumnStat(Some(2), Some("1"), Some("30000"), Some(1), Some(2), Some(2)),
    "cint" -> CatalogColumnStat(Some(2), Some("1"), Some("40000000"), Some(1), Some(4), Some(4)),
    "clong" -> CatalogColumnStat(Some(2), Some("1"), Some("5536453629"), Some(1), Some(8), Some(8)),
    "cdouble" -> CatalogColumnStat(
      Some(2), Some(double1.toString), Some(double2.toString), Some(1), Some(8), Some(8)),
    "cfloat" -> CatalogColumnStat(
      Some(2), Some("1.12345"), Some("7.54321"), Some(1), Some(4), Some(4)),
    "cdecimal" -> CatalogColumnStat(
      Some(2), Some(dec1.toString), Some(dec2.toString), Some(1), Some(16), Some(16)),
    "cstring" -> CatalogColumnStat(Some(2), None, None, Some(1), Some(20), Some(25)),
    "cbinary" -> CatalogColumnStat(Some(2), None, None, Some(1), Some(12), Some(22)),
    "cdate" -> CatalogColumnStat(Some(2), Some(d1Str), Some(d2Str),
      Some(1), Some(4), Some(4)),
    "ctimestamp" -> CatalogColumnStat(Some(2), Some(t1Str),
      Some(t2Str), Some(1), Some(8), Some(8)),
    "ctimestamp_ntz" -> CatalogColumnStat(Some(2), Some(t1Str),
      Some(t2Str), Some(1), Some(8), Some(8))
  )

  /**
   * A mapping from column to the stats collected including histograms.
   * The number of bins in the histograms is 2.
   */
  protected val statsWithHgms = {
    val colStats = mutable.LinkedHashMap(stats.toSeq: _*)
    colStats.update("cbyte", stats("cbyte").copy(histogram =
      Some(Histogram(1, Array(HistogramBin(1, 1, 1), HistogramBin(1, 2, 1))))))
    colStats.update("cshort", stats("cshort").copy(histogram =
      Some(Histogram(1, Array(HistogramBin(1, 1, 1), HistogramBin(1, 30000, 1))))))
    colStats.update("cint", stats("cint").copy(histogram =
      Some(Histogram(1, Array(HistogramBin(1, 1, 1), HistogramBin(1, 40000000, 1))))))
    colStats.update("clong", stats("clong").copy(histogram =
      Some(Histogram(1, Array(HistogramBin(1, 1, 1), HistogramBin(1, 5536453629L, 1))))))
    colStats.update("cdouble", stats("cdouble").copy(histogram =
      Some(Histogram(1, Array(
        HistogramBin(double1, double1, 1), HistogramBin(double1, double2, 1))))))
    colStats.update("cfloat", stats("cfloat").copy(histogram =
      Some(Histogram(1, Array(
        HistogramBin(1.12345, 1.12345, 1), HistogramBin(1.12345, 7.54321, 1))))))
    colStats.update("cdecimal", stats("cdecimal").copy(histogram =
      Some(Histogram(1, Array(
        HistogramBin(dec1.doubleValue(), dec1.doubleValue(), 1),
        HistogramBin(dec1.doubleValue(), dec2.doubleValue(), 1))))))
    colStats.update("cdate", stats("cdate").copy(histogram =
      Some(Histogram(1, Array(HistogramBin(d1Internal, d1Internal, 1),
        HistogramBin(d1Internal, d2Internal, 1))))))
    colStats.update("ctimestamp", stats("ctimestamp").copy(histogram =
      Some(Histogram(1, Array(HistogramBin(t1Internal, t1Internal, 1),
        HistogramBin(t1Internal, t2Internal, 1))))))
    colStats.update("ctimestamp_ntz", stats("ctimestamp_ntz").copy(histogram =
      Some(Histogram(1, Array(HistogramBin(t1Internal, t1Internal, 1),
        HistogramBin(t1Internal, t2Internal, 1))))))
    colStats
  }

  private val strVersion = CatalogColumnStat.VERSION.toString
  val expectedSerializedColStats = Map(
    "spark.sql.statistics.colStats.cbinary.avgLen" -> "12",
    "spark.sql.statistics.colStats.cbinary.distinctCount" -> "2",
    "spark.sql.statistics.colStats.cbinary.maxLen" -> "22",
    "spark.sql.statistics.colStats.cbinary.nullCount" -> "1",
    "spark.sql.statistics.colStats.cbinary.version" -> strVersion,
    "spark.sql.statistics.colStats.cbool.avgLen" -> "1",
    "spark.sql.statistics.colStats.cbool.distinctCount" -> "2",
    "spark.sql.statistics.colStats.cbool.max" -> "true",
    "spark.sql.statistics.colStats.cbool.maxLen" -> "1",
    "spark.sql.statistics.colStats.cbool.min" -> "false",
    "spark.sql.statistics.colStats.cbool.nullCount" -> "1",
    "spark.sql.statistics.colStats.cbool.version" -> strVersion,
    "spark.sql.statistics.colStats.cbyte.avgLen" -> "1",
    "spark.sql.statistics.colStats.cbyte.distinctCount" -> "2",
    "spark.sql.statistics.colStats.cbyte.max" -> "2",
    "spark.sql.statistics.colStats.cbyte.maxLen" -> "1",
    "spark.sql.statistics.colStats.cbyte.min" -> "1",
    "spark.sql.statistics.colStats.cbyte.nullCount" -> "1",
    "spark.sql.statistics.colStats.cbyte.version" -> strVersion,
    "spark.sql.statistics.colStats.cdate.avgLen" -> "4",
    "spark.sql.statistics.colStats.cdate.distinctCount" -> "2",
    "spark.sql.statistics.colStats.cdate.max" -> "2016-05-09",
    "spark.sql.statistics.colStats.cdate.maxLen" -> "4",
    "spark.sql.statistics.colStats.cdate.min" -> "2016-05-08",
    "spark.sql.statistics.colStats.cdate.nullCount" -> "1",
    "spark.sql.statistics.colStats.cdate.version" -> strVersion,
    "spark.sql.statistics.colStats.cdecimal.avgLen" -> "16",
    "spark.sql.statistics.colStats.cdecimal.distinctCount" -> "2",
    "spark.sql.statistics.colStats.cdecimal.max" -> dec2.toString,
    "spark.sql.statistics.colStats.cdecimal.maxLen" -> "16",
    "spark.sql.statistics.colStats.cdecimal.min" -> dec1.toString,
    "spark.sql.statistics.colStats.cdecimal.nullCount" -> "1",
    "spark.sql.statistics.colStats.cdecimal.version" -> strVersion,
    "spark.sql.statistics.colStats.cdouble.avgLen" -> "8",
    "spark.sql.statistics.colStats.cdouble.distinctCount" -> "2",
    "spark.sql.statistics.colStats.cdouble.max" -> double2.toString,
    "spark.sql.statistics.colStats.cdouble.maxLen" -> "8",
    "spark.sql.statistics.colStats.cdouble.min" -> double1.toString,
    "spark.sql.statistics.colStats.cdouble.nullCount" -> "1",
    "spark.sql.statistics.colStats.cdouble.version" -> strVersion,
    "spark.sql.statistics.colStats.cfloat.avgLen" -> "4",
    "spark.sql.statistics.colStats.cfloat.distinctCount" -> "2",
    "spark.sql.statistics.colStats.cfloat.max" -> "7.54321",
    "spark.sql.statistics.colStats.cfloat.maxLen" -> "4",
    "spark.sql.statistics.colStats.cfloat.min" -> "1.12345",
    "spark.sql.statistics.colStats.cfloat.nullCount" -> "1",
    "spark.sql.statistics.colStats.cfloat.version" -> strVersion,
    "spark.sql.statistics.colStats.cint.avgLen" -> "4",
    "spark.sql.statistics.colStats.cint.distinctCount" -> "2",
    "spark.sql.statistics.colStats.cint.max" -> "40000000",
    "spark.sql.statistics.colStats.cint.maxLen" -> "4",
    "spark.sql.statistics.colStats.cint.min" -> "1",
    "spark.sql.statistics.colStats.cint.nullCount" -> "1",
    "spark.sql.statistics.colStats.cint.version" -> strVersion,
    "spark.sql.statistics.colStats.clong.avgLen" -> "8",
    "spark.sql.statistics.colStats.clong.distinctCount" -> "2",
    "spark.sql.statistics.colStats.clong.max" -> "5536453629",
    "spark.sql.statistics.colStats.clong.maxLen" -> "8",
    "spark.sql.statistics.colStats.clong.min" -> "1",
    "spark.sql.statistics.colStats.clong.nullCount" -> "1",
    "spark.sql.statistics.colStats.clong.version" -> strVersion,
    "spark.sql.statistics.colStats.cshort.avgLen" -> "2",
    "spark.sql.statistics.colStats.cshort.distinctCount" -> "2",
    "spark.sql.statistics.colStats.cshort.max" -> "30000",
    "spark.sql.statistics.colStats.cshort.maxLen" -> "2",
    "spark.sql.statistics.colStats.cshort.min" -> "1",
    "spark.sql.statistics.colStats.cshort.nullCount" -> "1",
    "spark.sql.statistics.colStats.cshort.version" -> strVersion,
    "spark.sql.statistics.colStats.cstring.avgLen" -> "20",
    "spark.sql.statistics.colStats.cstring.distinctCount" -> "2",
    "spark.sql.statistics.colStats.cstring.maxLen" -> "25",
    "spark.sql.statistics.colStats.cstring.nullCount" -> "1",
    "spark.sql.statistics.colStats.cstring.version" -> strVersion,
    "spark.sql.statistics.colStats.ctimestamp.avgLen" -> "8",
    "spark.sql.statistics.colStats.ctimestamp.distinctCount" -> "2",
    "spark.sql.statistics.colStats.ctimestamp.max" -> "2016-05-09 00:00:02.987654",
    "spark.sql.statistics.colStats.ctimestamp.maxLen" -> "8",
    "spark.sql.statistics.colStats.ctimestamp.min" -> "2016-05-08 00:00:01.123456",
    "spark.sql.statistics.colStats.ctimestamp.nullCount" -> "1",
    "spark.sql.statistics.colStats.ctimestamp.version" -> strVersion,
    "spark.sql.statistics.colStats.ctimestamp_ntz.avgLen" -> "8",
    "spark.sql.statistics.colStats.ctimestamp_ntz.distinctCount" -> "2",
    "spark.sql.statistics.colStats.ctimestamp_ntz.max" -> "2016-05-09 00:00:02.987654",
    "spark.sql.statistics.colStats.ctimestamp_ntz.maxLen" -> "8",
    "spark.sql.statistics.colStats.ctimestamp_ntz.min" -> "2016-05-08 00:00:01.123456",
    "spark.sql.statistics.colStats.ctimestamp_ntz.nullCount" -> "1",
    "spark.sql.statistics.colStats.ctimestamp_ntz.version" -> strVersion
  )

  val expectedSerializedHistograms = Map(
    "spark.sql.statistics.colStats.cbyte.histogram" ->
      HistogramSerializer.serialize(statsWithHgms("cbyte").histogram.get),
    "spark.sql.statistics.colStats.cshort.histogram" ->
      HistogramSerializer.serialize(statsWithHgms("cshort").histogram.get),
    "spark.sql.statistics.colStats.cint.histogram" ->
      HistogramSerializer.serialize(statsWithHgms("cint").histogram.get),
    "spark.sql.statistics.colStats.clong.histogram" ->
      HistogramSerializer.serialize(statsWithHgms("clong").histogram.get),
    "spark.sql.statistics.colStats.cdouble.histogram" ->
      HistogramSerializer.serialize(statsWithHgms("cdouble").histogram.get),
    "spark.sql.statistics.colStats.cfloat.histogram" ->
      HistogramSerializer.serialize(statsWithHgms("cfloat").histogram.get),
    "spark.sql.statistics.colStats.cdecimal.histogram" ->
      HistogramSerializer.serialize(statsWithHgms("cdecimal").histogram.get),
    "spark.sql.statistics.colStats.cdate.histogram" ->
      HistogramSerializer.serialize(statsWithHgms("cdate").histogram.get),
    "spark.sql.statistics.colStats.ctimestamp.histogram" ->
      HistogramSerializer.serialize(statsWithHgms("ctimestamp").histogram.get),
    "spark.sql.statistics.colStats.ctimestamp_ntz.histogram" ->
      HistogramSerializer.serialize(statsWithHgms("ctimestamp_ntz").histogram.get)
  )

  private val randomName = new Random(31)

  def getCatalogTable(tableName: String): CatalogTable = {
    spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
  }

  def getTableFromCatalogCache(tableName: String): LogicalPlan = {
    val catalog = spark.sessionState.catalog
    val qualifiedTableName = FullQualifiedTableName(
      CatalogManager.SESSION_CATALOG_NAME, catalog.getCurrentDatabase, tableName)
    catalog.getCachedTable(qualifiedTableName)
  }

  def isTableInCatalogCache(tableName: String): Boolean = {
    getTableFromCatalogCache(tableName) != null
  }

  def getTableStats(tableName: String): CatalogStatistics = {
    getCatalogTable(tableName).stats.get
  }

  def getPartitionStats(tableName: String, partSpec: TablePartitionSpec): CatalogStatistics = {
    spark.sessionState.catalog.getPartition(TableIdentifier(tableName), partSpec).stats.get
  }

  def checkTableStats(
      tableName: String,
      hasSizeInBytes: Boolean,
      expectedRowCounts: Option[Int]): Option[CatalogStatistics] = {
    val stats = getCatalogTable(tableName).stats
    if (hasSizeInBytes || expectedRowCounts.nonEmpty) {
      assert(stats.isDefined)
      assert(stats.get.sizeInBytes >= 0)
      assert(stats.get.rowCount === expectedRowCounts)
    } else {
      assert(stats.isEmpty)
    }

    stats
  }

  /**
   * Compute column stats for the given DataFrame and compare it with colStats.
   */
  def checkColStats(
      df: DataFrame,
      colStats: mutable.LinkedHashMap[String, CatalogColumnStat]): Unit = {
    val tableName = "column_stats_test_" + randomName.nextInt(1000)
    withTable(tableName) {
      df.write.saveAsTable(tableName)

      // Collect statistics
      sql(s"analyze table $tableName compute STATISTICS FOR COLUMNS " +
        colStats.keys.mkString(", "))

      // Validate statistics
      validateColStats(tableName, colStats)
    }
  }

  /**
   * Validate if the given catalog table has the provided statistics.
   */
  def validateColStats(
      tableName: String,
      colStats: mutable.LinkedHashMap[String, CatalogColumnStat]): Unit = {

    val table = getCatalogTable(tableName)
    assert(table.stats.isDefined)
    assert(table.stats.get.colStats.size == colStats.size)

    colStats.foreach { case (k, v) =>
      withClue(s"column $k") {
        assert(table.stats.get.colStats(k) == v)
      }
    }
  }

  def checkOptimizedPlanStats(
      df: DataFrame,
      sizeInBytes: BigInt,
      rowCount: Option[BigInt],
      colStat: Seq[ColumnStat]): Unit = {
    val optimizedPlan = df.queryExecution.optimizedPlan
    val attributeStats = optimizedPlan.references.zip(colStat)
    assert(optimizedPlan.stats ===
      Statistics(sizeInBytes, rowCount, AttributeMap(attributeStats.toSeq)))
  }

  // Filter out the checksum file refer to ChecksumFileSystem#isChecksumFile.
  def getDataSize(file: File): Long = {
    file.listFiles.filter { f =>
      val name = f.getName
      !(name.startsWith(".") && name.endsWith(".crc"))
    }.map(_.length).sum
  }

  // This test will be run twice: with and without Hive support
  test("SPARK-18856: non-empty partitioned table should not report zero size") {
    withTable("ds_tbl", "hive_tbl") {
      spark.range(100).select($"id", $"id" % 5 as "p").write.partitionBy("p").saveAsTable("ds_tbl")
      val stats = spark.table("ds_tbl").queryExecution.optimizedPlan.stats
      assert(stats.sizeInBytes > 0, "non-empty partitioned table should not report zero size.")

      if (spark.conf.get(StaticSQLConf.CATALOG_IMPLEMENTATION) == "hive") {
        sql("CREATE TABLE hive_tbl(i int) PARTITIONED BY (j int)")
        sql("INSERT INTO hive_tbl PARTITION(j=1) SELECT 1")
        val stats2 = spark.table("hive_tbl").queryExecution.optimizedPlan.stats
        assert(stats2.sizeInBytes > 0, "non-empty partitioned table should not report zero size.")
      }
    }
  }

  // This test will be run twice: with and without Hive support
  test("conversion from CatalogStatistics to Statistics") {
    withTable("ds_tbl", "hive_tbl") {
      // Test data source table
      checkStatsConversion(tableName = "ds_tbl", isDatasourceTable = true)
      // Test hive serde table
      if (spark.conf.get(StaticSQLConf.CATALOG_IMPLEMENTATION) == "hive") {
        checkStatsConversion(tableName = "hive_tbl", isDatasourceTable = false)
      }
    }
  }

  private def checkStatsConversion(tableName: String, isDatasourceTable: Boolean): Unit = {
    // Create an empty table and run analyze command on it.
    val createTableSql = if (isDatasourceTable) {
      s"CREATE TABLE $tableName (c1 INT, c2 STRING) USING PARQUET"
    } else {
      s"CREATE TABLE $tableName (c1 INT, c2 STRING)"
    }
    sql(createTableSql)
    // Analyze only one column.
    sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS c1")
    val (relation, catalogTable) = spark.table(tableName).queryExecution.analyzed.collect {
      case catalogRel: HiveTableRelation => (catalogRel, catalogRel.tableMeta)
      case logicalRel: LogicalRelation => (logicalRel, logicalRel.catalogTable.get)
    }.head
    val emptyColStat = ColumnStat(Some(0), None, None, Some(0), Some(4), Some(4))
    val emptyCatalogColStat = CatalogColumnStat(Some(0), None, None, Some(0), Some(4), Some(4))
    // Check catalog statistics
    assert(catalogTable.stats.isDefined)
    assert(catalogTable.stats.get.sizeInBytes == 0)
    assert(catalogTable.stats.get.rowCount == Some(0))
    assert(catalogTable.stats.get.colStats == Map("c1" -> emptyCatalogColStat))

    // Check relation statistics
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true") {
      assert(relation.stats.sizeInBytes == 1)
      assert(relation.stats.rowCount == Some(0))
      assert(relation.stats.attributeStats.size == 1)
      val (attribute, colStat) = relation.stats.attributeStats.head
      assert(attribute.name == "c1")
      assert(colStat == emptyColStat)
    }
    relation.invalidateStatsCache()
    withSQLConf(SQLConf.CBO_ENABLED.key -> "false") {
      assert(relation.stats.sizeInBytes == 0)
      assert(relation.stats.rowCount.isEmpty)
      assert(relation.stats.attributeStats.isEmpty)
    }
  }
}
