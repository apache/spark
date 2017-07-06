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
import java.sql.{Date, Timestamp}

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, CatalogStatistics, CatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.Decimal


/**
 * The base for statistics test cases that we want to include in both the hive module (for
 * verifying behavior when using the Hive external catalog) as well as in the sql/core module.
 */
abstract class StatisticsCollectionTestBase extends QueryTest with SQLTestUtils {
  import testImplicits._

  private val dec1 = new java.math.BigDecimal("1.000000000000000000")
  private val dec2 = new java.math.BigDecimal("8.000000000000000000")
  private val d1 = Date.valueOf("2016-05-08")
  private val d2 = Date.valueOf("2016-05-09")
  private val t1 = Timestamp.valueOf("2016-05-08 00:00:01")
  private val t2 = Timestamp.valueOf("2016-05-09 00:00:02")

  /**
   * Define a very simple 3 row table used for testing column serialization.
   * Note: last column is seq[int] which doesn't support stats collection.
   */
  protected val data = Seq[
    (jl.Boolean, jl.Byte, jl.Short, jl.Integer, jl.Long,
      jl.Double, jl.Float, java.math.BigDecimal,
      String, Array[Byte], Date, Timestamp,
      Seq[Int])](
    (false, 1.toByte, 1.toShort, 1, 1L, 1.0, 1.0f, dec1, "s1", "b1".getBytes, d1, t1, null),
    (true, 2.toByte, 3.toShort, 4, 5L, 6.0, 7.0f, dec2, "ss9", "bb0".getBytes, d2, t2, null),
    (null, null, null, null, null, null, null, null, null, null, null, null, null)
  )

  /** A mapping from column to the stats collected. */
  protected val stats = mutable.LinkedHashMap(
    "cbool" -> ColumnStat(2, Some(false), Some(true), 1, 1, 1),
    "cbyte" -> ColumnStat(2, Some(1.toByte), Some(2.toByte), 1, 1, 1),
    "cshort" -> ColumnStat(2, Some(1.toShort), Some(3.toShort), 1, 2, 2),
    "cint" -> ColumnStat(2, Some(1), Some(4), 1, 4, 4),
    "clong" -> ColumnStat(2, Some(1L), Some(5L), 1, 8, 8),
    "cdouble" -> ColumnStat(2, Some(1.0), Some(6.0), 1, 8, 8),
    "cfloat" -> ColumnStat(2, Some(1.0f), Some(7.0f), 1, 4, 4),
    "cdecimal" -> ColumnStat(2, Some(Decimal(dec1)), Some(Decimal(dec2)), 1, 16, 16),
    "cstring" -> ColumnStat(2, None, None, 1, 3, 3),
    "cbinary" -> ColumnStat(2, None, None, 1, 3, 3),
    "cdate" -> ColumnStat(2, Some(DateTimeUtils.fromJavaDate(d1)),
      Some(DateTimeUtils.fromJavaDate(d2)), 1, 4, 4),
    "ctimestamp" -> ColumnStat(2, Some(DateTimeUtils.fromJavaTimestamp(t1)),
      Some(DateTimeUtils.fromJavaTimestamp(t2)), 1, 8, 8)
  )

  private val randomName = new Random(31)

  def getCatalogTable(tableName: String): CatalogTable = {
    spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
  }

  def getCatalogStatistics(tableName: String): CatalogStatistics = {
    getCatalogTable(tableName).stats.get
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
      colStats: mutable.LinkedHashMap[String, ColumnStat]): Unit = {
    val tableName = "column_stats_test_" + randomName.nextInt(1000)
    withTable(tableName) {
      df.write.saveAsTable(tableName)

      // Collect statistics
      sql(s"analyze table $tableName compute STATISTICS FOR COLUMNS " +
        colStats.keys.mkString(", "))

      // Validate statistics
      val table = getCatalogTable(tableName)
      assert(table.stats.isDefined)
      assert(table.stats.get.colStats.size == colStats.size)

      colStats.foreach { case (k, v) =>
        withClue(s"column $k") {
          assert(table.stats.get.colStats(k) == v)
        }
      }
    }
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
      case catalogRel: CatalogRelation => (catalogRel, catalogRel.tableMeta)
      case logicalRel: LogicalRelation => (logicalRel, logicalRel.catalogTable.get)
    }.head
    val emptyColStat = ColumnStat(0, None, None, 0, 4, 4)
    // Check catalog statistics
    assert(catalogTable.stats.isDefined)
    assert(catalogTable.stats.get.sizeInBytes == 0)
    assert(catalogTable.stats.get.rowCount == Some(0))
    assert(catalogTable.stats.get.colStats == Map("c1" -> emptyColStat))

    // Check relation statistics
    assert(relation.stats.sizeInBytes == 0)
    assert(relation.stats.rowCount == Some(0))
    assert(relation.stats.attributeStats.size == 1)
    val (attribute, colStat) = relation.stats.attributeStats.head
    assert(attribute.name == "c1")
    assert(colStat == emptyColStat)
  }
}
