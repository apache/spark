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

package org.apache.spark.sql.hive

import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.util.Locale

import scala.reflect.ClassTag
import scala.util.matching.Regex

import org.apache.hadoop.hive.common.StatsSetupConst

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.{AnalysisException, _}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.HistogramBin
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, StringUtils}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.execution.command.{AnalyzeColumnCommand, CommandUtils, DDLUtils}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.hive.HiveExternalCatalog._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class StatisticsSuite extends StatisticsCollectionTestBase with TestHiveSingleton {

  test("size estimation for relations is based on row size * number of rows") {
    val dsTbl = "rel_est_ds_table"
    val hiveTbl = "rel_est_hive_table"
    withTable(dsTbl, hiveTbl) {
      spark.range(1000L).write.format("parquet").saveAsTable(dsTbl)
      spark.range(1000L).write.format("hive").saveAsTable(hiveTbl)

      Seq(dsTbl, hiveTbl).foreach { tbl =>
        sql(s"ANALYZE TABLE $tbl COMPUTE STATISTICS")
        val catalogStats = getTableStats(tbl)
        withSQLConf(SQLConf.CBO_ENABLED.key -> "false") {
          val relationStats = spark.table(tbl).queryExecution.optimizedPlan.stats
          assert(relationStats.sizeInBytes == catalogStats.sizeInBytes)
          assert(relationStats.rowCount.isEmpty)
        }
        spark.sessionState.catalog.refreshTable(TableIdentifier(tbl))
        withSQLConf(SQLConf.CBO_ENABLED.key -> "true") {
          val relationStats = spark.table(tbl).queryExecution.optimizedPlan.stats
          // Due to compression in parquet files, in this test, file size is smaller than
          // in-memory size.
          assert(catalogStats.sizeInBytes < relationStats.sizeInBytes)
          assert(catalogStats.rowCount == relationStats.rowCount)
        }
      }
    }
  }

  test("Hive serde tables should fallback to HDFS for size estimation") {
    withSQLConf(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key -> "true") {
      withTable("csv_table") {
        withTempDir { tempDir =>
          // EXTERNAL OpenCSVSerde table pointing to LOCATION
          val file1 = new File(s"$tempDir/data1")
          Utils.tryWithResource(new PrintWriter(file1)) { writer =>
            writer.write("1,2")
          }

          val file2 = new File(s"$tempDir/data2")
          Utils.tryWithResource(new PrintWriter(file2)) { writer =>
            writer.write("1,2")
          }

          sql(
            s"""
               |CREATE EXTERNAL TABLE csv_table(page_id INT, impressions INT)
               |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
               |WITH SERDEPROPERTIES (
               |\"separatorChar\" = \",\",
               |\"quoteChar\"     = \"\\\"\",
               |\"escapeChar\"    = \"\\\\\")
               |LOCATION '${tempDir.toURI}'""".stripMargin)

          val relation = spark.table("csv_table").queryExecution.analyzed.children.head
            .asInstanceOf[HiveTableRelation]

          val properties = relation.tableMeta.ignoredProperties
          // Since HIVE-6727, Hive fixes table-level stats for external tables are incorrect.
          assert(properties("totalSize").toLong == 6)
          assert(properties.get("rawDataSize").isEmpty)

          val sizeInBytes = relation.stats.sizeInBytes
          assert(sizeInBytes === BigInt(file1.length() + file2.length()))
        }
      }
    }
  }

  test("Hive serde table with incorrect statistics") {
    withTempDir { tempDir =>
      withTable("t1") {
        spark.range(5).write.mode(SaveMode.Overwrite).parquet(tempDir.getCanonicalPath)
        val dataSize = getDataSize(tempDir)
        spark.sql(
          s"""
             |CREATE EXTERNAL TABLE t1(id BIGINT)
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
             |STORED AS
             |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
             |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
             |LOCATION '${tempDir.getCanonicalPath}'
             |TBLPROPERTIES (
             |'rawDataSize'='-1', 'numFiles'='0', 'totalSize'='0',
             |'COLUMN_STATS_ACCURATE'='false', 'numRows'='-1'
             |)""".stripMargin)

        spark.sql("REFRESH TABLE t1")
        // After SPARK-28573, sizeInBytes should be equal to dataSize.
        val relation1 = spark.table("t1").queryExecution.analyzed.children.head
        assert(relation1.stats.sizeInBytes === dataSize)

        spark.sql("REFRESH TABLE t1")
        // After SPARK-19678 and enable ENABLE_FALL_BACK_TO_HDFS_FOR_STATS,
        // sizeInBytes should be equal to dataSize.
        withSQLConf(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key -> "true") {
          val relation2 = spark.table("t1").queryExecution.analyzed.children.head
          assert(relation2.stats.sizeInBytes === dataSize)
        }
      }
    }
  }

  test("analyze Hive serde tables") {
    def queryTotalSize(tableName: String): BigInt =
      spark.table(tableName).queryExecution.analyzed.stats.sizeInBytes

    // Non-partitioned table
    val nonPartTable = "non_part_table"
    withTable(nonPartTable) {
      sql(s"CREATE TABLE $nonPartTable (key STRING, value STRING) USING hive")
      sql(s"INSERT INTO TABLE $nonPartTable SELECT * FROM src")
      sql(s"INSERT INTO TABLE $nonPartTable SELECT * FROM src")

      sql(s"ANALYZE TABLE $nonPartTable COMPUTE STATISTICS noscan")

      assert(queryTotalSize(nonPartTable) === BigInt(11624))
    }

    // Partitioned table
    val partTable = "part_table"
    withTable(partTable) {
      sql(s"CREATE TABLE $partTable (key STRING, value STRING) USING hive " +
        "PARTITIONED BY (ds STRING)")
      sql(s"INSERT INTO TABLE $partTable PARTITION (ds='2010-01-01') SELECT * FROM src")
      sql(s"INSERT INTO TABLE $partTable PARTITION (ds='2010-01-02') SELECT * FROM src")
      sql(s"INSERT INTO TABLE $partTable PARTITION (ds='2010-01-03') SELECT * FROM src")

      assert(queryTotalSize(partTable) === spark.sessionState.conf.defaultSizeInBytes)

      sql(s"ANALYZE TABLE $partTable COMPUTE STATISTICS noscan")

      assert(queryTotalSize(partTable) === BigInt(17436))
    }

    // Try to analyze a temp table
    withView("tempTable") {
      sql("""SELECT * FROM src""").createOrReplaceTempView("tempTable")
      intercept[AnalysisException] {
        sql("ANALYZE TABLE tempTable COMPUTE STATISTICS")
      }
    }
  }

  test("SPARK-24626 parallel file listing in Stats computation") {
    withSQLConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key -> "2",
      SQLConf.PARALLEL_FILE_LISTING_IN_STATS_COMPUTATION.key -> "True") {
      val checkSizeTable = "checkSizeTable"
      withTable(checkSizeTable) {
          sql(s"CREATE TABLE $checkSizeTable (key STRING, value STRING) USING hive " +
            "PARTITIONED BY (ds STRING)")
          sql(s"INSERT INTO TABLE $checkSizeTable PARTITION (ds='2010-01-01') SELECT * FROM src")
          sql(s"INSERT INTO TABLE $checkSizeTable PARTITION (ds='2010-01-02') SELECT * FROM src")
          sql(s"INSERT INTO TABLE $checkSizeTable PARTITION (ds='2010-01-03') SELECT * FROM src")
          val tableMeta = spark.sessionState.catalog
            .getTableMetadata(TableIdentifier(checkSizeTable))
          HiveCatalogMetrics.reset()
          assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == 0)
          val (size, _) = CommandUtils.calculateTotalSize(spark, tableMeta)
          assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == 1)
          assert(size === BigInt(17436))
      }
    }
  }

  test("analyze non hive compatible datasource tables") {
    val table = "parquet_tab"
    withTable(table) {
      sql(
        s"""
          |CREATE TABLE $table (a int, b int)
          |USING parquet
          |OPTIONS (skipHiveMetadata true)
        """.stripMargin)

      // Verify that the schema stored in catalog is a dummy one used for
      // data source tables. The actual schema is stored in table properties.
      val rawSchema = hiveClient.getTable("default", table).schema
      val metadata = new MetadataBuilder().putString("comment", "from deserializer").build()
      val expectedRawSchema = new StructType().add("col", "array<string>", true, metadata)
      assert(rawSchema == expectedRawSchema)

      val actualSchema = spark.sharedState.externalCatalog.getTable("default", table).schema
      val expectedActualSchema = new StructType()
        .add("a", "int")
        .add("b", "int")
      assert(actualSchema == expectedActualSchema)

      sql(s"INSERT INTO $table VALUES (1, 1)")
      sql(s"INSERT INTO $table VALUES (2, 1)")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS a, b")
      val fetchedStats0 =
        checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(2))
      assert(fetchedStats0.get.colStats == Map(
        "a" -> CatalogColumnStat(Some(2), Some("1"), Some("2"), Some(0), Some(4), Some(4)),
        "b" -> CatalogColumnStat(Some(1), Some("1"), Some("1"), Some(0), Some(4), Some(4))))
    }
  }

  test("Analyze hive serde tables when schema is not same as schema in table properties") {
    val table = "hive_serde"
    withTable(table) {
      sql(s"CREATE TABLE $table (C1 INT, C2 STRING, C3 DOUBLE)")

      // Verify that the table schema stored in hive catalog is
      // different than the schema stored in table properties.
      val rawSchema = hiveClient.getTable("default", table).schema
      val expectedRawSchema = new StructType()
        .add("c1", "int")
        .add("c2", "string")
        .add("c3", "double")
      assert(rawSchema == expectedRawSchema)

      val actualSchema = spark.sharedState.externalCatalog.getTable("default", table).schema
      val expectedActualSchema = new StructType()
        .add("C1", "int")
        .add("C2", "string")
        .add("C3", "double")
      assert(actualSchema == expectedActualSchema)

      sql(s"INSERT INTO TABLE $table SELECT 1, 'a', 10.0")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS C1")
      val fetchedStats1 =
        checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(1)).get
      assert(fetchedStats1.colStats == Map(
        "C1" -> CatalogColumnStat(distinctCount = Some(1), min = Some("1"), max = Some("1"),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))))
    }
  }

  test("SPARK-22745 - read Hive's statistics for partition") {
    val tableName = "hive_stats_part_table"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) USING hive " +
        "PARTITIONED BY (ds STRING)")
      sql(s"INSERT INTO TABLE $tableName PARTITION (ds='2017-01-01') SELECT * FROM src")
      var partition = spark.sessionState.catalog
        .getPartition(TableIdentifier(tableName), Map("ds" -> "2017-01-01"))

      assert(partition.stats.get.sizeInBytes == 5812)
      assert(partition.stats.get.rowCount.isEmpty)

      hiveClient
        .runSqlHive(s"ANALYZE TABLE $tableName PARTITION (ds='2017-01-01') COMPUTE STATISTICS")
      partition = spark.sessionState.catalog
        .getPartition(TableIdentifier(tableName), Map("ds" -> "2017-01-01"))

      assert(partition.stats.get.sizeInBytes == 5812)
      assert(partition.stats.get.rowCount == Some(500))
    }
  }

  test("SPARK-21079 - analyze table with location different than that of individual partitions") {
    val tableName = "analyzeTable_part"
    withTable(tableName) {
      withTempPath { path =>
        sql(s"CREATE TABLE $tableName (key STRING, value STRING) USING hive " +
          "PARTITIONED BY (ds STRING)")

        val partitionDates = List("2010-01-01", "2010-01-02", "2010-01-03")
        partitionDates.foreach { ds =>
          sql(s"INSERT INTO TABLE $tableName PARTITION (ds='$ds') SELECT * FROM src")
        }

        sql(s"ALTER TABLE $tableName SET LOCATION '${path.toURI}'")

        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS noscan")

        assert(getTableStats(tableName).sizeInBytes === BigInt(17436))
      }
    }
  }

  test("SPARK-21079 - analyze partitioned table with only a subset of partitions visible") {
    val sourceTableName = "analyzeTable_part"
    val tableName = "analyzeTable_part_vis"
    withTable(sourceTableName, tableName) {
      withTempPath { path =>
          // Create a table with 3 partitions all located under a single top-level directory 'path'
          sql(
            s"""
               |CREATE TABLE $sourceTableName (key STRING, value STRING)
               |USING hive
               |PARTITIONED BY (ds STRING)
               |LOCATION '${path.toURI}'
             """.stripMargin)

          val partitionDates = List("2010-01-01", "2010-01-02", "2010-01-03")
          partitionDates.foreach { ds =>
              sql(
                s"""
                   |INSERT INTO TABLE $sourceTableName PARTITION (ds='$ds')
                   |SELECT * FROM src
                 """.stripMargin)
          }

          // Create another table referring to the same location
          sql(
            s"""
               |CREATE TABLE $tableName (key STRING, value STRING)
               |USING hive
               |PARTITIONED BY (ds STRING)
               |LOCATION '${path.toURI}'
             """.stripMargin)

          // Register only one of the partitions found on disk
          val ds = partitionDates.head
          sql(s"ALTER TABLE $tableName ADD PARTITION (ds='$ds')")

          // Analyze original table - expect 3 partitions
          sql(s"ANALYZE TABLE $sourceTableName COMPUTE STATISTICS noscan")
          assert(getTableStats(sourceTableName).sizeInBytes === BigInt(3 * 5812))

          // Analyze partial-copy table - expect only 1 partition
          sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS noscan")
          assert(getTableStats(tableName).sizeInBytes === BigInt(5812))
        }
    }
  }

  test("SPARK-45731: update partition stats with ANALYZE TABLE") {
    val tableName = "analyzeTable_part"

    def queryStats(ds: String): Option[CatalogStatistics] = {
      val partition =
        spark.sessionState.catalog.getPartition(TableIdentifier(tableName), Map("ds" -> ds))
      partition.stats
    }

    val partitionDates = List("2010-01-01", "2010-01-02", "2010-01-03")
    val expectedRowCount = 500

    Seq(true, false).foreach { partitionStatsEnabled =>
      withSQLConf(SQLConf.UPDATE_PART_STATS_IN_ANALYZE_TABLE_ENABLED.key ->
        partitionStatsEnabled.toString) {
        withTable(tableName) {
          withTempPath { path =>
            // Create a table with 3 partitions all located under a directory 'path'
            sql(
              s"""
                 |CREATE TABLE $tableName (key INT, value STRING)
                 |USING parquet
                 |PARTITIONED BY (ds STRING)
                 |LOCATION '${path.toURI}'
               """.stripMargin)

            partitionDates.foreach { ds =>
              sql(s"ALTER TABLE $tableName ADD PARTITION (ds='$ds') LOCATION '$path/ds=$ds'")
              sql("SELECT * FROM src").write.mode(SaveMode.Overwrite)
                .format("parquet").save(s"$path/ds=$ds")
            }

            assert(getCatalogTable(tableName).stats.isEmpty)
            partitionDates.foreach { ds =>
              assert(queryStats(ds).isEmpty)
            }

            sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS NOSCAN")

            // Table size should also have been updated
            assert(getTableStats(tableName).sizeInBytes > 0)
            // Row count should NOT be updated with the `NOSCAN` option
            assert(getTableStats(tableName).rowCount.isEmpty)

            partitionDates.foreach { ds =>
              val partStats = queryStats(ds)
              if (partitionStatsEnabled) {
                assert(partStats.nonEmpty)
                assert(partStats.get.sizeInBytes > 0)
                assert(partStats.get.rowCount.isEmpty)
              } else {
                assert(partStats.isEmpty)
              }
            }

            sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")

            assert(getTableStats(tableName).sizeInBytes > 0)
            // Table row count should be updated
            assert(getTableStats(tableName).rowCount.get == 3 * expectedRowCount)

            partitionDates.foreach { ds =>
              val partStats = queryStats(ds)
              if (partitionStatsEnabled) {
                assert(partStats.nonEmpty)
                // The scan option should update partition row count
                assert(partStats.get.sizeInBytes > 0)
                assert(partStats.get.rowCount.get == expectedRowCount)
              } else {
                assert(partStats.isEmpty)
              }
            }
          }
        }
      }
    }
  }

  test("analyze single partition") {
    val tableName = "analyzeTable_part"

    def queryStats(ds: String): CatalogStatistics = {
      val partition =
        spark.sessionState.catalog.getPartition(TableIdentifier(tableName), Map("ds" -> ds))
      partition.stats.get
    }

    def createPartition(ds: String, query: String): Unit = {
      sql(s"INSERT INTO TABLE $tableName PARTITION (ds='$ds') $query")
    }

    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) USING hive " +
        "PARTITIONED BY (ds STRING)")

      createPartition("2010-01-01", "SELECT '1', 'A' from src")
      createPartition("2010-01-02", "SELECT '1', 'A' from src UNION ALL SELECT '1', 'A' from src")
      createPartition("2010-01-03", "SELECT '1', 'A' from src")

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-01') COMPUTE STATISTICS NOSCAN")

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-02') COMPUTE STATISTICS NOSCAN")

      assert(queryStats("2010-01-01").rowCount === None)
      assert(queryStats("2010-01-01").sizeInBytes === 2000)

      assert(queryStats("2010-01-02").rowCount === None)
      assert(queryStats("2010-01-02").sizeInBytes === 2*2000)

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-01') COMPUTE STATISTICS")

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-02') COMPUTE STATISTICS")

      assert(queryStats("2010-01-01").rowCount.get === 500)
      assert(queryStats("2010-01-01").sizeInBytes === 2000)

      assert(queryStats("2010-01-02").rowCount.get === 2*500)
      assert(queryStats("2010-01-02").sizeInBytes === 2*2000)
    }
  }

  test("analyze a set of partitions") {
    val tableName = "analyzeTable_part"

    def queryStats(ds: String, hr: String): Option[CatalogStatistics] = {
      val tableId = TableIdentifier(tableName)
      val partition =
        spark.sessionState.catalog.getPartition(tableId, Map("ds" -> ds, "hr" -> hr))
      partition.stats
    }

    def assertPartitionStats(
        ds: String,
        hr: String,
        rowCount: Option[BigInt],
        sizeInBytes: BigInt): Unit = {
      val stats = queryStats(ds, hr).get
      assert(stats.rowCount === rowCount)
      assert(stats.sizeInBytes === sizeInBytes)
    }

    def createPartition(ds: String, hr: Int, query: String): Unit = {
      sql(s"INSERT INTO TABLE $tableName PARTITION (ds='$ds', hr=$hr) $query")
    }

    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) USING hive " +
        "PARTITIONED BY (ds STRING, hr INT)")

      createPartition("2010-01-01", 10, "SELECT '1', 'A' from src")
      createPartition("2010-01-01", 11, "SELECT '1', 'A' from src")
      createPartition("2010-01-02", 10, "SELECT '1', 'A' from src")
      createPartition("2010-01-02", 11,
        "SELECT '1', 'A' from src UNION ALL SELECT '1', 'A' from src")

      assertPartitionStats("2010-01-01", "10", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "10", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "11", rowCount = None, sizeInBytes = 2*2000)

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-01') COMPUTE STATISTICS")

      assertPartitionStats("2010-01-01", "10", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "10", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "11", rowCount = None, sizeInBytes = 2*2000)

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-02') COMPUTE STATISTICS")

      assertPartitionStats("2010-01-01", "10", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "10", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "11", rowCount = Some(2*500), sizeInBytes = 2*2000)
    }
  }

  test("analyze all partitions") {
    val tableName = "analyzeTable_part"

    def assertPartitionStats(
        ds: String,
        hr: String,
        rowCount: Option[BigInt],
        sizeInBytes: BigInt): Unit = {
      val stats = spark.sessionState.catalog.getPartition(TableIdentifier(tableName),
        Map("ds" -> ds, "hr" -> hr)).stats.get
      assert(stats.rowCount === rowCount)
      assert(stats.sizeInBytes === sizeInBytes)
    }

    def createPartition(ds: String, hr: Int, query: String): Unit = {
      sql(s"INSERT INTO TABLE $tableName PARTITION (ds='$ds', hr=$hr) $query")
    }

    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) USING hive " +
        "PARTITIONED BY (ds STRING, hr INT)")

      createPartition("2010-01-01", 10, "SELECT '1', 'A' from src")
      createPartition("2010-01-01", 11, "SELECT '1', 'A' from src")
      createPartition("2010-01-02", 10, "SELECT '1', 'A' from src")
      createPartition("2010-01-02", 11,
        "SELECT '1', 'A' from src UNION ALL SELECT '1', 'A' from src")

      sql(s"ANALYZE TABLE $tableName PARTITION (ds, hr) COMPUTE STATISTICS NOSCAN")

      assertPartitionStats("2010-01-01", "10", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "11", rowCount = None, sizeInBytes = 2*2000)

      sql(s"ANALYZE TABLE $tableName PARTITION (ds, hr) COMPUTE STATISTICS")

      assertPartitionStats("2010-01-01", "10", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "11", rowCount = Some(2*500), sizeInBytes = 2*2000)
    }
  }

  test("analyze partitions for an empty table") {
    val tableName = "analyzeTable_part"

    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) PARTITIONED BY (ds STRING)")

      // make sure there is no exception
      sql(s"ANALYZE TABLE $tableName PARTITION (ds) COMPUTE STATISTICS NOSCAN")

      // make sure there is no exception
      sql(s"ANALYZE TABLE $tableName PARTITION (ds) COMPUTE STATISTICS")
    }
  }

  test("analyze partitions case sensitivity") {
    val tableName = "analyzeTable_part"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) PARTITIONED BY (ds STRING)")

      sql(s"INSERT INTO TABLE $tableName PARTITION (ds='2010-01-01') SELECT * FROM src")

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql(s"ANALYZE TABLE $tableName PARTITION (DS='2010-01-01') COMPUTE STATISTICS")
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val message = intercept[AnalysisException] {
          sql(s"ANALYZE TABLE $tableName PARTITION (DS='2010-01-01') COMPUTE STATISTICS")
        }.getMessage
        assert(message.contains(
          "DS is not a valid partition column in table " +
            s"`$SESSION_CATALOG_NAME`.`default`.`$tableName`"))
      }
    }
  }

  test("analyze partial partition specifications") {

    val tableName = "analyzeTable_part"

    def assertAnalysisException(partitionSpec: String): Unit = {
      val message = intercept[AnalysisException] {
        sql(s"ANALYZE TABLE $tableName $partitionSpec COMPUTE STATISTICS")
      }.getMessage
      assert(message.contains("The list of partition columns with values " +
        s"in partition specification for table '${tableName.toLowerCase(Locale.ROOT)}' in " +
        "database 'default' is not a prefix of the list of partition columns defined in " +
        "the table schema"))
    }

    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (key STRING, value STRING)
           |PARTITIONED BY (a STRING, b INT, c STRING)
         """.stripMargin)

      sql(s"INSERT INTO TABLE $tableName PARTITION (a='a1', b=10, c='c1') SELECT * FROM src")

      sql(s"ANALYZE TABLE $tableName PARTITION (a='a1') COMPUTE STATISTICS")
      sql(s"ANALYZE TABLE $tableName PARTITION (a='a1', b=10) COMPUTE STATISTICS")
      sql(s"ANALYZE TABLE $tableName PARTITION (A='a1', b=10) COMPUTE STATISTICS")
      sql(s"ANALYZE TABLE $tableName PARTITION (b=10, a='a1') COMPUTE STATISTICS")
      sql(s"ANALYZE TABLE $tableName PARTITION (b=10, A='a1') COMPUTE STATISTICS")

      assertAnalysisException("PARTITION (b=10)")
      assertAnalysisException("PARTITION (a, b=10)")
      assertAnalysisException("PARTITION (b=10, c='c1')")
      assertAnalysisException("PARTITION (a, b=10, c='c1')")
      assertAnalysisException("PARTITION (c='c1')")
      assertAnalysisException("PARTITION (a, b, c='c1')")
      assertAnalysisException("PARTITION (a='a1', c='c1')")
      assertAnalysisException("PARTITION (a='a1', b, c='c1')")
    }
  }

  test("analyze not found column") {
    val tableName = "analyzeTable"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) PARTITIONED BY (ds STRING)")

      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS fakeColumn")
        },
        condition = "COLUMN_NOT_FOUND",
        parameters = Map(
          "colName" -> "`fakeColumn`",
          "caseSensitiveConfig" -> "\"spark.sql.caseSensitive\""
        )
      )
    }
  }

  test("analyze non-existent partition") {

    def assertAnalysisException(analyzeCommand: String, errorMessage: String): Unit = {
      val message = intercept[AnalysisException] {
        sql(analyzeCommand)
      }.getMessage
      assert(message.contains(errorMessage))
    }

    val tableName = "analyzeTable_part"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) PARTITIONED BY (ds STRING)")

      sql(s"INSERT INTO TABLE $tableName PARTITION (ds='2010-01-01') SELECT * FROM src")

      assertAnalysisException(
        s"ANALYZE TABLE $tableName PARTITION (hour=20) COMPUTE STATISTICS",
        "hour is not a valid partition column in table " +
          s"`$SESSION_CATALOG_NAME`.`default`.`${tableName.toLowerCase(Locale.ROOT)}`"
      )

      assertAnalysisException(
        s"ANALYZE TABLE $tableName PARTITION (hour) COMPUTE STATISTICS",
        "hour is not a valid partition column in table " +
          s"`$SESSION_CATALOG_NAME`.`default`.`${tableName.toLowerCase(Locale.ROOT)}`"
      )

      intercept[NoSuchPartitionException] {
        sql(s"ANALYZE TABLE $tableName PARTITION (ds='2011-02-30') COMPUTE STATISTICS")
      }
    }
  }

  test("test table-level statistics for hive tables created in HiveExternalCatalog") {
    val textTable = "textTable"
    withTable(textTable) {
      // Currently Spark's statistics are self-contained, we don't have statistics until we use
      // the `ANALYZE TABLE` command.
      sql(s"CREATE TABLE $textTable (key STRING, value STRING) STORED AS TEXTFILE")
      checkTableStats(
        textTable,
        hasSizeInBytes = false,
        expectedRowCounts = None)
      sql(s"INSERT INTO TABLE $textTable SELECT * FROM src")
      checkTableStats(
        textTable,
        hasSizeInBytes = true,
        expectedRowCounts = None)

      // noscan won't count the number of rows
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS noscan")
      val fetchedStats1 =
        checkTableStats(textTable, hasSizeInBytes = true, expectedRowCounts = None)

      // without noscan, we count the number of rows
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS")
      val fetchedStats2 =
        checkTableStats(textTable, hasSizeInBytes = true, expectedRowCounts = Some(500))
      assert(fetchedStats1.get.sizeInBytes == fetchedStats2.get.sizeInBytes)
    }
  }

  test("keep existing row count in stats with noscan if table is not changed") {
    val textTable = "textTable"
    withTable(textTable) {
      sql(s"CREATE TABLE $textTable (key STRING, value STRING)")
      sql(s"INSERT INTO TABLE $textTable SELECT * FROM src")
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS")
      val fetchedStats1 =
        checkTableStats(textTable, hasSizeInBytes = true, expectedRowCounts = Some(500))

      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS noscan")
      // when the table is not changed, total size is the same, and the old row count is kept
      val fetchedStats2 =
        checkTableStats(textTable, hasSizeInBytes = true, expectedRowCounts = Some(500))
      assert(fetchedStats1 == fetchedStats2)
    }
  }

  test("keep existing column stats if table is not changed") {
    val table = "update_col_stats_table"
    withTable(table) {
      sql(s"CREATE TABLE $table (c1 INT, c2 STRING, c3 DOUBLE)")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c1")
      val fetchedStats0 =
        checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(0))
      assert(fetchedStats0.get.colStats ==
        Map("c1" -> CatalogColumnStat(Some(0), None, None, Some(0), Some(4), Some(4))))

      // Insert new data and analyze: have the latest column stats.
      sql(s"INSERT INTO TABLE $table SELECT 1, 'a', 10.0")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c1")
      val fetchedStats1 =
        checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(1)).get
      assert(fetchedStats1.colStats == Map(
        "c1" -> CatalogColumnStat(distinctCount = Some(1), min = Some("1"), max = Some("1"),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))))

      // Analyze another column: since the table is not changed, the precious column stats are kept.
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c2")
      val fetchedStats2 =
        checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(1)).get
      assert(fetchedStats2.colStats == Map(
        "c1" -> CatalogColumnStat(distinctCount = Some(1), min = Some("1"), max = Some("1"),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        "c2" -> CatalogColumnStat(distinctCount = Some(1), min = None, max = None,
          nullCount = Some(0), avgLen = Some(1), maxLen = Some(1))))

      // Insert new data and analyze: stale column stats are removed and newly collected column
      // stats are added.
      sql(s"INSERT INTO TABLE $table SELECT 2, 'b', 20.0")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c1, c3")
      val fetchedStats3 =
        checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(2)).get
      assert(fetchedStats3.colStats == Map(
        "c1" -> CatalogColumnStat(distinctCount = Some(2), min = Some("1"), max = Some("2"),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        "c3" -> CatalogColumnStat(distinctCount = Some(2), min = Some("10.0"), max = Some("20.0"),
          nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))))
    }
  }

  test("collecting statistics for all columns") {
    val table = "update_col_stats_table"
    withTable(table) {
      sql(s"CREATE TABLE $table (c1 INT, c2 STRING, c3 DOUBLE)")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR ALL COLUMNS")
      val fetchedStats0 =
        checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(0))
      assert(fetchedStats0.get.colStats == Map(
        "c1" -> CatalogColumnStat(distinctCount = Some(0), min = None, max = None,
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        "c3" -> CatalogColumnStat(distinctCount = Some(0), min = None, max = None,
          nullCount = Some(0), avgLen = Some(8), maxLen = Some(8)),
        "c2" -> CatalogColumnStat(distinctCount = Some(0), min = None, max = None,
          nullCount = Some(0), avgLen = Some(20), maxLen = Some(20))))

      // Insert new data and analyze: have the latest column stats.
      sql(s"INSERT INTO TABLE $table SELECT 1, 'a', 10.0")
      sql(s"INSERT INTO TABLE $table SELECT 1, 'b', null")

      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR ALL COLUMNS")
      val fetchedStats1 =
        checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(2))
      assert(fetchedStats1.get.colStats == Map(
        "c1" -> CatalogColumnStat(distinctCount = Some(1), min = Some("1"), max = Some("1"),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        "c3" -> CatalogColumnStat(distinctCount = Some(1), min = Some("10.0"), max = Some("10.0"),
          nullCount = Some(1), avgLen = Some(8), maxLen = Some(8)),
        "c2" -> CatalogColumnStat(distinctCount = Some(2), min = None, max = None,
          nullCount = Some(0), avgLen = Some(1), maxLen = Some(1))))
    }
  }

  test("analyze column command parameters validation") {
    val e1 = intercept[IllegalArgumentException] {
      AnalyzeColumnCommand(TableIdentifier("test"), Option(Seq("c1")), true).run(spark)
    }
    assert(e1.getMessage.contains("Parameter `columnNames` or `allColumns` are" +
      " mutually exclusive"))
    val e2 = intercept[IllegalArgumentException] {
      AnalyzeColumnCommand(TableIdentifier("test"), None, false).run(spark)
    }
    assert(e2.getMessage.contains("Parameter `columnNames` or `allColumns` are" +
      " mutually exclusive"))
  }

  private def createNonPartitionedTable(
      tabName: String,
      analyzedBySpark: Boolean = true,
      analyzedByHive: Boolean = true): Unit = {
    sql(
      s"""
         |CREATE TABLE $tabName (key STRING, value STRING)
         |STORED AS TEXTFILE
         |TBLPROPERTIES ('prop1' = 'val1', 'prop2' = 'val2')
       """.stripMargin)
    sql(s"INSERT INTO TABLE $tabName SELECT * FROM src")
    if (analyzedBySpark) sql(s"ANALYZE TABLE $tabName COMPUTE STATISTICS")
    // This is to mimic the scenario in which Hive generates statistics before we read it
    if (analyzedByHive) hiveClient.runSqlHive(s"ANALYZE TABLE $tabName COMPUTE STATISTICS")
    val describeResult1 = hiveClient.runSqlHive(s"DESCRIBE FORMATTED $tabName")

    val tableMetadata = getCatalogTable(tabName).properties
    // statistics info is not contained in the metadata of the original table
    assert(Seq(StatsSetupConst.COLUMN_STATS_ACCURATE,
      StatsSetupConst.NUM_FILES,
      StatsSetupConst.NUM_PARTITIONS,
      StatsSetupConst.ROW_COUNT,
      StatsSetupConst.RAW_DATA_SIZE,
      StatsSetupConst.TOTAL_SIZE).forall(!tableMetadata.contains(_)))

    if (analyzedByHive) {
      assert(StringUtils.filterPattern(describeResult1, "*numRows\\s+500*").nonEmpty)
    } else {
      assert(StringUtils.filterPattern(describeResult1, "*numRows\\s+500*").isEmpty)
    }
  }

  private def extractStatsPropValues(
      descOutput: Seq[String],
      propKey: String): Option[BigInt] = {
    val str = descOutput
      .filterNot(_.contains(STATISTICS_PREFIX))
      .filter(_.contains(propKey))
    if (str.isEmpty) {
      None
    } else {
      assert(str.length == 1, "found more than one matches")
      val pattern = new Regex(s"""$propKey\\s+(-?\\d+)""")
      val pattern(value) = str.head.trim
      Option(BigInt(value))
    }
  }

  test("get statistics when not analyzed in Hive or Spark") {
    val tabName = "tab1"
    withTable(tabName) {
      createNonPartitionedTable(tabName, analyzedByHive = false, analyzedBySpark = false)
      checkTableStats(tabName, hasSizeInBytes = true, expectedRowCounts = None)

      // ALTER TABLE SET TBLPROPERTIES invalidates some contents of Hive specific statistics
      // This is triggered by the Hive alterTable API
      val describeResult = hiveClient.runSqlHive(s"DESCRIBE FORMATTED $tabName")

      val rawDataSize = extractStatsPropValues(describeResult, "rawDataSize")
      val numRows = extractStatsPropValues(describeResult, "numRows")
      val totalSize = extractStatsPropValues(describeResult, "totalSize")
      assert(rawDataSize.isEmpty, "rawDataSize should not be shown without table analysis")
      assert(numRows.isEmpty, "numRows should not be shown without table analysis")
      assert(totalSize.isDefined && totalSize.get > 0, "totalSize is lost")
    }
  }

  test("alter table should not have the side effect to store statistics in Spark side") {
    val table = "alter_table_side_effect"
    withTable(table) {
      sql(s"CREATE TABLE $table (i string, j string) USING hive")
      sql(s"INSERT INTO TABLE $table SELECT 'a', 'b'")
      val catalogTable1 = getCatalogTable(table)
      val hiveSize1 = BigInt(catalogTable1.ignoredProperties(StatsSetupConst.TOTAL_SIZE))

      sql(s"ALTER TABLE $table SET TBLPROPERTIES ('prop1' = 'a')")

      sql(s"INSERT INTO TABLE $table SELECT 'c', 'd'")
      val catalogTable2 = getCatalogTable(table)
      val hiveSize2 = BigInt(catalogTable2.ignoredProperties(StatsSetupConst.TOTAL_SIZE))
      // After insertion, Hive's stats should be changed.
      assert(hiveSize2 > hiveSize1)
      // We haven't generate stats in Spark, so we should still use Hive's stats here.
      assert(catalogTable2.stats.get.sizeInBytes == hiveSize2)
    }
  }

  private def testAlterTableProperties(tabName: String, alterTablePropCmd: String): Unit = {
    Seq(true, false).foreach { analyzedBySpark =>
      withTable(tabName) {
        createNonPartitionedTable(tabName, analyzedByHive = true, analyzedBySpark = analyzedBySpark)
        checkTableStats(tabName, hasSizeInBytes = true, expectedRowCounts = Some(500))

        // Run ALTER TABLE command
        sql(alterTablePropCmd)

        val describeResult = hiveClient.runSqlHive(s"DESCRIBE FORMATTED $tabName")

        val totalSize = extractStatsPropValues(describeResult, "totalSize")
        assert(totalSize.isDefined && totalSize.get > 0, "totalSize is lost")

        val numRows = extractStatsPropValues(describeResult, "numRows")
        assert(numRows.isDefined && numRows.get == 500)
        val rawDataSize = extractStatsPropValues(describeResult, "rawDataSize")
        assert(rawDataSize.isDefined && rawDataSize.get == 5312)
        checkTableStats(tabName, hasSizeInBytes = true, expectedRowCounts = Some(500))
      }
    }
  }

  test("alter table SET TBLPROPERTIES after analyze table") {
    testAlterTableProperties("set_prop_table",
      "ALTER TABLE set_prop_table SET TBLPROPERTIES ('foo' = 'a')")
  }

  test("alter table UNSET TBLPROPERTIES after analyze table") {
    testAlterTableProperties("unset_prop_table",
      "ALTER TABLE unset_prop_table UNSET TBLPROPERTIES ('prop1')")
  }

  /**
   * To see if stats exist, we need to check spark's stats properties instead of catalog
   * statistics, because hive would change stats in metastore and thus change catalog statistics.
   */
  private def getStatsProperties(tableName: String): Map[String, String] = {
    val hTable = hiveClient.getTable(spark.sessionState.catalog.getCurrentDatabase, tableName)
    hTable.properties.filter { case (k, _) => k.startsWith(STATISTICS_PREFIX) }
  }

  test("change stats after insert command for hive table") {
    val table = s"change_stats_insert_hive_table"
    Seq(false, true).foreach { autoUpdate =>
      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> autoUpdate.toString) {
        withTable(table) {
          sql(s"CREATE TABLE $table (i int, j string)")
          // analyze to get initial stats
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS i, j")
          val fetched1 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(0))
          assert(fetched1.get.sizeInBytes == 0)
          assert(fetched1.get.colStats.size == 2)

          // insert into command
          sql(s"INSERT INTO TABLE $table SELECT 1, 'abc'")
          if (autoUpdate) {
            val fetched2 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
            assert(fetched2.get.sizeInBytes > 0)
            assert(fetched2.get.colStats.isEmpty)
            val statsProp = getStatsProperties(table)
            assert(statsProp(STATISTICS_TOTAL_SIZE).toLong == fetched2.get.sizeInBytes)
          } else {
            assert(getStatsProperties(table).isEmpty)
          }
        }
      }
    }
  }

  test("change stats after load data command") {
    val table = "change_stats_load_table"
    Seq(false, true).foreach { autoUpdate =>
      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> autoUpdate.toString) {
        withTable(table) {
          sql(s"CREATE TABLE $table (i INT, j STRING) STORED AS PARQUET")
          // analyze to get initial stats
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS i, j")
          val fetched1 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(0))
          assert(fetched1.get.sizeInBytes == 0)
          assert(fetched1.get.colStats.size == 2)

          withTempDir { loadPath =>
            // load data command
            val file = new File(s"$loadPath/data")
            Utils.tryWithResource(new PrintWriter(file)) { writer =>
              writer.write("2,xyz")
            }
            sql(s"LOAD DATA INPATH '${loadPath.toURI.toString}' INTO TABLE $table")
            if (autoUpdate) {
              val fetched2 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
              assert(fetched2.get.sizeInBytes > 0)
              assert(fetched2.get.colStats.isEmpty)
              val statsProp = getStatsProperties(table)
              assert(statsProp(STATISTICS_TOTAL_SIZE).toLong == fetched2.get.sizeInBytes)
            } else {
              assert(getStatsProperties(table).isEmpty)
            }
          }
        }
      }
    }
  }

  test("change stats after add/drop partition command") {
    val table = "change_stats_part_table"
    Seq(false, true).foreach { autoUpdate =>
      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> autoUpdate.toString) {
        withTable(table) {
          sql(s"CREATE TABLE $table (i INT, j STRING) USING hive " +
            "PARTITIONED BY (ds STRING, hr STRING)")
          // table has two partitions initially
          for (ds <- Seq("2008-04-08"); hr <- Seq("11", "12")) {
            sql(s"INSERT OVERWRITE TABLE $table PARTITION (ds='$ds',hr='$hr') SELECT 1, 'a'")
          }
          // analyze to get initial stats
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS i, j")
          val fetched1 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(2))
          assert(fetched1.get.sizeInBytes > 0)
          assert(fetched1.get.colStats.size == 2)

          withTempPaths(numPaths = 2) { case Seq(dir1, dir2) =>
            val partDir1 = new File(new File(dir1, "ds=2008-04-09"), "hr=11")
            val file1 = new File(partDir1, "data")
            file1.getParentFile.mkdirs()
            Utils.tryWithResource(new PrintWriter(file1)) { writer =>
              writer.write("1,a")
            }

            val partDir2 = new File(new File(dir2, "ds=2008-04-09"), "hr=12")
            val file2 = new File(partDir2, "data")
            file2.getParentFile.mkdirs()
            Utils.tryWithResource(new PrintWriter(file2)) { writer =>
              writer.write("1,a")
            }

            // add partition command
            sql(
              s"""
                 |ALTER TABLE $table ADD
                 |PARTITION (ds='2008-04-09', hr='11') LOCATION '${partDir1.toURI.toString}'
                 |PARTITION (ds='2008-04-09', hr='12') LOCATION '${partDir2.toURI.toString}'
            """.stripMargin)
            if (autoUpdate) {
              val fetched2 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
              assert(fetched2.get.sizeInBytes > fetched1.get.sizeInBytes)
              assert(fetched2.get.colStats.isEmpty)
              val statsProp = getStatsProperties(table)
              assert(statsProp(STATISTICS_TOTAL_SIZE).toLong == fetched2.get.sizeInBytes)

              // SPARK-38573: Support Partition Level Statistics Collection
              val partStats1 = getPartitionStats(table, Map("ds" -> "2008-04-08", "hr" -> "11"))
              assert(partStats1.sizeInBytes > 0)
              val partStats2 = getPartitionStats(table, Map("ds" -> "2008-04-08", "hr" -> "12"))
              assert(partStats2.sizeInBytes > 0)
              val partStats3 = getPartitionStats(table, Map("ds" -> "2008-04-09", "hr" -> "11"))
              assert(partStats3.sizeInBytes > 0)
              val partStats4 = getPartitionStats(table, Map("ds" -> "2008-04-09", "hr" -> "12"))
              assert(partStats4.sizeInBytes > 0)
            } else {
              assert(getStatsProperties(table).isEmpty)
            }

            // now the table has four partitions, generate stats again
            sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS i, j")
            val fetched3 = checkTableStats(
              table, hasSizeInBytes = true, expectedRowCounts = Some(4))
            assert(fetched3.get.sizeInBytes > 0)
            assert(fetched3.get.colStats.size == 2)

            // drop partition command
            sql(s"ALTER TABLE $table DROP PARTITION (ds='2008-04-08'), PARTITION (hr='12')")
            assert(spark.sessionState.catalog.listPartitions(TableIdentifier(table))
              .map(_.spec).toSet == Set(Map("ds" -> "2008-04-09", "hr" -> "11")))
            assert(partDir1.exists())
            // only one partition left
            if (autoUpdate) {
              val fetched4 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
              assert(fetched4.get.sizeInBytes < fetched1.get.sizeInBytes)
              assert(fetched4.get.colStats.isEmpty)
              val statsProp = getStatsProperties(table)
              assert(statsProp(STATISTICS_TOTAL_SIZE).toLong == fetched4.get.sizeInBytes)

              // SPARK-38573: Support Partition Level Statistics Collection
              val partStats3 = getPartitionStats(table, Map("ds" -> "2008-04-09", "hr" -> "11"))
              assert(partStats3.sizeInBytes > 0)
            } else {
              assert(getStatsProperties(table).isEmpty)
            }
          }
        }
      }
    }
  }

  test("add/drop partitions - managed table") {
    val catalog = spark.sessionState.catalog
    val managedTable = "partitionedTable"
    withTable(managedTable) {
      sql(
        s"""
           |CREATE TABLE $managedTable (key INT, value STRING)
           |USING hive
           |PARTITIONED BY (ds STRING, hr STRING)
         """.stripMargin)

      for (ds <- Seq("2008-04-08", "2008-04-09"); hr <- Seq("11", "12")) {
        sql(
          s"""
             |INSERT OVERWRITE TABLE $managedTable
             |partition (ds='$ds',hr='$hr')
             |SELECT 1, 'a'
           """.stripMargin)
      }

      checkTableStats(
        managedTable, hasSizeInBytes = false, expectedRowCounts = None)

      sql(s"ANALYZE TABLE $managedTable COMPUTE STATISTICS")

      val stats1 = checkTableStats(
        managedTable, hasSizeInBytes = true, expectedRowCounts = Some(4))

      sql(
        s"""
           |ALTER TABLE $managedTable DROP PARTITION (ds='2008-04-08'),
           |PARTITION (hr='12')
        """.stripMargin)
      assert(catalog.listPartitions(TableIdentifier(managedTable)).map(_.spec).toSet ==
        Set(Map("ds" -> "2008-04-09", "hr" -> "11")))

      sql(s"ANALYZE TABLE $managedTable COMPUTE STATISTICS")

      val stats2 = checkTableStats(
        managedTable, hasSizeInBytes = true, expectedRowCounts = Some(1))
      assert(stats1.get.sizeInBytes > stats2.get.sizeInBytes)

      sql(s"ALTER TABLE $managedTable ADD PARTITION (ds='2008-04-08', hr='12')")
      sql(s"ANALYZE TABLE $managedTable COMPUTE STATISTICS")
      val stats4 = checkTableStats(
        managedTable, hasSizeInBytes = true, expectedRowCounts = Some(1))

      assert(stats1.get.sizeInBytes > stats4.get.sizeInBytes)
      assert(stats4.get.sizeInBytes == stats2.get.sizeInBytes)
    }
  }

  test("test statistics of LogicalRelation converted from Hive serde tables") {
    Seq("orc", "parquet").foreach { format =>
      Seq(true, false).foreach { isConverted =>
        withSQLConf(
          HiveUtils.CONVERT_METASTORE_ORC.key -> s"$isConverted",
          HiveUtils.CONVERT_METASTORE_PARQUET.key -> s"$isConverted") {
          withTable(format) {
            sql(s"CREATE TABLE $format (key STRING, value STRING) STORED AS $format")
            sql(s"INSERT INTO TABLE $format SELECT * FROM src")

            checkTableStats(format, hasSizeInBytes = !isConverted, expectedRowCounts = None)
            sql(s"ANALYZE TABLE $format COMPUTE STATISTICS")
            checkTableStats(format, hasSizeInBytes = true, expectedRowCounts = Some(500))
          }
        }
      }
    }
  }

  test("verify serialized column stats after analyzing columns") {
    import testImplicits._

    val tableName = "column_stats_test_ser"
    // (data.head.productArity - 1) because the last column does not support stats collection.
    assert(stats.size == data.head.productArity - 1)
    val df = data.toDF(stats.keys.toSeq :+ "carray" : _*)

    def checkColStatsProps(expected: Map[String, String]): Unit = {
      sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS " + stats.keys.mkString(", "))
      val table = hiveClient.getTable("default", tableName)
      val props =
        table.properties.filter { case (k, _) => k.startsWith("spark.sql.statistics.colStats") }
      assert(props == expected)
    }

    withTable(tableName) {
      df.write.saveAsTable(tableName)

      // Collect and validate statistics
      checkColStatsProps(expectedSerializedColStats)

      withSQLConf(
        SQLConf.HISTOGRAM_ENABLED.key -> "true", SQLConf.HISTOGRAM_NUM_BINS.key -> "2") {

        checkColStatsProps(expectedSerializedColStats ++ expectedSerializedHistograms)
      }
    }
  }

  test("verify column stats can be deserialized from tblproperties") {
    import testImplicits._

    val tableName = "column_stats_test_de"
    // (data.head.productArity - 1) because the last column does not support stats collection.
    assert(stats.size == data.head.productArity - 1)
    // Hive can't parse data type "timestamp_ntz"
    val df = data.toDF(stats.keys.toSeq :+ "carray" : _*).drop("ctimestamp_ntz")

    withTable(tableName) {
      df.write.saveAsTable(tableName)

      // Put in stats properties manually.
      val table = getCatalogTable(tableName)
      val newTable = table.copy(
        properties = table.properties ++
          expectedSerializedColStats ++ expectedSerializedHistograms +
          ("spark.sql.statistics.totalSize" -> "1") /* totalSize always required */)
      hiveClient.alterTable(newTable)

      validateColStats(tableName, statsWithHgms)
    }
  }

  test("serialization and deserialization of histograms to/from hive metastore") {
    import testImplicits._

    def checkBinsOrder(bins: Array[HistogramBin]): Unit = {
      for (i <- bins.indices) {
        val b = bins(i)
        assert(b.lo <= b.hi)
        if (i > 0) {
          val pre = bins(i - 1)
          assert(pre.hi <= b.lo)
        }
      }
    }

    val startTimestamp = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2016-05-08 00:00:01"))
    val df = (1 to 5000)
      .map(i => (i, DateTimeUtils.toJavaTimestamp(startTimestamp + i)))
      .toDF("cint", "ctimestamp")
    val tableName = "histogram_serde_test"

    withTable(tableName) {
      df.write.saveAsTable(tableName)

      withSQLConf(SQLConf.HISTOGRAM_ENABLED.key -> "true") {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS cint, ctimestamp")
        val table = hiveClient.getTable("default", tableName)
        val intHistogramProps = table.properties
          .filter { case (k, _) => k.startsWith("spark.sql.statistics.colStats.cint.histogram") }
        assert(intHistogramProps.size == 1)

        val tsHistogramProps = table.properties.filter {
          case (k, _) => k.startsWith("spark.sql.statistics.colStats.ctimestamp.histogram") }
        assert(tsHistogramProps.size == 1)

        // Validate histogram after deserialization.
        val cs = getTableStats(tableName).colStats
        val intHistogram = cs("cint").histogram.get
        val tsHistogram = cs("ctimestamp").histogram.get
        assert(intHistogram.bins.length == spark.sessionState.conf.histogramNumBins)
        checkBinsOrder(intHistogram.bins)
        assert(tsHistogram.bins.length == spark.sessionState.conf.histogramNumBins)
        checkBinsOrder(tsHistogram.bins)
      }
    }
  }

  private def testUpdatingTableStats(tableDescription: String, createTableCmd: String): Unit = {
    test("test table-level statistics for " + tableDescription) {
      val parquetTable = "parquetTable"
      withTable(parquetTable) {
        sql(createTableCmd)
        val catalogTable = getCatalogTable(parquetTable)
        assert(DDLUtils.isDatasourceTable(catalogTable))

        // Add a filter to avoid creating too many partitions
        sql(s"INSERT INTO TABLE $parquetTable SELECT * FROM src WHERE key < 10")
        checkTableStats(parquetTable, hasSizeInBytes = false, expectedRowCounts = None)

        // noscan won't count the number of rows
        sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS noscan")
        val fetchedStats1 =
          checkTableStats(parquetTable, hasSizeInBytes = true, expectedRowCounts = None)

        sql(s"INSERT INTO TABLE $parquetTable SELECT * FROM src WHERE key < 10")
        sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS noscan")
        val fetchedStats2 =
          checkTableStats(parquetTable, hasSizeInBytes = true, expectedRowCounts = None)
        assert(fetchedStats2.get.sizeInBytes > fetchedStats1.get.sizeInBytes)

        // without noscan, we count the number of rows
        sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS")
        val fetchedStats3 =
          checkTableStats(parquetTable, hasSizeInBytes = true, expectedRowCounts = Some(20))
        assert(fetchedStats3.get.sizeInBytes == fetchedStats2.get.sizeInBytes)
      }
    }
  }

  testUpdatingTableStats(
    "data source table created in HiveExternalCatalog",
    "CREATE TABLE parquetTable (key STRING, value STRING) USING PARQUET")

  testUpdatingTableStats(
    "partitioned data source table",
    "CREATE TABLE parquetTable (key STRING, value STRING) USING PARQUET PARTITIONED BY (key)")

  /** Used to test refreshing cached metadata once table stats are updated. */
  private def getStatsBeforeAfterUpdate(isAnalyzeColumns: Boolean)
    : (CatalogStatistics, CatalogStatistics) = {
    val tableName = "tbl"
    var statsBeforeUpdate: CatalogStatistics = null
    var statsAfterUpdate: CatalogStatistics = null
    withTable(tableName) {
      val tableIndent = TableIdentifier(tableName, Some("default"))
      val catalog = spark.sessionState.catalog.asInstanceOf[HiveSessionCatalog]
      sql(s"CREATE TABLE $tableName (key int) USING PARQUET")
      sql(s"INSERT INTO $tableName SELECT 1")
      if (isAnalyzeColumns) {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS key")
      } else {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      }
      // Table lookup will make the table cached.
      spark.table(tableIndent)
      statsBeforeUpdate = catalog.metastoreCatalog.getCachedDataSourceTable(tableIndent)
        .asInstanceOf[LogicalRelation].catalogTable.get.stats.get

      sql(s"INSERT INTO $tableName SELECT 2")
      if (isAnalyzeColumns) {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS key")
      } else {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      }
      spark.table(tableIndent)
      statsAfterUpdate = catalog.metastoreCatalog.getCachedDataSourceTable(tableIndent)
        .asInstanceOf[LogicalRelation].catalogTable.get.stats.get
    }
    (statsBeforeUpdate, statsAfterUpdate)
  }

  test("test refreshing table stats of cached data source table by `ANALYZE TABLE` statement") {
    val (statsBeforeUpdate, statsAfterUpdate) = getStatsBeforeAfterUpdate(isAnalyzeColumns = false)

    assert(statsBeforeUpdate.sizeInBytes > 0)
    assert(statsBeforeUpdate.rowCount == Some(1))

    assert(statsAfterUpdate.sizeInBytes > statsBeforeUpdate.sizeInBytes)
    assert(statsAfterUpdate.rowCount == Some(2))
  }

  test("estimates the size of a test Hive serde tables") {
    val df = sql("""SELECT * FROM src""")
    val sizes = df.queryExecution.analyzed.collect {
      case relation: HiveTableRelation => relation.stats.sizeInBytes
    }
    assert(sizes.size === 1, s"Size wrong for:\n ${df.queryExecution}")
    assert(sizes(0).equals(BigInt(5812)),
      s"expected exact size 5812 for test table 'src', got: ${sizes(0)}")
  }

  test("auto converts to broadcast hash join, by size estimate of a relation") {
    def mkTest(
        before: () => Unit,
        after: () => Unit,
        query: String,
        expectedAnswer: Seq[Row],
        ct: ClassTag[_]): Unit = {
      before()

      var df = sql(query)

      // Assert src has a size smaller than the threshold.
      val sizes = df.queryExecution.analyzed.collect {
        case r if ct.runtimeClass.isAssignableFrom(r.getClass) => r.stats.sizeInBytes
      }
      assert(sizes.size === 2 && sizes(0) <= spark.sessionState.conf.autoBroadcastJoinThreshold
        && sizes(1) <= spark.sessionState.conf.autoBroadcastJoinThreshold,
        s"query should contain two relations, each of which has size smaller than autoConvertSize")

      // Using `sparkPlan` because for relevant patterns in HashJoin to be
      // matched, other strategies need to be applied.
      var bhj = df.queryExecution.sparkPlan.collect { case j: BroadcastHashJoinExec => j }
      assert(bhj.size === 1,
        s"actual query plans do not contain broadcast join: ${df.queryExecution}")

      checkAnswer(df, expectedAnswer) // check correctness of output

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        df = sql(query)
        bhj = df.queryExecution.sparkPlan.collect { case j: BroadcastHashJoinExec => j }
        assert(bhj.isEmpty, "BroadcastHashJoin still planned even though it is switched off")

        val shj = df.queryExecution.sparkPlan.collect { case j: SortMergeJoinExec => j }
        assert(shj.size === 1,
          "SortMergeJoin should be planned when BroadcastHashJoin is turned off")
      }
      after()
    }

    /** Tests for Hive serde tables */
    val metastoreQuery = """SELECT * FROM src a JOIN src b ON a.key = 238 AND a.key = b.key"""
    val metastoreAnswer = Seq.fill(4)(Row(238, "val_238", 238, "val_238"))
    mkTest(
      () => (),
      () => (),
      metastoreQuery,
      metastoreAnswer,
      implicitly[ClassTag[HiveTableRelation]]
    )
  }

  test("auto converts to broadcast left semi join, by size estimate of a relation") {
    val leftSemiJoinQuery =
      """SELECT * FROM src a
        |left semi JOIN src b ON a.key=86 and a.key = b.key""".stripMargin
    val answer = Row(86, "val_86")

    var df = sql(leftSemiJoinQuery)

    // Assert src has a size smaller than the threshold.
    val sizes = df.queryExecution.analyzed.collect {
      case relation: HiveTableRelation => relation.stats.sizeInBytes
    }
    assert(sizes.size === 2 && sizes(1) <= spark.sessionState.conf.autoBroadcastJoinThreshold
      && sizes(0) <= spark.sessionState.conf.autoBroadcastJoinThreshold,
      s"query should contain two relations, each of which has size smaller than autoConvertSize")

    // Using `sparkPlan` because for relevant patterns in HashJoin to be
    // matched, other strategies need to be applied.
    var bhj = df.queryExecution.sparkPlan.collect {
      case j: BroadcastHashJoinExec => j
    }
    assert(bhj.size === 1,
      s"actual query plans do not contain broadcast join: ${df.queryExecution}")

    checkAnswer(df, answer) // check correctness of output

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      df = sql(leftSemiJoinQuery)
      bhj = df.queryExecution.sparkPlan.collect {
        case j: BroadcastHashJoinExec => j
      }
      assert(bhj.isEmpty, "BroadcastHashJoin still planned even though it is switched off")

      val shj = df.queryExecution.sparkPlan.collect {
        case j: SortMergeJoinExec => j
      }
      assert(shj.size === 1,
        "SortMergeJoinExec should be planned when BroadcastHashJoin is turned off")
    }
  }

  test("Deals with wrong Hive's statistics (zero rowCount)") {
    withTable("maybe_big") {
      sql("CREATE TABLE maybe_big (c1 bigint)" +
        "TBLPROPERTIES ('numRows'='0', 'rawDataSize'='60000000000', 'totalSize'='8000000000000')")

      val catalogTable = getCatalogTable("maybe_big")

      val properties = catalogTable.ignoredProperties
      assert(properties("totalSize").toLong > 0)
      assert(properties("rawDataSize").toLong > 0)
      assert(properties("numRows").toLong == 0)

      val catalogStats = catalogTable.stats.get
      assert(catalogStats.sizeInBytes > 0)
      assert(catalogStats.rowCount.isEmpty)
    }
  }

  test(s"CTAS should update statistics if ${SQLConf.AUTO_SIZE_UPDATE_ENABLED.key} is enabled") {
    val tableName = "SPARK_23263"
    Seq(false, true).foreach { isConverted =>
      Seq(false, true).foreach { updateEnabled =>
        withSQLConf(
          SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> updateEnabled.toString,
          HiveUtils.CONVERT_METASTORE_PARQUET.key -> isConverted.toString) {
          withTable(tableName) {
            sql(s"CREATE TABLE $tableName STORED AS parquet AS SELECT 'a', 'b'")
            val catalogTable = getCatalogTable(tableName)
            // Hive serde tables always update statistics by Hive metastore
            if (!isConverted || updateEnabled) {
              assert(catalogTable.stats.nonEmpty)
            } else {
              assert(catalogTable.stats.isEmpty)
            }
          }
        }
      }
    }
  }

  test("SPARK-28518 fix getDataSize refer to ChecksumFileSystem#isChecksumFile") {
    withTempDir { tempDir =>
      withTable("t1") {
        spark.range(5).write.mode(SaveMode.Overwrite).parquet(tempDir.getCanonicalPath)
        Utils.tryWithResource(new PrintWriter(new File(s"$tempDir/temp.crc"))) { writer =>
          writer.write("1,2")
        }

        spark.sql(
          s"""
             |CREATE EXTERNAL TABLE t1(id BIGINT)
             |STORED AS parquet
             |LOCATION '${tempDir.getCanonicalPath}'
             |TBLPROPERTIES (
             |'rawDataSize'='-1', 'numFiles'='0', 'totalSize'='0',
             |'COLUMN_STATS_ACCURATE'='false', 'numRows'='-1'
             |)""".stripMargin)

        spark.sql("REFRESH TABLE t1")
        val relation1 = spark.table("t1").queryExecution.analyzed.children.head
        // After SPARK-28573, sizeInBytes should be the actual size.
        assert(relation1.stats.sizeInBytes === getDataSize(tempDir))

        spark.sql("REFRESH TABLE t1")
        withSQLConf(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key -> "true") {
          val relation2 = spark.table("t1").queryExecution.analyzed.children.head
          assert(relation2.stats.sizeInBytes === getDataSize(tempDir))
        }
      }
    }
  }

  test("fallBackToHdfs should not support Hive partitioned table") {
    Seq(true, false).foreach { fallBackToHdfs =>
      withSQLConf(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key -> s"$fallBackToHdfs",
        HiveUtils.CONVERT_METASTORE_PARQUET.key -> "false") {
        withTempDir { dir =>
          val tableDir = new File(dir, "table")
          val partitionDir = new File(dir, "partition")
          withTable("spark_28876") {
            sql(
              s"""
                 |CREATE TABLE spark_28876(id bigint)
                 |PARTITIONED BY (ds STRING)
                 |STORED AS PARQUET
                 |LOCATION '${tableDir.toURI}'
             """.stripMargin)

            spark.range(5).write.mode(SaveMode.Overwrite).parquet(partitionDir.getCanonicalPath)
            sql(s"ALTER TABLE spark_28876 ADD PARTITION (ds='p1') LOCATION '$partitionDir'")

            assert(getCatalogTable("spark_28876").stats.isEmpty)
            val sizeInBytes = spark.table("spark_28876").queryExecution.analyzed.children.head
              .asInstanceOf[HiveTableRelation].stats.sizeInBytes
            assert(sizeInBytes === conf.defaultSizeInBytes)
            assert(spark.table("spark_28876").count() === 5)
          }
        }
      }
    }
  }

  test("SPARK-30269 failed to update partition stats if it's equal to table's old stats") {
    val tbl = "SPARK_30269"
    val ext_tbl = "SPARK_30269_external"
    withTempDir { dir =>
      withTable(tbl, ext_tbl) {
        sql(s"CREATE TABLE $tbl (key INT, value STRING, ds STRING)" +
          "USING parquet PARTITIONED BY (ds)")
        sql(
          s"""
             | CREATE TABLE $ext_tbl (key INT, value STRING, ds STRING)
             | USING PARQUET
             | PARTITIONED BY (ds)
             | LOCATION '${dir.toURI}'
           """.stripMargin)

        Seq(tbl, ext_tbl).foreach { tblName =>
          sql(s"INSERT INTO $tblName VALUES (1, 'a', '2019-12-13')")

          val expectedSize = 690
          // analyze table
          sql(s"ANALYZE TABLE $tblName COMPUTE STATISTICS NOSCAN")
          var tableStats = getTableStats(tblName)
          assert(tableStats.sizeInBytes == expectedSize)
          assert(tableStats.rowCount.isEmpty)

          sql(s"ANALYZE TABLE $tblName COMPUTE STATISTICS")
          tableStats = getTableStats(tblName)
          assert(tableStats.sizeInBytes == expectedSize)
          assert(tableStats.rowCount.get == 1)

          // analyze a single partition
          sql(s"ANALYZE TABLE $tblName PARTITION (ds='2019-12-13') COMPUTE STATISTICS NOSCAN")
          var partStats = getPartitionStats(tblName, Map("ds" -> "2019-12-13"))
          assert(partStats.sizeInBytes == expectedSize)
          assert(partStats.rowCount.isEmpty)

          sql(s"ANALYZE TABLE $tblName PARTITION (ds='2019-12-13') COMPUTE STATISTICS")
          partStats = getPartitionStats(tblName, Map("ds" -> "2019-12-13"))
          assert(partStats.sizeInBytes == expectedSize)
          assert(partStats.rowCount.get == 1)
        }
      }
    }
  }

  test("SPARK-38573: partition stats auto update for dynamic partitions") {
    val table = "partition_stats_dynamic_partition"
    Seq("hive", "parquet").foreach { source =>
      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> "true") {
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, sp INT, dp INT) USING $source PARTITIONED BY (sp, dp)")
          sql(s"INSERT INTO $table PARTITION (sp=0, dp) VALUES (0, 0)")
          sql(s"INSERT OVERWRITE TABLE $table PARTITION (sp=0, dp) SELECT id, id FROM range(5)")
          for (i <- 0 until 5) {
            val partStats = getPartitionStats(table, Map("sp" -> s"0", "dp" -> s"$i"))
            assert(partStats.sizeInBytes > 0)
          }
        }
      }
    }
  }

  test("SPARK-38573: change partition stats after load/set/truncate data command") {
    val table = "partition_stats_load_set_truncate"
    withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> "true") {
      withTable(table) {
        sql(s"CREATE TABLE $table (i INT, j STRING) USING hive " +
          "PARTITIONED BY (ds STRING, hr STRING)")

        withTempPaths(numPaths = 2) { case Seq(dir1, dir2) =>
          val partDir1 = new File(new File(dir1, "ds=2008-04-09"), "hr=11")
          val file1 = new File(partDir1, "data")
          file1.getParentFile.mkdirs()
          Utils.tryWithResource(new PrintWriter(file1)) { writer =>
            writer.write("1,a")
          }

          val partDir2 = new File(new File(dir2, "ds=2008-04-09"), "hr=12")
          val file2 = new File(partDir2, "data")
          file2.getParentFile.mkdirs()
          Utils.tryWithResource(new PrintWriter(file2)) { writer =>
            writer.write("1,a")
          }

          sql(s"""
            |LOAD DATA INPATH '${file1.toURI.toString}' INTO TABLE $table
            |PARTITION (ds='2008-04-09', hr='11')
            """.stripMargin)
          sql(s"ALTER TABLE $table ADD PARTITION (ds='2008-04-09', hr='12')")
          sql(s"""
            |ALTER TABLE $table PARTITION (ds='2008-04-09', hr='12')
            |SET LOCATION '${partDir2.toURI.toString}'
            |""".stripMargin)
          val partStats1 = getPartitionStats(table, Map("ds" -> "2008-04-09", "hr" -> "11"))
          assert(partStats1.sizeInBytes > 0)
          val partStats2 = getPartitionStats(table, Map("ds" -> "2008-04-09", "hr" -> "12"))
          assert(partStats2.sizeInBytes > 0)


          sql(s"TRUNCATE TABLE $table PARTITION (ds='2008-04-09', hr='11')")
          val partStats3 = getPartitionStats(table, Map("ds" -> "2008-04-09", "hr" -> "11"))
          assert(partStats3.sizeInBytes == 0)
          val partStats4 = getPartitionStats(table, Map("ds" -> "2008-04-09", "hr" -> "12"))
          assert(partStats4.sizeInBytes > 0)
          sql(s"TRUNCATE TABLE $table")
          val partStats5 = getPartitionStats(table, Map("ds" -> "2008-04-09", "hr" -> "12"))
          assert(partStats5.sizeInBytes == 0)
        }
      }
    }
  }

  test("Don't support MapType") {
    val tableName = "analyzeTable_column"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value MAP<STRING, STRING>) " +
        s"PARTITIONED BY (ds STRING)")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS value")
        },
        condition = "UNSUPPORTED_FEATURE.ANALYZE_UNSUPPORTED_COLUMN_TYPE",
        parameters = Map(
          "columnType" -> "\"MAP<STRING, STRING>\"",
          "columnName" -> "`value`",
          "tableName" -> "`spark_catalog`.`default`.`analyzetable_column`"
        )
      )
    }
  }
}
