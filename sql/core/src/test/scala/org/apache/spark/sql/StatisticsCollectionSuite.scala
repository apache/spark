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

import java.io.{File, PrintWriter}
import java.net.URI
import java.util.TimeZone
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{DateTimeTestUtils, DateTimeUtils}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{withDefaultTimeZone, PST, UTC}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, TimeZoneUTC}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.test.SQLTestData.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * End-to-end suite testing statistics collection and use on both entire table and columns.
 */
class StatisticsCollectionSuite extends StatisticsCollectionTestBase with SharedSparkSession {
  import testImplicits._

  test("estimates the size of a limit 0 on outer join") {
    withTempView("test") {
      Seq(("one", 1), ("two", 2), ("three", 3), ("four", 4)).toDF("k", "v")
        .createOrReplaceTempView("test")
      val df1 = spark.table("test")
      val df2 = spark.table("test").limit(0)
      val df = df1.join(df2, Seq("k"), "left")

      val sizes = df.queryExecution.analyzed.collect { case g: Join =>
        g.stats.sizeInBytes
      }

      assert(sizes.size === 1, s"number of Join nodes is wrong:\n ${df.queryExecution}")
      assert(sizes.head === BigInt(128),
        s"expected exact size 96 for table 'test', got: ${sizes.head}")
    }
  }

  test("analyzing views is not supported") {
    val tableName = "tbl"
    withTable(tableName) {
      spark.range(10).write.saveAsTable(tableName)
      val viewName = "view"
      withView(viewName) {
        sql(s"CREATE VIEW $viewName AS SELECT * FROM $tableName")
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"ANALYZE TABLE $viewName COMPUTE STATISTICS")
          },
          errorClass = "UNSUPPORTED_FEATURE.ANALYZE_VIEW",
          parameters = Map.empty
        )
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"ANALYZE TABLE $viewName COMPUTE STATISTICS FOR COLUMNS id")
          },
          errorClass = "UNSUPPORTED_FEATURE.ANALYZE_VIEW",
          parameters = Map.empty
        )
      }
    }
  }

  test("statistics collection of a table with zero column") {
    val table_no_cols = "table_no_cols"
    withTable(table_no_cols) {
      val rddNoCols = sparkContext.parallelize(1 to 10).map(_ => Row.empty)
      val dfNoCols = spark.createDataFrame(rddNoCols, StructType(Seq.empty))
      dfNoCols.write.format("json").saveAsTable(table_no_cols)
      sql(s"ANALYZE TABLE $table_no_cols COMPUTE STATISTICS")
      checkTableStats(table_no_cols, hasSizeInBytes = true, expectedRowCounts = Some(10))
    }
  }

  test("analyze empty table") {
    val table = "emptyTable"
    withTable(table) {
      val df = Seq.empty[Int].toDF("key")
      df.write.format("json").saveAsTable(table)
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS noscan")
      val fetchedStats1 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
      assert(fetchedStats1.get.sizeInBytes == 0)
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS")
      val fetchedStats2 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(0))
      assert(fetchedStats2.get.sizeInBytes == 0)

      val expectedColStat =
        "key" -> CatalogColumnStat(Some(0), None, None, Some(0),
          Some(IntegerType.defaultSize), Some(IntegerType.defaultSize))

      // There won't be histogram for empty column.
      Seq("true", "false").foreach { histogramEnabled =>
        withSQLConf(SQLConf.HISTOGRAM_ENABLED.key -> histogramEnabled) {
          checkColStats(df, mutable.LinkedHashMap(expectedColStat))
        }
      }
    }
  }

  test("analyze column command - unsupported types and invalid columns") {
    val tableName = "column_stats_test1"
    withTable(tableName) {
      Seq(ArrayData(Seq(1, 2, 3), Seq(Seq(1, 2, 3)))).toDF().write.saveAsTable(tableName)

      // Test unsupported data types
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS data")
        },
        errorClass = "UNSUPPORTED_FEATURE.ANALYZE_UNSUPPORTED_COLUMN_TYPE",
        parameters = Map(
          "columnType" -> "\"ARRAY<INT>\"",
          "columnName" -> "`data`",
          "tableName" -> "`spark_catalog`.`default`.`column_stats_test1`"
        )
      )

      // Test invalid columns
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS some_random_column")
        },
        errorClass = "COLUMN_NOT_FOUND",
        parameters = Map(
          "colName" -> "`some_random_column`",
          "caseSensitiveConfig" -> "\"spark.sql.caseSensitive\""
        )
      )
    }
  }

  test("test table-level statistics for data source table") {
    val tableName = "tbl"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(i INT, j STRING) USING parquet")
      Seq(1 -> "a", 2 -> "b").toDF("i", "j").write.mode("overwrite").insertInto(tableName)

      // noscan won't count the number of rows
      sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS noscan")
      checkTableStats(tableName, hasSizeInBytes = true, expectedRowCounts = None)

      // without noscan, we count the number of rows
      sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      checkTableStats(tableName, hasSizeInBytes = true, expectedRowCounts = Some(2))
    }
  }

  test("SPARK-15392: DataFrame created from RDD should not be broadcasted") {
    val rdd = sparkContext.range(1, 100).map(i => Row(i, i))
    val df = spark.createDataFrame(rdd, new StructType().add("a", LongType).add("b", LongType))
    assert(df.queryExecution.analyzed.stats.sizeInBytes >
      spark.sessionState.conf.autoBroadcastJoinThreshold)
    assert(df.selectExpr("a").queryExecution.analyzed.stats.sizeInBytes >
      spark.sessionState.conf.autoBroadcastJoinThreshold)
  }

  test("column stats round trip serialization") {
    // Make sure we serialize and then deserialize and we will get the result data
    val df = data.toDF(stats.keys.toSeq :+ "carray" : _*)
    Seq(stats, statsWithHgms).foreach { s =>
      s.zip(df.schema).foreach { case ((k, v), field) =>
        withClue(s"column $k with type ${field.dataType}") {
          val roundtrip = CatalogColumnStat.fromMap("table_is_foo", field.name, v.toMap(k))
          assert(roundtrip == Some(v))
        }
      }
    }
  }

  test("SPARK-33812: column stats round trip serialization with splitting histogram property") {
    withSQLConf(SQLConf.HIVE_TABLE_PROPERTY_LENGTH_THRESHOLD.key -> "10") {
      statsWithHgms.foreach { case (k, v) =>
        val roundtrip = CatalogColumnStat.fromMap("t", k, v.toMap(k))
        assert(roundtrip == Some(v))
      }
    }
  }

  test("analyze column command - result verification") {
    // (data.head.productArity - 1) because the last column does not support stats collection.
    assert(stats.size == data.head.productArity - 1)
    val df = data.toDF(stats.keys.toSeq :+ "carray" : _*)
    checkColStats(df, stats)

    // test column stats with histograms
    withSQLConf(SQLConf.HISTOGRAM_ENABLED.key -> "true", SQLConf.HISTOGRAM_NUM_BINS.key -> "2") {
      checkColStats(df, statsWithHgms)
    }
  }

  test("column stats collection for null columns") {
    val dataTypes: Seq[(DataType, Int)] = Seq(
      BooleanType, ByteType, ShortType, IntegerType, LongType,
      DoubleType, FloatType, DecimalType.SYSTEM_DEFAULT,
      StringType, BinaryType, DateType, TimestampType
    ).zipWithIndex

    val df = sql("select " + dataTypes.map { case (tpe, idx) =>
      s"cast(null as ${tpe.sql}) as col$idx"
    }.mkString(", "))

    val expectedColStats = dataTypes.map { case (tpe, idx) =>
      (s"col$idx", CatalogColumnStat(Some(0), None, None, Some(1),
        Some(tpe.defaultSize.toLong), Some(tpe.defaultSize.toLong)))
    }

    // There won't be histograms for null columns.
    Seq("true", "false").foreach { histogramEnabled =>
      withSQLConf(SQLConf.HISTOGRAM_ENABLED.key -> histogramEnabled) {
        checkColStats(df, mutable.LinkedHashMap(expectedColStats: _*))
      }
    }
  }

  test("SPARK-25028: column stats collection for null partitioning columns") {
    val table = "analyze_partition_with_null"
    withTempDir { dir =>
      withTable(table) {
        sql(s"""
             |CREATE TABLE $table (value string, name string)
             |USING PARQUET
             |PARTITIONED BY (name)
             |LOCATION '${dir.toURI}'""".stripMargin)
        val df = Seq(("a", null), ("b", null)).toDF("value", "name")
        df.write.mode("overwrite").insertInto(table)
        sql(s"ANALYZE TABLE $table PARTITION (name) COMPUTE STATISTICS")
        val partitions = spark.sessionState.catalog.listPartitions(TableIdentifier(table))
        assert(partitions.head.stats.get.rowCount.get == 2)
      }
    }
  }

  test("number format in statistics") {
    val numbers = Seq(
      BigInt(0) -> (("0.0 B", "0")),
      BigInt(100) -> (("100.0 B", "100")),
      BigInt(2047) -> (("2047.0 B", "2.05E+3")),
      BigInt(2048) -> (("2.0 KiB", "2.05E+3")),
      BigInt(3333333) -> (("3.2 MiB", "3.33E+6")),
      BigInt(4444444444L) -> (("4.1 GiB", "4.44E+9")),
      BigInt(5555555555555L) -> (("5.1 TiB", "5.56E+12")),
      BigInt(6666666666666666L) -> (("5.9 PiB", "6.67E+15")),
      BigInt(1L << 10 ) * (1L << 60) -> (("1024.0 EiB", "1.18E+21")),
      BigInt(1L << 11) * (1L << 60) -> (("2.36E+21 B", "2.36E+21"))
    )
    numbers.foreach { case (input, (expectedSize, expectedRows)) =>
      val stats = Statistics(sizeInBytes = input, rowCount = Some(input))
      val expectedString = s"sizeInBytes=$expectedSize, rowCount=$expectedRows"
      assert(stats.simpleString == expectedString)
    }
  }

  test("change stats after set location command") {
    val table = "change_stats_set_location_table"
    val tableLoc = new File(spark.sessionState.catalog.defaultTablePath(TableIdentifier(table)))
    Seq(false, true).foreach { autoUpdate =>
      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> autoUpdate.toString) {
        withTable(table) {
          spark.range(100).select($"id", $"id" % 5 as "value").write.saveAsTable(table)
          // analyze to get initial stats
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS id, value")
          val fetched1 = checkTableStats(
            table, hasSizeInBytes = true, expectedRowCounts = Some(100))
          assert(fetched1.get.sizeInBytes > 0)
          assert(fetched1.get.colStats.size == 2)

          // set location command
          val initLocation = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table))
            .storage.locationUri.get.toString
          withTempDir { newLocation =>
            sql(s"ALTER TABLE $table SET LOCATION '${newLocation.toURI.toString}'")
            if (autoUpdate) {
              val fetched2 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
              assert(fetched2.get.sizeInBytes == 0)
              assert(fetched2.get.colStats.isEmpty)

              // set back to the initial location
              sql(s"ALTER TABLE $table SET LOCATION '$initLocation'")
              val fetched3 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
              assert(fetched3.get.sizeInBytes == fetched1.get.sizeInBytes)
            } else {
              checkTableStats(table, hasSizeInBytes = false, expectedRowCounts = None)
              // SPARK-19724: clean up the previous table location.
              waitForTasksToFinish()
              Utils.deleteRecursively(tableLoc)
            }
          }
        }
      }
    }
  }

  test("change stats after insert command for datasource table") {
    val table = "change_stats_insert_datasource_table"
    Seq(false, true).foreach { autoUpdate =>
      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> autoUpdate.toString) {
        withTable(table) {
          sql(s"CREATE TABLE $table (i int, j string) USING PARQUET")
          // analyze to get initial stats
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS i, j")
          val fetched1 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(0))
          assert(fetched1.get.sizeInBytes == 0)
          assert(fetched1.get.colStats.size == 2)

          // table lookup will make the table cached
          spark.table(table)
          assert(isTableInCatalogCache(table))

          // insert into command
          sql(s"INSERT INTO TABLE $table SELECT 1, 'abc'")
          if (autoUpdate) {
            val fetched2 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
            assert(fetched2.get.sizeInBytes > 0)
            assert(fetched2.get.colStats.isEmpty)
          } else {
            checkTableStats(table, hasSizeInBytes = false, expectedRowCounts = None)
          }

          // check that tableRelationCache inside the catalog was invalidated after insert
          assert(!isTableInCatalogCache(table))
        }
      }
    }
  }

  test("auto gather stats after insert command") {
    val table = "change_stats_insert_datasource_table"
    Seq(false, true).foreach { autoUpdate =>
      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> autoUpdate.toString) {
        withTable(table) {
          sql(s"CREATE TABLE $table (i int, j string) USING PARQUET")
          // insert into command
          sql(s"INSERT INTO TABLE $table SELECT 1, 'abc'")
          val stats = getCatalogTable(table).stats
          if (autoUpdate) {
            assert(stats.isDefined)
            assert(stats.get.sizeInBytes >= 0)
          } else {
            assert(stats.isEmpty)
          }
        }
      }
    }
  }

  test("invalidation of tableRelationCache after inserts") {
    val table = "invalidate_catalog_cache_table"
    Seq(false, true).foreach { autoUpdate =>
      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> autoUpdate.toString) {
        withTable(table) {
          spark.range(100).write.saveAsTable(table)
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS")
          spark.table(table)
          val initialSizeInBytes = getTableFromCatalogCache(table).stats.sizeInBytes
          spark.range(100).write.mode(SaveMode.Append).saveAsTable(table)
          spark.table(table)
          assert(getTableFromCatalogCache(table).stats.sizeInBytes == 2 * initialSizeInBytes)
        }
      }
    }
  }

  test("invalidation of tableRelationCache after alter table add partition") {
    val table = "invalidate_catalog_cache_table"
    Seq(false, true).foreach { autoUpdate =>
      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> autoUpdate.toString) {
        withTempDir { dir =>
          withTable(table) {
            val path = dir.getCanonicalPath
            sql(s"""
              |CREATE TABLE $table (col1 int, col2 int)
              |USING PARQUET
              |PARTITIONED BY (col2)
              |LOCATION '${dir.toURI}'""".stripMargin)
            sql(s"ANALYZE TABLE $table COMPUTE STATISTICS")
            spark.table(table)
            assert(getTableFromCatalogCache(table).stats.sizeInBytes == 0)
            spark.catalog.recoverPartitions(table)
            val df = Seq((1, 2), (1, 2)).toDF("col2", "col1")
            df.write.parquet(s"$path/col2=1")
            sql(s"ALTER TABLE $table ADD PARTITION (col2=1) LOCATION '${dir.toURI}'")
            spark.table(table)
            val cachedTable = getTableFromCatalogCache(table)
            val cachedTableSizeInBytes = cachedTable.stats.sizeInBytes
            val defaultSizeInBytes = conf.defaultSizeInBytes
            if (autoUpdate) {
              assert(cachedTableSizeInBytes != defaultSizeInBytes && cachedTableSizeInBytes > 0)
            } else {
              assert(cachedTableSizeInBytes == defaultSizeInBytes)
            }
          }
        }
      }
    }
  }

  test("Simple queries must be working, if CBO is turned on") {
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true") {
      withTable("TBL1", "TBL") {
        import org.apache.spark.sql.functions._
        val df = spark.range(1000L).select($"id",
          $"id" * 2 as "FLD1",
          $"id" * 12 as "FLD2",
          lit(null).cast(DoubleType) + $"id" as "fld3")
        df.write
          .mode(SaveMode.Overwrite)
          .bucketBy(10, "id", "FLD1", "FLD2")
          .sortBy("id", "FLD1", "FLD2")
          .saveAsTable("TBL")
        sql("ANALYZE TABLE TBL COMPUTE STATISTICS ")
        sql("ANALYZE TABLE TBL COMPUTE STATISTICS FOR COLUMNS ID, FLD1, FLD2, FLD3")
        val df2 = spark.sql(
          """
             |SELECT t1.id, t1.fld1, t1.fld2, t1.fld3
             |FROM tbl t1
             |JOIN tbl t2 on t1.id=t2.id
             |WHERE  t1.fld3 IN (-123.23,321.23)
          """.stripMargin)
        df2.createTempView("TBL2")
        sql("SELECT * FROM tbl2 WHERE fld3 IN (0,1)  ").queryExecution.executedPlan
      }
    }
  }

  test("store and retrieve column stats in different time zones") {
    val (start, end) = (0, TimeUnit.DAYS.toSeconds(2))

    def checkTimestampStats(
        t: DataType,
        srcTimeZone: TimeZone,
        dstTimeZone: TimeZone)(checker: ColumnStat => Unit): Unit = {
      val table = "time_table"
      val column = "T"
      val original = TimeZone.getDefault
      try {
        withTable(table) {
          TimeZone.setDefault(srcTimeZone)
          spark.range(start, end)
            .select(timestamp_seconds($"id").cast(t).as(column))
            .write.saveAsTable(table)
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS $column")

          TimeZone.setDefault(dstTimeZone)
          val stats = getCatalogTable(table)
            .stats.get.colStats(column).toPlanStat(column, t)
          checker(stats)
        }
      } finally {
        TimeZone.setDefault(original)
      }
    }

    DateTimeTestUtils.outstandingZoneIds.foreach { zid =>
      val timeZone = TimeZone.getTimeZone(zid)
      checkTimestampStats(DateType, TimeZoneUTC, timeZone) { stats =>
        assert(stats.min.get.asInstanceOf[Int] == TimeUnit.SECONDS.toDays(start))
        assert(stats.max.get.asInstanceOf[Int] == TimeUnit.SECONDS.toDays(end - 1))
      }
      checkTimestampStats(TimestampType, TimeZoneUTC, timeZone) { stats =>
        assert(stats.min.get.asInstanceOf[Long] == TimeUnit.SECONDS.toMicros(start))
        assert(stats.max.get.asInstanceOf[Long] == TimeUnit.SECONDS.toMicros(end - 1))
      }
    }
  }

  private def checkDescTimestampColStats(
      tableName: String,
      timestampColumn: String,
      expectedMinTimestamp: String,
      expectedMaxTimestamp: String): Unit = {

    def extractColumnStatsFromDesc(statsName: String, rows: Array[Row]): String = {
      rows.collect {
        case r: Row if r.getString(0) == statsName =>
          r.getString(1)
      }.head
    }

    val descTsCol = sql(s"DESC FORMATTED $tableName $timestampColumn").collect()
    assert(extractColumnStatsFromDesc("min", descTsCol) == expectedMinTimestamp)
    assert(extractColumnStatsFromDesc("max", descTsCol) == expectedMaxTimestamp)
  }

  test("SPARK-38140: describe column stats (min, max) for timestamp column: desc results should " +
    "be consistent with the written value if writing and desc happen in the same time zone") {

    val zoneIdAndOffsets =
      Seq((UTC, "+0000"), (PST, "-0800"), (getZoneId("Asia/Hong_Kong"), "+0800"))

    zoneIdAndOffsets.foreach { case (zoneId, offset) =>
      withDefaultTimeZone(zoneId) {
        val table = "insert_desc_same_time_zone"
        val tsCol = "timestamp_typed_col"
        withTable(table) {
          val minTimestamp = "make_timestamp(2022, 1, 1, 0, 0, 1.123456)"
          val maxTimestamp = "make_timestamp(2022, 1, 3, 0, 0, 2.987654)"
          sql(s"CREATE TABLE $table ($tsCol Timestamp) USING parquet")
          sql(s"INSERT INTO $table VALUES $minTimestamp, $maxTimestamp")
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR ALL COLUMNS")

          checkDescTimestampColStats(
            tableName = table,
            timestampColumn = tsCol,
            expectedMinTimestamp = "2022-01-01 00:00:01.123456 " + offset,
            expectedMaxTimestamp = "2022-01-03 00:00:02.987654 " + offset)
        }
      }
    }
  }

  test("SPARK-38140: describe column stats (min, max) for timestamp column: desc should show " +
    "different results if writing in UTC and desc in other time zones") {

    val table = "insert_desc_diff_time_zones"
    val tsCol = "timestamp_typed_col"

    withDefaultTimeZone(UTC) {
      withTable(table) {
        val minTimestamp = "make_timestamp(2022, 1, 1, 0, 0, 1.123456)"
        val maxTimestamp = "make_timestamp(2022, 1, 3, 0, 0, 2.987654)"
        sql(s"CREATE TABLE $table ($tsCol Timestamp) USING parquet")
        sql(s"INSERT INTO $table VALUES $minTimestamp, $maxTimestamp")
        sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR ALL COLUMNS")

        checkDescTimestampColStats(
          tableName = table,
          timestampColumn = tsCol,
          expectedMinTimestamp = "2022-01-01 00:00:01.123456 +0000",
          expectedMaxTimestamp = "2022-01-03 00:00:02.987654 +0000")

        TimeZone.setDefault(DateTimeUtils.getTimeZone("PST"))
        checkDescTimestampColStats(
          tableName = table,
          timestampColumn = tsCol,
          expectedMinTimestamp = "2021-12-31 16:00:01.123456 -0800",
          expectedMaxTimestamp = "2022-01-02 16:00:02.987654 -0800")

        TimeZone.setDefault(DateTimeUtils.getTimeZone("Asia/Hong_Kong"))
        checkDescTimestampColStats(
          tableName = table,
          timestampColumn = tsCol,
          expectedMinTimestamp = "2022-01-01 08:00:01.123456 +0800",
          expectedMaxTimestamp = "2022-01-03 08:00:02.987654 +0800")
      }
    }
  }

  test("SPARK-42777: describe column stats (min, max) for timestamp_ntz column") {
    val table = "insert_desc_same_time_zone"
    val tsCol = "timestamp_ntz_typed_col"
    withTable(table) {
      val minTimestamp = "make_timestamp_ntz(2022, 1, 1, 0, 0, 1.123456)"
      val maxTimestamp = "make_timestamp_ntz(2022, 1, 3, 0, 0, 2.987654)"
      sql(s"CREATE TABLE $table ($tsCol timestamp_ntz) USING parquet")
      sql(s"INSERT INTO $table VALUES $minTimestamp, $maxTimestamp")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR ALL COLUMNS")

      checkDescTimestampColStats(
        tableName = table,
        timestampColumn = tsCol,
        expectedMinTimestamp = "2022-01-01 00:00:01.123456",
        expectedMaxTimestamp = "2022-01-03 00:00:02.987654")

      // Converting TimestampNTZ catalog stats to plan stats
      val columnStat = getCatalogTable(table)
        .stats.get.colStats(tsCol).toPlanStat(tsCol, TimestampNTZType)
      assert(columnStat.min.contains(1640995201123456L))
      assert(columnStat.max.contains(1641168002987654L))
    }
  }

  private def getStatAttrNames(tableName: String): Set[String] = {
    val queryStats = spark.table(tableName).queryExecution.optimizedPlan.stats.attributeStats
    queryStats.map(_._1.name).toSet
  }

  test("analyzes column statistics in cached query") {
    withTempView("cachedQuery") {
      sql(
        """CACHE TABLE cachedQuery AS
          |  SELECT c0, avg(c1) AS v1, avg(c2) AS v2
          |  FROM (SELECT id % 3 AS c0, id % 5 AS c1, 2 AS c2 FROM range(1, 30))
          |  GROUP BY c0
        """.stripMargin)

      // Analyzes one column in the cached logical plan
      sql("ANALYZE TABLE cachedQuery COMPUTE STATISTICS FOR COLUMNS v1")
      assert(getStatAttrNames("cachedQuery") === Set("v1"))

      // Analyzes two more columns
      sql("ANALYZE TABLE cachedQuery COMPUTE STATISTICS FOR COLUMNS c0, v2")
      assert(getStatAttrNames("cachedQuery")  === Set("c0", "v1", "v2"))
    }
  }

  test("analyzes column statistics in cached local temporary view") {
    withTempView("tempView") {
      // Analyzes in a temporary view
      sql("CREATE TEMPORARY VIEW tempView AS SELECT 1 id")
      checkError(
        exception = intercept[AnalysisException] {
          sql("ANALYZE TABLE tempView COMPUTE STATISTICS FOR COLUMNS id")
        },
        errorClass = "UNSUPPORTED_FEATURE.ANALYZE_UNCACHED_TEMP_VIEW",
        parameters = Map("viewName" -> "`tempView`")
      )

      // Cache the view then analyze it
      sql("CACHE TABLE tempView")
      assert(getStatAttrNames("tempView") !== Set("id"))
      sql("ANALYZE TABLE tempView COMPUTE STATISTICS FOR COLUMNS id")
      assert(getStatAttrNames("tempView") === Set("id"))
    }
  }

  test("analyzes column statistics in cached global temporary view") {
    withGlobalTempView("gTempView") {
      val globalTempDB = spark.sharedState.globalTempDB
      val e1 = intercept[AnalysisException] {
        sql(s"ANALYZE TABLE $globalTempDB.gTempView COMPUTE STATISTICS FOR COLUMNS id")
      }
      checkErrorTableNotFound(e1, s"`$globalTempDB`.`gTempView`",
        ExpectedContext(s"$globalTempDB.gTempView", 14, 13 + s"$globalTempDB.gTempView".length))
      // Analyzes in a global temporary view
      sql("CREATE GLOBAL TEMP VIEW gTempView AS SELECT 1 id")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ANALYZE TABLE $globalTempDB.gTempView COMPUTE STATISTICS FOR COLUMNS id")
        },
        errorClass = "UNSUPPORTED_FEATURE.ANALYZE_UNCACHED_TEMP_VIEW",
        parameters = Map("viewName" -> "`global_temp`.`gTempView`")
      )

      // Cache the view then analyze it
      sql(s"CACHE TABLE $globalTempDB.gTempView")
      assert(getStatAttrNames(s"$globalTempDB.gTempView") !== Set("id"))
      sql(s"ANALYZE TABLE $globalTempDB.gTempView COMPUTE STATISTICS FOR COLUMNS id")
      assert(getStatAttrNames(s"$globalTempDB.gTempView") === Set("id"))
    }
  }

  test("analyzes column statistics in cached catalog view") {
    withTempDatabase { database =>
      sql(s"CREATE VIEW $database.v AS SELECT 1 c")
      sql(s"CACHE TABLE $database.v")
      assert(getStatAttrNames(s"$database.v") !== Set("c"))
      sql(s"ANALYZE TABLE $database.v COMPUTE STATISTICS FOR COLUMNS c")
      assert(getStatAttrNames(s"$database.v") === Set("c"))
    }
  }

  test("analyzes table statistics in cached catalog view") {
    def getTableStats(tableName: String): Statistics = {
      spark.table(tableName).queryExecution.optimizedPlan.stats
    }

    withTempDatabase { database =>
      sql(s"CREATE VIEW $database.v AS SELECT 1 c")
      // Cache data eagerly by default, so this operation collects table stats
      sql(s"CACHE TABLE $database.v")
      val stats1 = getTableStats(s"$database.v")
      assert(stats1.sizeInBytes > 0)
      assert(stats1.rowCount === Some(1))
      sql(s"UNCACHE TABLE $database.v")

      // Cache data lazily, then analyze table stats
      sql(s"CACHE LAZY TABLE $database.v")
      val stats2 = getTableStats(s"$database.v")
      assert(stats2.sizeInBytes === OneRowRelation().computeStats().sizeInBytes)
      assert(stats2.rowCount === None)

      sql(s"ANALYZE TABLE $database.v COMPUTE STATISTICS NOSCAN")
      val stats3 = getTableStats(s"$database.v")
      assert(stats3.sizeInBytes === OneRowRelation().computeStats().sizeInBytes)
      assert(stats3.rowCount === None)

      sql(s"ANALYZE TABLE $database.v COMPUTE STATISTICS")
      val stats4 = getTableStats(s"$database.v")
      assert(stats4.sizeInBytes === stats1.sizeInBytes)
      assert(stats4.rowCount === Some(1))
    }
  }

  test(s"CTAS should update statistics if ${SQLConf.AUTO_SIZE_UPDATE_ENABLED.key} is enabled") {
    val tableName = "spark_27694"
    Seq(false, true).foreach { updateEnabled =>
      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> updateEnabled.toString) {
        withTable(tableName) {
          // Create a data source table using the result of a query.
          sql(s"CREATE TABLE $tableName USING parquet AS SELECT 'a', 'b'")
          val catalogTable = getCatalogTable(tableName)
          if (updateEnabled) {
            assert(catalogTable.stats.nonEmpty)
          } else {
            assert(catalogTable.stats.isEmpty)
          }
        }
      }
    }
  }

  test("Metadata files and temporary files should not be counted as data files") {
    withTempDir { tempDir =>
      val tableName = "t1"
      val stagingDirName = ".test-staging-dir"
      val tableLocation = s"${tempDir.toURI}/$tableName"
      withSQLConf(
        SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> "true",
        "hive.exec.stagingdir" -> stagingDirName) {
        withTable("t1") {
          sql(s"CREATE TABLE $tableName(c1 BIGINT) USING PARQUET LOCATION '$tableLocation'")
          sql(s"INSERT INTO TABLE $tableName VALUES(1)")

          val staging = new File(new URI(s"$tableLocation/$stagingDirName"))
          Utils.tryWithResource(new PrintWriter(staging)) { stagingWriter =>
            stagingWriter.write("12")
          }

          val metadata = new File(new URI(s"$tableLocation/_metadata"))
          Utils.tryWithResource(new PrintWriter(metadata)) { metadataWriter =>
            metadataWriter.write("1234")
          }

          sql(s"INSERT INTO TABLE $tableName VALUES(1)")

          val stagingFileSize = staging.length()
          val metadataFileSize = metadata.length()
          val tableLocationSize = getDataSize(new File(new URI(tableLocation)))

          val stats = checkTableStats(tableName, hasSizeInBytes = true, expectedRowCounts = None)
          assert(stats.get.sizeInBytes === tableLocationSize - stagingFileSize - metadataFileSize)
        }
      }
    }
  }

  Seq(true, false).foreach { caseSensitive =>
    test(s"SPARK-30903: Fail fast on duplicate columns when analyze columns " +
      s"- caseSensitive=$caseSensitive") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val table = "test_table"
        withTable(table) {
          sql(s"CREATE TABLE $table (value string, name string) USING PARQUET")
          val dupCol = if (caseSensitive) "value" else "VaLuE"
          checkError(
            exception = intercept[AnalysisException] {
              sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS value, name, $dupCol")
            },
            errorClass = "COLUMN_ALREADY_EXISTS",
            parameters = Map("columnName" -> "`value`"))
        }
      }
    }
  }

  test("SPARK-34119: Keep necessary stats after PruneFileSourcePartitions") {
    withTable("SPARK_34119") {
      withSQLConf(SQLConf.CBO_ENABLED.key -> "true") {
        sql(s"CREATE TABLE SPARK_34119 using parquet PARTITIONED BY (p) AS " +
          "(SELECT id, CAST(id % 5 AS STRING) AS p FROM range(10))")
        sql(s"ANALYZE TABLE SPARK_34119 COMPUTE STATISTICS FOR ALL COLUMNS")

        checkOptimizedPlanStats(sql(s"SELECT id FROM SPARK_34119"),
          160L,
          Some(10),
          Seq(ColumnStat(
            distinctCount = Some(10),
            min = Some(0),
            max = Some(9),
            nullCount = Some(0),
            avgLen = Some(LongType.defaultSize),
            maxLen = Some(LongType.defaultSize))))

        checkOptimizedPlanStats(sql("SELECT id FROM SPARK_34119 WHERE p = '2'"),
          32L,
          Some(2),
          Seq(ColumnStat(
            distinctCount = Some(2),
            min = Some(0),
            max = Some(9),
            nullCount = Some(0),
            avgLen = Some(LongType.defaultSize),
            maxLen = Some(LongType.defaultSize))))
      }
    }
  }

  test("SPARK-33687: analyze all tables in a specific database") {
    withTempDatabase { database =>
      spark.catalog.setCurrentDatabase(database)
      withTempDir { dir =>
        withTable("t1", "t2") {
          spark.range(10).write.saveAsTable("t1")
          sql(s"CREATE EXTERNAL TABLE t2 USING parquet LOCATION '${dir.toURI}' " +
            "AS SELECT * FROM range(20)")
          withView("v1", "v2") {
            sql("CREATE VIEW v1 AS SELECT 1 c1")
            sql("CREATE VIEW v2 AS SELECT 2 c2")
            sql("CACHE TABLE v1")
            sql("CACHE LAZY TABLE v2")

            sql(s"ANALYZE TABLES IN $database COMPUTE STATISTICS NOSCAN")
            checkTableStats("t1", hasSizeInBytes = true, expectedRowCounts = None)
            checkTableStats("t2", hasSizeInBytes = true, expectedRowCounts = None)
            assert(getCatalogTable("v1").stats.isEmpty)
            checkOptimizedPlanStats(spark.table("v1"), 4, Some(1), Seq.empty)
            checkOptimizedPlanStats(spark.table("v2"), 1, None, Seq.empty)

            sql("ANALYZE TABLES COMPUTE STATISTICS")
            checkTableStats("t1", hasSizeInBytes = true, expectedRowCounts = Some(10))
            checkTableStats("t2", hasSizeInBytes = true, expectedRowCounts = Some(20))
            checkOptimizedPlanStats(spark.table("v1"), 4, Some(1), Seq.empty)
            checkOptimizedPlanStats(spark.table("v2"), 4, Some(1), Seq.empty)
          }
        }
      }
    }

    val e = intercept[AnalysisException] {
      sql(s"ANALYZE TABLES IN db_not_exists COMPUTE STATISTICS")
    }
    checkError(e,
      errorClass = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> "`spark_catalog`.`db_not_exists`"))
  }

  test("SPARK-43383: Add rowCount statistics to LocalRelation") {
    withSQLConf("spark.sql.test.localRelationRowCount" -> "true") {
      val optimizedPlan = spark.sql("select * from values(1),(2),(3),(4),(5),(6)")
        .queryExecution.optimizedPlan
      assert(optimizedPlan.isInstanceOf[LocalRelation])

      val stats = optimizedPlan.stats
      assert(stats.rowCount.isDefined && stats.rowCount.get == 6)
    }
  }

  test("SPARK-39834: build the stats for LogicalRDD based on origin stats") {
    def buildExpectedColumnStats(attrs: Seq[Attribute]): AttributeMap[ColumnStat] = {
      AttributeMap(
        attrs.map {
          case attr if attr.dataType == BooleanType =>
            attr -> ColumnStat(
              distinctCount = Some(2),
              min = Some(false),
              max = Some(true),
              nullCount = Some(0),
              avgLen = Some(1),
              maxLen = Some(1))

          case attr if attr.dataType == ByteType =>
            attr -> ColumnStat(
              distinctCount = Some(2),
              min = Some(1),
              max = Some(2),
              nullCount = Some(0),
              avgLen = Some(1),
              maxLen = Some(1))

          case attr => attr -> ColumnStat()
        }
      )
    }

    val outputList = Seq(
      AttributeReference("cbool", BooleanType)(),
      AttributeReference("cbyte", ByteType)(),
      AttributeReference("cint", IntegerType)()
    )

    val expectedSize = 16
    val statsPlan = OutputListAwareStatsTestPlan(
      outputList = outputList,
      rowCount = 2,
      size = Some(expectedSize))

    withSQLConf(SQLConf.CBO_ENABLED.key -> "true") {
      val df = Dataset.ofRows(spark, statsPlan)
        // add some map-like operations which optimizer will optimize away, and make a divergence
        // for output between logical plan and optimized plan
        // logical plan
        // Project [cb#6 AS cbool#12, cby#7 AS cbyte#13, ci#8 AS cint#14]
        // +- Project [cbool#0 AS cb#6, cbyte#1 AS cby#7, cint#2 AS ci#8]
        //    +- OutputListAwareStatsTestPlan [cbool#0, cbyte#1, cint#2], 2, 16
        // optimized plan
        // OutputListAwareStatsTestPlan [cbool#0, cbyte#1, cint#2], 2, 16
        .selectExpr("cbool AS cb", "cbyte AS cby", "cint AS ci")
        .selectExpr("cb AS cbool", "cby AS cbyte", "ci AS cint")

      // We can't leverage LogicalRDD.fromDataset here, since it triggers physical planning and
      // there is no matching physical node for OutputListAwareStatsTestPlan.
      val optimizedPlan = df.queryExecution.optimizedPlan
      val rewrite = LogicalRDD.buildOutputAssocForRewrite(optimizedPlan.output,
        df.logicalPlan.output)
      val logicalRDD = LogicalRDD(
        df.logicalPlan.output, spark.sparkContext.emptyRDD[InternalRow], isStreaming = true)(
        spark, Some(LogicalRDD.rewriteStatistics(optimizedPlan.stats, rewrite.get)), None)

      val stats = logicalRDD.computeStats()
      val expectedStats = Statistics(sizeInBytes = expectedSize, rowCount = Some(2),
        attributeStats = buildExpectedColumnStats(logicalRDD.output))
      assert(stats === expectedStats)

      // This method re-issues expression IDs for all outputs. We expect column stats to be
      // reflected as well.
      val newLogicalRDD = logicalRDD.newInstance()
      val newStats = newLogicalRDD.computeStats()
      val newExpectedStats = Statistics(sizeInBytes = expectedSize, rowCount = Some(2),
        attributeStats = buildExpectedColumnStats(newLogicalRDD.output))
      assert(newStats === newExpectedStats)
    }
  }
}

/**
 * This class is used for unit-testing. It's a logical plan whose output and stats are passed in.
 */
case class OutputListAwareStatsTestPlan(
    outputList: Seq[Attribute],
    rowCount: BigInt,
    size: Option[BigInt] = None) extends LeafNode with MultiInstanceRelation {
  override def output: Seq[Attribute] = outputList
  override def computeStats(): Statistics = {
    val columnInfo = outputList.map { attr =>
      attr.dataType match {
        case BooleanType =>
          attr -> ColumnStat(
            distinctCount = Some(2),
            min = Some(false),
            max = Some(true),
            nullCount = Some(0),
            avgLen = Some(1),
            maxLen = Some(1))

        case ByteType =>
          attr -> ColumnStat(
            distinctCount = Some(2),
            min = Some(1),
            max = Some(2),
            nullCount = Some(0),
            avgLen = Some(1),
            maxLen = Some(1))

        case _ =>
          attr -> ColumnStat()
      }
    }
    val attrStats = AttributeMap(columnInfo)

    Statistics(
      // If sizeInBytes is useless in testing, we just use a fake value
      sizeInBytes = size.getOrElse(Int.MaxValue),
      rowCount = Some(rowCount),
      attributeStats = attrStats)
  }
  override def newInstance(): LogicalPlan = copy(outputList = outputList.map(_.newInstance()))
}
