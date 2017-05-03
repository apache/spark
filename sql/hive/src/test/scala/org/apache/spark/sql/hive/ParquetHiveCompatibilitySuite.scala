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

import java.io.File
import java.net.URLDecoder
import java.sql.Timestamp
import java.util.TimeZone

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import org.apache.spark.sql.{AnalysisException, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.parquet.{ParquetCompatibilityTest, ParquetFileFormat}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

class ParquetHiveCompatibilitySuite extends ParquetCompatibilityTest with TestHiveSingleton {
  /**
   * Set the staging directory (and hence path to ignore Parquet files under)
   * to the default value of hive.exec.stagingdir.
   */
  private val stagingDir = ".hive-staging"

  override protected def logParquetSchema(path: String): Unit = {
    val schema = readParquetSchema(path, { path =>
      !path.getName.startsWith("_") && !path.getName.startsWith(stagingDir)
    })

    logInfo(
      s"""Schema of the Parquet file written by parquet-avro:
         |$schema
       """.stripMargin)
  }

  private def testParquetHiveCompatibility(row: Row, hiveTypes: String*): Unit = {
    withTable("parquet_compat") {
      withTempPath { dir =>
        val path = dir.toURI.toString

        // Hive columns are always nullable, so here we append a all-null row.
        val rows = row :: Row(Seq.fill(row.length)(null): _*) :: Nil

        // Don't convert Hive metastore Parquet tables to let Hive write those Parquet files.
        withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> "false") {
          withTempView("data") {
            val fields = hiveTypes.zipWithIndex.map { case (typ, index) => s"  col_$index $typ" }

            val ddl =
              s"""CREATE TABLE parquet_compat(
                 |${fields.mkString(",\n")}
                 |)
                 |STORED AS PARQUET
                 |LOCATION '$path'
               """.stripMargin

            logInfo(
              s"""Creating testing Parquet table with the following DDL:
                 |$ddl
               """.stripMargin)

            spark.sql(ddl)

            val schema = spark.table("parquet_compat").schema
            val rowRDD = spark.sparkContext.parallelize(rows).coalesce(1)
            spark.createDataFrame(rowRDD, schema).createOrReplaceTempView("data")
            spark.sql("INSERT INTO TABLE parquet_compat SELECT * FROM data")
          }
        }

        logParquetSchema(path)

        // Unfortunately parquet-hive doesn't add `UTF8` annotation to BINARY when writing strings.
        // Have to assume all BINARY values are strings here.
        withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "true") {
          checkAnswer(spark.read.parquet(path), rows)
        }
      }
    }
  }

  test("simple primitives") {
    testParquetHiveCompatibility(
      Row(true, 1.toByte, 2.toShort, 3, 4.toLong, 5.1f, 6.1d, "foo"),
      "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING")
  }

  test("SPARK-10177 timestamp") {
    testParquetHiveCompatibility(Row(Timestamp.valueOf("2015-08-24 00:31:00")), "TIMESTAMP")
  }

  test("array") {
    testParquetHiveCompatibility(
      Row(
        Seq[Integer](1: Integer, null, 2: Integer, null),
        Seq[String]("foo", null, "bar", null),
        Seq[Seq[Integer]](
          Seq[Integer](1: Integer, null),
          Seq[Integer](2: Integer, null))),
      "ARRAY<INT>",
      "ARRAY<STRING>",
      "ARRAY<ARRAY<INT>>")
  }

  test("map") {
    testParquetHiveCompatibility(
      Row(
        Map[Integer, String](
          (1: Integer) -> "foo",
          (2: Integer) -> null)),
      "MAP<INT, STRING>")
  }

  // HIVE-11625: Parquet map entries with null keys are dropped by Hive
  ignore("map entries with null keys") {
    testParquetHiveCompatibility(
      Row(
        Map[Integer, String](
          null.asInstanceOf[Integer] -> "bar",
          null.asInstanceOf[Integer] -> null)),
      "MAP<INT, STRING>")
  }

  test("struct") {
    testParquetHiveCompatibility(
      Row(Row(1, Seq("foo", "bar", null))),
      "STRUCT<f0: INT, f1: ARRAY<STRING>>")
  }

  test("SPARK-16344: array of struct with a single field named 'array_element'") {
    testParquetHiveCompatibility(
      Row(Seq(Row(1))),
      "ARRAY<STRUCT<array_element: INT>>")
  }

  val testTimezones = Seq(
    "UTC" -> "UTC",
    "LA" -> "America/Los_Angeles",
    "Berlin" -> "Europe/Berlin"
  )
  // Check creating parquet tables with timestamps, writing data into them, and reading it back out
  // under a variety of conditions:
  // * tables with explicit tz and those without
  // * altering table properties directly
  // * variety of timezones, local & non-local
  val sessionTimezones = testTimezones.map(_._2).map(Some(_)) ++ Seq(None)
  sessionTimezones.foreach { sessionTzOpt =>
    val sparkSession = spark.newSession()
    sessionTzOpt.foreach { tz => sparkSession.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, tz) }
    testCreateWriteRead(sparkSession, "no_tz", None, sessionTzOpt)
    val localTz = TimeZone.getDefault.getID()
    testCreateWriteRead(sparkSession, "local", Some(localTz), sessionTzOpt)
    // check with a variety of timezones.  The unit tests currently are configured to always use
    // America/Los_Angeles, but even if they didn't, we'd be sure to cover a non-local timezone.
    testTimezones.foreach { case (tableName, zone) =>
      if (zone != localTz) {
        testCreateWriteRead(sparkSession, tableName, Some(zone), sessionTzOpt)
      }
    }
  }

  private def testCreateWriteRead(
      sparkSession: SparkSession,
      baseTable: String,
      explicitTz: Option[String],
      sessionTzOpt: Option[String]): Unit = {
    testCreateAlterTablesWithTimezone(sparkSession, baseTable, explicitTz, sessionTzOpt)
    testWriteTablesWithTimezone(sparkSession, baseTable, explicitTz, sessionTzOpt)
    testReadTablesWithTimezone(sparkSession, baseTable, explicitTz, sessionTzOpt)
  }

  private def checkHasTz(spark: SparkSession, table: String, tz: Option[String]): Unit = {
    val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table))
    assert(tableMetadata.properties.get(ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY) === tz)
  }

  private def testCreateAlterTablesWithTimezone(
      spark: SparkSession,
      baseTable: String,
      explicitTz: Option[String],
      sessionTzOpt: Option[String]): Unit = {
    test(s"SPARK-12297: Create and Alter Parquet tables and timezones; explicitTz = $explicitTz; " +
      s"sessionTzOpt = $sessionTzOpt") {
      val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
      withTable(baseTable, s"like_$baseTable", s"select_$baseTable", s"partitioned_$baseTable") {
        // If we ever add a property to set the table timezone by default, defaultTz would change
        val defaultTz = None
        // check that created tables have correct TBLPROPERTIES
        val tblProperties = explicitTz.map {
          tz => s"""TBLPROPERTIES ($key="$tz")"""
        }.getOrElse("")
        spark.sql(
          s"""CREATE TABLE $baseTable (
                |  x int
                | )
                | STORED AS PARQUET
                | $tblProperties
            """.stripMargin)
        val expectedTableTz = explicitTz.orElse(defaultTz)
        checkHasTz(spark, baseTable, expectedTableTz)
        spark.sql(
          s"""CREATE TABLE partitioned_$baseTable (
                |  x int
                | )
                | PARTITIONED BY (y int)
                | STORED AS PARQUET
                | $tblProperties
            """.stripMargin)
        checkHasTz(spark, s"partitioned_$baseTable", expectedTableTz)
        spark.sql(s"CREATE TABLE like_$baseTable LIKE $baseTable")
        checkHasTz(spark, s"like_$baseTable", expectedTableTz)
        spark.sql(
          s"""CREATE TABLE select_$baseTable
                | STORED AS PARQUET
                | AS
                | SELECT * from $baseTable
            """.stripMargin)
        checkHasTz(spark, s"select_$baseTable", defaultTz)

        // check alter table, setting, unsetting, resetting the property
        spark.sql(
          s"""ALTER TABLE $baseTable SET TBLPROPERTIES ($key="America/Los_Angeles")""")
        checkHasTz(spark, baseTable, Some("America/Los_Angeles"))
        spark.sql(s"""ALTER TABLE $baseTable SET TBLPROPERTIES ($key="UTC")""")
        checkHasTz(spark, baseTable, Some("UTC"))
        spark.sql(s"""ALTER TABLE $baseTable UNSET TBLPROPERTIES ($key)""")
        checkHasTz(spark, baseTable, None)
        explicitTz.foreach { tz =>
          spark.sql(s"""ALTER TABLE $baseTable SET TBLPROPERTIES ($key="$tz")""")
          checkHasTz(spark, baseTable, expectedTableTz)
        }
      }
    }
  }

  val desiredTimestampStrings = Seq(
    "2015-12-31 22:49:59.123",
    "2015-12-31 23:50:59.123",
    "2016-01-01 00:39:59.123",
    "2016-01-01 01:29:59.123"
  )
  // We don't want to mess with timezones inside the tests themselves, since we use a shared
  // spark context, and then we might be prone to issues from lazy vals for timezones.  Instead,
  // we manually adjust the timezone just to determine what the desired millis (since epoch, in utc)
  // is for various "wall-clock" times in different timezones, and then we can compare against those
  // in our tests.
  val timestampTimezoneToMillis = {
    val originalTz = TimeZone.getDefault
    try {
      desiredTimestampStrings.flatMap { timestampString =>
        Seq("America/Los_Angeles", "Europe/Berlin", "UTC").map { tzId =>
          TimeZone.setDefault(TimeZone.getTimeZone(tzId))
          val timestamp = Timestamp.valueOf(timestampString)
          (timestampString, tzId) -> timestamp.getTime()
        }
      }.toMap
    } finally {
      TimeZone.setDefault(originalTz)
    }
  }

  private def createRawData(spark: SparkSession): Dataset[(String, Timestamp)] = {
    import spark.implicits._
    val df = desiredTimestampStrings.toDF("display")
    // this will get the millis corresponding to the display time given the current *session*
    // timezone.
    df.withColumn("ts", expr("cast(display as timestamp)")).as[(String, Timestamp)]
  }

  private def testWriteTablesWithTimezone(
      spark: SparkSession,
      baseTable: String,
      explicitTz: Option[String],
      sessionTzOpt: Option[String]) : Unit = {
    val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
    test(s"SPARK-12297: Write to Parquet tables with Timestamps; explicitTz = $explicitTz; " +
        s"sessionTzOpt = $sessionTzOpt") {

      withTable(s"saveAsTable_$baseTable", s"insert_$baseTable", s"partitioned_ts_$baseTable") {
        val sessionTzId = sessionTzOpt.getOrElse(TimeZone.getDefault().getID())
        // check that created tables have correct TBLPROPERTIES
        val tblProperties = explicitTz.map {
          tz => s"""TBLPROPERTIES ($key="$tz")"""
        }.getOrElse("")

        val rawData = createRawData(spark)
        // Check writing data out.
        // We write data into our tables, and then check the raw parquet files to see whether
        // the correct conversion was applied.
        rawData.write.saveAsTable(s"saveAsTable_$baseTable")
        checkHasTz(spark, s"saveAsTable_$baseTable", None)
        spark.sql(
          s"""CREATE TABLE insert_$baseTable (
                |  display string,
                |  ts timestamp
                | )
                | STORED AS PARQUET
                | $tblProperties
               """.stripMargin)
        checkHasTz(spark, s"insert_$baseTable", explicitTz)
        rawData.write.insertInto(s"insert_$baseTable")
        // no matter what, roundtripping via the table should leave the data unchanged
        val readFromTable = spark.table(s"insert_$baseTable").collect()
          .map { row => (row.getAs[String](0), row.getAs[Timestamp](1)).toString() }.sorted
        assert(readFromTable === rawData.collect().map(_.toString()).sorted)

        // Now we load the raw parquet data on disk, and check if it was adjusted correctly.
        // Note that we only store the timezone in the table property, so when we read the
        // data this way, we're bypassing all of the conversion logic, and reading the raw
        // values in the parquet file.
        val onDiskLocation = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier(s"insert_$baseTable")).location.getPath
        // we test reading the data back with and without the vectorized reader, to make sure we
        // haven't broken reading parquet from non-hive tables, with both readers.
        Seq(false, true).foreach { vectorized =>
          spark.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, vectorized)
          val readFromDisk = spark.read.parquet(onDiskLocation).collect()
          val storageTzId = explicitTz.getOrElse(sessionTzId)
          readFromDisk.foreach { row =>
            val displayTime = row.getAs[String](0)
            val millis = row.getAs[Timestamp](1).getTime()
            val expectedMillis = timestampTimezoneToMillis((displayTime, storageTzId))
            assert(expectedMillis === millis, s"Display time '$displayTime' was stored " +
              s"incorrectly with sessionTz = ${sessionTzOpt}; Got $millis, expected " +
              s"$expectedMillis (delta = ${millis - expectedMillis})")
          }
        }

        // check tables partitioned by timestamps.  We don't compare the "raw" data in this case,
        // since they are adjusted even when we bypass the hive table.
        rawData.write.partitionBy("ts").saveAsTable(s"partitioned_ts_$baseTable")
        val partitionDiskLocation = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier(s"partitioned_ts_$baseTable")).location.getPath
        // no matter what mix of timezones we use, the dirs should specify the value with the
        // same time we use for display.
        val parts = new File(partitionDiskLocation).list().collect {
          case name if name.startsWith("ts=") => URLDecoder.decode(name.stripPrefix("ts="))
        }.toSet
        assert(parts === desiredTimestampStrings.toSet)
      }
    }
  }

  private def testReadTablesWithTimezone(
      spark: SparkSession,
      baseTable: String,
      explicitTz: Option[String],
      sessionTzOpt: Option[String]): Unit = {
    val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
    test(s"SPARK-12297: Read from Parquet tables with Timestamps; explicitTz = $explicitTz; " +
      s"sessionTzOpt = $sessionTzOpt") {
      withTable(s"external_$baseTable", s"partitioned_$baseTable") {
        // we intentionally save this data directly, without creating a table, so we can
        // see that the data is read back differently depending on table properties.
        // we'll save with adjusted millis, so that it should be the correct millis after reading
        // back.
        val rawData = createRawData(spark)
        // to avoid closing over entire class
        val timestampTimezoneToMillis = this.timestampTimezoneToMillis
        import spark.implicits._
        val adjustedRawData = (explicitTz match {
          case Some(tzId) =>
            rawData.map { case (displayTime, _) =>
              val storageMillis = timestampTimezoneToMillis((displayTime, tzId))
              (displayTime, new Timestamp(storageMillis))
            }
          case _ =>
            rawData
        }).withColumnRenamed("_1", "display").withColumnRenamed("_2", "ts")
        withTempPath { basePath =>
          val unpartitionedPath = new File(basePath, "flat")
          val partitionedPath = new File(basePath, "partitioned")
          adjustedRawData.write.parquet(unpartitionedPath.getCanonicalPath)
          val options = Map("path" -> unpartitionedPath.getCanonicalPath) ++
            explicitTz.map { tz => Map(key -> tz) }.getOrElse(Map())

          spark.catalog.createTable(
            tableName = s"external_$baseTable",
            source = "parquet",
            schema = new StructType().add("display", StringType).add("ts", TimestampType),
            options = options
          )

          // also write out a partitioned table, to make sure we can access that correctly.
          // add a column we can partition by (value doesn't particularly matter).
          val partitionedData = adjustedRawData.withColumn("id", monotonicallyIncreasingId)
          partitionedData.write.partitionBy("id")
            .parquet(partitionedPath.getCanonicalPath)
          // unfortunately, catalog.createTable() doesn't let us specify partitioning, so just use
          // a "CREATE TABLE" stmt.
          val tblOpts = explicitTz.map { tz => s"""TBLPROPERTIES ($key="$tz")""" }.getOrElse("")
          spark.sql(s"""CREATE EXTERNAL TABLE partitioned_$baseTable (
                         |  display string,
                         |  ts timestamp
                         |)
                         |PARTITIONED BY (id bigint)
                         |STORED AS parquet
                         |LOCATION 'file:${partitionedPath.getCanonicalPath}'
                         |$tblOpts
                          """.stripMargin)
          spark.sql(s"msck repair table partitioned_$baseTable")

          for {
            vectorized <- Seq(false, true)
            partitioned <- Seq(false, true)
          } {
            withClue(s"vectorized = $vectorized; partitioned = $partitioned") {
              spark.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, vectorized)
              val sessionTz = sessionTzOpt.getOrElse(TimeZone.getDefault().getID())
              val table = if (partitioned) s"partitioned_$baseTable" else s"external_$baseTable"
              val query = s"select display, cast(ts as string) as ts_as_string, ts " +
                s"from $table"
              val collectedFromExternal = spark.sql(query).collect()
              assert( collectedFromExternal.size === 4)
              collectedFromExternal.foreach { row =>
                val displayTime = row.getAs[String](0)
                // the timestamp should still display the same, despite the changes in timezones
                assert(displayTime === row.getAs[String](1).toString())
                // we'll also check that the millis behind the timestamp has the appropriate
                // adjustments.
                val millis = row.getAs[Timestamp](2).getTime()
                val expectedMillis = timestampTimezoneToMillis((displayTime, sessionTz))
                val delta = millis - expectedMillis
                val deltaHours = delta / (1000L * 60 * 60)
                assert(millis === expectedMillis, s"Display time '$displayTime' did not have " +
                  s"correct millis: was $millis, expected $expectedMillis; delta = $delta " +
                  s"($deltaHours hours)")
              }

              // Now test that the behavior is still correct even with a filter which could get
              // pushed down into parquet.  We don't need extra handling for pushed down
              // predicates because (a) in ParquetFilters, we ignore TimestampType and (b) parquet
              // does not read statistics from int96 fields, as they are unsigned.  See
              // scalastyle:off line.size.limit
              // https://github.com/apache/parquet-mr/blob/2fd62ee4d524c270764e9b91dca72e5cf1a005b7/parquet-hadoop/src/main/java/org/apache/parquet/format/converter/ParquetMetadataConverter.java#L419
              // https://github.com/apache/parquet-mr/blob/2fd62ee4d524c270764e9b91dca72e5cf1a005b7/parquet-hadoop/src/main/java/org/apache/parquet/format/converter/ParquetMetadataConverter.java#L348
              // scalastyle:on line.size.limit
              //
              // Just to be defensive in case anything ever changes in parquet, this test checks
              // the assumption on column stats, and also the end-to-end behavior.

              val hadoopConf = sparkContext.hadoopConfiguration
              val fs = FileSystem.get(hadoopConf)
              val parts = if (partitioned) {
                val subdirs = fs.listStatus(new Path(partitionedPath.getCanonicalPath))
                  .filter(_.getPath().getName().startsWith("id="))
                fs.listStatus(subdirs.head.getPath())
                  .filter(_.getPath().getName().endsWith(".parquet"))
              } else {
                fs.listStatus(new Path(unpartitionedPath.getCanonicalPath))
                  .filter(_.getPath().getName().endsWith(".parquet"))
              }
              // grab the meta data from the parquet file.  The next section of asserts just make
              // sure the test is configured correctly.
              assert(parts.size == 1)
              val oneFooter = ParquetFileReader.readFooter(hadoopConf, parts.head.getPath)
              assert(oneFooter.getFileMetaData.getSchema.getColumns.size === 2)
              assert(oneFooter.getFileMetaData.getSchema.getColumns.get(1).getType() ===
                PrimitiveTypeName.INT96)
              val oneBlockMeta = oneFooter.getBlocks().get(0)
              val oneBlockColumnMeta = oneBlockMeta.getColumns().get(1)
              val columnStats = oneBlockColumnMeta.getStatistics
              // This is the important assert.  Column stats are written, but they are ignored
              // when the data is read back as mentioned above, b/c int96 is unsigned.  This
              // assert makes sure this holds even if we change parquet versions (if eg. there
              // were ever statistics even on unsigned columns).
              assert(columnStats.isEmpty)

              // These queries should return the entire dataset, but if the predicates were
              // applied to the raw values in parquet, they would incorrectly filter data out.
              Seq(
                ">" -> "2015-12-31 22:00:00",
                "<" -> "2016-01-01 02:00:00"
              ).foreach { case (comparison, value) =>
                val query =
                  s"select ts from $table where ts $comparison '$value'"
                val countWithFilter = spark.sql(query).count()
                assert(countWithFilter === 4, query)
              }
            }
          }
        }
      }
    }
  }

  test("SPARK-12297: exception on bad timezone") {
    val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
    val badTzException = intercept[AnalysisException] {
      spark.sql(
        s"""CREATE TABLE bad_tz_table (
              |  x int
              | )
              | STORED AS PARQUET
              | TBLPROPERTIES ($key="Blart Versenwald III")
            """.stripMargin)
    }
    assert(badTzException.getMessage.contains("Blart Versenwald III"))
  }
}
