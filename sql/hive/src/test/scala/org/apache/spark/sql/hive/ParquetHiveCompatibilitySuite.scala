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

import org.apache.spark.sql._
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

  // Check creating parquet tables with timestamps, writing data into them, and reading it back out
  // under a variety of conditions:
  // * tables with explicit tz and those without
  // * altering table properties directly
  // * variety of timezones, local & non-local
  testCreateAlterTablesWithTimezone("no_tz", None, None)
  testCreateAlterTablesWithTimezone("LA", Some("America/Los_Angeles"), None)
  testCreateAlterTablesWithTimezone("LA", Some("Europe/Berlin"), None)
  testCreateAlterTablesWithTimezone("LA", Some("Europe/Berlin"), Some("UTC"))

  private def checkHasTz(spark: SparkSession, table: String, tz: Option[String]): Unit = {
    val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table))
    assert(tableMetadata.properties.get(ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY) === tz,
      s"for table $table")
  }

  private def testCreateAlterTablesWithTimezone(
      baseTable: String,
      explicitTz: Option[String],
      sessionTzOpt: Option[String]): Unit = {
    test(s"SPARK-12297: Create and Alter Parquet tables and timezones; explicitTz = $explicitTz; " +
      s"sessionTzOpt = $sessionTzOpt") {
      val localSession = spark.newSession()
      sessionTzOpt.foreach { tz => localSession.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, tz) }
      val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
      withTable(baseTable, s"like_$baseTable", s"select_$baseTable", s"partitioned_$baseTable") {
        // If we ever add a property to set the table timezone by default, defaultTz would change
        val defaultTz = None
        // check that created tables have correct TBLPROPERTIES
        val tblProperties = explicitTz.map {
          tz => s"""TBLPROPERTIES ($key="$tz")"""
        }.getOrElse("")
        localSession.sql(
          s"""CREATE TABLE $baseTable (
                |  x int
                | )
                | STORED AS PARQUET
                | $tblProperties
            """.stripMargin)
        val expectedTableTz = explicitTz.orElse(defaultTz)
        checkHasTz(localSession, baseTable, expectedTableTz)
        localSession.sql(
          s"""CREATE TABLE partitioned_$baseTable (
                |  x int
                | )
                | PARTITIONED BY (y int)
                | STORED AS PARQUET
                | $tblProperties
            """.stripMargin)
        checkHasTz(localSession, s"partitioned_$baseTable", expectedTableTz)
        localSession.sql(s"CREATE TABLE like_$baseTable LIKE $baseTable")
        checkHasTz(localSession, s"like_$baseTable", expectedTableTz)
        localSession.sql(
          s"""CREATE TABLE select_$baseTable
                | STORED AS PARQUET
                | AS
                | SELECT * from $baseTable
            """.stripMargin)
        checkHasTz(localSession, s"select_$baseTable", defaultTz)

        // check alter table, setting, unsetting, resetting the property
        localSession.sql(
          s"""ALTER TABLE $baseTable SET TBLPROPERTIES ($key="America/Los_Angeles")""")
        checkHasTz(localSession, baseTable, Some("America/Los_Angeles"))
        localSession.sql(s"""ALTER TABLE $baseTable SET TBLPROPERTIES ($key="UTC")""")
        checkHasTz(localSession, baseTable, Some("UTC"))
        localSession.sql(s"""ALTER TABLE $baseTable UNSET TBLPROPERTIES ($key)""")
        checkHasTz(localSession, baseTable, None)
        explicitTz.foreach { tz =>
          localSession.sql(s"""ALTER TABLE $baseTable SET TBLPROPERTIES ($key="$tz")""")
          checkHasTz(localSession, baseTable, expectedTableTz)
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

  test("SPARK-12297: Read and write with timezone adjustments") {
    assert(TimeZone.getDefault.getID() === "America/Los_Angeles")
    val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
    val originalData = createRawData(spark)
    withTempPath { basePath =>
      val dsPath = new File(basePath, "dsFlat").getAbsolutePath
      originalData.write
        .option(key, "Europe/Berlin")
        .parquet(dsPath)

      def checkRawData(data: DataFrame, tz: String): Unit = {
        data.collect().foreach { row =>
          val disp = row.getAs[String]("display")
          val ts = row.getAs[Timestamp]("ts")
          assert(disp != ts.toString)
          val expMillis = timestampTimezoneToMillis((disp, tz))
          assert(ts.getTime() === expMillis)
        }
      }

      // read it back, without supplying the right timezone.  Won't match the original, but we
      // expect specific values.
      val readNoCorrection = spark.read.parquet(dsPath)
      checkRawData(readNoCorrection, "Europe/Berlin")

      // now read it back *with* the right timezone -- everything should match.
      val readWithCorrection = spark.read.option(key, "Europe/Berlin").parquet(dsPath)

      readWithCorrection.collect().foreach { row =>
        assert(row.getAs[String]("display") === row.getAs[Timestamp]("ts").toString())
      }
      // now make sure it works if we read and write together
      withTable("adjusted_table", "adjusted_table_2", "save_as_table") {
        Seq("adjusted_table", "adjusted_table_2").foreach { table =>
          spark.sql(s"""CREATE EXTERNAL TABLE $table (
                       |  display string,
                       |  ts timestamp
                       |)
                       |STORED AS parquet
                       |LOCATION 'file:${basePath.getAbsolutePath}/$table'
                       |TBLPROPERTIES ($key="UTC")
                       |""".stripMargin)
          checkHasTz(spark, "adjusted_table", Some("UTC"))
        }

        // do the read-write twice -- we want to make sure it works when the read-portion of the
        // plan has already been analyzed, and also when it hasn't
        Seq(false, true).foreach { fromHiveTable =>
          withClue(s"reading from ${if (fromHiveTable) "hive table" else "ds table"}") {
            val dest = if (fromHiveTable) {
              spark.sql("insert into adjusted_table_2 select * from adjusted_table")
              // we dont' need to test CTAS, since CTAS tables won't ever have the table properties
              "adjusted_table_2"
            } else {
              readWithCorrection.write.insertInto("adjusted_table")
              "adjusted_table"
            }

            val readFromTable = spark.sql(s"select display, ts from $dest")
            val tableMeta =
              spark.sessionState.catalog.getTableMetadata(TableIdentifier(dest))
            readFromTable.collect().foreach { row =>
              assert(row.getAs[String]("display") === row.getAs[Timestamp]("ts").toString())
            }
            val tablePath = s"${basePath.getAbsolutePath}/$dest"
            val readFromHiveUncorrected = spark.read.parquet(tablePath)
            checkRawData(readFromHiveUncorrected, "UTC")
          }
        }
      }

      readWithCorrection.write.option(key, "UTC").saveAsTable("save_as_table")
      // I don't really understand why, but the timezone gets put into a table storage property,
      // not a table property, this way.  So round-tripping via spark works, but it won't
      // be accessible to other engines.
      checkHasTz(spark, "save_as_table", Some("UTC"))

      val readFromDsTable = spark.sql("select * from save_as_table")
      readFromDsTable.collect().foreach { row =>
        assert(row.getAs[String]("display") === row.getAs[Timestamp]("ts").toString())
      }
      val tableMeta = spark.sessionState.catalog.getTableMetadata(TableIdentifier("save_as_table"))
      val readFromDsTableUncorrected = spark.read.parquet(tableMeta.location.toString)
      checkRawData(readFromDsTableUncorrected, "UTC")
    }
  }

  test("SPARK-12297: exception on bad timezone") {
    val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
    val badVal = "Blart Versenwald III"
    def hasBadTzException(command: => Unit): Unit = {
      withTable("bad_tz_table") {
        val badTzException = intercept[AnalysisException] { command }
        assert(badTzException.getMessage.contains(badVal))
        logWarning(badTzException.getMessage)
      }
    }
    hasBadTzException{
      spark.sql(
        s"""CREATE TABLE bad_tz_table (
              |  x int
              | )
              | STORED AS PARQUET
              | TBLPROPERTIES ($key="$badVal")
            """.stripMargin)
    }

    spark.sql("CREATE TABLE bad_tz_table (x int)")
    hasBadTzException {
      spark.sql(s"""ALTER TABLE bad_tz_table SET TBLPROPERTIES($key="$badVal")""")
    }

    hasBadTzException {
      createRawData(spark).write.format("parquet").option(key, badVal).saveAsTable("blah")
    }

    withTempPath { p =>
      hasBadTzException {
        createRawData(spark).write.option(key, badVal).parquet(p.getAbsolutePath)
      }
    }
  }

  test("SPARK-12297: insertInto must not specify timezone") {
    // after you've already created a table, its too late to specify the timezone, so fail loudly
    // if the user tries.
    withTable("some_table") {
      spark.sql("CREATE TABLE some_table (x string, y timestamp)")
      val exc = intercept[AnalysisException]{
        createRawData(spark).write.option(ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY, "UTC")
          .insertInto("some_table")
      }
      assert(exc.getMessage.contains("Cannot provide a table timezone on insert"))
    }

  }
}
