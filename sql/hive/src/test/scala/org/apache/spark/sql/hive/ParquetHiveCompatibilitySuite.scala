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

import java.sql.Timestamp
import java.util.TimeZone

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.ParquetFileReader
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, Dataset, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.parquet.{ParquetCompatibilityTest, ParquetFileFormat}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructType, TimestampType}

class ParquetHiveCompatibilitySuite extends ParquetCompatibilityTest with TestHiveSingleton
    with BeforeAndAfterEach {
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

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
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
  testCreateWriteRead("no_tz", None)
  val localTz = TimeZone.getDefault.getID()
  testCreateWriteRead("local", Some(localTz))
  // check with a variety of timezones.  The unit tests currently are configured to always use
  // America/Los_Angeles, but even if they didn't, we'd be sure to cover a non-local timezone.
  Seq(
    "UTC" -> "UTC",
    "LA" -> "America/Los_Angeles",
    "Berlin" -> "Europe/Berlin"
  ).foreach { case (tableName, zone) =>
    if (zone != localTz) {
      testCreateWriteRead(tableName, Some(zone))
    }
  }

  private def testCreateWriteRead(
      baseTable: String,
      explicitTz: Option[String]): Unit = {
    testCreateAlterTablesWithTimezone(baseTable, explicitTz)
    testWriteTablesWithTimezone(baseTable, explicitTz)
    testReadTablesWithTimezone(baseTable, explicitTz)
  }

  private def checkHasTz(table: String, tz: Option[String]): Unit = {
    val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table))
    assert(tableMetadata.properties.get(ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY) === tz)
  }

  private def testCreateAlterTablesWithTimezone(
      baseTable: String,
      explicitTz: Option[String]): Unit = {

    test(s"SPARK-12297: Create and Alter Parquet tables and timezones; explicitTz = $explicitTz") {
      // we're cheating a bit here, in general SparkConf isn't meant to be set at runtime,
      // but its OK in this case, and lets us run this test, because these tests don't like
      // creating multiple HiveContexts in the same jvm
      val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
      withTable(baseTable, s"like_$baseTable", s"select_$baseTable") {
        val localTz = TimeZone.getDefault()
        val localTzId = localTz.getID()
        // If we ever add a property to set the table timezone by default, defaultTz would change
        val defaultTz = None
        // check that created tables have correct TBLPROPERTIES
        val tblProperties = explicitTz.map {
          tz => raw"""TBLPROPERTIES ($key="$tz")"""
        }.getOrElse("")
        spark.sql(
          raw"""CREATE TABLE $baseTable (
                |  x int
                | )
                | STORED AS PARQUET
                | $tblProperties
            """.stripMargin)
        val expectedTableTz = explicitTz.orElse(defaultTz)
        checkHasTz(baseTable, expectedTableTz)
        spark.sql(s"CREATE TABLE like_$baseTable LIKE $baseTable")
        checkHasTz(s"like_$baseTable", expectedTableTz)
        spark.sql(
          raw"""CREATE TABLE select_$baseTable
                | STORED AS PARQUET
                | AS
                | SELECT * from $baseTable
            """.stripMargin)
        checkHasTz(s"select_$baseTable", defaultTz)

        // check alter table, setting, unsetting, resetting the property
        spark.sql(
          raw"""ALTER TABLE $baseTable SET TBLPROPERTIES ($key="America/Los_Angeles")""")
        checkHasTz(baseTable, Some("America/Los_Angeles"))
        spark.sql( raw"""ALTER TABLE $baseTable SET TBLPROPERTIES ($key="UTC")""")
        checkHasTz(baseTable, Some("UTC"))
        spark.sql( raw"""ALTER TABLE $baseTable UNSET TBLPROPERTIES ($key)""")
        checkHasTz(baseTable, None)
        explicitTz.foreach { tz =>
          spark.sql( raw"""ALTER TABLE $baseTable SET TBLPROPERTIES ($key="$tz")""")
          checkHasTz(baseTable, expectedTableTz)
        }
      }
    }
  }

  private def createRawData(): Dataset[Timestamp] = {
    import spark.implicits._
    val originalTsStrings = Seq(
      "2015-12-31 22:49:59.123",
      "2015-12-31 23:50:59.123",
      "2016-01-01 00:39:59.123",
      "2016-01-01 01:29:59.123"
    )
    spark.createDataset(
      originalTsStrings.map { x => java.sql.Timestamp.valueOf(x) })
  }

  private def testWriteTablesWithTimezone(
      baseTable: String,
      explicitTz: Option[String]) : Unit = {
    val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
    test(s"SPARK-12297: Write to Parquet tables with Timestamps; explicitTz = $explicitTz") {

      withTable(s"saveAsTable_$baseTable", s"insert_$baseTable") {
        val localTz = TimeZone.getDefault()
        val localTzId = localTz.getID()
        // If we ever add a property to set the table timezone by default, defaultTz would change
        val defaultTz = None
        val expectedTableTz = explicitTz.orElse(defaultTz)
        // check that created tables have correct TBLPROPERTIES
        val tblProperties = explicitTz.map {
          tz => raw"""TBLPROPERTIES ($key="$tz")"""
        }.getOrElse("")


        val rawData = createRawData()
        // Check writing data out.
        // We write data into our tables, and then check the raw parquet files to see whether
        // the correct conversion was applied.
        rawData.write.saveAsTable(s"saveAsTable_$baseTable")
        checkHasTz(s"saveAsTable_$baseTable", defaultTz)
        spark.sql(
          raw"""CREATE TABLE insert_$baseTable (
                |  ts timestamp
                | )
                | STORED AS PARQUET
                | $tblProperties
               """.stripMargin)
        checkHasTz(s"insert_$baseTable", expectedTableTz)
        rawData.write.insertInto(s"insert_$baseTable")
        val readFromTable = spark.table(s"insert_$baseTable").collect()
          .map(_.getAs[Timestamp](0))
        // no matter what, roundtripping via the table should leave the data unchanged
        assert(readFromTable === rawData.collect())

        // Now we load the raw parquet data on disk, and check if it was adjusted correctly.
        // Note that we only store the timezone in the table property, so when we read the
        // data this way, we're bypassing all of the conversion logic, and reading the raw
        // values in the parquet file.
        val onDiskLocation = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier(s"insert_$baseTable")).location.getPath
        val readFromDisk = spark.read.parquet(onDiskLocation).collect()
          .map(_.getAs[Timestamp](0))
        val expectedReadFromDisk = expectedTableTz match {
          case Some(tzId) =>
            // We should have shifted the data from our local timezone to the storage timezone
            // when we saved the data.
            val storageTz = TimeZone.getTimeZone(tzId)
            rawData.collect().map { ts =>
              val t = ts.getTime()
              new Timestamp(t + storageTz.getOffset(t) - localTz.getOffset(t))
            }
          case _ =>
            rawData.collect()
        }
        assert(readFromDisk === expectedReadFromDisk,
          s"timestamps changed string format after reading back from parquet with " +
            s"local = $localTzId & storage = $expectedTableTz")
      }
    }
  }

  private def testReadTablesWithTimezone(
      baseTable: String,
      explicitTz: Option[String]): Unit = {
      val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
    test(s"SPARK-12297: Read from Parquet tables with Timestamps; explicitTz = $explicitTz") {
      withTable(s"external_$baseTable") {
        // we intentionally save this data directly, without creating a table, so we can
        // see that the data is read back differently depending on table properties
        val localTz = TimeZone.getDefault()
        val rawData = createRawData()
        // adjust the raw parquet data based on the timezones, so that it should get read back the
        // same way
        val adjustedRawData = explicitTz match {
          case Some(tzId) =>
            val storageTz = TimeZone.getTimeZone(tzId)
            import spark.implicits._
            rawData.map { ts =>
              val t = ts.getTime()
              new Timestamp(t + storageTz.getOffset(t) - localTz.getOffset(t))
            }
          case _ =>
            rawData
        }
        withTempPath { path =>
          adjustedRawData.write.parquet(path.getCanonicalPath)
          val options = Map("path" -> path.getCanonicalPath) ++
            explicitTz.map { tz => Map(key -> tz) }.getOrElse(Map())

          spark.catalog.createTable(
            tableName = s"external_$baseTable",
            source = "parquet",
            schema = new StructType().add("value", TimestampType),
            options = options
          )
          Seq(false, true).foreach { vectorized =>
            withSQLConf((SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, vectorized.toString)) {
              withClue(s"vectorized = $vectorized;") {
                val collectedFromExternal =
                  spark.sql(s"select value from external_$baseTable").collect()
                    .map(_.getAs[Timestamp](0))
                val expTimestamps = rawData.collect()
                assert(collectedFromExternal === expTimestamps,
                  s"collected = ${collectedFromExternal.mkString(",")}")

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
                val parts = fs.listStatus(new Path(path.getCanonicalPath))
                  .filter(_.getPath().getName().endsWith(".parquet"))
                assert(parts.size == 1)
                val oneFooter = ParquetFileReader.readFooter(hadoopConf, parts.head.getPath)
                assert(oneFooter.getFileMetaData.getSchema.getColumns.size == 1)
                val oneBlockMeta = oneFooter.getBlocks().get(0)
                val oneBlockColumnMeta = oneBlockMeta.getColumns().get(0)
                val columnStats = oneBlockColumnMeta.getStatistics
                // Column stats are written, but they are ignored when the data is read back as
                // mentioned above, b/c int96 is unsigned.  This assert makes sure this holds even
                // if we change parquet versions (if eg. there were ever statistics even on unsigned
                // columns).
                assert(columnStats.isEmpty)

                // These queries should return the entire dataset, but if the predicates were
                // applied to the raw values in parquet, they would incorrectly filter data out.
                Seq(
                  ">" -> "2015-12-31 22:00:00",
                  "<" -> "2016-01-01 02:00:00"
                ).foreach { case (comparison, value) =>
                  val query =
                    s"select value from external_$baseTable where value $comparison '$value'"
                  val countWithFilter = spark.sql(query).count()
                  assert(countWithFilter === 4, query)
                }

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
        raw"""CREATE TABLE bad_tz_table (
              |  x int
              | )
              | STORED AS PARQUET
              | TBLPROPERTIES ($key="Blart Versenwald III")
            """.stripMargin)
    }
    assert(badTzException.getMessage.contains("Blart Versenwald III"))
  }
}
