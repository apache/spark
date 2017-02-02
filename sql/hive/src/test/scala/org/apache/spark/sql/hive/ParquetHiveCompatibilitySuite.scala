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
import java.sql.Timestamp
import java.util.TimeZone

import org.apache.spark._
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.parquet.{ParquetCompatibilityTest, ParquetFileFormat}
import org.apache.spark.sql.hive.test.{TestHiveContext, TestHiveSingleton}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

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

  test(s"SPARK-12297: Parquet Timestamp & Hive timezone") {
    // Check creating parquet tables, writing data into them, and reading it back out under a
    // variety of conditions:
    // * global conf for setting table tz by default
    // * tables with explicit tz and those without
    // * altering table properties directly
    // * UTC and non-UTC timezones
    Seq(false, true).foreach { setTableTzByDefault =>
      // we're cheating a bit here, in general SparkConf isn't meant to be set at runtime,
      // but its OK in this case, and lets us run this test, because these tests don't like
      // creating multiple HiveContexts in the same jvm
      sparkContext.conf.set(
        SQLConf.PARQUET_TABLE_INCLUDE_TIMEZONE.key, setTableTzByDefault.toString)
      val key = ParquetFileFormat.PARQUET_TIMEZONE_TABLE_PROPERTY
      def checkHasTz(table: String, tz: Option[String]): Unit = {
        val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table))
        assert(tableMetadata.properties.get(key) === tz)
      }
      def checkCreateReadWrite(baseTable: String, explicitTz: Option[String]): Unit = {
        withTable(baseTable, s"like_$baseTable", s"select_$baseTable", s"external_$baseTable",
            s"saveAsTable_$baseTable", s"insert_$baseTable") {
          withClue(s"table tz default=$setTableTzByDefault; explicitTz = $explicitTz;") {
            // check that created tables have correct TBLPROPERTIES
            val tblProperties = explicitTz.map {
              tz => raw"""TBLPROPERTIES ($key="$tz")"""
            }.getOrElse("")
            val defaultTz = if (setTableTzByDefault) Some("UTC") else None
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
            spark.sql(raw"""ALTER TABLE $baseTable SET TBLPROPERTIES ($key="UTC")""")
            checkHasTz(baseTable, Some("UTC"))
            spark.sql(raw"""ALTER TABLE $baseTable UNSET TBLPROPERTIES ($key)""")
            checkHasTz(baseTable, None)
            explicitTz.foreach { tz =>
              spark.sql( raw"""ALTER TABLE $baseTable SET TBLPROPERTIES ($key="$tz")""")
              checkHasTz(baseTable, expectedTableTz)
            }


            import spark.implicits._
            val rawData = spark.createDataset(Seq(
              "2015-12-31 23:50:59.123",
              "2015-12-31 22:49:59.123",
              "2016-01-01 00:39:59.123",
              "2016-01-01 01:29:59.123"
            ).map { x => java.sql.Timestamp.valueOf(x) })

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
            rawData.createOrReplaceTempView(s"tempView_$baseTable")
            spark.sql(s"INSERT INTO insert_$baseTable SELECT value AS ts FROM tempView_$baseTable")
            val readFromTable = spark.table(s"insert_$baseTable").collect()
              .map(_.getAs[Timestamp](0))
            // no matter what, roundtripping via the table should leave the data unchanged
            assert(readFromTable === rawData.collect())
            // lets load the raw parquet data on disk, and check if it was adjusted correctly
            val onDiskLocation = """file:(.*)""".r.findFirstMatchIn(spark.sessionState.catalog
              .getTableMetadata(TableIdentifier(s"insert_$baseTable")).location).get.group(1)
            val readFromDisk = spark.read.parquet(onDiskLocation).collect()
              .map(_.getAs[Timestamp](0))
            val expectedReadFromDisk = expectedTableTz match {
              case Some("America/Los_Angeles") =>
                // we should take a timestamp that is assumed to be in LA time, and shift it to UTC
                // by subtracting the offset from UTC.
                val tz = TimeZone.getTimeZone("America/Los_Angeles")
                rawData.collect().map { ts =>
                  new Timestamp(ts.getTime - tz.getOffset(ts.getTime))
                }
              case _ =>
                rawData.collect()
            }
            assert(readFromDisk === expectedReadFromDisk, readFromDisk.mkString(","))

            // check reading data back in
            // TODO check predicate pushdown
            // we intentionally save this data directly, without creating a table, so we can
            // see that the data is read back differently depending on table properties
            withTempPath { path =>
              rawData.write.parquet(path.getCanonicalPath)
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
                    val expTimestamps = explicitTz match {
                      case Some("America/Los_Angeles") =>
                        // this is assuming that the times were adjusted *from* LA *to* UTC when
                        // they were written out, so on reading we do the reverse.  Eg., originally
                        // the data was 12pm, but it got written out as 8pm.  We need to read it
                        // back as 12pm.  The offset is stored as the difference from UTC, so in
                        // this case it would be -8.
                        val tz = TimeZone.getTimeZone("America/Los_Angeles")
                        rawData.collect().map { ts =>
                          new Timestamp(ts.getTime + tz.getOffset(ts.getTime))
                        }
                      case _ =>
                        // no modification to raw data in parquet
                        rawData.collect()
                    }
                    assert(collectedFromExternal === expTimestamps)
                  }
                }
              }
            }
          }
        }
      }
      checkCreateReadWrite("no_tz", None)

      checkCreateReadWrite("UTC", Some("UTC"))
      checkCreateReadWrite("LA", Some("America/Los_Angeles"))

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

}
