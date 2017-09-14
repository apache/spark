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
package org.apache.spark.sql.execution.datasources

import java.io.File
import java.sql.Timestamp
import java.util.TimeZone

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.SparkPlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}

abstract class BaseTimestampTableTimeZoneSuite extends SparkPlanTest with SQLTestUtils
    with BeforeAndAfterAll {

  var originalTz: TimeZone = _
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    originalTz = TimeZone.getDefault()
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  }

  protected override def afterAll(): Unit = {
    TimeZone.setDefault(originalTz)
    super.afterAll()
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

  private def checkHasTz(spark: SparkSession, table: String, tz: Option[String]): Unit = {
    val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table))
    assert(tableMetadata.properties.get(TimestampTableTimeZone.TIMEZONE_PROPERTY) === tz,
      s"for table $table")
  }

  /**
   * This checks that the dataframe contains data that matches our original data set, modified for
   * the given timezone, *if* we didn't apply any conversions when reading it back.  You should
   * pass in a dataframe that reads from the raw files, without any timezone specified.
   */
  private def checkRawData(data: DataFrame, tz: String): Unit = {
    val rows = data.collect()
    assert(rows.size == 4)
    rows.foreach { row =>
      val disp = row.getAs[String]("display")
      val ts = row.getAs[Timestamp]("ts")
      val expMillis = timestampTimezoneToMillis((disp, tz))
      assert(ts.getTime() === expMillis)
    }
  }

  // we want to test that this works w/ hive-only methods as well, so provide a few extension
  // points so we can also easily re-use this with hive support.
  protected def createAndSaveTableFunctions(): Seq[CreateAndSaveTable] = {
    Seq(CreateAndSaveDatasourceTable)
  }
  protected def ctasFunctions(): Seq[CTAS] = {
    Seq(DatasourceCTAS)
  }

  trait CreateAndSaveTable {
    /**
     * if the format is unsupported return false (and do nothing else).
     * otherwise, create the table, save the dataset into it, and return true
     */
    def createAndSave(
        ds: DataFrame,
        table: String,
        tz: Option[String],
        format: String): Boolean
  }

  object CreateAndSaveDatasourceTable extends CreateAndSaveTable {
    override def createAndSave(
        df: DataFrame,
        table: String,
        tz: Option[String],
        format: String): Boolean = {
      val writer = df.write.format(format)
      tz.foreach { writer.option(TimestampTableTimeZone.TIMEZONE_PROPERTY, _)}
      writer.saveAsTable(table)
      true
    }
  }

  trait CTAS {
    /**
     * If the format is unsupported, return false (and do nothing else).  Otherwise, create a table
     * with the given time zone, and copy the entire contents of another table into it.
     */
    def createTableFromSourceTable(
      source: String,
      dest: String,
      destTz: Option[String],
      destFormat: String): Boolean
  }

  object DatasourceCTAS extends CTAS {
    override def createTableFromSourceTable(
        source: String,
        dest: String,
        destTz: Option[String],
        destFormat: String): Boolean = {
      val writer = spark.sql(s"select * from $source").write.format(destFormat)
      destTz.foreach { writer.option(TimestampTableTimeZone.TIMEZONE_PROPERTY, _)}
      writer.saveAsTable(dest)
      true
    }
  }

  val formats = Seq("parquet", "csv", "json")

  test("SPARK-12297: Read and write with table timezones") {
    assert(TimeZone.getDefault.getID() === "America/Los_Angeles")
    val key = TimestampTableTimeZone.TIMEZONE_PROPERTY
    val originalData = createRawData(spark)
    withTempPath { basePath =>
      val dsPath = new File(basePath, "dsFlat").getAbsolutePath
      originalData.write
        .option(key, "Europe/Berlin")
        .parquet(dsPath)

      /**
       * Reads the raw data underlying the table, and assuming the data came from
       * [[createRawData()]], make sure the values are correct.
       */
      def checkTableData(table: String, format: String): Unit = {
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

        // also, read the raw parquet data, without any TZ correction, and make sure the raw
        // values have been adjusted as we expect.
        val tableMeta = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table))
        val location = tableMeta.location.toString()
        val tz = tableMeta.properties.get(TimestampTableTimeZone.TIMEZONE_PROPERTY)
        // some formats need the schema specified
        val df = spark.read.schema(originalData.schema).format(format).load(location)
        checkRawData(df, tz.getOrElse("America/Los_Angeles"))
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

      // save to tables, and read the data back -- this time, the timezone conversion should be
      // automatic from the table metadata, we don't need to supply any options when reading the
      // data.  Works across different ways of creating the tables and different data formats.
      createAndSaveTableFunctions().foreach { createAndSave =>
        formats.foreach { format =>
          val tblName = s"save_$format"
          withTable(tblName) {
            // create the table (if we can -- not all createAndSave() methods support all formats,
            // eg. hive tables don't support json)
            if (createAndSave.createAndSave(readWithCorrection, tblName, Some("UTC"), format)) {
              // make sure it has the right timezone, and the data is correct.
              checkHasTz(spark, tblName, Some("UTC"))
              checkTableData(tblName, format)

              // also try to copy this table directly into another table with a different timezone
              // setting, for all formats.
              ctasFunctions().foreach { ctas =>
                formats.foreach { destFormat =>
                  val destTableUTC = s"copy_to_utc_$format"
                  val destTableNoTZ = s"copy_to_no_tz_$format"
                  withTable(destTableUTC, destTableNoTZ) {
                    val ctasSupported = ctas.createTableFromSourceTable(source = tblName,
                      dest = destTableUTC, destTz = Some("UTC"), destFormat = destFormat)
                    if (ctasSupported) {
                      checkHasTz(spark, destTableUTC, Some("UTC"))
                      checkTableData(destTableUTC, destFormat)

                      ctas.createTableFromSourceTable(source = tblName, dest = destTableNoTZ,
                        destTz = None, destFormat = destFormat)
                      checkHasTz(spark, destTableNoTZ, None)
                      checkTableData(destTableNoTZ, destFormat)

                      // By now, we've checked that the data in both tables is different in terms
                      // of the raw values on disk, but they are the same after we apply the
                      // timezone conversions from the table properties.  Just to be extra-sure,
                      // we join the tables and make sure its OK.
                      val joinedRows = spark.sql(
                        s"""SELECT a.display, a.ts
                           |FROM $tblName AS a
                           |JOIN $destTableUTC AS b
                           |ON (a.ts = b.ts)""".stripMargin).collect()
                      assert(joinedRows.size === 4)
                      joinedRows.foreach { row =>
                        assert(row.getAs[String]("display") ===
                          row.getAs[Timestamp]("ts").toString())
                      }
                    }
                  }
                }
              }

              // Finally, try changing the tbl timezone.  This destroys integrity
              // of the existing data, but at this point we're just checking we can change
              // the metadata
              spark.sql(s"""ALTER TABLE $tblName SET TBLPROPERTIES ($key="America/Los_Angeles")""")
              checkHasTz(spark, tblName, Some("America/Los_Angeles"))

              spark.sql(s"""ALTER TABLE $tblName UNSET TBLPROPERTIES ($key)""")
              checkHasTz(spark, tblName, None)

              spark.sql(s"""ALTER TABLE $tblName SET TBLPROPERTIES ($key="UTC")""")
              checkHasTz(spark, tblName, Some("UTC"))
            }
          }
        }
      }
    }
  }

  test("SPARK-12297: exception on bad timezone") {
    // make sure there is an exception anytime we try to read or write with a bad timezone
    val key = TimestampTableTimeZone.TIMEZONE_PROPERTY
    val badVal = "Blart Versenwald III"
    val data = createRawData(spark)
    def hasBadTzException(command: => Unit): Unit = {
      withTable("bad_tz_table") {
        val badTzException = intercept[AnalysisException] { command }
        assert(badTzException.getMessage.contains(badVal))
      }
    }

    withTempPath { p =>
      hasBadTzException {
        data.write.option(key, badVal).parquet(p.getAbsolutePath)
      }

      data.write.parquet(p.getAbsolutePath)
      hasBadTzException {
        spark.read.option(key, badVal).parquet(p.getAbsolutePath)
      }
    }

    createAndSaveTableFunctions().foreach { createAndSave =>
      hasBadTzException{
        createAndSave.createAndSave(data.toDF(), "bad_tz_table", Some(badVal), "parquet")
      }

      createAndSave.createAndSave(data.toDF(), "bad_tz_table", None, "parquet")
      hasBadTzException {
        spark.sql(s"""ALTER TABLE bad_tz_table SET TBLPROPERTIES($key="$badVal")""")
      }
    }
  }

  test("SPARK-12297: insertInto must not specify timezone") {
    // You can't specify the timezone for just a portion of inserted data.  You can only specify
    // the timezone for the *entire* table (data previously in the table and any future data) so
    // complain loudly if the user tries to set the timezone on an insert.
    withTable("some_table") {
      val origData = createRawData(spark)
      origData.write.saveAsTable("some_table")
      val exc = intercept[AnalysisException]{
        createRawData(spark).write.option(TimestampTableTimeZone.TIMEZONE_PROPERTY, "UTC")
          .insertInto("some_table")
      }
      assert(exc.getMessage.contains("Cannot provide a table timezone on insert"))
    }
  }

  test("SPARK-12297: refuse table timezone on views") {
    val key = TimestampTableTimeZone.TIMEZONE_PROPERTY
    val originalData = createRawData(spark)

    withTable("ok_table") {
      originalData.write.saveAsTable("ok_table")
      withView("view_with_tz") {
        val exc1 = intercept[AnalysisException]{
          spark.sql(s"""CREATE VIEW view_with_tz
                        | TBLPROPERTIES ($key="UTC")
                        | AS SELECT * FROM ok_table
           """.stripMargin)
        }
        assert(exc1.getMessage.contains("Timezone cannot be set for view"))
        spark.sql("CREATE VIEW view_with_tz AS SELECT * FROM ok_table")
        val exc2 = intercept[AnalysisException]{
          spark.sql(s"""ALTER VIEW view_with_tz SET TBLPROPERTIES($key="UTC")""")
        }
        assert(exc2.getMessage.contains("Timezone cannot be set for view"))
      }
    }
  }
}

class TimestampTableTimeZoneSuite extends BaseTimestampTableTimeZoneSuite with SharedSQLContext
