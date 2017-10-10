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
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.SparkPlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}

abstract class BaseAdjustTimestampsSuite extends SparkPlanTest with SQLTestUtils
    with BeforeAndAfterAll {

  protected val SESSION_TZ = "America/Los_Angeles"
  protected val TABLE_TZ = "Europe/Berlin"
  protected val UTC = "UTC"
  protected val TZ_KEY = DateTimeUtils.TIMEZONE_PROPERTY

  var originalTz: TimeZone = _
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    originalTz = TimeZone.getDefault()
    TimeZone.setDefault(TimeZone.getTimeZone(SESSION_TZ))
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
  // spark context in the hive tests, and then we might be prone to issues from lazy vals for
  // timezones.  Instead, we manually adjust the timezone just to determine what the desired millis
  // (since epoch, in utc) is for various "wall-clock" times in different timezones, and then we can
  // compare against those in our tests.
  val timestampTimezoneToMillis = {
    val originalTz = TimeZone.getDefault
    try {
      desiredTimestampStrings.flatMap { timestampString =>
        Seq(SESSION_TZ, TABLE_TZ, UTC).map { tzId =>
          TimeZone.setDefault(TimeZone.getTimeZone(tzId))
          val timestamp = Timestamp.valueOf(timestampString)
          (timestampString, tzId) -> timestamp.getTime()
        }
      }.toMap
    } finally {
      TimeZone.setDefault(originalTz)
    }
  }

  protected def createRawData(spark: SparkSession): Dataset[(String, Timestamp)] = {
    import spark.implicits._
    val df = desiredTimestampStrings.toDF("display")
    // this will get the millis corresponding to the display time given the current session tz
    df.withColumn("ts", expr("cast(display as timestamp)")).as[(String, Timestamp)]
  }

  protected def checkHasTz(spark: SparkSession, table: String, tz: Option[String]): Unit = {
    val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table))
    assert(tableMetadata.properties.get(TZ_KEY) === tz,
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

  private val formats = Seq("parquet", "csv", "json")

  // we want to test that this works w/ hive-only methods as well, so provide a few extension
  // points so we can also easily re-use this with hive support.
  protected def createAndSaveTableFunctions(): Map[String, CreateAndSaveTable] = {
    formats.map { f => (f, new CreateAndSaveDatasourceTable(f)) }.toMap
  }

  protected def ctasFunctions(): Map[String, CTAS] = {
    formats.map { f => (f, new DatasourceCTAS(f)) }.toMap
  }

  trait CreateAndSaveTable {
    /** Create the table and save the contents of the dataset into it. */
    def createAndSave(df: DataFrame, table: String, tz: Option[String]): Unit

    /** The target table's format. */
    val format: String
  }

  class CreateAndSaveDatasourceTable(override val format: String) extends CreateAndSaveTable {
    override def createAndSave(df: DataFrame, table: String, tz: Option[String]): Unit = {
      val writer = df.write.format(format)
      tz.foreach(writer.option(TZ_KEY, _))
      writer.saveAsTable(table)
    }
  }

  trait CTAS {
    /**
     * Create a table with the given time zone, and copy the entire contents of the source table
     * into it.
     */
    def createFromSource(source: String, dest: String, destTz: Option[String]): Unit

    /** The target table's format. */
    val format: String
  }

  class DatasourceCTAS(override val format: String) extends CTAS {
    override def createFromSource(source: String, dest: String, destTz: Option[String]): Unit = {
      val writer = spark.sql(s"select * from $source").write.format(format)
      destTz.foreach { writer.option(TZ_KEY, _)}
      writer.saveAsTable(dest)
    }
  }

  createAndSaveTableFunctions().foreach { case (fmt, createFn) =>
    ctasFunctions().foreach { case (destFmt, ctasFn) =>
      test(s"timestamp adjustment: in=$fmt, out=$destFmt") {
        testTimestampAdjustment(fmt, destFmt, createFn, ctasFn)
      }
    }
  }

  private def testTimestampAdjustment(
      format: String,
      destFormat: String,
      createFn: CreateAndSaveTable,
      ctasFn: CTAS): Unit = {
    assert(TimeZone.getDefault.getID() === SESSION_TZ)
    val originalData = createRawData(spark)
    withTempPath { basePath =>
      val dsPath = new File(basePath, "dsFlat").getAbsolutePath
      originalData.write
        .option(TZ_KEY, TABLE_TZ)
        .parquet(dsPath)

      /**
       * Reads the raw data underlying the table, and assuming the data came from
       * [[createRawData()]], make sure the values are correct.
       */
      def checkTableData(table: String, format: String): Unit = {
        // These queries should return the entire dataset, but if the predicates were
        // applied to the raw values in parquet, they would incorrectly filter data out.
        Seq(
          "ts > '2015-12-31 22:00:00'",
          "ts < '2016-01-01 02:00:00'"
        ).foreach { filter =>
          val query =
            s"select ts from $table where $filter"
          val countWithFilter = spark.sql(query).count()
          assert(countWithFilter === desiredTimestampStrings.size, query)
        }

        // also, read the raw table data, without any TZ correction, and make sure the raw
        // values have been adjusted as we expect.
        val tableMeta = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table))
        val location = tableMeta.location.toString()
        val tz = tableMeta.properties.get(TZ_KEY)
        // some formats need the schema specified
        val df = spark.read.schema(originalData.schema).format(format).load(location)
        checkRawData(df, tz.getOrElse(SESSION_TZ))
      }

      // read it back, without supplying the right timezone.  Won't match the original, but we
      // expect specific values.
      val readNoCorrection = spark.read.parquet(dsPath)
      checkRawData(readNoCorrection, TABLE_TZ)

      // now read it back *with* the right timezone -- everything should match.
      val readWithCorrection = spark.read.option(TZ_KEY, TABLE_TZ).parquet(dsPath)

      readWithCorrection.collect().foreach { row =>
        assert(row.getAs[String]("display") === row.getAs[Timestamp]("ts").toString())
      }

      // save to tables, and read the data back -- this time, the timezone conversion should be
      // automatic from the table metadata, we don't need to supply any options when reading the
      // data.  Works across different ways of creating the tables and different data formats.
      val tblName = s"save_$format"
      withTable(tblName) {
        // create the table (if we can -- not all createAndSave() methods support all formats,
        // eg. hive tables don't support json)
        createFn.createAndSave(readWithCorrection, tblName, Some(UTC))
        // make sure it has the right timezone, and the data is correct.
        checkHasTz(spark, tblName, Some(UTC))
        checkTableData(tblName, createFn.format)

        // also try to copy this table directly into another table with a different timezone
        // setting, for all formats.
        val destTableUTC = s"copy_to_utc_$destFormat"
        val destTableNoTZ = s"copy_to_no_tz_$destFormat"
        withTable(destTableUTC, destTableNoTZ) {
          ctasFn.createFromSource(tblName, destTableUTC, Some(UTC))
          checkHasTz(spark, destTableUTC, Some(UTC))
          checkTableData(destTableUTC, ctasFn.format)

          ctasFn.createFromSource(tblName, destTableNoTZ, None)
          checkHasTz(spark, destTableNoTZ, None)
          checkTableData(destTableNoTZ, ctasFn.format)

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

        // Finally, try changing the tbl timezone.  This destroys integrity
        // of the existing data, but at this point we're just checking we can change
        // the metadata
        spark.sql(
          s"""ALTER TABLE $tblName SET TBLPROPERTIES ("$TZ_KEY"="$SESSION_TZ")""")
        checkHasTz(spark, tblName, Some(SESSION_TZ))

        spark.sql(s"""ALTER TABLE $tblName UNSET TBLPROPERTIES ("$TZ_KEY")""")
        checkHasTz(spark, tblName, None)

        spark.sql(s"""ALTER TABLE $tblName SET TBLPROPERTIES ("$TZ_KEY"="$UTC")""")
        checkHasTz(spark, tblName, Some(UTC))
      }
    }
  }

  test("exception on bad timezone") {
    // make sure there is an exception anytime we try to read or write with a bad timezone
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
        data.write.option(TZ_KEY, badVal).parquet(p.getAbsolutePath)
      }

      data.write.parquet(p.getAbsolutePath)
      hasBadTzException {
        spark.read.option(TZ_KEY, badVal).parquet(p.getAbsolutePath)
      }
    }

    createAndSaveTableFunctions().foreach { case (_, createFn) =>
      hasBadTzException{
        createFn.createAndSave(data.toDF(), "bad_tz_table", Some(badVal))
      }

      createFn.createAndSave(data.toDF(), "bad_tz_table", None)
      hasBadTzException {
        spark.sql(s"""ALTER TABLE bad_tz_table SET TBLPROPERTIES("$TZ_KEY"="$badVal")""")
      }
    }
  }

  test("insertInto must not specify timezone") {
    // You can't specify the timezone for just a portion of inserted data.  You can only specify
    // the timezone for the *entire* table (data previously in the table and any future data) so
    // complain loudly if the user tries to set the timezone on an insert.
    withTable("some_table") {
      val origData = createRawData(spark)
      origData.write.saveAsTable("some_table")
      val exc = intercept[AnalysisException]{
        createRawData(spark).write.option(TZ_KEY, UTC)
          .insertInto("some_table")
      }
      assert(exc.getMessage.contains("Cannot provide a table timezone on insert"))
    }
  }

  test("disallow table timezone on views") {
    val originalData = createRawData(spark)

    withTable("ok_table") {
      originalData.write.saveAsTable("ok_table")
      withView("view_with_tz") {
        val exc1 = intercept[AnalysisException]{
          spark.sql(s"""CREATE VIEW view_with_tz
                       |TBLPROPERTIES ("$TZ_KEY"="$UTC")
                       |AS SELECT * FROM ok_table
           """.stripMargin)
        }
        assert(exc1.getMessage.contains("Timezone cannot be set for view"))
        spark.sql("CREATE VIEW view_with_tz AS SELECT * FROM ok_table")
        val exc2 = intercept[AnalysisException]{
          spark.sql(s"""ALTER VIEW view_with_tz SET TBLPROPERTIES("$TZ_KEY"="$UTC")""")
        }
        assert(exc2.getMessage.contains("Timezone cannot be set for view"))
      }
    }
  }
}

class AdjustTimestampsSuite extends BaseAdjustTimestampsSuite with SharedSQLContext
