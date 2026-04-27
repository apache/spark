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

package org.apache.spark.sql.connector

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.TypeUtils._
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.connector.catalog.InMemoryBaseTable
import org.apache.spark.sql.connector.write.InsertSummary
import org.apache.spark.sql.execution.datasources.v2.ExtractV2Table
import org.apache.spark.sql.functions.{array, lit, map, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * A collection of "INSERT INTO" tests that can be run through the SQL or DataFrameWriter APIs.
 * Extending test suites can implement the `doInsert` method to run the insert through either
 * API.
 *
 * @param supportsDynamicOverwrite Whether the Table implementations used in the test suite support
 *                                 dynamic partition overwrites. If they do, we will check for the
 *                                 success of the operations. If not, then we will check that we
 *                                 failed with the right error message.
 * @param includeSQLOnlyTests Certain INSERT INTO behavior can be achieved purely through SQL, e.g.
 *                            static or dynamic partition overwrites. This flag should be set to
 *                            true if we would like to test these cases.
 */
abstract class InsertIntoTests(
    override protected val supportsDynamicOverwrite: Boolean,
    override protected val includeSQLOnlyTests: Boolean) extends InsertIntoSQLOnlyTests {

  import testImplicits._

  /**
   * Insert data into a table using the insertInto statement. Implementations can be in SQL
   * ("INSERT") or using the DataFrameWriter (`df.write.insertInto`).
   */
  protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode = null): Unit

  test("insertInto: append") {
    val t1 = s"${catalogAndNamespace}tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    doInsert(t1, df)
    checkInsertMetrics(t1, numInsertedRows = 3)
    verifyTable(t1, df)
  }

  test("insertInto: append by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")

    doInsert(t1, dfr)
    checkInsertMetrics(t1, numInsertedRows = 3)
    verifyTable(t1, df)
  }

  test("insertInto: append partitioned table") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      doInsert(t1, df)
      checkInsertMetrics(t1, numInsertedRows = 3)
      verifyTable(t1, df)
    }
  }

  test("insertInto: overwrite non-partitioned table") {
    val t1 = s"${catalogAndNamespace}tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    val df2 = Seq((4L, "d"), (5L, "e"), (6L, "f")).toDF("id", "data")
    doInsert(t1, df)
    checkInsertMetrics(t1, numInsertedRows = 3)
    doInsert(t1, df2, SaveMode.Overwrite)
    verifyTable(t1, df2)
  }

  test("insertInto: overwrite partitioned table in static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = s"${catalogAndNamespace}tbl"
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
      doInsert(t1, init)
      checkInsertMetrics(t1, numInsertedRows = 2)

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      doInsert(t1, df, SaveMode.Overwrite)
      verifyTable(t1, df)
    }
  }


  test("insertInto: overwrite partitioned table in static mode by position") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = s"${catalogAndNamespace}tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
        doInsert(t1, init)
        checkInsertMetrics(t1, numInsertedRows = 2)

        val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")
        doInsert(t1, dfr, SaveMode.Overwrite)

        val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
        verifyTable(t1, df)
      }
    }
  }

  test("insertInto: fails when missing a column") {
    val t1 = s"${catalogAndNamespace}tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string, missing string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")

    verifyTable(t1, Seq.empty[(Long, String, String)].toDF("id", "data", "missing"))
    val tableName = if (catalogAndNamespace.isEmpty) {
      toSQLId(s"spark_catalog.default.$t1")
    } else {
      toSQLId(t1)
    }
    checkError(
      exception = intercept[AnalysisException] {
        doInsert(t1, df)
      },
      condition = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
      parameters = Map(
        "tableName" -> tableName,
        "tableColumns" -> "`id`, `data`, `missing`",
        "dataColumns" -> "`id`, `data`")
    )
  }

  test("insertInto: fails when an extra column is present") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val df = Seq((1L, "a", "mango")).toDF("id", "data", "fruit")
      verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"))
      val tableName = if (catalogAndNamespace.isEmpty) {
        toSQLId(s"spark_catalog.default.$t1")
      } else {
        toSQLId(t1)
      }
      checkError(
        exception = intercept[AnalysisException] {
          doInsert(t1, df)
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> tableName,
          "tableColumns" -> "`id`, `data`",
          "dataColumns" -> "`id`, `data`, `fruit`")
      )
    }
  }

  dynamicOverwriteTest("insertInto: overwrite partitioned table in dynamic mode") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
      doInsert(t1, init)

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      doInsert(t1, df, SaveMode.Overwrite)

      verifyTable(t1, df.union(sql("SELECT 4L, 'keep'")))
    }
  }

  dynamicOverwriteTest("insertInto: overwrite partitioned table in dynamic mode by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
      doInsert(t1, init)

      val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")
      doInsert(t1, dfr, SaveMode.Overwrite)

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "keep")).toDF("id", "data")
      verifyTable(t1, df)
    }
  }
}

trait InsertIntoSQLOnlyTests
  extends SharedSparkSession
  with BeforeAndAfter {

  import testImplicits._

  /** Check that the results in `tableName` match the `expected` DataFrame. */
  protected def verifyTable(tableName: String, expected: DataFrame): Unit

  protected def checkInsertMetrics(tableName: String, numInsertedRows: Long): Unit = {
    val inMemoryTable = spark.table(tableName).queryExecution.analyzed.collectFirst {
      case ExtractV2Table(t) => t.asInstanceOf[InMemoryBaseTable]
    }.get
    val summary = inMemoryTable.commits.last.writeSummary.get.asInstanceOf[InsertSummary]
    assert(summary.numInsertedRows() === numInsertedRows,
      s"Expected numInsertedRows=$numInsertedRows, got ${summary.numInsertedRows()}")
  }

  protected val v2Format: String
  protected val catalogAndNamespace: String

  /**
   * Whether dynamic partition overwrites are supported by the `Table` definitions used in the
   * test suites. Tables that leverage the V1 Write interface do not support dynamic partition
   * overwrites.
   */
  protected val supportsDynamicOverwrite: Boolean

  /** Whether to include the SQL specific tests in this trait within the extending test suite. */
  protected val includeSQLOnlyTests: Boolean

  protected def withTableAndData(tableName: String)(testFn: String => Unit): Unit = {
    withTable(tableName) {
      val viewName = "tmp_view"
      val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
      df.createOrReplaceTempView(viewName)
      withTempView(viewName) {
        testFn(viewName)
      }
    }
  }

  protected def dynamicOverwriteTest(testName: String)(f: => Unit): Unit = {
    test(testName) {
      try {
        withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
          f
        }
        if (!supportsDynamicOverwrite) {
          fail("Expected failure from test, because the table doesn't support dynamic overwrites")
        }
      } catch {
        case a: AnalysisException if !supportsDynamicOverwrite =>
          assert(a.getMessage.contains("does not support dynamic overwrite"))
      }
    }
  }

  if (includeSQLOnlyTests) {
    test("InsertInto: when the table doesn't exist") {
      val t1 = s"${catalogAndNamespace}tbl"
      val t2 = s"${catalogAndNamespace}tbl2"
      withTableAndData(t1) { _ =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
        val parsed = CatalystSqlParser.parseMultipartIdentifier(t2)
          .map(part => quoteIdentifier(part)).mkString(".")
        val e = intercept[AnalysisException] {
          sql(s"INSERT INTO $t2 VALUES (2L, 'dummy')")
        }
        checkErrorTableNotFound(e, parsed,
          ExpectedContext(t2, 12, 11 + t2.length))
      }
    }

  test("InsertInto: extra column by name fails without schema evolution") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val tableName = if (catalogAndNamespace.isEmpty) {
        toSQLId(s"spark_catalog.default.$t1")
      } else {
        toSQLId(t1)
      }
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT INTO $t1 BY NAME SELECT 2L AS id, TRUE AS active, 'b' AS data")
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> tableName,
          "tableColumns" -> "`id`, `data`",
          "dataColumns" -> "`id`, `active`, `data`")
      )
    }
  }

    test("InsertInto: append to partitioned table - static clause") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 PARTITION (id = 23) SELECT data FROM $view")
        checkInsertMetrics(t1, numInsertedRows = 3)
        verifyTable(t1, sql(s"SELECT 23, data FROM $view"))
      }
    }

    test("InsertInto: overwrite - dynamic clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = s"${catalogAndNamespace}tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'also-deleted')")
          checkInsertMetrics(t1, numInsertedRows = 2)
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a"),
            (2, "b"),
            (3, "c")).toDF())
        }
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - dynamic clause - dynamic mode") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'keep')")
        checkInsertMetrics(t1, numInsertedRows = 2)
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a"),
          (2, "b"),
          (3, "c"),
          (4, "keep")).toDF("id", "data"))
      }
    }

    test("InsertInto: overwrite - missing clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = s"${catalogAndNamespace}tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'also-deleted')")
          checkInsertMetrics(t1, numInsertedRows = 2)
          sql(s"INSERT OVERWRITE TABLE $t1 SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a"),
            (2, "b"),
            (3, "c")).toDF("id", "data"))
        }
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - missing clause - dynamic mode") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'keep')")
        checkInsertMetrics(t1, numInsertedRows = 2)
        sql(s"INSERT OVERWRITE TABLE $t1 SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a"),
          (2, "b"),
          (3, "c"),
          (4, "keep")).toDF("id", "data"))
      }
    }

    test("InsertInto: overwrite - static clause") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p1 int) " +
          s"USING $v2Format PARTITIONED BY (p1)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 23), (4L, 'keep', 2)")
        checkInsertMetrics(t1, numInsertedRows = 2)
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p1 = 23) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 23),
          (2, "b", 23),
          (3, "c", 23),
          (4, "keep", 2)).toDF("id", "data", "p1"))
      }
    }

    test("InsertInto: overwrite - mixed clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = s"${catalogAndNamespace}tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'also-deleted', 2)")
          checkInsertMetrics(t1, numInsertedRows = 2)
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id, p = 2) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 2),
            (2, "b", 2),
            (3, "c", 2)).toDF("id", "data", "p"))
        }
      }
    }

    test("InsertInto: overwrite - mixed clause reordered - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = s"${catalogAndNamespace}tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'also-deleted', 2)")
          checkInsertMetrics(t1, numInsertedRows = 2)
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2, id) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 2),
            (2, "b", 2),
            (3, "c", 2)).toDF("id", "data", "p"))
        }
      }
    }

    test("InsertInto: overwrite - implicit dynamic partition - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = s"${catalogAndNamespace}tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'also-deleted', 2)")
          checkInsertMetrics(t1, numInsertedRows = 2)
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 2),
            (2, "b", 2),
            (3, "c", 2)).toDF("id", "data", "p"))
        }
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - mixed clause - dynamic mode") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
          s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        checkInsertMetrics(t1, numInsertedRows = 2)
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2, id) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 2),
          (2, "b", 2),
          (3, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"))
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - mixed clause reordered - dynamic mode") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
          s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        checkInsertMetrics(t1, numInsertedRows = 2)
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id, p = 2) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 2),
          (2, "b", 2),
          (3, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"))
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - implicit dynamic partition - dynamic mode") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
          s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        checkInsertMetrics(t1, numInsertedRows = 2)
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 2),
          (2, "b", 2),
          (3, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"))
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - multiple static partitions - dynamic mode") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
          s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        checkInsertMetrics(t1, numInsertedRows = 2)
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id = 2, p = 2) SELECT data FROM $view")
        verifyTable(t1, Seq(
          (2, "a", 2),
          (2, "b", 2),
          (2, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"))
      }
    }

    test("do not double insert on INSERT INTO collect()") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
        val df = sql(s"INSERT INTO TABLE $t1 SELECT * FROM $view")

        df.collect()
        df.take(5)
        df.tail(5)
        df.where("true").collect()
        df.where("true").take(5)
        df.where("true").tail(5)

        checkInsertMetrics(t1, numInsertedRows = 3)
        verifyTable(t1, spark.table(view))
      }
    }

    test("SPARK-34599: InsertInto: overwrite - dot in the partition column name - static mode") {
      import testImplicits._
      val t1 = "tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (`a.b` string, `c.d` string) USING $v2Format PARTITIONED BY (`a.b`)")
        sql(s"INSERT OVERWRITE $t1 PARTITION (`a.b` = 'a') (`c.d`) VALUES('b')")
        verifyTable(t1, Seq("a" -> "b").toDF("id", "data"))
      }
    }

    test("InsertInto: column DEFAULT values") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (c1 INT DEFAULT 42, c2 STRING DEFAULT 'hello') USING $v2Format")
        sql(s"INSERT INTO $t1 VALUES (1, DEFAULT)")
        checkInsertMetrics(t1, numInsertedRows = 1)
        checkAnswer(sql(s"SELECT * FROM $t1"), Row(1, "hello"))

        sql(s"INSERT INTO $t1 VALUES (DEFAULT, DEFAULT)")
        checkInsertMetrics(t1, numInsertedRows = 1)
        checkAnswer(
          sql(s"SELECT * FROM $t1 ORDER BY c1"),
          Seq(Row(1, "hello"), Row(42, "hello")))

        sql(s"INSERT OVERWRITE $t1 VALUES (100, DEFAULT)")
        checkAnswer(sql(s"SELECT * FROM $t1"), Row(100, "hello"))
      }
    }
  }
}

/**
 * INSERT schema evolution tests that cover adding new column or fields present in the source
 * query to the target table. Covers by-position and by-name inserts.
 */
trait InsertIntoSchemaEvolutionTests { this: InsertIntoTests =>

  import testImplicits._

  /** Insert data into a table with schema evolution and optional by-name resolution. */
  protected def doInsertWithSchemaEvolution(
      tableName: String,
      insert: DataFrame,
      mode: SaveMode = SaveMode.Append,
      byName: Boolean = false,
      replaceWhere: Option[String] = None): Unit

  /** Insert data into a table by name without schema evolution. */
  protected def doInsertByName(
      tableName: String,
      insert: DataFrame,
      mode: SaveMode = SaveMode.Append): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      sql(s"INSERT $overwrite TABLE $tableName BY NAME SELECT * FROM $tmpView")
    }
  }

  /** Run a block with INSERT nested type coercion enabled. */
  protected def withInsertNestedTypeCoercion(f: => Unit): Unit = {
    withSQLConf(SQLConf.INSERT_INTO_NESTED_TYPE_COERCION_ENABLED.key -> "true") {
      f
    }
  }

  test("Insert schema evolution: extra column - no auto-schema-evolution capability") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format " +
        s"TBLPROPERTIES ('auto-schema-evolution' = 'false')")
      checkError(
        exception = intercept[AnalysisException] {
          doInsertWithSchemaEvolution(t1,
            Seq((2L, "b", true)).toDF("id", "data", "active"))
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> toSQLId(t1),
          "tableColumns" -> "`id`, `data`",
          "dataColumns" -> "`id`, `data`, `active`")
      )
    }
  }

  test("Insert schema evolution: no-op without AUTOMATIC_SCHEMA_EVOLUTION capability") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format " +
        s"TBLPROPERTIES ('auto-schema-evolution' = 'false')")
      doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      // Same column count, no evolution needed: should succeed even without capability.
      doInsertWithSchemaEvolution(t1, Seq((2L, "b")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      verifyTable(t1, Seq((1L, "a"), (2L, "b")).toDF("id", "data"))
    }
  }

  test("Insert schema evolution: by position - same column count, different names") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1, Seq((2L, "b")).toDF("x", "y"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      // No evolution
      verifyTable(t1, Seq((1L, "a"), (2L, "b")).toDF("id", "data"))
    }
  }

  test("Insert schema evolution: extra top-level column by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "b", true)).toDF("id", "data", "active"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      verifyTable(t1, Seq[(Long, String, java.lang.Boolean)](
        (1L, "a", null),
        (2L, "b", true)
      ).toDF("id", "data", "active"))
    }
  }

  test("Insert schema evolution: multiple extra top-level columns by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "b", true, 100L)).toDF("id", "data", "active", "score"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      verifyTable(t1, Seq[(Long, String, java.lang.Boolean, java.lang.Long)](
        (1L, "a", null, null),
        (2L, "b", true, 100L)
      ).toDF("id", "data", "active", "score"))
    }
  }

  test("Insert schema evolution: extra column by position - different column names") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1, Seq((2L, "b", true)).toDF("x", "y", "z"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      verifyTable(t1, Seq[(Long, String, java.lang.Boolean)](
        (1L, "a", null),
        (2L, "b", true)
      ).toDF("id", "data", "z"))
    }
  }

  test("Insert schema evolution: extra column into empty target by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      doInsertWithSchemaEvolution(t1,
        Seq((1L, "a", true)).toDF("id", "data", "active"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      verifyTable(t1, Seq(
        (1L, "a", true)
      ).toDF("id", "data", "active"))
    }
  }

  test("Insert schema evolution: extra nested field by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, info struct<name:string>) USING $v2Format")
      doInsert(t1,
        Seq((1, "Alice")).toDF("id", "name")
          .select($"id", struct($"name").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2, "Bob", 30)).toDF("id", "name", "age")
          .select($"id", struct($"name", $"age").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1, Row("Alice", null)), Row(2, Row("Bob", 30))))
    }
  }

  test("Insert schema evolution: extra nested field by position - different field name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, info struct<name:string>) USING $v2Format")
      doInsert(t1,
        Seq((1, "Alice")).toDF("id", "name")
          .select($"id", struct($"name").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2, "Bob", 30)).toDF("id", "firstName", "age")
          .select($"id", struct($"firstName", $"age").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1, Row("Alice", null)), Row(2, Row("Bob", 30))))
    }
  }

  test("Insert schema evolution: extra column by name - different column order") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq(("b", true, 2L)).toDF("data", "active", "id"), byName = true)
      checkInsertMetrics(t1, numInsertedRows = 1)
      verifyTable(t1, Seq[(Long, String, java.lang.Boolean)](
        (1L, "a", null),
        (2L, "b", true)
      ).toDF("id", "data", "active"))
    }
  }

  test("Insert schema evolution: extra nested field by name - different field order") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, info struct<name:string>) USING $v2Format")
      doInsert(t1,
        Seq((1, "Alice")).toDF("id", "name")
          .select($"id", struct($"name").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2, 30, "Bob")).toDF("id", "age", "name")
          .select($"id", struct($"age", $"name").as("info")),
        byName = true)
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1, Row("Alice", null)), Row(2, Row("Bob", 30))))
    }
  }

  test("Insert schema evolution: multiple extra nested fields by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, info struct<name:string>) USING $v2Format")
      doInsert(t1,
        Seq((1, "Alice")).toDF("id", "name")
          .select($"id", struct($"name").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2, 30, "Bob", "NYC")).toDF("id", "age", "name", "city")
          .select($"id", struct($"age", $"name", $"city").as("info")),
        byName = true)
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1, Row("Alice", null, null)), Row(2, Row("Bob", 30, "NYC"))))
    }
  }

  test("Insert schema evolution: by name - same columns, different order") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq(("b", 2L)).toDF("data", "id"), byName = true)
      checkInsertMetrics(t1, numInsertedRows = 1)
      // No evolution
      verifyTable(t1, Seq((1L, "a"), (2L, "b")).toDF("id", "data"))
    }
  }

  test("Insert schema evolution: by name - all different columns") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq(("b", 2L)).toDF("x", "y"), byName = true)
      checkInsertMetrics(t1, numInsertedRows = 1)
      verifyTable(t1, Seq[(java.lang.Long, String, String, java.lang.Long)](
        (1L, "a", null, null),
        (null, null, "b", 2L)
      ).toDF("id", "data", "x", "y"))
    }
  }

  test("Insert schema evolution: extra nested field by name in overwrite") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, info struct<name:string>) USING $v2Format")
      doInsert(t1,
        Seq((1, "Alice")).toDF("id", "name")
          .select($"id", struct($"name").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2, 30, "Bob")).toDF("id", "age", "name")
          .select($"id", struct($"age", $"name").as("info")),
        mode = SaveMode.Overwrite,
        byName = true)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(2, Row("Bob", 30))))
    }
  }

  test("Insert schema evolution: REPLACE WHERE with extra column by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      doInsert(t1, Seq((1L, "a"), (2L, "b")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 2)
      // REPLACE WHERE only deletes rows matching the predicate, then inserts new data.
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "x", true), (4L, "y", false)).toDF("id", "data", "active"),
        replaceWhere = Some("id = 2"))
      verifyTable(t1, Seq[(java.lang.Long, String, java.lang.Boolean)](
        (1L, "a", null),
        (2L, "x", true),
        (4L, "y", false)
      ).toDF("id", "data", "active"))
    }
  }

  test("Insert schema evolution: REPLACE WHERE with extra column by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      doInsert(t1, Seq((1L, "a"), (2L, "b")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 2)
      doInsertWithSchemaEvolution(t1,
        Seq((true, "x", 2L), (false, "y", 4L)).toDF("active", "data", "id"),
        byName = true,
        replaceWhere = Some("id = 2"))
      verifyTable(t1, Seq[(java.lang.Long, String, java.lang.Boolean)](
        (1L, "a", null),
        (2L, "x", true),
        (4L, "y", false)
      ).toDF("id", "data", "active"))
    }
  }

  test("Insert schema evolution: REPLACE WHERE with nested struct evolution by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, info struct<name:string>) USING $v2Format " +
        s"PARTITIONED BY (id)")
      val initDf = Seq((1L, "Alice"), (2L, "Bob")).toDF("id", "name")
        .select($"id", struct($"name").as("info"))
      doInsert(t1, initDf)
      checkInsertMetrics(t1, numInsertedRows = 2)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "Bobby", 25)).toDF("id", "name", "age")
          .select($"id", struct($"name", $"age").as("info")),
        replaceWhere = Some("id = 2"))
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1L, Row("Alice", null)), Row(2L, Row("Bobby", 25))))
    }
  }


  test("Insert schema evolution: REPLACE WHERE with nested struct evolution by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, info struct<name:string>) USING $v2Format " +
        s"PARTITIONED BY (id)")
      val initDf = Seq((1L, "Alice"), (2L, "Bob")).toDF("id", "name")
        .select($"id", struct($"name").as("info"))
      doInsert(t1, initDf)
      checkInsertMetrics(t1, numInsertedRows = 2)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "Bobby", 25)).toDF("id", "name", "age")
          .select($"id", struct($"age", $"name").as("info")),
        byName = true,
        replaceWhere = Some("id = 2"))
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1L, Row("Alice", null)), Row(2L, Row("Bobby", 25))))
    }
  }

  test("Insert extra column by position fails without schema evolution") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      checkError(
        exception = intercept[AnalysisException] {
          doInsert(t1, Seq((2L, "b", true)).toDF("id", "data", "active"))
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> toSQLId(t1),
          "tableColumns" -> "`id`, `data`",
          "dataColumns" -> "`id`, `data`, `active`")
      )
    }
  }
  test("Insert schema evolution: INSERT OVERWRITE with dynamic partition mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = s"${catalogAndNamespace}tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        doInsert(t1, Seq((1L, "a"), (2L, "b")).toDF("id", "data"))
        checkInsertMetrics(t1, numInsertedRows = 2)
        // Overwrite with schema evolution adding a new column, dynamic mode should only replace
        // partitions present in the inserted data.
        doInsertWithSchemaEvolution(t1,
          Seq((2L, "x", true), (3L, "y", false)).toDF("id", "data", "active"),
          mode = SaveMode.Overwrite)
        checkAnswer(
          sql(s"SELECT * FROM $t1"),
          Seq(
            Row(1L, "a", null),
            Row(2L, "x", true),
            Row(3L, "y", false)))
      }
    }
  }

  test("Insert schema evolution: INSERT OVERWRITE with dynamic partition mode by name") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = s"${catalogAndNamespace}tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        doInsert(t1, Seq((1L, "a"), (2L, "b")).toDF("id", "data"))
        checkInsertMetrics(t1, numInsertedRows = 2)
        doInsertWithSchemaEvolution(t1,
          Seq((true, "x", 2L), (false, "y", 3L)).toDF("active", "data", "id"),
          mode = SaveMode.Overwrite,
          byName = true)
        checkAnswer(
          sql(s"SELECT * FROM $t1"),
          Seq(
            Row(1L, "a", null),
            Row(2L, "x", true),
            Row(3L, "y", false)))
      }
    }
  }

  test("Insert schema evolution: INSERT OVERWRITE with static partition mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = s"${catalogAndNamespace}tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        doInsert(t1, Seq((1L, "a"), (2L, "b")).toDF("id", "data"))
        checkInsertMetrics(t1, numInsertedRows = 2)
        // Static mode overwrites the entire table.
        doInsertWithSchemaEvolution(t1,
          Seq((2L, "x", true), (3L, "y", false)).toDF("id", "data", "active"),
          mode = SaveMode.Overwrite)
        checkAnswer(
          sql(s"SELECT * FROM $t1"),
          Seq(
            Row(2L, "x", true),
            Row(3L, "y", false)))
      }
    }
  }

  test("Insert schema evolution: case-insensitive column matching by name") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
        doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
        // Column names differ only in case - should match and not create new columns,
        // while "active" is truly new and should be added.
        doInsertWithSchemaEvolution(t1,
          Seq(("b", true, 2L)).toDF("DATA", "active", "ID"), byName = true)
        verifyTable(t1, Seq[(Long, String, java.lang.Boolean)](
          (1L, "a", null),
          (2L, "b", true)
        ).toDF("id", "data", "active"))
      }
    }
  }

  test("Insert schema evolution: case-sensitive column matching by name") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
        doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
        // In case-sensitive mode, "ID" and "DATA" don't match "id" and "data",
        // so all source columns are new.
        doInsertWithSchemaEvolution(t1,
          Seq(("b", 2L)).toDF("DATA", "ID"), byName = true)
        verifyTable(t1, Seq[(java.lang.Long, String, String, java.lang.Long)](
          (1L, "a", null, null),
          (null, null, "b", 2L)
        ).toDF("id", "data", "DATA", "ID"))
      }
    }
  }

  test("Insert schema evolution: multiple inserts accumulate schema changes") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint) USING $v2Format")
      doInsertWithSchemaEvolution(t1,
        Seq((1L, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "b", true)).toDF("id", "data", "active"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      verifyTable(t1, Seq[(Long, String, java.lang.Boolean)](
        (1L, "a", null),
        (2L, "b", true)
      ).toDF("id", "data", "active"))
    }
  }

  test("Insert schema evolution: extra field in 2-level nested struct by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, info struct<nested:struct<name:string>>) USING $v2Format")
      doInsert(t1,
        Seq((1L, "Alice")).toDF("id", "name")
          .select($"id", struct(struct($"name").as("nested")).as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "Bob", 30)).toDF("id", "name", "age")
          .select($"id", struct(struct($"name", $"age").as("nested")).as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1L, Row(Row("Alice", null))), Row(2L, Row(Row("Bob", 30)))))
    }
  }

  test("Insert schema evolution: extra field in 2-level nested struct by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, info struct<nested:struct<name:string>>) USING $v2Format")
      doInsert(t1,
        Seq((1L, "Alice")).toDF("id", "name")
          .select($"id", struct(struct($"name").as("nested")).as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "Bob", 30)).toDF("id", "name", "age")
          .select($"id", struct(struct($"age", $"name").as("nested")).as("info")),
        byName = true)
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1L, Row(Row("Alice", null))), Row(2L, Row(Row("Bob", 30)))))
    }
  }

  test("Insert schema evolution: extra field in array element struct by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, info array<struct<name:string>>) USING $v2Format")
      doInsert(t1,
        Seq((1L, "Alice")).toDF("id", "name")
          .select($"id", array(struct($"name")).as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "Bob", 30)).toDF("id", "name", "age")
          .select($"id", array(struct($"name", $"age")).as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(
          Row(1L, Seq(Row("Alice", null))),
          Row(2L, Seq(Row("Bob", 30)))))
    }
  }

  test("Insert schema evolution: extra field in map value struct by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, info map<string, struct<name:string>>) USING $v2Format")
      doInsert(t1,
        Seq((1L, "A", "Alice")).toDF("id", "key", "name")
          .select($"id", map($"key", struct($"name")).as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "B", "Bob", 30)).toDF("id", "key", "name", "age")
          .select($"id", map($"key", struct($"name", $"age")).as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(
          Row(1L, Map("A" -> Row("Alice", null))),
          Row(2L, Map("B" -> Row("Bob", 30)))))
    }
  }

  test("Insert schema evolution: extra field in map key struct by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, info map<struct<name:string>, string>) USING $v2Format")
      doInsert(t1,
        Seq((1L, "Alice", "A")).toDF("id", "name", "value")
          .select($"id", map(struct($"name"), $"value").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "Bob", 30, "B")).toDF("id", "name", "age", "value")
          .select($"id", map(struct($"name", $"age"), $"value").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(
          Row(1L, Map(Row("Alice", null) -> "A")),
          Row(2L, Map(Row("Bob", 30) -> "B"))))
    }
  }

  test("Insert schema evolution: type widening int to long by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, data string) USING $v2Format")
      doInsert(t1, Seq((1, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((Long.MaxValue, "b")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1L, "a"), Row(Long.MaxValue, "b")))
      assert(spark.table(t1).schema("id").dataType === LongType)
    }
  }

  test("Insert schema evolution: type widening int to long by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, data string) USING $v2Format")
      doInsert(t1, Seq((1, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq(("b", Long.MaxValue)).toDF("data", "id"), byName = true)
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1L, "a"), Row(Long.MaxValue, "b")))
      assert(spark.table(t1).schema("id").dataType === LongType)
    }
  }

  test("Insert schema evolution: type widening + extra column combined") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, data string) USING $v2Format")
      doInsert(t1, Seq((1, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((Long.MaxValue, "b", true)).toDF("id", "data", "active"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(
          Row(1L, "a", null),
          Row(Long.MaxValue, "b", true)))
      val schema = spark.table(t1).schema
      assert(schema("id").dataType === LongType)
      assert(schema.fieldNames.contains("active"))
    }
  }

  test("Insert schema evolution: type widening nested struct field by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, info struct<value:int,name:string>) USING $v2Format")
      doInsert(t1,
        Seq((1L, "Alice", 100)).toDF("id", "name", "value")
          .select($"id", struct($"value", $"name").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "Bob", Long.MaxValue)).toDF("id", "name", "value")
          .select($"id", struct($"value", $"name").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT id, info.value, info.name FROM $t1"),
        Seq(Row(1L, 100L, "Alice"), Row(2L, Long.MaxValue, "Bob")))
      val infoType = spark.table(t1).schema("info").dataType.asInstanceOf[StructType]
      assert(infoType("value").dataType === LongType)
    }
  }

  test("Insert schema evolution: type widening nested struct field by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, info struct<value:int,name:string>) USING $v2Format")
      doInsert(t1,
        Seq((1L, "Alice", 100)).toDF("id", "name", "value")
          .select($"id", struct($"value", $"name").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "Bob", Long.MaxValue)).toDF("id", "name", "value")
          .select($"id", struct($"name", $"value").as("info")),
        byName = true)
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT id, info.value, info.name FROM $t1"),
        Seq(Row(1L, 100L, "Alice"), Row(2L, Long.MaxValue, "Bob")))
      val infoType = spark.table(t1).schema("info").dataType.asInstanceOf[StructType]
      assert(infoType("value").dataType === LongType)
    }
  }

  test("Insert schema evolution: type widening in array element struct by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, arr array<struct<value:int>>) USING $v2Format")
      doInsert(t1,
        Seq((1L, 100)).toDF("id", "value")
          .select($"id", array(struct($"value")).as("arr")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, Long.MaxValue)).toDF("id", "value")
          .select($"id", array(struct($"value")).as("arr")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT id, arr[0].value FROM $t1"),
        Seq(Row(1L, 100L), Row(2L, Long.MaxValue)))
      val arrType = spark.table(t1).schema("arr").dataType.asInstanceOf[ArrayType]
      val elemType = arrType.elementType.asInstanceOf[StructType]
      assert(elemType("value").dataType === LongType)
    }
  }

  test("Insert schema evolution: type widening in map value struct by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, m map<string,struct<value:int>>) USING $v2Format")
      doInsert(t1,
        Seq((1L, "k1", 100)).toDF("id", "key", "value")
          .select($"id", map($"key", struct($"value")).as("m")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((2L, "k2", Long.MaxValue)).toDF("id", "key", "value")
          .select($"id", map($"key", struct($"value")).as("m")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT id, m['k1'].value, m['k2'].value FROM $t1"),
        Seq(Row(1L, 100L, null), Row(2L, null, Long.MaxValue)))
      val mapType = spark.table(t1).schema("m").dataType.asInstanceOf[MapType]
      val valueType = mapType.valueType.asInstanceOf[StructType]
      assert(valueType("value").dataType === LongType)
    }
  }

  test("Insert schema evolution: type widening with overwrite mode") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, data string) USING $v2Format")
      doInsert(t1, Seq((1, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq((Long.MaxValue, "b")).toDF("id", "data"), mode = SaveMode.Overwrite)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(Long.MaxValue, "b")))
      assert(spark.table(t1).schema("id").dataType === LongType)
    }
  }

  test("Insert schema evolution: no type narrowing - inserting int into long should not change") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      // Inserting an int into a long column should not narrow the schema.
      doInsertWithSchemaEvolution(t1, Seq((2, "b")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1L, "a"), Row(2L, "b")))
      // Schema should still be long.
      assert(spark.table(t1).schema("id").dataType === LongType)
    }
  }

  test("Insert schema evolution: type widening and other type mismatch") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id float, data string) USING $v2Format")
      doInsert(t1, Seq((1f, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      // Inserting a double into a float should widen the schema, inserting an int into a string
      // should retain the string type.
      doInsertWithSchemaEvolution(t1, Seq((2d, 3)).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1d, "a"), Row(2d, "3")))
      assert(spark.table(t1).schema("id").dataType === DoubleType)
      assert(spark.table(t1).schema("data").dataType === StringType)
    }
  }

  test("Insert schema evolution: incompatible type change - struct to atomic fails") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, info struct<value:int>) USING $v2Format")
      // Inserting a string where a struct is expected is incompatible and should fail.
      checkError(
        exception = intercept[SparkException] {
          doInsertWithSchemaEvolution(t1, Seq((1L, "not_a_struct")).toDF("id", "info"))
        },
        condition = "_LEGACY_ERROR_TEMP_2095",
        parameters = Map(
          "left" -> new StructType()
            .add("id", LongType)
            .add("info", new StructType().add("value", IntegerType))
            .toString,
          "right" -> new StructType()
            .add("id", LongType, nullable = false)
            .add("info", StringType)
            .toString
      ))
    }
  }

  test("Insert schema evolution: source NullType column does not change target type by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      // Insert a null value with NullType - should not change the target column type.
      doInsertWithSchemaEvolution(t1,
        Seq(2L).toDF("id").withColumn("data", lit(null)))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1L, "a"), Row(2L, null)))
      assert(spark.table(t1).schema("data").dataType === StringType)
    }
  }

  test("Insert schema evolution: source NullType column does not change target type by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      doInsert(t1, Seq((1L, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      doInsertWithSchemaEvolution(t1,
        Seq(2L).toDF("id").withColumn("data", lit(null)),
        byName = true)
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1L, "a"), Row(2L, null)))
      assert(spark.table(t1).schema("data").dataType === StringType)
    }
  }

  test("Insert schema evolution: source NullType for nested struct field by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, info struct<value:int,name:string>) USING $v2Format")
      doInsert(t1,
        Seq((1L, "Alice", 100)).toDF("id", "name", "value")
          .select($"id", struct($"value", $"name").as("info")))
      checkInsertMetrics(t1, numInsertedRows = 1)
      // Insert with NullType for nested field - should not change the struct field type.
      doInsertWithSchemaEvolution(t1,
        Seq(2L).toDF("id")
          .withColumn("info", struct(lit(null).as("value"), lit("Bob").as("name"))))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT id, info.value, info.name FROM $t1"),
        Seq(Row(1L, 100, "Alice"), Row(2L, null, "Bob")))
      val infoType = spark.table(t1).schema("info").dataType.asInstanceOf[StructType]
      assert(infoType("value").dataType === IntegerType)
    }
  }

  test("Insert schema evolution: type widening without schema evolution flag") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, data string) USING $v2Format")
      doInsert(t1, Seq((1, "a")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      // Insert without schema evolution - should cast to target type, not widen.
      doInsert(t1, Seq((2L, "b")).toDF("id", "data"))
      checkInsertMetrics(t1, numInsertedRows = 1)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(1, "a"), Row(2, "b")))
      // Schema should still be int since we didn't use schema evolution.
      assert(spark.table(t1).schema("id").dataType === IntegerType)
    }
  }

  // ---------------------------------------------------------------------------
  // Tests for source with fewer columns/fields than target
  // ---------------------------------------------------------------------------

  test("Insert schema evolution: source missing top-level column by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val schema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("salary", IntegerType),
        StructField("dep", StringType)))
      val data = Seq(Row(0, 100, "sales"))
      sql(s"CREATE TABLE $t1 (id int, salary int, dep string) USING $v2Format")
      doInsert(t1, spark.createDataFrame(spark.sparkContext.parallelize(data), schema))
      doInsertWithSchemaEvolution(t1,
        Seq((1, "engineering")).toDF("id", "dep"),
        byName = true)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, 100, "sales"), Row(1, null, "engineering")))
    }
  }

  test("Insert schema evolution: source missing top-level column by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, salary int, dep string) USING $v2Format")
      doInsert(t1, Seq((0, 100, "sales")).toDF("id", "salary", "dep"))
      // By position: source col 1 maps to target col 1, source col 2 maps to target col 2,
      // trailing target col 3 is filled with null.
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1,
          Seq((1, 200)).toDF("id", "salary"))
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, 100, "sales"), Row(1, 200, null)))
    }
  }

  test("Insert schema evolution: source missing top-level column with DEFAULT by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, salary int DEFAULT 200, dep string) USING $v2Format")
      doInsert(t1, Seq((0, 100, "sales")).toDF("id", "salary", "dep"))
      doInsertWithSchemaEvolution(t1,
        Seq((1, "engineering")).toDF("id", "dep"),
        byName = true)
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, 100, "sales"), Row(1, 200, "engineering")))
    }
  }

  test("Insert schema evolution: source missing top-level column with DEFAULT by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, salary int, dep string DEFAULT 'unknown') USING $v2Format")
      doInsert(t1, Seq((0, 100, "sales")).toDF("id", "salary", "dep"))
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1,
          Seq((1, 200)).toDF("id", "salary"))
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, 100, "sales"), Row(1, 200, "unknown")))
    }
  }

  test("Insert schema evolution: source missing nested struct field by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", BooleanType))))))
      sql(s"CREATE TABLE $t1 (id int, s struct<c1:int,c2:string,c3:boolean>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Row(1, "a", true)))), targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Row(10, "b")))), sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData, byName = true)
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, Row(1, "a", true)), Row(1, Row(10, "b", null))))
    }
  }

  test("Insert schema evolution: source missing nested struct field by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", BooleanType))))))
      sql(s"CREATE TABLE $t1 (id int, s struct<c1:int,c2:string,c3:boolean>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Row(1, "a", true)))), targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Row(10, "b")))), sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData)
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, Row(1, "a", true)), Row(1, Row(10, "b", null))))
    }
  }

  test("Insert schema evolution: source missing field in struct nested in array by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("a", ArrayType(StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", BooleanType)))))))
      sql(s"CREATE TABLE $t1 (id int, " +
        s"a array<struct<c1:int,c2:string,c3:boolean>>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Seq(Row(1, "a", true))))),
        targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("a", ArrayType(StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType)))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Seq(Row(10, "b"))))),
        sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData, byName = true)
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, Seq(Row(1, "a", true))), Row(1, Seq(Row(10, "b", null)))))
    }
  }

  test("Insert schema evolution: source missing field in struct nested in array by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("a", ArrayType(StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", BooleanType)))))))
      sql(s"CREATE TABLE $t1 (id int, " +
        s"a array<struct<c1:int,c2:string,c3:boolean>>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Seq(Row(1, "a", true))))),
        targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("a", ArrayType(StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType)))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Seq(Row(10, "b"))))),
        sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData)
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, Seq(Row(1, "a", true))), Row(1, Seq(Row(10, "b", null)))))
    }
  }

  test("Insert schema evolution: source missing deeply nested struct field by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", BooleanType)))))))))
      sql(s"CREATE TABLE $t1 (id int, " +
        s"s struct<c1:int,c2:struct<a:int,b:boolean>>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Row(1, Row(10, true))))),
        targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StructType(Seq(
            StructField("a", IntegerType)))))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Row(20, Row(30))))),
        sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData, byName = true)
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, Row(1, Row(10, true))), Row(1, Row(20, Row(30, null)))))
    }
  }

  test("Insert schema evolution: source with null struct and missing nested field by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", IntegerType))))))
      sql(s"CREATE TABLE $t1 (id int, " +
        s"s struct<c1:int,c2:string,c3:int>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Row(1, "a", 10)))),
        targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, null))),
        sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData, byName = true)
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, Row(1, "a", 10)), Row(1, null)))
    }
  }

  test("Insert schema evolution: source with null struct and missing nested field by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", IntegerType))))))
      sql(s"CREATE TABLE $t1 (id int, " +
        s"s struct<c1:int,c2:string,c3:int>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Row(1, "a", 10)))),
        targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, null))),
        sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData)
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, Row(1, "a", 10)), Row(1, null)))
    }
  }

  test("Insert schema evolution: mixed null and non-null structs with missing field by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", BooleanType))))))
      sql(s"CREATE TABLE $t1 (id int, s struct<c1:int,c2:string,c3:boolean>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Row(1, "a", true)))),
        targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Row(10, "b")), Row(2, null))),
        sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData, byName = true)
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, Row(1, "a", true)), Row(1, Row(10, "b", null)), Row(2, null)))
    }
  }

  test("Insert schema evolution: null deeply nested struct with missing field by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", BooleanType)))))))))
      sql(s"CREATE TABLE $t1 (id int, " +
        s"s struct<c1:int,c2:struct<a:int,b:boolean>>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Row(1, Row(10, true))))),
        targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StructType(Seq(
            StructField("a", IntegerType)))))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Row(20, null)))),
        sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData, byName = true)
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, Row(1, Row(10, true))), Row(1, Row(20, null))))
    }
  }

  test("Insert schema evolution: null struct in array with missing field by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("a", ArrayType(StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", BooleanType)))))))
      sql(s"CREATE TABLE $t1 (id int, " +
        s"a array<struct<c1:int,c2:boolean>>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Seq(Row(1, true))))),
        targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("a", ArrayType(StructType(Seq(
          StructField("c1", IntegerType)))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Seq(Row(10), null)))),
        sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData, byName = true)
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(Row(0, Seq(Row(1, true))), Row(1, Seq(Row(10, null), null))))
    }
  }

  test("Insert schema evolution: source missing field in struct nested in map value by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("m", MapType(StringType, StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", BooleanType)))))))
      sql(s"CREATE TABLE $t1 (id int, " +
        s"m map<string, struct<c1:int,c2:boolean>>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Map("x" -> Row(1, true))))),
        targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("m", MapType(StringType, StructType(Seq(
          StructField("c1", IntegerType)))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Map("y" -> Row(10))))),
        sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData, byName = true)
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(
          Row(0, Map("x" -> Row(1, true))),
          Row(1, Map("y" -> Row(10, null)))))
    }
  }

  test("Insert schema evolution: source missing field in struct nested in map value by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("m", MapType(StringType, StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", BooleanType)))))))
      sql(s"CREATE TABLE $t1 (id int, " +
        s"m map<string, struct<c1:int,c2:boolean>>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Map("x" -> Row(1, true))))),
        targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("m", MapType(StringType, StructType(Seq(
          StructField("c1", IntegerType)))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Map("y" -> Row(10))))),
        sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData)
      }
      checkAnswer(
        sql(s"SELECT * FROM $t1"),
        Seq(
          Row(0, Map("x" -> Row(1, true))),
          Row(1, Map("y" -> Row(10, null)))))
    }
  }

  test("Insert schema evolution: extra and missing top-level column by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, salary int, dep string) USING $v2Format")
      doInsert(t1, Seq((0, 100, "sales")).toDF("id", "salary", "dep"))
      // Source has "active" (extra) but is missing "salary". Column count is the same (3)
      // but names differ; by-name resolution should add "active" via schema evolution
      // and fill "salary" with null.
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1,
          Seq((1, "engineering", true)).toDF("id", "dep", "active"),
          byName = true)
      }
      checkAnswer(
        sql(s"SELECT id, salary, dep, active FROM $t1"),
        Seq(Row(0, 100, "sales", null), Row(1, null, "engineering", true)))
    }
  }

  test("Insert schema evolution: extra and missing nested struct field by name") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", BooleanType))))))
      sql(s"CREATE TABLE $t1 (id int, s struct<c1:int,c2:string,c3:boolean>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Row(1, "a", true)))), targetSchema)
      doInsert(t1, targetData)

      // Source struct has "c1", "c2", "c4" (extra) but is missing "c3". Field count is the same
      // (3) but names differ; by-name resolution should add "c4" via schema evolution and fill
      // "c3" with null.
      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c4", DoubleType))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Row(10, "b", 3.14)))), sourceSchema)
      withInsertNestedTypeCoercion {
        doInsertWithSchemaEvolution(t1, sourceData, byName = true)
      }
      checkAnswer(
        sql(s"SELECT id, s.c1, s.c2, s.c3, s.c4 FROM $t1"),
        Seq(Row(0, 1, "a", true, null), Row(1, 10, "b", null, 3.14)))
    }
  }

  // ---------------------------------------------------------------------------
  // Negative tests: missing columns/fields should fail WITHOUT schema evolution
  // ---------------------------------------------------------------------------

  test("Insert without evolution: source missing top-level column by name fails") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, salary int, dep string) USING $v2Format")
      doInsert(t1, Seq((0, 100, "sales")).toDF("id", "salary", "dep"))
      // Without explicit DEFAULT on `salary`, missing by-name data only errors when null-fill
      // for missing defaults is disabled; otherwise FILL mode inserts null for `salary`.
      withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
        checkError(
          exception = intercept[AnalysisException] {
            doInsertByName(t1, Seq((1, "engineering")).toDF("id", "dep"))
          },
          condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          parameters = Map(
            "tableName" -> toSQLId(s"${catalogAndNamespace}tbl"),
            "colName" -> "`salary`")
        )
      }
    }
  }

  test("Insert schema evolution: source missing top-level column by position fails " +
      "when null default disabled and column has no explicit DEFAULT") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, salary int, dep string) USING $v2Format")
      doInsert(t1, Seq((0, 100, "sales")).toDF("id", "salary", "dep"))
      withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
        withInsertNestedTypeCoercion {
          checkError(
            exception = intercept[AnalysisException] {
              doInsertWithSchemaEvolution(t1,
                Seq((1, 200)).toDF("id", "salary"))
            },
            condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
            parameters = Map(
              "tableName" -> toSQLId(s"${catalogAndNamespace}tbl"),
              "colName" -> "`dep`")
          )
        }
      }
    }
  }

  test("Insert schema evolution: source missing nested struct field by position fails " +
      "when null default disabled") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", BooleanType))))))
      sql(s"CREATE TABLE $t1 (id int, s struct<c1:int,c2:string,c3:boolean>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Row(1, "a", true)))), targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Row(10, "b")))), sourceSchema)
      withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
        withInsertNestedTypeCoercion {
          checkError(
            exception = intercept[AnalysisException] {
              doInsertWithSchemaEvolution(t1, sourceData)
            },
            condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
            parameters = Map(
              "tableName" -> toSQLId(s"${catalogAndNamespace}tbl"),
              "colName" -> "`s`.`c3`")
          )
        }
      }
    }
  }

  test("Insert without evolution: source missing top-level column by position fails") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, salary int, dep string) USING $v2Format")
      doInsert(t1, Seq((0, 100, "sales")).toDF("id", "salary", "dep"))
      checkError(
        exception = intercept[AnalysisException] {
          doInsert(t1, Seq((1, 200)).toDF("id", "salary"))
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> toSQLId(s"${catalogAndNamespace}tbl"),
          "tableColumns" -> "`id`, `salary`, `dep`",
          "dataColumns" -> "`id`, `salary`")
      )
    }
  }

  test("Insert without evolution: source missing nested struct field by name fails") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, s struct<c1:int,c2:string,c3:boolean>) USING $v2Format")
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", BooleanType))))))
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Row(1, "a", true)))), targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Row(10, "b")))), sourceSchema)
      val ex = intercept[AnalysisException] {
        doInsertByName(t1, sourceData)
      }
      assert(ex.getMessage.contains("Cannot find data"))
    }
  }

  test("Insert with evolution but without coercion flag:" +
      " source missing nested struct field by name fails") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, s struct<c1:int,c2:string,c3:boolean>) USING $v2Format")
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", BooleanType))))))
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Row(1, "a", true)))), targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Row(10, "b")))), sourceSchema)
      val ex = intercept[AnalysisException] {
        doInsertWithSchemaEvolution(t1, sourceData, byName = true)
      }
      assert(ex.getMessage.contains("Cannot find data"))
    }
  }

  test("Insert without evolution: source missing nested struct field by position fails") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      val targetSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", BooleanType))))))
      sql(s"CREATE TABLE $t1 (id int, s struct<c1:int,c2:string,c3:boolean>) USING $v2Format")
      val targetData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(0, Row(1, "a", true)))), targetSchema)
      doInsert(t1, targetData)

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType))))))
      val sourceData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, Row(10, "b")))), sourceSchema)
      checkError(
        exception = intercept[AnalysisException] {
          doInsert(t1, sourceData)
        },
        condition = "INCOMPATIBLE_DATA_FOR_TABLE.STRUCT_MISSING_FIELDS",
        parameters = Map(
          "tableName" -> toSQLId(s"${catalogAndNamespace}tbl"),
          "colName" -> "`s`",
          "missingFields" -> "`c3`")
      )
    }
  }
}
