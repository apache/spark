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

import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode}
import org.apache.spark.sql.test.SharedSparkSession

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
    verifyTable(t1, df)
  }

  test("insertInto: append by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")

    doInsert(t1, dfr)
    verifyTable(t1, df)
  }

  test("insertInto: append partitioned table") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      doInsert(t1, df)
      verifyTable(t1, df)
    }
  }

  test("insertInto: overwrite non-partitioned table") {
    val t1 = s"${catalogAndNamespace}tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    val df2 = Seq((4L, "d"), (5L, "e"), (6L, "f")).toDF("id", "data")
    doInsert(t1, df)
    doInsert(t1, df2, SaveMode.Overwrite)
    verifyTable(t1, df2)
  }

  test("insertInto: overwrite partitioned table in static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = s"${catalogAndNamespace}tbl"
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
      doInsert(t1, init)

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
    val exc = intercept[AnalysisException] {
      doInsert(t1, df)
    }

    verifyTable(t1, Seq.empty[(Long, String, String)].toDF("id", "data", "missing"))
    val tableName = if (catalogAndNamespace.isEmpty) s"default.$t1" else t1
    assert(exc.getMessage.contains(s"Cannot write to '$tableName', not enough data columns"))
  }

  test("insertInto: fails when an extra column is present") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val df = Seq((1L, "a", "mango")).toDF("id", "data", "fruit")
      val exc = intercept[AnalysisException] {
        doInsert(t1, df)
      }

      verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"))
      val tableName = if (catalogAndNamespace.isEmpty) s"default.$t1" else t1
      assert(exc.getMessage.contains(s"Cannot write to '$tableName', too many data columns"))
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
  extends QueryTest
  with SharedSparkSession
  with BeforeAndAfter {

  import testImplicits._

  /** Check that the results in `tableName` match the `expected` DataFrame. */
  protected def verifyTable(tableName: String, expected: DataFrame): Unit

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

  private def withTableAndData(tableName: String)(testFn: String => Unit): Unit = {
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
        val e = intercept[AnalysisException] {
          sql(s"INSERT INTO $t2 VALUES (2L, 'dummy')")
        }
        assert(e.getMessage.contains(t2))
        assert(e.getMessage.contains("Table not found"))
      }
    }

    test("InsertInto: append to partitioned table - static clause") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 PARTITION (id = 23) SELECT data FROM $view")
        verifyTable(t1, sql(s"SELECT 23, data FROM $view"))
      }
    }

    test("InsertInto: static PARTITION clause fails with non-partition column") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (data)")

        val exc = intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $t1 PARTITION (id=1) SELECT data FROM $view")
        }

        verifyTable(t1, spark.emptyDataFrame)
        assert(exc.getMessage.contains(
          "PARTITION clause cannot contain a non-partition column name"))
        assert(exc.getMessage.contains("id"))
      }
    }

    test("InsertInto: dynamic PARTITION clause fails with non-partition column") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")

        val exc = intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $t1 PARTITION (data) SELECT * FROM $view")
        }

        verifyTable(t1, spark.emptyDataFrame)
        assert(exc.getMessage.contains(
          "PARTITION clause cannot contain a non-partition column name"))
        assert(exc.getMessage.contains("data"))
      }
    }

    test("InsertInto: overwrite - dynamic clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = s"${catalogAndNamespace}tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'also-deleted')")
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
  }
}
