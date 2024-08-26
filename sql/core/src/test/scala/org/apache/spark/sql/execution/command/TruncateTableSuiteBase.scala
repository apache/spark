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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `TRUNCATE TABLE` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.TruncateTableSuite`
 *   - V1 table catalog tests: `org.apache.spark.sql.execution.command.v1.TruncateTableSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.TruncateTableSuite`
 *     - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.TruncateTableSuite`
 */
trait TruncateTableSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "TRUNCATE TABLE"

  test("table does not exist") {
    withNamespaceAndTable("ns", "does_not_exist") { t =>
      val parsed = CatalystSqlParser.parseMultipartIdentifier(t)
        .map(part => quoteIdentifier(part)).mkString(".")
      val e = intercept[AnalysisException] {
        sql(s"TRUNCATE TABLE $t")
      }
      checkErrorTableNotFound(e, parsed, ExpectedContext(t, 15, 14 + t.length))
    }
  }

  test("truncate non-partitioned table") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (c0 INT, c1 INT) $defaultUsing")
      sql(s"INSERT INTO $t SELECT 0, 1")

      sql(s"TRUNCATE TABLE $t")
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Nil)
    }
  }

  protected def createPartTable(t: String): Unit = {
    sql(s"""
      |CREATE TABLE $t (width INT, length INT, height INT)
      |$defaultUsing
      |PARTITIONED BY (width, length)""".stripMargin)
    sql(s"INSERT INTO $t PARTITION (width = 0, length = 0) SELECT 0")
    sql(s"INSERT INTO $t PARTITION (width = 1, length = 1) SELECT 1")
    sql(s"INSERT INTO $t PARTITION (width = 1, length = 2) SELECT 3")
  }

  test("SPARK-34418: truncate partitioned tables") {
    withNamespaceAndTable("ns", "partTable") { t =>
      createPartTable(t)
      sql(s"TRUNCATE TABLE $t PARTITION (width = 1, length = 1)")
      checkAnswer(sql(s"SELECT width, length, height FROM $t"), Seq(Row(0, 0, 0), Row(1, 2, 3)))
      checkPartitions(t,
        Map("width" -> "0", "length" -> "0"),
        Map("width" -> "1", "length" -> "1"),
        Map("width" -> "1", "length" -> "2"))
    }

    withNamespaceAndTable("ns", "partTable") { t =>
      createPartTable(t)
      // support partial partition spec
      sql(s"TRUNCATE TABLE $t PARTITION (width = 1)")
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Row(0, 0, 0) :: Nil)
      checkPartitions(t,
        Map("width" -> "0", "length" -> "0"),
        Map("width" -> "1", "length" -> "1"),
        Map("width" -> "1", "length" -> "2"))
    }

    withNamespaceAndTable("ns", "partTable") { t =>
      createPartTable(t)
      // do nothing if no partition is matched for the given partial partition spec
      sql(s"TRUNCATE TABLE $t PARTITION (width = 100)")
      QueryTest.checkAnswer(
        sql(s"SELECT width, length, height FROM $t"),
        Seq(Row(0, 0, 0), Row(1, 1, 1), Row(1, 2, 3)))

      // throw exception if no partition is matched for the given non-partial partition spec.
      intercept[NoSuchPartitionException] {
        sql(s"TRUNCATE TABLE $t PARTITION (width = 100, length = 100)")
      }

      // throw exception if the column in partition spec is not a partition column.
      val errMsg = intercept[AnalysisException] {
        sql(s"TRUNCATE TABLE $t PARTITION (unknown = 1)")
      }.getMessage
      assert(errMsg.contains("unknown is not a valid partition column"))
    }
  }

  protected def invalidPartColumnError: String

  test("truncate a partition of non partitioned table") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (c0 INT) $defaultUsing")
      sql(s"INSERT INTO $t SELECT 0")

      val errMsg = intercept[AnalysisException] {
        sql(s"TRUNCATE TABLE $t PARTITION (c0=1)")
      }.getMessage
      assert(errMsg.contains(invalidPartColumnError))
    }
  }

  test("SPARK-34418: preserve partitions in truncated table") {
    withNamespaceAndTable("ns", "partTable") { t =>
      createPartTable(t)
      checkAnswer(
        sql(s"SELECT width, length, height FROM $t"),
        Seq(Row(0, 0, 0), Row(1, 1, 1), Row(1, 2, 3)))
      sql(s"TRUNCATE TABLE $t")
      checkAnswer(sql(s"SELECT width, length, height FROM $t"), Nil)
      checkPartitions(t,
        Map("width" -> "0", "length" -> "0"),
        Map("width" -> "1", "length" -> "1"),
        Map("width" -> "1", "length" -> "2"))
    }
  }

  test("case sensitivity in resolving partition specs") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      sql(s"INSERT INTO $t PARTITION (id=0) SELECT 'abc'")
      sql(s"INSERT INTO $t PARTITION (id=1) SELECT 'def'")
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val errMsg = intercept[AnalysisException] {
          sql(s"TRUNCATE TABLE $t PARTITION (ID=1)")
        }.getMessage
        assert(errMsg.contains("ID is not a valid partition column"))
      }
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql(s"TRUNCATE TABLE $t PARTITION (ID=1)")
        QueryTest.checkAnswer(sql(s"SELECT id, data FROM $t"), Row(0, "abc") :: Nil)
      }
    }
  }

  test("SPARK-34215: keep table cached after truncation") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (c0 int) $defaultUsing")
      sql(s"INSERT INTO $t SELECT 0")
      sql(s"CACHE TABLE $t")
      assert(spark.catalog.isCached(t))
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Row(0) :: Nil)
      sql(s"TRUNCATE TABLE $t")
      assert(spark.catalog.isCached(t))
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Nil)
    }
  }

  test("truncation of views is not allowed") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")

      withView("v0") {
        sql(s"CREATE VIEW v0 AS SELECT * FROM $t")
        checkError(
          exception = intercept[AnalysisException] {
            sql("TRUNCATE TABLE v0")
          },
          errorClass = "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE",
          parameters = Map(
            "viewName" -> "`spark_catalog`.`default`.`v0`",
            "operation" -> "TRUNCATE TABLE"),
          context = ExpectedContext(
            fragment = "v0",
            start = 15,
            stop = 16)
        )
      }

      withTempView("v1") {
        sql(s"CREATE TEMP VIEW v1 AS SELECT * FROM $t")
        checkError(
          exception = intercept[AnalysisException] {
            sql("TRUNCATE TABLE v1")
          },
          errorClass = "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE",
          parameters = Map(
            "viewName" -> "`v1`",
            "operation" -> "TRUNCATE TABLE"),
          context = ExpectedContext(fragment = "v1", start = 15, stop = 16)
        )
      }

      val v2 = s"${spark.sharedState.globalTempDB}.v2"
      withGlobalTempView("v2") {
        sql(s"CREATE GLOBAL TEMP VIEW v2 AS SELECT * FROM $t")
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"TRUNCATE TABLE $v2")
          },
          errorClass = "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE",
          parameters = Map(
            "viewName" -> "`global_temp`.`v2`",
            "operation" -> "TRUNCATE TABLE"),
          context = ExpectedContext(fragment = v2, start = 15, stop = 28)
        )
      }
    }
  }

  test("keep dependents as cached after table truncation") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createPartTable(t)
      cacheRelation(t)
      QueryTest.checkAnswer(
        sql(s"SELECT width, length, height FROM $t"),
        Seq(Row(0, 0, 0), Row(1, 1, 1), Row(1, 2, 3)))

      withView("v0") {
        sql(s"CREATE VIEW v0 AS SELECT * FROM $t")
        cacheRelation("v0")
        sql(s"TRUNCATE TABLE $t PARTITION (width = 1, length = 2)")
        checkCachedRelation("v0", Seq(Row(0, 0, 0), Row(1, 1, 1)))
      }

      withTempView("v1") {
        sql(s"CREATE TEMP VIEW v1 AS SELECT * FROM $t")
        cacheRelation("v1")
        sql(s"TRUNCATE TABLE $t PARTITION (width = 1, length = 1)")
        checkCachedRelation("v1", Seq(Row(0, 0, 0)))
      }

      val v2 = s"${spark.sharedState.globalTempDB}.v2"
      withGlobalTempView("v2") {
        sql(s"INSERT INTO $t PARTITION (width = 10, length = 10) SELECT 10")
        sql(s"CREATE GLOBAL TEMP VIEW v2 AS SELECT * FROM $t")
        cacheRelation(v2)
        sql(s"TRUNCATE TABLE $t PARTITION (width = 10, length = 10)")
        checkCachedRelation(v2, Seq(Row(0, 0, 0)))
      }
    }
  }
}
