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
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, PartitionsAlreadyExistException}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `ALTER TABLE .. RENAME PARTITION` command that
 * check V1 and V2 table catalogs. The tests that cannot run for all supported catalogs are
 * located in more specific test suites:
 *
 *   - V2 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v2.AlterTableRenamePartitionSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.AlterTableRenamePartitionSuiteBase`
 *     - V1 In-Memory catalog:
 *       `org.apache.spark.sql.execution.command.v1.AlterTableRenamePartitionSuite`
 *     - V1 Hive External catalog:
 *       `org.apache.spark.sql.hive.execution.command.AlterTableRenamePartitionSuite`
 */
trait AlterTableRenamePartitionSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER TABLE .. RENAME PARTITION"

  protected def createSinglePartTable(t: String): Unit = {
    sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
    sql(s"INSERT INTO $t PARTITION (id = 1) SELECT 'abc'")
  }

  test("rename without explicitly specifying database") {
    withSQLConf(SQLConf.DEFAULT_CATALOG.key -> catalog) {
      createSinglePartTable("t")
      checkPartitions("t", Map("id" -> "1"))

      sql(s"ALTER TABLE t PARTITION (id = 1) RENAME TO PARTITION (id = 2)")
      checkPartitions("t", Map("id" -> "2"))
      checkAnswer(sql(s"SELECT id, data FROM t"), Row(2, "abc"))
    }
  }

  test("table to alter does not exist") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $catalog.ns.no_tbl PARTITION (id=1) RENAME TO PARTITION (id=2)")
      }
      checkErrorTableNotFound(e, s"`$catalog`.`ns`.`no_tbl`",
        ExpectedContext(s"$catalog.ns.no_tbl", 12, 11 + s"$catalog.ns.no_tbl".length))
    }
  }

  test("partition to rename does not exist") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      checkPartitions(t, Map("id" -> "1"))
      val parsed = if (commandVersion == DDLCommandTestUtils.V1_COMMAND_VERSION) {
        "`ns`.`tbl`"
      } else {
        CatalystSqlParser.parseMultipartIdentifier(t)
          .map(part => quoteIdentifier(part)).mkString(".")
      }
      val e = intercept[NoSuchPartitionException] {
        sql(s"ALTER TABLE $t PARTITION (id = 3) RENAME TO PARTITION (id = 2)")
      }
      checkError(e,
        condition = "PARTITIONS_NOT_FOUND",
        parameters = Map("partitionList" -> "PARTITION (`id` = 3)",
          "tableName" -> parsed))
    }
  }

  test("target partitions exist") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      sql(s"INSERT INTO $t PARTITION (id = 2) SELECT 'def'")
      checkPartitions(t, Map("id" -> "1"), Map("id" -> "2"))
      val parsed = if (commandVersion == DDLCommandTestUtils.V1_COMMAND_VERSION) {
        "`ns`.`tbl`"
      } else {
        CatalystSqlParser.parseMultipartIdentifier(t)
          .map(part => quoteIdentifier(part)).mkString(".")
      }

      val e = intercept[PartitionsAlreadyExistException] {
        sql(s"ALTER TABLE $t PARTITION (id = 1) RENAME TO PARTITION (id = 2)")
      }
      checkError(e,
        condition = "PARTITIONS_ALREADY_EXIST",
        parameters = Map("partitionList" -> "PARTITION (`id` = 2)", "tableName" -> parsed))
    }
  }

  test("single part partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      checkPartitions(t, Map("id" -> "1"))

      sql(s"ALTER TABLE $t PARTITION (id = 1) RENAME TO PARTITION (id = 2)")
      checkPartitions(t, Map("id" -> "2"))
      checkAnswer(sql(s"SELECT id, data FROM $t"), Row(2, "abc"))
    }
  }

  test("multi part partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createWideTable(t)
      checkPartitions(t,
        Map(
          "year" -> "2016",
          "month" -> "3",
          "hour" -> "10",
          "minute" -> "10",
          "sec" -> "10",
          "extra" -> "1"),
        Map(
          "year" -> "2016",
          "month" -> "4",
          "hour" -> "10",
          "minute" -> "10",
          "sec" -> "10",
          "extra" -> "1"))

      sql(s"""
        |ALTER TABLE $t
        |PARTITION (
        |  year = 2016, month = 3, hour = 10, minute = 10, sec = 10, extra = 1
        |) RENAME TO PARTITION (
        |  year = 2016, month = 3, hour = 10, minute = 10, sec = 123, extra = 1
        |)""".stripMargin)
      checkPartitions(t,
        Map(
          "year" -> "2016",
          "month" -> "3",
          "hour" -> "10",
          "minute" -> "10",
          "sec" -> "123",
          "extra" -> "1"),
        Map(
          "year" -> "2016",
          "month" -> "4",
          "hour" -> "10",
          "minute" -> "10",
          "sec" -> "10",
          "extra" -> "1"))
      checkAnswer(sql(s"SELECT month, sec, price FROM $t"), Row(3, 123, 3))
    }
  }

  test("partition spec in RENAME PARTITION should be case insensitive") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      checkPartitions(t, Map("id" -> "1"))

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val errMsg = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t PARTITION (ID = 1) RENAME TO PARTITION (id = 2)")
        }.getMessage
        assert(errMsg.contains("ID is not a valid partition column"))
        checkPartitions(t, Map("id" -> "1"))
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql(s"ALTER TABLE $t PARTITION (ID = 1) RENAME TO PARTITION (id = 2)")
        checkPartitions(t, Map("id" -> "2"))
        checkAnswer(sql(s"SELECT id, data FROM $t"), Row(2, "abc"))
      }
    }
  }

  test("SPARK-34011: refresh cache after partition renaming") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")
      assert(!spark.catalog.isCached(t))
      sql(s"CACHE TABLE $t")
      assert(spark.catalog.isCached(t))
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(0, 0), Row(1, 1)))
      sql(s"ALTER TABLE $t PARTITION (part=0) RENAME TO PARTITION (part=2)")
      assert(spark.catalog.isCached(t))
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(0, 2), Row(1, 1)))
    }
  }

  test("SPARK-34161, SPARK-34138, SPARK-34099: keep dependents cached after table altering") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")
      cacheRelation(t)
      checkCachedRelation(t, Seq(Row(0, 0), Row(1, 1)))

      withView("v0") {
        sql(s"CREATE VIEW v0 AS SELECT * FROM $t")
        cacheRelation("v0")
        sql(s"ALTER TABLE $t PARTITION (part=0) RENAME TO PARTITION (part=2)")
        checkCachedRelation("v0", Seq(Row(0, 2), Row(1, 1)))
      }

      withTempView("v1") {
        sql(s"CREATE TEMP VIEW v1 AS SELECT * FROM $t")
        cacheRelation("v1")
        sql(s"ALTER TABLE $t PARTITION (part=1) RENAME TO PARTITION (part=3)")
        checkCachedRelation("v1", Seq(Row(0, 2), Row(1, 3)))
      }

      val v2 = s"${spark.sharedState.globalTempDB}.v2"
      withGlobalTempView("v2") {
        sql(s"CREATE GLOBAL TEMP VIEW v2 AS SELECT * FROM $t")
        cacheRelation(v2)
        sql(s"ALTER TABLE $t PARTITION (part=2) RENAME TO PARTITION (part=4)")
        checkCachedRelation(v2, Seq(Row(0, 4), Row(1, 3)))
      }
    }
  }

  test("SPARK-33474: Support typed literals as partition spec values") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t(name STRING, part DATE) USING PARQUET PARTITIONED BY (part)")
      sql(s"ALTER TABLE $t ADD PARTITION(part = date'2020-01-01')")
      checkPartitions(t, Map("part" -> "2020-01-01"))
      sql(s"ALTER TABLE $t PARTITION (part = date'2020-01-01')" +
        s" RENAME TO PARTITION (part = date'2020-01-02')")
      checkPartitions(t, Map("part" -> "2020-01-02"))
    }
  }

  test("SPARK-41982: rename partition when keepPartitionSpecAsString set `true`") {
    withSQLConf(SQLConf.LEGACY_KEEP_PARTITION_SPEC_AS_STRING_LITERAL.key -> "true") {
      withNamespaceAndTable("ns", "tbl") { t =>
        sql(s"CREATE TABLE $t(name STRING, age INT) USING PARQUET PARTITIONED BY (dt STRING)")
        sql(s"ALTER TABLE $t ADD PARTITION(dt = 08)")
        checkPartitions(t, Map("dt" -> "08"))
        sql(s"ALTER TABLE $t PARTITION (dt = 08)" +
          s" RENAME TO PARTITION (dt = 09)")
        checkPartitions(t, Map("dt" -> "09"))
        sql(s"ALTER TABLE $t PARTITION (dt = 09)" +
          s" RENAME TO PARTITION (dt = '08')")
        checkPartitions(t, Map("dt" -> "08"))
        sql(s"ALTER TABLE $t PARTITION (dt = '08')" +
          s" RENAME TO PARTITION (dt = '09')")
        checkPartitions(t, Map("dt" -> "09"))
      }
    }

    withSQLConf(SQLConf.LEGACY_KEEP_PARTITION_SPEC_AS_STRING_LITERAL.key -> "false") {
      withNamespaceAndTable("ns", "tb2") { t =>
        sql(s"CREATE TABLE $t(name STRING, age INT) USING PARQUET PARTITIONED BY (dt STRING)")
        sql(s"ALTER TABLE $t ADD PARTITION(dt = 08)")
        checkPartitions(t, Map("dt" -> "8"))
        sql(s"ALTER TABLE $t PARTITION (dt = 08)" +
          s" RENAME TO PARTITION (dt = 09)")
        checkPartitions(t, Map("dt" -> "9"))
        sql(s"ALTER TABLE $t PARTITION (dt = 09)" +
          s" RENAME TO PARTITION (dt = '08')")
        checkPartitions(t, Map("dt" -> "08"))
        sql(s"ALTER TABLE $t PARTITION (dt = '08')" +
          s" RENAME TO PARTITION (dt = '09')")
        checkPartitions(t, Map("dt" -> "09"))
      }
    }
  }
}
