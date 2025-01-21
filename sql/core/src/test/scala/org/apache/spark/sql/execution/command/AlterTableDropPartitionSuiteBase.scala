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
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionsException
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `ALTER TABLE .. DROP PARTITION` command that
 * check V1 and V2 table catalogs. The tests that cannot run for all supported catalogs are
 * located in more specific test suites:
 *
 *   - V2 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v2.AlterTableDropPartitionSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.AlterTableDropPartitionSuiteBase`
 *     - V1 In-Memory catalog:
 *       `org.apache.spark.sql.execution.command.v1.AlterTableDropPartitionSuite`
 *     - V1 Hive External catalog:
 *       `org.apache.spark.sql.hive.execution.command.AlterTableDropPartitionSuite`
 */
trait AlterTableDropPartitionSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER TABLE .. DROP PARTITION"

  protected def notFullPartitionSpecErr: String
  protected def nullPartitionValue: String

  protected def checkDropPartition(
      t: String,
      ifExists: String,
      specs: Map[String, Any]*): Unit = {
    checkPartitions(t,
      specs.map(_.map { case (k, v) => (k, v.toString) }.toMap): _*)
    val specStr = specs.map(partSpecToString).mkString(", ")
    sql(s"ALTER TABLE $t DROP $ifExists $specStr")
    checkPartitions(t)
  }

  test("single partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      Seq("", "IF EXISTS").foreach { ifExists =>
        sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'")
        checkDropPartition(t, ifExists, Map("id" -> 1))
      }
    }
  }

  test("multiple partitions") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      Seq("", "IF EXISTS").foreach { ifExists =>
        sql(s"""
          |ALTER TABLE $t ADD
          |PARTITION (id=1) LOCATION 'loc'
          |PARTITION (id=2) LOCATION 'loc1'""".stripMargin)
        checkDropPartition(t, ifExists, Map("id" -> 1), Map("id" -> 2))
      }
    }
  }

  test("multi-part partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, a int, b string) $defaultUsing PARTITIONED BY (a, b)")
      Seq("", "IF EXISTS").foreach { ifExists =>
        sql(s"ALTER TABLE $t ADD PARTITION (a = 2, b = 'abc')")
        checkDropPartition(t, ifExists, Map("a" -> 2, "b" -> "abc"))
      }
    }
  }

  test("table to alter does not exist") {
    withNamespaceAndTable("ns", "does_not_exist") { t =>
      val parsed = CatalystSqlParser.parseMultipartIdentifier(t)
        .map(part => quoteIdentifier(part)).mkString(".")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP PARTITION (a='4', b='9')")
      }
      checkErrorTableNotFound(e, parsed,
        ExpectedContext(t, 12, 11 + t.length))
    }
  }

  test("case sensitivity in resolving partition specs") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val expectedTableName = if (commandVersion == DDLCommandTestUtils.V1_COMMAND_VERSION) {
          s"`$SESSION_CATALOG_NAME`.`ns`.`tbl`"
        } else {
          "`test_catalog`.`ns`.`tbl`"
        }
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"ALTER TABLE $t DROP PARTITION (ID=1)")
          },
          condition = "PARTITIONS_NOT_FOUND",
          parameters = Map(
            "partitionList" -> "`ID`",
            "tableName" -> expectedTableName)
        )
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        Seq("", "IF EXISTS").foreach { ifExists =>
          sql(s"ALTER TABLE $t ADD PARTITION (ID=1) LOCATION 'loc1'")
          checkDropPartition(t, ifExists, Map("id" -> 1))
        }
      }
    }
  }

  test("SPARK-33676: not fully specified partition spec") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"""
        |CREATE TABLE $t (id bigint, part0 int, part1 string)
        |$defaultUsing
        |PARTITIONED BY (part0, part1)""".stripMargin)
      val errMsg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ADD PARTITION (part0 = 1)")
      }.getMessage
      assert(errMsg.contains("Partition spec is invalid. " +
        "The spec (part0) must match the partition spec (part0, part1)"))
    }
  }

  test("partition not exists") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'")

      val e = intercept[NoSuchPartitionsException] {
        sql(s"ALTER TABLE $t DROP PARTITION (id=1), PARTITION (id=2)")
      }
      val expectedTableName = if (commandVersion == DDLCommandTestUtils.V1_COMMAND_VERSION) {
        "`ns`.`tbl`"
      } else {
        "`test_catalog`.`ns`.`tbl`"
      }
      checkError(e,
        condition = "PARTITIONS_NOT_FOUND",
        parameters = Map("partitionList" -> "PARTITION (`id` = 2)",
        "tableName" -> expectedTableName))

      checkPartitions(t, Map("id" -> "1"))
      sql(s"ALTER TABLE $t DROP IF EXISTS PARTITION (id=1), PARTITION (id=2)")
      checkPartitions(t)
    }
  }

  test("SPARK-33990: do not return data from dropped partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(0, 0), Row(1, 1)))
      sql(s"ALTER TABLE $t DROP PARTITION (part=0)")
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(1, 1)))
    }
  }

  test("SPARK-33950, SPARK-33987: refresh cache after partition dropping") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")
      assert(!spark.catalog.isCached(t))
      sql(s"CACHE TABLE $t")
      assert(spark.catalog.isCached(t))
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(0, 0), Row(1, 1)))
      sql(s"ALTER TABLE $t DROP PARTITION (part=0)")
      assert(spark.catalog.isCached(t))
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(1, 1)))
    }
  }

  test("SPARK-33591: null as a partition value") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 INT, p1 STRING) $defaultUsing PARTITIONED BY (p1)")
      sql(s"ALTER TABLE $t ADD PARTITION (p1 = null)")
      checkPartitions(t, Map("p1" -> nullPartitionValue))
      sql(s"ALTER TABLE $t DROP PARTITION (p1 = null)")
      checkPartitions(t)
    }
  }

  test("SPARK-33591, SPARK-34203: insert and drop partitions with null values") {
    def insertAndDropNullPart(t: String, insertCmd: String): Unit = {
      sql(s"CREATE TABLE $t (col1 INT, p1 STRING) $defaultUsing PARTITIONED BY (p1)")
      sql(insertCmd)
      checkPartitions(t, Map("p1" -> nullPartitionValue))
      sql(s"ALTER TABLE $t DROP PARTITION (p1 = null)")
      checkPartitions(t)
    }

    withNamespaceAndTable("ns", "tbl") { t =>
      insertAndDropNullPart(t, s"INSERT INTO TABLE $t PARTITION (p1 = null) SELECT 0")
    }

    withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
      withNamespaceAndTable("ns", "tbl") { t =>
        insertAndDropNullPart(t, s"INSERT OVERWRITE TABLE $t VALUES (0, null)")
      }
    }
  }

  test("SPARK-34161, SPARK-34138, SPARK-34099: keep dependents cached after table altering") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")
      sql(s"INSERT INTO $t PARTITION (part=2) SELECT 2")
      sql(s"INSERT INTO $t PARTITION (part=3) SELECT 3")
      cacheRelation(t)
      checkCachedRelation(t, Seq(Row(0, 0), Row(1, 1), Row(2, 2), Row(3, 3)))

      withView("v0") {
        sql(s"CREATE VIEW v0 AS SELECT * FROM $t")
        cacheRelation("v0")
        sql(s"ALTER TABLE $t DROP PARTITION (part=1)")
        checkCachedRelation("v0", Seq(Row(0, 0), Row(2, 2), Row(3, 3)))
      }

      withTempView("v1") {
        sql(s"CREATE TEMP VIEW v1 AS SELECT * FROM $t")
        cacheRelation("v1")
        sql(s"ALTER TABLE $t DROP PARTITION (part=2)")
        checkCachedRelation("v1", Seq(Row(0, 0), Row(3, 3)))
      }

      val v2 = s"${spark.sharedState.globalTempDB}.v2"
      withGlobalTempView("v2") {
        sql(s"CREATE GLOBAL TEMP VIEW v2 AS SELECT * FROM $t")
        cacheRelation(v2)
        sql(s"ALTER TABLE $t DROP PARTITION (part=3)")
        checkCachedRelation(v2, Seq(Row(0, 0)))
      }
    }
  }

  test("SPARK-33474: Support typed literals as partition spec values") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t(name STRING, part DATE) USING PARQUET PARTITIONED BY (part)")
      sql(s"ALTER TABLE $t ADD PARTITION(part = date'2020-01-01')")
      checkPartitions(t, Map("part" -> "2020-01-01"))
      sql(s"ALTER TABLE $t DROP PARTITION (part = date'2020-01-01')")
      checkPartitions(t)
    }
  }

  test("SPARK-41982: drop partition when keepPartitionSpecAsString set `true`") {
    withSQLConf(SQLConf.LEGACY_KEEP_PARTITION_SPEC_AS_STRING_LITERAL.key -> "true") {
      withNamespaceAndTable("ns", "tbl") { t =>
        sql(s"CREATE TABLE $t(name STRING, age INT) using orc PARTITIONED BY (dt STRING)")
        sql(s"ALTER TABLE $t ADD PARTITION(dt = 08)")
        checkPartitions(t, Map("dt" -> "08"))
        sql(s"ALTER TABLE $t DROP PARTITION (dt = 08)")
        checkPartitions(t)
        sql(s"ALTER TABLE $t ADD PARTITION(dt = '08')")
        checkPartitions(t, Map("dt" -> "08"))
        sql(s"ALTER TABLE $t DROP PARTITION (dt = '08')")
        checkPartitions(t)
      }
    }

    withSQLConf(SQLConf.LEGACY_KEEP_PARTITION_SPEC_AS_STRING_LITERAL.key -> "false") {
      withNamespaceAndTable("ns", "tb2") { t =>
        sql(s"CREATE TABLE $t(name STRING, age INT) using orc PARTITIONED BY (dt STRING)")
        sql(s"ALTER TABLE $t ADD PARTITION(dt = 08)")
        checkPartitions(t, Map("dt" -> "8"))
        sql(s"ALTER TABLE $t DROP PARTITION (dt = 08)")
        checkPartitions(t)
        sql(s"ALTER TABLE $t ADD PARTITION(dt = 08)")
        checkPartitions(t, Map("dt" -> "8"))
        sql(s"ALTER TABLE $t DROP PARTITION (dt = 8)")
        checkPartitions(t)
      }
    }
  }

  test("SPARK-42480: drop partition when dropPartitionByName enabled") {
    withSQLConf(SQLConf.HIVE_METASTORE_DROP_PARTITION_BY_NAME.key -> "true") {
      withNamespaceAndTable("ns", "tbl") { t =>
        sql(s"CREATE TABLE $t(name STRING, age INT) USING PARQUET PARTITIONED BY (region STRING)")
        sql(s"ALTER TABLE $t ADD PARTITION (region='=reg1') LOCATION 'loc1'")
        checkPartitions(t, Map("region" -> "=reg1"))
        sql(s"ALTER TABLE $t PARTITION (region='=reg1') RENAME TO PARTITION (region='=%reg1')")
        checkPartitions(t, Map("region" -> "=%reg1"))
        sql(s"ALTER TABLE $t DROP PARTITION (region='=%reg1')")
        checkPartitions(t)
        sql(s"ALTER TABLE $t ADD PARTITION (region='reg?2') LOCATION 'loc2'")
        checkPartitions(t, Map("region" -> "reg?2"))
        sql(s"ALTER TABLE $t DROP PARTITION (region='reg?2')")
        checkPartitions(t)
      }
    }
  }
}
