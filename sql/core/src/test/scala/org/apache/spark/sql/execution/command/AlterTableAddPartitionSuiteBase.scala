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

import java.time.{Duration, Period}

import org.apache.spark.SparkNumberFormatException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `ALTER TABLE .. ADD PARTITION` command that
 * check V1 and V2 table catalogs. The tests that cannot run for all supported catalogs are
 * located in more specific test suites:
 *
 *   - V2 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v2.AlterTableAddPartitionSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.AlterTableAddPartitionSuiteBase`
 *     - V1 In-Memory catalog:
 *       `org.apache.spark.sql.execution.command.v1.AlterTableAddPartitionSuite`
 *     - V1 Hive External catalog:
 *       `org.apache.spark.sql.hive.execution.command.AlterTableAddPartitionSuite`
 */
trait AlterTableAddPartitionSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER TABLE .. ADD PARTITION"
  def defaultPartitionName: String

  test("one partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      Seq("", "IF NOT EXISTS").foreach { exists =>
        sql(s"ALTER TABLE $t ADD $exists PARTITION (id=1) LOCATION 'loc'")

        checkPartitions(t, Map("id" -> "1"))
        checkLocation(t, Map("id" -> "1"), "loc")
      }
    }
  }

  test("multiple partitions") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      Seq("", "IF NOT EXISTS").foreach { exists =>
        sql(s"""
          |ALTER TABLE $t ADD $exists
          |PARTITION (id=1) LOCATION 'loc'
          |PARTITION (id=2) LOCATION 'loc1'""".stripMargin)

        checkPartitions(t, Map("id" -> "1"), Map("id" -> "2"))
        checkLocation(t, Map("id" -> "1"), "loc")
        checkLocation(t, Map("id" -> "2"), "loc1")
      }
    }
  }

  test("multi-part partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, a int, b string) $defaultUsing PARTITIONED BY (a, b)")
      Seq("", "IF NOT EXISTS").foreach { exists =>
        sql(s"ALTER TABLE $t ADD $exists PARTITION (a=2, b='abc')")

        checkPartitions(t, Map("a" -> "2", "b" -> "abc"))
      }
    }
  }

  test("table to alter does not exist") {
    withNamespaceAndTable("ns", "does_not_exist") { t =>
      val parsed = CatalystSqlParser.parseMultipartIdentifier(t)
        .map(part => quoteIdentifier(part)).mkString(".")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ADD IF NOT EXISTS PARTITION (a='4', b='9')")
      }
      checkErrorTableNotFound(e, parsed, ExpectedContext(t, 12, 11 + t.length))
    }
  }

  test("case sensitivity in resolving partition specs") {
    withNamespaceAndTable("ns", "tbl") { t =>
      spark.sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val errMsg = intercept[AnalysisException] {
          spark.sql(s"ALTER TABLE $t ADD PARTITION (ID=1) LOCATION 'loc1'")
        }.getMessage
        assert(errMsg.contains("ID is not a valid partition column"))
      }
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        spark.sql(s"ALTER TABLE $t ADD PARTITION (ID=1) LOCATION 'loc1'")
        checkPartitions(t, Map("id" -> "1"))
        checkLocation(t, Map("id" -> "1"), "loc1")
      }
    }
  }

  test("SPARK-33521: universal type conversions of partition values") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"""
        |CREATE TABLE $t (
        |  id int,
        |  part0 tinyint,
        |  part1 smallint,
        |  part2 int,
        |  part3 bigint,
        |  part4 float,
        |  part5 double,
        |  part6 string,
        |  part7 boolean,
        |  part8 date,
        |  part9 timestamp
        |) $defaultUsing
        |PARTITIONED BY (part0, part1, part2, part3, part4, part5, part6, part7, part8, part9)
        |""".stripMargin)
      val partSpec = """
        |  part0 = -1,
        |  part1 = 0,
        |  part2 = 1,
        |  part3 = 2,
        |  part4 = 3.14,
        |  part5 = 3.14,
        |  part6 = 'abc',
        |  part7 = true,
        |  part8 = '2020-11-23',
        |  part9 = '2020-11-23 22:13:10.123456'
        |""".stripMargin
      sql(s"ALTER TABLE $t ADD PARTITION ($partSpec)")
      val expected = Map(
        "part0" -> "-1",
        "part1" -> "0",
        "part2" -> "1",
        "part3" -> "2",
        "part4" -> "3.14",
        "part5" -> "3.14",
        "part6" -> "abc",
        "part7" -> "true",
        "part8" -> "2020-11-23",
        "part9" -> "2020-11-23 22:13:10.123456")
      checkPartitions(t, expected)
      sql(s"ALTER TABLE $t DROP PARTITION ($partSpec)")
      checkPartitions(t) // no partitions
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

  test("SPARK-33474: Support typed literals as partition spec values") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t(name STRING, part DATE) USING PARQUET PARTITIONED BY (part)")
      sql(s"ALTER TABLE $t ADD PARTITION(part = date'2020-01-01')")
      checkPartitions(t, Map("part" ->"2020-01-01"))
    }
  }

  test("SPARK-37261: Add ANSI intervals as partition values") {
    assume(!catalogVersion.contains("Hive")) // Hive catalog doesn't support the interval types

    withNamespaceAndTable("ns", "tbl") { t =>
      sql(
        s"""CREATE TABLE $t (
           | ym INTERVAL YEAR,
           | dt INTERVAL DAY,
           | data STRING) $defaultUsing
           |PARTITIONED BY (ym, dt)""".stripMargin)
      sql(
        s"""ALTER TABLE $t ADD PARTITION (
           | ym = INTERVAL '100' YEAR,
           | dt = INTERVAL '10' DAY
           |) LOCATION 'loc'""".stripMargin)

      checkPartitions(t, Map("ym" -> "INTERVAL '100' YEAR", "dt" -> "INTERVAL '10' DAY"))
      checkLocation(t, Map("ym" -> "INTERVAL '100' YEAR", "dt" -> "INTERVAL '10' DAY"), "loc")

      sql(
        s"""INSERT INTO $t PARTITION (
           | ym = INTERVAL '100' YEAR,
           | dt = INTERVAL '10' DAY) SELECT 'aaa'""".stripMargin)
      sql(
        s"""INSERT INTO $t PARTITION (
           | ym = INTERVAL '1' YEAR,
           | dt = INTERVAL '-1' DAY) SELECT 'bbb'""".stripMargin)

      checkAnswer(
        sql(s"SELECT ym, dt, data FROM $t"),
        Seq(
          Row(Period.ofYears(100), Duration.ofDays(10), "aaa"),
          Row(Period.ofYears(1), Duration.ofDays(-1), "bbb")))
    }
  }

  test("SPARK-40798: Alter partition should verify partition value") {
    def shouldThrowException(policy: SQLConf.StoreAssignmentPolicy.Value): Boolean = policy match {
      case SQLConf.StoreAssignmentPolicy.ANSI | SQLConf.StoreAssignmentPolicy.STRICT =>
        true
      case SQLConf.StoreAssignmentPolicy.LEGACY =>
        false
    }

    SQLConf.StoreAssignmentPolicy.values.foreach { policy =>
      withNamespaceAndTable("ns", "tbl") { t =>
        sql(s"CREATE TABLE $t (c int) $defaultUsing PARTITIONED BY (p int)")

        withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> policy.toString) {
          if (shouldThrowException(policy)) {
            checkError(
              exception = intercept[SparkNumberFormatException] {
                sql(s"ALTER TABLE $t ADD PARTITION (p='aaa')")
              },
              condition = "CAST_INVALID_INPUT",
              parameters = Map(
                "ansiConfig" -> "\"spark.sql.ansi.enabled\"",
                "expression" -> "'aaa'",
                "sourceType" -> "\"STRING\"",
                "targetType" -> "\"INT\""),
              context = ExpectedContext(
                fragment = s"ALTER TABLE $t ADD PARTITION (p='aaa')",
                start = 0,
                stop = 35 + t.length))
          } else {
            sql(s"ALTER TABLE $t ADD PARTITION (p='aaa')")
            checkPartitions(t, Map("p" -> defaultPartitionName))
            sql(s"ALTER TABLE $t DROP PARTITION (p=null)")
          }

          sql(s"ALTER TABLE $t ADD PARTITION (p=null)")
          checkPartitions(t, Map("p" -> defaultPartitionName))
          sql(s"ALTER TABLE $t DROP PARTITION (p=null)")
        }
      }
    }
  }

  test("SPARK-41982: add partition when keepPartitionSpecAsString set `true`") {
    withSQLConf(SQLConf.LEGACY_KEEP_PARTITION_SPEC_AS_STRING_LITERAL.key -> "true") {
      withNamespaceAndTable("ns", "tbl") { t =>
        sql(s"CREATE TABLE $t(name STRING, age INT) USING PARQUET PARTITIONED BY (dt STRING)")
        sql(s"ALTER TABLE $t ADD PARTITION(dt = 08)")
        checkPartitions(t, Map("dt" -> "08"))
        sql(s"ALTER TABLE $t ADD PARTITION(dt = '09')")
        checkPartitions(t, Map("dt" -> "09"), Map("dt" -> "08"))
      }
    }

    withSQLConf(SQLConf.LEGACY_KEEP_PARTITION_SPEC_AS_STRING_LITERAL.key -> "false") {
      withNamespaceAndTable("ns", "tb2") { t =>
        sql(s"CREATE TABLE $t(name STRING, age INT) USING PARQUET PARTITIONED BY (dt STRING)")
        sql(s"ALTER TABLE $t ADD PARTITION(dt = 08)")
        checkPartitions(t, Map("dt" -> "8"))
        sql(s"ALTER TABLE $t ADD PARTITION(dt = '09')")
        checkPartitions(t, Map("dt" -> "09"), Map("dt" -> "8"))
      }
    }
  }
}
