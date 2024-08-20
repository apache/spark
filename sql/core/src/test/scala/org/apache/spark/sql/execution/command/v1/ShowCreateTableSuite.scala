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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.execution.command.DDLCommandTestUtils.V1_COMMAND_VERSION

/**
 * This base suite contains unified tests for the `SHOW CREATE TABLE` command that checks V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.ShowCreateTableSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.ShowCreateTableSuite`
 */
trait ShowCreateTableSuiteBase extends command.ShowCreateTableSuiteBase
    with command.TestsV1AndV2Commands {
  override def fullName: String = commandVersion match {
    case V1_COMMAND_VERSION => s"$ns.$table"
    case _ => s"$catalog.$ns.$table"
  }

  test("show create table[simple]") {
    // todo After SPARK-37517 unify the testcase both v1 and v2
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""
           |CREATE TABLE $t (
           |  a bigint NOT NULL,
           |  b bigint,
           |  c bigint,
           |  `extraCol` ARRAY<INT>,
           |  `<another>` STRUCT<x: INT, y: ARRAY<BOOLEAN>>
           |)
           |using parquet
           |OPTIONS (
           |  from = 0,
           |  to = 1,
           |  via = 2)
           |COMMENT 'This is a comment'
           |TBLPROPERTIES ('prop1' = '1', 'prop2' = '2', 'prop3' = 3, 'prop4' = 4)
           |PARTITIONED BY (a)
           |LOCATION 'file:/tmp'
        """.stripMargin)
      val showDDL = getShowCreateDDL(t)
      assert(showDDL === Array(
        s"CREATE TABLE $fullName (",
        "b BIGINT,",
        "c BIGINT,",
        "extraCol ARRAY<INT>,",
        "`<another>` STRUCT<x: INT, y: ARRAY<BOOLEAN>>,",
        "a BIGINT NOT NULL)",
        "USING parquet",
        "OPTIONS (",
        "'from' = '0',",
        "'to' = '1',",
        "'via' = '2')",
        "PARTITIONED BY (a)",
        "COMMENT 'This is a comment'",
        "LOCATION 'file:/tmp'",
        "TBLPROPERTIES (",
        "'prop1' = '1',",
        "'prop2' = '2',",
        "'prop3' = '3',",
        "'prop4' = '4')"
      ))
    }
  }

  test("bucketed data source table") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t
           |USING json
           |CLUSTERED BY (a) INTO 2 BUCKETS
           |AS SELECT 1 AS a, "foo" AS b
         """.stripMargin
      )
      val expected = s"CREATE TABLE $fullName ( a INT, b STRING) USING json" +
        s" CLUSTERED BY (a) INTO 2 BUCKETS"
      assert(getShowCreateDDL(t).mkString(" ") == expected)
    }
  }

  test("sort bucketed data source table") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t
           |USING json
           |CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS
           |AS SELECT 1 AS a, "foo" AS b
         """.stripMargin
      )
      val expected = s"CREATE TABLE $fullName ( a INT, b STRING) USING json" +
        s" CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS"
      assert(getShowCreateDDL(t).mkString(" ") == expected)
    }
  }

  test("partitioned bucketed data source table") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t
           |USING json
           |PARTITIONED BY (c)
           |CLUSTERED BY (a) INTO 2 BUCKETS
           |AS SELECT 1 AS a, "foo" AS b, 2.5 AS c
         """.stripMargin
      )
      val expected = s"CREATE TABLE $fullName ( a INT, b STRING, c DECIMAL(2,1)) USING json" +
        s" PARTITIONED BY (c) CLUSTERED BY (a) INTO 2 BUCKETS"
      assert(getShowCreateDDL(t).mkString(" ") == expected)
    }
  }

  test("partitioned sort bucketed data source table") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t
           |USING json
           |PARTITIONED BY (c)
           |CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS
           |AS SELECT 1 AS a, "foo" AS b, 2.5 AS c
         """.stripMargin
      )
      val expected = s"CREATE TABLE $fullName ( a INT, b STRING, c DECIMAL(2,1)) USING json" +
        s" PARTITIONED BY (c) CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS"
      assert(getShowCreateDDL(t).mkString(" ") == expected)
    }
  }

  test("show create table as serde can't work on data source table") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""
           |CREATE TABLE $t (
           |  c1 STRING COMMENT 'bla',
           |  c2 STRING
           |)
           |USING orc
         """.stripMargin
      )

      checkError(
        exception = intercept[AnalysisException] {
          getShowCreateDDL(t, true)
        },
        errorClass = "UNSUPPORTED_SHOW_CREATE_TABLE.ON_DATA_SOURCE_TABLE_WITH_AS_SERDE",
        sqlState = "0A000",
        parameters = Map("table" -> "`spark_catalog`.`ns1`.`tbl`")
      )
    }
  }

  test("show create table with default column values") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""
           |CREATE TABLE $t (
           |  a bigint NOT NULL,
           |  b bigint DEFAULT 42,
           |  c string DEFAULT 'abc, "def"' COMMENT 'comment'
           |)
           |USING parquet
           |COMMENT 'This is a comment'
        """.stripMargin)
      val showDDL = getShowCreateDDL(t)
      assert(showDDL === Array(
        s"CREATE TABLE $fullName (",
        "a BIGINT,",
        "b BIGINT DEFAULT 42,",
        "c STRING DEFAULT 'abc, \"def\"' COMMENT 'comment')",
        "USING parquet",
        "COMMENT 'This is a comment'"
      ))
    }
  }
}

/**
 * The class contains tests for the `SHOW CREATE TABLE` command to check V1 In-Memory
 * table catalog.
 */
class ShowCreateTableSuite extends ShowCreateTableSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[ShowCreateTableSuiteBase].commandVersion
}
