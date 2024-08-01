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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.sources.SimpleInsertSource
import org.apache.spark.util.Utils

/**
 * This base suite contains unified tests for the `SHOW CREATE TABLE` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.ShowCreateTableSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.ShowCreateTableSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.ShowCreateTableSuite`
 *     - V1 Hive External catalog:
*        `org.apache.spark.sql.hive.execution.command.ShowCreateTableSuite`
 */
trait ShowCreateTableSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "SHOW CREATE TABLE"
  protected def ns: String = "ns1"
  protected def table: String = "tbl"
  protected def fullName: String

  test("SPARK-36012: add null flag when show create table") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""
           |CREATE TABLE $t (
           |  a bigint NOT NULL,
           |  b bigint
           |)
           |USING ${classOf[SimpleInsertSource].getName}
        """.stripMargin)
      val showDDL = getShowCreateDDL(t)
      assert(showDDL(0) == s"CREATE TABLE $fullName (")
      assert(showDDL(1) == "a BIGINT NOT NULL,")
      assert(showDDL(2) == "b BIGINT)")
      assert(showDDL(3) == s"USING ${classOf[SimpleInsertSource].getName}")
    }
  }

  test("data source table with user specified schema") {
    withNamespaceAndTable(ns, table) { t =>
      val jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
      sql(
        s"""CREATE TABLE $t (
           |  a STRING,
           |  b STRING,
           |  `extra col` ARRAY<INT>,
           |  `<another>` STRUCT<x: INT, y: ARRAY<BOOLEAN>>
           |)
           |USING json
           |OPTIONS (
           | PATH '$jsonFilePath'
           |)
         """.stripMargin
      )
      val showDDL = getShowCreateDDL(t)
      assert(showDDL(0) == s"CREATE TABLE $fullName (")
      assert(showDDL(1) == "a STRING,")
      assert(showDDL(2) == "b STRING,")
      assert(showDDL(3) == "`extra col` ARRAY<INT>,")
      assert(showDDL(4) == "`<another>` STRUCT<x: INT, y: ARRAY<BOOLEAN>>)")
      assert(showDDL(5) == "USING json")
      assert(showDDL(6).startsWith("LOCATION 'file:") && showDDL(6).endsWith("sample.json'"))
    }
  }

  test("SPARK-24911: keep quotes for nested fields") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""
           |CREATE TABLE $t (
           |  `a` STRUCT<`b`: STRING>
           |)
           |USING json
        """.stripMargin)
      val showDDL = getShowCreateDDL(t)
      assert(showDDL(0) == s"CREATE TABLE $fullName (")
      assert(showDDL(1) == "a STRUCT<b: STRING>)")
      assert(showDDL(2) == "USING json")
    }
  }

  test("SPARK-37494: Unify v1 and v2 option output") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t (
           |  a STRING
           |)
           |USING json
           |TBLPROPERTIES (
           | 'b' = '1',
           | 'a' = '2')
           |OPTIONS (
           | k4 'v4',
           | `k3` 'v3',
           | 'k5' 'v5',
           | 'k1' = 'v1',
           | k2 = 'v2'
           |)
         """.stripMargin
      )
      val expected = s"CREATE TABLE $fullName ( a STRING) USING json" +
        " OPTIONS ( 'k1' = 'v1', 'k2' = 'v2', 'k3' = 'v3', 'k4' = 'v4', 'k5' = 'v5')" +
        " TBLPROPERTIES ( 'a' = '2', 'b' = '1')"
      assert(getShowCreateDDL(t).mkString(" ") == expected)
    }
  }

  test("data source table CTAS") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t
           |USING json
           |AS SELECT 1 AS a, "foo" AS b
         """.stripMargin
      )
      val expected = s"CREATE TABLE $fullName ( a INT, b STRING) USING json"
      assert(getShowCreateDDL(t).mkString(" ") == expected)
    }
  }

  test("partitioned data source table") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t
           |USING json
           |PARTITIONED BY (b)
           |AS SELECT 1 AS a, "foo" AS b
         """.stripMargin
      )
      val expected = s"CREATE TABLE $fullName ( a INT, b STRING) USING json PARTITIONED BY (b)"
      assert(getShowCreateDDL(t).mkString(" ") == expected)
    }
  }

  test("data source table with a comment") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t
           |USING json
           |COMMENT 'This is a comment'
           |AS SELECT 1 AS a, "foo" AS b, 2.5 AS c
         """.stripMargin
      )
      val expected = s"CREATE TABLE $fullName ( a INT, b STRING, c DECIMAL(2,1)) USING json" +
        s" COMMENT 'This is a comment'"
      assert(getShowCreateDDL(t).mkString(" ") == expected)
    }
  }

  test("data source table with table properties") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t
           |USING json
           |TBLPROPERTIES ('a' = '1')
           |AS SELECT 1 AS a, "foo" AS b, 2.5 AS c
         """.stripMargin
      )
      val expected = s"CREATE TABLE $fullName ( a INT, b STRING, c DECIMAL(2,1)) USING json" +
        s" TBLPROPERTIES ( 'a' = '1')"
      assert(getShowCreateDDL(t).mkString(" ") == expected)
    }
  }

  def getShowCreateDDL(table: String, serde: Boolean = false): Array[String] = {
    val result = if (serde) {
      sql(s"SHOW CREATE TABLE $table AS SERDE")
    } else {
      sql(s"SHOW CREATE TABLE $table")
    }
    result.head().getString(0).split("\n").map(_.trim)
  }
}
