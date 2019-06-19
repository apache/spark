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
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Test various parser errors.
 */
class ErrorParserSuite extends AnalysisTest {
  import CatalystSqlParser._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  private def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    assert(parsePlan(sqlCommand) == plan)
  }

  def intercept(sqlCommand: String, messages: String*): Unit =
    interceptParseException(CatalystSqlParser.parsePlan)(sqlCommand, messages: _*)

  def intercept(sql: String, line: Int, startPosition: Int, stopPosition: Int,
                messages: String*): Unit = {
    val e = intercept[ParseException](CatalystSqlParser.parsePlan(sql))

    // Check position.
    assert(e.line.isDefined)
    assert(e.line.get === line)
    assert(e.startPosition.isDefined)
    assert(e.startPosition.get === startPosition)
    assert(e.stop.startPosition.isDefined)
    assert(e.stop.startPosition.get === stopPosition)

    // Check messages.
    val error = e.getMessage
    messages.foreach { message =>
      assert(error.contains(message))
    }
  }

  test("no viable input") {
    intercept("select ((r + 1) ", 1, 16, 16,
      "no viable alternative at input", "----------------^^^")
  }

  test("extraneous input") {
    intercept("select 1 1", 1, 9, 10, "extraneous input '1' expecting", "---------^^^")
    intercept("select *\nfrom r as q t", 2, 12, 13, "extraneous input", "------------^^^")
  }

  test("mismatched input") {
    intercept("select * from r order by q from t", 1, 27, 31,
      "mismatched input",
      "---------------------------^^^")
    intercept("select *\nfrom r\norder by q\nfrom t", 4, 0, 4, "mismatched input", "^^^")
  }

  test("semantic errors") {
    intercept("select *\nfrom r\norder by q\ncluster by q", 3, 0, 11,
      "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported",
      "^^^")
  }

  test("SPARK-21136: misleading error message due to problematic antlr grammar") {
    intercept("select * from a left joinn b on a.id = b.id", "missing 'JOIN' at 'joinn'")
    intercept("select * from test where test.t is like 'test'", "mismatched input 'is' expecting")
    intercept("SELECT * FROM test WHERE x NOT NULL", "mismatched input 'NOT' expecting")
  }

  test("hyphen in identifier - DDL tests") {
    val msg = "unquoted identifier"
    intercept("USE test-test", 1, 8, 9, msg + " test-test")
    intercept("CREATE DATABASE IF NOT EXISTS my-database", 1, 32, 33, msg + " my-database")
    intercept(
      """
        |ALTER DATABASE my-database
        |SET DBPROPERTIES ('p1'='v1')""".stripMargin, 2, 17, 18, msg + " my-database")
    intercept("DROP DATABASE my-database", 1, 16, 17, msg + " my-database")
    intercept(
      """
        |ALTER TABLE t
        |CHANGE COLUMN
        |test-col BIGINT
      """.stripMargin, 4, 4, 5, msg + " test-col")
    intercept("CREATE TABLE test (attri-bute INT)", 1, 24, 25, msg + " attri-bute")
    intercept(
      """
        |CREATE TABLE IF NOT EXISTS mydb.page-view
        |USING parquet
        |COMMENT 'This is the staging page view table'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src""".stripMargin, 2, 36, 37, msg + " page-view")
    intercept("SHOW TABLES IN hyphen-database", 1, 21, 22, msg + " hyphen-database")
    intercept("SHOW TABLE EXTENDED IN hyphen-db LIKE \"str\"", 1, 29, 30, msg + " hyphen-db")
    intercept("SHOW COLUMNS IN t FROM test-db", 1, 27, 28, msg + " test-db")
    intercept("DESC SCHEMA EXTENDED test-db", 1, 25, 26, msg + " test-db")
    intercept("ANALYZE TABLE test-table PARTITION (part1)", 1, 18, 19, msg + " test-table")
    intercept("LOAD DATA INPATH \"path\" INTO TABLE my-tab", 1, 37, 38, msg + " my-tab")
  }

  test("hyphen in identifier - DML tests") {
    val msg = "unquoted identifier"
    // dml tests
    intercept("SELECT * FROM table-with-hyphen", 1, 19, 25, msg + " table-with-hyphen")
    // special test case: minus in expression shouldn't be treated as hyphen in identifiers
    intercept("SELECT a-b FROM table-with-hyphen", 1, 21, 27, msg + " table-with-hyphen")
    intercept("SELECT a-b AS a-b FROM t", 1, 15, 16, msg + " a-b")
    intercept("SELECT a-b FROM table-hyphen WHERE a-b = 0", 1, 21, 22, msg + " table-hyphen")
    intercept("SELECT (a - test_func(b-c)) FROM test-table", 1, 37, 38, msg + " test-table")
    intercept("WITH a-b AS (SELECT 1 FROM s) SELECT * FROM s;", 1, 6, 7, msg + " a-b")
    intercept(
      """
        |SELECT a, b
        |FROM t1 JOIN t2
        |USING (a, b, at-tr)
      """.stripMargin, 4, 15, 16, msg + " at-tr"
    )
    intercept(
      """
        |SELECT product, category, dense_rank()
        |OVER (PARTITION BY category ORDER BY revenue DESC) as hyphen-rank
        |FROM productRevenue
      """.stripMargin, 3, 60, 61, msg + " hyphen-rank"
    )
    intercept(
      """
        |SELECT a, b
        |FROM grammar-breaker
        |WHERE a-b > 10
        |GROUP BY fake-breaker
        |ORDER BY c
      """.stripMargin, 3, 12, 13, msg + " grammar-breaker")
    assertEqual(
      """
        |SELECT a, b
        |FROM t
        |WHERE a-b > 10
        |GROUP BY fake-breaker
        |ORDER BY c
      """.stripMargin,
      table("t")
        .where('a - 'b > 10)
        .groupBy('fake - 'breaker)('a, 'b)
        .orderBy('c.asc))
    intercept(
      """
        |SELECT * FROM tab
        |WINDOW hyphen-window AS
        |  (PARTITION BY a, b ORDER BY c rows BETWEEN 1 PRECEDING AND 1 FOLLOWING)
      """.stripMargin, 3, 13, 14, msg + " hyphen-window")
    intercept(
      """
        |SELECT * FROM tab
        |WINDOW window_ref AS window-ref
      """.stripMargin, 3, 27, 28, msg + " window-ref")
    intercept(
      """
        |SELECT tb.*
        |FROM t-a INNER JOIN tb
        |ON ta.a = tb.a AND ta.tag = tb.tag
      """.stripMargin, 3, 6, 7, msg + " t-a")
    intercept(
      """
        |FROM test-table
        |SELECT a
        |SELECT b
      """.stripMargin, 2, 9, 10, msg + " test-table")
  }
}
