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

import org.apache.spark.SparkThrowableHelper
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

  private def interceptImpl(sql: String, messages: String*)(
      line: Option[Int] = None,
      startPosition: Option[Int] = None,
      stopPosition: Option[Int] = None,
      errorClass: Option[String] = None): Unit = {
    val e = intercept[ParseException](CatalystSqlParser.parsePlan(sql))

    // Check messages.
    val error = e.getMessage
    messages.foreach { message =>
      assert(error.contains(message))
    }

    // Check position.
    if (line.isDefined) {
      assert(line.isDefined && startPosition.isDefined && stopPosition.isDefined)
      assert(e.line.isDefined)
      assert(e.line.get === line.get)
      assert(e.startPosition.isDefined)
      assert(e.startPosition.get === startPosition.get)
      assert(e.stop.startPosition.isDefined)
      assert(e.stop.startPosition.get === stopPosition.get)
    }

    // Check error class.
    if (errorClass.isDefined) {
      assert(e.getErrorClass == errorClass.get)
    }
  }

  def intercept(sqlCommand: String, errorClass: Option[String], messages: String*): Unit = {
    interceptImpl(sqlCommand, messages: _*)(errorClass = errorClass)
  }

  def intercept(
      sql: String, line: Int, startPosition: Int, stopPosition: Int, messages: String*): Unit = {
    interceptImpl(sql, messages: _*)(Some(line), Some(startPosition), Some(stopPosition))
  }

  def intercept(sql: String, errorClass: String, line: Int, startPosition: Int, stopPosition: Int,
      messages: String*): Unit = {
    interceptImpl(sql, messages: _*)(
      Some(line), Some(startPosition), Some(stopPosition), Some(errorClass))
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
    intercept("select * from r order by q from t", "PARSE_INPUT_MISMATCHED",
      1, 27, 31,
      "Syntax error at or near",
      "---------------------------^^^"
    )
    intercept("select *\nfrom r\norder by q\nfrom t", "PARSE_INPUT_MISMATCHED",
      4, 0, 4,
      "Syntax error at or near", "^^^")
  }

  test("empty input") {
    val expectedErrMsg = SparkThrowableHelper.getMessage("PARSE_EMPTY_STATEMENT", Array[String]())
    intercept("", Some("PARSE_EMPTY_STATEMENT"), expectedErrMsg)
    intercept("   ", Some("PARSE_EMPTY_STATEMENT"), expectedErrMsg)
    intercept(" \n", Some("PARSE_EMPTY_STATEMENT"), expectedErrMsg)
  }

  test("jargon token substitute to user-facing language") {
    // '<EOF>' -> end of input
    intercept("select count(*", "PARSE_INPUT_MISMATCHED",
      1, 14, 14, "Syntax error at or near end of input")
    intercept("select 1 as a from", "PARSE_INPUT_MISMATCHED",
      1, 18, 18, "Syntax error at or near end of input")
  }

  test("semantic errors") {
    intercept("select *\nfrom r\norder by q\ncluster by q", 3, 0, 11,
      "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported",
      "^^^")
  }

  test("SPARK-21136: misleading error message due to problematic antlr grammar") {
    intercept("select * from a left join_ b on a.id = b.id", None, "missing 'JOIN' at 'join_'")
    intercept("select * from test where test.t is like 'test'", Some("PARSE_INPUT_MISMATCHED"),
      SparkThrowableHelper.getMessage("PARSE_INPUT_MISMATCHED", Array("'is'")))
    intercept("SELECT * FROM test WHERE x NOT NULL", Some("PARSE_INPUT_MISMATCHED"),
      SparkThrowableHelper.getMessage("PARSE_INPUT_MISMATCHED", Array("'NOT'")))
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
        |test-col TYPE BIGINT
      """.stripMargin, 4, 4, 5, msg + " test-col")
    intercept(
      """
        |ALTER TABLE t
        |RENAME COLUMN
        |test-col TO test
      """.stripMargin, 4, 4, 5, msg + " test-col")
    intercept(
      """
        |ALTER TABLE t
        |RENAME COLUMN
        |test TO test-col
      """.stripMargin, 4, 12, 13, msg + " test-col")
    intercept(
      """
        |ALTER TABLE t
        |DROP COLUMN
        |test-col, test
      """.stripMargin, 4, 4, 5, msg + " test-col")
    intercept("CREATE TABLE test (attri-bute INT)", 1, 24, 25, msg + " attri-bute")
    intercept("CREATE FUNCTION test-func as org.test.func", 1, 20, 21, msg + " test-func")
    intercept("DROP FUNCTION test-func as org.test.func", 1, 18, 19, msg + " test-func")
    intercept("SHOW FUNCTIONS LIKE test-func", 1, 24, 25, msg + " test-func")
    intercept(
      """
        |CREATE TABLE IF NOT EXISTS mydb.page-view
        |USING parquet
        |COMMENT 'This is the staging page view table'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src""".stripMargin, 2, 36, 37, msg + " page-view")
    intercept(
      """
        |CREATE TABLE IF NOT EXISTS tab
        |USING test-provider
        |AS SELECT * FROM src""".stripMargin, 3, 10, 11, msg + " test-provider")
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
