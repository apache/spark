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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.analysis.AnalysisTest

/**
 * Test various parser errors.
 */
class ErrorParserSuite extends AnalysisTest {
  import CatalystSqlParser._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  def parseException(sql: String): SparkThrowable = {
    intercept[ParseException](CatalystSqlParser.parsePlan(sql))
  }

  test("semantic errors") {
    checkError(
      exception = parseException("select *\nfrom r\norder by q\ncluster by q"),
      condition = "UNSUPPORTED_FEATURE.COMBINATION_QUERY_RESULT_CLAUSES",
      parameters = Map.empty,
      context = ExpectedContext(fragment = "order by q\ncluster by q", start = 16, stop = 38))
  }

  test("Illegal characters in unquoted identifier") {
    // scalastyle:off
    checkError(
      exception = parseException("USE \u0196pfel"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "\u0196pfel"))
    checkError(
      exception = parseException("USE \u88681"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "\u88681"))
    // scalastyle:on
    checkError(
      exception = parseException("USE https://www.spa.rk/bucket/pa-th.json?=&#%"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "https://www.spa.rk/bucket/pa-th.json?=&#%"))
  }

  test("hyphen in identifier - DDL tests") {
    checkError(
      exception = parseException("USE test-test"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-test"))
    checkError(
      exception = parseException("SET CATALOG test-test"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-test"))
    checkError(
      exception = parseException("CREATE DATABASE IF NOT EXISTS my-database"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "my-database"))
    checkError(
      exception = parseException(
      """
        |ALTER DATABASE my-database
        |SET DBPROPERTIES ('p1'='v1')""".stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "my-database"))
    checkError(
      exception = parseException("DROP DATABASE my-database"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "my-database"))
    checkError(
      exception = parseException(
        """
          |ALTER TABLE t
          |CHANGE COLUMN
          |test-col TYPE BIGINT
        """.stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-col"))
    checkError(
      exception = parseException(
        """
          |ALTER TABLE t
          |DROP COLUMN
          |test-col, test
        """.stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-col"))
    checkError(
      exception = parseException("CREATE TABLE test (attri-bute INT)"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "attri-bute"))
    checkError(
      exception = parseException("CREATE FUNCTION test-func as org.test.func"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-func"))
    checkError(
      exception = parseException("DROP FUNCTION test-func as org.test.func"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-func"))
    checkError(
      exception = parseException("SHOW FUNCTIONS LIKE test-func"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-func"))
    checkError(
      exception = parseException(
        """
          |CREATE TABLE IF NOT EXISTS mydb.page-view
          |USING parquet
          |COMMENT 'This is the staging page view table'
          |LOCATION '/user/external/page_view'
          |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
          |AS SELECT * FROM src""".stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "page-view"))
    checkError(
      exception = parseException(
        """
          |CREATE TABLE IF NOT EXISTS tab
          |USING test-provider
          |AS SELECT * FROM src""".stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-provider"))
    checkError(
      exception = parseException("SHOW TABLES IN hyphen-database"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "hyphen-database"))
    checkError(
      exception = parseException("SHOW TABLE EXTENDED IN hyphen-db LIKE \"str\""),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "hyphen-db"))
    checkError(
      exception = parseException("DESC SCHEMA EXTENDED test-db"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-db"))
    checkError(
      exception = parseException("ANALYZE TABLE test-table PARTITION (part1)"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-table"))
    checkError(
      exception = parseException("CREATE TABLE t(c1 struct<test-test INT, c2 INT>)"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-test"))
    checkError(
      exception = parseException("LOAD DATA INPATH \"path\" INTO TABLE my-tab"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "my-tab"))
  }

  test("hyphen in identifier - DML tests") {
    // dml tests
    checkError(
      exception = parseException("SELECT * FROM table-with-hyphen"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "table-with-hyphen"))
    // special test case: minus in expression shouldn't be treated as hyphen in identifiers
    checkError(
      exception = parseException("SELECT a-b FROM table-with-hyphen"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "table-with-hyphen"))
    checkError(
      exception = parseException("SELECT a-b AS a-b FROM t"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "a-b"))
    checkError(
      exception = parseException("SELECT a-b FROM table-hyphen WHERE a-b = 0"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "table-hyphen"))
    checkError(
      exception = parseException("SELECT (a - test_func(b-c)) FROM test-table"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-table"))
    checkError(
      exception = parseException("WITH a-b AS (SELECT 1 FROM s) SELECT * FROM s;"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "a-b"))
    checkError(
      exception = parseException(
        """
          |SELECT a, b
          |FROM t1 JOIN t2
          |USING (a, b, at-tr)
        """.stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "at-tr"))
    checkError(
      exception = parseException(
        """
          |SELECT product, category, dense_rank()
          |OVER (PARTITION BY category ORDER BY revenue DESC) as hyphen-rank
          |FROM productRevenue
        """.stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "hyphen-rank"))
    checkError(
      exception = parseException(
        """
          |SELECT a, b
          |FROM grammar-breaker
          |WHERE a-b > 10
          |GROUP BY fake-breaker
          |ORDER BY c
        """.stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "grammar-breaker"))
    assert(parsePlan(
      """
        |SELECT a, b
        |FROM t
        |WHERE a-b > 10
        |GROUP BY fake-breaker
        |ORDER BY c
      """.stripMargin) ===
      table("t")
        .where($"a" - $"b" > 10)
        .groupBy($"fake" - $"breaker")($"a", $"b")
        .orderBy($"c".asc))
    checkError(
      exception = parseException(
        """
          |SELECT * FROM tab
          |WINDOW hyphen-window AS
          |  (PARTITION BY a, b ORDER BY c rows BETWEEN 1 PRECEDING AND 1 FOLLOWING)
        """.stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "hyphen-window"))
    checkError(
      exception = parseException(
        """
          |SELECT * FROM tab
          |WINDOW window_ref AS window-ref
        """.stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "window-ref"))
    checkError(
      exception = parseException(
        """
          |SELECT tb.*
          |FROM t-a INNER JOIN tb
          |ON ta.a = tb.a AND ta.tag = tb.tag
        """.stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "t-a"))
    checkError(
      exception = parseException(
        """
          |FROM test-table
          |SELECT a
          |SELECT b
        """.stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-table"))
    checkError(
      exception = parseException(
        """
          |SELECT * FROM (
          |  SELECT year, course, earnings FROM courseSales
          |)
          |PIVOT (
          |  sum(earnings)
          |  FOR test-test IN ('dotNET', 'Java')
          |);
        """.stripMargin),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-test"))
  }

  test("datatype not supported") {
    // general bad types
    checkError(
      exception = parseException("SELECT cast(1 as badtype)"),
      condition = "UNSUPPORTED_DATATYPE",
      parameters = Map("typeName" -> "\"BADTYPE\""),
      context = ExpectedContext(fragment = "badtype", start = 17, stop = 23))
    // special handling on char and varchar
    checkError(
      exception = parseException("SELECT cast('a' as CHAR)"),
      condition = "DATATYPE_MISSING_SIZE",
      parameters = Map("type" -> "\"CHAR\""),
      context = ExpectedContext(fragment = "CHAR", start = 19, stop = 22))
    checkError(
      exception = parseException("SELECT cast('a' as Varchar)"),
      condition = "DATATYPE_MISSING_SIZE",
      parameters = Map("type" -> "\"VARCHAR\""),
      context = ExpectedContext(fragment = "Varchar", start = 19, stop = 25))
    checkError(
      exception = parseException("SELECT cast('a' as Character)"),
      condition = "DATATYPE_MISSING_SIZE",
      parameters = Map("type" -> "\"CHARACTER\""),
      context = ExpectedContext(fragment = "Character", start = 19, stop = 27))
  }

  test("'!' where only NOT should be allowed") {
    checkError(
      exception = parseException("SELECT 1 ! IN (2)"),
      condition = "SYNTAX_DISCONTINUED.BANG_EQUALS_NOT",
      parameters = Map("clause" -> "!"),
      context = ExpectedContext(fragment = "!", start = 9, stop = 9))
    checkError(
      exception = parseException("SELECT 'a' ! LIKE 'b'"),
      condition = "SYNTAX_DISCONTINUED.BANG_EQUALS_NOT",
      parameters = Map("clause" -> "!"),
      context = ExpectedContext(fragment = "!", start = 11, stop = 11))
    checkError(
      exception = parseException("SELECT 1 ! BETWEEN 1 AND 2"),
      condition = "SYNTAX_DISCONTINUED.BANG_EQUALS_NOT",
      parameters = Map("clause" -> "!"),
      context = ExpectedContext(fragment = "!", start = 9, stop = 9))
    checkError(
      exception = parseException("SELECT 1 IS ! NULL"),
      condition = "SYNTAX_DISCONTINUED.BANG_EQUALS_NOT",
      parameters = Map("clause" -> "!"),
      context = ExpectedContext(fragment = "!", start = 12, stop = 12))
    checkError(
      exception = parseException("CREATE TABLE IF ! EXISTS t(c1 INT)"),
      condition = "SYNTAX_DISCONTINUED.BANG_EQUALS_NOT",
      parameters = Map("clause" -> "!"),
      context = ExpectedContext(fragment = "!", start = 16, stop = 16))
    checkError(
      exception = parseException("CREATE TABLE t(c1 INT ! NULL)"),
      condition = "SYNTAX_DISCONTINUED.BANG_EQUALS_NOT",
      parameters = Map("clause" -> "!"),
      context = ExpectedContext(fragment = "!", start = 22, stop = 22))
  }
}
