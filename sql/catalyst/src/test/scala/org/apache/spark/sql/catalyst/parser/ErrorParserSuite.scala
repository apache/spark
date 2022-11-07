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
      errorClass = "_LEGACY_ERROR_TEMP_0011",
      parameters = Map.empty,
      context = ExpectedContext(fragment = "order by q\ncluster by q", start = 16, stop = 38))
  }

  test("hyphen in identifier - DDL tests") {
    checkError(
      exception = parseException("USE test-test"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-test"))
    checkError(
      exception = parseException("CREATE DATABASE IF NOT EXISTS my-database"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "my-database"))
    checkError(
      exception = parseException(
      """
        |ALTER DATABASE my-database
        |SET DBPROPERTIES ('p1'='v1')""".stripMargin),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "my-database"))
    checkError(
      exception = parseException("DROP DATABASE my-database"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "my-database"))
    checkError(
      exception = parseException(
        """
          |ALTER TABLE t
          |CHANGE COLUMN
          |test-col TYPE BIGINT
        """.stripMargin),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-col"))
    checkError(
      exception = parseException(
        """
          |ALTER TABLE t
          |RENAME COLUMN
          |test-col TO test
        """.stripMargin),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-col"))
    checkError(
      exception = parseException(
        """
          |ALTER TABLE t
          |RENAME COLUMN
          |test TO test-col
        """.stripMargin),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-col"))
    checkError(
      exception = parseException(
        """
          |ALTER TABLE t
          |DROP COLUMN
          |test-col, test
        """.stripMargin),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-col"))
    checkError(
      exception = parseException("CREATE TABLE test (attri-bute INT)"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "attri-bute"))
    checkError(
      exception = parseException("CREATE FUNCTION test-func as org.test.func"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-func"))
    checkError(
      exception = parseException("DROP FUNCTION test-func as org.test.func"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-func"))
    checkError(
      exception = parseException("SHOW FUNCTIONS LIKE test-func"),
      errorClass = "INVALID_IDENTIFIER",
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
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "page-view"))
    checkError(
      exception = parseException(
        """
          |CREATE TABLE IF NOT EXISTS tab
          |USING test-provider
          |AS SELECT * FROM src""".stripMargin),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-provider"))
    checkError(
      exception = parseException("SHOW TABLES IN hyphen-database"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "hyphen-database"))
    checkError(
      exception = parseException("SHOW TABLE EXTENDED IN hyphen-db LIKE \"str\""),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "hyphen-db"))
    checkError(
      exception = parseException("SHOW COLUMNS IN t FROM test-db"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-db"))
    checkError(
      exception = parseException("DESC SCHEMA EXTENDED test-db"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-db"))
    checkError(
      exception = parseException("ANALYZE TABLE test-table PARTITION (part1)"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-table"))
    checkError(
      exception = parseException("LOAD DATA INPATH \"path\" INTO TABLE my-tab"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "my-tab"))
  }

  test("hyphen in identifier - DML tests") {
    // dml tests
    checkError(
      exception = parseException("SELECT * FROM table-with-hyphen"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "table-with-hyphen"))
    // special test case: minus in expression shouldn't be treated as hyphen in identifiers
    checkError(
      exception = parseException("SELECT a-b FROM table-with-hyphen"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "table-with-hyphen"))
    checkError(
      exception = parseException("SELECT a-b AS a-b FROM t"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "a-b"))
    checkError(
      exception = parseException("SELECT a-b FROM table-hyphen WHERE a-b = 0"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "table-hyphen"))
    checkError(
      exception = parseException("SELECT (a - test_func(b-c)) FROM test-table"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-table"))
    checkError(
      exception = parseException("WITH a-b AS (SELECT 1 FROM s) SELECT * FROM s;"),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "a-b"))
    checkError(
      exception = parseException(
        """
          |SELECT a, b
          |FROM t1 JOIN t2
          |USING (a, b, at-tr)
        """.stripMargin),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "at-tr"))
    checkError(
      exception = parseException(
        """
          |SELECT product, category, dense_rank()
          |OVER (PARTITION BY category ORDER BY revenue DESC) as hyphen-rank
          |FROM productRevenue
        """.stripMargin),
      errorClass = "INVALID_IDENTIFIER",
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
      errorClass = "INVALID_IDENTIFIER",
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
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "hyphen-window"))
    checkError(
      exception = parseException(
        """
          |SELECT * FROM tab
          |WINDOW window_ref AS window-ref
        """.stripMargin),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "window-ref"))
    checkError(
      exception = parseException(
        """
          |SELECT tb.*
          |FROM t-a INNER JOIN tb
          |ON ta.a = tb.a AND ta.tag = tb.tag
        """.stripMargin),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "t-a"))
    checkError(
      exception = parseException(
        """
          |FROM test-table
          |SELECT a
          |SELECT b
        """.stripMargin),
      errorClass = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-table"))
  }

  test("datatype not supported") {
    // general bad types
    checkError(
      exception = parseException("SELECT cast(1 as badtype)"),
      errorClass = "_LEGACY_ERROR_TEMP_0030",
      parameters = Map("dataType" -> "badtype"),
      context = ExpectedContext(fragment = "badtype", start = 17, stop = 23))
    // special handling on char and varchar
    checkError(
      exception = parseException("SELECT cast('a' as CHAR)"),
      errorClass = "PARSE_CHAR_MISSING_LENGTH",
      parameters = Map("type" -> "\"CHAR\""),
      context = ExpectedContext(fragment = "CHAR", start = 19, stop = 22))
    checkError(
      exception = parseException("SELECT cast('a' as Varchar)"),
      errorClass = "PARSE_CHAR_MISSING_LENGTH",
      parameters = Map("type" -> "\"VARCHAR\""),
      context = ExpectedContext(fragment = "Varchar", start = 19, stop = 25))
    checkError(
      exception = parseException("SELECT cast('a' as Character)"),
      errorClass = "PARSE_CHAR_MISSING_LENGTH",
      parameters = Map("type" -> "\"CHARACTER\""),
      context = ExpectedContext(fragment = "Character", start = 19, stop = 27))
  }
}
