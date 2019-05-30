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

import org.apache.spark.SparkFunSuite

/**
 * Test various parser errors.
 */
class ErrorParserSuite extends SparkFunSuite {
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

  test("hyphen in identifier - DDL tests") {
    val msg = "unquoted identifier"
    // ddl tests
    intercept("use test-test", 1, 4, 9, msg, "^^^")
    intercept("CREATE TABLE test (attri-bute INT)", 1, 19, 25, msg, "^^^")
    intercept("CREATE TABLE test (attri-bute INT)", 1, 19, 25, msg, "^^^")
    intercept("ALTER TABLE test ADD COLUMNS (h-col BIGINT)", 1, 30, 32, msg, "^^^")
    intercept(
      """
        |CREATE TABLE IF NOT EXISTS mydb.page-view
        |USING parquet
        |COMMENT 'This is the staging page view table'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src""".stripMargin, 2, 32, 37, msg, "^^^")
  }

  test("hyphen in identifier - DML tests") {
    val msg = "unquoted identifier"
    // dml tests
    intercept("SELECT * FROM table-with-hyphen", 1, 14, 25, msg, "^^^")
    // special test case: minus in expression shouldn't be treated as hyphen in identifiers
    intercept("SELECT a-b FROM table-with-hyphen", 1, 16, 27, msg, "^^^")
    intercept("SELECT a-b FROM table-with-hyphen WHERE a-b = 0", 1, 16, 27, msg, "^^^")
    intercept("SELECT (a - test_func(b-c)) FROM test-table", 1, 33, 38, msg, "^^^")
    intercept(
      """
        |SELECT a, b
        |FROM grammar-breaker
        |WHERE a-b > 10
        |GROUP BY fake-breaker
        |ORDER BY c
      """.stripMargin, 3, 5, 13, msg, "^^^"
    )
    intercept(
      """
        |SELECT * FROM tab
        |window hyphen-window as
        |  (partition by a, b order by c rows between 1 preceding and 1 following)
      """.stripMargin, 3, 7, 14, msg, "^^^"
    )
  }
}
