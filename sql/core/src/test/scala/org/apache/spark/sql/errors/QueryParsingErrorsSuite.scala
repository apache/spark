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

package org.apache.spark.sql.errors

import org.apache.spark.sql.QueryTest

// Turn of the length check because most of the tests check entire error messages
// scalastyle:off line.size.limit
class QueryParsingErrorsSuite extends QueryTest with QueryErrorsSuiteBase {

  test("UNSUPPORTED_FEATURE: LATERAL join with NATURAL join not supported") {
    validateParsingError(
      sqlText = "SELECT * FROM t1 NATURAL JOIN LATERAL (SELECT c1 + c2 AS c2)",
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("LATERAL_NATURAL_JOIN"),
      sqlState = "0A000")
  }

  test("UNSUPPORTED_FEATURE: LATERAL join with USING join not supported") {
    validateParsingError(
      sqlText = "SELECT * FROM t1 JOIN LATERAL (SELECT c1 + c2 AS c2) USING (c2)",
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("LATERAL_JOIN_USING"),
      sqlState = "0A000")
  }

  test("UNSUPPORTED_FEATURE: Unsupported LATERAL join type") {
    Seq("RIGHT OUTER", "FULL OUTER", "LEFT SEMI", "LEFT ANTI").foreach { joinType =>
      validateParsingError(
        sqlText = s"SELECT * FROM t1 $joinType JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3",
        errorClass = "UNSUPPORTED_FEATURE",
        errorSubClass = Some("LATERAL_JOIN_OF_TYPE"),
        sqlState = "0A000",
        parameters = Map("joinType" -> joinType))
    }
  }

  test("INVALID_SQL_SYNTAX: LATERAL can only be used with subquery") {
    Seq(
      "SELECT * FROM t1, LATERAL t2" -> 26,
      "SELECT * FROM t1 JOIN LATERAL t2" -> 30,
      "SELECT * FROM t1, LATERAL (t2 JOIN t3)" -> 26,
      "SELECT * FROM t1, LATERAL (LATERAL t2)" -> 26,
      "SELECT * FROM t1, LATERAL VALUES (0, 1)" -> 26,
      "SELECT * FROM t1, LATERAL RANGE(0, 1)" -> 26
    ).foreach { case (sqlText, pos) =>
      validateParsingError(
        sqlText = sqlText,
        errorClass = "INVALID_SQL_SYNTAX",
        sqlState = "42000",
        parameters = Map("inputString" -> "LATERAL can only be used with subquery."))
    }
  }

  test("UNSUPPORTED_FEATURE: NATURAL CROSS JOIN is not supported") {
    validateParsingError(
      sqlText = "SELECT * FROM a NATURAL CROSS JOIN b",
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("NATURAL_CROSS_JOIN"),
      sqlState = "0A000")
  }

  test("INVALID_SQL_SYNTAX: redefine window") {
    validateParsingError(
      sqlText = "SELECT min(a) OVER win FROM t1 WINDOW win AS win, win AS win2",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "The definition of window `win` is repetitive."))
  }

  test("INVALID_SQL_SYNTAX: invalid window reference") {
    validateParsingError(
      sqlText = "SELECT min(a) OVER win FROM t1 WINDOW win AS win",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Window reference `win` is not a window specification."))
  }

  test("INVALID_SQL_SYNTAX: window reference cannot be resolved") {
    validateParsingError(
      sqlText = "SELECT min(a) OVER win FROM t1 WINDOW win AS win2",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Cannot resolve window reference `win2`."))
  }

  test("UNSUPPORTED_FEATURE: TRANSFORM does not support DISTINCT/ALL") {
    validateParsingError(
      sqlText = "SELECT TRANSFORM(DISTINCT a) USING 'a' FROM t",
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("TRANSFORM_DISTINCT_ALL"),
      sqlState = "0A000")
  }

  test("UNSUPPORTED_FEATURE: In-memory mode does not support TRANSFORM with serde") {
    validateParsingError(
      sqlText = "SELECT TRANSFORM(a) ROW FORMAT SERDE " +
        "'org.apache.hadoop.hive.serde2.OpenCSVSerde' USING 'a' FROM t",
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("TRANSFORM_NON_HIVE"),
      sqlState = "0A000")
  }

  test("INVALID_SQL_SYNTAX: Too many arguments for transform") {
    validateParsingError(
      sqlText = "CREATE TABLE table(col int) PARTITIONED BY (years(col,col))",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Too many arguments for transform `years`"))
  }

  test("INVALID_SQL_SYNTAX: Invalid table value function name") {
    validateParsingError(
      sqlText = "SELECT * FROM db.func()",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "table valued function cannot specify database name "))

    validateParsingError(
      sqlText = "SELECT * FROM ns.db.func()",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "table valued function cannot specify database name "))
  }

  test("INVALID_SQL_SYNTAX: Invalid scope in show functions") {
    validateParsingError(
      sqlText = "SHOW sys FUNCTIONS",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "SHOW `sys` FUNCTIONS not supported"))
  }

  test("INVALID_SQL_SYNTAX: Invalid pattern in show functions") {
    validateParsingError(
      sqlText = "SHOW FUNCTIONS IN db f1",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" ->
        "Invalid pattern in SHOW FUNCTIONS: `f1`. It must be a \"STRING\" literal."))
    validateParsingError(
      sqlText = "SHOW FUNCTIONS IN db LIKE f1",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" ->
        "Invalid pattern in SHOW FUNCTIONS: `f1`. It must be a \"STRING\" literal."))
  }

  test("INVALID_SQL_SYNTAX: Create function with both if not exists and replace") {
    val sqlText =
      """
        |CREATE OR REPLACE FUNCTION IF NOT EXISTS func1 as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'
        |""".stripMargin

    validateParsingError(
      sqlText = sqlText,
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" ->
        "CREATE FUNCTION with both IF NOT EXISTS and REPLACE is not allowed."))
  }

  test("INVALID_SQL_SYNTAX: Create temporary function with if not exists") {
    val sqlText =
      """
        |CREATE TEMPORARY FUNCTION IF NOT EXISTS func1 as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'
        |""".stripMargin

    validateParsingError(
      sqlText = sqlText,
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" ->
        "It is not allowed to define a TEMPORARY FUNCTION with IF NOT EXISTS."))
  }

  test("INVALID_SQL_SYNTAX: Create temporary function with multi-part name") {
    val sqlText =
      """
        |CREATE TEMPORARY FUNCTION ns.db.func as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'
        |""".stripMargin

    validateParsingError(
      sqlText = sqlText,
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Unsupported function name `ns`.`db`.`func`"))
  }

  test("INVALID_SQL_SYNTAX: Specifying database while creating temporary function") {
    val sqlText =
      """
        |CREATE TEMPORARY FUNCTION db.func as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'
        |""".stripMargin

    validateParsingError(
      sqlText = sqlText,
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" ->
        "Specifying a database in CREATE TEMPORARY FUNCTION is not allowed: `db`"))
  }

  test("INVALID_SQL_SYNTAX: Drop temporary function requires a single part name") {
    validateParsingError(
      sqlText = "DROP TEMPORARY FUNCTION db.func",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" ->
        "DROP TEMPORARY FUNCTION requires a single part name but got: `db`.`func`"))
  }

  test("DUPLICATE_KEY: Found duplicate partition keys") {
    validateParsingError(
      sqlText = "INSERT OVERWRITE TABLE table PARTITION(p1='1', p1='1') SELECT 'col1', 'col2'",
      errorClass = "DUPLICATE_KEY",
      sqlState = "23000",
      parameters = Map("keyColumn" -> "`p1`"))
  }

  test("DUPLICATE_KEY: in table properties") {
    validateParsingError(
      sqlText = "ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('key1' = '1', 'key1' = '2')",
      errorClass = "DUPLICATE_KEY",
      sqlState = "23000",
      parameters = Map("keyColumn" -> "`key1`"))
  }

  test("PARSE_EMPTY_STATEMENT: empty input") {
    validateParsingError(
      sqlText = "",
      errorClass = "PARSE_EMPTY_STATEMENT",
      sqlState = "42000")

    validateParsingError(
      sqlText = "   ",
      errorClass = "PARSE_EMPTY_STATEMENT",
      sqlState = "42000")

    validateParsingError(
      sqlText = " \n",
      errorClass = "PARSE_EMPTY_STATEMENT",
      sqlState = "42000")
  }

  test("PARSE_SYNTAX_ERROR: no viable input") {
    val sqlText = "select ((r + 1) "
    validateParsingError(
      sqlText = sqlText,
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "end of input", "hint" -> ""))
  }

  test("PARSE_SYNTAX_ERROR: extraneous input") {
    validateParsingError(
      sqlText = "select 1 1",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'1'", "hint" -> ": extra input '1'"))

    validateParsingError(
      sqlText = "select *\nfrom r as q t",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'t'", "hint" -> ": extra input 't'"))
  }

  test("PARSE_SYNTAX_ERROR: mismatched input") {
    validateParsingError(
      sqlText = "select * from r order by q from t",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'from'", "hint" -> ""))

    validateParsingError(
      sqlText = "select *\nfrom r\norder by q\nfrom t",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'from'", "hint" -> ""))
  }

  test("PARSE_SYNTAX_ERROR: jargon token substitute to user-facing language") {
    // '<EOF>' -> end of input
    validateParsingError(
      sqlText = "select count(*",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "end of input", "hint" -> ""))

    validateParsingError(
      sqlText = "select 1 as a from",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "end of input", "hint" -> ""))
  }

  test("PARSE_SYNTAX_ERROR - SPARK-21136: " +
    "misleading error message due to problematic antlr grammar") {
    validateParsingError(
      sqlText = "select * from a left join_ b on a.id = b.id",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'join_'", "hint" -> ": missing 'JOIN'"))

    validateParsingError(
      sqlText = "select * from test where test.t is like 'test'",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'is'", "hint" -> ""))

    validateParsingError(
      sqlText = "SELECT * FROM test WHERE x NOT NULL",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'NOT'", "hint" -> ""))
  }

  test("INVALID_SQL_SYNTAX: show table partition key must set value") {
    validateParsingError(
      sqlText = "SHOW TABLE EXTENDED IN default LIKE 'employee' PARTITION (grade)",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Partition key `grade` must set value (can't be empty)."))
  }

  test("INVALID_SQL_SYNTAX: expected a column reference for transform bucket") {
    validateParsingError(
      sqlText =
        "CREATE TABLE my_tab(a INT, b STRING) USING parquet PARTITIONED BY (bucket(32, a, 66))",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Expected a column reference for transform `bucket`: 66"))
  }

  test("UNSUPPORTED_FEATURE: DESC TABLE COLUMN for a specific partition") {
    validateParsingError(
      sqlText = "DESCRIBE TABLE EXTENDED customer PARTITION (grade = 'A') customer.age",
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("DESC_TABLE_COLUMN_PARTITION"),
      sqlState = "0A000")
  }

  test("INVALID_SQL_SYNTAX: PARTITION specification is incomplete") {
    validateParsingError(
      sqlText = "DESCRIBE TABLE EXTENDED customer PARTITION (grade)",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "PARTITION specification is incomplete: `grade`"))
  }

  test("UNSUPPORTED_FEATURE: cannot set reserved namespace property") {
    val sql = "CREATE NAMESPACE IF NOT EXISTS a.b.c WITH PROPERTIES ('location'='/home/user/db')"
    validateParsingError(
      sqlText = sql,
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("SET_NAMESPACE_PROPERTY"),
      sqlState = "0A000",
      parameters = Map(
        "property" -> "location",
        "msg" -> "please use the LOCATION clause to specify it"))
  }

  test("UNSUPPORTED_FEATURE: cannot set reserved table property") {
    val sql = "CREATE TABLE student (id INT, name STRING, age INT) " +
      "USING PARQUET TBLPROPERTIES ('provider'='parquet')"
    validateParsingError(
      sqlText = sql,
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("SET_TABLE_PROPERTY"),
      sqlState = "0A000",
      parameters = Map(
        "property" -> "provider",
        "msg" -> "please use the USING clause to specify it"))
  }

  test("INVALID_PROPERTY_KEY: invalid property key for set quoted configuration") {
    val sql = "set =`value`"
    validateParsingError(
      sqlText = sql,
      errorClass = "INVALID_PROPERTY_KEY",
      sqlState = null,
      parameters = Map("key" -> "\"\"", "value" -> "\"value\""))
  }

  test("INVALID_PROPERTY_VALUE: invalid property value for set quoted configuration") {
    val sql = "set `key`=1;2;;"
    validateParsingError(
      sqlText = sql,
      errorClass = "INVALID_PROPERTY_VALUE",
      sqlState = null,
      parameters = Map("value" -> "\"1;2;;\"", "key" -> "\"key\""))
  }

  test("UNSUPPORTED_FEATURE: cannot set Properties and DbProperties at the same time") {
    val sql = "CREATE NAMESPACE IF NOT EXISTS a.b.c WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c') " +
      "WITH DBPROPERTIES('a'='a', 'b'='b', 'c'='c')"
    validateParsingError(
      sqlText = sql,
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("SET_PROPERTIES_AND_DBPROPERTIES"),
      sqlState = "0A000")
  }
}
