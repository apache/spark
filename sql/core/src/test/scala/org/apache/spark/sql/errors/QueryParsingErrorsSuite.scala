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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSparkSession

// Turn of the length check because most of the tests check entire error messages
// scalastyle:off line.size.limit
class QueryParsingErrorsSuite extends QueryTest with SharedSparkSession {

  private def parseException(sqlText: String): SparkThrowable = {
    intercept[ParseException](sql(sqlText).collect())
  }

  test("UNSUPPORTED_FEATURE: LATERAL join with NATURAL join not supported") {
    checkError(
      exception = parseException("SELECT * FROM t1 NATURAL JOIN LATERAL (SELECT c1 + c2 AS c2)"),
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "LATERAL_NATURAL_JOIN",
      sqlState = "0A000",
      context = ExpectedContext(
        fragment = "NATURAL JOIN LATERAL (SELECT c1 + c2 AS c2)",
        start = 17,
        stop = 59))
  }

  test("UNSUPPORTED_FEATURE: LATERAL join with USING join not supported") {
    checkError(
      exception = parseException("SELECT * FROM t1 JOIN LATERAL (SELECT c1 + c2 AS c2) USING (c2)"),
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "LATERAL_JOIN_USING",
      sqlState = "0A000",
      context = ExpectedContext(
        fragment = "JOIN LATERAL (SELECT c1 + c2 AS c2) USING (c2)",
        start = 17,
        stop = 62))
  }

  test("UNSUPPORTED_FEATURE: Unsupported LATERAL join type") {
    Seq(
      "RIGHT OUTER" -> (17, 74),
      "FULL OUTER" -> (17, 73),
      "LEFT SEMI" -> (17, 72),
      "LEFT ANTI" -> (17, 72)).foreach { case (joinType, (start, stop)) =>
      checkError(
        exception = parseException(s"SELECT * FROM t1 $joinType JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3"),
        errorClass = "UNSUPPORTED_FEATURE",
        errorSubClass = "LATERAL_JOIN_OF_TYPE",
        sqlState = "0A000",
        parameters = Map("joinType" -> joinType),
        context = ExpectedContext(
          fragment = s"$joinType JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3",
          start = start,
          stop = stop))
    }
  }

  test("INVALID_SQL_SYNTAX: LATERAL can only be used with subquery") {
    Seq(
      ", LATERAL t2" -> ("FROM t1, LATERAL t2", 9, 27),
      " JOIN LATERAL t2" -> ("JOIN LATERAL t2", 17, 31),
      ", LATERAL (t2 JOIN t3)" -> ("FROM t1, LATERAL (t2 JOIN t3)", 9, 37),
      ", LATERAL (LATERAL t2)" -> ("FROM t1, LATERAL (LATERAL t2)", 9, 37),
      ", LATERAL VALUES (0, 1)" -> ("FROM t1, LATERAL VALUES (0, 1)", 9, 38),
      ", LATERAL RANGE(0, 1)" -> ("FROM t1, LATERAL RANGE(0, 1)", 9, 36)
    ).foreach { case (sqlText, (fragment, start, stop)) =>
      checkError(
        exception = parseException(s"SELECT * FROM t1$sqlText"),
        errorClass = "INVALID_SQL_SYNTAX",
        sqlState = "42000",
        parameters = Map("inputString" -> "LATERAL can only be used with subquery."),
        context = ExpectedContext(fragment, start, stop))
    }
  }

  test("UNSUPPORTED_FEATURE: NATURAL CROSS JOIN is not supported") {
    checkError(
      exception = parseException("SELECT * FROM a NATURAL CROSS JOIN b"),
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "NATURAL_CROSS_JOIN",
      sqlState = "0A000",
      context = ExpectedContext(
        fragment = "NATURAL CROSS JOIN b",
        start = 16,
        stop = 35))
  }

  test("INVALID_SQL_SYNTAX: redefine window") {
    checkError(
      exception = parseException("SELECT min(a) OVER win FROM t1 WINDOW win AS win, win AS win2"),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "The definition of window `win` is repetitive."),
      context = ExpectedContext(
        fragment = "WINDOW win AS win, win AS win2",
        start = 31,
        stop = 60))
  }

  test("INVALID_SQL_SYNTAX: invalid window reference") {
    checkError(
      exception = parseException("SELECT min(a) OVER win FROM t1 WINDOW win AS win"),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Window reference `win` is not a window specification."),
      context = ExpectedContext(
        fragment = "WINDOW win AS win",
        start = 31,
        stop = 47))
  }

  test("INVALID_SQL_SYNTAX: window reference cannot be resolved") {
    checkError(
      exception = parseException("SELECT min(a) OVER win FROM t1 WINDOW win AS win2"),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Cannot resolve window reference `win2`."),
      context = ExpectedContext(
        fragment = "WINDOW win AS win2",
        start = 31,
        stop = 48))
  }

  test("UNSUPPORTED_FEATURE: TRANSFORM does not support DISTINCT/ALL") {
    val sqlText = "SELECT TRANSFORM(DISTINCT a) USING 'a' FROM t"
    checkError(
      exception = parseException(sqlText),
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "TRANSFORM_DISTINCT_ALL",
      sqlState = "0A000",
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 44))
  }

  test("UNSUPPORTED_FEATURE: In-memory mode does not support TRANSFORM with serde") {
    val sqlText = "SELECT TRANSFORM(a) ROW FORMAT SERDE " +
      "'org.apache.hadoop.hive.serde2.OpenCSVSerde' USING 'a' FROM t"
    checkError(
      exception = parseException(sqlText),
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "TRANSFORM_NON_HIVE",
      sqlState = "0A000",
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 97))
  }

  test("INVALID_SQL_SYNTAX: Too many arguments for transform") {
    checkError(
      exception = parseException("CREATE TABLE table(col int) PARTITIONED BY (years(col,col))"),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Too many arguments for transform `years`"),
      context = ExpectedContext(
        fragment = "years(col,col)",
        start = 44,
        stop = 57))
  }

  test("INVALID_SQL_SYNTAX: Invalid table value function name") {
    checkError(
      exception = parseException("SELECT * FROM db.func()"),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map(
        "inputString" -> "table valued function cannot specify database name: `db`.`func`"),
      context = ExpectedContext(
        fragment = "db.func()",
        start = 14,
        stop = 22))

    checkError(
      exception = parseException("SELECT * FROM ns.db.func()"),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map(
        "inputString" -> "table valued function cannot specify database name: `ns`.`db`.`func`"),
      context = ExpectedContext(
        fragment = "ns.db.func()",
        start = 14,
        stop = 25))
  }

  test("INVALID_SQL_SYNTAX: Invalid scope in show functions") {
    val sqlText = "SHOW sys FUNCTIONS"
    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "SHOW `sys` FUNCTIONS not supported"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 17))
  }

  test("INVALID_SQL_SYNTAX: Invalid pattern in show functions") {
    val sqlText1 = "SHOW FUNCTIONS IN db f1"
    checkError(
      exception = parseException(sqlText1),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" ->
        "Invalid pattern in SHOW FUNCTIONS: `f1`. It must be a \"STRING\" literal."),
      context = ExpectedContext(
        fragment = sqlText1,
        start = 0,
        stop = 22))
    val sqlText2 = "SHOW FUNCTIONS IN db LIKE f1"
    checkError(
      exception = parseException(sqlText2),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" ->
        "Invalid pattern in SHOW FUNCTIONS: `f1`. It must be a \"STRING\" literal."),
      context = ExpectedContext(
        fragment = sqlText2,
        start = 0,
        stop = 27))
  }

  test("INVALID_SQL_SYNTAX: Create function with both if not exists and replace") {
    val sqlText =
      """CREATE OR REPLACE FUNCTION IF NOT EXISTS func1 as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'""".stripMargin

    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" ->
        "CREATE FUNCTION with both IF NOT EXISTS and REPLACE is not allowed."),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 142))
  }

  test("INVALID_SQL_SYNTAX: Create temporary function with if not exists") {
    val sqlText =
      """CREATE TEMPORARY FUNCTION IF NOT EXISTS func1 as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'""".stripMargin

    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" ->
        "It is not allowed to define a TEMPORARY FUNCTION with IF NOT EXISTS."),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 141))
  }

  test("INVALID_SQL_SYNTAX: Create temporary function with multi-part name") {
    val sqlText =
      """CREATE TEMPORARY FUNCTION ns.db.func as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'""".stripMargin

    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Unsupported function name `ns`.`db`.`func`"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 132))
  }

  test("INVALID_SQL_SYNTAX: Specifying database while creating temporary function") {
    val sqlText =
      """CREATE TEMPORARY FUNCTION db.func as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'""".stripMargin

    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" ->
        "Specifying a database in CREATE TEMPORARY FUNCTION is not allowed: `db`"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 129))
  }

  test("INVALID_SQL_SYNTAX: Drop temporary function requires a single part name") {
    val sqlText = "DROP TEMPORARY FUNCTION db.func"
    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" ->
        "DROP TEMPORARY FUNCTION requires a single part name but got: `db`.`func`"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 30))
  }

  test("DUPLICATE_KEY: Found duplicate partition keys") {
    checkError(
      exception = parseException("INSERT OVERWRITE TABLE table PARTITION(p1='1', p1='1') SELECT 'col1', 'col2'"),
      errorClass = "DUPLICATE_KEY",
      sqlState = "23000",
      parameters = Map("keyColumn" -> "`p1`"),
      context = ExpectedContext(
        fragment = "PARTITION(p1='1', p1='1')",
        start = 29,
        stop = 53))
  }

  test("DUPLICATE_KEY: in table properties") {
    checkError(
      exception = parseException("ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('key1' = '1', 'key1' = '2')"),
      errorClass = "DUPLICATE_KEY",
      sqlState = "23000",
      parameters = Map("keyColumn" -> "`key1`"),
      context = ExpectedContext(
        fragment = "('key1' = '1', 'key1' = '2')",
        start = 39,
        stop = 66))
  }

  test("PARSE_EMPTY_STATEMENT: empty input") {
    checkError(
      exception = parseException(""),
      errorClass = "PARSE_EMPTY_STATEMENT",
      sqlState = Some("42000"))

    checkError(
      exception = parseException("   "),
      errorClass = "PARSE_EMPTY_STATEMENT",
      sqlState = Some("42000"))

    checkError(
      exception = parseException(" \n"),
      errorClass = "PARSE_EMPTY_STATEMENT",
      sqlState = Some("42000"))
  }

  test("PARSE_SYNTAX_ERROR: no viable input") {
    checkError(
      exception = parseException("select ((r + 1) "),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "end of input", "hint" -> ""))
  }

  test("PARSE_SYNTAX_ERROR: extraneous input") {
    checkError(
      exception = parseException("select 1 1"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'1'", "hint" -> ": extra input '1'"))

    checkError(
      exception = parseException("select *\nfrom r as q t"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'t'", "hint" -> ": extra input 't'"))
  }

  test("PARSE_SYNTAX_ERROR: mismatched input") {
    checkError(
      exception = parseException("select * from r order by q from t"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'from'", "hint" -> ""))

    checkError(
      exception = parseException("select *\nfrom r\norder by q\nfrom t"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'from'", "hint" -> ""))
  }

  test("PARSE_SYNTAX_ERROR: jargon token substitute to user-facing language") {
    // '<EOF>' -> end of input
    checkError(
      exception = parseException("select count(*"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "end of input", "hint" -> ""))

    checkError(
      exception = parseException("select 1 as a from"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "end of input", "hint" -> ""))
  }

  test("PARSE_SYNTAX_ERROR - SPARK-21136: " +
    "misleading error message due to problematic antlr grammar") {
    checkError(
      exception = parseException("select * from a left join_ b on a.id = b.id"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'join_'", "hint" -> ": missing 'JOIN'"))

    checkError(
      exception = parseException("select * from test where test.t is like 'test'"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'is'", "hint" -> ""))

    checkError(
      exception = parseException("SELECT * FROM test WHERE x NOT NULL"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      parameters = Map("error" -> "'NOT'", "hint" -> ""))
  }

  test("INVALID_SQL_SYNTAX: show table partition key must set value") {
    checkError(
      exception = parseException("SHOW TABLE EXTENDED IN default LIKE 'employee' PARTITION (grade)"),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Partition key `grade` must set value (can't be empty)."),
      context = ExpectedContext(
        fragment = "PARTITION (grade)",
        start = 47,
        stop = 63))
  }

  test("INVALID_SQL_SYNTAX: expected a column reference for transform bucket") {
    checkError(
      exception = parseException("CREATE TABLE my_tab(a INT, b STRING) USING parquet PARTITIONED BY (bucket(32, a, 66))"),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "Expected a column reference for transform `bucket`: 66"),
      context = ExpectedContext(
        fragment = "bucket(32, a, 66)",
        start = 67,
        stop = 83))
  }

  test("UNSUPPORTED_FEATURE: DESC TABLE COLUMN for a specific partition") {
    val sqlText = "DESCRIBE TABLE EXTENDED customer PARTITION (grade = 'A') customer.age"
    checkError(
      exception = parseException(sqlText),
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "DESC_TABLE_COLUMN_PARTITION",
      sqlState = "0A000",
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 68))
  }

  test("INVALID_SQL_SYNTAX: PARTITION specification is incomplete") {
    val sqlText = "DESCRIBE TABLE EXTENDED customer PARTITION (grade)"
    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      parameters = Map("inputString" -> "PARTITION specification is incomplete: `grade`"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 49))
  }

  test("UNSUPPORTED_FEATURE: cannot set reserved namespace property") {
    val sqlText = "CREATE NAMESPACE IF NOT EXISTS a.b.c WITH PROPERTIES ('location'='/home/user/db')"
    checkError(
      exception = parseException(sqlText),
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "SET_NAMESPACE_PROPERTY",
      sqlState = "0A000",
      parameters = Map(
        "property" -> "location",
        "msg" -> "please use the LOCATION clause to specify it"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 80))
  }

  test("UNSUPPORTED_FEATURE: cannot set reserved table property") {
    val sqlText = "CREATE TABLE student (id INT, name STRING, age INT) " +
      "USING PARQUET TBLPROPERTIES ('provider'='parquet')"
    checkError(
      exception = parseException(sqlText),
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "SET_TABLE_PROPERTY",
      sqlState = "0A000",
      parameters = Map(
        "property" -> "provider",
        "msg" -> "please use the USING clause to specify it"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 101))
  }

  test("INVALID_PROPERTY_KEY: invalid property key for set quoted configuration") {
    val sqlText = "set =`value`"
    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_PROPERTY_KEY",
      sqlState = null,
      parameters = Map("key" -> "\"\"", "value" -> "\"value\""),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 11))
  }

  test("INVALID_PROPERTY_VALUE: invalid property value for set quoted configuration") {
    checkError(
      exception = parseException("set `key`=1;2;;"),
      errorClass = "INVALID_PROPERTY_VALUE",
      sqlState = null,
      parameters = Map("value" -> "\"1;2;;\"", "key" -> "\"key\""),
      context = ExpectedContext(
        fragment = "set `key`=1;2",
        start = 0,
        stop = 12))
  }

  test("UNSUPPORTED_FEATURE: cannot set Properties and DbProperties at the same time") {
    val sqlText = "CREATE NAMESPACE IF NOT EXISTS a.b.c WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c') " +
      "WITH DBPROPERTIES('a'='a', 'b'='b', 'c'='c')"
    checkError(
      exception = parseException(sqlText),
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "SET_PROPERTIES_AND_DBPROPERTIES",
      sqlState = "0A000",
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 124))
  }
}
