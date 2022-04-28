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
      sqlState = "0A000",
      message =
        """The feature is not supported: NATURAL join with LATERAL correlation.(line 1, pos 14)
          |
          |== SQL ==
          |SELECT * FROM t1 NATURAL JOIN LATERAL (SELECT c1 + c2 AS c2)
          |--------------^^^
          |""".stripMargin)
  }

  test("UNSUPPORTED_FEATURE: LATERAL join with USING join not supported") {
    validateParsingError(
      sqlText = "SELECT * FROM t1 JOIN LATERAL (SELECT c1 + c2 AS c2) USING (c2)",
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("LATERAL_JOIN_USING"),
      sqlState = "0A000",
      message =
        """The feature is not supported: JOIN USING with LATERAL correlation.(line 1, pos 14)
          |
          |== SQL ==
          |SELECT * FROM t1 JOIN LATERAL (SELECT c1 + c2 AS c2) USING (c2)
          |--------------^^^
          |""".stripMargin)
  }

  test("UNSUPPORTED_FEATURE: Unsupported LATERAL join type") {
    Seq("RIGHT OUTER", "FULL OUTER", "LEFT SEMI", "LEFT ANTI").foreach { joinType =>
      validateParsingError(
        sqlText = s"SELECT * FROM t1 $joinType JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3",
        errorClass = "UNSUPPORTED_FEATURE",
        errorSubClass = Some("LATERAL_JOIN_OF_TYPE"),
        sqlState = "0A000",
        message =
          s"""The feature is not supported: $joinType JOIN with LATERAL correlation.(line 1, pos 14)
            |
            |== SQL ==
            |SELECT * FROM t1 $joinType JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3
            |--------------^^^
            |""".stripMargin)
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
        message =
          s"""Invalid SQL syntax: LATERAL can only be used with subquery.(line 1, pos $pos)
            |
            |== SQL ==
            |$sqlText
            |${"-" * pos}^^^
            |""".stripMargin)
    }
  }

  test("UNSUPPORTED_FEATURE: NATURAL CROSS JOIN is not supported") {
    validateParsingError(
      sqlText = "SELECT * FROM a NATURAL CROSS JOIN b",
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("NATURAL_CROSS_JOIN"),
      sqlState = "0A000",
      message =
        """The feature is not supported: NATURAL CROSS JOIN.(line 1, pos 14)
          |
          |== SQL ==
          |SELECT * FROM a NATURAL CROSS JOIN b
          |--------------^^^
          |""".stripMargin)
  }

  test("INVALID_SQL_SYNTAX: redefine window") {
    validateParsingError(
      sqlText = "SELECT min(a) OVER win FROM t1 WINDOW win AS win, win AS win2",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        """Invalid SQL syntax: The definition of window `win` is repetitive.(line 1, pos 31)
          |
          |== SQL ==
          |SELECT min(a) OVER win FROM t1 WINDOW win AS win, win AS win2
          |-------------------------------^^^
          |""".stripMargin)
  }

  test("INVALID_SQL_SYNTAX: invalid window reference") {
    validateParsingError(
      sqlText = "SELECT min(a) OVER win FROM t1 WINDOW win AS win",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        """Invalid SQL syntax: Window reference `win` is not a window specification.(line 1, pos 31)
          |
          |== SQL ==
          |SELECT min(a) OVER win FROM t1 WINDOW win AS win
          |-------------------------------^^^
          |""".stripMargin)
  }

  test("INVALID_SQL_SYNTAX: window reference cannot be resolved") {
    validateParsingError(
      sqlText = "SELECT min(a) OVER win FROM t1 WINDOW win AS win2",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        """Invalid SQL syntax: Cannot resolve window reference `win2`.(line 1, pos 31)
          |
          |== SQL ==
          |SELECT min(a) OVER win FROM t1 WINDOW win AS win2
          |-------------------------------^^^
          |""".stripMargin)
  }

  test("UNSUPPORTED_FEATURE: TRANSFORM does not support DISTINCT/ALL") {
    validateParsingError(
      sqlText = "SELECT TRANSFORM(DISTINCT a) USING 'a' FROM t",
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("TRANSFORM_DISTINCT_ALL"),
      sqlState = "0A000",
      message =
        """The feature is not supported: TRANSFORM with the DISTINCT/ALL clause.(line 1, pos 17)
          |
          |== SQL ==
          |SELECT TRANSFORM(DISTINCT a) USING 'a' FROM t
          |-----------------^^^
          |""".stripMargin)
  }

  test("UNSUPPORTED_FEATURE: In-memory mode does not support TRANSFORM with serde") {
    validateParsingError(
      sqlText = "SELECT TRANSFORM(a) ROW FORMAT SERDE " +
        "'org.apache.hadoop.hive.serde2.OpenCSVSerde' USING 'a' FROM t",
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = Some("TRANSFORM_NON_HIVE"),
      sqlState = "0A000",
      message =
        """The feature is not supported: TRANSFORM with SERDE is only supported in hive mode.(line 1, pos 0)
          |
          |== SQL ==
          |SELECT TRANSFORM(a) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' USING 'a' FROM t
          |^^^
          |""".stripMargin)
  }

  test("INVALID_SQL_SYNTAX: Too many arguments for transform") {
    validateParsingError(
      sqlText = "CREATE TABLE table(col int) PARTITIONED BY (years(col,col))",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        """Invalid SQL syntax: Too many arguments for transform `years`(line 1, pos 44)
          |
          |== SQL ==
          |CREATE TABLE table(col int) PARTITIONED BY (years(col,col))
          |--------------------------------------------^^^
          |""".stripMargin)
  }

  test("INVALID_SQL_SYNTAX: Invalid table value function name") {
    validateParsingError(
      sqlText = "SELECT * FROM db.func()",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        """Invalid SQL syntax: table valued function cannot specify database name (line 1, pos 14)
          |
          |== SQL ==
          |SELECT * FROM db.func()
          |--------------^^^
          |""".stripMargin
    )

    validateParsingError(
      sqlText = "SELECT * FROM ns.db.func()",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        """Invalid SQL syntax: table valued function cannot specify database name (line 1, pos 14)
          |
          |== SQL ==
          |SELECT * FROM ns.db.func()
          |--------------^^^
          |""".stripMargin)
  }

  test("INVALID_SQL_SYNTAX: Invalid scope in show functions") {
    validateParsingError(
      sqlText = "SHOW sys FUNCTIONS",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        """Invalid SQL syntax: SHOW `sys` FUNCTIONS not supported(line 1, pos 5)
          |
          |== SQL ==
          |SHOW sys FUNCTIONS
          |-----^^^
          |""".stripMargin)
  }

  test("INVALID_SQL_SYNTAX: Invalid pattern in show functions") {
    validateParsingError(
      sqlText = "SHOW FUNCTIONS IN db f1",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        """Invalid SQL syntax: Invalid pattern in SHOW FUNCTIONS: `f1`. It must be a "STRING" literal.(line 1, pos 21)
          |
          |== SQL ==
          |SHOW FUNCTIONS IN db f1
          |---------------------^^^
          |""".stripMargin)
    validateParsingError(
      sqlText = "SHOW FUNCTIONS IN db LIKE f1",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        """Invalid SQL syntax: Invalid pattern in SHOW FUNCTIONS: `f1`. It must be a "STRING" literal.(line 1, pos 26)
          |
          |== SQL ==
          |SHOW FUNCTIONS IN db LIKE f1
          |--------------------------^^^
          |""".stripMargin)
  }

  test("INVALID_SQL_SYNTAX: Create function with both if not exists and replace") {
    val sqlText =
      """
        |CREATE OR REPLACE FUNCTION IF NOT EXISTS func1 as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'
        |""".stripMargin
    val errorDesc =
      """CREATE FUNCTION with both IF NOT EXISTS and REPLACE is not allowed.(line 2, pos 0)"""

    validateParsingError(
      sqlText = sqlText,
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        s"""Invalid SQL syntax: $errorDesc
          |
          |== SQL ==
          |
          |CREATE OR REPLACE FUNCTION IF NOT EXISTS func1 as
          |^^^
          |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
          |JAR '/path/to/jar2'
          |""".stripMargin)
  }

  test("INVALID_SQL_SYNTAX: Create temporary function with if not exists") {
    val sqlText =
      """
        |CREATE TEMPORARY FUNCTION IF NOT EXISTS func1 as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'
        |""".stripMargin
    val errorDesc =
      """It is not allowed to define a TEMPORARY FUNCTION with IF NOT EXISTS.(line 2, pos 0)"""

    validateParsingError(
      sqlText = sqlText,
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        s"""Invalid SQL syntax: $errorDesc
          |
          |== SQL ==
          |
          |CREATE TEMPORARY FUNCTION IF NOT EXISTS func1 as
          |^^^
          |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
          |JAR '/path/to/jar2'
          |""".stripMargin)
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
      message =
        """Invalid SQL syntax: Unsupported function name `ns`.`db`.`func`(line 2, pos 0)
          |
          |== SQL ==
          |
          |CREATE TEMPORARY FUNCTION ns.db.func as
          |^^^
          |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
          |JAR '/path/to/jar2'
          |""".stripMargin)
  }

  test("INVALID_SQL_SYNTAX: Specifying database while creating temporary function") {
    val sqlText =
      """
        |CREATE TEMPORARY FUNCTION db.func as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'
        |""".stripMargin
    val errorDesc =
      """Specifying a database in CREATE TEMPORARY FUNCTION is not allowed: `db`(line 2, pos 0)"""

    validateParsingError(
      sqlText = sqlText,
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        s"""Invalid SQL syntax: $errorDesc
          |
          |== SQL ==
          |
          |CREATE TEMPORARY FUNCTION db.func as
          |^^^
          |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
          |JAR '/path/to/jar2'
          |""".stripMargin)
  }

  test("INVALID_SQL_SYNTAX: Drop temporary function requires a single part name") {
    val errorDesc =
      "DROP TEMPORARY FUNCTION requires a single part name but got: `db`.`func`(line 1, pos 0)"

    validateParsingError(
      sqlText = "DROP TEMPORARY FUNCTION db.func",
      errorClass = "INVALID_SQL_SYNTAX",
      sqlState = "42000",
      message =
        s"""Invalid SQL syntax: $errorDesc
          |
          |== SQL ==
          |DROP TEMPORARY FUNCTION db.func
          |^^^
          |""".stripMargin)
  }

  test("DUPLICATE_KEY: Found duplicate partition keys") {
    validateParsingError(
      sqlText = "INSERT OVERWRITE TABLE table PARTITION(p1='1', p1='1') SELECT 'col1', 'col2'",
      errorClass = "DUPLICATE_KEY",
      sqlState = "23000",
      message =
        """Found duplicate keys `p1`(line 1, pos 29)
          |
          |== SQL ==
          |INSERT OVERWRITE TABLE table PARTITION(p1='1', p1='1') SELECT 'col1', 'col2'
          |-----------------------------^^^
          |""".stripMargin)
  }

  test("DUPLICATE_KEY: in table properties") {
    validateParsingError(
      sqlText = "ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('key1' = '1', 'key1' = '2')",
      errorClass = "DUPLICATE_KEY",
      sqlState = "23000",
      message =
        """Found duplicate keys `key1`(line 1, pos 39)
          |
          |== SQL ==
          |ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('key1' = '1', 'key1' = '2')
          |---------------------------------------^^^
          |""".stripMargin)
  }

  test("PARSE_EMPTY_STATEMENT: empty input") {
    validateParsingError(
      sqlText = "",
      errorClass = "PARSE_EMPTY_STATEMENT",
      sqlState = "42000",
      message =
        """Syntax error, unexpected empty statement(line 1, pos 0)
          |
          |== SQL ==
          |
          |^^^
          |""".stripMargin)

    validateParsingError(
      sqlText = "   ",
      errorClass = "PARSE_EMPTY_STATEMENT",
      sqlState = "42000",
      message =
        s"""Syntax error, unexpected empty statement(line 1, pos 3)
           |
           |== SQL ==
           |${"   "}
           |---^^^
           |""".stripMargin)

    validateParsingError(
      sqlText = " \n",
      errorClass = "PARSE_EMPTY_STATEMENT",
      sqlState = "42000",
      message =
        s"""Syntax error, unexpected empty statement(line 2, pos 0)
           |
           |== SQL ==
           |${" "}
           |^^^
           |""".stripMargin)
  }

  test("PARSE_SYNTAX_ERROR: no viable input") {
    val sqlText = "select ((r + 1) "
    validateParsingError(
      sqlText = sqlText,
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      message =
        s"""Syntax error at or near end of input(line 1, pos 16)
          |
          |== SQL ==
          |$sqlText
          |----------------^^^
          |""".stripMargin)
  }

  test("PARSE_SYNTAX_ERROR: extraneous input") {
    validateParsingError(
      sqlText = "select 1 1",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      message =
        """Syntax error at or near '1': extra input '1'(line 1, pos 9)
          |
          |== SQL ==
          |select 1 1
          |---------^^^
          |""".stripMargin)

    validateParsingError(
      sqlText = "select *\nfrom r as q t",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      message =
        """Syntax error at or near 't': extra input 't'(line 2, pos 12)
          |
          |== SQL ==
          |select *
          |from r as q t
          |------------^^^
          |""".stripMargin)
  }

  test("PARSE_SYNTAX_ERROR: mismatched input") {
    validateParsingError(
      sqlText = "select * from r order by q from t",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      message =
        """Syntax error at or near 'from'(line 1, pos 27)
          |
          |== SQL ==
          |select * from r order by q from t
          |---------------------------^^^
          |""".stripMargin)

    validateParsingError(
      sqlText = "select *\nfrom r\norder by q\nfrom t",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      message =
        """Syntax error at or near 'from'(line 4, pos 0)
          |
          |== SQL ==
          |select *
          |from r
          |order by q
          |from t
          |^^^
          |""".stripMargin)
  }

  test("PARSE_SYNTAX_ERROR: jargon token substitute to user-facing language") {
    // '<EOF>' -> end of input
    validateParsingError(
      sqlText = "select count(*",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      message =
        """Syntax error at or near end of input(line 1, pos 14)
          |
          |== SQL ==
          |select count(*
          |--------------^^^
          |""".stripMargin)

    validateParsingError(
      sqlText = "select 1 as a from",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      message =
        """Syntax error at or near end of input(line 1, pos 18)
          |
          |== SQL ==
          |select 1 as a from
          |------------------^^^
          |""".stripMargin)
  }

  test("PARSE_SYNTAX_ERROR - SPARK-21136: " +
    "misleading error message due to problematic antlr grammar") {
    validateParsingError(
      sqlText = "select * from a left join_ b on a.id = b.id",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      message =
        """Syntax error at or near 'join_': missing 'JOIN'(line 1, pos 21)
          |
          |== SQL ==
          |select * from a left join_ b on a.id = b.id
          |---------------------^^^
          |""".stripMargin)

    validateParsingError(
      sqlText = "select * from test where test.t is like 'test'",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      message =
        """Syntax error at or near 'is'(line 1, pos 32)
          |
          |== SQL ==
          |select * from test where test.t is like 'test'
          |--------------------------------^^^
          |""".stripMargin)

    validateParsingError(
      sqlText = "SELECT * FROM test WHERE x NOT NULL",
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42000",
      message =
        """Syntax error at or near 'NOT'(line 1, pos 27)
          |
          |== SQL ==
          |SELECT * FROM test WHERE x NOT NULL
          |---------------------------^^^
          |""".stripMargin)
  }
}
