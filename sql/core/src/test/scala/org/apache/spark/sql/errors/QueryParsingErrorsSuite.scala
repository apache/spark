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
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.test.SharedSparkSession

// Turn of the length check because most of the tests check entire error messages
// scalastyle:off line.size.limit
class QueryParsingErrorsSuite extends QueryTest with SharedSparkSession with SQLHelper {

  private def parseException(sqlText: String): SparkThrowable = {
    intercept[ParseException](sql(sqlText).collect())
  }

  test("NAMED_PARAMETER_SUPPORT_DISABLED: named arguments not turned on") {
    withSQLConf("spark.sql.allowNamedFunctionArguments" -> "false") {
      checkError(
        exception = parseException("SELECT explode(arr => array(10, 20))"),
        errorClass = "NAMED_PARAMETER_SUPPORT_DISABLED",
        parameters = Map("functionName"-> toSQLId("explode"), "argument" -> toSQLId("arr"))
      )
    }
  }

  test("UNSUPPORTED_FEATURE: LATERAL join with NATURAL join not supported") {
    checkError(
      exception = parseException("SELECT * FROM t1 NATURAL JOIN LATERAL (SELECT c1 + c2 AS c2)"),
      errorClass = "INCOMPATIBLE_JOIN_TYPES",
      parameters = Map("joinType1" -> "LATERAL", "joinType2" -> "NATURAL"),
      sqlState = "42613",
      context = ExpectedContext(
        fragment = "NATURAL JOIN LATERAL (SELECT c1 + c2 AS c2)",
        start = 17,
        stop = 59))
  }

  test("UNSUPPORTED_FEATURE: LATERAL join with USING join not supported") {
    checkError(
      exception = parseException("SELECT * FROM t1 JOIN LATERAL (SELECT c1 + c2 AS c2) USING (c2)"),
      errorClass = "UNSUPPORTED_FEATURE.LATERAL_JOIN_USING",
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
        errorClass = "INVALID_LATERAL_JOIN_TYPE",
        parameters = Map("joinType" -> joinType),
        context = ExpectedContext(
          fragment = s"$joinType JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3",
          start = start,
          stop = stop))
    }
  }

  test("INVALID_SQL_SYNTAX.LATERAL_WITHOUT_SUBQUERY_OR_TABLE_VALUED_FUNC: " +
    "LATERAL can only be used with subquery") {
    Seq(
      ", LATERAL t2" -> ("FROM t1, LATERAL t2", 9, 27),
      " JOIN LATERAL t2" -> ("JOIN LATERAL t2", 17, 31),
      ", LATERAL (t2 JOIN t3)" -> ("FROM t1, LATERAL (t2 JOIN t3)", 9, 37),
      ", LATERAL (LATERAL t2)" -> ("FROM t1, LATERAL (LATERAL t2)", 9, 37),
      ", LATERAL VALUES (0, 1)" -> ("FROM t1, LATERAL VALUES (0, 1)", 9, 38)
    ).foreach { case (sqlText, (fragment, start, stop)) =>
      checkError(
        exception = parseException(s"SELECT * FROM t1$sqlText"),
        errorClass = "INVALID_SQL_SYNTAX.LATERAL_WITHOUT_SUBQUERY_OR_TABLE_VALUED_FUNC",
        sqlState = "42000",
        context = ExpectedContext(fragment, start, stop))
    }
  }

  test("UNSUPPORTED_FEATURE: NATURAL CROSS JOIN is not supported") {
    checkError(
      exception = parseException("SELECT * FROM a NATURAL CROSS JOIN b"),
      errorClass = "INCOMPATIBLE_JOIN_TYPES",
      parameters = Map("joinType1" -> "NATURAL", "joinType2" -> "CROSS"),
      sqlState = "42613",
      context = ExpectedContext(
        fragment = "NATURAL CROSS JOIN b",
        start = 16,
        stop = 35))
  }

  test("INVALID_SQL_SYNTAX.REPETITIVE_WINDOW_DEFINITION: redefine window") {
    checkError(
      exception = parseException("SELECT min(a) OVER win FROM t1 WINDOW win AS win, win AS win2"),
      errorClass = "INVALID_SQL_SYNTAX.REPETITIVE_WINDOW_DEFINITION",
      sqlState = "42000",
      parameters = Map("windowName" -> "`win`"),
      context = ExpectedContext(
        fragment = "WINDOW win AS win, win AS win2",
        start = 31,
        stop = 60))
  }

  test("INVALID_SQL_SYNTAX.INVALID_WINDOW_REFERENCE: invalid window reference") {
    checkError(
      exception = parseException("SELECT min(a) OVER win FROM t1 WINDOW win AS win"),
      errorClass = "INVALID_SQL_SYNTAX.INVALID_WINDOW_REFERENCE",
      sqlState = "42000",
      parameters = Map("windowName" -> "`win`"),
      context = ExpectedContext(
        fragment = "WINDOW win AS win",
        start = 31,
        stop = 47))
  }

  test("INVALID_SQL_SYNTAX.UNRESOLVED_WINDOW_REFERENCE: window reference cannot be resolved") {
    checkError(
      exception = parseException("SELECT min(a) OVER win FROM t1 WINDOW win AS win2"),
      errorClass = "INVALID_SQL_SYNTAX.UNRESOLVED_WINDOW_REFERENCE",
      sqlState = "42000",
      parameters = Map("windowName" -> "`win2`"),
      context = ExpectedContext(
        fragment = "WINDOW win AS win2",
        start = 31,
        stop = 48))
  }

  test("UNSUPPORTED_FEATURE: TRANSFORM does not support DISTINCT/ALL") {
    val sqlText = "SELECT TRANSFORM(DISTINCT a) USING 'a' FROM t"
    checkError(
      exception = parseException(sqlText),
      errorClass = "UNSUPPORTED_FEATURE.TRANSFORM_DISTINCT_ALL",
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
      errorClass = "UNSUPPORTED_FEATURE.TRANSFORM_NON_HIVE",
      sqlState = "0A000",
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 97))
  }

  test("INVALID_SQL_SYNTAX.TRANSFORM_WRONG_NUM_ARGS: Wrong number arguments for transform") {
    checkError(
      exception = parseException("CREATE TABLE table(col int) PARTITIONED BY (years(col,col))"),
      errorClass = "INVALID_SQL_SYNTAX.TRANSFORM_WRONG_NUM_ARGS",
      sqlState = "42000",
      parameters = Map(
        "transform" -> "`years`",
        "expectedNum" -> "1",
        "actualNum" -> "2"),
      context = ExpectedContext(
        fragment = "years(col,col)",
        start = 44,
        stop = 57))
  }

  test("INVALID_SQL_SYNTAX.INVALID_TABLE_VALUED_FUNC_NAME: Invalid table value function name") {
    checkError(
      exception = parseException("SELECT * FROM db.func()"),
      errorClass = "INVALID_SQL_SYNTAX.INVALID_TABLE_VALUED_FUNC_NAME",
      sqlState = "42000",
      parameters = Map("funcName" -> "`db`.`func`"),
      context = ExpectedContext(
        fragment = "db.func()",
        start = 14,
        stop = 22))

    checkError(
      exception = parseException("SELECT * FROM ns.db.func()"),
      errorClass = "INVALID_SQL_SYNTAX.INVALID_TABLE_VALUED_FUNC_NAME",
      sqlState = "42000",
      parameters = Map("funcName" -> "`ns`.`db`.`func`"),
      context = ExpectedContext(
        fragment = "ns.db.func()",
        start = 14,
        stop = 25))
  }

  test("INVALID_SQL_SYNTAX.SHOW_FUNCTIONS_INVALID_SCOPE: Invalid scope in show functions") {
    val sqlText = "SHOW sys FUNCTIONS"
    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX.SHOW_FUNCTIONS_INVALID_SCOPE",
      sqlState = "42000",
      parameters = Map("scope" -> "`sys`"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 17))
  }

  test("INVALID_SQL_SYNTAX.SHOW_FUNCTIONS_INVALID_PATTERN: Invalid pattern in show functions") {
    val sqlText1 = "SHOW FUNCTIONS IN db f1"
    checkError(
      exception = parseException(sqlText1),
      errorClass = "INVALID_SQL_SYNTAX.SHOW_FUNCTIONS_INVALID_PATTERN",
      sqlState = "42000",
      parameters = Map("pattern" -> "`f1`"),
      context = ExpectedContext(
        fragment = sqlText1,
        start = 0,
        stop = 22))
    val sqlText2 = "SHOW FUNCTIONS IN db LIKE f1"
    checkError(
      exception = parseException(sqlText2),
      errorClass = "INVALID_SQL_SYNTAX.SHOW_FUNCTIONS_INVALID_PATTERN",
      sqlState = "42000",
      parameters = Map("pattern" -> "`f1`"),
      context = ExpectedContext(
        fragment = sqlText2,
        start = 0,
        stop = 27))
  }

  test("INVALID_SQL_SYNTAX.CREATE_FUNC_WITH_IF_NOT_EXISTS_AND_REPLACE: " +
    "Create function with both if not exists and replace") {
    val sqlText =
      """CREATE OR REPLACE FUNCTION IF NOT EXISTS func1 as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'""".stripMargin

    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX.CREATE_FUNC_WITH_IF_NOT_EXISTS_AND_REPLACE",
      sqlState = "42000",
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 142))
  }

  test("INVALID_SQL_SYNTAX.CREATE_TEMP_FUNC_WITH_IF_NOT_EXISTS: " +
    "Create temporary function with if not exists") {
    val sqlText =
      """CREATE TEMPORARY FUNCTION IF NOT EXISTS func1 as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'""".stripMargin

    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX.CREATE_TEMP_FUNC_WITH_IF_NOT_EXISTS",
      sqlState = "42000",
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 141))
  }

  test("INVALID_SQL_SYNTAX.MULTI_PART_NAME: Create temporary function with multi-part name") {
    val sqlText =
      """CREATE TEMPORARY FUNCTION ns.db.func as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'""".stripMargin

    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX.MULTI_PART_NAME",
      sqlState = "42000",
      parameters = Map(
        "statement" -> "CREATE TEMPORARY FUNCTION",
        "funcName" -> "`ns`.`db`.`func`"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 132))
  }

  test("INVALID_SQL_SYNTAX.CREATE_TEMP_FUNC_WITH_DATABASE: " +
    "Specifying database while creating temporary function") {
    val sqlText =
      """CREATE TEMPORARY FUNCTION db.func as
        |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'""".stripMargin

    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX.CREATE_TEMP_FUNC_WITH_DATABASE",
      sqlState = "42000",
      parameters = Map("database" -> "`db`"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 129))
  }

  test("INVALID_SQL_SYNTAX.MULTI_PART_NAME: Drop temporary function requires a single part name") {
    val sqlText = "DROP TEMPORARY FUNCTION db.func"
    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX.MULTI_PART_NAME",
      sqlState = "42000",
      parameters = Map(
        "statement" -> "DROP TEMPORARY FUNCTION",
        "funcName" -> "`db`.`func`"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 30))
  }

  test("DUPLICATE_KEY: Found duplicate partition keys") {
    checkError(
      exception = parseException("INSERT OVERWRITE TABLE table PARTITION(p1='1', p1='1') SELECT 'col1', 'col2'"),
      errorClass = "DUPLICATE_KEY",
      sqlState = "23505",
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
      sqlState = "23505",
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
      sqlState = Some("42617"))

    checkError(
      exception = parseException("   "),
      errorClass = "PARSE_EMPTY_STATEMENT",
      sqlState = Some("42617"))

    checkError(
      exception = parseException(" \n"),
      errorClass = "PARSE_EMPTY_STATEMENT",
      sqlState = Some("42617"))
  }

  test("PARSE_SYNTAX_ERROR: no viable input") {
    checkError(
      exception = parseException("select ((r + 1) "),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "end of input", "hint" -> ""))
  }

  def checkParseSyntaxError(sqlCommand: String, errorString: String, hint: String = ""): Unit = {
    checkError(
      exception = parseException(sqlCommand),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> errorString, "hint" -> hint)
    )
  }

  test("PARSE_SYNTAX_ERROR: named arguments invalid syntax") {
    checkParseSyntaxError("select * from my_tvf(arg1 ==> 'value1')", "'>'")
    checkParseSyntaxError("select * from my_tvf(arg1 = => 'value1')", "'=>'")
    checkParseSyntaxError("select * from my_tvf((arg1 => 'value1'))", "'=>'")
    checkParseSyntaxError("select * from my_tvf(arg1 => )", "')'")
    checkParseSyntaxError("select * from my_tvf(arg1 => , 42)", "','")
    checkParseSyntaxError("select * from my_tvf(my_tvf.arg1 => 'value1')", "'=>'")
  }

  test("PARSE_SYNTAX_ERROR: extraneous input") {
    checkError(
      exception = parseException("select 1 1"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'1'", "hint" -> ": extra input '1'"))

    checkError(
      exception = parseException("select *\nfrom r as q t"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'t'", "hint" -> ": extra input 't'"))
  }

  test("PARSE_SYNTAX_ERROR: mismatched input") {
    checkError(
      exception = parseException("select * from r order by q from t"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'from'", "hint" -> ""))

    checkError(
      exception = parseException("select *\nfrom r\norder by q\nfrom t"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'from'", "hint" -> ""))
  }

  test("PARSE_SYNTAX_ERROR: jargon token substitute to user-facing language") {
    // '<EOF>' -> end of input
    checkError(
      exception = parseException("select count(*"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "end of input", "hint" -> ""))

    checkError(
      exception = parseException("select 1 as a from"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "end of input", "hint" -> ""))
  }

  test("PARSE_SYNTAX_ERROR - SPARK-21136: " +
    "misleading error message due to problematic antlr grammar") {
    checkError(
      exception = parseException("select * from a left join_ b on a.id = b.id"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'join_'", "hint" -> ": missing 'JOIN'"))

    checkError(
      exception = parseException("select * from test where test.t is like 'test'"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'is'", "hint" -> ""))

    checkError(
      exception = parseException("SELECT * FROM test WHERE x NOT NULL"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'NOT'", "hint" -> ""))
  }

  test("INVALID_SQL_SYNTAX.EMPTY_PARTITION_VALUE: show table partition key must set value") {
    checkError(
      exception = parseException("SHOW TABLE EXTENDED IN default LIKE 'employee' PARTITION (grade)"),
      errorClass = "INVALID_SQL_SYNTAX.EMPTY_PARTITION_VALUE",
      sqlState = "42000",
      parameters = Map("partKey" -> "`grade`"),
      context = ExpectedContext(
        fragment = "PARTITION (grade)",
        start = 47,
        stop = 63))
  }

  test("INVALID_SQL_SYNTAX.INVALID_COLUMN_REFERENCE: " +
    "expected a column reference for transform bucket") {
    checkError(
      exception = parseException("CREATE TABLE my_tab(a INT, b STRING) " +
        "USING parquet PARTITIONED BY (bucket(32, a, 66))"),
      errorClass = "INVALID_SQL_SYNTAX.INVALID_COLUMN_REFERENCE",
      sqlState = "42000",
      parameters = Map(
        "transform" -> "`bucket`",
        "expr" -> "66"),
      context = ExpectedContext(
        fragment = "bucket(32, a, 66)",
        start = 67,
        stop = 83))
  }

  test("UNSUPPORTED_FEATURE: DESC TABLE COLUMN for a specific partition") {
    val sqlText = "DESCRIBE TABLE EXTENDED customer PARTITION (grade = 'A') customer.age"
    checkError(
      exception = parseException(sqlText),
      errorClass = "UNSUPPORTED_FEATURE.DESC_TABLE_COLUMN_PARTITION",
      sqlState = "0A000",
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 68))
  }

  test("INVALID_SQL_SYNTAX.EMPTY_PARTITION_VALUE: PARTITION specification is incomplete") {
    val sqlText = "DESCRIBE TABLE EXTENDED customer PARTITION (grade)"
    checkError(
      exception = parseException(sqlText),
      errorClass = "INVALID_SQL_SYNTAX.EMPTY_PARTITION_VALUE",
      sqlState = "42000",
      parameters = Map("partKey" -> "`grade`"),
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 49))
  }

  test("UNSUPPORTED_FEATURE: cannot set reserved namespace property") {
    val sqlText = "CREATE NAMESPACE IF NOT EXISTS a.b.c WITH PROPERTIES ('location'='/home/user/db')"
    checkError(
      exception = parseException(sqlText),
      errorClass = "UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY",
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
      errorClass = "UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY",
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
      errorClass = "UNSUPPORTED_FEATURE.SET_PROPERTIES_AND_DBPROPERTIES",
      sqlState = "0A000",
      context = ExpectedContext(
        fragment = sqlText,
        start = 0,
        stop = 124))
  }

  test("INCOMPLETE_TYPE_DEFINITION: array type definition is incomplete") {
    // Cast simple array without specifying element type
    checkError(
      exception = parseException("SELECT CAST(array(1,2,3) AS ARRAY)"),
      errorClass = "INCOMPLETE_TYPE_DEFINITION.ARRAY",
      sqlState = "42K01",
      parameters = Map("elementType" -> "<INT>"),
      context = ExpectedContext(fragment = "ARRAY", start = 28, stop = 32))
    // Cast array of array without specifying element type for inner array
    checkError(
      exception = parseException("SELECT CAST(array(array(3)) AS ARRAY<ARRAY>)"),
      errorClass = "INCOMPLETE_TYPE_DEFINITION.ARRAY",
      sqlState = "42K01",
      parameters = Map("elementType" -> "<INT>"),
      context = ExpectedContext(fragment = "ARRAY", start = 37, stop = 41))
    // Create column of array type without specifying element type
    checkError(
      exception = parseException("CREATE TABLE tbl_120691 (col1 ARRAY)"),
      errorClass = "INCOMPLETE_TYPE_DEFINITION.ARRAY",
      sqlState = "42K01",
      parameters = Map("elementType" -> "<INT>"),
      context = ExpectedContext(fragment = "ARRAY", start = 30, stop = 34))
    // Create column of array type without specifying element type in lowercase
    checkError(
      exception = parseException("CREATE TABLE tbl_120691 (col1 array)"),
      errorClass = "INCOMPLETE_TYPE_DEFINITION.ARRAY",
      sqlState = "42K01",
      parameters = Map("elementType" -> "<INT>"),
      context = ExpectedContext(fragment = "array", start = 30, stop = 34))
  }

  test("INCOMPLETE_TYPE_DEFINITION: struct type definition is incomplete") {
    // Cast simple struct without specifying field type
    checkError(
      exception = parseException("SELECT CAST(struct(1,2,3) AS STRUCT)"),
      errorClass = "INCOMPLETE_TYPE_DEFINITION.STRUCT",
      sqlState = "42K01",
      context = ExpectedContext(fragment = "STRUCT", start = 29, stop = 34))
    // Cast array of struct without specifying field type in struct
    checkError(
      exception = parseException("SELECT CAST(array(struct(1,2)) AS ARRAY<STRUCT>)"),
      errorClass = "INCOMPLETE_TYPE_DEFINITION.STRUCT",
      sqlState = "42K01",
      context = ExpectedContext(fragment = "STRUCT", start = 40, stop = 45))
    // Create column of struct type without specifying field type
    checkError(
      exception = parseException("CREATE TABLE tbl_120691 (col1 STRUCT)"),
      errorClass = "INCOMPLETE_TYPE_DEFINITION.STRUCT",
      sqlState = "42K01",
      context = ExpectedContext(fragment = "STRUCT", start = 30, stop = 35))
    // Invalid syntax `STRUCT<INT>` without field name
    checkError(
      exception = parseException("SELECT CAST(struct(1,2,3) AS STRUCT<INT>)"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'>'", "hint" -> ""))
    // Create column of struct type without specifying field type in lowercase
    checkError(
      exception = parseException("CREATE TABLE tbl_120691 (col1 struct)"),
      errorClass = "INCOMPLETE_TYPE_DEFINITION.STRUCT",
      sqlState = "42K01",
      context = ExpectedContext(fragment = "struct", start = 30, stop = 35))
  }

  test("INCOMPLETE_TYPE_DEFINITION: map type definition is incomplete") {
    // Cast simple map without specifying element type
    checkError(
      exception = parseException("SELECT CAST(map(1,'2') AS MAP)"),
      errorClass = "INCOMPLETE_TYPE_DEFINITION.MAP",
      sqlState = "42K01",
      context = ExpectedContext(fragment = "MAP", start = 26, stop = 28))
    // Create column of map type without specifying key/value types
    checkError(
      exception = parseException("CREATE TABLE tbl_120691 (col1 MAP)"),
      errorClass = "INCOMPLETE_TYPE_DEFINITION.MAP",
      sqlState = "42K01",
      context = ExpectedContext(fragment = "MAP", start = 30, stop = 32))
    // Invalid syntax `MAP<String>` with only key type
    checkError(
      exception = parseException("SELECT CAST(map('1',2) AS MAP<STRING>)"),
      errorClass = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'>'", "hint" -> ""))
    // Create column of map type without specifying key/value types in lowercase
    checkError(
      exception = parseException("SELECT CAST(map('1',2) AS map)"),
      errorClass = "INCOMPLETE_TYPE_DEFINITION.MAP",
      sqlState = "42K01",
      context = ExpectedContext(fragment = "map", start = 26, stop = 28))
  }

  test("INVALID_ESC: Escape string must contain only one character") {
    checkError(
      exception = parseException("select * from test where test.t like 'pattern%' escape '##'"),
      errorClass = "INVALID_ESC",
      parameters = Map("invalidEscape" -> "'##'"),
      context = ExpectedContext(
        fragment = "like 'pattern%' escape '##'",
        start = 32,
        stop = 58))
  }
}
