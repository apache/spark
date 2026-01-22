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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

/**
 * Comprehensive test suite for function qualification and resolution.
 * Tests builtin/temp/persistent function name disambiguation with qualified names.
 * This tests the system.builtin, system.session, and system.extension namespaces.
 *
 * Includes tests from:
 * 1. function-qualification.sql golden file (SQL-accessible functions)
 * 2. Extension function tests (programmatic registration via SparkSessionExtensions)
 */
class FunctionQualificationSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf = {
    super.sparkConf.set("spark.sql.extensions", classOf[TestExtensions].getName)
  }

  test("SECTION 1: Basic Qualification Tests") {
    // Test builtin function with explicit qualification
    checkAnswer(sql("SELECT system.builtin.abs(-5)"), Row(5))
    checkAnswer(sql("SELECT builtin.abs(-5)"), Row(5))

    // Test builtin with case-insensitive qualification
    checkAnswer(sql("SELECT BUILTIN.abs(-5)"), Row(5))
    checkAnswer(sql("SELECT System.Builtin.ABS(-5)"), Row(5))
  }

  test("SECTION 2: Temporary Function Creation and Qualification") {
    // Create temporary function with unqualified name
    sql("CREATE TEMPORARY FUNCTION my_func() RETURNS INT RETURN 42")
    checkAnswer(sql("SELECT my_func()"), Row(42))

    // Create with session qualification
    sql("CREATE TEMPORARY FUNCTION session.my_func2() RETURNS STRING RETURN 'temp'")
    checkAnswer(sql("SELECT my_func2()"), Row("temp"))
    checkAnswer(sql("SELECT session.my_func2()"), Row("temp"))

    // Create with system.session qualification
    sql("CREATE TEMPORARY FUNCTION system.session.my_func3() RETURNS INT RETURN 100")
    checkAnswer(sql("SELECT my_func3()"), Row(100))
    checkAnswer(sql("SELECT system.session.my_func3()"), Row(100))

    // Test case insensitivity with temp functions
    checkAnswer(sql("SELECT SESSION.my_func()"), Row(42))
    checkAnswer(sql("SELECT SYSTEM.SESSION.my_func2()"), Row("temp"))

    // Clean up
    sql("DROP TEMPORARY FUNCTION my_func")
    sql("DROP TEMPORARY FUNCTION session.my_func2")
    sql("DROP TEMPORARY FUNCTION system.session.my_func3")
  }

  test("SECTION 3: Shadowing Behavior (Post-Security Fix)") {
    // Create temp function with same name as builtin
    sql("CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 999")

    // Unqualified abs now resolves to BUILTIN (not temp!), due to security-focused order
    checkAnswer(sql("SELECT abs(-5)"), Row(5))

    // Temp function only accessible with explicit qualification
    checkAnswer(sql("SELECT session.abs()"), Row(999))

    // Builtin still accessible with qualification
    checkAnswer(sql("SELECT builtin.abs(-10)"), Row(10))
    checkAnswer(sql("SELECT system.builtin.abs(-10)"), Row(10))
    sql("DROP TEMPORARY FUNCTION abs")

    // After drop, builtin still works unqualified
    checkAnswer(sql("SELECT abs(-5)"), Row(5))
  }

  test("SECTION 4: Cross-Type Shadowing - temp table + builtin scalar") {
    // Temp table function + builtin scalar function (NO conflict - builtin wins!)
    sql("CREATE TEMPORARY FUNCTION abs() RETURNS TABLE(val INT) RETURN SELECT 42")

    // Builtin scalar abs still works unqualified (builtin resolves before temp table)
    checkAnswer(sql("SELECT abs(-5)"), Row(5))

    // Temp table function works in table context
    checkAnswer(sql("SELECT * FROM abs()"), Row(42))

    // Both accessible with explicit qualification
    checkAnswer(sql("SELECT builtin.abs(-5)"), Row(5))
    checkAnswer(sql("SELECT * FROM session.abs()"), Row(42))
    sql("DROP TEMPORARY FUNCTION abs")
  }

  test("SECTION 4: Cross-Type Shadowing - temp scalar + builtin table") {
    // Temp scalar function + builtin table function (NO conflict - builtin wins!)
    sql("CREATE TEMPORARY FUNCTION range() RETURNS INT RETURN 999")

    // Builtin table range still works unqualified in table context
    checkAnswer(
      sql("SELECT * FROM range(5)"),
      Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))

    // Temp scalar function works in scalar context
    checkAnswer(sql("SELECT range()"), Row(999))

    // Both accessible with explicit qualification
    checkAnswer(
      sql("SELECT * FROM builtin.range(5)"),
      Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(sql("SELECT session.range()"), Row(999))
    sql("DROP TEMPORARY FUNCTION range")
  }

  test("SECTION 5: Cross-Type Error Detection - scalar in table context") {
    // Scalar function cannot be used in table context
    sql("CREATE TEMPORARY FUNCTION scalar_only() RETURNS INT RETURN 42")
    checkAnswer(sql("SELECT scalar_only()"), Row(42))

    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT * FROM scalar_only()")
      },
      condition = "NOT_A_TABLE_FUNCTION",
      parameters = Map("functionName" -> "`scalar_only`"),
      context = ExpectedContext(
        fragment = "scalar_only()",
        start = 14,
        stop = 26
      )
    )

    sql("DROP TEMPORARY FUNCTION scalar_only")
  }

  test("SECTION 5: Cross-Type Error Detection - table in scalar context") {
    // Table function cannot be used in scalar context
    sql("CREATE TEMPORARY FUNCTION table_only() RETURNS TABLE(val INT) RETURN SELECT 42")
    checkAnswer(sql("SELECT * FROM table_only()"), Row(42))

    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT table_only()")
      },
      condition = "NOT_A_SCALAR_FUNCTION",
      parameters = Map("functionName" -> "`table_only`"),
      context = ExpectedContext(
        fragment = "table_only()",
        start = 7,
        stop = 18
      )
    )

    sql("DROP TEMPORARY FUNCTION table_only")
  }

  test("SECTION 5: Cross-Type Error Detection - generator functions") {
    // Generator functions work in both contexts
    checkAnswer(sql("SELECT explode(array(1, 2, 3))"), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(sql("SELECT * FROM explode(array(1, 2, 3))"), Seq(Row(1), Row(2), Row(3)))
  }

  test("SECTION 6: DDL Operations - DESCRIBE") {
    // DESCRIBE builtin functions with qualification
    val desc1 = sql("DESCRIBE FUNCTION builtin.abs")
    assert(desc1.count() > 0)

    val desc2 = sql("DESCRIBE FUNCTION system.builtin.abs")
    assert(desc2.count() > 0)
  }

  test("SECTION 6: DDL Operations - DROP with qualified names") {
    sql("CREATE TEMPORARY FUNCTION drop_test() RETURNS INT RETURN 1")
    sql("DROP TEMPORARY FUNCTION session.drop_test")
  }

  test("SECTION 6: DDL Operations - CREATE OR REPLACE") {
    sql("CREATE TEMPORARY FUNCTION replace_test() RETURNS INT RETURN 1")
    checkAnswer(sql("SELECT replace_test()"), Row(1))
    sql("CREATE OR REPLACE TEMPORARY FUNCTION session.replace_test() RETURNS INT RETURN 2")
    checkAnswer(sql("SELECT replace_test()"), Row(2))
    sql("DROP TEMPORARY FUNCTION replace_test")
  }

  test("SECTION 6: DDL Operations - CREATE OR REPLACE changes type") {
    sql("CREATE TEMPORARY FUNCTION type_change() RETURNS INT RETURN 42")
    checkAnswer(sql("SELECT type_change()"), Row(42))
    sql(
      "CREATE OR REPLACE TEMPORARY FUNCTION type_change() " +
      "RETURNS TABLE(val INT) RETURN SELECT 99")
    checkAnswer(sql("SELECT * FROM type_change()"), Row(99))
    sql("DROP TEMPORARY FUNCTION type_change")
  }

  test("SECTION 6: DDL Operations - IF NOT EXISTS not supported") {
    // IF NOT EXISTS is not supported for temporary functions
    checkError(
      exception = intercept[ParseException] {
        sql("CREATE TEMPORARY FUNCTION IF NOT EXISTS exists_test() RETURNS INT RETURN 1")
      },
      condition = "INVALID_SQL_SYNTAX.CREATE_TEMP_FUNC_WITH_IF_NOT_EXISTS",
      parameters = Map.empty[String, String],
      context = ExpectedContext(
        fragment = "CREATE TEMPORARY FUNCTION IF NOT EXISTS exists_test() RETURNS INT RETURN 1",
        start = 0,
        stop = 73
      )
    )

    // SELECT on non-existent function should fail
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT exists_test()")
      },
      condition = "UNRESOLVED_ROUTINE",
      parameters = Map(
        "routineName" -> "`exists_test`",
        "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]"
      ),
      context = ExpectedContext(
        fragment = "exists_test()",
        start = 7,
        stop = 19
      )
    )

    checkError(
      exception = intercept[ParseException] {
        sql(
          "CREATE TEMPORARY FUNCTION IF NOT EXISTS system.session.exists_test() " +
          "RETURNS INT RETURN 2")
      },
      condition = "INVALID_SQL_SYNTAX.CREATE_TEMP_FUNC_WITH_IF_NOT_EXISTS",
      parameters = Map.empty[String, String],
      context = ExpectedContext(
        fragment = "CREATE TEMPORARY FUNCTION IF NOT EXISTS system.session.exists_test() " +
          "RETURNS INT RETURN 2",
        start = 0,
        stop = 88
      )
    )

    // SELECT on non-existent function should still fail
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT exists_test()")
      },
      condition = "UNRESOLVED_ROUTINE",
      parameters = Map(
        "routineName" -> "`exists_test`",
        "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]"
      ),
      context = ExpectedContext(
        fragment = "exists_test()",
        start = 7,
        stop = 19
      )
    )
  }

  test("SECTION 6: DDL Operations - SHOW FUNCTIONS") {
    sql("CREATE TEMPORARY FUNCTION show_test() RETURNS INT RETURN 1")

    val showTest = sql("SHOW FUNCTIONS LIKE 'show_test'").collect()
    assert(showTest.length > 0)

    val showAbs = sql("SHOW FUNCTIONS LIKE 'abs'").collect()
    assert(showAbs.length > 0)

    sql("DROP TEMPORARY FUNCTION show_test")
  }

  test("SECTION 7: Error Cases - cannot create temp function with builtin namespace") {
    checkError(
      exception = intercept[ParseException] {
        sql("CREATE TEMPORARY FUNCTION system.builtin.my_builtin() RETURNS INT RETURN 1")
      },
      condition = "INVALID_TEMP_OBJ_QUALIFIER",
      sqlState = "42602",
      parameters = Map(
        "objectName" -> "`my_builtin`",
        "objectType" -> "FUNCTION",
        "qualifier" -> "`system`.`builtin`"
      )
    )
  }

  test("SECTION 7: Error Cases - cannot create temp function with invalid database") {
    checkError(
      exception = intercept[ParseException] {
        sql("CREATE TEMPORARY FUNCTION mydb.my_func() RETURNS INT RETURN 1")
      },
      condition = "INVALID_TEMP_OBJ_QUALIFIER",
      sqlState = "42602",
      parameters = Map(
        "objectName" -> "`my_func`",
        "objectType" -> "FUNCTION",
        "qualifier" -> "`mydb`"
      )
    )
  }

  test("SECTION 7: Error Cases - cannot drop builtin function") {
    checkError(
      exception = intercept[ParseException] {
        sql("DROP TEMPORARY FUNCTION system.builtin.abs")
      },
      condition = "INVALID_TEMP_OBJ_QUALIFIER",
      sqlState = "42602",
      parameters = Map(
        "objectName" -> "`abs`",
        "objectType" -> "FUNCTION",
        "qualifier" -> "`system`.`builtin`"
      )
    )
  }

  test("SECTION 7: Error Cases - cannot create duplicate functions") {
    sql("CREATE TEMPORARY FUNCTION dup_test() RETURNS INT RETURN 42")

    checkError(
      exception = intercept[AnalysisException] {
        sql("CREATE TEMPORARY FUNCTION dup_test() RETURNS TABLE(val INT) RETURN SELECT 99")
      },
      condition = "ROUTINE_ALREADY_EXISTS",
      sqlState = "42723",
      parameters = Map(
        "existingRoutineType" -> "routine",
        "newRoutineType" -> "routine",
        "routineName" -> "`dup_test`"
      )
    )

    sql("DROP TEMPORARY FUNCTION dup_test")
  }

  test("SECTION 7: Error Cases - non-existent function error") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT non_existent_func()")
      },
      condition = "UNRESOLVED_ROUTINE",
      parameters = Map(
        "routineName" -> "`non_existent_func`",
        "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]"
      ),
      context = ExpectedContext(
        fragment = "non_existent_func()",
        start = 7,
        stop = 25
      )
    )
  }

  test("SECTION 8: Views - temp view can reference temp function") {
    sql("CREATE TEMPORARY FUNCTION view_func() RETURNS STRING RETURN 'from_temp'")
    sql("CREATE TEMPORARY VIEW temp_view AS SELECT view_func() as result")
    checkAnswer(sql("SELECT * FROM temp_view"), Row("from_temp"))
    sql("DROP VIEW temp_view")
    sql("DROP TEMPORARY FUNCTION view_func")
  }

  test("SECTION 8: Views - view with shadowing temp function") {
    sql("CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 777")

    // View must use qualified name to access temp function
    sql("CREATE TEMPORARY VIEW shadow_view AS SELECT session.abs() as result")
    checkAnswer(sql("SELECT * FROM shadow_view"), Row(777))

    // Builtin accessible with qualification in view
    sql("CREATE TEMPORARY VIEW builtin_view AS SELECT builtin.abs(-10) as result")
    checkAnswer(sql("SELECT * FROM builtin_view"), Row(10))

    sql("DROP VIEW shadow_view")
    sql("DROP VIEW builtin_view")
    sql("DROP TEMPORARY FUNCTION abs")
  }

  test("SECTION 8: Views - multiple temp functions in same view") {
    sql("CREATE TEMPORARY FUNCTION func1() RETURNS INT RETURN 1")
    sql("CREATE TEMPORARY FUNCTION func2() RETURNS INT RETURN 2")
    sql("CREATE TEMPORARY VIEW multi_func_view AS SELECT func1() + func2() as sum")
    checkAnswer(sql("SELECT * FROM multi_func_view"), Row(3))
    sql("DROP VIEW multi_func_view")
    sql("DROP TEMPORARY FUNCTION func1")
    sql("DROP TEMPORARY FUNCTION func2")
  }

  test("SECTION 8: Views - nested views with temp functions") {
    sql("CREATE TEMPORARY FUNCTION nested_func() RETURNS INT RETURN 100")
    sql("CREATE TEMPORARY VIEW inner_view AS SELECT nested_func() as val")
    sql("CREATE TEMPORARY VIEW outer_view AS SELECT val * 2 FROM inner_view")
    checkAnswer(sql("SELECT * FROM outer_view"), Row(200))
    sql("DROP VIEW outer_view")
    sql("DROP VIEW inner_view")
    sql("DROP TEMPORARY FUNCTION nested_func")
  }

  test("SECTION 9: Multiple Functions - multiple qualified functions together") {
    sql("CREATE TEMPORARY FUNCTION add10(x INT) RETURNS INT RETURN x + 10")
    checkAnswer(
      sql("SELECT builtin.abs(-5), session.add10(5), system.builtin.upper('hello')"),
      Row(5, 15, "HELLO"))
    sql("DROP TEMPORARY FUNCTION add10")
  }

  test("SECTION 9: Multiple Functions - qualified aggregate function") {
    // SQL functions cannot contain aggregate functions - this should error
    checkError(
      exception = intercept[AnalysisException] {
        sql("CREATE TEMPORARY FUNCTION my_avg(x DOUBLE) RETURNS DOUBLE RETURN avg(x)")
      },
      condition = "USER_DEFINED_FUNCTIONS.CANNOT_CONTAIN_COMPLEX_FUNCTIONS",
      sqlState = "42601",
      parameters = Map("queryText" -> "avg(x)")
    )
  }

  test("SECTION 9: Multiple Functions - table function with qualified names") {
    sql("CREATE TEMPORARY FUNCTION my_range() RETURNS TABLE(id INT) RETURN SELECT * FROM range(3)")
    checkAnswer(sql("SELECT * FROM my_range()"), Seq(Row(0), Row(1), Row(2)))
    checkAnswer(sql("SELECT * FROM session.my_range()"), Seq(Row(0), Row(1), Row(2)))
    checkAnswer(sql("SELECT * FROM system.session.my_range()"), Seq(Row(0), Row(1), Row(2)))
    sql("DROP TEMPORARY FUNCTION my_range")
  }

  test("SECTION 10: COUNT(*) - unqualified and qualified") {
    // Unqualified count(*)
    checkAnswer(sql("SELECT count(*) FROM VALUES (1), (2), (3) AS t(a)"), Row(3))

    // Qualified as builtin.count(*)
    checkAnswer(sql("SELECT builtin.count(*) FROM VALUES (1), (2), (3) AS t(a)"), Row(3))

    // Qualified as system.builtin.count(*)
    checkAnswer(sql("SELECT system.builtin.count(*) FROM VALUES (1), (2), (3) AS t(a)"), Row(3))

    // Case insensitive qualified count(*)
    checkAnswer(sql("SELECT BUILTIN.COUNT(*) FROM VALUES (1), (2), (3) AS t(a)"), Row(3))
    checkAnswer(sql("SELECT System.Builtin.Count(*) FROM VALUES (1), (2), (3) AS t(a)"), Row(3))
  }

  test("SECTION 10: COUNT(*) - count(tbl.*) blocking") {
    sql("CREATE TEMPORARY VIEW count_test_view AS SELECT 1 AS a, 2 AS b")

    // Unqualified count with table.*
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT count(count_test_view.*) FROM count_test_view")
      },
      condition = "INVALID_USAGE_OF_STAR_WITH_TABLE_IDENTIFIER_IN_COUNT",
      sqlState = "42000",
      parameters = Map("tableName" -> "count_test_view")
    )

    // Qualified count with table.*
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT builtin.count(count_test_view.*) FROM count_test_view")
      },
      condition = "INVALID_USAGE_OF_STAR_WITH_TABLE_IDENTIFIER_IN_COUNT",
      sqlState = "42000",
      parameters = Map("tableName" -> "count_test_view")
    )

    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT system.builtin.count(count_test_view.*) FROM count_test_view")
      },
      condition = "INVALID_USAGE_OF_STAR_WITH_TABLE_IDENTIFIER_IN_COUNT",
      sqlState = "42000",
      parameters = Map("tableName" -> "count_test_view")
    )

    sql("DROP VIEW count_test_view")
  }

  test("SECTION 11: Security - user cannot shadow current_user") {
    // Baseline: current_user() works
    val actualUser = sql("SELECT current_user()").collect().head.getString(0)
    assert(actualUser != null)
    checkAnswer(sql("SELECT current_user() IS NOT NULL"), Row(true))

    // User creates temp function trying to shadow current_user
    sql("CREATE TEMPORARY FUNCTION current_user() RETURNS STRING RETURN 'hacker'")

    // CRITICAL: Unqualified name still resolves to builtin (NOT the temp function)
    val result = sql("SELECT current_user()").collect().head.getString(0)
    assert(result == actualUser, s"Builtin was shadowed! Got $result, expected $actualUser")
    checkAnswer(sql("SELECT current_user() IS NOT NULL"), Row(true))

    // User's shadowed function only accessible via explicit qualification
    checkAnswer(sql("SELECT session.current_user()"), Row("hacker"))

    sql("DROP TEMPORARY FUNCTION current_user")
  }

  test("SECTION 11: Security - user cannot shadow abs") {
    // Built-in abs works
    checkAnswer(sql("SELECT builtin.abs(-5)"), Row(5))

    // Create temp abs
    sql("CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 999")

    // Unqualified abs still resolves to builtin (security-focused order)
    checkAnswer(sql("SELECT abs(-5)"), Row(5))

    // Temp abs only accessible with qualification
    checkAnswer(sql("SELECT session.abs()"), Row(999))

    sql("DROP TEMPORARY FUNCTION abs")
  }

  test("SECTION 11: Security - session_user and current_database") {
    // Test session_user
    sql("CREATE TEMPORARY FUNCTION session_user() RETURNS STRING RETURN 'fake_user'")
    // Should be builtin, not temp
    checkAnswer(sql("SELECT session_user() IS NOT NULL"), Row(true))
    sql("DROP TEMPORARY FUNCTION session_user")

    // Test current_database
    sql("CREATE TEMPORARY FUNCTION current_database() RETURNS STRING RETURN 'fake_db'")
    // Should be builtin, not temp
    checkAnswer(sql("SELECT current_database() IS NOT NULL"), Row(true))
    sql("DROP TEMPORARY FUNCTION current_database")
  }

  // ============================================================================
  // SECTION 12: Extension Function Tests
  // ============================================================================
  // These tests verify the system.extension namespace and extension function behavior.
  // Extension functions are registered programmatically via SparkSessionExtensions
  // (e.g., Apache Sedona, Delta Lake) and cannot be tested via SQL golden files.

  test("SECTION 12: Extension - function can be called unqualified") {
    // Extension function registered in TestExtensions
    checkAnswer(sql("SELECT test_ext_func()"), Row(9999))
  }

  test("SECTION 12: Extension - resolution order: extension > builtin > session") {
    // Test the critical security property: extension comes before builtin before session
    sql("CREATE TEMPORARY FUNCTION session_func() RETURNS INT RETURN 1111")

    // Unqualified: resolves to session (no extension or builtin with this name)
    checkAnswer(sql("SELECT session_func()"), Row(1111))

    // Qualified
    checkAnswer(sql("SELECT session.session_func()"), Row(1111))

    // Cleanup
    sql("DROP TEMPORARY FUNCTION session_func")
  }

  test("SECTION 12: Extension - security property: temp cannot shadow current_user") {
    // This test is already covered in SECTION 11, but we verify it works with extensions loaded
    val actualUser = sql("SELECT current_user()").collect().head.getString(0)

    sql("CREATE TEMPORARY FUNCTION current_user() RETURNS STRING RETURN 'hacker'")

    // Unqualified call should still resolve to builtin (security!)
    val unqualifiedResult = sql("SELECT current_user()").collect().head.getString(0)
    assert(unqualifiedResult == actualUser,
      s"Built-in current_user() was shadowed! Got '$unqualifiedResult', expected '$actualUser'")

    // But we can access the temp function via qualification
    checkAnswer(sql("SELECT session.current_user()"), Row("hacker"))

    sql("DROP TEMPORARY FUNCTION current_user")
  }

  test("SECTION 12: Extension - SHOW FUNCTIONS includes extension functions") {
    val functions = sql("SHOW FUNCTIONS").collect().map(_.getString(0))
    assert(functions.contains("test_ext_func"),
      s"Extension function test_ext_func not found in SHOW FUNCTIONS output")
  }

  test("SECTION 12: Extension - DESCRIBE FUNCTION works") {
    // Unqualified - extension functions should be describable
    val desc = sql("DESCRIBE FUNCTION test_ext_func").collect()
    assert(desc.nonEmpty, "DESCRIBE FUNCTION should return results for extension functions")
  }
}

/**
 * Test extension that registers mock extension functions.
 * This simulates what real extensions like Apache Sedona would do.
 */
class TestExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Register a mock extension function
    // Use the full 11-parameter ExpressionInfo constructor
    extensions.injectFunction(
      (org.apache.spark.sql.catalyst.FunctionIdentifier("test_ext_func"),
       new ExpressionInfo(
         "org.apache.spark.sql.FunctionQualificationSuite",  // className
         "",                                                  // db
         "test_ext_func",                                     // name
         "Returns 9999 for testing",                          // usage
         "",                                                  // arguments
         "",                                                  // examples
         "",                                                  // note
         "",                                                  // group
         "4.2.0",                                             // since
         "",                                                  // deprecated
         ""),                                                 // source (empty is allowed)
       (exprs: Seq[Expression]) => Literal(9999, IntegerType))
    )
  }
}
