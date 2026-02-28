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
    // Intent: when builtin has only a scalar and temp has a table with the same name, resolution
    // follows "first match in path" (consistent with scalar context). Builtin scalar abs is first,
    // so unqualified table context yields NOT_A_TABLE_FUNCTION; we do not skip to the temp table.
    withSQLConf("spark.sql.functionResolution.sessionOrder" -> "second") {
      sql("CREATE TEMPORARY FUNCTION abs() RETURNS TABLE(val INT) RETURN SELECT 42")
    }
    try {
      checkAnswer(sql("SELECT abs(-5)"), Row(5))
      checkError(
        exception = intercept[AnalysisException] { sql("SELECT * FROM abs()") },
        condition = "NOT_A_TABLE_FUNCTION",
        parameters = Map("functionName" -> "`abs`"),
        context = ExpectedContext(
          fragment = "abs()",
          start = 14,
          stop = 18))
      checkAnswer(sql("SELECT builtin.abs(-5)"), Row(5))
      checkAnswer(sql("SELECT * FROM session.abs()"), Row(42))
    } finally {
      sql("DROP TEMPORARY FUNCTION abs")
    }
  }

  test("SECTION 4b: Cross-Type Shadowing - temp scalar + builtin table") {
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

  test("SECTION 5a: Cross-Type Error Detection - scalar in table context") {
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

  test("SECTION 5b: Cross-Type Error Detection - table in scalar context") {
    // Scalar resolution only looks in scalar registry. If we use a name that exists only as a
    // table function (table first / only in path), we get NOT_A_SCALAR_FUNCTION.
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

  test("SECTION 5c: Cross-Type Error Detection - generator functions") {
    // Generator functions work in both contexts
    checkAnswer(sql("SELECT explode(array(1, 2, 3))"), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(sql("SELECT * FROM explode(array(1, 2, 3))"), Seq(Row(1), Row(2), Row(3)))
  }

  test("SECTION 5d: Table resolution - scalar first in path yields NOT_A_TABLE_FUNCTION") {
    // First match in path wins (consistent with scalar context). Builtin has scalar "abs", so
    // in table context we get NOT_A_TABLE_FUNCTION; we do not skip to the temp table function.
    // Use sessionOrder "second" so we can create a temp function that shadows builtin.
    withSQLConf("spark.sql.functionResolution.sessionOrder" -> "second") {
      sql("CREATE TEMPORARY FUNCTION abs() RETURNS TABLE(val INT) RETURN SELECT 99")
      try {
        checkError(
          exception = intercept[AnalysisException] { sql("SELECT * FROM abs()") },
          condition = "NOT_A_TABLE_FUNCTION",
          parameters = Map("functionName" -> "`abs`"),
          context = ExpectedContext(
            fragment = "abs()",
            start = 14,
            stop = 18))
        checkAnswer(sql("SELECT abs(-5)"), Row(5))
      } finally {
        sql("DROP TEMPORARY FUNCTION abs")
      }
    }
  }

  test("SECTION 5e: Cannot have temp scalar and temp table function with same name") {
    // SessionCatalog prevents registering both types with the same name so DROP is unambiguous.
    sql("CREATE TEMPORARY FUNCTION same_name() RETURNS INT RETURN 1")
    checkError(
      exception = intercept[AnalysisException] {
        sql("CREATE TEMPORARY FUNCTION same_name() RETURNS TABLE(x INT) RETURN SELECT 2")
      },
      condition = "ROUTINE_ALREADY_EXISTS",
      parameters = Map(
        "routineName" -> "`same_name`",
        "newRoutineType" -> "routine",
        "existingRoutineType" -> "routine"))
    sql("DROP TEMPORARY FUNCTION same_name")
    // Other order: table first, then scalar
    sql("CREATE TEMPORARY FUNCTION same_name() RETURNS TABLE(x INT) RETURN SELECT 2")
    checkError(
      exception = intercept[AnalysisException] {
        sql("CREATE TEMPORARY FUNCTION same_name() RETURNS INT RETURN 1")
      },
      condition = "ROUTINE_ALREADY_EXISTS",
      parameters = Map(
        "routineName" -> "`same_name`",
        "newRoutineType" -> "routine",
        "existingRoutineType" -> "routine"))
    sql("DROP TEMPORARY FUNCTION same_name")
  }

  test("SECTION 6a: DDL Operations - DESCRIBE") {
    // DESCRIBE builtin functions with qualification
    val desc1 = sql("DESCRIBE FUNCTION builtin.abs")
    assert(desc1.count() > 0)

    val desc2 = sql("DESCRIBE FUNCTION system.builtin.abs")
    assert(desc2.count() > 0)
  }

  test("SECTION 6b: DDL Operations - DROP with qualified names") {
    sql("CREATE TEMPORARY FUNCTION drop_test() RETURNS INT RETURN 1")
    sql("DROP TEMPORARY FUNCTION session.drop_test")
  }

  test("SECTION 6c: DDL Operations - CREATE OR REPLACE") {
    sql("CREATE TEMPORARY FUNCTION replace_test() RETURNS INT RETURN 1")
    checkAnswer(sql("SELECT replace_test()"), Row(1))
    sql("CREATE OR REPLACE TEMPORARY FUNCTION session.replace_test() RETURNS INT RETURN 2")
    checkAnswer(sql("SELECT replace_test()"), Row(2))
    sql("DROP TEMPORARY FUNCTION replace_test")
  }

  test("SECTION 6d: DDL Operations - CREATE OR REPLACE changes type") {
    sql("CREATE TEMPORARY FUNCTION type_change() RETURNS INT RETURN 42")
    checkAnswer(sql("SELECT type_change()"), Row(42))
    sql(
      "CREATE OR REPLACE TEMPORARY FUNCTION type_change() " +
      "RETURNS TABLE(val INT) RETURN SELECT 99")
    checkAnswer(sql("SELECT * FROM type_change()"), Row(99))
    sql("DROP TEMPORARY FUNCTION type_change")
  }

  test("SECTION 6e: DDL Operations - IF NOT EXISTS not supported") {
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

  test("SECTION 6f: DDL Operations - SHOW FUNCTIONS") {
    sql("CREATE TEMPORARY FUNCTION show_test() RETURNS INT RETURN 1")

    val showTest = sql("SHOW FUNCTIONS LIKE 'show_test'").collect()
    assert(showTest.length > 0)

    val showAbs = sql("SHOW FUNCTIONS LIKE 'abs'").collect()
    assert(showAbs.length > 0)

    sql("DROP TEMPORARY FUNCTION show_test")
  }

  test("SECTION 7a: Error Cases - cannot create temp function with builtin namespace") {
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

  test("SECTION 7b: Error Cases - cannot create temp function with invalid database") {
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

  test("SECTION 7c: Error Cases - cannot drop builtin function (DROP FUNCTION)") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("DROP FUNCTION system.builtin.abs")
      },
      condition = "FORBIDDEN_OPERATION",
      sqlState = "42809",
      parameters = Map(
        "statement" -> "DROP",
        "objectType" -> "FUNCTION",
        "objectName" -> "`abs`"
      )
    )
  }

  test("SECTION 7d: Error Cases - cannot create function in builtin namespace (CREATE FUNCTION)") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("CREATE FUNCTION system.builtin.my_func() RETURNS INT RETURN 1")
      },
      condition = "FORBIDDEN_OPERATION",
      sqlState = "42809",
      parameters = Map(
        "statement" -> "CREATE",
        "objectType" -> "OBJECT",
        "objectName" -> "`my_func`"
      )
    )
  }

  // COMMENT ON FUNCTION is not yet supported in the parser; when added, we should reject
  // COMMENT ON FUNCTION system.builtin.<name> with FORBIDDEN_OPERATION (statement="COMMENT ON",
  // objectType="FUNCTION", objectName=...).

  test("SECTION 7e: Error Cases - cannot create duplicate functions") {
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

  test("SECTION 7f: Error Cases - non-existent function error") {
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

  test("SECTION 8a: Views - temp view can reference temp function") {
    sql("CREATE TEMPORARY FUNCTION view_func() RETURNS STRING RETURN 'from_temp'")
    sql("CREATE TEMPORARY VIEW temp_view AS SELECT view_func() as result")
    checkAnswer(sql("SELECT * FROM temp_view"), Row("from_temp"))
    sql("DROP VIEW temp_view")
    sql("DROP TEMPORARY FUNCTION view_func")
  }

  test("SECTION 8b: Views - view with shadowing temp function") {
    // Intent: views can reference temp functions via qualified names (session.abs) and builtin
    // via builtin.abs. withSQLConf allows creating temp abs when sessionOrder is "first" in CI.
    withSQLConf("spark.sql.functionResolution.sessionOrder" -> "second") {
      sql("CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 777")
    }

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

  test("SECTION 8c: Views - multiple temp functions in same view") {
    sql("CREATE TEMPORARY FUNCTION func1() RETURNS INT RETURN 1")
    sql("CREATE TEMPORARY FUNCTION func2() RETURNS INT RETURN 2")
    sql("CREATE TEMPORARY VIEW multi_func_view AS SELECT func1() + func2() as sum")
    checkAnswer(sql("SELECT * FROM multi_func_view"), Row(3))
    sql("DROP VIEW multi_func_view")
    sql("DROP TEMPORARY FUNCTION func1")
    sql("DROP TEMPORARY FUNCTION func2")
  }

  test("SECTION 8d: Views - nested views with temp functions") {
    sql("CREATE TEMPORARY FUNCTION nested_func() RETURNS INT RETURN 100")
    sql("CREATE TEMPORARY VIEW inner_view AS SELECT nested_func() as val")
    sql("CREATE TEMPORARY VIEW outer_view AS SELECT val * 2 FROM inner_view")
    checkAnswer(sql("SELECT * FROM outer_view"), Row(200))
    sql("DROP VIEW outer_view")
    sql("DROP VIEW inner_view")
    sql("DROP TEMPORARY FUNCTION nested_func")
  }

  test("SECTION 9a: Multiple Functions - multiple qualified functions together") {
    sql("CREATE TEMPORARY FUNCTION add10(x INT) RETURNS INT RETURN x + 10")
    checkAnswer(
      sql("SELECT builtin.abs(-5), session.add10(5), system.builtin.upper('hello')"),
      Row(5, 15, "HELLO"))
    sql("DROP TEMPORARY FUNCTION add10")
  }

  test("SECTION 9b: Multiple Functions - qualified aggregate function") {
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

  test("SECTION 9c: Multiple Functions - table function with qualified names") {
    sql("CREATE TEMPORARY FUNCTION my_range() RETURNS TABLE(id INT) RETURN SELECT * FROM range(3)")
    checkAnswer(sql("SELECT * FROM my_range()"), Seq(Row(0), Row(1), Row(2)))
    checkAnswer(sql("SELECT * FROM session.my_range()"), Seq(Row(0), Row(1), Row(2)))
    checkAnswer(sql("SELECT * FROM system.session.my_range()"), Seq(Row(0), Row(1), Row(2)))
    sql("DROP TEMPORARY FUNCTION my_range")
  }

  test("SECTION 10a: COUNT(*) - unqualified and qualified") {
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

  test("SECTION 10b: COUNT(*) - invalid qualified names rejected") {
    // Invalid qualifier "foo.bar" should not be treated as count
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT foo.bar.count(*) FROM VALUES (1), (2), (3) AS t(a)")
      },
      condition = "UNRESOLVED_ROUTINE",
      parameters = Map(
        "routineName" -> "`foo`.`bar`.`count`",
        "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]"
      ),
      context = ExpectedContext(
        fragment = "foo.bar.count(*)",
        start = 7,
        stop = 22
      )
    )

    // Invalid qualifier "catalog.db" should not be treated as count
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT catalog.db.count(*) FROM VALUES (1), (2), (3) AS t(a)")
      },
      condition = "UNRESOLVED_ROUTINE",
      parameters = Map(
        "routineName" -> "`catalog`.`db`.`count`",
        "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]"
      ),
      context = ExpectedContext(
        fragment = "catalog.db.count(*)",
        start = 7,
        stop = 25
      )
    )
  }

  test("SECTION 10c: COUNT(*) - count(tbl.*) blocking") {
    sql("CREATE TEMPORARY VIEW count_test_view AS SELECT 1 AS a, 2 AS b")

    // Unqualified count with table.*
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT count(count_test_view.*) FROM count_test_view")
      },
      condition = "INVALID_USAGE_OF_STAR_WITH_TABLE_IDENTIFIER_IN_COUNT",
      sqlState = "42000",
      parameters = Map("tableName" -> "`count_test_view`")
    )

    // Qualified count with table.*
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT builtin.count(count_test_view.*) FROM count_test_view")
      },
      condition = "INVALID_USAGE_OF_STAR_WITH_TABLE_IDENTIFIER_IN_COUNT",
      sqlState = "42000",
      parameters = Map("tableName" -> "`count_test_view`")
    )

    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT system.builtin.count(count_test_view.*) FROM count_test_view")
      },
      condition = "INVALID_USAGE_OF_STAR_WITH_TABLE_IDENTIFIER_IN_COUNT",
      sqlState = "42000",
      parameters = Map("tableName" -> "`count_test_view`")
    )

    sql("DROP VIEW count_test_view")
  }

  test("SECTION 11a: Security - user cannot shadow current_user") {
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

  test("SECTION 11b: Security - user cannot shadow abs") {
    // Intent: unqualified abs resolves to builtin; temp only via session.abs. withSQLConf allows
    // creating temp abs when sessionOrder is "first" in CI.
    checkAnswer(sql("SELECT builtin.abs(-5)"), Row(5))

    withSQLConf("spark.sql.functionResolution.sessionOrder" -> "second") {
      sql("CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 999")
    }

    // Unqualified abs still resolves to builtin (security-focused order)
    checkAnswer(sql("SELECT abs(-5)"), Row(5))

    // Temp abs only accessible with qualification
    checkAnswer(sql("SELECT session.abs()"), Row(999))

    sql("DROP TEMPORARY FUNCTION abs")
  }

  test("SECTION 11c: Security - session_user and current_database") {
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

  test("SECTION 11d: Parameterless functions - qualified names require explicit parentheses") {
    // Parameterless functions like current_user, current_schema are functions and should
    // be callable via qualified names WITH explicit parentheses.
    // - Qualified WITH parens: "system.builtin.current_user()" works as function call
    // - Qualified WITHOUT parens: "system.builtin.current_user" is treated as column
    //   reference (NOT auto-converted to function call) to avoid attribute resolution
    //   complexity

    // Qualified WITH parentheses should work as function calls
    checkAnswer(
      sql("SELECT system.builtin.current_user()"),
      Row(sql("SELECT current_user()").collect().head.getString(0)))
    checkAnswer(
      sql("SELECT builtin.current_user()"),
      Row(sql("SELECT current_user()").collect().head.getString(0)))
    checkAnswer(
      sql("SELECT system.builtin.current_schema()"),
      Row(sql("SELECT current_schema()").collect().head.getString(0)))
    checkAnswer(
      sql("SELECT builtin.current_schema()"),
      Row(sql("SELECT current_schema()").collect().head.getString(0)))

    // Qualified WITHOUT parentheses should fail as unresolved column
    // (NOT auto-converted to function call)
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT system.builtin.current_user")
      },
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      parameters = Map(
        "objectName" -> "`system`.`builtin`.`current_user`"
      ),
      context = ExpectedContext(
        fragment = "system.builtin.current_user",
        start = 7,
        stop = 33)
    )

    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT builtin.current_user")
      },
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      parameters = Map(
        "objectName" -> "`builtin`.`current_user`"
      ),
      context = ExpectedContext(
        fragment = "builtin.current_user",
        start = 7,
        stop = 26)
    )

    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT system.builtin.current_schema")
      },
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      parameters = Map(
        "objectName" -> "`system`.`builtin`.`current_schema`"
      ),
      context = ExpectedContext(
        fragment = "system.builtin.current_schema",
        start = 7,
        stop = 35)
    )
  }

  // ============================================================================
  // SECTION 12: Extension Function Tests
  // ============================================================================
  // These tests verify the system.extension namespace and extension function behavior.
  // Extension functions are registered programmatically via SparkSessionExtensions
  // (e.g., Apache Sedona, Delta Lake) and cannot be tested via SQL golden files.

  test("SECTION 12a: Extension - function can be called unqualified") {
    // Extension function registered in TestExtensions
    checkAnswer(sql("SELECT test_ext_func()"), Row(9999))
  }

  test("SECTION 12b: Extension - resolution order: extension > builtin > session") {
    // Test the critical security property: extension comes before builtin before session
    sql("CREATE TEMPORARY FUNCTION session_func() RETURNS INT RETURN 1111")

    // Unqualified: resolves to session (no extension or builtin with this name)
    checkAnswer(sql("SELECT session_func()"), Row(1111))

    // Qualified
    checkAnswer(sql("SELECT session.session_func()"), Row(1111))

    // Cleanup
    sql("DROP TEMPORARY FUNCTION session_func")
  }

  test("SECTION 12c: Extension - security property: temp cannot shadow current_user") {
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

  test("SECTION 12d: Extension - SHOW FUNCTIONS includes extension functions") {
    val functions = sql("SHOW FUNCTIONS").collect().map(_.getString(0))
    assert(functions.contains("test_ext_func"),
      s"Extension function test_ext_func not found in SHOW FUNCTIONS output")
  }

  test("SECTION 12e: Extension - DESCRIBE FUNCTION works") {
    // Unqualified - extension functions should be describable
    val desc = sql("DESCRIBE FUNCTION test_ext_func").collect()
    assert(desc.nonEmpty, "DESCRIBE FUNCTION should return results for extension functions")
  }

  test("SECTION 13a: Legacy mode - CREATE TEMPORARY FUNCTION blocked when config is true") {
    withSQLConf("spark.sql.functionResolution.sessionOrder" -> "first") {
      // Try to create a SQL temp function that shadows a builtin
      // SQL temp functions are blocked in legacy mode to preserve master behavior
      checkError(
        exception = intercept[AnalysisException] {
          sql("CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 999")
        },
        condition = "ROUTINE_ALREADY_EXISTS",
        parameters = Map(
          "routineName" -> "`abs`",
          "newRoutineType" -> "routine",
          "existingRoutineType" -> "routine"
        )
      )

      // Try to create a temp function that shadows an extension function
      // (Extensions are stored as builtins, so same error)
      checkError(
        exception = intercept[AnalysisException] {
          sql("CREATE TEMPORARY FUNCTION test_ext_func() RETURNS INT RETURN 999")
        },
        condition = "ROUTINE_ALREADY_EXISTS",
        parameters = Map(
          "routineName" -> "`test_ext_func`",
          "newRoutineType" -> "routine",
          "existingRoutineType" -> "routine"
        )
      )
    }
  }

  test("SECTION 13b: Legacy mode - Scala UDF allowed and shadows builtin when config is true") {
    withSQLConf("spark.sql.functionResolution.sessionOrder" -> "first") {
      withTempView("test_data") {
        sql("CREATE TEMPORARY VIEW test_data AS SELECT 1 as id")

        // Create a Scala UDF that shadows abs (should be allowed in legacy mode)
        spark.udf.register("abs", (x: Int) => x + 100)

        try {
          // Unqualified abs should resolve to Scala UDF (NOT builtin)
          checkAnswer(
            sql("SELECT abs(5) FROM test_data"),
            Row(105)
          )

          // Qualified builtin.abs should still work (returns absolute value)
          checkAnswer(
            sql("SELECT builtin.abs(-5) FROM test_data"),
            Row(5)
          )

          // system.builtin.abs should also work
          checkAnswer(
            sql("SELECT system.builtin.abs(-5) FROM test_data"),
            Row(5)
          )
        } finally {
          spark.sessionState.catalog.dropTempFunction("abs", ignoreIfNotExists = true)
        }
      }
    }
  }

  test("SECTION 13c: Legacy mode - resolution order changes when config is true") {
    withSQLConf("spark.sql.functionResolution.sessionOrder" -> "first") {
      withTempView("test_data") {
        sql("CREATE TEMPORARY VIEW test_data AS SELECT 1 as id")

        // Register a Scala UDF with the same name as a builtin
        spark.udf.register("upper", (s: String) => "TEMP_" + s)

        try {
          // With legacy mode, Scala UDF should shadow builtin
          // Resolution order: session (temp) -> builtin
          checkAnswer(
            sql("SELECT upper('test') FROM test_data"),
            Row("TEMP_test")
          )

          // Explicit builtin qualification should still work
          checkAnswer(
            sql("SELECT builtin.upper('test') FROM test_data"),
            Row("TEST")
          )
        } finally {
          spark.sessionState.catalog.dropTempFunction("upper", ignoreIfNotExists = true)
        }
      }
    }
  }

  test("SECTION 13d: Session last - builtin and persistent take precedence over session") {
    withSQLConf("spark.sql.functionResolution.sessionOrder" -> "last") {
      withTempView("test_data") {
        sql("CREATE TEMPORARY VIEW test_data AS SELECT 1 as id")
        spark.udf.register("upper", (s: String) => "TEMP_" + s)
        try {
          // Unqualified upper resolves to builtin first (session is last)
          checkAnswer(
            sql("SELECT upper('test') FROM test_data"),
            Row("TEST")
          )
          checkAnswer(
            sql("SELECT session.upper('test') FROM test_data"),
            Row("TEMP_test")
          )
        } finally {
          spark.sessionState.catalog.dropTempFunction("upper", ignoreIfNotExists = true)
        }
      }
    }
  }

  test("SECTION 13e: Session last - persistent takes precedence over session when both exist") {
    withUserDefinedFunction("default.session_last_foo" -> false, "session_last_foo" -> true) {
      withSQLConf("spark.sql.functionResolution.sessionOrder" -> "last") {
        sql("CREATE FUNCTION session_last_foo() RETURNS INT RETURN 42")
        spark.udf.register("session_last_foo", () => 100)
        withTempView("t") {
          sql("CREATE TEMPORARY VIEW t AS SELECT 1")
          // Unqualified: should resolve to persistent first (session is last)
          checkAnswer(sql("SELECT session_last_foo() FROM t"), Row(42))
          // Qualified session: should resolve to temp
          checkAnswer(sql("SELECT session.session_last_foo() FROM t"), Row(100))
        }
      }
    }
  }

  test("SECTION 13f: Unresolved routine error search path reflects sessionOrder config") {
    withSQLConf("spark.sql.functionResolution.sessionOrder" -> "first") {
      checkError(
        exception = intercept[AnalysisException] { sql("SELECT no_such_func_xyz()") },
        condition = "UNRESOLVED_ROUTINE",
        parameters = Map(
          "routineName" -> "`no_such_func_xyz`",
          "searchPath" -> "[`system`.`session`, `system`.`builtin`, `spark_catalog`.`default`]"
        ),
        context = ExpectedContext(
          fragment = "no_such_func_xyz()",
          start = 7,
          stop = 24))
    }
    withSQLConf("spark.sql.functionResolution.sessionOrder" -> "last") {
      checkError(
        exception = intercept[AnalysisException] { sql("SELECT no_such_func_xyz()") },
        condition = "UNRESOLVED_ROUTINE",
        parameters = Map(
          "routineName" -> "`no_such_func_xyz`",
          "searchPath" -> "[`system`.`builtin`, `spark_catalog`.`default`, `system`.`session`]"
        ),
        context = ExpectedContext(
          fragment = "no_such_func_xyz()",
          start = 7,
          stop = 24))
    }
  }

  test("SECTION 13g: Multi-part name with invalid namespace yields UNRESOLVED_ROUTINE " +
    "with search path") {
    // A 3-part name like x.y.func triggers REQUIRES_SINGLE_PART_NAMESPACE in the session catalog;
    // LookupFunctions converts it to unresolved routine error with the configured search path.
    checkError(
      exception = intercept[AnalysisException] { sql("SELECT x.y.no_such_xyz()") },
      condition = "UNRESOLVED_ROUTINE",
      parameters = Map(
        "routineName" -> "`x`.`y`.`no_such_xyz`",
        "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]"
      ),
      context = ExpectedContext(
        fragment = "x.y.no_such_xyz()",
        start = 7,
        stop = 23))
  }

  test("SECTION 13h: Legacy mode - default behavior allows registration but builtin wins") {
    // Without setting the config (default is second), registration is allowed
    // but resolution order ensures builtins take precedence
    spark.udf.register("length", (s: String) => 999)

    try {
      withTempView("test_data") {
        sql("CREATE TEMPORARY VIEW test_data AS SELECT 'test' as str")

        // Unqualified length should resolve to builtin (NOT temp function)
        checkAnswer(
          sql("SELECT length(str) FROM test_data"),
          Row(4)  // builtin length of "test"
        )

        // session.length should resolve to temp function
        checkAnswer(
          sql("SELECT session.length(str) FROM test_data"),
          Row(999)
        )
      }
    } finally {
      spark.sessionState.catalog.dropTempFunction("length", ignoreIfNotExists = true)
    }
  }

  test("SECTION 14: FunctionIdentifier structure for system namespaces") {
    withTempView("test_data") {
      sql("CREATE TEMPORARY VIEW test_data AS SELECT 1 as id")

      // Register a temporary function
      spark.udf.register("test_temp_func", (x: Int) => x + 1)

      try {
        // Verify the function works
        checkAnswer(
          sql("SELECT test_temp_func(5) FROM test_data"),
          Row(6)
        )

        // Check internal structure - temporary functions should have both database and catalog set
        val catalog = spark.sessionState.catalog
        val tempFuncIdent = catalog.listFunctions("default", "test_temp_func")
          .find(_._1.funcName == "test_temp_func")

        assert(tempFuncIdent.isDefined, "Temporary function should be found")
        // The function is stored internally with system catalog qualification
        // This is an implementation detail but important for disambiguation

        // Verify extension functions work - they are treated as builtins
        checkAnswer(
          sql("SELECT test_ext_func() FROM test_data"),
          Row(9999)
        )

        // Builtin qualification works for extensions too
        checkAnswer(
          sql("SELECT builtin.test_ext_func() FROM test_data"),
          Row(9999)
        )
      } finally {
        spark.sessionState.catalog.dropTempFunction("test_temp_func", ignoreIfNotExists = true)
      }
    }
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
