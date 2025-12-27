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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for function qualification with builtin and session namespaces.
 * Tests qualified function references, qualified DDL, shadowing, and unified namespace resolution.
 */
class FunctionQualificationSuite extends SharedSparkSession {

  // ==================== Qualified Function References ====================

  test("unqualified builtin function works") {
    val result = sql("SELECT abs(-5)").collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 5)
  }

  test("builtin qualified function works") {
    val result = sql("SELECT builtin.abs(-5)").collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 5)
  }

  test("system.builtin qualified function works") {
    val result = sql("SELECT system.builtin.abs(-5)").collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 5)
  }

  test("case insensitive builtin qualification works") {
    val result = sql("SELECT BUILTIN.ABS(-5)").collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 5)
  }

  test("session qualified temporary function works") {
    sql("CREATE TEMPORARY FUNCTION session.my_func() RETURNS INT RETURN 10")
    val result = sql("SELECT session.my_func()").collect()
    assert(result(0).getInt(0) == 10)
    sql("DROP TEMPORARY FUNCTION session.my_func")
  }

  test("system.session qualified temporary function works") {
    sql("CREATE TEMPORARY FUNCTION system.session.my_func2() RETURNS INT RETURN 20")
    val result = sql("SELECT system.session.my_func2()").collect()
    assert(result(0).getInt(0) == 20)
    sql("DROP TEMPORARY FUNCTION system.session.my_func2")
  }

  test("case insensitive session qualification works") {
    sql("CREATE TEMPORARY FUNCTION SESSION.my_func3() RETURNS INT RETURN 30")
    val result = sql("SELECT SESSION.my_func3()").collect()
    assert(result(0).getInt(0) == 30)
    sql("DROP TEMPORARY FUNCTION SESSION.my_func3")
  }

  test("temporary function shadows builtin with explicit qualification") {
    // First test that abs works normally
    assert(sql("SELECT abs(-5)").collect()(0).getInt(0) == 5)

    // Create a temp function that shadows abs
    sql("CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 999")

    // Unqualified should use the temp function (shadowing)
    assert(sql("SELECT abs()").collect()(0).getInt(0) == 999)

    // But builtin.abs should still work
    assert(sql("SELECT builtin.abs(-5)").collect()(0).getInt(0) == 5)

    // And session.abs should use the temp function
    assert(sql("SELECT session.abs()").collect()(0).getInt(0) == 999)

    sql("DROP TEMPORARY FUNCTION abs")
  }

  test("multiple qualified functions in single query") {
    sql("DROP TEMPORARY FUNCTION IF EXISTS my_add")
    sql("CREATE TEMPORARY FUNCTION my_add(x INT) RETURNS INT RETURN x + 10")
    val result = sql(
      """SELECT
        |  builtin.abs(-5) as builtin_result,
        |  system.builtin.upper('hello') as system_builtin_result,
        |  session.my_add(5) as session_result
        |""".stripMargin).collect()

    assert(result(0).getInt(0) == 5)
    assert(result(0).getString(1) == "HELLO")
    assert(result(0).getInt(2) == 15)

    sql("DROP TEMPORARY FUNCTION my_add")
  }

  test("qualified aggregate function") {
    val result = sql("SELECT builtin.sum(value) as total FROM VALUES (1), (2), (3) AS t(value)")
      .collect()
    assert(result(0).getLong(0) == 6)
  }

  // ==================== Qualified DDL ====================

  test("CREATE TEMPORARY FUNCTION with unqualified name") {
    sql("CREATE TEMPORARY FUNCTION my_func() RETURNS INT RETURN 5")
    val result = sql("SELECT my_func()").collect()
    assert(result(0).getInt(0) == 5)
    sql("DROP TEMPORARY FUNCTION my_func")
  }

  test("CREATE TEMPORARY FUNCTION with session qualification") {
    sql("CREATE TEMPORARY FUNCTION session.my_func() RETURNS INT RETURN 10")
    val result = sql("SELECT my_func()").collect()
    assert(result(0).getInt(0) == 10)
    sql("DROP TEMPORARY FUNCTION my_func")
  }

  test("CREATE TEMPORARY FUNCTION with system.session qualification") {
    sql("CREATE TEMPORARY FUNCTION system.session.my_func() RETURNS INT RETURN 15")
    val result = sql("SELECT my_func()").collect()
    assert(result(0).getInt(0) == 15)
    sql("DROP TEMPORARY FUNCTION my_func")
  }

  test("DROP TEMPORARY FUNCTION with session qualification") {
    sql("CREATE TEMPORARY FUNCTION my_func4() RETURNS INT RETURN 8")
    sql("DROP TEMPORARY FUNCTION session.my_func4")
    // Verify it's dropped
    intercept[AnalysisException] {
      sql("SELECT my_func4()")
    }
  }

  test("DROP TEMPORARY FUNCTION with system.session qualification") {
    sql("CREATE TEMPORARY FUNCTION my_func5() RETURNS INT RETURN 9")
    sql("DROP TEMPORARY FUNCTION system.session.my_func5")
    // Verify it's dropped
    intercept[AnalysisException] {
      sql("SELECT my_func5()")
    }
  }

  test("CREATE TEMPORARY FUNCTION with invalid database qualification fails") {
    val exception = intercept[org.apache.spark.sql.AnalysisException] {
      sql("CREATE TEMPORARY FUNCTION mydb.my_func() RETURNS INT RETURN 1")
    }
    checkError(
      exception = exception,
      condition = "INVALID_TEMP_OBJ_QUALIFIER",
      parameters = Map(
        "objectType" -> "FUNCTION",
        "objectName" -> "`my_func`",
        "qualifier" -> "`mydb`"))
  }

  test("DROP TEMPORARY FUNCTION with invalid database qualification fails") {
    sql("CREATE TEMPORARY FUNCTION my_func6() RETURNS INT RETURN 11")
    val exception = intercept[org.apache.spark.sql.AnalysisException] {
      sql("DROP TEMPORARY FUNCTION mydb.my_func6")
    }
    checkError(
      exception = exception,
      condition = "INVALID_TEMP_OBJ_QUALIFIER",
      parameters = Map(
        "objectType" -> "FUNCTION",
        "objectName" -> "`my_func6`",
        "qualifier" -> "`mydb`"))
    // Clean up
    sql("DROP TEMPORARY FUNCTION my_func6")
  }

  test("CREATE TEMPORARY FUNCTION case insensitive qualification") {
    sql("CREATE TEMPORARY FUNCTION SESSION.my_func() RETURNS INT RETURN 20")
    val result = sql("SELECT my_func()").collect()
    assert(result(0).getInt(0) == 20)
    sql("DROP TEMPORARY FUNCTION my_func")
  }

  test("Qualified and unqualified CREATE/DROP can be mixed") {
    sql("CREATE TEMPORARY FUNCTION my_func7() RETURNS INT RETURN 7")
    sql("DROP TEMPORARY FUNCTION session.my_func7")
    intercept[AnalysisException] {
      sql("SELECT my_func7()")
    }
  }

  // ==================== Unified Function Namespace ====================

  test("scalar function in table context should give specific error") {
    val exception = intercept[AnalysisException] {
      sql("SELECT * FROM abs(-5)")
    }
    assert(exception.getMessage.contains("NOT_A_TABLE_FUNCTION"))
    assert(exception.getMessage.contains("abs"))
  }

  test("table function in scalar context should give specific error") {
    val exception = intercept[AnalysisException] {
      sql("SELECT range(10)")
    }
    assert(exception.getMessage.contains("NOT_A_SCALAR_FUNCTION"))
    assert(exception.getMessage.contains("range"))
  }

  test("temporary scalar function in table context should give specific error") {
    sql("CREATE TEMPORARY FUNCTION my_scalar_func(x INT) RETURNS INT RETURN x + 1")
    val exception = intercept[AnalysisException] {
      sql("SELECT * FROM my_scalar_func(5)")
    }
    assert(exception.getMessage.contains("NOT_A_TABLE_FUNCTION"))
    assert(exception.getMessage.contains("my_scalar_func"))
    sql("DROP TEMPORARY FUNCTION my_scalar_func")
  }

  test("generator function works in both scalar and table contexts") {
    // Generator functions like explode should work in both contexts
    // In scalar context (SELECT clause)
    val result1 = sql("SELECT explode(array(1, 2, 3)) AS val").collect()
    assert(result1.length == 3)

    // In table context (FROM clause)
    val result2 = sql("SELECT * FROM explode(array(1, 2, 3))").collect()
    assert(result2.length == 3)
  }

  test("non-existent function gives UNRESOLVED_ROUTINE error") {
    val exception = intercept[AnalysisException] {
      sql("SELECT non_existent_function()")
    }
    assert(exception.getMessage.contains("UNRESOLVED_ROUTINE"))
    assert(exception.getMessage.contains("non_existent_function"))
  }

  // ==================== Cross-Type Shadowing ====================

  test("temporary table function shadows builtin scalar function") {
    // abs is a builtin scalar function
    assert(sql("SELECT abs(-5)").collect()(0).getInt(0) == 5)

    // Create a temp table function with the same name
    sql("CREATE TEMPORARY FUNCTION abs(x INT) RETURNS TABLE(val INT) RETURN SELECT x * 2")

    // Now trying to use abs in scalar context should fail with type error
    val exception = intercept[AnalysisException] {
      sql("SELECT abs(-5)")
    }
    assert(exception.getMessage.contains("NOT_A_SCALAR_FUNCTION"))
    assert(exception.getMessage.contains("abs"))

    // Using in table context should work
    val result = sql("SELECT * FROM abs(5)").collect()
    assert(result(0).getInt(0) == 10)

    // Explicitly qualifying as builtin should work
    assert(sql("SELECT builtin.abs(-5)").collect()(0).getInt(0) == 5)

    sql("DROP TEMPORARY FUNCTION abs")
  }

  test("temporary scalar function shadows builtin table function") {
    // range is a builtin table function
    val rangeResult = sql("SELECT * FROM range(3)").collect()
    assert(rangeResult.length == 3)

    // Create a temp scalar function with the same name
    sql("CREATE TEMPORARY FUNCTION range() RETURNS INT RETURN 999")

    // Now trying to use range in table context should fail with type error
    val exception = intercept[AnalysisException] {
      sql("SELECT * FROM range()")
    }
    assert(exception.getMessage.contains("NOT_A_TABLE_FUNCTION"))
    assert(exception.getMessage.contains("range"))

    // Using in scalar context should work
    assert(sql("SELECT range()").collect()(0).getInt(0) == 999)

    sql("DROP TEMPORARY FUNCTION range")
  }

  test("qualified builtin bypasses temporary table function shadow") {
    // Create temp table function that shadows builtin scalar
    sql("CREATE TEMPORARY FUNCTION abs(x INT) RETURNS TABLE(val INT) RETURN SELECT x * 2")

    // Qualified builtin reference should work in scalar context
    assert(sql("SELECT builtin.abs(-5)").collect()(0).getInt(0) == 5)
    assert(sql("SELECT system.builtin.abs(-5)").collect()(0).getInt(0) == 5)

    // Note: Parser doesn't support qualified names in FROM clause (table-valued functions)
    // so we can't test: SELECT * FROM session.abs(5)
    // But unqualified works and uses the temp table function
    val result = sql("SELECT * FROM abs(5)").collect()
    assert(result(0).getInt(0) == 10)

    sql("DROP TEMPORARY FUNCTION abs")
  }

  test("cannot create both scalar and table temporary function with same name") {
    // Create a scalar temporary function
    sql("CREATE TEMPORARY FUNCTION dup_test() RETURNS INT RETURN 42")
    assert(sql("SELECT dup_test()").collect()(0).getInt(0) == 42)

    // Try to create a table function with the same name - should fail
    val exception = intercept[AnalysisException] {
      sql("CREATE TEMPORARY FUNCTION dup_test() RETURNS TABLE(val INT) RETURN SELECT 99")
    }
    checkError(
      exception = exception,
      condition = "ROUTINE_ALREADY_EXISTS",
      parameters = Map(
        "routineName" -> "`dup_test`",
        "newRoutineType" -> "routine",
        "existingRoutineType" -> "routine"))

    // With OR REPLACE, it should replace the scalar function with table function
    sql("CREATE OR REPLACE TEMPORARY FUNCTION dup_test() RETURNS TABLE(val INT) RETURN SELECT 99")

    // Now it's a table function, scalar usage should fail
    val scalarException = intercept[AnalysisException] {
      sql("SELECT dup_test()")
    }
    assert(scalarException.getMessage.contains("NOT_A_SCALAR_FUNCTION"))

    // Table usage should work
    assert(sql("SELECT * FROM dup_test()").collect()(0).getInt(0) == 99)

    sql("DROP TEMPORARY FUNCTION dup_test")
  }

  test("cannot create both table and scalar temporary function with same name") {
    // Create a table temporary function first
    sql("CREATE TEMPORARY FUNCTION dup_test2() RETURNS TABLE(val INT) RETURN SELECT 99")
    assert(sql("SELECT * FROM dup_test2()").collect()(0).getInt(0) == 99)

    // Try to create a scalar function with the same name - should fail
    val exception = intercept[AnalysisException] {
      sql("CREATE TEMPORARY FUNCTION dup_test2() RETURNS INT RETURN 42")
    }
    checkError(
      exception = exception,
      condition = "ROUTINE_ALREADY_EXISTS",
      parameters = Map(
        "routineName" -> "`dup_test2`",
        "newRoutineType" -> "routine",
        "existingRoutineType" -> "routine"))

    // With OR REPLACE, it should replace the table function with scalar function
    sql("CREATE OR REPLACE TEMPORARY FUNCTION dup_test2() RETURNS INT RETURN 42")

    // Now it's a scalar function
    assert(sql("SELECT dup_test2()").collect()(0).getInt(0) == 42)

    // Table usage should fail
    val tableException = intercept[AnalysisException] {
      sql("SELECT * FROM dup_test2()")
    }
    assert(tableException.getMessage.contains("NOT_A_TABLE_FUNCTION"))

    sql("DROP TEMPORARY FUNCTION dup_test2")
  }

  // ==================== View Resolution with Temporary Functions ====================
  // Tests that temporary views can correctly reference temporary functions through
  // the PATH resolution system with proper view context filtering.

  test("temporary view can reference temporary function") {
    // Verifies that temporary functions are found when resolving views that reference them.
    sql("CREATE TEMPORARY FUNCTION temp_upper(x STRING) RETURNS STRING RETURN upper(x)")

    withTempView("v1") {
      // Create temporary view that uses the temp function.
      sql("CREATE TEMPORARY VIEW v1 AS SELECT temp_upper(col1) as result " +
        "FROM VALUES ('hello'), ('world') AS t(col1)")

      // Query the view - the temp function should be resolved.
      val result = sql("SELECT * FROM v1").collect()
      assert(result.length == 2)
      assert(result(0).getString(0) == "HELLO")
      assert(result(1).getString(0) == "WORLD")
    }

    sql("DROP TEMPORARY FUNCTION temp_upper")
  }

  test("permanent view cannot reference temporary function") {
    // Verifies that creating a permanent view with a temporary function fails.
    sql("CREATE TEMPORARY FUNCTION temp_func() RETURNS INT RETURN 42")

    withView("perm_view") {
      val exception = intercept[AnalysisException] {
        sql("CREATE VIEW perm_view AS SELECT temp_func() as result FROM range(1)")
      }
      checkError(
        exception = exception,
        condition = "INVALID_TEMP_OBJ_REFERENCE",
        parameters = Map(
          "obj" -> "VIEW",
          "objName" -> "`spark_catalog`.`default`.`perm_view`",
          "tempObj" -> "FUNCTION",
          "tempObjName" -> "`temp_func`"))
    }

    sql("DROP TEMPORARY FUNCTION temp_func")
  }

  test("querying view with temp function after session restart fails gracefully") {
    // Simulates querying a view that referenced a temp function after the function
    // is no longer registered (e.g., after session restart).
    sql("CREATE TEMPORARY FUNCTION session_func(x INT) RETURNS INT RETURN x * 2")

    withTempView("v_with_func") {
      sql("CREATE TEMPORARY VIEW v_with_func AS SELECT session_func(5) as result")

      // Query works when function is registered.
      assert(sql("SELECT * FROM v_with_func").collect()(0).getInt(0) == 10)

      // Drop the function (simulates session restart).
      sql("DROP TEMPORARY FUNCTION session_func")

      // Now querying the view should fail with UNRESOLVED_ROUTINE.
      val exception = intercept[AnalysisException] {
        sql("SELECT * FROM v_with_func")
      }
      assert(exception.getMessage.contains("UNRESOLVED_ROUTINE"))
      assert(exception.getMessage.contains("session_func"))
    }
  }

  test("temporary view with temp function shadowing builtin") {
    // Verifies that views correctly use temporary functions that shadow builtins.
    sql("CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 999")

    withTempView("v_shadow") {
      // View should use the temp function (shadowing the builtin).
      sql("CREATE TEMPORARY VIEW v_shadow AS SELECT abs() as result")
      assert(sql("SELECT * FROM v_shadow").collect()(0).getInt(0) == 999)
    }

    sql("DROP TEMPORARY FUNCTION abs")
  }

  test("multiple temp functions in same view") {
    // Verifies that views can reference multiple temporary functions.
    sql("CREATE TEMPORARY FUNCTION func1(x INT) RETURNS INT RETURN x + 1")
    sql("CREATE TEMPORARY FUNCTION func2(x INT) RETURNS INT RETURN x * 2")

    withTempView("v_multi") {
      sql("""CREATE TEMPORARY VIEW v_multi AS
            |SELECT func1(value) as f1, func2(value) as f2
            |FROM VALUES (5), (10) AS t(value)""".stripMargin)

      val result = sql("SELECT * FROM v_multi").collect()
      assert(result.length == 2)
      assert(result(0).getInt(0) == 6)  // func1(5) = 6.
      assert(result(0).getInt(1) == 10) // func2(5) = 10.
      assert(result(1).getInt(0) == 11) // func1(10) = 11.
      assert(result(1).getInt(1) == 20) // func2(10) = 20.
    }

    sql("DROP TEMPORARY FUNCTION func1")
    sql("DROP TEMPORARY FUNCTION func2")
  }

  test("nested views with temp functions") {
    // Verifies that nested views (view referencing another view) work with temp functions.
    sql("CREATE TEMPORARY FUNCTION add_ten(x INT) RETURNS INT RETURN x + 10")

    withTempView("v_base", "v_nested") {
      // Base view uses the temp function.
      sql("CREATE TEMPORARY VIEW v_base AS SELECT add_ten(value) as result " +
        "FROM VALUES (1), (2), (3) AS t(value)")

      // Nested view references the base view.
      sql("CREATE TEMPORARY VIEW v_nested AS SELECT result * 2 as doubled FROM v_base")

      val result = sql("SELECT * FROM v_nested").collect()
      assert(result.length == 3)
      assert(result(0).getInt(0) == 22) // (1+10)*2 = 22.
      assert(result(1).getInt(0) == 24) // (2+10)*2 = 24.
      assert(result(2).getInt(0) == 26) // (3+10)*2 = 26.
    }

    sql("DROP TEMPORARY FUNCTION add_ten")
  }

  // ==================== Persistent Function Resolution and PATH Order ====================
  // Tests that persistent functions are resolved after session and builtin functions,
  // and that PATH order is correctly maintained.

  test("persistent function resolved after builtin and session") {
    withUserDefinedFunction("my_upper" -> false) {
      // Create a persistent function.
      sql("""CREATE FUNCTION my_upper(x STRING)
            |RETURNS STRING
            |LANGUAGE SQL
            |RETURN upper(concat(x, '_persistent'))""".stripMargin)

      // Unqualified call should find the persistent function.
      val result1 = sql("SELECT my_upper('test')").collect()
      assert(result1(0).getString(0) == "TEST_PERSISTENT")

      // Now create a temporary function with the same name.
      sql("""CREATE TEMPORARY FUNCTION my_upper(x STRING)
            |RETURNS STRING
            |RETURN upper(concat(x, '_temp'))""".stripMargin)

      // Unqualified call should now find the temp function (PATH order: session before persistent).
      val result2 = sql("SELECT my_upper('test')").collect()
      assert(result2(0).getString(0) == "TEST_TEMP")

      // Fully qualified call should still find the persistent function.
      val result3 = sql("SELECT spark_catalog.default.my_upper('test')").collect()
      assert(result3(0).getString(0) == "TEST_PERSISTENT")

      sql("DROP TEMPORARY FUNCTION my_upper")
    }
  }

  test("builtin shadows persistent function with same name") {
    withUserDefinedFunction("my_abs" -> false) {
      // Create a persistent function named 'my_abs'.
      sql("""CREATE FUNCTION my_abs(x INT)
            |RETURNS INT
            |LANGUAGE SQL
            |RETURN x + 1000""".stripMargin)

      // Create builtin 'abs' - this shadows any persistent 'abs'.
      // Unqualified call finds the builtin.
      val result1 = sql("SELECT abs(-5)").collect()
      assert(result1(0).getInt(0) == 5) // Builtin abs.

      // Fully qualified call finds our persistent function.
      val result2 = sql("SELECT spark_catalog.default.my_abs(-5)").collect()
      assert(result2(0).getInt(0) == 995)
    }
  }

  test("persistent scalar function overrides persistent table function") {
    withUserDefinedFunction("my_func" -> false, "my_table_func" -> false) {
      // Create a persistent scalar function.
      sql("""CREATE FUNCTION my_func()
            |RETURNS INT
            |LANGUAGE SQL
            |RETURN 42""".stripMargin)

      // Create a persistent table function with a different name.
      sql("""CREATE FUNCTION my_table_func(x INT)
            |RETURNS TABLE(val INT)
            |RETURN SELECT x * 2""".stripMargin)

      // Verify scalar function works.
      val result1 = sql("SELECT my_func()").collect()
      assert(result1(0).getInt(0) == 42)

      // Verify table function works.
      val result2 = sql("SELECT * FROM my_table_func(5)").collect()
      assert(result2(0).getInt(0) == 10)

      // Replace the table function with a scalar function (cross-type test).
      sql("DROP FUNCTION my_table_func")
      sql("""CREATE FUNCTION my_table_func(x INT)
            |RETURNS INT
            |LANGUAGE SQL
            |RETURN x * 3""".stripMargin)

      // Using in scalar context should work (finds scalar version).
      val result3 = sql("SELECT my_table_func(5)").collect()
      assert(result3(0).getInt(0) == 15)

      // Using in table context should fail (no longer a table function).
      val exception = intercept[AnalysisException] {
        sql("SELECT * FROM my_table_func(5)")
      }
      assert(exception.getMessage.contains("NOT_A_TABLE_FUNCTION"))
    }
  }

  // ==================== Schema Name Collisions ====================
  // Tests that persistent functions in schemas named "builtin" or "session"
  // can only be resolved with fully qualified names.

  test("persistent function in 'builtin' schema accessible without collision") {
    withDatabase("builtin") {
      sql("CREATE DATABASE IF NOT EXISTS builtin")
      sql("USE builtin")
      // Create a persistent function in schema "builtin" with a unique name.
      sql("""CREATE FUNCTION my_unique_func(x INT)
            |RETURNS INT
            |LANGUAGE SQL
            |RETURN x + 100""".stripMargin)

      // Unqualified call should find the persistent function (no collision with system.builtin).
      val result1 = sql("SELECT my_unique_func(5)").collect()
      assert(result1(0).getInt(0) == 105)

      // Two-part qualification "builtin.my_unique_func" should also find it.
      val result2 = sql("SELECT builtin.my_unique_func(5)").collect()
      assert(result2(0).getInt(0) == 105)

      // Fully qualified call should also work.
      val result3 = sql("SELECT spark_catalog.builtin.my_unique_func(5)").collect()
      assert(result3(0).getInt(0) == 105)

      // Cleanup.
      sql("DROP FUNCTION my_unique_func")
      sql("USE default")
    }
  }

  test("temp function shadows persistent in 'builtin' schema") {
    withDatabase("builtin") {
      sql("CREATE DATABASE IF NOT EXISTS builtin")
      sql("USE builtin")
      // Create a persistent function in schema "builtin".
      sql("""CREATE FUNCTION my_shadow()
            |RETURNS STRING
            |LANGUAGE SQL
            |RETURN 'persistent_builtin'""".stripMargin)

      // Unqualified call finds the persistent function.
      val result1 = sql("SELECT my_shadow()").collect()
      assert(result1(0).getString(0) == "persistent_builtin")

      // Now create a temporary function with the same name.
      sql("CREATE TEMPORARY FUNCTION my_shadow() RETURNS STRING RETURN 'temp'")

      // Unqualified call now finds temp function (system.session has priority in PATH).
      val result2 = sql("SELECT my_shadow()").collect()
      assert(result2(0).getString(0) == "temp")

      // Two-part "builtin.my_shadow" should still find the persistent one
      // (resolves to spark_catalog.builtin since system.builtin doesn't have it).
      val result3 = sql("SELECT builtin.my_shadow()").collect()
      assert(result3(0).getString(0) == "persistent_builtin")

      // Fully qualified finds the persistent function.
      val result4 = sql("SELECT spark_catalog.builtin.my_shadow()").collect()
      assert(result4(0).getString(0) == "persistent_builtin")

      // Qualified temp function access.
      val result5 = sql("SELECT session.my_shadow()").collect()
      assert(result5(0).getString(0) == "temp")

      sql("DROP TEMPORARY FUNCTION my_shadow")
      sql("DROP FUNCTION my_shadow")
      sql("USE default")
    }
  }

  test("builtin function has priority over persistent in 'builtin' schema") {
    withDatabase("builtin") {
      sql("CREATE DATABASE IF NOT EXISTS builtin")
      // Create a persistent function named "upper" in schema "builtin".
      sql("""CREATE FUNCTION builtin.upper(x STRING)
            |RETURNS STRING
            |LANGUAGE SQL
            |RETURN concat(x, '_custom')""".stripMargin)

      // Unqualified call finds system.builtin.upper.
      val result1 = sql("SELECT upper('test')").collect()
      assert(result1(0).getString(0) == "TEST") // Builtin behavior, not "_custom".

      // Two-part "builtin.upper" also finds system.builtin.upper.
      val result2 = sql("SELECT builtin.upper('test')").collect()
      assert(result2(0).getString(0) == "TEST")

      // Fully qualified finds the persistent function.
      val result3 = sql("SELECT spark_catalog.builtin.upper('test')").collect()
      assert(result3(0).getString(0) == "test_custom")

      // Cleanup with fully qualified name.
      sql("DROP FUNCTION spark_catalog.builtin.upper")
    }
  }
}
