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
}
