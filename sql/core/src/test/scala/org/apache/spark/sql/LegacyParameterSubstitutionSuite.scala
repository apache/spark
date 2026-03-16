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

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for the legacy parameter substitution configuration.
 * Tests the behavior when spark.sql.legacy.parameterSubstitution.constantsOnly is enabled.
 */
class LegacyParameterSubstitutionSuite extends QueryTest with SharedSparkSession {

  test("parameter substitution works everywhere when legacy config is disabled (default)") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "false") {
      // Test positional parameters work
      val result1 = spark.sql("SELECT ? as value", Array(42)).collect()
      assert(result1.length == 1)
      assert(result1(0).getInt(0) == 42)

      // Test named parameters work
      val result2 = spark.sql("SELECT :param as value", Map("param" -> "hello")).collect()
      assert(result2.length == 1)
      assert(result2(0).getString(0) == "hello")
    }
  }

  test("parameter substitution works by default (config not set)") {
    // Default behavior should allow parameter substitution
    val result1 = spark.sql("SELECT ? as value", Array(123)).collect()
    assert(result1.length == 1)
    assert(result1(0).getInt(0) == 123)

    val result2 = spark.sql("SELECT :test as value", Map("test" -> "world")).collect()
    assert(result2.length == 1)
    assert(result2(0).getString(0) == "world")
  }

  test("legacy config limits param substitution - positional params work via analyzer") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      // When legacy mode is enabled, parameter substitution is disabled but analyzer binding works
      // This means param markers are bound by the analyzer and should work correctly

      // Test positional parameter markers work in legacy mode through analyzer binding
      val result1 = spark.sql("SELECT ? as value", Array(42)).collect()
      assert(result1.length == 1)
      assert(result1(0).getInt(0) == 42)

      // Test that simple parameter usage works
      val result2 = spark.sql("SELECT ?", Array(1)).collect()
      assert(result2.length == 1)
      assert(result2(0).getInt(0) == 1)
    }
  }

  test("legacy config limits param substitution - named params work via analyzer") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      // Test named parameter markers work in legacy mode through analyzer binding
      val result1 = spark.sql("SELECT :param as value", Map("param" -> "hello")).collect()
      assert(result1.length == 1)
      assert(result1(0).getString(0) == "hello")

      // Test simple named parameter usage works
      val result2 = spark.sql("SELECT :test", Map("test" -> 42)).collect()
      assert(result2.length == 1)
      assert(result2(0).getInt(0) == 42)
    }
  }

  test("legacy config can be toggled dynamically") {
    // Start with parameter substitution enabled
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "false") {
      val result1 = spark.sql("SELECT ? as value", Array(1)).collect()
      assert(result1(0).getInt(0) == 1)
    }

    // Switch to legacy mode - parameters should still work through analyzer binding
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      val result2 = spark.sql("SELECT ? as value", Array(2)).collect()
      assert(result2(0).getInt(0) == 2)
    }

    // Switch back to enabled
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "false") {
      val result3 = spark.sql("SELECT ? as value", Array(3)).collect()
      assert(result3(0).getInt(0) == 3)
    }
  }

  test("legacy config preserves normal SQL parsing") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      // Normal SQL without parameters should work exactly as before
      val result = spark.sql("SELECT 42 as value").collect()
      assert(result.length == 1)
      assert(result(0).getInt(0) == 42)

      // Complex SQL should also work
      val complexResult = spark.sql("""
        SELECT
          CASE WHEN 1 = 1 THEN 'true' ELSE 'false' END as condition,
          42 + 8 as calculation
      """).collect()
      assert(complexResult.length == 1)
      assert(complexResult(0).getString(0) == "true")
      assert(complexResult(0).getInt(1) == 50)
    }
  }

  test("legacy config disables parameter substitution but preserves analyzer binding") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      // When legacy config is enabled, parameter substitution should be disabled
      // but parameter binding through the analyzer should still work

      // Test that positional parameters work through analyzer binding
      checkAnswer(
        spark.sql("SELECT ? FROM VALUES (1)", Array(42)),
        Row(42)
      )

      // Test that named parameters work through analyzer binding
      checkAnswer(
        spark.sql("SELECT :param FROM VALUES (1)", Map("param" -> "test")),
        Row("test")
      )
    }
  }

  test("parameter markers in data type contexts work when legacy config is disabled") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "false") {
      // These should work when legacy mode is disabled (parameter substitution enabled)
      // Note: These might fail for other reasons (like invalid SQL syntax), but should not
      // fail specifically due to parameter markers being disallowed
      // The important thing is that we don't get the PARAMETER_MARKER_NOT_ENABLED_IN_THIS_CONTEXT
      try {
        spark.sql("SELECT CAST('123' AS INT(?))", Array(10)).collect()
      } catch {
        case e: Exception =>
          // Should not be the parameter marker context error
          assert(!e.getMessage.contains("PARAMETER_MARKER_NOT_ENABLED_IN_THIS_CONTEXT"))
      }

      try {
        spark.sql("SELECT CAST('test' AS VARCHAR(:len))", Map("len" -> "10")).collect()
      } catch {
        case e: Exception =>
          // Should not be the parameter marker context error
          assert(!e.getMessage.contains("PARAMETER_MARKER_NOT_ENABLED_IN_THIS_CONTEXT"))
      }
    }
  }

  test("legacy config blocks parameter markers in data type specifications") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      // Test that parameter markers in DECIMAL type specifications cause parse errors
      checkError(
        exception = intercept[ParseException] {
          spark.sql("SELECT 5::DECIMAL(?, ?)").show()
        },
        condition = "PARSE_SYNTAX_ERROR",
        parameters = Map("error" -> "'?'", "hint" -> "")
      )

      // Test that named parameters in VARCHAR type specifications cause parse errors
      checkError(
        exception = intercept[ParseException] {
          spark.sql("CREATE TABLE test (col VARCHAR(:len))", Map("len" -> "50"))
        },
        condition = "PARSE_SYNTAX_ERROR",
        parameters = Map("error" -> "':'", "hint" -> ""),
        queryContext = Array(ExpectedContext("CREATE TABLE test (col VARCHAR(:len))", 0, 36))
      )

      // Test that parameter markers in CHAR type specifications cause parse errors
      checkError(
        exception = intercept[ParseException] {
          spark.sql("SELECT CAST('hello' AS CHAR(?))", Array(10))
        },
        condition = "PARSE_SYNTAX_ERROR",
        parameters = Map("error" -> "'?'", "hint" -> ""),
        queryContext = Array(ExpectedContext("SELECT CAST('hello' AS CHAR(?))", 0, 30))
      )
    }
  }

  test("legacy config blocks parameter markers in table properties") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      // Test that parameter markers in TBLPROPERTIES cause parse errors
      checkError(
        exception = intercept[ParseException] {
          spark.sql("CREATE TABLE test (id INT) TBLPROPERTIES ('key' = ?)", Array("value"))
        },
        condition = "PARSE_SYNTAX_ERROR",
        parameters = Map("error" -> "'?'", "hint" -> ""),
        queryContext = Array(ExpectedContext(
          "CREATE TABLE test (id INT) TBLPROPERTIES ('key' = ?)", 0, 51))
      )

      // Test that named parameters in TBLPROPERTIES cause parse errors
      checkError(
        exception = intercept[ParseException] {
          spark.sql(
            "CREATE TABLE test (id INT) TBLPROPERTIES ('timeout' = :timeout)",
                   Map("timeout" -> "300"))
        },
        condition = "PARSE_SYNTAX_ERROR",
        parameters = Map("error" -> "':'", "hint" -> ""),
        queryContext = Array(ExpectedContext(
          "CREATE TABLE test (id INT) TBLPROPERTIES ('timeout' = :timeout)", 0, 62))
      )
    }
  }

  test("parameter markers work normally when legacy config is disabled") {
    withSQLConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "false") {
      // These should work fine when the legacy config is disabled (default behavior)
      // Test DECIMAL with parameter markers works
      val result1 = spark.sql("SELECT CAST(123.45 AS DECIMAL(?, ?))", Array(10, 2)).collect()
      assert(result1.length == 1)
      assert(result1(0).getDecimal(0).toString == "123.45")

      // Test simple parameter substitution works in SELECT statements
      val result2 = spark.sql("SELECT :message as greeting", Map("message" -> "hello")).collect()
      assert(result2.length == 1)
      assert(result2(0).getString(0) == "hello")
    }
  }
}
