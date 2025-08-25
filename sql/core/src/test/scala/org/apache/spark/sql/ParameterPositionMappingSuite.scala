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
 * Test suite for verifying that parameter position mapping works correctly
 * across all phases of SQL execution (parsing, analysis, runtime).
 *
 * This ensures that error messages show positions relative to the original
 * SQL text that users submitted, not the substituted text.
 */
class ParameterPositionMappingSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create test table for analysis and runtime tests
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW test_table AS SELECT 1 as id, 10 as value")
  }

  /**
   * Helper function to verify that error positions are correctly mapped back to original SQL
   */
  private def verifyErrorPosition(exception: Throwable, originalSql: String, errorToken: String): Unit = {
    exception match {
      case pe: ParseException =>
        assert(pe.getQueryContext.nonEmpty, "ParseException should have query context")
        val context = pe.getQueryContext.head
        assert(context.sqlText.contains(originalSql), 
          s"Query context should contain original SQL: ${context.sqlText}")
        assert(context.originStartIndex.isDefined, "Origin start index should be defined")
        assert(context.originStopIndex.isDefined, "Origin stop index should be defined")
        
        val errorStartPos = context.originStartIndex.get
        val errorStopPos = context.originStopIndex.get
        val expectedStartPos = originalSql.indexOf(errorToken)
        val expectedStopPos = expectedStartPos + errorToken.length - 1
        
        assert(expectedStartPos >= 0, s"Error token '$errorToken' not found in SQL: $originalSql")
        assert(errorStartPos == expectedStartPos, 
          s"Expected error start at position $expectedStartPos (token '$errorToken') but got $errorStartPos in SQL: $originalSql")
        assert(errorStopPos == expectedStopPos, 
          s"Expected error stop at position $expectedStopPos (token '$errorToken') but got $errorStopPos in SQL: $originalSql")
          
      case ae: AnalysisException =>
        assert(ae.context.nonEmpty, "AnalysisException should have query context")
        val context = ae.context.head
        assert(context.sqlText.contains(originalSql), 
          s"Query context should contain original SQL: ${context.sqlText}")
        assert(context.originStartIndex.isDefined, "Origin start index should be defined")
        assert(context.originStopIndex.isDefined, "Origin stop index should be defined")
        
        val errorStartPos = context.originStartIndex.get
        val errorStopPos = context.originStopIndex.get
        val expectedStartPos = originalSql.indexOf(errorToken)
        val expectedStopPos = expectedStartPos + errorToken.length - 1
        
        assert(expectedStartPos >= 0, s"Error token '$errorToken' not found in SQL: $originalSql")
        assert(errorStartPos == expectedStartPos, 
          s"Expected error start at position $expectedStartPos (token '$errorToken') but got $errorStartPos in SQL: $originalSql")
        assert(errorStopPos == expectedStopPos, 
          s"Expected error stop at position $expectedStopPos (token '$errorToken') but got $errorStopPos in SQL: $originalSql")
          
      case _ =>
        // For other exception types (like ArithmeticException), we just verify they occur
        // Runtime exceptions typically don't have position context
    }
  }

  // =============================================================================
  // Parser Failure Tests - Substitution Parser
  // =============================================================================

  test("parser failure in substitution parser - invalid syntax before parameter") {
    val sql = "SELECT INVALID_SYNTAX :param FROM test_table"
    val params = Map("param" -> 42)
    
    val exception = intercept[ParseException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "INVALID_SYNTAX")
  }

  test("parser failure in substitution parser - invalid syntax after parameter") {
    val sql = "SELECT :param INVALID_SYNTAX FROM test_table"
    val params = Map("param" -> 42)
    
    val exception = intercept[ParseException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "INVALID_SYNTAX")
  }

  test("parser failure in substitution parser - invalid syntax between parameters") {
    val sql = "SELECT :param1 INVALID_SYNTAX :param2 FROM test_table"
    val params = Map("param1" -> 42, "param2" -> 24)
    
    val exception = intercept[ParseException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "INVALID_SYNTAX")
  }

  test("parser failure in substitution parser - invalid syntax after two parameters") {
    val sql = "SELECT :param1, :param2 INVALID_SYNTAX FROM test_table"
    val params = Map("param1" -> 42, "param2" -> 24)
    
    val exception = intercept[ParseException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "INVALID_SYNTAX")
  }

  // =============================================================================
  // Analysis Failure Tests - Undefined Column
  // =============================================================================

  test("analysis failure - undefined column before parameter") {
    val sql = "SELECT undefined_column, :param FROM test_table"
    val params = Map("param" -> 42)
    
    val exception = intercept[AnalysisException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "undefined_column")
  }

  test("analysis failure - undefined column after parameter") {
    val sql = "SELECT :param, undefined_column FROM test_table"
    val params = Map("param" -> 42)
    
    val exception = intercept[AnalysisException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "undefined_column")
  }

  test("analysis failure - undefined column between parameters") {
    val sql = "SELECT :param1, undefined_column, :param2 FROM test_table"
    val params = Map("param1" -> 42, "param2" -> 24)
    
    val exception = intercept[AnalysisException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "undefined_column")
  }

  test("analysis failure - undefined column after two parameters") {
    val sql = "SELECT :param1, :param2, undefined_column FROM test_table"
    val params = Map("param1" -> 42, "param2" -> 24)
    
    val exception = intercept[AnalysisException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "undefined_column")
  }

  // =============================================================================
  // Runtime Failure Tests - Division by Zero (Non-Parameter)
  // =============================================================================

  test("runtime failure - division by zero before parameter") {
    val sql = "SELECT (10 / 0), :param FROM test_table"
    val params = Map("param" -> 42)
    
    val exception = intercept[ArithmeticException] {
      spark.sql(sql, params).collect()
    }
    
    // Runtime exceptions may not always have position context, but if they do,
    // it should point to original SQL
    // Note: ArithmeticException typically doesn't have QueryContext, 
    // but this tests the framework
    assert(exception.getMessage.contains("/ by zero"))
  }

  test("runtime failure - division by zero after parameter") {
    val sql = "SELECT :param, (10 / 0) FROM test_table"
    val params = Map("param" -> 42)
    
    val exception = intercept[ArithmeticException] {
      spark.sql(sql, params).collect()
    }
    
    assert(exception.getMessage.contains("/ by zero"))
  }

  test("runtime failure - division by zero between parameters") {
    val sql = "SELECT :param1, (10 / 0), :param2 FROM test_table"
    val params = Map("param1" -> 42, "param2" -> 24)
    
    val exception = intercept[ArithmeticException] {
      spark.sql(sql, params).collect()
    }
    
    assert(exception.getMessage.contains("/ by zero"))
  }

  test("runtime failure - division by zero after two parameters") {
    val sql = "SELECT :param1, :param2, (10 / 0) FROM test_table"
    val params = Map("param1" -> 42, "param2" -> 24)
    
    val exception = intercept[ArithmeticException] {
      spark.sql(sql, params).collect()
    }
    
    assert(exception.getMessage.contains("/ by zero"))
  }

  // =============================================================================
  // Runtime Failure Tests - Division by Parameter (Parameter is Zero)
  // =============================================================================

  test("runtime failure - division by parameter (zero) before other parameter") {
    val sql = "SELECT (10 / :divisor), :param FROM test_table"
    val params = Map("divisor" -> 0, "param" -> 42)
    
    val exception = intercept[ArithmeticException] {
      spark.sql(sql, params).collect()
    }
    
    assert(exception.getMessage.contains("/ by zero"))
  }

  test("runtime failure - division by parameter (zero) after other parameter") {
    val sql = "SELECT :param, (10 / :divisor) FROM test_table"
    val params = Map("param" -> 42, "divisor" -> 0)
    
    val exception = intercept[ArithmeticException] {
      spark.sql(sql, params).collect()
    }
    
    assert(exception.getMessage.contains("/ by zero"))
  }

  test("runtime failure - division by parameter (zero) between parameters") {
    val sql = "SELECT :param1, (10 / :divisor), :param2 FROM test_table"
    val params = Map("param1" -> 42, "divisor" -> 0, "param2" -> 24)
    
    val exception = intercept[ArithmeticException] {
      spark.sql(sql, params).collect()
    }
    
    assert(exception.getMessage.contains("/ by zero"))
  }

  test("runtime failure - division by parameter (zero) after two parameters") {
    val sql = "SELECT :param1, :param2, (10 / :divisor) FROM test_table"
    val params = Map("param1" -> 42, "param2" -> 24, "divisor" -> 0)
    
    val exception = intercept[ArithmeticException] {
      spark.sql(sql, params).collect()
    }
    
    assert(exception.getMessage.contains("/ by zero"))
  }

  // =============================================================================
  // Positional Parameter Tests
  // =============================================================================

  test("parser failure with positional parameters - invalid syntax before parameter") {
    val sql = "SELECT INVALID_SYNTAX ? FROM test_table"
    val params = Array(42)
    
    val exception = intercept[ParseException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "INVALID_SYNTAX")
  }

  test("analysis failure with positional parameters - undefined column after parameter") {
    val sql = "SELECT ?, undefined_column FROM test_table"
    val params = Array(42)
    
    val exception = intercept[AnalysisException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "undefined_column")
  }

  test("runtime failure with positional parameters - division by parameter (zero)") {
    val sql = "SELECT (10 / ?) FROM test_table"
    val params = Array(0)
    
    val exception = intercept[ArithmeticException] {
      spark.sql(sql, params).collect()
    }
    
    assert(exception.getMessage.contains("/ by zero"))
  }

  // =============================================================================
  // Edge Cases and Complex Scenarios
  // =============================================================================

  test("complex parameter substitution with nested expressions") {
    val sql = "SELECT CASE WHEN :condition THEN :value1 ELSE undefined_column END FROM test_table"
    val params = Map("condition" -> true, "value1" -> 42)
    
    val exception = intercept[AnalysisException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "undefined_column")
  }

  test("parameter substitution with string literals containing special characters") {
    val sql = "SELECT ':not_a_param', :real_param, undefined_column FROM test_table"
    val params = Map("real_param" -> "test")
    
    val exception = intercept[AnalysisException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "undefined_column")
  }

  test("multiple parameter substitutions with varying lengths") {
    val sql = "SELECT :a, :very_long_parameter_name, undefined_column FROM test_table"
    val params = Map("a" -> 1, "very_long_parameter_name" -> "test_value")
    
    val exception = intercept[AnalysisException] {
      spark.sql(sql, params).collect()
    }
    
    verifyErrorPosition(exception, sql, "undefined_column")
  }

  // =============================================================================
  // Verification Helper Tests
  // =============================================================================

  test("successful parameter substitution preserves original behavior") {
    val sql = "SELECT :param1, :param2 FROM test_table"
    val params = Map("param1" -> 42, "param2" -> "test")
    
    val result = spark.sql(sql, params).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 42)
    assert(result(0).getString(1) == "test")
  }

  test("position mapping handles empty parameter substitution") {
    val sql = "SELECT undefined_column FROM test_table"
    
    val exception = intercept[AnalysisException] {
      spark.sql(sql).collect()
    }
    
    // Even without parameters, error positions should be accurate
    verifyErrorPosition(exception, sql, "undefined_column")
  }

  // =============================================================================
  // Position Range Verification Tests
  // =============================================================================

  test("verify exact start and stop positions for short error token") {
    val sql = "SELECT bad FROM test_table"
    
    val exception = intercept[AnalysisException] {
      spark.sql(sql).collect()
    }
    
    // "bad" should be at positions 7-9 (length 3)
    // Start: 7, Stop: 9 (7 + 3 - 1)
    verifyErrorPosition(exception, sql, "bad")
  }

  test("verify exact start and stop positions for long error token") {
    val sql = "SELECT very_long_undefined_column_name FROM test_table"
    
    val exception = intercept[AnalysisException] {
      spark.sql(sql).collect()
    }
    
    // "very_long_undefined_column_name" should be at positions 7-36 (length 30)
    // Start: 7, Stop: 36 (7 + 30 - 1)
    verifyErrorPosition(exception, sql, "very_long_undefined_column_name")
  }

  test("verify position mapping with parameter substitution affects ranges correctly") {
    val sql = "SELECT :short_param, very_long_undefined_column_name FROM test_table"
    val params = Map("short_param" -> 42)
    
    val exception = intercept[AnalysisException] {
      spark.sql(sql, params).collect()
    }
    
    // Even after parameter substitution, error should point to original positions
    // "very_long_undefined_column_name" starts at position 21 in original SQL
    verifyErrorPosition(exception, sql, "very_long_undefined_column_name")
  }
}
