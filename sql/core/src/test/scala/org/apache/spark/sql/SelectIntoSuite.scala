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

import org.apache.spark.{SparkArithmeticException, SparkConf, SparkException, SparkRuntimeException}
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Comprehensive test suite for SELECT INTO functionality.
 * Tests all error conditions and valid use cases.
 *
 * TABLE OF CONTENTS
 * =================
 * A. FEATURE CONFIGURATION
 *   1. Enable ANSI mode for overflow detection (setup)
 *   2. Disable SELECT INTO feature flag
 *   3. SELECT INTO rejected when feature disabled
 *   4. Re-enable SELECT INTO feature flag
 *
 * B. BASIC USAGE
 *   5. Single column into single variable
 *   6. Multiple columns into multiple variables
 *   7. Expressions into variables
 *   8. SELECT * with INTO
 *   9. Column aliases with INTO
 *
 * C. TYPE CASTING
 *  10. Implicit upcast (INT to BIGINT)
 *  11. Implicit downcast with precision loss (DOUBLE to INT)
 *  12. Invalid cast error (STRING to INT)
 *  13. Numeric overflow (large DOUBLE to INT)
 *  14. Numeric overflow (INT to SMALLINT)
 *
 * D. VARIABLE SCOPING
 *  15. Local and session variables combined
 *  16. Qualified variable names (system.session.varname)
 *  17. IDENTIFIER clause for variable names
 *
 * E. STRUCT HANDLING
 *  18. Struct unpacking (multiple columns into single struct)
 *  19. Struct field access after unpacking
 *  20. Struct field count mismatch (error)
 *
 * F. ZERO ROWS BEHAVIOR (NO DATA CONDITION)
 *  21. SELECT INTO raises NO DATA (SQLSTATE 02000) on zero rows - unhandled
 *  22. SELECT INTO with CONTINUE HANDLER for NOT FOUND
 *  23. SELECT INTO with CONTINUE HANDLER for SQLSTATE '02000'
 *  24. SELECT INTO with EXIT HANDLER for NOT FOUND
 *  24b. SELECT INTO NO DATA with struct variable and handler
 *
 * G. RESULT SET BEHAVIOR
 *  25. SELECT INTO does not return a result set
 *
 * H. ERROR CASES - CONTEXT/PLACEMENT
 *  26. SELECT INTO outside SQL script (not in BEGIN...END)
 *  27. SELECT INTO in subquery
 *  28. SELECT INTO in UNION (first branch)
 *  29. SELECT INTO in UNION (last branch)
 *  30. SELECT INTO in INTERSECT
 *  31. SELECT INTO in EXCEPT
 *
 * I. ERROR CASES - DATA ISSUES
 *  32. Too many variables (3 vars, 2 columns)
 *  33. Too few variables (1 var, 2 columns)
 *  34. Variable count mismatch with expressions
 *  35. Multiple rows returned (error)
 */
class SelectIntoSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.variable.substitute", "false")
      .set(SQLConf.ANSI_ENABLED.key, "true")
  }

  // Setup test data view used by most tests
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Enable handler support for NO DATA conditions
    conf.setConf(SQLConf.SQL_SCRIPTING_CONTINUE_HANDLER_ENABLED, true)
    createTestView()
  }

  override def afterEach(): Unit = {
    try {
      // Clean up session variables after each test
      spark.sql("DROP TEMPORARY VARIABLE IF EXISTS session_var")
      spark.sql("DROP TEMPORARY VARIABLE IF EXISTS myvar")
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
    super.afterEach()
  }

  override def afterAll(): Unit = {
    try {
      spark.sql("DROP VIEW IF EXISTS tbl_view")
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
    super.afterAll()
  }

  private def createTestView(): Unit = {
    spark.sql(
      """
        |CREATE OR REPLACE TEMP VIEW tbl_view AS SELECT * FROM VALUES
        |  (10, 'name1'),
        |  (20, 'name2'),
        |  (30, 'name3')
        |AS tbl_view(id, name)
      """.stripMargin)
  }

  // =============================================================================
  // SECTION A: FEATURE CONFIGURATION
  // =============================================================================

  // Test 1: Enable ANSI mode for overflow detection
  // This is handled in sparkConf

  // =============================================================================
  // Test 2: Disable SELECT INTO feature flag
  // =============================================================================
  test("Test 2: Disable SELECT INTO feature flag") {
    withSQLConf(SQLConf.SQL_SCRIPTING_SELECT_INTO_ENABLED.key -> "false") {
      // Feature is disabled, verify via config
      assert(spark.conf.get(SQLConf.SQL_SCRIPTING_SELECT_INTO_ENABLED.key) == "false")
    }
  }

  // =============================================================================
  // Test 3: SELECT INTO rejected when feature disabled
  // =============================================================================
  test("Test 3: SELECT INTO rejected when feature disabled") {
    withSQLConf(SQLConf.SQL_SCRIPTING_SELECT_INTO_ENABLED.key -> "false") {
      val script =
        """
          |BEGIN
          |  DECLARE v1 INT;
          |  SELECT id INTO v1 FROM tbl_view WHERE id = 10;
          |END;
        """.stripMargin

      checkError(
        exception = intercept[AnalysisException] {
          spark.sql(script).collect()
        },
        condition = "SELECT_INTO_FEATURE_DISABLED",
        parameters = Map.empty,
        sqlState = Some("0A000"))
    }
  }

  // Test 4: Re-enable SELECT INTO feature flag
  // This is the default state in sparkConf

  // =============================================================================
  // SECTION B: BASIC USAGE
  // =============================================================================

  // =============================================================================
  // Test 5: Single column into single variable
  // =============================================================================
  test("Test 5: Single column into single variable") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |  SELECT id INTO v1 FROM tbl_view WHERE id = 30;
        |  SELECT v1;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 30)
  }

  // =============================================================================
  // Test 6: Multiple columns into multiple variables
  // =============================================================================
  test("Test 6: Multiple columns into multiple variables") {
    val script =
      """
        |BEGIN
        |  DECLARE var1 INT;
        |  DECLARE var2 STRING;
        |  SELECT id, name INTO var1, var2 FROM tbl_view WHERE id = 20;
        |  SELECT var1, var2;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 20)
    assert(result(0).getString(1) == "name2")
  }

  // =============================================================================
  // Test 7: Expressions into variables
  // =============================================================================
  test("Test 7: Expressions into variables") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |  DECLARE v2 INT;
        |  SELECT id + 10, id * 2 INTO v1, v2 FROM tbl_view WHERE id = 10;
        |  SELECT v1, v2;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 20)
    assert(result(0).getInt(1) == 20)
  }

  // =============================================================================
  // Test 8: SELECT * with INTO
  // =============================================================================
  test("Test 8: SELECT * with INTO") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |  DECLARE v2 STRING;
        |  SELECT * INTO v1, v2 FROM tbl_view WHERE id = 10;
        |  SELECT v1, v2;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 10)
    assert(result(0).getString(1) == "name1")
  }

  // =============================================================================
  // Test 9: Column aliases with INTO
  // =============================================================================
  test("Test 9: Column aliases with INTO") {
    val script =
      """
        |BEGIN
        |  DECLARE a INT;
        |  DECLARE b STRING;
        |  SELECT id AS my_id, name AS my_name INTO a, b FROM tbl_view WHERE id = 30;
        |  SELECT a, b;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 30)
    assert(result(0).getString(1) == "name3")
  }

  // =============================================================================
  // SECTION C: TYPE CASTING
  // =============================================================================

  // =============================================================================
  // Test 10: Implicit upcast (INT to BIGINT)
  // =============================================================================
  test("Test 10: Implicit upcast (INT to BIGINT)") {
    val script =
      """
        |BEGIN
        |  DECLARE big_var BIGINT;
        |  SELECT id INTO big_var FROM tbl_view WHERE id = 10;
        |  SELECT big_var, typeof(big_var);
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 10L)
    assert(result(0).getString(1) == "bigint")
  }

  // =============================================================================
  // Test 11: Implicit downcast with precision loss (DOUBLE to INT)
  // =============================================================================
  test("Test 11: Implicit downcast with precision loss (DOUBLE to INT)") {
    val script =
      """
        |BEGIN
        |  DECLARE int_var INT;
        |  SELECT CAST(1.9 AS DOUBLE) AS dval INTO int_var;
        |  SELECT int_var, typeof(int_var);
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 1)
    assert(result(0).getString(1) == "int")
  }

  // =============================================================================
  // Test 12: Invalid cast error (STRING to INT)
  // =============================================================================
  test("Test 12: Invalid cast error (STRING to INT)") {
    val script =
      """
        |BEGIN
        |  DECLARE int_var INT;
        |  SELECT name INTO int_var FROM tbl_view WHERE id = 10;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[SparkRuntimeException] {
        spark.sql(script).collect()
      },
      condition = "INVALID_VARIABLE_TYPE_FOR_ASSIGNMENT",
      parameters = Map(
        "sqlValue" -> "name1",
        "sqlValueType" -> "STRING",
        "varName" -> "`int_var`",
        "varType" -> "INT"),
      sqlState = Some("42821"))
  }

  // =============================================================================
  // Test 13: Numeric overflow (large DOUBLE to INT)
  // =============================================================================
  test("Test 13: Numeric overflow (large DOUBLE to INT)") {
    val script =
      """
        |BEGIN
        |  DECLARE int_var INT;
        |  SELECT CAST(1.0E10 AS DOUBLE) AS bigval INTO int_var;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[SparkArithmeticException] {
        spark.sql(script).collect()
      },
      condition = "CAST_OVERFLOW",
      parameters = Map(
        "ansiConfig" -> "\"spark.sql.ansi.enabled\"",
        "sourceType" -> "\"DOUBLE\"",
        "targetType" -> "\"INT\"",
        "value" -> "1.0E10D"),
      sqlState = Some("22003"))
  }

  // =============================================================================
  // Test 14: Numeric overflow (INT to SMALLINT)
  // =============================================================================
  test("Test 14: Numeric overflow (INT to SMALLINT)") {
    val script =
      """
        |BEGIN
        |  DECLARE small_var SMALLINT;
        |  SELECT 100000 AS bigint INTO small_var;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[SparkArithmeticException] {
        spark.sql(script).collect()
      },
      condition = "CAST_OVERFLOW",
      parameters = Map(
        "ansiConfig" -> "\"spark.sql.ansi.enabled\"",
        "sourceType" -> "\"INT\"",
        "targetType" -> "\"SMALLINT\"",
        "value" -> "100000"),
      sqlState = Some("22003"))
  }

  // =============================================================================
  // SECTION D: VARIABLE SCOPING
  // =============================================================================

  // =============================================================================
  // Test 15: Local and session variables combined
  // =============================================================================
  test("Test 15: Local and session variables combined") {
    val script =
      """
        |BEGIN
        |  DECLARE local_var INT;
        |  DECLARE VARIABLE session_var STRING DEFAULT 'initial';
        |  SELECT id, name INTO local_var, session_var FROM tbl_view WHERE id = 10;
        |  SELECT local_var, session_var;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 10)
    assert(result(0).getString(1) == "name1")

    // Verify session variable persists
    val result2 = spark.sql("SELECT session_var").collect()
    assert(result2.length == 1)
    assert(result2(0).getString(0) == "name1")
  }

  // =============================================================================
  // Test 16: Qualified variable names (system.session.varname)
  // =============================================================================
  test("Test 16: Qualified variable names (system.session.varname)") {
    val script =
      """
        |BEGIN
        |  DECLARE myvar INT DEFAULT 99;
        |  SELECT id INTO system.session.myvar FROM tbl_view WHERE id = 20;
        |  SELECT system.session.myvar;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 20)
  }

  // =============================================================================
  // Test 17: IDENTIFIER clause for variable names
  // =============================================================================
  test("Test 17: IDENTIFIER clause for variable names") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |  DECLARE v2 STRING;
        |  SELECT id, name INTO IDENTIFIER('v1'), IDENTIFIER('v2')
        |  FROM tbl_view WHERE id = 30;
        |  SELECT v1, v2;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 30)
    assert(result(0).getString(1) == "name3")
  }

  // =============================================================================
  // SECTION E: STRUCT HANDLING
  // =============================================================================

  // =============================================================================
  // Test 18: Struct unpacking (multiple columns into single struct)
  // =============================================================================
  test("Test 18: Struct unpacking (multiple columns into single struct)") {
    val script =
      """
        |BEGIN
        |  DECLARE result_struct STRUCT<field1: INT, field2: STRING>;
        |  SELECT id, name INTO result_struct FROM tbl_view WHERE id = 10;
        |  SELECT result_struct;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    val struct = result(0).getStruct(0)
    assert(struct.getInt(0) == 10)
    assert(struct.getString(1) == "name1")
  }

  // =============================================================================
  // Test 19: Struct field access after unpacking
  // =============================================================================
  test("Test 19: Struct field access after unpacking") {
    val script =
      """
        |BEGIN
        |  DECLARE my_struct STRUCT<a: INT, b: STRING>;
        |  SELECT id, name INTO my_struct FROM tbl_view WHERE id = 20;
        |  SELECT my_struct.a, my_struct.b;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 20)
    assert(result(0).getString(1) == "name2")
  }

  // =============================================================================
  // Test 20: Struct field count mismatch (error)
  // =============================================================================
  test("Test 20: Struct field count mismatch (error)") {
    val script =
      """
        |BEGIN
        |  DECLARE result_struct STRUCT<f1: INT, f2: STRING>;
        |  SELECT id, name, id + 10 INTO result_struct FROM tbl_view WHERE id = 10;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(script).collect()
      },
      condition = "SELECT_INTO_STRUCT_FIELD_MISMATCH",
      parameters = Map(
        "numCols" -> "3",
        "numFields" -> "2",
        "structType" -> "\"STRUCT<f1: INT, f2: STRING>\""),
      sqlState = Some("21000"))
  }

  // =============================================================================
  // SECTION F: ZERO ROWS BEHAVIOR (NO DATA CONDITION)
  // =============================================================================

  // =============================================================================
  // Test 21: SELECT INTO raises NO DATA (SQLSTATE 02000) on zero rows - unhandled
  // =============================================================================
  test("Test 21: SELECT INTO raises NO DATA (SQLSTATE 02000) on zero rows - unhandled") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |  SET VAR v1 = 42;
        |  SELECT id INTO v1 FROM tbl_view WHERE 1=0;
        |  SELECT v1;  -- Should not execute
        |END;
      """.stripMargin

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(script).collect()
      },
      condition = "SELECT_INTO_NO_DATA",
      parameters = Map.empty,
      sqlState = Some("02000"))
  }

  // =============================================================================
  // Test 22: SELECT INTO with CONTINUE HANDLER for NOT FOUND
  // =============================================================================
  test("Test 22: SELECT INTO with CONTINUE HANDLER for NOT FOUND") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |  DECLARE no_data_flag BOOLEAN DEFAULT false;
        |
        |  -- Handler catches NO DATA condition
        |  DECLARE CONTINUE HANDLER FOR NOT FOUND SET no_data_flag = true;
        |
        |  SET VAR v1 = 42;
        |  SELECT id INTO v1 FROM tbl_view WHERE 1=0;  -- Triggers handler
        |
        |  -- Execution continues, variables unchanged, flag set
        |  SELECT v1, no_data_flag;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 42)
    assert(result(0).getBoolean(1) == true)
  }

  // =============================================================================
  // Test 23: SELECT INTO with CONTINUE HANDLER for SQLSTATE '02000'
  // =============================================================================
  test("Test 23: SELECT INTO with CONTINUE HANDLER for SQLSTATE '02000'") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |  DECLARE v2 STRING;
        |  DECLARE found BOOLEAN DEFAULT true;
        |
        |  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET found = false;
        |
        |  SET VAR v1 = 99;
        |  SET VAR v2 = 'initial';
        |  SELECT id, name INTO v1, v2 FROM tbl_view WHERE id = 999;  -- Triggers handler
        |
        |  SELECT v1, v2, found;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 99)
    assert(result(0).getString(1) == "initial")
    assert(result(0).getBoolean(2) == false)
  }

  // =============================================================================
  // Test 24: SELECT INTO with EXIT HANDLER for NOT FOUND
  // =============================================================================
  test("Test 24: SELECT INTO with EXIT HANDLER for NOT FOUND") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |
        |  -- EXIT handler terminates the block
        |  DECLARE EXIT HANDLER FOR NOT FOUND BEGIN
        |    VALUES ('Handler executed - no data found');
        |  END;
        |
        |  SET VAR v1 = 100;
        |  SELECT id INTO v1 FROM tbl_view WHERE FALSE;  -- Triggers handler, exits block
        |
        |  VALUES ('This should not execute');
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "Handler executed - no data found")
  }

  // =============================================================================
  // Test 24b: SELECT INTO NO DATA with struct variable and handler
  // =============================================================================
  test("Test 24b: SELECT INTO NO DATA with struct variable and handler") {
    val script =
      """
        |BEGIN
        |  DECLARE my_struct STRUCT<x: INT, y: STRING>;
        |  DECLARE handled BOOLEAN DEFAULT false;
        |
        |  DECLARE CONTINUE HANDLER FOR NOT FOUND SET handled = true;
        |
        |  SET VAR my_struct = named_struct('x', 100, 'y', 'original');
        |  SELECT id, name INTO my_struct FROM tbl_view WHERE id < 0;  -- Triggers handler
        |
        |  SELECT my_struct, handled;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    val struct = result(0).getStruct(0)
    assert(struct.getInt(0) == 100)
    assert(struct.getString(1) == "original")
    assert(result(0).getBoolean(1) == true)
  }

  // =============================================================================
  // SECTION G: RESULT SET BEHAVIOR
  // =============================================================================

  // =============================================================================
  // Test 25: SELECT INTO does not return a result set
  // =============================================================================
  test("Test 25: SELECT INTO does not return a result set") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |  DECLARE v2 STRING;
        |  SELECT id, name FROM tbl_view WHERE id = 10;
        |  SELECT id INTO v1 FROM tbl_view WHERE id = 20;
        |  SELECT name INTO v2 FROM tbl_view WHERE id = 30;
        |  SELECT v1, v2;
        |END;
      """.stripMargin

    // Only the last SELECT (without INTO) returns a result set
    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 20)
    assert(result(0).getString(1) == "name3")
  }

  // =============================================================================
  // SECTION H: ERROR CASES - CONTEXT/PLACEMENT
  // =============================================================================

  // =============================================================================
  // Test 26: SELECT INTO outside SQL script (not in BEGIN...END)
  // =============================================================================
  test("Test 26: SELECT INTO outside SQL script (not in BEGIN...END)") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("SELECT id INTO res_id FROM tbl_view WHERE id = 10").collect()
      },
      condition = "SELECT_INTO_NOT_IN_SQL_SCRIPT",
      parameters = Map.empty,
      sqlState = Some("0A000"))
  }

  // =============================================================================
  // Test 27: SELECT INTO in subquery
  // =============================================================================
  test("Test 27: SELECT INTO in subquery") {
    val script =
      """
        |BEGIN
        |  DECLARE outer_id INT;
        |  SELECT (SELECT id INTO outer_id FROM tbl_view WHERE id = 10) as nested;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(script).collect()
      },
      condition = "SELECT_INTO_ONLY_AT_TOP_LEVEL",
      parameters = Map.empty,
      sqlState = Some("42601"))
  }

  // =============================================================================
  // Test 28: SELECT INTO in UNION (first branch)
  // =============================================================================
  test("Test 28: SELECT INTO in UNION (first branch)") {
    val script =
      """
        |BEGIN
        |  DECLARE res_id INT;
        |  SELECT id INTO res_id FROM tbl_view WHERE id = 10
        |  UNION
        |  SELECT id FROM tbl_view WHERE id = 20;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(script).collect()
      },
      condition = "SELECT_INTO_ONLY_AT_TOP_LEVEL",
      parameters = Map.empty,
      sqlState = Some("42601"))
  }

  // =============================================================================
  // Test 29: SELECT INTO in UNION (last branch)
  // =============================================================================
  test("Test 29: SELECT INTO in UNION (last branch)") {
    val script =
      """
        |BEGIN
        |  DECLARE v INT;
        |  SELECT id FROM tbl_view WHERE id = 10
        |  UNION
        |  SELECT id INTO v FROM tbl_view WHERE id = 20;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(script).collect()
      },
      condition = "SELECT_INTO_ONLY_AT_TOP_LEVEL",
      parameters = Map.empty,
      sqlState = Some("42601"))
  }

  // =============================================================================
  // Test 30: SELECT INTO in INTERSECT
  // =============================================================================
  test("Test 30: SELECT INTO in INTERSECT") {
    val script =
      """
        |BEGIN
        |  DECLARE res_id INT;
        |  SELECT id INTO res_id FROM tbl_view WHERE id = 10
        |  INTERSECT
        |  SELECT id FROM tbl_view WHERE id = 10;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(script).collect()
      },
      condition = "SELECT_INTO_ONLY_AT_TOP_LEVEL",
      parameters = Map.empty,
      sqlState = Some("42601"))
  }

  // =============================================================================
  // Test 31: SELECT INTO in EXCEPT
  // =============================================================================
  test("Test 31: SELECT INTO in EXCEPT") {
    val script =
      """
        |BEGIN
        |  DECLARE res_id INT;
        |  SELECT id INTO res_id FROM tbl_view WHERE id = 10
        |  EXCEPT
        |  SELECT id FROM tbl_view WHERE id = 20;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(script).collect()
      },
      condition = "SELECT_INTO_ONLY_AT_TOP_LEVEL",
      parameters = Map.empty,
      sqlState = Some("42601"))
  }

  // =============================================================================
  // SECTION I: ERROR CASES - DATA ISSUES
  // =============================================================================

  // =============================================================================
  // Test 32: Too many variables (3 vars, 2 columns)
  // =============================================================================
  test("Test 32: Too many variables (3 vars, 2 columns)") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |  DECLARE v2 STRING;
        |  DECLARE v3 INT;
        |  SELECT id, name INTO v1, v2, v3 FROM tbl_view WHERE id = 10;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(script).collect()
      },
      condition = "SELECT_INTO_VARIABLE_COUNT_MISMATCH",
      parameters = Map(
        "numCols" -> "2",
        "numVars" -> "3"),
      sqlState = Some("21000"))
  }

  // =============================================================================
  // Test 33: Too few variables (1 var, 2 columns)
  // =============================================================================
  test("Test 33: Too few variables (1 var, 2 columns)") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |  SELECT id, name INTO v1 FROM tbl_view WHERE id = 10;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(script).collect()
      },
      condition = "SELECT_INTO_VARIABLE_COUNT_MISMATCH",
      parameters = Map(
        "numCols" -> "2",
        "numVars" -> "1"),
      sqlState = Some("21000"))
  }

  // =============================================================================
  // Test 34: Variable count mismatch with expressions
  // =============================================================================
  test("Test 34: Variable count mismatch with expressions") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |  DECLARE v2 INT;
        |  DECLARE v3 INT;
        |  SELECT id + 10, id * 2 INTO v1, v2, v3 FROM tbl_view WHERE id = 10;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(script).collect()
      },
      condition = "SELECT_INTO_VARIABLE_COUNT_MISMATCH",
      parameters = Map(
        "numCols" -> "2",
        "numVars" -> "3"),
      sqlState = Some("21000"))
  }

  // =============================================================================
  // Test 35: Multiple rows returned (error)
  // =============================================================================
  test("Test 35: Multiple rows returned (error)") {
    val script =
      """
        |BEGIN
        |  DECLARE v1 INT;
        |  SELECT id INTO v1 FROM tbl_view;
        |END;
      """.stripMargin

    checkError(
      exception = intercept[SparkException] {
        spark.sql(script).collect()
      },
      condition = "ROW_SUBQUERY_TOO_MANY_ROWS",
      parameters = Map.empty,
      sqlState = Some("21000"))
  }
}
