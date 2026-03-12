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

// scalastyle:off line.size.limit
package org.apache.spark.sql.scripting

import org.apache.spark.{SparkArithmeticException, SparkConf}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for SQL Cursors.
 * Complete transcription of cursors.sql - all 87 tests with inline expected results.
 */
class SqlScriptingCursorE2eSuite extends QueryTest with SharedSparkSession {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    conf.setConf(SQLConf.SQL_SCRIPTING_CURSOR_ENABLED, true)
    conf.setConf(SQLConf.SQL_SCRIPTING_CONTINUE_HANDLER_ENABLED, true)
  }

  protected override def afterAll(): Unit = {
    conf.unsetConf(SQLConf.SQL_SCRIPTING_CURSOR_ENABLED.key)
    conf.unsetConf(SQLConf.SQL_SCRIPTING_CONTINUE_HANDLER_ENABLED.key)
    super.afterAll()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.ANSI_ENABLED.key, "true")
  }

  test("Test 1: Verify DECLARE CURSOR is disabled by default") {
    withSQLConf(SQLConf.SQL_SCRIPTING_CURSOR_ENABLED.key -> "false") {
      checkError(
        exception = intercept[SqlScriptingException] {
          sql("""BEGIN
  DECLARE cur CURSOR FOR SELECT 1;
END;""")
        },
        condition = "UNSUPPORTED_FEATURE.SQL_CURSOR",
        parameters = Map.empty
      )
    }
  }

  test("Test 2: Verify OPEN is disabled by default") {
    withSQLConf(SQLConf.SQL_SCRIPTING_CURSOR_ENABLED.key -> "false") {
      checkError(
        exception = intercept[SqlScriptingException] {
          sql("""BEGIN
  OPEN cur;
END;""")
        },
        condition = "UNSUPPORTED_FEATURE.SQL_CURSOR",
        parameters = Map.empty
      )
    }
  }

  test("Test 3: Verify FETCH is disabled by default") {
    withSQLConf(SQLConf.SQL_SCRIPTING_CURSOR_ENABLED.key -> "false") {
      checkError(
        exception = intercept[SqlScriptingException] {
          sql("""BEGIN
  DECLARE x INT;
  FETCH cur INTO x;
END;""")
        },
        condition = "UNSUPPORTED_FEATURE.SQL_CURSOR",
        parameters = Map.empty
      )
    }
  }

  test("Test 4: Verify CLOSE is disabled by default") {
    withSQLConf(SQLConf.SQL_SCRIPTING_CURSOR_ENABLED.key -> "false") {
      checkError(
        exception = intercept[SqlScriptingException] {
          sql("""BEGIN
  CLOSE cur;
END;""")
        },
        condition = "UNSUPPORTED_FEATURE.SQL_CURSOR",
        parameters = Map.empty
      )
    }
  }

  test("Test 5: Cursors have a separate namespace from local variables") {
    // Success - cursor and variable can have same name
    val result = sql("""BEGIN
  DECLARE x INT DEFAULT 10;
  DECLARE x CURSOR FOR SELECT 1 AS col;
  OPEN x;
  FETCH x INTO x;
  VALUES (x); -- Should return 1
  CLOSE x;
END;""")
    checkAnswer(result, Seq(
      Row(1)))
  }

  test("Test 6: Duplicate cursor names in same compound statement are not...") {
    // Error - CURSOR_ALREADY_EXISTS
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE c1 CURSOR FOR SELECT 1;
  DECLARE c1 CURSOR FOR SELECT 2;
END;""")
      },
      condition = "CURSOR_ALREADY_EXISTS",
      parameters = Map("cursorName" -> "`c1`")
    )
  }

  test("Test 7: Inner scope cursors shadow outer scope cursors") {
    // Success - inner cursor shadows outer cursor with same name
    val result = sql("""BEGIN
  DECLARE y INT;
  DECLARE c1 CURSOR FOR SELECT 1 AS val;
  BEGIN
    DECLARE x INT;
    DECLARE c1 CURSOR FOR SELECT 2 AS val;
    OPEN c1;  -- Opens inner c1
    FETCH c1 INTO x;
    VALUES (x); -- Should return 2
    CLOSE c1;
  END;
  OPEN c1;  -- Opens outer c1
  FETCH c1 INTO y;
  VALUES (y); -- Should return 1
  CLOSE c1;
END;""")
    checkAnswer(result, Seq(
      Row(1)))
  }

  test("Test 8: A cursor cannot be opened twice") {
    // Error - CURSOR_ALREADY_OPEN
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE c1 CURSOR FOR SELECT 1;
  OPEN c1;
  OPEN c1; -- Should fail
END;""")
      },
      condition = "CURSOR_ALREADY_OPEN",
      parameters = Map("cursorName" -> "`c1`")
    )
  }

  test("Test 9: A cursor can be closed and then re-opened") {
    // Success
    val result = sql("""BEGIN
  DECLARE x INT;
  DECLARE c1 CURSOR FOR SELECT 1 AS val;
  OPEN c1;
  FETCH c1 INTO x;
  VALUES (x); -- Should return 1
  CLOSE c1;
  OPEN c1; -- Should succeed
  FETCH c1 INTO x;
  VALUES (x); -- Should return 1
  CLOSE c1;
END;""")
    checkAnswer(result, Seq(
      Row(1)))
  }

  test("Test 10: A cursor that is not open cannot be closed") {
    // Error - CURSOR_NOT_OPEN
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE c1 CURSOR FOR SELECT 1;
  CLOSE c1; -- Should fail
END;""")
      },
      condition = "CURSOR_NOT_OPEN",
      parameters = Map("cursorName" -> "`c1`")
    )
  }

  test("Test 11: A cursor cannot be closed twice") {
    // Error - CURSOR_NOT_OPEN
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE c1 CURSOR FOR SELECT 1;
  OPEN c1;
  CLOSE c1;
  CLOSE c1; -- Should fail
END;""")
      },
      condition = "CURSOR_NOT_OPEN",
      parameters = Map("cursorName" -> "`c1`")
    )
  }

  test("Test 12: A cursor that is not open cannot be fetched") {
    // Error - CURSOR_NOT_OPEN
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE x INT;
  DECLARE c1 CURSOR FOR SELECT 1 AS val;
  FETCH c1 INTO x; -- Should fail
END;""")
      },
      condition = "CURSOR_NOT_OPEN",
      parameters = Map("cursorName" -> "`c1`")
    )
  }

  test("Test 13: Cannot fetch after closing") {
    // Error - CURSOR_NOT_OPEN
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE x INT;
  DECLARE c1 CURSOR FOR SELECT 1 AS val;
  OPEN c1;
  FETCH c1 INTO x;
  CLOSE c1;
  FETCH c1 INTO x; -- Should fail
END;""")
      },
      condition = "CURSOR_NOT_OPEN",
      parameters = Map("cursorName" -> "`c1`")
    )
  }

  test("Test 14: Cursor is implicitly closed when it goes out of scope.") {
    // Success, return 10
    val result = sql("""BEGIN
  DECLARE step, x INT DEFAULT 0;
  REPEAT
    BEGIN
      DECLARE c1 CURSOR FOR SELECT step AS val;
      OPEN c1;
      FETCH c1 INTO x;
      SET step = step + 1;
    END;
  UNTIL step = 10 END REPEAT;
  VALUES(step);
END;""")
    checkAnswer(result, Seq(
      Row(10)))
  }

  test("Test 15: Invalid declaration order - cursor before variable") {
    // Error - variables must be declared before cursors
    checkError(
      exception = intercept[SqlScriptingException] {
        sql("""BEGIN
  DECLARE cur CURSOR FOR SELECT 123 AS val;
  DECLARE result INT;  -- Invalid: variable after cursor
END;""")
      },
      condition = "INVALID_VARIABLE_DECLARATION.ONLY_AT_BEGINNING",
      parameters = Map("varName" -> "`result`")
    )
  }

  test("Test 16: Invalid declaration order - cursor after handler") {
    // Error - cursors must be declared before handlers
    checkError(
      exception = intercept[SqlScriptingException] {
        sql("""BEGIN
  DECLARE result INT;
  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' BEGIN END;
  DECLARE cur CURSOR FOR SELECT 123 AS val;  -- Invalid: cursor after handler
END;""")
      },
      condition = "INVALID_CURSOR_DECLARATION",
      parameters = Map.empty
    )
  }

  test("Test 17: Invalid declaration - cursor after statement") {
    // Error - INVALID_CURSOR_DECLARATION - cursors must be declared before statements
    checkError(
      exception = intercept[SqlScriptingException] {
        sql("""BEGIN
  DECLARE x INT DEFAULT 0;
  SET x = 42;  -- Statement
  DECLARE cur CURSOR FOR SELECT 123 AS val;  -- Invalid: cursor after statement
END;""")
      },
      condition = "INVALID_CURSOR_DECLARATION",
      parameters = Map.empty
    )
  }

  test("Test 18: Cursor implicitly closed when exiting DECLARE scope (not ...") {
    // Success - cursor declared in outer scope, opened in inner scope,
    val result = sql("""BEGIN
  outer: BEGIN
    DECLARE x INT;
    DECLARE cur CURSOR FOR SELECT 42 AS val;

    inner_block: BEGIN
      OPEN cur;  -- Open in inner scope
      FETCH cur INTO x;
      -- Cursor remains open when exiting inner scope
    END;

    -- Cursor should still be open here (we're still in outer scope where it was declared)
    FETCH cur INTO x;  -- This should succeed
    VALUES (x);  -- Should return 42

    -- Cursor will be implicitly closed when exiting outer scope
  END;
END;""")
    checkAnswer(result, Seq(
      Row(42)))
  }

  test("Test 19: Verify cursor closed when exiting DECLARE scope, not OPEN...") {
    // Error - cursor not open after exiting the scope where it was declared
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE y INT;

  scope1: BEGIN
    DECLARE cur CURSOR FOR SELECT 99 AS val;
    OPEN cur;
    FETCH cur INTO y;
  END;  -- cursor is implicitly closed here (exiting DECLARE scope)

  -- This should fail because cursor no longer exists (declared in scope1)
  FETCH cur INTO y;
END;""")
      },
      condition = "CURSOR_NOT_FOUND",
      parameters = Map("cursorName" -> "cur")
    )
  }

  test("Test 20: Cursor sensitivity - INSENSITIVE cursors capture snapshot at OPEN") {
    // Spark cursors are INSENSITIVE by default, which means they capture a snapshot when
    // OPEN is called. The physical plan is generated at OPEN time, locking in file lists,
    // Delta snapshots, and other data source metadata.
    //
    // This test verifies snapshot semantics: rows inserted AFTER OPEN but BEFORE first
    // FETCH should NOT be visible to the cursor.
    sql("CREATE TABLE cursor_sensitivity_test (id INT, value STRING) USING parquet;")
    sql("INSERT INTO cursor_sensitivity_test VALUES (1, 'row1'), (2, 'row2');")
    val result = sql("""BEGIN
  -- Declare all variables first
  DECLARE fetched_id INT;
  DECLARE fetched_value STRING;
  DECLARE row_count_first_open INT DEFAULT 0;
  DECLARE row_count_second_open INT DEFAULT 0;
  DECLARE nomorerows BOOLEAN DEFAULT false;

  -- Step 2: Declare cursor
  DECLARE cur CURSOR FOR SELECT id, value FROM cursor_sensitivity_test ORDER BY id;

  -- Declare handler
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET nomorerows = true;

  -- Step 3: Add more rows before OPEN
  INSERT INTO cursor_sensitivity_test VALUES (3, 'row3'), (4, 'row4');

  -- Step 4: OPEN the cursor (captures snapshot - should see rows 1-4)
  OPEN cur;

  -- Step 5: Add more rows after OPEN but before first FETCH
  INSERT INTO cursor_sensitivity_test VALUES (5, 'row5'), (6, 'row6');

  -- Step 6: Fetch rows - should see snapshot from OPEN time (4 rows only)
  REPEAT
    FETCH cur INTO fetched_id, fetched_value;
    IF NOT nomorerows THEN
      SET row_count_first_open = row_count_first_open + 1;
    END IF;
  UNTIL nomorerows END REPEAT;

  -- Step 7: Close the cursor
  CLOSE cur;

  -- Step 8: Open the cursor again (captures new snapshot with all 6 rows)
  SET nomorerows = false;
  OPEN cur;

  -- Step 9: Fetch rows - should see all 6 rows now
  REPEAT
    FETCH cur INTO fetched_id, fetched_value;
    IF NOT nomorerows THEN
      SET row_count_second_open = row_count_second_open + 1;
    END IF;
  UNTIL nomorerows END REPEAT;

  -- Return both counts (first=4, second=6)
  VALUES (row_count_first_open, row_count_second_open);

  CLOSE cur;
END;""")
    checkAnswer(result, Seq(
      Row(4, 6)))
  }

  test("Test 21: Basic parameterized cursor with positional parameters") {
    val result = sql("""BEGIN
  DECLARE min_id INT DEFAULT 2;
  DECLARE max_id INT DEFAULT 4;
  DECLARE fetched_id INT;
  DECLARE fetched_value STRING;
  DECLARE nomorerows BOOLEAN DEFAULT false;
  DECLARE result STRING DEFAULT '';
  DECLARE cur CURSOR FOR SELECT id, value FROM VALUES(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e') AS t(id, value) WHERE id >= ? AND id <= ?;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET nomorerows = true;

  OPEN cur USING min_id, max_id;

  REPEAT
    FETCH cur INTO fetched_id, fetched_value;
    IF NOT nomorerows THEN
      SET result = result || fetched_value;
    END IF;
  UNTIL nomorerows END REPEAT;

  CLOSE cur;
  VALUES (result);
END;""")
    checkAnswer(result, Seq(
      Row("bcd")))
  }

  test("Test 22: Parameterized cursor with named parameters") {
    val result = sql("""BEGIN
  DECLARE search_value STRING DEFAULT 'c';
  DECLARE fetched_id INT;
  DECLARE fetched_value STRING;
  DECLARE nomorerows BOOLEAN DEFAULT false;
  DECLARE id_sum INT DEFAULT 0;
  DECLARE cur CURSOR FOR SELECT id, value FROM VALUES(1, 'a'), (2, 'b'), (3, 'c'), (4, 'c'), (5, 'e') AS t(id, value) WHERE value = :search_val;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET nomorerows = true;

  OPEN cur USING search_value AS search_val;

  REPEAT
    FETCH cur INTO fetched_id, fetched_value;
    IF NOT nomorerows THEN
      SET id_sum = id_sum + fetched_id;
    END IF;
  UNTIL nomorerows END REPEAT;

  CLOSE cur;
  VALUES (id_sum);
END;""")
    checkAnswer(result, Seq(
      Row(7)))
  }

  test("Test 23: Parameterized cursor - reopen with different parameters") {
    val result = sql("""BEGIN
  DECLARE fetched_id INT;
  DECLARE nomorerows BOOLEAN DEFAULT false;
  DECLARE count1 INT DEFAULT 0;
  DECLARE count2 INT DEFAULT 0;
  DECLARE cur CURSOR FOR SELECT id FROM VALUES(1), (2), (3), (4), (5) AS t(id) WHERE id >= ? AND id <= ?;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET nomorerows = true;

  -- First open with parameters 2, 3 (should get 2 rows)
  OPEN cur USING 2, 3;
  REPEAT
    FETCH cur INTO fetched_id;
    IF NOT nomorerows THEN
      SET count1 = count1 + 1;
    END IF;
  UNTIL nomorerows END REPEAT;
  CLOSE cur;

  -- Reopen with different parameters 1, 5 (should get 5 rows)
  SET nomorerows = false;
  OPEN cur USING 1, 5;
  REPEAT
    FETCH cur INTO fetched_id;
    IF NOT nomorerows THEN
      SET count2 = count2 + 1;
    END IF;
  UNTIL nomorerows END REPEAT;
  CLOSE cur;

  VALUES (count1, count2);
END;""")
    checkAnswer(result, Seq(
      Row(2, 5)))
  }

  test("Test 24: Parameterized cursor with expressions") {
    val result = sql("""BEGIN
  DECLARE base INT DEFAULT 10;
  DECLARE fetched_id INT;
  DECLARE nomorerows BOOLEAN DEFAULT false;
  DECLARE sum INT DEFAULT 0;
  DECLARE cur CURSOR FOR SELECT id FROM VALUES(5), (10), (15), (20), (25) AS t(id) WHERE id > ?;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET nomorerows = true;

  -- Use expression as parameter
  OPEN cur USING base + 5;

  REPEAT
    FETCH cur INTO fetched_id;
    IF NOT nomorerows THEN
      SET sum = sum + fetched_id;
    END IF;
  UNTIL nomorerows END REPEAT;

  CLOSE cur;
  VALUES (sum); -- Should be 20 + 25 = 45
END;""")
    checkAnswer(result, Seq(
      Row(45)))
  }

  test("Test 25: USING clause - various data types (matching EXECUTE IMMED...") {
    // Success - test that USING clause handles various types correctly
    val result = sql("""BEGIN
  DECLARE int_val INT;
  DECLARE str_val STRING;
  DECLARE str_val2 STRING;
  DECLARE date_val DATE;
  DECLARE bool_val BOOLEAN;

  -- Declare all cursors upfront (SQL/PSM declaration order)
  DECLARE cur_int CURSOR FOR SELECT typeof(:p) as type, :p as val;
  DECLARE cur_str CURSOR FOR SELECT typeof(:p) as type, :p as val;
  DECLARE cur_date CURSOR FOR SELECT typeof(:p) as type, :p as val;
  DECLARE cur_bool CURSOR FOR SELECT typeof(:p) as type, :p as val;

  -- Integer type
  OPEN cur_int USING 42 AS p;
  FETCH cur_int INTO str_val, int_val;
  CLOSE cur_int;
  VALUES ('INT', int_val);

  -- String type
  OPEN cur_str USING 'hello' AS p;
  FETCH cur_str INTO str_val, str_val2;
  CLOSE cur_str;
  VALUES ('STRING', str_val2);

  -- Date type
  OPEN cur_date USING DATE '2023-12-25' AS p;
  FETCH cur_date INTO str_val, date_val;
  CLOSE cur_date;
  VALUES ('DATE', date_val);

  -- Boolean type
  OPEN cur_bool USING true AS p;
  FETCH cur_bool INTO str_val, bool_val;
  CLOSE cur_bool;
  VALUES ('BOOLEAN', bool_val);
END;""")
    checkAnswer(result, Seq(
      Row("BOOLEAN", true)))
  }

  test("Test 26: USING clause - positional vs named parameters") {
    // Success - verify positional and named parameter binding work correctly
    val result = sql("""BEGIN
  DECLARE result INT;
  DECLARE x INT DEFAULT 100;

  -- Declare all cursors upfront (SQL/PSM declaration order)
  DECLARE cur_pos CURSOR FOR SELECT ? + ? AS sum;
  DECLARE cur_named CURSOR FOR SELECT :a + :b AS sum;
  DECLARE cur_var CURSOR FOR SELECT ? + 1 AS val;

  -- Positional parameters (no aliases)
  OPEN cur_pos USING 10, 20;
  FETCH cur_pos INTO result;
  CLOSE cur_pos;
  VALUES ('positional', result); -- Should be 30

  -- Named parameters (with aliases)
  OPEN cur_named USING 15 AS a, 25 AS b;
  FETCH cur_named INTO result;
  CLOSE cur_named;
  VALUES ('named', result); -- Should be 40

  -- Mixed: variable reference without alias (positional)
  OPEN cur_var USING x;
  FETCH cur_var INTO result;
  CLOSE cur_var;
  VALUES ('variable', result); -- Should be 101
END;""")
    checkAnswer(result, Seq(
      Row("variable", 101)))
  }

  test("Test 27: USING clause - expressions (matching EXECUTE IMMEDIATE)") {
    // Success - test constant expressions in USING clause
    val result = sql("""BEGIN
  DECLARE result INT;
  DECLARE base INT DEFAULT 5;

  -- Declare all cursors upfront (SQL/PSM declaration order)
  DECLARE cur1 CURSOR FOR SELECT :p AS val;
  DECLARE cur2 CURSOR FOR SELECT :p AS val;

  -- Arithmetic expression
  OPEN cur1 USING 5 + 10 AS p;
  FETCH cur1 INTO result;
  CLOSE cur1;
  VALUES ('arithmetic', result); -- Should be 15

  -- Variable reference with expression
  OPEN cur2 USING base * 2 AS p;
  FETCH cur2 INTO result;
  CLOSE cur2;
  VALUES ('variable_expr', result); -- Should be 10
END;""")
    checkAnswer(result, Seq(
      Row("variable_expr", 10)))
  }

  test("Test 28: USING clause with MORE expressions than needed (allowed -...") {
    // Success - extra parameters are allowed and ignored
    val result = sql("""BEGIN
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT ? + 10 AS val;

  OPEN cur USING 5, 99, 100;  -- Only first parameter (5) is used, others ignored
  FETCH cur INTO result;
  CLOSE cur;

  VALUES (result);  -- Should return 15 (5 + 10)
END;""")
    checkAnswer(result, Seq(
      Row(15)))
  }

  test("Test 29: USING clause with TOO FEW expressions for positional para...") {
    // Error - UNBOUND_SQL_PARAMETER
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT ? + ? AS val;

  OPEN cur USING 10;  -- Only 1 parameter provided, but 2 needed
  FETCH cur INTO result;
  CLOSE cur;
END;""")
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "_11"),
      queryContext = Array(ExpectedContext("?"))
    )
  }

  test("Test 30: USING clause missing a named parameter") {
    // Error - UNRESOLVED_COLUMN (missing parameter 'y')
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT :x + :y AS val;

  OPEN cur USING 42 AS x;  -- Missing parameter 'y'
  FETCH cur INTO result;
  CLOSE cur;
END;""")
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "y"),
      queryContext = Array(ExpectedContext(":y"))
    )
  }

  test("Test 31: USING clause with named parameters but wrong name") {
    // Error - UNBOUND_SQL_PARAMETER
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT :alpha + :beta AS val;

  OPEN cur USING (5 AS alpha, 10 AS gamma);  -- 'gamma' provided but 'beta' expected
  FETCH cur INTO result;
  CLOSE cur;
END;""")
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "beta"),
      queryContext = Array(ExpectedContext(":beta"))
    )
  }

  test("Test 32: DECLARE mixes named and unnamed parameter markers") {
    // Error - UNRECOGNIZED_SQL_TYPE (due to invalid parameter marker syntax)
    checkError(
      exception = intercept[ParseException] {
        sql("""BEGIN
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT ? + :named AS val;  -- Mixed positional and named

  OPEN cur USING 10, (20 AS named);
  FETCH cur INTO result;
  CLOSE cur;
END;""")
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "')'", "hint" -> "")
    )
  }

  test("Test 33: Local variable with same name as named parameter (implici...") {
    // Success - variable name becomes the parameter name implicitly
    val result = sql("""BEGIN
  DECLARE x INT DEFAULT 100;
  DECLARE y INT DEFAULT 200;
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT :x + :y AS val;

  -- Variable names match parameter names, so no explicit alias needed
  OPEN cur USING (x AS x, y AS y);
  FETCH cur INTO result;
  CLOSE cur;

  VALUES (result);  -- Should return 300 (100 + 200)
END;""")
    checkAnswer(result, Seq(
      Row(300)))
  }

  test("Test 34: Named parameters with expressions (not just variable refe...") {
    // Success - expressions can be used with explicit aliases
    val result = sql("""BEGIN
  DECLARE base INT DEFAULT 10;
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT :a * :b AS val;

  OPEN cur USING base * 2 AS a, base + 5 AS b;  -- Expressions with aliases
  FETCH cur INTO result;
  CLOSE cur;

  VALUES (result);  -- Should return 300 (20 * 15)
END;""")
    checkAnswer(result, Seq(
      Row(300)))
  }

  test("Test 35: Positional parameters with complex expressions") {
    // Success - expressions work for positional parameters
    val result = sql("""BEGIN
  DECLARE x INT DEFAULT 5;
  DECLARE y INT DEFAULT 3;
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT ? * ? + ? AS val;

  OPEN cur USING x * 2, y + 1, 10;  -- (5*2) * (3+1) + 10 = 10 * 4 + 10 = 50
  FETCH cur INTO result;
  CLOSE cur;

  VALUES (result);  -- Should return 50
END;""")
    checkAnswer(result, Seq(
      Row(50)))
  }

  test("Test 36: Reopen cursor with different parameter values (positional)") {
    // Success - cursor can be reopened with different positional parameters
    val result = sql("""BEGIN
  DECLARE result1 INT;
  DECLARE result2 INT;
  DECLARE cur CURSOR FOR SELECT ? * 10 AS val;

  -- First open with parameter 5
  OPEN cur USING 5;
  FETCH cur INTO result1;
  CLOSE cur;

  -- Reopen with parameter 8
  OPEN cur USING 8;
  FETCH cur INTO result2;
  CLOSE cur;

  VALUES (result1, result2);  -- Should return (50, 80)
END;""")
    checkAnswer(result, Seq(
      Row(50, 80)))
  }

  test("Test 37: Reopen cursor with different parameter values (named)") {
    // Success - cursor can be reopened with different named parameters
    val result = sql("""BEGIN
  DECLARE result1 INT;
  DECLARE result2 INT;
  DECLARE cur CURSOR FOR SELECT :factor * 10 AS val;

  -- First open with factor = 3
  OPEN cur USING (3 AS factor);
  FETCH cur INTO result1;
  CLOSE cur;

  -- Reopen with factor = 7
  OPEN cur USING (7 AS factor);
  FETCH cur INTO result2;
  CLOSE cur;

  VALUES (result1, result2);  -- Should return (30, 70)
END;""")
    checkAnswer(result, Seq(
      Row(30, 70)))
  }

  test("Test 38: ALL_PARAMETERS_MUST_BE_NAMED - Mix of named and positiona...") {
    // Error - ALL_PARAMETERS_MUST_BE_NAMED
    checkError(
      exception = intercept[ParseException] {
        sql("""BEGIN
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT :x + :y AS val;

  -- USING clause mixes positional (10) and named (y AS y)
  OPEN cur USING 10, (20 AS y);
  FETCH cur INTO result;
  CLOSE cur;
END;""")
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "')'", "hint" -> "")
    )
  }

  test("Test 39: Verify variable name inference for parameters (without ex...") {
    // Success - variable names inferred from identifiers in USING clause
    val result = sql("""BEGIN
  DECLARE multiplier INT DEFAULT 4;
  DECLARE addend INT DEFAULT 6;
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT :multiplier * 10 + :addend AS val;

  -- Variable names match parameter names, implicit binding
  OPEN cur USING multiplier AS multiplier, addend AS addend;
  FETCH cur INTO result;
  CLOSE cur;

  VALUES (result);  -- Should return 46 (4 * 10 + 6)
END;""")
    checkAnswer(result, Seq(
      Row(46)))
  }

  test("Test 40: Label-qualified cursor - basic case") {
    // Success - cursor qualified with label
    val result = sql("""BEGIN
  outer: BEGIN
    DECLARE x INT;
    DECLARE c1 CURSOR FOR SELECT 42 AS val;
    OPEN outer.c1;
    FETCH outer.c1 INTO x;
    VALUES (x); -- Should return 42
    CLOSE outer.c1;
  END;
END;""")
    checkAnswer(result, Seq(
      Row(42)))
  }

  test("Test 41: Label-qualified cursor - nested scopes") {
    // Success - inner and outer cursors with same name, qualified access
    val result = sql("""BEGIN
  outer_lbl: BEGIN
    DECLARE x, y INT;
    DECLARE cur CURSOR FOR SELECT 1 AS val;

    inner_lbl: BEGIN
      DECLARE cur CURSOR FOR SELECT 2 AS val;

      -- Open both cursors
      OPEN outer_lbl.cur;  -- Opens outer cursor
      OPEN inner_lbl.cur;  -- Opens inner cursor

      -- Fetch from inner cursor (unqualified reference in inner scope)
      FETCH cur INTO x;

      -- Fetch from outer cursor (qualified reference)
      FETCH outer_lbl.cur INTO y;

      CLOSE inner_lbl.cur;
    END;

    CLOSE outer_lbl.cur;

    -- Return both values: x should be 2 (from inner), y should be 1 (from outer)
    VALUES (x, y);
  END;
END;""")
    checkAnswer(result, Seq(
      Row(2, 1)))
  }

  test("Test 42: Label-qualified cursor with parameterized query") {
    // Success - qualified cursor with parameters
    val result = sql("""BEGIN
  lbl: BEGIN
    DECLARE min_val INT DEFAULT 3;
    DECLARE max_val INT DEFAULT 4;
    DECLARE fetched_id INT;
    DECLARE result STRING DEFAULT '';
    DECLARE cur CURSOR FOR SELECT id FROM VALUES(1), (2), (3), (4), (5) AS t(id) WHERE id >= ? AND id <= ?;

    OPEN lbl.cur USING min_val, max_val;

    FETCH lbl.cur INTO fetched_id;
    SET result = result || CAST(fetched_id AS STRING);
    FETCH lbl.cur INTO fetched_id;
    SET result = result || CAST(fetched_id AS STRING);

    CLOSE lbl.cur;
    VALUES (result); -- Should be '34'
  END;
END;""")
    checkAnswer(result, Seq(
      Row("34")))
  }

  test("Test 43: Case insensitivity - DECLARE lowercase, OPEN uppercase") {
    // Success - cursor names should be case-insensitive by default
    val result = sql("""BEGIN
  DECLARE result INT;
  DECLARE my_cursor CURSOR FOR SELECT 42 AS val;
  OPEN MY_CURSOR;
  FETCH MY_CURSOR INTO result;
  CLOSE MY_CURSOR;
  VALUES (result);  -- Should return 42
END;""")
    checkAnswer(result, Seq(
      Row(42)))
  }

  test("Test 44: Case insensitivity - DECLARE MixedCase, OPEN lowercase") {
    // Success - cursor names should be case-insensitive by default
    val result = sql("""BEGIN
  DECLARE result INT;
  DECLARE MyCursor CURSOR FOR SELECT 99 AS val;
  OPEN mycursor;
  FETCH mycursor INTO result;
  CLOSE mycursor;
  VALUES (result);  -- Should return 99
END;""")
    checkAnswer(result, Seq(
      Row(99)))
  }

  test("Test 45: Case insensitivity - Label-qualified cursor with differen...") {
    // Success - both label and cursor name should be case-insensitive
    val result = sql("""BEGIN
  outer_lbl: BEGIN
    DECLARE result INT;
    DECLARE cur CURSOR FOR SELECT 123 AS val;

    OPEN OUTER_LBL.cur;  -- Label in different case
    FETCH OUTER_LBL.CUR INTO result;  -- Both in different case
    CLOSE outer_lbl.CUR;  -- Cursor in different case

    VALUES (result);  -- Should return 123
  END;
END;""")
    checkAnswer(result, Seq(
      Row(123)))
  }

  test("Test 46: Case insensitivity with IDENTIFIER() - resolution still c...") {
    // Success - IDENTIFIER() preserves literal but resolution uses caseSensitiveAnalysis
    val result = sql("""BEGIN
  DECLARE result INT;
  DECLARE IDENTIFIER('MyCase') CURSOR FOR SELECT 42;
  OPEN IDENTIFIER('mycase');  -- Different case but should work (case-insensitive resolution)
  FETCH IDENTIFIER('MYCASE') INTO result;  -- Another case variation
  CLOSE IDENTIFIER('MyCaSe');  -- Yet another variation
  VALUES (result);  -- Should return 42
END;""")
    checkAnswer(result, Seq(
      Row(42)))
  }

  test("Test 47: FETCH INTO with duplicate variable names") {
    // Error - DUPLICATE_ASSIGNMENTS
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE x, y INT;
  DECLARE cur CURSOR FOR SELECT 1 AS a, 2 AS b;
  OPEN cur;
  FETCH cur INTO x, x;  -- Should fail - duplicate variable
END;""")
      },
      condition = "DUPLICATE_ASSIGNMENTS",
      parameters = Map("nameList" -> "`x`")
    )
  }

  test("Test 48: FETCH INTO with type casting (store assignment)") {
    // Success - values should be cast according to ANSI store assignment rules
    val result = sql("""BEGIN
  DECLARE int_var INT;
  DECLARE str_var STRING;
  DECLARE cur CURSOR FOR SELECT 100.7 AS double_val, 42 AS int_val;

  OPEN cur;
  FETCH cur INTO int_var, str_var;  -- double->int cast, int->string cast
  CLOSE cur;

  VALUES (int_var, str_var);  -- Should be (100, '42') with ANSI rounding
END;""")
    checkAnswer(result, Seq(
      Row(100, "42")))
  }

  test("Test 49: FETCH INTO with arity mismatch - too few variables") {
    // Error - ASSIGNMENT_ARITY_MISMATCH
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE x INT;
  DECLARE cur CURSOR FOR SELECT 1, 2, 3;
  OPEN cur;
  FETCH cur INTO x;  -- Should fail - 1 target but 3 columns
END;""")
      },
      condition = "ASSIGNMENT_ARITY_MISMATCH",
      parameters = Map("numExpr" -> "3", "numTarget" -> "1")
    )
  }

  test("Test 50: FETCH INTO with arity mismatch - too many variables") {
    // Error - ASSIGNMENT_ARITY_MISMATCH
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE x, y, z, w INT;
  DECLARE cur CURSOR FOR SELECT 1, 2;
  OPEN cur;
  FETCH cur INTO x, y, z, w;  -- Should fail - 4 targets but 2 columns
END;""")
      },
      condition = "ASSIGNMENT_ARITY_MISMATCH",
      parameters = Map("numExpr" -> "2", "numTarget" -> "4")
    )
  }

  test("Test 51: FETCH INTO single STRUCT variable - basic case") {
    // Success - SQL Standard allows multi-column fetch into single struct
    val result = sql("""BEGIN
  DECLARE person_record STRUCT<name STRING, age INT>;
  DECLARE cur CURSOR FOR SELECT 'Alice' AS name, 30 AS age;
  OPEN cur;
  FETCH cur INTO person_record;
  CLOSE cur;
  VALUES (person_record.name, person_record.age); -- Should return 'Alice', 30
END;""")
    checkAnswer(result, Seq(
      Row("Alice", 30)))
  }

  test("Test 52: FETCH INTO STRUCT with type casting") {
    // Success - ANSI casting should apply to struct fields
    val result = sql("""BEGIN
  DECLARE record_var STRUCT<id INT, value STRING>;
  DECLARE cur CURSOR FOR SELECT 42.7 AS id, 100 AS value;
  OPEN cur;
  FETCH cur INTO record_var;
  CLOSE cur;
  VALUES (record_var.id, record_var.value); -- Should return 42, '100' (with casting)
END;""")
    checkAnswer(result, Seq(
      Row(42, "100")))
  }

  test("Test 53: FETCH INTO STRUCT - field count mismatch") {
    // Error - ASSIGNMENT_ARITY_MISMATCH
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE record_var STRUCT<a INT, b INT>;
  DECLARE cur CURSOR FOR SELECT 1, 2, 3;
  OPEN cur;
  FETCH cur INTO record_var;  -- Should fail - 2 struct fields but 3 cursor columns
END;""")
      },
      condition = "ASSIGNMENT_ARITY_MISMATCH",
      parameters = Map("numExpr" -> "3", "numTarget" -> "2")
    )
  }

  test("Test 54: FETCH INTO non-STRUCT single variable with multiple columns") {
    // Error - ASSIGNMENT_ARITY_MISMATCH (not a struct, so arity must match)
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE x INT;
  DECLARE cur CURSOR FOR SELECT 1, 2;
  OPEN cur;
  FETCH cur INTO x;  -- Should fail - single non-struct variable but 2 columns
END;""")
      },
      condition = "ASSIGNMENT_ARITY_MISMATCH",
      parameters = Map("numExpr" -> "2", "numTarget" -> "1")
    )
  }

  test("Test 55: FETCH INTO STRUCT with complex types") {
    // Success - Struct with mixed types
    val result = sql("""BEGIN
  DECLARE complex_record STRUCT<id BIGINT, name STRING, value DOUBLE>;
  DECLARE cur CURSOR FOR SELECT 100 AS id, 'test' AS name, 99.5 AS value;
  OPEN cur;
  FETCH cur INTO complex_record;
  CLOSE cur;
  VALUES (complex_record); -- Should return struct(100, 'test', 99.5)
END;""")
    checkAnswer(result, Seq(
      Row(Row(100L, "test", 99.5))))
  }

  test("Test 56: FETCH INTO with session variables") {
    // Success - session variables work with FETCH INTO
    sql("DECLARE session_x INT DEFAULT 0;")
    sql("DECLARE session_y STRING DEFAULT '';")
    val result = sql("""BEGIN
  DECLARE cur CURSOR FOR SELECT 42 AS num, 'hello' AS text;
  OPEN cur;
  FETCH cur INTO session_x, session_y;
  CLOSE cur;
END;""")
    checkAnswer(result, Seq.empty[Row])
  }

  test("Test 57: FETCH INTO mixing local and session variables") {
    // Success - can mix local and session variables in FETCH INTO
    sql("DECLARE session_var INT DEFAULT 0;")
    val result = sql("""BEGIN
  DECLARE local_var STRING;
  DECLARE cur CURSOR FOR SELECT 100 AS a, 'world' AS b;
  OPEN cur;
  FETCH cur INTO session_var, local_var;
  CLOSE cur;
  VALUES (session_var, local_var);  -- Should return 100, 'world'
END;""")
    checkAnswer(result, Seq(
      Row(100, "world")))
  }

  test("Test 58: FETCH INTO session variables with type casting") {
    // Success - ANSI store assignment rules apply to session variables
    sql("DECLARE session_int INT DEFAULT 0;")
    sql("DECLARE session_str STRING DEFAULT '';")
    val result = sql("""BEGIN
  DECLARE cur CURSOR FOR SELECT 99.9 AS double_val, 42 AS int_val;
  OPEN cur;
  FETCH cur INTO session_int, session_str;  -- double->int cast, int->string cast
  CLOSE cur;
END;""")
    checkAnswer(result, Seq.empty[Row])
  }

  test("Test 59: FETCH INTO mixing local and session with duplicate sessio...") {
    // Error - DUPLICATE_ASSIGNMENTS (applies to session variables too)
    sql("DECLARE session_dup INT DEFAULT 0;")
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE cur CURSOR FOR SELECT 1, 2;
  OPEN cur;
  FETCH cur INTO session_dup, session_dup;  -- Should fail - duplicate session variable
END;""")
      },
      condition = "DUPLICATE_ASSIGNMENTS",
      parameters = Map("nameList" -> "`session_dup`")
    )
  }

  test("Test 60: FETCH INTO mixing with duplicate across local and session") {
    // Error - DUPLICATE_ASSIGNMENTS (same variable name in local and session scope)
    sql("DECLARE dup_var INT DEFAULT 0;")
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE dup_var INT;
  DECLARE cur CURSOR FOR SELECT 1, 2;
  OPEN cur;
  FETCH cur INTO dup_var, dup_var;  -- Should fail - duplicate variable name
END;""")
      },
      condition = "DUPLICATE_ASSIGNMENTS",
      parameters = Map("nameList" -> "`dup_var`")
    )
  }

  test("Test 61: DECLARE CURSOR with INSENSITIVE keyword") {
    // Success - INSENSITIVE is a valid optional keyword
    val result = sql("""BEGIN
  DECLARE x INT;
  DECLARE cur INSENSITIVE CURSOR FOR SELECT 42 AS val;
  OPEN cur;
  FETCH cur INTO x;
  CLOSE cur;
  VALUES (x); -- Should return 42
END;""")
    checkAnswer(result, Seq(
      Row(42)))
  }

  test("Test 62: DECLARE CURSOR with ASENSITIVE keyword") {
    // Success - ASENSITIVE is a valid optional keyword
    val result = sql("""BEGIN
  DECLARE y INT;
  DECLARE cur ASENSITIVE CURSOR FOR SELECT 99 AS val;
  OPEN cur;
  FETCH cur INTO y;
  CLOSE cur;
  VALUES (y); -- Should return 99
END;""")
    checkAnswer(result, Seq(
      Row(99)))
  }

  test("Test 63: DECLARE CURSOR with FOR READ ONLY clause") {
    // Success - FOR READ ONLY is a valid optional clause
    val result = sql("""BEGIN
  DECLARE z INT;
  DECLARE cur CURSOR FOR SELECT 77 AS val FOR READ ONLY;
  OPEN cur;
  FETCH cur INTO z;
  CLOSE cur;
  VALUES (z); -- Should return 77
END;""")
    checkAnswer(result, Seq(
      Row(77)))
  }

  test("Test 64: DECLARE CURSOR with all optional keywords") {
    // Success - Combination of INSENSITIVE and FOR READ ONLY
    val result = sql("""BEGIN
  DECLARE w INT;
  DECLARE cur INSENSITIVE CURSOR FOR SELECT 123 AS val FOR READ ONLY;
  OPEN cur;
  FETCH cur INTO w;
  CLOSE cur;
  VALUES (w); -- Should return 123
END;""")
    checkAnswer(result, Seq(
      Row(123)))
  }

  test("Test 65: FETCH with NEXT FROM keywords") {
    // Success - NEXT FROM is a valid optional keyword combination
    val result = sql("""BEGIN
  DECLARE a INT;
  DECLARE cur CURSOR FOR SELECT 55 AS val;
  OPEN cur;
  FETCH NEXT FROM cur INTO a;
  CLOSE cur;
  VALUES (a); -- Should return 55
END;""")
    checkAnswer(result, Seq(
      Row(55)))
  }

  test("Test 66: FETCH with FROM keyword") {
    // Success - FROM is a valid optional keyword
    val result = sql("""BEGIN
  DECLARE b INT;
  DECLARE cur CURSOR FOR SELECT 66 AS val;
  OPEN cur;
  FETCH FROM cur INTO b;
  CLOSE cur;
  VALUES (b); -- Should return 66
END;""")
    checkAnswer(result, Seq(
      Row(66)))
  }

  test("Test 67: FETCH with NEXT FROM keywords") {
    // Success - NEXT FROM is a valid optional keyword combination
    val result = sql("""BEGIN
  DECLARE c INT;
  DECLARE cur CURSOR FOR SELECT 88 AS val;
  OPEN cur;
  FETCH NEXT FROM cur INTO c;
  CLOSE cur;
  VALUES (c); -- Should return 88
END;""")
    checkAnswer(result, Seq(
      Row(88)))
  }

  test("Test 68: IDENTIFIER() clause for cursor names - basic case") {
    // Success - cursor name specified using IDENTIFIER()
    val result = sql("""BEGIN
  DECLARE x INT;
  DECLARE IDENTIFIER('my_cursor') CURSOR FOR SELECT 99 AS val;
  OPEN IDENTIFIER('my_cursor');
  FETCH IDENTIFIER('my_cursor') INTO x;
  CLOSE IDENTIFIER('my_cursor');
  VALUES (x); -- Should return 99
END;""")
    checkAnswer(result, Seq(
      Row(99)))
  }

  test("Test 69: IDENTIFIER() clause for cursor names - preserves literal ...") {
    // Success - IDENTIFIER() preserves the literal, but resolution is still case-insensitive
    val result = sql("""BEGIN
  DECLARE result INT;
  DECLARE IDENTIFIER('MixedCase') CURSOR FOR SELECT 42;
  OPEN IDENTIFIER('MixedCase');
  FETCH IDENTIFIER('MixedCase') INTO result;
  CLOSE IDENTIFIER('MixedCase');
  VALUES (result); -- Should return 42
END;""")
    checkAnswer(result, Seq(
      Row(42)))
  }

  test("Test 70: IDENTIFIER() clause in FETCH INTO statement") {
    // Success - IDENTIFIER() works for variable names in FETCH INTO
    val result = sql("""BEGIN
  DECLARE IDENTIFIER('my_result') INT;
  DECLARE my_cursor CURSOR FOR SELECT 123 AS val;
  OPEN my_cursor;
  FETCH my_cursor INTO IDENTIFIER('my_result');
  CLOSE my_cursor;
  VALUES (IDENTIFIER('my_result')); -- Should return 123
END;""")
    checkAnswer(result, Seq(
      Row(123)))
  }

  test("Test 71: IDENTIFIER() in FETCH INTO with multiple variables") {
    // Success - IDENTIFIER() works with multiple target variables
    val result = sql("""BEGIN
  DECLARE IDENTIFIER('val1') INT;
  DECLARE IDENTIFIER('val2') STRING;
  DECLARE cur CURSOR FOR SELECT 42, 'test';
  OPEN cur;
  FETCH cur INTO IDENTIFIER('val1'), IDENTIFIER('val2');
  CLOSE cur;
  VALUES (IDENTIFIER('val1'), IDENTIFIER('val2')); -- Should return 42, 'test'
END;""")
    checkAnswer(result, Seq(
      Row(42, "test")))
  }

  test("Test 72: Complex SQL in DECLARE - Recursive CTE") {
    // Success - cursor with recursive common table expression
    val result = sql("""BEGIN
  DECLARE n INT;
  DECLARE sum_result INT DEFAULT 0;
  DECLARE done BOOLEAN DEFAULT false;
  DECLARE cur CURSOR FOR
    WITH RECURSIVE numbers(n) AS (
      SELECT 1 AS n
      UNION ALL
      SELECT n + 1 FROM numbers WHERE n < 5
    )
    SELECT n FROM numbers;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;

  OPEN cur;
  REPEAT
    FETCH cur INTO n;
    IF NOT done THEN
      SET sum_result = sum_result + n;
    END IF;
  UNTIL done END REPEAT;
  CLOSE cur;

  VALUES (sum_result); -- Should return 15 (1+2+3+4+5)
END;""")
    checkAnswer(result, Seq(
      Row(15)))
  }

  test("Test 73: Complex SQL in DECLARE - Subqueries and joins") {
    // Success - cursor with complex query
    checkError(
      exception = intercept[ParseException] {
        sql("""CREATE TEMPORARY VIEW customers AS SELECT 1 AS id, 'Alice' AS name
UNION ALL SELECT 2, 'Bob'
UNION ALL SELECT 3, 'Charlie';

CREATE TEMPORARY VIEW orders AS SELECT 1 AS customer_id, 100 AS amount
UNION ALL SELECT 1, 200
UNION ALL SELECT 2, 150;

BEGIN
  DECLARE customer_name STRING;
  DECLARE total_amount INT;
  DECLARE result STRING DEFAULT '';
  DECLARE done BOOLEAN DEFAULT false;
  DECLARE cur CURSOR FOR
    SELECT c.name, COALESCE(SUM(o.amount), 0) AS total
    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id
    WHERE c.id IN (SELECT customer_id FROM orders WHERE amount > 50)
    GROUP BY c.name
    ORDER BY total DESC;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;

  OPEN cur;
  REPEAT
    FETCH cur INTO customer_name, total_amount;
    IF NOT done THEN
      SET result = result || customer_name || ':' || CAST(total_amount AS STRING) || ',';
    END IF;
  UNTIL done END REPEAT;
  CLOSE cur;

  VALUES (result); -- Should return 'Alice:300,Bob:150,'
END;""")
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'CREATE'", "hint" -> ": extra input 'CREATE'")
    )
  }

  test("Test 74: Nested cursors (3 levels)") {
    // Success - demonstrates proper nesting and scope management
    checkError(
      exception = intercept[SqlScriptingException] {
        sql("""BEGIN
  DECLARE result STRING DEFAULT '';
  -- Level 1: Outer cursor
  DECLARE i INT;
  DECLARE done1 BOOLEAN DEFAULT false;
  DECLARE cur1 CURSOR FOR SELECT id FROM VALUES(1), (2) AS t(id);
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done1 = true;

  OPEN cur1;
  REPEAT
    FETCH cur1 INTO i;
    IF NOT done1 THEN
      -- Level 2: Middle cursor
      DECLARE j INT;
      DECLARE done2 BOOLEAN DEFAULT false;
      DECLARE cur2 CURSOR FOR SELECT id FROM VALUES(10), (20) AS t(id);
      DECLARE CONTINUE HANDLER FOR NOT FOUND SET done2 = true;

      OPEN cur2;
      REPEAT
        FETCH cur2 INTO j;
        IF NOT done2 THEN
          -- Level 3: Inner cursor
          DECLARE k INT;
          DECLARE done3 BOOLEAN DEFAULT false;
          DECLARE cur3 CURSOR FOR SELECT id FROM VALUES(100) AS t(id);
          DECLARE CONTINUE HANDLER FOR NOT FOUND SET done3 = true;

          OPEN cur3;
          REPEAT
            FETCH cur3 INTO k;
            IF NOT done3 THEN
              SET result = result || CAST(i AS STRING) || '-' ||
                          CAST(j AS STRING) || '-' ||
                          CAST(k AS STRING) || ',';
            END IF;
          UNTIL done3 END REPEAT;
          CLOSE cur3;
        END IF;
        SET done3 = false; -- Reset for next iteration
      UNTIL done2 END REPEAT;
      CLOSE cur2;
    END IF;
    SET done2 = false; -- Reset for next iteration
  UNTIL done1 END REPEAT;
  CLOSE cur1;

  VALUES (result); -- Should return '1-10-100,1-20-100,2-10-100,2-20-100,'
END;""")
      },
      condition = "INVALID_VARIABLE_DECLARATION.NOT_ALLOWED_IN_SCOPE",
      parameters = Map("varName" -> "`j`")
    )
  }

  test("Test 75: Unhandled CURSOR_NO_MORE_ROWS continues execution (SQL St...") {
    // Success - SQLSTATE '02xxx' is a completion condition (no data), continues without handler
    val result = sql("""BEGIN
  DECLARE x INT DEFAULT 0;
  DECLARE result STRING DEFAULT '';
  DECLARE cur CURSOR FOR SELECT 42 AS val;

  -- No NOT FOUND handler declared
  OPEN cur;
  FETCH cur INTO x;  -- OK: gets value 42
  SET result = result || CAST(x AS STRING);

  -- This FETCH will hit CURSOR_NO_MORE_ROWS (SQLSTATE 02000)
  -- As a completion condition (warning), it should CONTINUE execution without throwing
  FETCH cur INTO x;  -- Continues execution (no handler needed for completion conditions)

  SET result = result || '-after-fetch';
  CLOSE cur;

  VALUES (result);  -- Should return '42-after-fetch', proving execution continued
END;""")
    checkAnswer(result, Seq(
      Row("42-after-fetch")))
  }

  test("Test 76: Verify unhandled exception conditions still throw (not co...") {
    // Error - DIVIDE_BY_ZERO is an exception condition, should throw without handler
    checkError(
      exception = intercept[SparkArithmeticException] {
        sql("""BEGIN
  DECLARE x INT;
  -- No handler declared
  SET x = 1 / 0;  -- Should throw DIVIDE_BY_ZERO (SQLSTATE 22012), not continue
  VALUES ('This should not be reached');
END;""")
      },
      condition = "DIVIDE_BY_ZERO",
      parameters = Map("config" -> "\"spark.sql.ansi.enabled\""),
      queryContext = Array(ExpectedContext("1 / 0"))
    )
  }

  test("Test 77: Unhandled NO DATA condition - should continue silently pe...") {
    // Success - Script continues after CURSOR_NO_MORE_ROWS without aborting
    val result = sql("""BEGIN
  DECLARE x INT;
  DECLARE cur CURSOR FOR SELECT 1 AS val;

  OPEN cur;
  FETCH cur INTO x;  -- Succeeds, x = 1
  VALUES ('After first fetch', x);

  FETCH cur INTO x;  -- No more rows, raises CURSOR_NO_MORE_ROWS (SQLSTATE 02000)
                     -- No handler declared, so per SQL Standard this is a completion
                     -- condition (not exception) and execution continues

  VALUES ('After second fetch - script continued', x);  -- Should execute, x still = 1
  CLOSE cur;
END;""")
    checkAnswer(result, Seq(
      Row("After second fetch - script continued", 1)))
  }

  test("Test 78: CONTINUE HANDLER for specific CURSOR_NO_MORE_ROWS condition") {
    // Success - Handler catches specific error and sets flag
    val result = sql("""BEGIN
  DECLARE x INT;
  DECLARE no_more_data BOOLEAN DEFAULT false;
  DECLARE cur CURSOR FOR SELECT 10 AS val;

  -- Handler for specific error condition
  DECLARE CONTINUE HANDLER FOR CURSOR_NO_MORE_ROWS SET no_more_data = true;

  OPEN cur;
  FETCH cur INTO x;
  VALUES ('First fetch', x, no_more_data);  -- Should be (10, false)

  FETCH cur INTO x;  -- Triggers handler
  VALUES ('After no data', x, no_more_data);  -- Should be (10, true)

  CLOSE cur;
END;""")
    checkAnswer(result, Seq(
      Row("After no data", 10, true)))
  }

  test("Test 79: CONTINUE HANDLER for generic NOT FOUND class") {
    // Success - Handler catches all SQLSTATE 02xxx conditions
    val result = sql("""BEGIN
  DECLARE x INT;
  DECLARE found BOOLEAN DEFAULT true;
  DECLARE cur CURSOR FOR SELECT 20 AS val;

  -- Generic NOT FOUND handler catches all SQLSTATE class 02xxx
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET found = false;

  OPEN cur;
  FETCH cur INTO x;
  VALUES ('First fetch', x, found);  -- Should be (20, true)

  FETCH cur INTO x;  -- No more rows, triggers NOT FOUND handler
  VALUES ('After NOT FOUND', x, found);  -- Should be (20, false)

  CLOSE cur;
END;""")
    checkAnswer(result, Seq(
      Row("After NOT FOUND", 20, false)))
  }

  test("Test 80: EXIT HANDLER for NOT FOUND - exits immediately") {
    // Success - Handler executes and exits, statements after handler don't run
    val result = sql("""BEGIN
  DECLARE x INT DEFAULT 0;
  DECLARE cur CURSOR FOR SELECT 30 AS val;

  -- EXIT handler runs its body then exits the compound statement
  DECLARE EXIT HANDLER FOR NOT FOUND
  BEGIN
    VALUES ('In EXIT handler', x);
    CLOSE cur;  -- Clean up in the handler
  END;

  OPEN cur;
  FETCH cur INTO x;
  VALUES ('First fetch', x);  -- Should execute (30)

  FETCH cur INTO x;  -- Triggers EXIT handler, which closes cursor and exits

  VALUES ('After handler');  -- Should NOT execute (handler exits)
END;""")
    checkAnswer(result, Seq(
      Row("In EXIT handler", 30)))
  }

  test("Test 81: EXIT HANDLER vs CONTINUE HANDLER precedence with loops") {
    // Success - CONTINUE handler keeps loop going, EXIT would exit
    val result = sql("""BEGIN
  DECLARE x INT;
  DECLARE row_count INT DEFAULT 0;
  DECLARE done BOOLEAN DEFAULT false;
  DECLARE cur CURSOR FOR SELECT id FROM VALUES(1), (2), (3) AS t(id);

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;

  OPEN cur;

  -- Loop through all rows
  REPEAT
    FETCH cur INTO x;
    IF NOT done THEN
      SET row_count = row_count + 1;
      VALUES ('Fetched', x, row_count);
    END IF;
  UNTIL done END REPEAT;

  VALUES ('Total rows', row_count);  -- Should be 3
  CLOSE cur;
END;""")
    checkAnswer(result, Seq(
      Row("Total rows", 3)))
  }

  test("Test 82: Multiple handlers - specific condition takes precedence o...") {
    // Success - CURSOR_NO_MORE_ROWS handler runs instead of NOT FOUND
    val result = sql("""BEGIN
  DECLARE x INT;
  DECLARE specific_handler_ran BOOLEAN DEFAULT false;
  DECLARE generic_handler_ran BOOLEAN DEFAULT false;
  DECLARE cur CURSOR FOR SELECT 40 AS val;

  -- Specific error condition handler
  DECLARE CONTINUE HANDLER FOR CURSOR_NO_MORE_ROWS SET specific_handler_ran = true;

  -- Generic NOT FOUND handler (should not run for CURSOR_NO_MORE_ROWS)
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET generic_handler_ran = true;

  OPEN cur;
  FETCH cur INTO x;
  FETCH cur INTO x;  -- Should trigger specific handler, not generic

  VALUES ('Specific ran', specific_handler_ran);  -- Should be true
  VALUES ('Generic ran', generic_handler_ran);    -- Should be false

  CLOSE cur;
END;""")
    checkAnswer(result, Seq(
      Row("Generic ran", false)))
  }

  test("Test 83: Nested blocks with different NOT FOUND handlers") {
    // Success - Inner handler doesn't affect outer cursor
    val result = sql("""BEGIN
  DECLARE x INT;
  DECLARE outer_done BOOLEAN DEFAULT false;
  DECLARE outer_cur CURSOR FOR SELECT 50 AS val;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET outer_done = true;

  OPEN outer_cur;
  FETCH outer_cur INTO x;
  VALUES ('Outer fetch 1', x, outer_done);  -- Should be (50, false)

  -- Inner block with its own cursor and handler
  BEGIN
    DECLARE y INT;
    DECLARE inner_done BOOLEAN DEFAULT false;
    DECLARE inner_cur CURSOR FOR SELECT 60 AS val;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET inner_done = true;

    OPEN inner_cur;
    FETCH inner_cur INTO y;
    FETCH inner_cur INTO y;  -- Triggers inner handler only

    VALUES ('Inner', y, inner_done, outer_done);  -- Should be (60, true, false)
    CLOSE inner_cur;
  END;

  -- Outer cursor still works normally
  FETCH outer_cur INTO x;  -- Triggers outer handler
  VALUES ('Outer fetch 2', x, outer_done);  -- Should be (50, true)

  CLOSE outer_cur;
END;""")
    checkAnswer(result, Seq(
      Row("Outer fetch 2", 50, true)))
  }

  test("Test 84: Variable access in CONTINUE handler BEGIN...END (no cursors)") {
    // Handler should be able to read and write outer scope variable
    val result = sql("""BEGIN
  DECLARE x INT DEFAULT 10;

  DECLARE CONTINUE HANDLER FOR SQLSTATE '22012'
  BEGIN
    SET x = x + 1;  -- Read x and write x from outer scope
  END;

  SELECT x AS before_handler;  -- Should be 10

  -- Trigger handler
  SELECT 1 / 0;

  SELECT x AS after_handler;  -- Should be 11 (handler incremented it)
END;""")
    checkAnswer(result, Seq(
      Row(11)))
  }

  test("Test 85: CLOSE cursor in CONTINUE handler (cross-frame cursor access)") {
    // Handler closes cursor, subsequent FETCH fails
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE x INT DEFAULT 0;
  DECLARE handler_ran BOOLEAN DEFAULT false;
  DECLARE cur CURSOR FOR SELECT 99 AS val;

  -- Handler that closes cursor from outer scope
  DECLARE CONTINUE HANDLER FOR SQLSTATE '22012'
  BEGIN
    CLOSE cur;
    SET handler_ran = true;
  END;

  OPEN cur;
  FETCH cur INTO x;

  -- Trigger handler - it will close the cursor
  SELECT 1 / 0;

  -- After handler, try to fetch from closed cursor (should fail with CURSOR_NOT_OPEN)
  FETCH cur INTO x;
END;""")
      },
      condition = "CURSOR_NOT_OPEN",
      parameters = Map("cursorName" -> "`cur`")
    )
  }

  test("Test 86: FETCH and CLOSE in handler BEGIN block (cross-frame curso...") {
    // Handler fetches and closes cursor, subsequent FETCH fails
    checkError(
      exception = intercept[AnalysisException] {
        sql("""BEGIN
  DECLARE x INT DEFAULT 0;
  DECLARE y INT DEFAULT 0;
  DECLARE handler_ran BOOLEAN DEFAULT false;
  DECLARE cur CURSOR FOR SELECT 99 AS val;

  DECLARE CONTINUE HANDLER FOR SQLSTATE '22012'
  BEGIN
    FETCH cur INTO y;  -- Fetch into different variable
    CLOSE cur;
    SET handler_ran = true;
  END;

  OPEN cur;
  FETCH cur INTO x;  -- First fetch into x

  -- Trigger handler - it will fetch into y and close cursor
  SELECT 1 / 0;

  -- After handler, try to fetch from closed cursor (should fail with CURSOR_NOT_OPEN)
  FETCH cur INTO x;
END;""")
      },
      condition = "CURSOR_NOT_OPEN",
      parameters = Map("cursorName" -> "`cur`")
    )
  }

  test("Test 87: Cursor shadowing in handler - handler's cursor should sha...") {
    // Success - handler's cursor (returns 999) shadows script's cursor (returns 42)
    val result = sql("""BEGIN
  DECLARE x INT DEFAULT 0;
  DECLARE cur CURSOR FOR SELECT 42 AS val;  -- Script cursor

  DECLARE CONTINUE HANDLER FOR SQLSTATE '22012'
  BEGIN
    DECLARE cur CURSOR FOR SELECT 999 AS val;  -- Handler cursor shadows script's
    OPEN cur;
    FETCH cur INTO x;
    CLOSE cur;
  END;

  -- Trigger handler
  SELECT 1 / 0;

  VALUES (x);  -- Should return 999 (from handler's cursor, not script's)
END;""")
    checkAnswer(result, Seq(
      Row(999)))
  }

  test("Test 88: Triple nested EXIT handlers with cursors - scope resolution") {
    // Based on "local variable - handlers - triple chained handlers" test
    // This verifies that cursors can be resolved across multiple levels of EXIT handlers
    val result = sql("""BEGIN
  -- Outer script block: declare cursor cur1
  DECLARE cur1 CURSOR FOR SELECT 1 AS val1;

  l1: BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
      -- Inside first handler: declare cursor cur2 and use cur1
      DECLARE x1 INT;
      DECLARE x2 INT;
      DECLARE cur2 CURSOR FOR SELECT 2 AS val2;

      -- Should be able to access cur1 from outer scope
      OPEN cur1;
      FETCH cur1 INTO x1;

      -- Should be able to use cur2 declared in this scope
      OPEN cur2;
      FETCH cur2 INTO x2;
      CLOSE cur2;
      CLOSE cur1;

      l2: BEGIN
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
          -- Inside second handler: declare cursor cur3 and use cur1, cur2
          DECLARE y1 INT;
          DECLARE y2 INT;
          DECLARE y3 INT;
          DECLARE cur3 CURSOR FOR SELECT 3 AS val3;

          -- Should be able to access cur1 from outermost scope
          OPEN cur1;
          FETCH cur1 INTO y1;

          -- Should be able to access cur2 from parent handler scope
          OPEN cur2;
          FETCH cur2 INTO y2;

          -- Should be able to use cur3 declared in this scope
          OPEN cur3;
          FETCH cur3 INTO y3;
          CLOSE cur3;
          CLOSE cur2;
          CLOSE cur1;
        END;

        SELECT 5;
        SELECT 1/0; -- Trigger second handler
        SELECT 6;
      END;
    END;

    SELECT 7;
    SELECT 1/0; -- Trigger first handler
    SELECT 8;
  END;

  VALUES ('Done');
END;""")
    // EXIT handlers execute and exit - no intermediate results are returned
    // The test succeeds if all cursor operations complete without errors
    checkAnswer(result, Seq(Row("Done")))
  }

  test("Test 92: Runtime error in cursor query caught at OPEN (snapshot semantics)") {
    // With snapshot semantics, executeToIterator() is called at OPEN time to capture
    // the data snapshot. This means query execution begins at OPEN, not at first FETCH.
    //
    // IMPORTANT: Runtime errors in the cursor query are now thrown at OPEN time because
    // that's when execution starts. This is a consequence of implementing INSENSITIVE
    // cursor semantics where the snapshot must be captured at OPEN.
    //
    // This test verifies that runtime errors (e.g., divide by zero) are caught at OPEN.
    sql("CREATE TABLE cursor_row_error_test (id INT, value INT) USING parquet")
    sql("INSERT INTO cursor_row_error_test VALUES (1, 10), (2, 0), (3, 5)")
    try {
      val result = sql("""BEGIN
  DECLARE result STRING DEFAULT 'start';
  DECLARE x INT;
  DECLARE y INT;

  BEGIN
    DECLARE cur CURSOR FOR SELECT id, 100 / value AS result FROM cursor_row_error_test ORDER BY id;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
      SET result = result || ' | failed at open';

    SET result = 'declared';
    OPEN cur;  -- Fails here: executeToIterator() starts execution, hits divide-by-zero
    SET result = 'opened';  -- Should NOT reach here
    FETCH cur INTO x, y;
    SET result = 'fetched: ' || x || ',' || y;  -- Should NOT reach here
  END;

  SELECT result;
END;""")
      // Should show DECLARE succeeded, but OPEN failed due to divide-by-zero
      checkAnswer(result, Seq(Row("declared | failed at open")))
    } finally {
      sql("DROP TABLE IF EXISTS cursor_row_error_test")
    }
  }

}
// scalastyle:on line.size.limit
