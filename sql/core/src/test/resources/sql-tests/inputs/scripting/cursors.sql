-- ============================================================================
-- Spark SQL Cursor Test Suite
-- ============================================================================
--
-- This test suite verifies SQL cursor functionality in Spark SQL Scripting.
-- Cursors enable procedural iteration over query result sets within compound
-- statements (BEGIN...END blocks).
--
-- Coverage:
--   1. Cursor Lifecycle: DECLARE, OPEN, FETCH, CLOSE
--   2. Cursor Scoping: Nested scopes, label qualification, shadowing
--   3. Cursor State: Open/closed validation, reopening, sensitivity
--   4. Parameterized Cursors: Positional/named parameters with USING clause
--   5. FETCH INTO: Store assignment rules, type casting, arity validation
--   6. FETCH INTO: Session/local variables, STRUCT special case
--   7. Exception Handling: NOT FOUND condition (SQLSTATE 02000)
--   8. SQL Standard Compliance: Completion conditions, unhandled behavior
--   9. Complex Queries: CTEs, subqueries, joins in cursor declarations
--  10. Identifier Syntax: IDENTIFIER() clause for cursor names
--  11. Nested Cursors: Multiple cursor levels within compound statements
--
-- SQL Standard References:
--   - ISO/IEC 9075-2:2016 (SQL/Foundation) - Cursor operations
--   - SQLSTATE class '02' - Completion conditions (no data)
--   - Store assignment rules for FETCH INTO (same as SET VARIABLE)
--
-- ============================================================================

-- Test 0a: Verify DECLARE CURSOR is disabled by default
--QUERY-DELIMITER-START
BEGIN
  DECLARE cur CURSOR FOR SELECT 1;
END;
--QUERY-DELIMITER-END

-- Test 0b: Verify OPEN is disabled by default
--QUERY-DELIMITER-START
BEGIN
  OPEN cur;
END;
--QUERY-DELIMITER-END

-- Test 0c: Verify FETCH is disabled by default
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT;
  FETCH cur INTO x;
END;
--QUERY-DELIMITER-END

-- Test 0d: Verify CLOSE is disabled by default
--QUERY-DELIMITER-START
BEGIN
  CLOSE cur;
END;
--QUERY-DELIMITER-END

-- Enable cursor and continue handler features for this test suite
SET spark.sql.scripting.cursorEnabled=true;
SET spark.sql.scripting.continueHandlerEnabled=true;

-- Cursor scoping and state management tests

-- Test 1a: Cursors have a separate namespace from local variables
-- EXPECTED: Success - cursor and variable can have same name
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT DEFAULT 10;
  DECLARE x CURSOR FOR SELECT 1 AS col;
  OPEN x;
  FETCH x INTO x;
  VALUES (x); -- Should return 1
  CLOSE x;
END;
--QUERY-DELIMITER-END

-- Test 1b: Duplicate cursor names in same compound statement are not allowed
-- EXPECTED: Error - CURSOR_ALREADY_EXISTS
--QUERY-DELIMITER-START
BEGIN
  DECLARE c1 CURSOR FOR SELECT 1;
  DECLARE c1 CURSOR FOR SELECT 2;
END;
--QUERY-DELIMITER-END

-- Test 1c: Inner scope cursors shadow outer scope cursors
-- EXPECTED: Success - inner cursor shadows outer cursor with same name
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END

-- Test 2a: A cursor cannot be opened twice
-- EXPECTED: Error - CURSOR_ALREADY_OPEN
--QUERY-DELIMITER-START
BEGIN
  DECLARE c1 CURSOR FOR SELECT 1;
  OPEN c1;
  OPEN c1; -- Should fail
END;
--QUERY-DELIMITER-END

-- Test 2b: A cursor can be closed and then re-opened
-- EXPECTED: Success
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END

-- Test 2c: A cursor that is not open cannot be closed
-- EXPECTED: Error - CURSOR_NOT_OPEN
--QUERY-DELIMITER-START
BEGIN
  DECLARE c1 CURSOR FOR SELECT 1;
  CLOSE c1; -- Should fail
END;
--QUERY-DELIMITER-END

-- Test 2d: A cursor cannot be closed twice
-- EXPECTED: Error - CURSOR_NOT_OPEN
--QUERY-DELIMITER-START
BEGIN
  DECLARE c1 CURSOR FOR SELECT 1;
  OPEN c1;
  CLOSE c1;
  CLOSE c1; -- Should fail
END;
--QUERY-DELIMITER-END

-- Test 2e: A cursor that is not open cannot be fetched
-- EXPECTED: Error - CURSOR_NOT_OPEN
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT;
  DECLARE c1 CURSOR FOR SELECT 1 AS val;
  FETCH c1 INTO x; -- Should fail
END;
--QUERY-DELIMITER-END

-- Test 2f: Cannot fetch after closing
-- EXPECTED: Error - CURSOR_NOT_OPEN
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT;
  DECLARE c1 CURSOR FOR SELECT 1 AS val;
  OPEN c1;
  FETCH c1 INTO x;
  CLOSE c1;
  FETCH c1 INTO x; -- Should fail
END;
--QUERY-DELIMITER-END

-- Test 2g: Cursor is implicitly closed when it goes out of scope.
-- EXPECTED: Success, return 10
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END

-- Additional test: Cursor state is independent across scopes
-- EXPECTED: Success
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT;
  DECLARE c1 CURSOR FOR SELECT 1 AS val;
  OPEN c1;
  BEGIN
    DECLARE y INT;
    DECLARE c1 CURSOR FOR SELECT 2 AS val;
    OPEN c1; -- This is the inner c1, should succeed
    FETCH c1 INTO y;
    VALUES (y); -- Should return 2
    CLOSE c1;
  END;
  FETCH c1 INTO x; -- This is the outer c1, should still be open
  VALUES (x); -- Should return 1
  CLOSE c1;
END;
--QUERY-DELIMITER-END

-- Test 3: Cursor sensitivity - cursor captures snapshot when opened
-- Setup: Create table with initial rows
CREATE TABLE cursor_sensitivity_test (id INT, value STRING) USING parquet;
INSERT INTO cursor_sensitivity_test VALUES (1, 'row1'), (2, 'row2');

-- EXPECTED: Cursor captures snapshot at OPEN time
-- Note: With Spark's default behavior and parquet tables, the analyzed plan
-- may cache table metadata, so both opens may see the same snapshot (4 rows).
-- This demonstrates that cursors are INSENSITIVE by default.
--QUERY-DELIMITER-START
BEGIN
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

  -- Step 5: Add more rows after OPEN
  INSERT INTO cursor_sensitivity_test VALUES (5, 'row5'), (6, 'row6');

  -- Step 6: Fetch rows - should see snapshot from OPEN time (4 rows)
  REPEAT
    FETCH cur INTO fetched_id, fetched_value;
    IF NOT nomorerows THEN
      SET row_count_first_open = row_count_first_open + 1;
    END IF;
  UNTIL nomorerows END REPEAT;

  -- Step 7: Close the cursor
  CLOSE cur;

  -- Step 8: Open the cursor again (should capture new snapshot)
  SET nomorerows = false;
  OPEN cur;

  -- Step 9: Fetch rows - demonstrates cursor behavior
  REPEAT
    FETCH cur INTO fetched_id, fetched_value;
    IF NOT nomorerows THEN
      SET row_count_second_open = row_count_second_open + 1;
    END IF;
  UNTIL nomorerows END REPEAT;

  -- Return both counts
  VALUES (row_count_first_open, row_count_second_open);

  CLOSE cur;
END;
--QUERY-DELIMITER-END

-- Cleanup
DROP TABLE cursor_sensitivity_test;


-- Test 4: Basic parameterized cursor with positional parameters
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 5: Parameterized cursor with named parameters
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 6: Parameterized cursor - reopen with different parameters
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 7: Parameterized cursor with expressions
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 7a: USING clause - various data types (matching EXECUTE IMMEDIATE behavior)
-- EXPECTED: Success - test that USING clause handles various types correctly
--QUERY-DELIMITER-START
BEGIN
  DECLARE int_val INT;
  DECLARE str_val STRING;
  DECLARE date_val DATE;
  DECLARE bool_val BOOLEAN;

  -- Integer type
  DECLARE cur_int CURSOR FOR SELECT typeof(:p) as type, :p as val;
  OPEN cur_int USING 42 AS p;
  FETCH cur_int INTO str_val, int_val;
  CLOSE cur_int;
  VALUES ('INT', int_val);

  -- String type
  DECLARE cur_str CURSOR FOR SELECT typeof(:p) as type, :p as val;
  OPEN cur_str USING 'hello' AS p;
  FETCH cur_str INTO str_val, str_val;
  CLOSE cur_str;
  VALUES ('STRING', str_val);

  -- Date type
  DECLARE cur_date CURSOR FOR SELECT typeof(:p) as type, :p as val;
  OPEN cur_date USING DATE '2023-12-25' AS p;
  FETCH cur_date INTO str_val, date_val;
  CLOSE cur_date;
  VALUES ('DATE', date_val);

  -- Boolean type
  DECLARE cur_bool CURSOR FOR SELECT typeof(:p) as type, :p as val;
  OPEN cur_bool USING true AS p;
  FETCH cur_bool INTO str_val, bool_val;
  CLOSE cur_bool;
  VALUES ('BOOLEAN', bool_val);
END;
--QUERY-DELIMITER-END


-- Test 7b: USING clause - positional vs named parameters
-- EXPECTED: Success - verify positional and named parameter binding work correctly
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;

  -- Positional parameters (no aliases)
  DECLARE cur_pos CURSOR FOR SELECT ? + ? AS sum;
  OPEN cur_pos USING 10, 20;
  FETCH cur_pos INTO result;
  CLOSE cur_pos;
  VALUES ('positional', result); -- Should be 30

  -- Named parameters (with aliases)
  DECLARE cur_named CURSOR FOR SELECT :a + :b AS sum;
  OPEN cur_named USING 15 AS a, 25 AS b;
  FETCH cur_named INTO result;
  CLOSE cur_named;
  VALUES ('named', result); -- Should be 40

  -- Mixed: variable reference without alias (positional)
  DECLARE x INT DEFAULT 100;
  DECLARE cur_var CURSOR FOR SELECT ? + 1 AS val;
  OPEN cur_var USING x;
  FETCH cur_var INTO result;
  CLOSE cur_var;
  VALUES ('variable', result); -- Should be 101
END;
--QUERY-DELIMITER-END


-- Test 7c: USING clause - expressions (matching EXECUTE IMMEDIATE)
-- EXPECTED: Success - test constant expressions in USING clause
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;
  DECLARE base INT DEFAULT 5;

  -- Arithmetic expression
  DECLARE cur1 CURSOR FOR SELECT :p AS val;
  OPEN cur1 USING 5 + 10 AS p;
  FETCH cur1 INTO result;
  CLOSE cur1;
  VALUES ('arithmetic', result); -- Should be 15

  -- Variable reference with expression
  DECLARE cur2 CURSOR FOR SELECT :p AS val;
  OPEN cur2 USING base * 2 AS p;
  FETCH cur2 INTO result;
  CLOSE cur2;
  VALUES ('variable_expr', result); -- Should be 10
END;
--QUERY-DELIMITER-END


-- Test 8: Label-qualified cursor - basic case
-- EXPECTED: Success - cursor qualified with label
--QUERY-DELIMITER-START
BEGIN
  outer: BEGIN
    DECLARE x INT;
    DECLARE c1 CURSOR FOR SELECT 42 AS val;
    OPEN outer.c1;
    FETCH outer.c1 INTO x;
    VALUES (x); -- Should return 42
    CLOSE outer.c1;
  END;
END;
--QUERY-DELIMITER-END


-- Test 9: Label-qualified cursor - nested scopes
-- EXPECTED: Success - inner and outer cursors with same name, qualified access
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 10: Label-qualified cursor with parameterized query
-- EXPECTED: Success - qualified cursor with parameters
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 11: FETCH INTO with duplicate variable names
-- EXPECTED: Error - DUPLICATE_ASSIGNMENTS
--QUERY-DELIMITER-START
BEGIN
  DECLARE x, y INT;
  DECLARE cur CURSOR FOR SELECT 1 AS a, 2 AS b;
  OPEN cur;
  FETCH cur INTO x, x;  -- Should fail - duplicate variable
END;
--QUERY-DELIMITER-END


-- Test 12: FETCH INTO with type casting (store assignment)
-- EXPECTED: Success - values should be cast according to ANSI store assignment rules
--QUERY-DELIMITER-START
BEGIN
  DECLARE int_var INT;
  DECLARE str_var STRING;
  DECLARE cur CURSOR FOR SELECT 100.7 AS double_val, 42 AS int_val;

  OPEN cur;
  FETCH cur INTO int_var, str_var;  -- double->int cast, int->string cast
  CLOSE cur;

  VALUES (int_var, str_var);  -- Should be (100, '42') with ANSI rounding
END;
--QUERY-DELIMITER-END


-- Test 13: FETCH INTO with arity mismatch - too few variables
-- EXPECTED: Error - ASSIGNMENT_ARITY_MISMATCH
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT;
  DECLARE cur CURSOR FOR SELECT 1, 2, 3;
  OPEN cur;
  FETCH cur INTO x;  -- Should fail - 1 target but 3 columns
END;
--QUERY-DELIMITER-END


-- Test 14: FETCH INTO with arity mismatch - too many variables
-- EXPECTED: Error - ASSIGNMENT_ARITY_MISMATCH
--QUERY-DELIMITER-START
BEGIN
  DECLARE x, y, z, w INT;
  DECLARE cur CURSOR FOR SELECT 1, 2;
  OPEN cur;
  FETCH cur INTO x, y, z, w;  -- Should fail - 4 targets but 2 columns
END;
--QUERY-DELIMITER-END


-- Test 15: DECLARE CURSOR with INSENSITIVE keyword
-- EXPECTED: Success - INSENSITIVE is a valid optional keyword
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT;
  DECLARE cur INSENSITIVE CURSOR FOR SELECT 42 AS val;
  OPEN cur;
  FETCH cur INTO x;
  CLOSE cur;
  VALUES (x); -- Should return 42
END;
--QUERY-DELIMITER-END


-- Test 16: DECLARE CURSOR with ASENSITIVE keyword
-- EXPECTED: Success - ASENSITIVE is a valid optional keyword
--QUERY-DELIMITER-START
BEGIN
  DECLARE y INT;
  DECLARE cur ASENSITIVE CURSOR FOR SELECT 99 AS val;
  OPEN cur;
  FETCH cur INTO y;
  CLOSE cur;
  VALUES (y); -- Should return 99
END;
--QUERY-DELIMITER-END


-- Test 17: DECLARE CURSOR with FOR READ ONLY clause
-- EXPECTED: Success - FOR READ ONLY is a valid optional clause
--QUERY-DELIMITER-START
BEGIN
  DECLARE z INT;
  DECLARE cur CURSOR FOR SELECT 77 AS val FOR READ ONLY;
  OPEN cur;
  FETCH cur INTO z;
  CLOSE cur;
  VALUES (z); -- Should return 77
END;
--QUERY-DELIMITER-END


-- Test 18: DECLARE CURSOR with all optional keywords
-- EXPECTED: Success - Combination of INSENSITIVE and FOR READ ONLY
--QUERY-DELIMITER-START
BEGIN
  DECLARE w INT;
  DECLARE cur INSENSITIVE CURSOR FOR SELECT 123 AS val FOR READ ONLY;
  OPEN cur;
  FETCH cur INTO w;
  CLOSE cur;
  VALUES (w); -- Should return 123
END;
--QUERY-DELIMITER-END


-- Test 19: FETCH with NEXT keyword
-- EXPECTED: Success - NEXT is a valid optional keyword
--QUERY-DELIMITER-START
BEGIN
  DECLARE a INT;
  DECLARE cur CURSOR FOR SELECT 55 AS val;
  OPEN cur;
  FETCH NEXT cur INTO a;
  CLOSE cur;
  VALUES (a); -- Should return 55
END;
--QUERY-DELIMITER-END


-- Test 20: FETCH with FROM keyword
-- EXPECTED: Success - FROM is a valid optional keyword
--QUERY-DELIMITER-START
BEGIN
  DECLARE b INT;
  DECLARE cur CURSOR FOR SELECT 66 AS val;
  OPEN cur;
  FETCH FROM cur INTO b;
  CLOSE cur;
  VALUES (b); -- Should return 66
END;
--QUERY-DELIMITER-END


-- Test 21: FETCH with NEXT FROM keywords
-- EXPECTED: Success - NEXT FROM is a valid optional keyword combination
--QUERY-DELIMITER-START
BEGIN
  DECLARE c INT;
  DECLARE cur CURSOR FOR SELECT 88 AS val;
  OPEN cur;
  FETCH NEXT FROM cur INTO c;
  CLOSE cur;
  VALUES (c); -- Should return 88
END;
--QUERY-DELIMITER-END


-- Test 22: FETCH INTO single STRUCT variable - basic case
-- EXPECTED: Success - SQL Standard allows multi-column fetch into single struct
--QUERY-DELIMITER-START
BEGIN
  DECLARE person_record STRUCT<name STRING, age INT>;
  DECLARE cur CURSOR FOR SELECT 'Alice' AS name, 30 AS age;
  OPEN cur;
  FETCH cur INTO person_record;
  CLOSE cur;
  VALUES (person_record.name, person_record.age); -- Should return 'Alice', 30
END;
--QUERY-DELIMITER-END


-- Test 23: FETCH INTO STRUCT with type casting
-- EXPECTED: Success - ANSI casting should apply to struct fields
--QUERY-DELIMITER-START
BEGIN
  DECLARE record_var STRUCT<id INT, value STRING>;
  DECLARE cur CURSOR FOR SELECT 42.7 AS id, 100 AS value;
  OPEN cur;
  FETCH cur INTO record_var;
  CLOSE cur;
  VALUES (record_var.id, record_var.value); -- Should return 42, '100' (with casting)
END;
--QUERY-DELIMITER-END


-- Test 24: FETCH INTO STRUCT - field count mismatch
-- EXPECTED: Error - ASSIGNMENT_ARITY_MISMATCH
--QUERY-DELIMITER-START
BEGIN
  DECLARE record_var STRUCT<a INT, b INT>;
  DECLARE cur CURSOR FOR SELECT 1, 2, 3;
  OPEN cur;
  FETCH cur INTO record_var;  -- Should fail - 2 struct fields but 3 cursor columns
END;
--QUERY-DELIMITER-END


-- Test 25: FETCH INTO non-STRUCT single variable with multiple columns
-- EXPECTED: Error - ASSIGNMENT_ARITY_MISMATCH (not a struct, so arity must match)
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT;
  DECLARE cur CURSOR FOR SELECT 1, 2;
  OPEN cur;
  FETCH cur INTO x;  -- Should fail - single non-struct variable but 2 columns
END;
--QUERY-DELIMITER-END


-- Test 26: FETCH INTO STRUCT with complex types
-- EXPECTED: Success - Struct with mixed types
--QUERY-DELIMITER-START
BEGIN
  DECLARE complex_record STRUCT<id BIGINT, name STRING, value DOUBLE>;
  DECLARE cur CURSOR FOR SELECT 100 AS id, 'test' AS name, 99.5 AS value;
  OPEN cur;
  FETCH cur INTO complex_record;
  CLOSE cur;
  VALUES (complex_record); -- Should return struct(100, 'test', 99.5)
END;
--QUERY-DELIMITER-END


-- Test 27: FETCH INTO with session variables
-- EXPECTED: Success - session variables work with FETCH INTO
--QUERY-DELIMITER-START
SET VAR session_x = 0;
SET VAR session_y = '';
BEGIN
  DECLARE cur CURSOR FOR SELECT 42 AS num, 'hello' AS text;
  OPEN cur;
  FETCH cur INTO session_x, session_y;
  CLOSE cur;
END;
SELECT session_x, session_y;  -- Should return 42, 'hello'
--QUERY-DELIMITER-END


-- Test 28: FETCH INTO mixing local and session variables
-- EXPECTED: Success - can mix local and session variables in FETCH INTO
--QUERY-DELIMITER-START
SET VAR session_var = 0;
BEGIN
  DECLARE local_var STRING;
  DECLARE cur CURSOR FOR SELECT 100 AS a, 'world' AS b;
  OPEN cur;
  FETCH cur INTO session_var, local_var;
  CLOSE cur;
  VALUES (session_var, local_var);  -- Should return 100, 'world'
END;
--QUERY-DELIMITER-END


-- Test 29: FETCH INTO session variables with type casting
-- EXPECTED: Success - ANSI store assignment rules apply to session variables
--QUERY-DELIMITER-START
SET VAR session_int = 0;
SET VAR session_str = '';
BEGIN
  DECLARE cur CURSOR FOR SELECT 99.9 AS double_val, 42 AS int_val;
  OPEN cur;
  FETCH cur INTO session_int, session_str;  -- double->int cast, int->string cast
  CLOSE cur;
END;
SELECT session_int, session_str;  -- Should return 99, '42' (with ANSI rounding)
--QUERY-DELIMITER-END


-- Test 30: FETCH INTO mixing local and session with duplicate session variable
-- EXPECTED: Error - DUPLICATE_ASSIGNMENTS (applies to session variables too)
--QUERY-DELIMITER-START
SET VAR session_dup = 0;
BEGIN
  DECLARE cur CURSOR FOR SELECT 1, 2;
  OPEN cur;
  FETCH cur INTO session_dup, session_dup;  -- Should fail - duplicate session variable
END;
--QUERY-DELIMITER-END


-- Test 31: FETCH INTO mixing with duplicate across local and session
-- EXPECTED: Error - DUPLICATE_ASSIGNMENTS (same variable name in local and session scope)
--QUERY-DELIMITER-START
SET VAR dup_var = 0;
BEGIN
  DECLARE dup_var INT;
  DECLARE cur CURSOR FOR SELECT 1, 2;
  OPEN cur;
  FETCH cur INTO dup_var, dup_var;  -- Should fail - duplicate variable name
END;
--QUERY-DELIMITER-END


-- Test 32: Cursor implicitly closed when exiting DECLARE scope (not OPEN scope)
-- EXPECTED: Success - cursor declared in outer scope, opened in inner scope,
--           implicitly closed when exiting outer scope (where it was declared)
--QUERY-DELIMITER-START
BEGIN
  outer: BEGIN
    DECLARE x INT;
    DECLARE cur CURSOR FOR SELECT 42 AS val;

    inner: BEGIN
      OPEN cur;  -- Open in inner scope
      FETCH cur INTO x;
      -- Cursor remains open when exiting inner scope
    END;

    -- Cursor should still be open here (we're still in outer scope where it was declared)
    FETCH cur INTO x;  -- This should succeed
    VALUES (x);  -- Should return 42

    -- Cursor will be implicitly closed when exiting outer scope
  END;
END;
--QUERY-DELIMITER-END


-- Test 33: Verify cursor closed when exiting DECLARE scope, not OPEN scope
-- EXPECTED: Error - cursor not open after exiting the scope where it was declared
--QUERY-DELIMITER-START
BEGIN
  DECLARE y INT;

  scope1: BEGIN
    DECLARE cur CURSOR FOR SELECT 99 AS val;
    OPEN cur;
    FETCH cur INTO y;
  END;  -- cursor is implicitly closed here (exiting DECLARE scope)

  -- This should fail because cursor no longer exists (declared in scope1)
  FETCH cur INTO y;
END;
--QUERY-DELIMITER-END


-- Test 34: Unhandled CURSOR_NO_MORE_ROWS continues execution (SQL Standard completion condition)
-- EXPECTED: Success - SQLSTATE '02xxx' is a completion condition (no data), continues without handler
-- Note: This is distinct from warnings (SQLSTATE '01xxx') which Spark doesn't currently raise
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 35: Verify unhandled exception conditions still throw (not completion conditions)
-- EXPECTED: Error - DIVIDE_BY_ZERO is an exception condition, should throw without handler
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT;
  -- No handler declared
  SET x = 1 / 0;  -- Should throw DIVIDE_BY_ZERO (SQLSTATE 22012), not continue
  VALUES ('This should not be reached');
END;
--QUERY-DELIMITER-END


-- Test 36: IDENTIFIER() clause for cursor names - basic case
-- EXPECTED: Success - cursor name specified using IDENTIFIER()
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT;
  DECLARE IDENTIFIER('my_cursor') CURSOR FOR SELECT 99 AS val;
  OPEN IDENTIFIER('my_cursor');
  FETCH IDENTIFIER('my_cursor') INTO x;
  CLOSE IDENTIFIER('my_cursor');
  VALUES (x); -- Should return 99
END;
--QUERY-DELIMITER-END


-- Test 37: IDENTIFIER() clause for cursor names - preserves literal case
-- EXPECTED: Success - IDENTIFIER() preserves the literal, but resolution is still case-insensitive
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;
  DECLARE IDENTIFIER('MixedCase') CURSOR FOR SELECT 42;
  OPEN IDENTIFIER('MixedCase');
  FETCH IDENTIFIER('MixedCase') INTO result;
  CLOSE IDENTIFIER('MixedCase');
  VALUES (result); -- Should return 42
END;
--QUERY-DELIMITER-END


-- Test 37a: IDENTIFIER() clause in FETCH INTO statement
-- EXPECTED: Success - IDENTIFIER() works for variable names in FETCH INTO
--QUERY-DELIMITER-START
BEGIN
  DECLARE IDENTIFIER('my_result') INT;
  DECLARE my_cursor CURSOR FOR SELECT 123 AS val;
  OPEN my_cursor;
  FETCH my_cursor INTO IDENTIFIER('my_result');
  CLOSE my_cursor;
  VALUES (IDENTIFIER('my_result')); -- Should return 123
END;
--QUERY-DELIMITER-END


-- Test 37b: IDENTIFIER() in FETCH INTO with multiple variables
-- EXPECTED: Success - IDENTIFIER() works with multiple target variables
--QUERY-DELIMITER-START
BEGIN
  DECLARE IDENTIFIER('val1') INT;
  DECLARE IDENTIFIER('val2') STRING;
  DECLARE cur CURSOR FOR SELECT 42, 'test';
  OPEN cur;
  FETCH cur INTO IDENTIFIER('val1'), IDENTIFIER('val2');
  CLOSE cur;
  VALUES (IDENTIFIER('val1'), IDENTIFIER('val2')); -- Should return 42, 'test'
END;
--QUERY-DELIMITER-END


-- Test 38: Complex SQL in DECLARE - Recursive CTE
-- EXPECTED: Success - cursor with recursive common table expression
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 39: Complex SQL in DECLARE - Subqueries and joins
-- EXPECTED: Success - cursor with complex query
--QUERY-DELIMITER-START
CREATE TEMPORARY VIEW customers AS SELECT 1 AS id, 'Alice' AS name
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
END;

DROP VIEW customers;
DROP VIEW orders;
--QUERY-DELIMITER-END


-- Test 40: Nested cursors (3 levels)
-- EXPECTED: Success - demonstrates proper nesting and scope management
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- ============================================================================
-- Parameter Marker Tests - Edge Cases and Error Conditions
-- ============================================================================

-- Test 41: USING clause with MORE expressions than needed (allowed - extras ignored)
-- EXPECTED: Success - extra parameters are allowed and ignored
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT ? + 10 AS val;

  OPEN cur USING 5, 99, 100;  -- Only first parameter (5) is used, others ignored
  FETCH cur INTO result;
  CLOSE cur;

  VALUES (result);  -- Should return 15 (5 + 10)
END;
--QUERY-DELIMITER-END


-- Test 42: USING clause with TOO FEW expressions for positional parameters
-- EXPECTED: Error - UNBOUND_SQL_PARAMETER
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT ? + ? AS val;

  OPEN cur USING 10;  -- Only 1 parameter provided, but 2 needed
  FETCH cur INTO result;
  CLOSE cur;
END;
--QUERY-DELIMITER-END


-- Test 43: USING clause missing a named parameter
-- EXPECTED: Error - UNBOUND_SQL_PARAMETER
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT :x + :y AS val;

  OPEN cur USING (x AS x);  -- Missing parameter 'y'
  FETCH cur INTO result;
  CLOSE cur;
END;
--QUERY-DELIMITER-END


-- Test 44: USING clause with named parameters but wrong name
-- EXPECTED: Error - UNBOUND_SQL_PARAMETER
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT :alpha + :beta AS val;

  OPEN cur USING (5 AS alpha, 10 AS gamma);  -- 'gamma' provided but 'beta' expected
  FETCH cur INTO result;
  CLOSE cur;
END;
--QUERY-DELIMITER-END


-- Test 45: DECLARE mixes named and unnamed parameter markers
-- EXPECTED: Error - UNRECOGNIZED_SQL_TYPE (due to invalid parameter marker syntax)
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT ? + :named AS val;  -- Mixed positional and named

  OPEN cur USING 10, (20 AS named);
  FETCH cur INTO result;
  CLOSE cur;
END;
--QUERY-DELIMITER-END


-- Test 46: Local variable with same name as named parameter (implicit alias)
-- EXPECTED: Success - variable name becomes the parameter name implicitly
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT DEFAULT 100;
  DECLARE y INT DEFAULT 200;
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT :x + :y AS val;

  -- Variable names match parameter names, so no explicit alias needed
  OPEN cur USING (x AS x, y AS y);
  FETCH cur INTO result;
  CLOSE cur;

  VALUES (result);  -- Should return 300 (100 + 200)
END;
--QUERY-DELIMITER-END


-- Test 47: Named parameters with expressions (not just variable references)
-- EXPECTED: Success - expressions can be used with explicit aliases
--QUERY-DELIMITER-START
BEGIN
  DECLARE base INT DEFAULT 10;
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT :a * :b AS val;

  OPEN cur USING (base * 2 AS a, base + 5 AS b);  -- Expressions with aliases
  FETCH cur INTO result;
  CLOSE cur;

  VALUES (result);  -- Should return 300 (20 * 15)
END;
--QUERY-DELIMITER-END


-- Test 48: Positional parameters with complex expressions
-- EXPECTED: Success - expressions work for positional parameters
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT DEFAULT 5;
  DECLARE y INT DEFAULT 3;
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT ? * ? + ? AS val;

  OPEN cur USING x * 2, y + 1, 10;  -- (5*2) * (3+1) + 10 = 10 * 4 + 10 = 50
  FETCH cur INTO result;
  CLOSE cur;

  VALUES (result);  -- Should return 50
END;
--QUERY-DELIMITER-END


-- Test 49: Reopen cursor with different parameter values (positional)
-- EXPECTED: Success - cursor can be reopened with different positional parameters
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 50: Reopen cursor with different parameter values (named)
-- EXPECTED: Success - cursor can be reopened with different named parameters
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 51: ALL_PARAMETERS_MUST_BE_NAMED - Mix of named and positional in USING
-- EXPECTED: Error - ALL_PARAMETERS_MUST_BE_NAMED
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT :x + :y AS val;

  -- USING clause mixes positional (10) and named (y AS y)
  OPEN cur USING 10, (20 AS y);
  FETCH cur INTO result;
  CLOSE cur;
END;
--QUERY-DELIMITER-END


-- Test 52: Verify variable name inference for parameters (without explicit AS)
-- EXPECTED: Success - variable names inferred from identifiers in USING clause
--QUERY-DELIMITER-START
BEGIN
  DECLARE multiplier INT DEFAULT 4;
  DECLARE addend INT DEFAULT 6;
  DECLARE result INT;
  DECLARE cur CURSOR FOR SELECT :multiplier * 10 + :addend AS val;

  -- Variable names match parameter names, implicit binding
  OPEN cur USING (multiplier AS multiplier, addend AS addend);
  FETCH cur INTO result;
  CLOSE cur;

  VALUES (result);  -- Should return 46 (4 * 10 + 6)
END;
--QUERY-DELIMITER-END


-- Test 53: Case insensitivity - DECLARE lowercase, OPEN uppercase
-- EXPECTED: Success - cursor names should be case-insensitive by default
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;
  DECLARE my_cursor CURSOR FOR SELECT 42 AS val;
  OPEN MY_CURSOR;
  FETCH MY_CURSOR INTO result;
  CLOSE MY_CURSOR;
  VALUES (result);  -- Should return 42
END;
--QUERY-DELIMITER-END


-- Test 54: Case insensitivity - DECLARE MixedCase, OPEN lowercase
-- EXPECTED: Success - cursor names should be case-insensitive by default
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;
  DECLARE MyCursor CURSOR FOR SELECT 99 AS val;
  OPEN mycursor;
  FETCH mycursor INTO result;
  CLOSE mycursor;
  VALUES (result);  -- Should return 99
END;
--QUERY-DELIMITER-END


-- Test 55: Case insensitivity - Label-qualified cursor with different cases
-- EXPECTED: Success - both label and cursor name should be case-insensitive
--QUERY-DELIMITER-START
BEGIN
  outer_lbl: BEGIN
    DECLARE result INT;
    DECLARE cur CURSOR FOR SELECT 123 AS val;

    OPEN OUTER_LBL.cur;  -- Label in different case
    FETCH OUTER_LBL.CUR INTO result;  -- Both in different case
    CLOSE outer_lbl.CUR;  -- Cursor in different case

    VALUES (result);  -- Should return 123
  END;
END;
--QUERY-DELIMITER-END


-- Test 56: Case insensitivity with IDENTIFIER() - resolution still case-insensitive
-- EXPECTED: Success - IDENTIFIER() preserves literal but resolution uses caseSensitiveAnalysis
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;
  DECLARE IDENTIFIER('MyCase') CURSOR FOR SELECT 42;
  OPEN IDENTIFIER('mycase');  -- Different case but should work (case-insensitive resolution)
  FETCH IDENTIFIER('MYCASE') INTO result;  -- Another case variation
  CLOSE IDENTIFIER('MyCaSe');  -- Yet another variation
  VALUES (result);  -- Should return 42
END;
--QUERY-DELIMITER-END


-- Test 57: Unhandled NO DATA condition - should continue silently per SQL Standard
-- EXPECTED: Success - Script continues after CURSOR_NO_MORE_ROWS without aborting
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 58: CONTINUE HANDLER for specific CURSOR_NO_MORE_ROWS condition
-- EXPECTED: Success - Handler catches specific error and sets flag
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 59: CONTINUE HANDLER for generic NOT FOUND class
-- EXPECTED: Success - Handler catches all SQLSTATE 02xxx conditions
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 60: EXIT HANDLER for NOT FOUND - exits immediately
-- EXPECTED: Success - Handler executes and exits, statements after handler don't run
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 61: EXIT HANDLER vs CONTINUE HANDLER precedence with loops
-- EXPECTED: Success - CONTINUE handler keeps loop going, EXIT would exit
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 62: Multiple handlers - specific condition takes precedence over generic
-- EXPECTED: Success - CURSOR_NO_MORE_ROWS handler runs instead of NOT FOUND
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 63: Nested blocks with different NOT FOUND handlers
-- EXPECTED: Success - Inner handler doesn't affect outer cursor
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 64: Variable access in CONTINUE handler BEGIN...END (no cursors)
-- EXPECTED: Handler should be able to read and write outer scope variable
--QUERY-DELIMITER-START
BEGIN
  DECLARE x INT DEFAULT 10;

  DECLARE CONTINUE HANDLER FOR SQLSTATE '22012'
  BEGIN
    SET x = x + 1;  -- Read x and write x from outer scope
  END;

  SELECT x AS before_handler;  -- Should be 10

  -- Trigger handler
  SELECT 1 / 0;

  SELECT x AS after_handler;  -- Should be 11 (handler incremented it)
END;
--QUERY-DELIMITER-END


-- Test 65: CLOSE cursor in CONTINUE handler (cross-frame cursor access)
-- EXPECTED: Handler closes cursor, subsequent FETCH fails
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 66: FETCH and CLOSE in handler BEGIN block (cross-frame cursor access)
-- EXPECTED: Handler fetches and closes cursor, subsequent FETCH fails
--QUERY-DELIMITER-START
BEGIN
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
END;
--QUERY-DELIMITER-END


-- Test 67: Invalid declaration order - cursor before variable
-- EXPECTED: Error - variables must be declared before cursors
--QUERY-DELIMITER-START
BEGIN
  DECLARE cur CURSOR FOR SELECT 123 AS val;
  DECLARE result INT;  -- Invalid: variable after cursor
END;
--QUERY-DELIMITER-END


-- Test 68: Invalid declaration order - cursor after handler
-- EXPECTED: Error - cursors must be declared before handlers
--QUERY-DELIMITER-START
BEGIN
  DECLARE result INT;
  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' BEGIN END;
  DECLARE cur CURSOR FOR SELECT 123 AS val;  -- Invalid: cursor after handler
END;
--QUERY-DELIMITER-END
