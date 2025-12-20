-- Cursor scoping and state management tests
--SET spark.sql.scripting.continueHandlerEnabled=true

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
