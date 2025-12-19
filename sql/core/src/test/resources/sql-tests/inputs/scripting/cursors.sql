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
