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

-- Test 2e variant: Cannot fetch after closing
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
