-- Tests for builtin/temp/persistent function name disambiguation with qualified names
-- This tests the system.builtin and system.session namespaces

-- Test 1: Use builtin function with explicit qualification
SELECT system.builtin.abs(-5);
SELECT builtin.abs(-5);

-- Test 2: Create temporary function and verify it shadows builtin
CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 999;
SELECT abs();
SELECT system.session.abs();
SELECT session.abs();

-- Test 3: Builtin is still accessible with explicit qualification even when shadowed
SELECT system.builtin.abs(-10);
SELECT builtin.abs(-10);

-- Test 4: Drop temp function and verify builtin works again
DROP TEMPORARY FUNCTION abs;
SELECT abs(-5);

-- Test 5: Create temp function with qualified name
CREATE TEMPORARY FUNCTION session.my_temp() RETURNS STRING RETURN 'temp';
SELECT my_temp();
SELECT session.my_temp();
SELECT system.session.my_temp();

-- Test 6: Create another temp function with system.session prefix
CREATE TEMPORARY FUNCTION system.session.my_temp2() RETURNS STRING RETURN 'temp2';
SELECT my_temp2();
SELECT session.my_temp2();
SELECT system.session.my_temp2();

-- Test 7: Drop temp functions with qualified names
DROP TEMPORARY FUNCTION session.my_temp;
DROP TEMPORARY FUNCTION system.session.my_temp2;

-- Test 8: Test case insensitivity of qualifications
CREATE TEMPORARY FUNCTION my_func() RETURNS INT RETURN 42;
SELECT SESSION.my_func();
SELECT SYSTEM.SESSION.my_func();
SELECT SyStEm.SeSsIoN.my_func();
DROP TEMPORARY FUNCTION my_func;

-- Test 9: Error - cannot create builtin qualified temp function
CREATE TEMPORARY FUNCTION system.builtin.my_builtin() RETURNS INT RETURN 1;

-- Test 10: Error - cannot create with invalid database qualification
CREATE TEMPORARY FUNCTION mydb.my_func() RETURNS INT RETURN 1;

-- Test 11: Error - cannot drop builtin function
DROP TEMPORARY FUNCTION system.builtin.abs;

-- Test 12: Verify DESCRIBE works with qualified names
CREATE TEMPORARY FUNCTION desc_test() RETURNS INT RETURN 100;
DESCRIBE FUNCTION system.session.desc_test;
DESCRIBE FUNCTION session.desc_test;
DESCRIBE FUNCTION desc_test;
DROP TEMPORARY FUNCTION desc_test;

-- Test 13: Verify DESCRIBE works for builtin with qualification
DESCRIBE FUNCTION system.builtin.abs;
DESCRIBE FUNCTION builtin.abs;

-- Test 14: Test with table functions
CREATE TEMPORARY FUNCTION my_range() RETURNS TABLE(id INT) RETURN SELECT * FROM range(3);
SELECT * FROM my_range();
SELECT * FROM session.my_range();
SELECT * FROM system.session.my_range();
DROP TEMPORARY FUNCTION my_range;

-- Test 15: Cross-type shadowing - temp table function shadows builtin scalar
CREATE TEMPORARY FUNCTION abs() RETURNS TABLE(val INT) RETURN SELECT 42;
SELECT abs(-5);
SELECT * FROM abs();
SELECT system.builtin.abs(-5);
DROP TEMPORARY FUNCTION abs;

-- Test 16: Verify temp function doesn't persist across sessions (conceptual test)
-- This just documents the behavior
CREATE TEMPORARY FUNCTION session_test() RETURNS STRING RETURN 'session-scoped';
SELECT session_test();
DROP TEMPORARY FUNCTION session_test;

-- Test 17: Test OR REPLACE with qualified names
CREATE TEMPORARY FUNCTION replace_test() RETURNS INT RETURN 1;
SELECT replace_test();
CREATE OR REPLACE TEMPORARY FUNCTION session.replace_test() RETURNS INT RETURN 2;
SELECT replace_test();
DROP TEMPORARY FUNCTION replace_test;

-- Test 18: Test IF NOT EXISTS with qualified names
CREATE TEMPORARY FUNCTION IF NOT EXISTS exists_test() RETURNS INT RETURN 1;
SELECT exists_test();
CREATE TEMPORARY FUNCTION IF NOT EXISTS system.session.exists_test() RETURNS INT RETURN 2;
SELECT exists_test();
DROP TEMPORARY FUNCTION exists_test;
