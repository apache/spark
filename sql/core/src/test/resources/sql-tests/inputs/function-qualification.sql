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
-- This should fail - abs is now a table function
SELECT abs(-5);
-- But works in table context
SELECT * FROM abs();
-- Builtin scalar still accessible with qualification
SELECT system.builtin.abs(-5);
DROP TEMPORARY FUNCTION abs;

-- Test 16: Cross-type error - scalar function in table context
CREATE TEMPORARY FUNCTION scalar_only() RETURNS INT RETURN 42;
SELECT scalar_only();
-- This should fail
SELECT * FROM scalar_only();
DROP TEMPORARY FUNCTION scalar_only;

-- Test 17: Cross-type error - table function in scalar context
CREATE TEMPORARY FUNCTION table_only() RETURNS TABLE(val INT) RETURN SELECT 42;
SELECT * FROM table_only();
-- This should fail
SELECT table_only();
DROP TEMPORARY FUNCTION table_only;

-- Test 18: Generator function works in both contexts
-- explode is a builtin generator that works in both scalar and table contexts
SELECT explode(array(1, 2, 3));
SELECT * FROM explode(array(1, 2, 3));

-- Test 19: Test OR REPLACE with qualified names
CREATE TEMPORARY FUNCTION replace_test() RETURNS INT RETURN 1;
SELECT replace_test();
CREATE OR REPLACE TEMPORARY FUNCTION session.replace_test() RETURNS INT RETURN 2;
SELECT replace_test();
DROP TEMPORARY FUNCTION replace_test;

-- Test 20: Test IF NOT EXISTS with qualified names
CREATE TEMPORARY FUNCTION IF NOT EXISTS exists_test() RETURNS INT RETURN 1;
SELECT exists_test();
CREATE TEMPORARY FUNCTION IF NOT EXISTS system.session.exists_test() RETURNS INT RETURN 2;
SELECT exists_test();
DROP TEMPORARY FUNCTION exists_test;

-- Test 21: Multiple qualified functions in single query
CREATE TEMPORARY FUNCTION add10(x INT) RETURNS INT RETURN x + 10;
SELECT builtin.abs(-5), session.add10(5), system.builtin.upper('hello');
DROP TEMPORARY FUNCTION add10;

-- Test 22: Qualified aggregate function
CREATE TEMPORARY FUNCTION my_avg(x DOUBLE) RETURNS DOUBLE RETURN avg(x);
SELECT session.my_avg(value) FROM VALUES (1.0), (2.0), (3.0) AS t(value);
DROP TEMPORARY FUNCTION my_avg;

-- Test 23: Non-existent function gives UNRESOLVED_ROUTINE error
SELECT non_existent_func();

-- Test 24: Temporary scalar function shadows builtin table function
CREATE TEMPORARY FUNCTION range() RETURNS INT RETURN 999;
SELECT range();
-- This should fail - range is now scalar
SELECT * FROM range(5);
-- Builtin table function accessible with qualification
SELECT * FROM builtin.range(5);
DROP TEMPORARY FUNCTION range;

-- Test 25: Qualified builtin bypasses temporary shadow
CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 888;
SELECT abs();
SELECT builtin.abs(-42);
DROP TEMPORARY FUNCTION abs;

-- Test 26: Cannot create duplicate temp functions of different types
CREATE TEMPORARY FUNCTION dup_test() RETURNS INT RETURN 42;
-- This should fail
CREATE TEMPORARY FUNCTION dup_test() RETURNS TABLE(val INT) RETURN SELECT 99;

-- Test 27: CREATE OR REPLACE can change function type
CREATE OR REPLACE TEMPORARY FUNCTION dup_test() RETURNS TABLE(val INT) RETURN SELECT 99;
SELECT * FROM dup_test();
DROP TEMPORARY FUNCTION dup_test;

-- Test 28: Views with temporary functions
CREATE TEMPORARY FUNCTION view_func() RETURNS STRING RETURN 'from_temp';
CREATE TEMPORARY VIEW temp_view AS SELECT view_func() as result;
SELECT * FROM temp_view;
DROP VIEW temp_view;
DROP TEMPORARY FUNCTION view_func;

-- Test 29: Temporary view with temp function shadowing builtin
CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 777;
CREATE TEMPORARY VIEW shadow_view AS SELECT abs() as result;
SELECT * FROM shadow_view;
-- Builtin still works with qualification in view
CREATE TEMPORARY VIEW builtin_view AS SELECT builtin.abs(-10) as result;
SELECT * FROM builtin_view;
DROP VIEW shadow_view;
DROP VIEW builtin_view;
DROP TEMPORARY FUNCTION abs;

-- Test 30: Multiple temp functions in same view
CREATE TEMPORARY FUNCTION func1() RETURNS INT RETURN 1;
CREATE TEMPORARY FUNCTION func2() RETURNS INT RETURN 2;
CREATE TEMPORARY VIEW multi_func_view AS SELECT func1() + func2() as sum;
SELECT * FROM multi_func_view;
DROP VIEW multi_func_view;
DROP TEMPORARY FUNCTION func1;
DROP TEMPORARY FUNCTION func2;

-- Test 31: Nested views with temp functions
CREATE TEMPORARY FUNCTION nested_func() RETURNS INT RETURN 100;
CREATE TEMPORARY VIEW inner_view AS SELECT nested_func() as val;
CREATE TEMPORARY VIEW outer_view AS SELECT val * 2 FROM inner_view;
SELECT * FROM outer_view;
DROP VIEW outer_view;
DROP VIEW inner_view;
DROP TEMPORARY FUNCTION nested_func;

-- Test 32: SHOW FUNCTIONS includes builtin and session functions
CREATE TEMPORARY FUNCTION show_test() RETURNS INT RETURN 1;
SHOW FUNCTIONS LIKE 'show_test';
SHOW FUNCTIONS LIKE 'abs';
DROP TEMPORARY FUNCTION show_test;
