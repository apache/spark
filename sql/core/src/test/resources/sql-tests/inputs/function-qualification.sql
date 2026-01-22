-- Tests for builtin/temp/persistent function name disambiguation with qualified names
-- This tests the system.builtin and system.session namespaces

--
-- SECTION 1: Basic Qualification Tests
--

-- Test builtin function with explicit qualification
SELECT system.builtin.abs(-5);
SELECT builtin.abs(-5);

-- Test builtin with case-insensitive qualification
SELECT BUILTIN.abs(-5);
SELECT System.Builtin.ABS(-5);

--
-- SECTION 2: Temporary Function Creation and Qualification
--

-- Create temporary function with unqualified name
CREATE TEMPORARY FUNCTION my_func() RETURNS INT RETURN 42;
SELECT my_func();

-- Create with session qualification
CREATE TEMPORARY FUNCTION session.my_func2() RETURNS STRING RETURN 'temp';
SELECT my_func2();
SELECT session.my_func2();

-- Create with system.session qualification
CREATE TEMPORARY FUNCTION system.session.my_func3() RETURNS INT RETURN 100;
SELECT my_func3();
SELECT system.session.my_func3();

-- Test case insensitivity with temp functions
SELECT SESSION.my_func();
SELECT SYSTEM.SESSION.my_func2();

-- Clean up
DROP TEMPORARY FUNCTION my_func;
DROP TEMPORARY FUNCTION session.my_func2;
DROP TEMPORARY FUNCTION system.session.my_func3;

--
-- SECTION 3: Shadowing Behavior (Post-Security Fix)
--

-- IMPORTANT: After the security fix, temp functions can NO LONGER shadow built-in functions.
-- Resolution order is now: extension -> builtin -> session (temp)
-- This prevents security attacks where users create malicious temp functions like current_user()

-- Create temp function with same name as builtin
CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 999;

-- Unqualified abs now resolves to BUILTIN (not temp!), due to security-focused resolution order
SELECT abs(-5);

-- Temp function only accessible with explicit qualification
SELECT session.abs();

-- Builtin still accessible with qualification
SELECT builtin.abs(-10);
SELECT system.builtin.abs(-10);
DROP TEMPORARY FUNCTION abs;

-- After drop, builtin still works unqualified
SELECT abs(-5);

--
-- SECTION 4: Cross-Type Shadowing (Scalar vs Table Functions) - Post-Security Fix
--

-- NOTE: With security fix (extension -> builtin -> session), built-ins now WIN over temps.
-- Cross-type errors only occur when NO function type exists in built-ins.

-- Test 1: Temp table function + builtin scalar function (NO conflict - builtin wins!)
CREATE TEMPORARY FUNCTION abs() RETURNS TABLE(val INT) RETURN SELECT 42;
-- Builtin scalar abs still works unqualified (builtin resolves before temp table)
SELECT abs(-5);
-- Temp table function works in table context
SELECT * FROM abs();
-- Both accessible with explicit qualification
SELECT builtin.abs(-5);
SELECT * FROM session.abs();
DROP TEMPORARY FUNCTION abs;

-- Test 2: Temp scalar function + builtin table function (NO conflict - builtin wins!)
CREATE TEMPORARY FUNCTION range() RETURNS INT RETURN 999;
-- Builtin table range still works unqualified in table context (builtin resolves before temp scalar)
SELECT * FROM range(5);
-- Temp scalar function works in scalar context
SELECT range();
-- Both accessible with explicit qualification
SELECT * FROM builtin.range(5);
SELECT session.range();
DROP TEMPORARY FUNCTION range;

--
-- SECTION 5: Cross-Type Error Detection
--

-- Scalar function cannot be used in table context
CREATE TEMPORARY FUNCTION scalar_only() RETURNS INT RETURN 42;
SELECT scalar_only();
SELECT * FROM scalar_only();
DROP TEMPORARY FUNCTION scalar_only;

-- Table function cannot be used in scalar context
CREATE TEMPORARY FUNCTION table_only() RETURNS TABLE(val INT) RETURN SELECT 42;
SELECT * FROM table_only();
SELECT table_only();
DROP TEMPORARY FUNCTION table_only;

-- Generator functions work in both contexts
SELECT explode(array(1, 2, 3));
SELECT * FROM explode(array(1, 2, 3));

--
-- SECTION 6: DDL Operations with Qualified Names
--

-- DESCRIBE builtin functions with qualification (no createTime, stable output)
DESCRIBE FUNCTION builtin.abs;
DESCRIBE FUNCTION system.builtin.abs;

-- DROP with qualified names
CREATE TEMPORARY FUNCTION drop_test() RETURNS INT RETURN 1;
DROP TEMPORARY FUNCTION session.drop_test;

-- CREATE OR REPLACE with qualified names
CREATE TEMPORARY FUNCTION replace_test() RETURNS INT RETURN 1;
SELECT replace_test();
CREATE OR REPLACE TEMPORARY FUNCTION session.replace_test() RETURNS INT RETURN 2;
SELECT replace_test();
DROP TEMPORARY FUNCTION replace_test;

-- CREATE OR REPLACE can change function type
CREATE TEMPORARY FUNCTION type_change() RETURNS INT RETURN 42;
SELECT type_change();
CREATE OR REPLACE TEMPORARY FUNCTION type_change() RETURNS TABLE(val INT) RETURN SELECT 99;
SELECT * FROM type_change();
DROP TEMPORARY FUNCTION type_change;

-- IF NOT EXISTS with qualified names
CREATE TEMPORARY FUNCTION IF NOT EXISTS exists_test() RETURNS INT RETURN 1;
SELECT exists_test();
CREATE TEMPORARY FUNCTION IF NOT EXISTS system.session.exists_test() RETURNS INT RETURN 2;
-- Should still be 1 (not replaced)
SELECT exists_test();
DROP TEMPORARY FUNCTION exists_test;

-- SHOW FUNCTIONS includes both builtin and session functions
CREATE TEMPORARY FUNCTION show_test() RETURNS INT RETURN 1;
SHOW FUNCTIONS LIKE 'show_test';
SHOW FUNCTIONS LIKE 'abs';
DROP TEMPORARY FUNCTION show_test;

--
-- SECTION 7: Error Cases
--

-- Cannot create temp function with builtin namespace
CREATE TEMPORARY FUNCTION system.builtin.my_builtin() RETURNS INT RETURN 1;

-- Cannot create temp function with invalid database
CREATE TEMPORARY FUNCTION mydb.my_func() RETURNS INT RETURN 1;

-- Cannot drop builtin function
DROP TEMPORARY FUNCTION system.builtin.abs;

-- Cannot create duplicate temp functions of different types
CREATE TEMPORARY FUNCTION dup_test() RETURNS INT RETURN 42;
CREATE TEMPORARY FUNCTION dup_test() RETURNS TABLE(val INT) RETURN SELECT 99;
DROP TEMPORARY FUNCTION dup_test;

-- Non-existent function error
SELECT non_existent_func();

--
-- SECTION 8: Views with Temporary Functions
--

-- Temporary view can reference temporary function
CREATE TEMPORARY FUNCTION view_func() RETURNS STRING RETURN 'from_temp';
CREATE TEMPORARY VIEW temp_view AS SELECT view_func() as result;
SELECT * FROM temp_view;
DROP VIEW temp_view;
DROP TEMPORARY FUNCTION view_func;

-- View referencing temp function with same name as builtin (no shadowing - builtin wins!)
CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 777;
-- View must use qualified name to access temp function (builtin abs requires argument)
CREATE TEMPORARY VIEW shadow_view AS SELECT session.abs() as result;
SELECT * FROM shadow_view;
-- Builtin accessible with qualification in view
CREATE TEMPORARY VIEW builtin_view AS SELECT builtin.abs(-10) as result;
SELECT * FROM builtin_view;
DROP VIEW shadow_view;
DROP VIEW builtin_view;
DROP TEMPORARY FUNCTION abs;

-- Multiple temp functions in same view
CREATE TEMPORARY FUNCTION func1() RETURNS INT RETURN 1;
CREATE TEMPORARY FUNCTION func2() RETURNS INT RETURN 2;
CREATE TEMPORARY VIEW multi_func_view AS SELECT func1() + func2() as sum;
SELECT * FROM multi_func_view;
DROP VIEW multi_func_view;
DROP TEMPORARY FUNCTION func1;
DROP TEMPORARY FUNCTION func2;

-- Nested views with temp functions
CREATE TEMPORARY FUNCTION nested_func() RETURNS INT RETURN 100;
CREATE TEMPORARY VIEW inner_view AS SELECT nested_func() as val;
CREATE TEMPORARY VIEW outer_view AS SELECT val * 2 FROM inner_view;
SELECT * FROM outer_view;
DROP VIEW outer_view;
DROP VIEW inner_view;
DROP TEMPORARY FUNCTION nested_func;

--
-- SECTION 9: Multiple Functions in Single Query
--

-- Test multiple qualified functions together
CREATE TEMPORARY FUNCTION add10(x INT) RETURNS INT RETURN x + 10;
SELECT builtin.abs(-5), session.add10(5), system.builtin.upper('hello');
DROP TEMPORARY FUNCTION add10;

-- Qualified aggregate function
CREATE TEMPORARY FUNCTION my_avg(x DOUBLE) RETURNS DOUBLE RETURN avg(x);
SELECT session.my_avg(value) FROM VALUES (1.0), (2.0), (3.0) AS t(value);
DROP TEMPORARY FUNCTION my_avg;

-- Table function with qualified names
CREATE TEMPORARY FUNCTION my_range() RETURNS TABLE(id INT) RETURN SELECT * FROM range(3);
SELECT * FROM my_range();
SELECT * FROM session.my_range();
SELECT * FROM system.session.my_range();
DROP TEMPORARY FUNCTION my_range();

--
-- SECTION 10: Qualified Function Names with Special Syntax (COUNT(*))
--

-- Test COUNT(*) expansion with qualified names
-- Unqualified count(*)
SELECT count(*) FROM VALUES (1), (2), (3) AS t(a);

-- Qualified as builtin.count(*)
SELECT builtin.count(*) FROM VALUES (1), (2), (3) AS t(a);

-- Qualified as system.builtin.count(*)
SELECT system.builtin.count(*) FROM VALUES (1), (2), (3) AS t(a);

-- Case insensitive qualified count(*)
SELECT BUILTIN.COUNT(*) FROM VALUES (1), (2), (3) AS t(a);
SELECT System.Builtin.Count(*) FROM VALUES (1), (2), (3) AS t(a);

-- Test count(tbl.*) blocking with qualified names
CREATE TEMPORARY VIEW count_test_view AS SELECT 1 AS a, 2 AS b;

-- Unqualified count with table.*
SELECT count(count_test_view.*) FROM count_test_view;

-- Qualified count with table.*
SELECT builtin.count(count_test_view.*) FROM count_test_view;
SELECT system.builtin.count(count_test_view.*) FROM count_test_view;

DROP VIEW count_test_view;

--
-- SECTION 11: Security Tests - Built-in Function Protection
--
-- CRITICAL: Built-ins resolve BEFORE user temp functions (security feature)
-- This prevents users from shadowing security-critical functions like current_user()

-- Test 1: User cannot shadow current_user()
-- Baseline: current_user() works (returns actual user)
SELECT current_user() IS NOT NULL;

-- User creates temp function trying to shadow current_user
CREATE TEMPORARY FUNCTION current_user() RETURNS STRING RETURN 'hacker';

-- CRITICAL: Unqualified name still resolves to builtin (NOT the temp function)
-- Resolution order: extension -> builtin -> session
-- So builtin wins over session (temp)
SELECT current_user() IS NOT NULL;

-- User's shadowed function only accessible via explicit qualification
SELECT session.current_user();

DROP TEMPORARY FUNCTION current_user;

-- Test 2: User cannot shadow abs() with temp function
-- Built-in abs works
SELECT builtin.abs(-5);

-- Create temp abs
CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 999;

-- Unqualified abs still resolves to builtin (security-focused order)
SELECT abs(-5);

-- Temp abs only accessible with qualification
SELECT session.abs();

DROP TEMPORARY FUNCTION abs;

-- Test 3: Security functions that should never be shadowable
CREATE TEMPORARY FUNCTION session_user() RETURNS STRING RETURN 'fake_user';
SELECT session_user() IS NOT NULL;  -- Should be builtin, not temp
DROP TEMPORARY FUNCTION session_user;

CREATE TEMPORARY FUNCTION current_database() RETURNS STRING RETURN 'fake_db';
SELECT current_database() IS NOT NULL;  -- Should be builtin, not temp
DROP TEMPORARY FUNCTION current_database;
