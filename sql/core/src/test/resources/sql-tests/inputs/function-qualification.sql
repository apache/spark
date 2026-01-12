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
-- SECTION 3: Shadowing Behavior
--

-- Temp function shadows builtin
CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 999;
SELECT abs();
SELECT session.abs();
-- Builtin still accessible with qualification
SELECT builtin.abs(-10);
SELECT system.builtin.abs(-10);
DROP TEMPORARY FUNCTION abs;

-- After drop, builtin works unqualified again
SELECT abs(-5);

--
-- SECTION 4: Cross-Type Shadowing (Scalar vs Table Functions)
--

-- Temp table function shadows builtin scalar function
CREATE TEMPORARY FUNCTION abs() RETURNS TABLE(val INT) RETURN SELECT 42;
-- Error: abs is now a table function, cannot use in scalar context
SELECT abs(-5);
-- Works in table context
SELECT * FROM abs();
-- Builtin scalar still accessible with qualification
SELECT builtin.abs(-5);
DROP TEMPORARY FUNCTION abs;

-- Temp scalar function shadows builtin table function
CREATE TEMPORARY FUNCTION range() RETURNS INT RETURN 999;
SELECT range();
-- Error: range is now scalar, cannot use in table context
SELECT * FROM range(5);
-- Builtin table function accessible with qualification
SELECT * FROM builtin.range(5);
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

-- DESCRIBE with qualified names
CREATE TEMPORARY FUNCTION desc_test() RETURNS INT RETURN 100;
DESCRIBE FUNCTION desc_test;
DESCRIBE FUNCTION session.desc_test;
DESCRIBE FUNCTION system.session.desc_test;
DROP TEMPORARY FUNCTION desc_test;

-- DESCRIBE builtin with qualification
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

-- View with temp function shadowing builtin
CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 777;
CREATE TEMPORARY VIEW shadow_view AS SELECT abs() as result;
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
DROP TEMPORARY FUNCTION my_range;
