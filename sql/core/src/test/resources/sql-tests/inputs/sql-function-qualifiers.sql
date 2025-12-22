-- Test qualified function names for builtin and session functions
-- Tests qualification with system.builtin, builtin, system.session, and session prefixes

SET spark.sql.ansi.enabled = true;

-- Test 1: Builtin function qualification
SELECT 'Test 1: Builtin function qualification' AS test_name;
SELECT abs(-5) AS unqualified;
SELECT builtin.abs(-5) AS schema_qualified;
SELECT system.builtin.abs(-5) AS fully_qualified;
SELECT BUILTIN.ABS(-5) AS uppercase;
SELECT System.Builtin.Abs(-5) AS mixed_case;

-- Test 2: Temporary function without shadowing
SELECT 'Test 2: Temporary function without shadowing' AS test_name;
CREATE TEMPORARY FUNCTION my_temp_upper() RETURNS STRING RETURN 'UPPERCASE';
SELECT my_temp_upper() AS unqualified;
SELECT session.my_temp_upper() AS schema_qualified;
SELECT system.session.my_temp_upper() AS fully_qualified;
SELECT SESSION.my_temp_upper() AS uppercase;
SELECT System.Session.my_temp_upper() AS mixed_case;
DROP TEMPORARY FUNCTION my_temp_upper;

-- Test 3: Temporary function shadows builtin
SELECT 'Test 3: Temporary function shadows builtin' AS test_name;
SELECT abs(-10) AS builtin_before;
CREATE TEMPORARY FUNCTION my_abs(x INT) RETURNS INT RETURN x * 100;
SELECT my_abs(-10) AS unqualified_shadowed;
SELECT builtin.abs(-10) AS builtin_still_works;
SELECT system.builtin.abs(-10) AS builtin_fully_qualified;
DROP TEMPORARY FUNCTION my_abs;
SELECT abs(-10) AS builtin_after;

-- Test 4: Multiple builtin functions
SELECT 'Test 4: Multiple builtin functions' AS test_name;
SELECT
  builtin.abs(-5) AS abs_result,
  builtin.upper('hello') AS upper_result,
  builtin.length('test') AS length_result,
  builtin.round(3.14159, 2) AS round_result;

-- Test 5: Verify temp and builtin are separate
SELECT 'Test 5: Temp and builtin registries are separate' AS test_name;
CREATE TEMPORARY FUNCTION my_custom(s STRING) RETURNS STRING RETURN CONCAT('CUSTOM: ', s);
SELECT
  my_custom('test') AS temp_func,
  builtin.abs(-20) AS builtin_func,
  session.my_custom('test') AS temp_qualified;
DROP TEMPORARY FUNCTION my_custom;

SET spark.sql.ansi.enabled = false;
