-- Precedence between parameterless built-in functions and other resolution candidates
-- (column, LCA, outer reference, session variable). The outer-reference cases for
-- `current_time` are omitted because the value is non-deterministic; other `current_time`
-- patterns are covered (column-wins below, and the UDF-param case in the companion
-- sql-udf-name-precedence.sql).

CREATE OR REPLACE TEMPORARY VIEW v_user AS SELECT 'admin.admin' AS current_user;
CREATE OR REPLACE TEMPORARY VIEW v_time AS SELECT CAST(0 AS INT) AS current_time;

-- Column wins over parameterless function.
SELECT current_user FROM v_user;
SELECT current_time FROM v_time;

-- Parameterless function wins over LCA. Compared against `current_user()` (with alias) so
-- the golden stays stable across test envs that return different user names.
SELECT 'abc' AS current_user, current_user = current_user() AS function_won;

-- Parameterless function wins over outer reference.
SELECT (SELECT current_user) = current_user() AS function_won FROM v_user;

DECLARE current_user = 'abc';

-- Column wins over both the parameterless function and the session variable.
SELECT current_user, current_user FROM v_user;

DROP TEMPORARY VARIABLE current_user;

-- Parameterless function wins over outer reference (current_date / current_timestamp).
-- typeof keeps the golden stable across clock changes.
WITH t1 AS (SELECT 1 AS current_date)
SELECT typeof((SELECT current_date)) FROM t1;

WITH t1 AS (SELECT 1 AS current_timestamp)
SELECT typeof((SELECT current_timestamp)) FROM t1;

-- Parameterless function wins over outer reference (user / session_user).
WITH t1 AS (SELECT 1 AS user)
SELECT (SELECT user) = current_user() AS function_won FROM t1;

WITH t1 AS (SELECT 1 AS session_user)
SELECT (SELECT session_user) = current_user() AS function_won FROM t1;

-- grouping__id: the resolution rule applies but the function itself only makes sense
-- inside grouping analytics. Assert the rule fires structurally via type/error pattern.
SELECT typeof(grouping__id) FROM v_user GROUP BY current_user GROUPING SETS ((current_user));

DROP VIEW v_user;
DROP VIEW v_time;
