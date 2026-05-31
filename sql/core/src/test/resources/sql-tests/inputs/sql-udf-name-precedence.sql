-- Precedence between a SQL UDF parameter and other resolution candidates (column,
-- parameterless built-in function, LCA, outer reference, session variable, nested UDF).

CREATE OR REPLACE TEMPORARY VIEW v1 AS SELECT 1 AS x;
CREATE OR REPLACE TEMPORARY FUNCTION identity_fn(x INT) RETURNS INT RETURN x;

-- UDF parameter resolves when no column conflict.
SELECT identity_fn(42);

CREATE OR REPLACE TEMPORARY FUNCTION col_vs_param(x INT) RETURNS INT RETURN (SELECT x FROM v1);

-- Column wins over UDF parameter.
SELECT col_vs_param(42);

CREATE OR REPLACE TEMPORARY FUNCTION paramless_vs_param(current_user STRING)
RETURNS STRING RETURN current_user;

-- Parameterless function wins over UDF parameter (current_user). Comparison against
-- `current_user()` keeps the golden stable across envs returning different user names.
SELECT paramless_vs_param('should_be_ignored') = current_user() AS function_won;

CREATE OR REPLACE TEMPORARY FUNCTION paramless_vs_param_date(current_date INT)
RETURNS STRING RETURN typeof(current_date);

-- Parameterless function wins over UDF parameter (current_date): the body returns 'date',
-- which would be 'int' if the parameter alias had won.
SELECT paramless_vs_param_date(42);

CREATE OR REPLACE TEMPORARY FUNCTION paramless_vs_param_time(current_time INT)
RETURNS STRING RETURN typeof(current_time);

-- Parameterless function wins over UDF parameter (current_time): the body returns a
-- time-typed value, vs 'int' if the parameter alias had won.
SELECT paramless_vs_param_time(42);

CREATE OR REPLACE TEMPORARY FUNCTION paramless_vs_param_grouping(grouping__id INT)
RETURNS INT RETURN grouping__id;

-- Parameterless function (grouping__id) wins over UDF parameter. grouping__id outside of
-- a GROUPING SETS context fails analysis; assert via the resulting error class rather
-- than a value comparison.
SELECT paramless_vs_param_grouping(42);

CREATE OR REPLACE TEMPORARY FUNCTION lca_vs_param(x INT)
RETURNS INT RETURN (SELECT y FROM (SELECT 999 AS x, x + 1 AS y));

-- LCA wins over UDF parameter (1000 = LCA won, 43 = param won).
SELECT lca_vs_param(42);

CREATE OR REPLACE TEMPORARY FUNCTION outer_vs_param(x INT)
RETURNS INT RETURN (SELECT (SELECT x) FROM v1);

-- Outer column wins over UDF parameter.
SELECT outer_vs_param(42);

CREATE OR REPLACE TEMPORARY FUNCTION outer_param_pure(x INT)
RETURNS INT RETURN (SELECT (SELECT x));

-- UDF parameter is visible via outer reference when no other binding is in scope.
SELECT outer_param_pure(42);

DECLARE x = 999;

-- UDF parameter wins over session variable.
SELECT identity_fn(42);

CREATE OR REPLACE TEMPORARY FUNCTION inner_fn(y INT) RETURNS INT RETURN x;
CREATE OR REPLACE TEMPORARY FUNCTION outer_fn(x INT) RETURNS INT RETURN inner_fn(x);

-- Nested UDF only sees innermost scope: inner_fn resolves 'x' from session variable (999),
-- not outer_fn parameter (42).
SELECT outer_fn(42);

CREATE OR REPLACE TEMPORARY FUNCTION tvf_paramless_vs_param(current_user STRING)
RETURNS TABLE(c STRING) RETURN SELECT current_user AS c;

-- Parameterless function wins over TVF parameter (same rule must apply on the
-- makeSQLTableFunctionPlan path).
SELECT c = current_user() AS function_won FROM tvf_paramless_vs_param('should_be_ignored');

DROP TEMPORARY VARIABLE x;
DROP VIEW v1;
DROP TEMPORARY FUNCTION identity_fn;
DROP TEMPORARY FUNCTION col_vs_param;
DROP TEMPORARY FUNCTION paramless_vs_param;
DROP TEMPORARY FUNCTION paramless_vs_param_date;
DROP TEMPORARY FUNCTION paramless_vs_param_time;
DROP TEMPORARY FUNCTION paramless_vs_param_grouping;
DROP TEMPORARY FUNCTION lca_vs_param;
DROP TEMPORARY FUNCTION outer_vs_param;
DROP TEMPORARY FUNCTION outer_param_pure;
DROP TEMPORARY FUNCTION inner_fn;
DROP TEMPORARY FUNCTION outer_fn;
DROP TEMPORARY FUNCTION tvf_paramless_vs_param;
