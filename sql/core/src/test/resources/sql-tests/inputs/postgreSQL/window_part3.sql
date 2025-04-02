-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
-- Window Functions Testing
-- https://github.com/postgres/postgres/blob/REL_12_STABLE/src/test/regress/sql/window.sql#L564-L911

-- Test window operator with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

CREATE TEMPORARY VIEW tenk2 AS SELECT * FROM tenk1;

CREATE TABLE empsalary (
    depname string,
    empno integer,
    salary int,
    enroll_date date
) USING parquet;

INSERT INTO empsalary VALUES
  ('develop', 10, 5200, date '2007-08-01'),
  ('sales', 1, 5000, date '2006-10-01'),
  ('personnel', 5, 3500, date '2007-12-10'),
  ('sales', 4, 4800, date '2007-08-08'),
  ('personnel', 2, 3900, date '2006-12-23'),
  ('develop', 7, 4200, date '2008-01-01'),
  ('develop', 9, 4500, date '2008-01-01'),
  ('sales', 3, 4800, date '2007-08-01'),
  ('develop', 8, 6000, date '2006-10-01'),
  ('develop', 11, 5200, date '2007-08-15');

-- Test in_range for other datetime datatypes

-- Spark only supports timestamp
-- [SPARK-29636] Spark can't parse '11:00 BST' or '2000-10-19 10:23:54+01' signatures to timestamp
create table datetimes (
    id int,
    f_time timestamp,
    f_timetz timestamp,
    f_interval timestamp,
    f_timestamptz timestamp,
    f_timestamp timestamp
) using parquet;

-- Spark cannot safely cast string to timestamp
-- [SPARK-29636] Spark can't parse '11:00 BST' or '2000-10-19 10:23:54+01' signatures to timestamp
insert into datetimes values
(1, timestamp '11:00', cast ('11:00 BST' as timestamp), cast ('1 year' as timestamp), cast ('2000-10-19 10:23:54+01' as timestamp), timestamp '2000-10-19 10:23:54'),
(2, timestamp '12:00', cast ('12:00 BST' as timestamp), cast ('2 years' as timestamp), cast ('2001-10-19 10:23:54+01' as timestamp), timestamp '2001-10-19 10:23:54'),
(3, timestamp '13:00', cast ('13:00 BST' as timestamp), cast ('3 years' as timestamp), cast ('2001-10-19 10:23:54+01' as timestamp), timestamp '2001-10-19 10:23:54'),
(4, timestamp '14:00', cast ('14:00 BST' as timestamp), cast ('4 years' as timestamp), cast ('2002-10-19 10:23:54+01' as timestamp), timestamp '2002-10-19 10:23:54'),
(5, timestamp '15:00', cast ('15:00 BST' as timestamp), cast ('5 years' as timestamp), cast ('2003-10-19 10:23:54+01' as timestamp), timestamp '2003-10-19 10:23:54'),
(6, timestamp '15:00', cast ('15:00 BST' as timestamp), cast ('5 years' as timestamp), cast ('2004-10-19 10:23:54+01' as timestamp), timestamp '2004-10-19 10:23:54'),
(7, timestamp '17:00', cast ('17:00 BST' as timestamp), cast ('7 years' as timestamp), cast ('2005-10-19 10:23:54+01' as timestamp), timestamp '2005-10-19 10:23:54'),
(8, timestamp '18:00', cast ('18:00 BST' as timestamp), cast ('8 years' as timestamp), cast ('2006-10-19 10:23:54+01' as timestamp), timestamp '2006-10-19 10:23:54'),
(9, timestamp '19:00', cast ('19:00 BST' as timestamp), cast ('9 years' as timestamp), cast ('2007-10-19 10:23:54+01' as timestamp), timestamp '2007-10-19 10:23:54'),
(10, timestamp '20:00', cast ('20:00 BST' as timestamp), cast ('10 years' as timestamp), cast ('2008-10-19 10:23:54+01' as timestamp), timestamp '2008-10-19 10:23:54');

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select id, f_time, first(id) over w, last(id) over w
-- from datetimes
-- window w as (order by f_time range between
--              '70 min' preceding and '2 hours' following);

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select id, f_time, first(id) over w, last(id) over w
-- from datetimes
-- window w as (order by f_time desc range between
--              '70 min' preceding and '2 hours' following);

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select id, f_timetz, first(id) over w, last(id) over w
-- from datetimes
-- window w as (order by f_timetz range between
--              '70 min' preceding and '2 hours' following);

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select id, f_timetz, first(id) over w, last(id) over w
-- from datetimes
-- window w as (order by f_timetz desc range between
--              '70 min' preceding and '2 hours' following);

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select id, f_interval, first(id) over w, last(id) over w
-- from datetimes
-- window w as (order by f_interval range between
--              '1 year' preceding and '1 year' following);

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select id, f_interval, first(id) over w, last(id) over w
-- from datetimes
-- window w as (order by f_interval desc range between
--              '1 year' preceding and '1 year' following);

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select id, f_timestamptz, first(id) over w, last(id) over w
-- from datetimes
-- window w as (order by f_timestamptz range between
--              '1 year' preceding and '1 year' following);

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select id, f_timestamptz, first(id) over w, last(id) over w
-- from datetimes
-- window w as (order by f_timestamptz desc range between
--              '1 year' preceding and '1 year' following);

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select id, f_timestamp, first(id) over w, last(id) over w
-- from datetimes
-- window w as (order by f_timestamp range between
--              '1 year' preceding and '1 year' following);

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select id, f_timestamp, first(id) over w, last(id) over w
-- from datetimes
-- window w as (order by f_timestamp desc range between
--              '1 year' preceding and '1 year' following);

-- RANGE offset PRECEDING/FOLLOWING error cases
-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select sum(salary) over (order by enroll_date, salary range between '1 year' preceding and '2 years' following
-- 	exclude ties), salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select sum(salary) over (range between '1 year' preceding and '2 years' following
-- 	exclude ties), salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select sum(salary) over (order by depname range between '1 year' preceding and '2 years' following
-- 	exclude ties), salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select max(enroll_date) over (order by enroll_date range between 1 preceding and 2 following
-- 	exclude ties), salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select max(enroll_date) over (order by salary range between -1 preceding and 2 following
-- 	exclude ties), salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select max(enroll_date) over (order by salary range between 1 preceding and -2 following
-- 	exclude ties), salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select max(enroll_date) over (order by salary range between '1 year' preceding and '2 years' following
-- 	exclude ties), salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select max(enroll_date) over (order by enroll_date range between '1 year' preceding and '-2 years' following
-- 	exclude ties), salary, enroll_date from empsalary;

-- GROUPS tests

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (order by four groups between unbounded preceding and current row),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (order by four groups between unbounded preceding and unbounded following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (order by four groups between current row and unbounded following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (order by four groups between 1 preceding and unbounded following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (order by four groups between 1 following and unbounded following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (order by four groups between unbounded preceding and 2 following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (order by four groups between 2 preceding and 1 preceding),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (order by four groups between 2 preceding and 1 following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (order by four groups between 0 preceding and 0 following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (order by four groups between 2 preceding and 1 following
--   exclude current row), unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (order by four range between 2 preceding and 1 following
--   exclude group), unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (order by four range between 2 preceding and 1 following
--   exclude ties), unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (partition by ten
--   order by four groups between 0 preceding and 0 following),unique1, four, ten
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (partition by ten
--   order by four groups between 0 preceding and 0 following exclude current row), unique1, four, ten
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (partition by ten
--   order by four groups between 0 preceding and 0 following exclude group), unique1, four, ten
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- SELECT sum(unique1) over (partition by ten
--   order by four groups between 0 preceding and 0 following exclude ties), unique1, four, ten
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-27951] ANSI SQL: NTH_VALUE function
-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- select first_value(salary) over(order by enroll_date groups between 1 preceding and 1 following),
-- lead(salary) over(order by enroll_date groups between 1 preceding and 1 following),
-- nth_value(salary, 1) over(order by enroll_date groups between 1 preceding and 1 following),
-- salary, enroll_date from empsalary;

-- [SPARK-28508] Support for range frame+row frame in the same query
-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- select last(salary) over(order by enroll_date groups between 1 preceding and 1 following),
-- lag(salary)         over(order by enroll_date groups between 1 preceding and 1 following),
-- salary, enroll_date from empsalary;

-- [SPARK-27951] ANSI SQL: NTH_VALUE function
-- select first_value(salary) over(order by enroll_date groups between 1 following and 3 following
--   exclude current row),
-- lead(salary) over(order by enroll_date groups between 1 following and 3 following exclude ties),
-- nth_value(salary, 1) over(order by enroll_date groups between 1 following and 3 following
--   exclude ties),
-- salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select last(salary) over(order by enroll_date groups between 1 following and 3 following
--   exclude group),
-- lag(salary) over(order by enroll_date groups between 1 following and 3 following exclude group),
-- salary, enroll_date from empsalary;

-- Show differences in offset interpretation between ROWS, RANGE, and GROUPS
WITH cte (x) AS (
        SELECT * FROM range(1, 36, 2)
)
SELECT x, (sum(x) over w)
FROM cte
WINDOW w AS (ORDER BY x rows between 1 preceding and 1 following);

WITH cte (x) AS (
        SELECT * FROM range(1, 36, 2)
)
SELECT x, (sum(x) over w)
FROM cte
WINDOW w AS (ORDER BY x range between 1 preceding and 1 following);

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- WITH cte (x) AS (
--         SELECT * FROM range(1, 36, 2)
-- )
-- SELECT x, (sum(x) over w)
-- FROM cte
-- WINDOW w AS (ORDER BY x groups between 1 preceding and 1 following);

WITH cte (x) AS (
        select 1 union all select 1 union all select 1 union all
        SELECT * FROM range(5, 50, 2)
)
SELECT x, (sum(x) over w)
FROM cte
WINDOW w AS (ORDER BY x rows between 1 preceding and 1 following);

WITH cte (x) AS (
        select 1 union all select 1 union all select 1 union all
        SELECT * FROM range(5, 50, 2)
)
SELECT x, (sum(x) over w)
FROM cte
WINDOW w AS (ORDER BY x range between 1 preceding and 1 following);

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- WITH cte (x) AS (
--         select 1 union all select 1 union all select 1 union all
--         SELECT * FROM range(5, 50, 2)
-- )
-- SELECT x, (sum(x) over w)
-- FROM cte
-- WINDOW w AS (ORDER BY x groups between 1 preceding and 1 following);

-- with UNION
SELECT count(*) OVER (PARTITION BY four) FROM (SELECT * FROM tenk1 UNION ALL SELECT * FROM tenk2)s LIMIT 0;

-- check some degenerate cases
create table t1 (f1 int, f2 int) using parquet;
insert into t1 values (1,1),(1,2),(2,2);

select f1, sum(f1) over (partition by f1
                         range between 1 preceding and 1 following)
from t1 where f1 = f2; -- error, must have order by

-- Since EXPLAIN clause rely on host physical location, it is commented out
-- explain
-- select f1, sum(f1) over (partition by f1 order by f2
-- range between 1 preceding and 1 following)
-- from t1 where f1 = f2;

select f1, sum(f1) over (partition by f1 order by f2
range between 1 preceding and 1 following)
from t1 where f1 = f2;

select f1, sum(f1) over (partition by f1, f1 order by f2
range between 2 preceding and 1 preceding)
from t1 where f1 = f2;

select f1, sum(f1) over (partition by f1, f2 order by f2
range between 1 following and 2 following)
from t1 where f1 = f2;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- select f1, sum(f1) over (partition by f1,
-- groups between 1 preceding and 1 following)
-- from t1 where f1 = f2;

-- Since EXPLAIN clause rely on host physical location, it is commented out
-- explain
-- select f1, sum(f1) over (partition by f1 order by f2
-- range between 1 preceding and 1 following)
-- from t1 where f1 = f2;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- select f1, sum(f1) over (partition by f1 order by f2
-- groups between 1 preceding and 1 following)
-- from t1 where f1 = f2;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- select f1, sum(f1) over (partition by f1, f1 order by f2
-- groups between 2 preceding and 1 preceding)
-- from t1 where f1 = f2;
 
-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- select f1, sum(f1) over (partition by f1, f2 order by f2
-- groups between 1 following and 2 following)
-- from t1 where f1 = f2;

-- ordering by a non-integer constant is allowed
SELECT rank() OVER (ORDER BY length('abc'));

-- can't order by another window function
-- [SPARK-28566] window functions should not be allowed in window definitions
-- SELECT rank() OVER (ORDER BY rank() OVER (ORDER BY random()));

-- some other errors
SELECT * FROM empsalary WHERE row_number() OVER (ORDER BY salary) < 10;

SELECT * FROM empsalary INNER JOIN tenk1 ON row_number() OVER (ORDER BY salary) < 10;

SELECT rank() OVER (ORDER BY 1), count(*) FROM empsalary GROUP BY 1;

SELECT * FROM rank() OVER (ORDER BY random());

-- Original query: DELETE FROM empsalary WHERE (rank() OVER (ORDER BY random())) > 10;
SELECT * FROM empsalary WHERE (rank() OVER (ORDER BY random())) > 10;

-- Original query: DELETE FROM empsalary RETURNING rank() OVER (ORDER BY random());
SELECT * FROM empsalary WHERE rank() OVER (ORDER BY random());

-- [SPARK-28645] Throw an error on window redefinition
-- select count(*) OVER w FROM tenk1 WINDOW w AS (ORDER BY unique1), w AS (ORDER BY unique1);

select rank() OVER (PARTITION BY four, ORDER BY ten) FROM tenk1;

-- [SPARK-28646] Allow usage of `count` only for parameterless aggregate function
-- select count() OVER () FROM tenk1;

-- The output is the expected one: `range` is not a window or aggregate function.
SELECT range(1, 100) OVER () FROM empsalary;

SELECT ntile(0) OVER (ORDER BY ten), ten, four FROM tenk1;

SELECT nth_value(four, 0) OVER (ORDER BY ten), ten, four FROM tenk1;

-- filter

-- [SPARK-30182] Support nested aggregates
-- SELECT sum(salary), row_number() OVER (ORDER BY depname), sum(
--     sum(salary) FILTER (WHERE enroll_date > '2007-01-01')
-- )
-- FROM empsalary GROUP BY depname;

-- Test pushdown of quals into a subquery containing window functions

-- pushdown is safe because all PARTITION BY clauses include depname:
-- Since EXPLAIN clause rely on host physical location, it is commented out
-- EXPLAIN
-- SELECT * FROM
-- (SELECT depname,
-- sum(salary) OVER (PARTITION BY depname) depsalary,
-- min(salary) OVER (PARTITION BY depname || 'A', depname) depminsalary
-- FROM empsalary) emp
-- WHERE depname = 'sales';

-- pushdown is unsafe because there's a PARTITION BY clause without depname:
-- Since EXPLAIN clause rely on host physical location, it is commented out
-- EXPLAIN
-- SELECT * FROM
-- (SELECT depname,
-- sum(salary) OVER (PARTITION BY enroll_date) enroll_salary,
-- min(salary) OVER (PARTITION BY depname) depminsalary
-- FROM empsalary) emp
-- WHERE depname = 'sales';

-- Test Sort node collapsing
-- Since EXPLAIN clause rely on host physical location, it is commented out
-- EXPLAIN
-- SELECT * FROM
-- (SELECT depname,
-- sum(salary) OVER (PARTITION BY depname order by empno) depsalary,
-- min(salary) OVER (PARTITION BY depname, empno order by enroll_date) depminsalary
-- FROM empsalary) emp
-- WHERE depname = 'sales';

-- Test Sort node reordering
-- Since EXPLAIN clause rely on host physical location, it is commented out
-- EXPLAIN
-- SELECT
-- lead(1) OVER (PARTITION BY depname ORDER BY salary, enroll_date),
-- lag(1) OVER (PARTITION BY depname ORDER BY salary,enroll_date,empno)
-- FROM empsalary;

-- cleanup
DROP TABLE empsalary;
DROP TABLE datetimes;
DROP TABLE t1;
