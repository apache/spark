--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
-- Window Functions Testing
-- https://github.com/postgres/postgres/blob/REL_12_BETA1/src/test/regress/sql/window.sql

-- The queries that are not (fully) available at Spark, I added an [ERROR] tag, so if you
-- are looking for queries to fix, you can just look for ERROR.

CREATE TABLE empsalary (
    depname string,
    empno integer,
    salary int,
    enroll_date date
) USING parquet;

INSERT INTO empsalary VALUES
('develop', 10, 5200, '2007-08-01'),
('sales', 1, 5000, '2006-10-01'),
('personnel', 5, 3500, '2007-12-10'),
('sales', 4, 4800, '2007-08-08'),
('personnel', 2, 3900, '2006-12-23'),
('develop', 7, 4200, '2008-01-01'),
('develop', 9, 4500, '2008-01-01'),
('sales', 3, 4800, '2007-08-01'),
('develop', 8, 6000, '2006-10-01'),
('develop', 11, 5200, '2007-08-15');

SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname) FROM empsalary ORDER BY depname, salary;

SELECT depname, empno, salary, rank() OVER (PARTITION BY depname ORDER BY salary) FROM empsalary;

SELECT four, ten, SUM(SUM(four)) OVER (PARTITION BY four), AVG(ten) FROM tenk1
GROUP BY four, ten ORDER BY four, ten;

SELECT depname, empno, salary, sum(salary) OVER w FROM empsalary WINDOW w AS (PARTITION BY depname);

-- [SPARK-28064] Order by does not accept a call to rank()
-- [ERROR] SELECT depname, empno, salary, rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary) ORDER BY rank() OVER w;

SELECT COUNT(*) OVER () FROM tenk1 WHERE unique2 < 10;

SELECT COUNT(*) OVER w FROM tenk1 WHERE unique2 < 10 WINDOW w AS ();

SELECT four FROM tenk1 WHERE FALSE WINDOW w AS (PARTITION BY ten);

SELECT sum(four) OVER (PARTITION BY ten ORDER BY unique2) AS sum_1, ten, four FROM tenk1 WHERE unique2 < 10;

SELECT row_number() OVER (ORDER BY unique2) FROM tenk1 WHERE unique2 < 10;

SELECT rank() OVER (PARTITION BY four ORDER BY ten) AS rank_1, ten, four FROM tenk1 WHERE unique2 < 10;

SELECT dense_rank() OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT percent_rank() OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT cume_dist() OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT ntile(3) OVER (ORDER BY ten, four), ten, four FROM tenk1 WHERE unique2 < 10;

-- [SPARK-28065] ntile does not accept NULL as input
-- [ERROR] SELECT ntile(NULL) OVER (ORDER BY ten, four), ten, four FROM tenk1 LIMIT 2;

-- Spark fills with NULL instead of white space
SELECT lag(ten) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- [SPARK-28068] `lag` second argument must be a literal in Spark
-- [ERROR] SELECT lag(ten, four) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;
-- [ERROR] SELECT lag(ten, four, 0) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT lead(ten) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT lead(ten * 2, 1) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT lead(ten * 2, 1, -1) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT first_value(ten) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT last_value(four) OVER (ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT last_value(ten) OVER (PARTITION BY four), ten, four FROM
(SELECT * FROM tenk1 WHERE unique2 < 10 ORDER BY four, ten)s
ORDER BY four, ten;

-- [SPARK-27764] Currently, Spark is missing nth_value
-- [ERROR] SELECT nth_value(ten, four + 1) OVER (PARTITION BY four), ten, four
-- FROM (SELECT * FROM tenk1 WHERE unique2 < 10 ORDER BY four, ten)s;

SELECT ten, two, sum(hundred) AS gsum, sum(sum(hundred)) OVER (PARTITION BY two ORDER BY ten) AS wsum
FROM tenk1 GROUP BY ten, two;

SELECT count(*) OVER (PARTITION BY four), four FROM (SELECT * FROM tenk1 WHERE two = 1)s WHERE unique2 < 10;

SELECT (count(*) OVER (PARTITION BY four ORDER BY ten) +
  sum(hundred) OVER (PARTITION BY four ORDER BY ten)) AS cntsum
  FROM tenk1 WHERE unique2 < 10;

SELECT * FROM(
  SELECT count(*) OVER (PARTITION BY four ORDER BY ten) +
    sum(hundred) OVER (PARTITION BY two ORDER BY ten) AS total,
    count(*) OVER (PARTITION BY four ORDER BY ten) AS fourcount,
    sum(hundred) OVER (PARTITION BY two ORDER BY ten) AS twosum
    FROM tenk1
)sub WHERE total <> fourcount + twosum;

SELECT avg(four) OVER (PARTITION BY four ORDER BY thousand / 100) FROM tenk1 WHERE unique2 < 10;

SELECT ten, two, sum(hundred) AS gsum, sum(sum(hundred)) OVER win AS wsum
FROM tenk1 GROUP BY ten, two WINDOW win AS (PARTITION BY two ORDER BY ten);

SELECT sum(salary),
row_number() OVER (ORDER BY depname),
sum(sum(salary)) OVER (ORDER BY depname DESC)
FROM empsalary GROUP BY depname;

SELECT sum(salary) OVER w1, count(*) OVER w2
FROM empsalary WINDOW w1 AS (ORDER BY salary), w2 AS (ORDER BY salary);

-- 
-- [ERROR] SELECT lead(ten, (SELECT two FROM tenk1 WHERE s.unique2 = unique2)) OVER (PARTITION BY four ORDER BY ten)
-- FROM tenk1 s WHERE unique2 < 10;

SELECT count(*) OVER (PARTITION BY four) FROM (SELECT * FROM tenk1 WHERE FALSE)s;

SELECT sum(salary) OVER w, rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);

SELECT empno, depname, salary, bonus, depadj, MIN(bonus) OVER (ORDER BY empno), MAX(depadj) OVER () FROM(
  SELECT *,
    CASE WHEN enroll_date < '2008-01-01' THEN 2008 - extract(YEAR FROM enroll_date) END * 500 AS bonus,
    CASE WHEN
      AVG(salary) OVER (PARTITION BY depname) < salary
      THEN 200 END AS depadj FROM empsalary
    )s;

create temporary view int4_tbl as select * from values
  (0),
  (123456),
  (-123456),
  (2147483647),
  (-2147483647)
  as int4_tbl(f1);

SELECT SUM(COUNT(f1)) OVER () FROM int4_tbl WHERE f1=42;

select ten,
  sum(unique1) + sum(unique2) as res,
  rank() over (order by sum(unique1) + sum(unique2)) as rank
from tenk1
group by ten order by ten;

select first_value(max(x)) over (), y
  from (select unique1 as x, ten+four as y from tenk1) ss
  group by y;

SELECT four, ten,
sum(ten) over (partition by four order by ten),
last_value(ten) over (partition by four order by ten)
FROM (select distinct ten, four from tenk1) ss;

SELECT four, ten,
sum(ten) over (partition by four order by ten range between unbounded preceding and current row),
last_value(ten) over (partition by four order by ten range between unbounded preceding and current row)
FROM (select distinct ten, four from tenk1) ss;

SELECT four, ten,
sum(ten) over (partition by four order by ten range between unbounded preceding and unbounded following),
last_value(ten) over (partition by four order by ten range between unbounded preceding and unbounded following)
FROM (select distinct ten, four from tenk1) ss;

SELECT four, ten/4 as two,
sum(ten/4) over (partition by four order by ten/4 range between unbounded preceding and current row),
last_value(ten/4) over (partition by four order by ten/4 range between unbounded preceding and current row)
FROM (select distinct ten, four from tenk1) ss;

SELECT four, ten/4 as two,
sum(ten/4) over (partition by four order by ten/4 rows between unbounded preceding and current row),
last_value(ten/4) over (partition by four order by ten/4 rows between unbounded preceding and current row)
FROM (select distinct ten, four from tenk1) ss;

SELECT sum(unique1) over (order by four range between current row and unbounded following),
unique1, four
FROM tenk1 WHERE unique1 < 10;

SELECT sum(unique1) over (rows between current row and unbounded following),
unique1, four
FROM tenk1 WHERE unique1 < 10;

SELECT sum(unique1) over (rows between 2 preceding and 2 following),
unique1, four
FROM tenk1 WHERE unique1 < 10;

-- Error in the following queries.
-- Related with `exclude` or the `following`
-- [ERROR] SELECT sum(unique1) over (rows between 2 preceding and 2 following exclude no others),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (rows between 2 preceding and 2 following exclude current row),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (rows between 2 preceding and 2 following exclude group),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (rows between 2 preceding and 2 following exclude ties),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;
-- SELECT first_value(unique1) over (ORDER BY four rows between current row and 2 following exclude current row),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT first_value(unique1) over (ORDER BY four rows between current row and 2 following exclude group),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT first_value(unique1) over (ORDER BY four rows between current row and 2 following exclude ties),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT last_value(unique1) over (ORDER BY four rows between current row and 2 following exclude current row),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT last_value(unique1) over (ORDER BY four rows between current row and 2 following exclude group),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT last_value(unique1) over (ORDER BY four rows between current row and 2 following exclude ties),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (rows between 2 preceding and 1 preceding),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (rows between 1 following and 3 following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (rows between unbounded preceding and 1 following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- Spark does not accept the window definition too far?
-- [ERROR] SELECT sum(unique1) over (w range between current row and unbounded following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10 WINDOW w AS (order by four);

-- [ERROR] SELECT sum(unique1) over (w range between unbounded preceding and current row exclude current row),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10 WINDOW w AS (order by four);

-- [ERROR] SELECT sum(unique1) over (w range between unbounded preceding and current row exclude group),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10 WINDOW w AS (order by four);

-- [ERROR] SELECT sum(unique1) over (w range between unbounded preceding and current row exclude ties),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10 WINDOW w AS (order by four);
--
-- [SPARK-27764] Currently, Spark is missing nth_value
-- [ERROR] SELECT first_value(unique1) over w,
-- nth_value(unique1, 2) over w AS nth_2,
-- last_value(unique1) over w, unique1, four
-- FROM tenk1 WHERE unique1 < 10
-- WINDOW w AS (order by four range between current row and unbounded following);

-- [ERROR] SELECT sum(unique1) over
-- (order by unique1
--   rows (SELECT unique1 FROM tenk1 ORDER BY unique1 LIMIT 1) + 1 PRECEDING),
-- unique1
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] CREATE TEMP VIEW v_window AS
-- SELECT i, sum(i) over (order by i rows between 1 preceding and 1 following) as sum_rows
-- FROM range(1, 10) i;

-- [ERROR] SELECT * FROM v_window;

-- [ERROR] CREATE OR REPLACE TEMP VIEW v_window AS
-- SELECT i, sum(i) over (order by i rows between 1 preceding and 1 following
--   exclude current row) as sum_rows FROM range(1, 10) i;

-- [ERROR] SELECT * FROM v_window;

-- [ERROR] CREATE OR REPLACE TEMP VIEW v_window AS
-- SELECT i, sum(i) over (order by i rows between 1 preceding and 1 following
--   exclude group) as sum_rows FROM range(1, 10) i;

-- [ERROR] SELECT * FROM v_window;

-- [ERROR] CREATE OR REPLACE TEMP VIEW v_window AS
-- SELECT i, sum(i) over (order by i rows between 1 preceding and 1 following
--   exclude ties) as sum_rows FROM generate_series(1, 10) i;

-- [ERROR] CREATE OR REPLACE TEMP VIEW v_window AS
-- SELECT i, sum(i) over (order by i rows between 1 preceding and 1 following
--   exclude no others) as sum_rows FROM generate_series(1, 10) i;

-- [ERROR] SELECT * FROM v_window;

-- [ERROR] CREATE OR REPLACE TEMP VIEW v_window AS
-- SELECT i, sum(i) over (order by i groups between 1 preceding and 1 following) as sum_rows FROM generate_series(1, 10) i;

-- [ERROR] SELECT * FROM v_window;

-- [ERROR] SELECT pg_get_viewdef('v_window');

-- [ERROR] DROP VIEW v_window;

-- [ERROR] CREATE TEMP VIEW v_window AS
-- SELECT i, min(i) over (order by i range between '1 day' preceding and '10 days' following) as min_i
--   FROM generate_series(now(), now()+'100 days'::interval, '1 hour') i;

-- RANGE offset PRECEDING/FOLLOWING tests
-- [ERROR] SELECT sum(unique1) over (order by four range between 2::int8 preceding and 1::int2 preceding),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four desc range between 2::int8 preceding and 1::int2 preceding),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four range between 2::int8 preceding and 1::int2 preceding exclude no others),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four range between 2::int8 preceding and 1::int2 preceding exclude current row),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four range between 2::int8 preceding and 1::int2 preceding exclude group),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four range between 2::int8 preceding and 1::int2 preceding exclude ties),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four range between 2::int8 preceding and 6::int2 following exclude ties),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four range between 2::int8 preceding and 6::int2 following exclude group),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (partition by four order by unique1 range between 5::int8 preceding and 6::int2 following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;
--
-- [ERROR] SELECT sum(unique1) over (partition by four order by unique1 range between 5::int8 preceding and 6::int2 following
--   exclude current row),unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following),
-- salary, enroll_date from empsalary;

-- [ERROR] select sum(salary) over (order by enroll_date desc range between '1 year'::interval preceding and '1 year'::interval following),
-- salary, enroll_date from empsalary;

-- [ERROR] select sum(salary) over (order by enroll_date desc range between '1 year'::interval following and '1 year'::interval following),
-- salary, enroll_date from empsalary;

-- [ERROR] select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following
--   exclude current row), salary, enroll_date from empsalary;

-- [ERROR] select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following
--   exclude group), salary, enroll_date from empsalary;

-- [ERROR] select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following
--   exclude ties), salary, enroll_date from empsalary;

-- [SPARK-27764] Currently, Spark is missing nth_value
-- [ERROR] select first_value(salary) over(order by salary range between 1000 preceding and 1000 following),
-- lead(salary) over(order by salary range between 1000 preceding and 1000 following),
-- nth_value(salary, 1) over(order by salary range between 1000 preceding and 1000 following),
-- salary from empsalary;

-- [SPARK-27764] Currently, Spark is missing nth_value
-- [ERROR] select last_value(salary) over(order by salary range between 1000 preceding and 1000 following),
-- lag(salary) over(order by salary range between 1000 preceding and 1000 following),
-- salary from empsalary;

-- [SPARK-27764] Currently, Spark is missing nth_value
-- [ERROR] select first_value(salary) over(order by salary range between 1000 following and 3000 following
--   exclude current row),
-- lead(salary) over(order by salary range between 1000 following and 3000 following exclude ties),
-- nth_value(salary, 1) over(order by salary range between 1000 following and 3000 following
--   exclude ties),
-- salary from empsalary;

-- [ERROR] select last_value(salary) over(order by salary range between 1000 following and 3000 following
--   exclude group),
-- lag(salary) over(order by salary range between 1000 following and 3000 following exclude group),
-- salary from empsalary;

-- [ERROR] select first_value(salary) over(order by enroll_date range between unbounded preceding and '1 year'::interval following
--   exclude ties),
-- last_value(salary) over(order by enroll_date range between unbounded preceding and '1 year'::interval following),
-- salary, enroll_date from empsalary;

-- [ERROR] select first_value(salary) over(order by enroll_date range between unbounded preceding and '1 year'::interval following
--   exclude ties),
-- last_value(salary) over(order by enroll_date range between unbounded preceding and '1 year'::interval following
--   exclude ties),
-- salary, enroll_date from empsalary;

-- [ERROR] select first_value(salary) over(order by enroll_date range between unbounded preceding and '1 year'::interval following
--   exclude group),
-- last_value(salary) over(order by enroll_date range between unbounded preceding and '1 year'::interval following
--   exclude group),
-- salary, enroll_date from empsalary;

-- [ERROR] select first_value(salary) over(order by enroll_date range between unbounded preceding and '1 year'::interval following
--   exclude current row),
-- last_value(salary) over(order by enroll_date range between unbounded preceding and '1 year'::interval following
--   exclude current row),
-- salary, enroll_date from empsalary;

-- in Spark, x is ambiguous for the following queries
-- [ERROR] select x, y,
--        first_value(y) over w,
--        last_value(y) over w
-- from
--   (select x, x as y from range(1,5) as x
--    union all select null, 42
--    union all select null, 43) ss
-- window w as
--   (order by x asc nulls first range between 2 preceding and 2 following);

-- [ERROR] select x, y,
--        first_value(y) over w,
--        last_value(y) over w
-- from
--   (select x, x as y from range(1,5) as x
--    union all select null, 42
--    union all select null, 43) ss
-- window w as
--   (order by x asc nulls last range between 2 preceding and 2 following);

-- [ERROR] select x, y,
--        first_value(y) over w,
--        last_value(y) over w
-- from
--   (select x, x as y from range(1,5) as x
--    union all select null, 42
--    union all select null, 43) ss
-- window w as
--   (order by x desc nulls first range between 2 preceding and 2 following);

-- [ERROR] select x, y,
--        first_value(y) over w,
--        last_value(y) over w
-- from
--   (select x, x as y from range(1,5) as x
--    union all select null, 42
--    union all select null, 43) ss
-- window w as
--   (order by x desc nulls last range between 2 preceding and 2 following);

-- Check overflow behavior for various integer sizes
-- [ERROR] select x, last_value(x) over (order by x range between current row and 2147450884 following)
-- from range(32764, 32766) x;

-- Spark does not define default column name as the name of the sub-query
-- instead, it calls the default column for `range()` as id
select x.id, last_value(x.id) over (order by x.id desc range between current row and 2147450885 following)
from range(-32766, -32764) x;

select x.id, last_value(x.id) over (order by x.id range between current row and 4 following)
from range(2147483644, 2147483646) x;

select x.id, last_value(x.id) over (order by x.id desc range between current row and 5 following)
from range(-2147483646, -2147483644) x;

select x.id, last_value(x.id) over (order by x.id range between current row and 4 following)
from range(9223372036854775804, 9223372036854775806) x;

select x.id, last_value(x.id) over (order by x.id desc range between current row and 5 following)
from range(-9223372036854775806, -9223372036854775804) x;

-- GROUPS tests
-- no `groups`?
-- [ERROR] SELECT sum(unique1) over (order by four groups between unbounded preceding and current row),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four groups between unbounded preceding and unbounded following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four groups between current row and unbounded following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four groups between 1 preceding and unbounded following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four groups between 1 following and unbounded following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four groups between unbounded preceding and 2 following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four groups between 2 preceding and 1 preceding),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four groups between 2 preceding and 1 following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four groups between 0 preceding and 0 following),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four groups between 2 preceding and 1 following
--   exclude current row), unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four groups between 2 preceding and 1 following
--   exclude group), unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (order by four groups between 2 preceding and 1 following
--   exclude ties), unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (partition by ten
--   order by four groups between 0 preceding and 0 following),unique1, four, ten
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (partition by ten
--   order by four groups between 0 preceding and 0 following exclude current row), unique1, four, ten
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (partition by ten
--   order by four groups between 0 preceding and 0 following exclude group), unique1, four, ten
-- FROM tenk1 WHERE unique1 < 10;

-- [ERROR] SELECT sum(unique1) over (partition by ten
--   order by four groups between 0 preceding and 0 following exclude ties), unique1, four, ten
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-27764] Currently, Spark is missing nth_value
-- [ERROR] select first_value(salary) over(order by enroll_date groups between 1 preceding and 1 following),
-- lead(salary) over(order by enroll_date groups between 1 preceding and 1 following),
-- nth_value(salary, 1) over(order by enroll_date groups between 1 preceding and 1 following),
-- salary, enroll_date from empsalary;

-- [ERROR] select last_value(salary) over(order by enroll_date groups between 1 preceding and 1 following),
-- lag(salary) over(order by enroll_date groups between 1 preceding and 1 following),
-- salary, enroll_date from empsalary;

-- [SPARK-27764] Currently, Spark is missing nth_value
-- [ERROR] select first_value(salary) over(order by enroll_date groups between 1 following and 3 following
--   exclude current row),
-- lead(salary) over(order by enroll_date groups between 1 following and 3 following exclude ties),
-- nth_value(salary, 1) over(order by enroll_date groups between 1 following and 3 following
--   exclude ties),
-- salary, enroll_date from empsalary;

-- [ERROR] select last_value(salary) over(order by enroll_date groups between 1 following and 3 following
--   exclude group),
-- lag(salary) over(order by enroll_date groups between 1 following and 3 following exclude group),
-- salary, enroll_date from empsalary;

-- Show differences in offset interpretation between ROWS, RANGE, and GROUPS
-- [ERROR] WITH cte (x) AS (
--         SELECT * FROM generate_series(1, 35, 2)
-- )
-- SELECT x, (sum(x) over w)
-- FROM cte
-- WINDOW w AS (ORDER BY x rows between 1 preceding and 1 following);

-- [ERROR] WITH cte (x) AS (
--         SELECT * FROM generate_series(1, 35, 2)
-- )
-- SELECT x, (sum(x) over w)
-- FROM cte
-- WINDOW w AS (ORDER BY x range between 1 preceding and 1 following);

-- [ERROR] WITH cte (x) AS (
--         SELECT * FROM generate_series(1, 35, 2)
-- )
-- SELECT x, (sum(x) over w)
-- FROM cte
-- WINDOW w AS (ORDER BY x groups between 1 preceding and 1 following);

-- [ERROR] WITH cte (x) AS (
--         select 1 union all select 1 union all select 1 union all
--         SELECT * FROM generate_series(5, 49, 2)
-- )
-- SELECT x, (sum(x) over w)
-- FROM cte
-- WINDOW w AS (ORDER BY x rows between 1 preceding and 1 following);

-- [ERROR] WITH cte (x) AS (
--         select 1 union all select 1 union all select 1 union all
--         SELECT * FROM generate_series(5, 49, 2)
-- )
-- SELECT x, (sum(x) over w)
-- FROM cte
-- WINDOW w AS (ORDER BY x range between 1 preceding and 1 following);

-- [ERROR] WITH cte (x) AS (
--         select 1 union all select 1 union all select 1 union all
--         SELECT * FROM generate_series(5, 49, 2)
-- )
-- SELECT x, (sum(x) over w)
-- FROM cte
-- WINDOW w AS (ORDER BY x groups between 1 preceding and 1 following);

-- with UNION
SELECT count(*) OVER (PARTITION BY four) FROM (SELECT * FROM tenk1 UNION ALL SELECT * FROM tenk2)s LIMIT 0;

create table t1 (f1 int, f2 int) using parquet;
insert into t1 values (1,1),(1,2),(2,2);

-- broken - costs and range are not available?
-- [ERROR] select f1, sum(f1) over (partition by f1
--                          range between 1 preceding and 1 following)
-- from t1 where f1 = f2;

-- [ERROR] explain (costs off)
-- select f1, sum(f1) over (partition by f1 order by f2
-- range between 1 preceding and 1 following)
-- from t1 where f1 = f2;
-- select f1, sum(f1) over (partition by f1 order by f2
--                          range between 1 preceding and 1 following)
-- from t1 where f1 = f2;

-- [ERROR] select f1, sum(f1) over (partition by f1, f1 order by f2
-- range between 2 preceding and 1 preceding)
-- from t1 where f1 = f2;

-- [ERROR] select f1, sum(f1) over (partition by f1, f2 order by f2
-- range between 1 following and 2 following)
-- from t1 where f1 = f2;

-- [ERROR] select f1, sum(f1) over (partition by f1
-- groups between 1 preceding and 1 following)
-- from t1 where f1 = f2;

-- [ERROR] explain (costs off)
-- select f1, sum(f1) over (partition by f1 order by f2
-- groups between 1 preceding and 1 following)
-- from t1 where f1 = f2;

-- [ERROR] select f1, sum(f1) over (partition by f1 order by f2
-- groups between 1 preceding and 1 following)
-- from t1 where f1 = f2;

-- [ERROR] select f1, sum(f1) over (partition by f1, f1 order by f2
-- groups between 2 preceding and 1 preceding)
-- from t1 where f1 = f2;

-- [ERROR] select f1, sum(f1) over (partition by f1, f2 order by f2
-- groups between 1 following and 2 following)
-- from t1 where f1 = f2;

SELECT rank() OVER (ORDER BY length('abc'));

-- [SPARK-28086] Adds `random()` to Spark
-- [ERROR] SELECT rank() OVER (ORDER BY rank() OVER (ORDER BY random()));

-- some other errors
-- [ERROR] SELECT * FROM empsalary WHERE row_number() OVER (ORDER BY salary) < 10;

-- [ERROR] SELECT * FROM empsalary INNER JOIN tenk1 ON row_number() OVER (ORDER BY salary) < 10;

-- [ERROR] SELECT rank() OVER (ORDER BY 1), count(*) FROM empsalary GROUP BY 1;

-- [SPARK-28086] Adds `random()` to Spark
-- [ERROR] SELECT * FROM rank() OVER (ORDER BY random());

SELECT count(*) OVER w FROM tenk1 WINDOW w AS (ORDER BY unique1), w AS (ORDER BY unique1);

-- It does not work. Maybe it is related to not defining a window?
-- [ERROR] SELECT rank() OVER (PARTITION BY four, ORDER BY ten) FROM tenk1;

SELECT count() OVER () FROM tenk1;


-- Ok, first I migrated a call to `generate_series()`, given by Postgres, to
-- the equivalent Spark function, called `range()`. But `range()` seems less
-- flexible than `generate_series()`
-- [ERROR] SELECT range(1, 100) OVER () FROM empsalary;

-- the error is weird
-- [ERROR] SELECT ntile(0) OVER (ORDER BY ten), ten, four FROM tenk1;

-- [SPARK-27764] Currently, Spark is missing nth_value
-- [ERROR] SELECT nth_value(four, 0) OVER (ORDER BY ten), ten, four FROM tenk1;

-- FILTER does not work on spark?
-- [ERROR] SELECT sum(salary), row_number() OVER (ORDER BY depname), sum(
--     sum(salary) FILTER (WHERE enroll_date > '2007-01-01')
-- ) FILTER (WHERE depname <> 'sales') OVER (ORDER BY depname DESC) AS "filtered_sum",
--     depname
-- FROM empsalary GROUP BY depname;

-- costs off didn't work?
-- [ERROR] EXPLAIN (COSTS OFF)
-- SELECT * FROM
-- (SELECT depname,
-- sum(salary) OVER (PARTITION BY depname) depsalary,
-- min(salary) OVER (PARTITION BY depname || 'A', depname) depminsalary
-- FROM empsalary) emp
-- WHERE depname = 'sales';

-- [ERROR] EXPLAIN (COSTS OFF)
-- SELECT * FROM
-- (SELECT depname,
-- sum(salary) OVER (PARTITION BY enroll_date) enroll_salary,
-- min(salary) OVER (PARTITION BY depname) depminsalary
-- FROM empsalary) emp
-- WHERE depname = 'sales';

-- [ERROR] EXPLAIN (COSTS OFF)
-- SELECT * FROM
-- (SELECT depname,
-- sum(salary) OVER (PARTITION BY depname order by empno) depsalary,
-- min(salary) OVER (PARTITION BY depname, empno order by enroll_date) depminsalary
-- FROM empsalary) emp
-- WHERE depname = 'sales';

-- [ERROR] EXPLAIN (COSTS OFF)
-- SELECT
-- lead(1) OVER (PARTITION BY depname ORDER BY salary, enroll_date),
-- lag(1) OVER (PARTITION BY depname ORDER BY salary,enroll_date,empno)
-- FROM empsalary;

SELECT i,AVG(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,AVG(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,AVG(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,AVG(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1.5),(2,2.5),(3,NULL),(4,NULL)) t(i,v);

SELECT i,AVG(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,'1 sec'),(2,'2 sec'),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,'1.10'),(2,'2.20'),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,'1 sec'),(2,'2 sec'),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1.1),(2,2.2),(3,NULL),(4,NULL)) t(i,v);

SELECT SUM(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1.01),(2,2),(3,3)) v(i,n);

SELECT i,COUNT(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,COUNT(*) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT VAR_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VARIANCE(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VARIANCE(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VARIANCE(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VARIANCE(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT STDDEV_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(0,NULL),(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT STDDEV(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(0,NULL),(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT STDDEV(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(0,NULL),(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT STDDEV(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(0,NULL),(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND CURRENT ROW)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,3),(4,4)) t(i,v);

-- [SPARK-27880] Implement boolean aggregates(BOOL_AND, BOOL_OR and EVERY)
-- [ERROR] SELECT i, b, bool_and(b) OVER w, bool_or(b) OVER w
--   FROM (VALUES (1,true), (2,true), (3,false), (4,false), (5,true)) v(i,b)
--   WINDOW w AS (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING);

-- cleanup
drop table empsalary;
drop table t1;
drop view int4_tbl;
