-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
-- Window Functions Testing
-- https://github.com/postgres/postgres/blob/REL_12_STABLE/src/test/regress/sql/window.sql#L1-L319

-- Test window operator with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

CREATE TEMPORARY VIEW tenk2 AS SELECT * FROM tenk1;

-- [SPARK-29540] Thrift in some cases can't parse string to date
-- CREATE TABLE empsalary (
--     depname string,
--     empno integer,
--     salary int,
--     enroll_date date
-- ) USING parquet;

-- [SPARK-29540] Thrift in some cases can't parse string to date
-- INSERT INTO empsalary VALUES ('develop', 10, 5200, '2007-08-01');
-- INSERT INTO empsalary VALUES ('sales', 1, 5000, '2006-10-01');
-- INSERT INTO empsalary VALUES ('personnel', 5, 3500, '2007-12-10');
-- INSERT INTO empsalary VALUES ('sales', 4, 4800, '2007-08-08');
-- INSERT INTO empsalary VALUES ('personnel', 2, 3900, '2006-12-23');
-- INSERT INTO empsalary VALUES ('develop', 7, 4200, '2008-01-01');
-- INSERT INTO empsalary VALUES ('develop', 9, 4500, '2008-01-01');
-- INSERT INTO empsalary VALUES ('sales', 3, 4800, '2007-08-01');
-- INSERT INTO empsalary VALUES ('develop', 8, 6000, '2006-10-01');
-- INSERT INTO empsalary VALUES ('develop', 11, 5200, '2007-08-15');

-- [SPARK-29540] Thrift in some cases can't parse string to date
-- SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname) FROM empsalary ORDER BY depname, salary;

-- [SPARK-29540] Thrift in some cases can't parse string to date
-- SELECT depname, empno, salary, rank() OVER (PARTITION BY depname ORDER BY salary) FROM empsalary;

-- with GROUP BY
SELECT four, ten, SUM(SUM(four)) OVER (PARTITION BY four), AVG(ten) FROM tenk1
GROUP BY four, ten ORDER BY four, ten;

-- [SPARK-29540] Thrift in some cases can't parse string to date
-- SELECT depname, empno, salary, sum(salary) OVER w FROM empsalary WINDOW w AS (PARTITION BY depname);

-- [SPARK-28064] Order by does not accept a call to rank()
-- SELECT depname, empno, salary, rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary) ORDER BY rank() OVER w;

-- empty window specification
SELECT COUNT(*) OVER () FROM tenk1 WHERE unique2 < 10;

SELECT COUNT(*) OVER w FROM tenk1 WHERE unique2 < 10 WINDOW w AS ();

-- no window operation
SELECT four FROM tenk1 WHERE FALSE WINDOW w AS (PARTITION BY ten);

-- cumulative aggregate
SELECT sum(four) OVER (PARTITION BY ten ORDER BY unique2) AS sum_1, ten, four FROM tenk1 WHERE unique2 < 10;

SELECT row_number() OVER (ORDER BY unique2) FROM tenk1 WHERE unique2 < 10;

SELECT rank() OVER (PARTITION BY four ORDER BY ten) AS rank_1, ten, four FROM tenk1 WHERE unique2 < 10;

SELECT dense_rank() OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT percent_rank() OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT cume_dist() OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT ntile(3) OVER (ORDER BY ten, four), ten, four FROM tenk1 WHERE unique2 < 10;

-- [SPARK-28065] ntile does not accept NULL as input
-- SELECT ntile(NULL) OVER (ORDER BY ten, four), ten, four FROM tenk1 LIMIT 2;

SELECT lag(ten) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- [SPARK-28068] `lag` second argument must be a literal in Spark
-- SELECT lag(ten, four) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- [SPARK-28068] `lag` second argument must be a literal in Spark
-- SELECT lag(ten, four, 0) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT lead(ten) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT lead(ten * 2, 1) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT lead(ten * 2, 1, -1) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT first(ten) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- last returns the last row of the frame, which is CURRENT ROW in ORDER BY window.
SELECT last(four) OVER (ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

SELECT last(ten) OVER (PARTITION BY four), ten, four FROM
(SELECT * FROM tenk1 WHERE unique2 < 10 ORDER BY four, ten)s
ORDER BY four, ten;

-- [SPARK-30707] Lead/Lag window function throws AnalysisException without ORDER BY clause
-- SELECT nth_value(ten, four + 1) OVER (PARTITION BY four), ten, four
-- FROM (SELECT * FROM tenk1 WHERE unique2 < 10 ORDER BY four, ten)s;

SELECT ten, two, sum(hundred) AS gsum, sum(sum(hundred)) OVER (PARTITION BY two ORDER BY ten) AS wsum
FROM tenk1 GROUP BY ten, two;

SELECT count(*) OVER (PARTITION BY four), four FROM (SELECT * FROM tenk1 WHERE two = 1)s WHERE unique2 < 10;

SELECT (count(*) OVER (PARTITION BY four ORDER BY ten) +
  sum(hundred) OVER (PARTITION BY four ORDER BY ten)) AS cntsum
  FROM tenk1 WHERE unique2 < 10;

-- opexpr with different windows evaluation.
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

-- [SPARK-29540] Thrift in some cases can't parse string to date
-- more than one window with GROUP BY
-- SELECT sum(salary),
--   row_number() OVER (ORDER BY depname),
--   sum(sum(salary)) OVER (ORDER BY depname DESC)
-- FROM empsalary GROUP BY depname;

-- [SPARK-29540] Thrift in some cases can't parse string to date
-- identical windows with different names
-- SELECT sum(salary) OVER w1, count(*) OVER w2
-- FROM empsalary WINDOW w1 AS (ORDER BY salary), w2 AS (ORDER BY salary);

-- subplan
-- Cannot specify window frame for lead function
-- SELECT lead(ten, (SELECT two FROM tenk1 WHERE s.unique2 = unique2)) OVER (PARTITION BY four ORDER BY ten)
-- FROM tenk1 s WHERE unique2 < 10;

-- empty table
SELECT count(*) OVER (PARTITION BY four) FROM (SELECT * FROM tenk1 WHERE FALSE)s;

-- [SPARK-29540] Thrift in some cases can't parse string to date
-- mixture of agg/wfunc in the same window
-- SELECT sum(salary) OVER w, rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);

-- Cannot safely cast 'enroll_date': string to date;
-- SELECT empno, depname, salary, bonus, depadj, MIN(bonus) OVER (ORDER BY empno), MAX(depadj) OVER () FROM(
-- SELECT *,
--   CASE WHEN enroll_date < '2008-01-01' THEN 2008 - extract(year FROM enroll_date) END * 500 AS bonus,
--   CASE WHEN
--     AVG(salary) OVER (PARTITION BY depname) < salary
--     THEN 200 END AS depadj FROM empsalary
--   )s;

create temporary view int4_tbl as select * from values
  (0),
  (123456),
  (-123456),
  (2147483647),
  (-2147483647)
  as int4_tbl(f1);

-- window function over ungrouped agg over empty row set (bug before 9.1)
SELECT SUM(COUNT(f1)) OVER () FROM int4_tbl WHERE f1=42;

-- window function with ORDER BY an expression involving aggregates (9.1 bug)
select ten,
  sum(unique1) + sum(unique2) as res,
  rank() over (order by sum(unique1) + sum(unique2)) as rank
from tenk1
group by ten order by ten;

-- window and aggregate with GROUP BY expression (9.2 bug)
-- explain
-- select first(max(x)) over (), y
--   from (select unique1 as x, ten+four as y from tenk1) ss
--   group by y;

-- test non-default frame specifications
SELECT four, ten,
sum(ten) over (partition by four order by ten),
last(ten) over (partition by four order by ten)
FROM (select distinct ten, four from tenk1) ss;

SELECT four, ten,
sum(ten) over (partition by four order by ten range between unbounded preceding and current row),
last(ten) over (partition by four order by ten range between unbounded preceding and current row)
FROM (select distinct ten, four from tenk1) ss;

SELECT four, ten,
sum(ten) over (partition by four order by ten range between unbounded preceding and unbounded following),
last(ten) over (partition by four order by ten range between unbounded preceding and unbounded following)
FROM (select distinct ten, four from tenk1) ss;

-- [SPARK-29451] Some queries with divisions in SQL windows are failling in Thrift
-- SELECT four, ten/4 as two,
-- sum(ten/4) over (partition by four order by ten/4 range between unbounded preceding and current row),
-- last(ten/4) over (partition by four order by ten/4 range between unbounded preceding and current row)
-- FROM (select distinct ten, four from tenk1) ss;

-- [SPARK-29451] Some queries with divisions in SQL windows are failling in Thrift
-- SELECT four, ten/4 as two,
-- sum(ten/4) over (partition by four order by ten/4 rows between unbounded preceding and current row),
-- last(ten/4) over (partition by four order by ten/4 rows between unbounded preceding and current row)
-- FROM (select distinct ten, four from tenk1) ss;

SELECT sum(unique1) over (order by four range between current row and unbounded following),
unique1, four
FROM tenk1 WHERE unique1 < 10;

SELECT sum(unique1) over (rows between current row and unbounded following),
unique1, four
FROM tenk1 WHERE unique1 < 10;

SELECT sum(unique1) over (rows between 2 preceding and 2 following),
unique1, four
FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (rows between 2 preceding and 2 following exclude no others),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (rows between 2 preceding and 2 following exclude current row),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (rows between 2 preceding and 2 following exclude group),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (rows between 2 preceding and 2 following exclude ties),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT first(unique1) over (ORDER BY four rows between current row and 2 following exclude current row),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT first(unique1) over (ORDER BY four rows between current row and 2 following exclude group),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT first(unique1) over (ORDER BY four rows between current row and 2 following exclude ties),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT last(unique1) over (ORDER BY four rows between current row and 2 following exclude current row),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT last(unique1) over (ORDER BY four rows between current row and 2 following exclude group),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT last(unique1) over (ORDER BY four rows between current row and 2 following exclude ties),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

SELECT sum(unique1) over (rows between 2 preceding and 1 preceding),
unique1, four
FROM tenk1 WHERE unique1 < 10;

SELECT sum(unique1) over (rows between 1 following and 3 following),
unique1, four
FROM tenk1 WHERE unique1 < 10;

SELECT sum(unique1) over (rows between unbounded preceding and 1 following),
unique1, four
FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (w range between current row and unbounded following),
-- 	unique1, four
-- FROM tenk1 WHERE unique1 < 10 WINDOW w AS (order by four);

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (w range between unbounded preceding and current row exclude current row),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10 WINDOW w AS (order by four);

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (w range between unbounded preceding and current row exclude group),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10 WINDOW w AS (order by four);

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (w range between unbounded preceding and current row exclude ties),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10 WINDOW w AS (order by four);

-- [SPARK-30707] Lead/Lag window function throws AnalysisException without ORDER BY clause
-- SELECT first_value(unique1) over w,
-- nth_value(unique1, 2) over w AS nth_2,
-- last_value(unique1) over w, unique1, four
-- FROM tenk1 WHERE unique1 < 10
-- WINDOW w AS (order by four range between current row and unbounded following);

-- [SPARK-28501] Frame bound value must be a literal.
-- SELECT sum(unique1) over
-- (order by unique1
--   rows (SELECT unique1 FROM tenk1 ORDER BY unique1 LIMIT 1) + 1 PRECEDING),
-- unique1
-- FROM tenk1 WHERE unique1 < 10;

CREATE TEMP VIEW v_window AS
SELECT i.id, sum(i.id) over (order by i.id rows between 1 preceding and 1 following) as sum_rows
FROM range(1, 11) i;

SELECT * FROM v_window;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- CREATE OR REPLACE TEMP VIEW v_window AS
-- SELECT i, sum(i) over (order by i rows between 1 preceding and 1 following
--   exclude current row) as sum_rows FROM range(1, 10) i;

-- SELECT * FROM v_window;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- CREATE OR REPLACE TEMP VIEW v_window AS
-- SELECT i, sum(i) over (order by i rows between 1 preceding and 1 following
--   exclude group) as sum_rows FROM range(1, 10) i;
-- SELECT * FROM v_window;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- CREATE OR REPLACE TEMP VIEW v_window AS
-- SELECT i, sum(i) over (order by i rows between 1 preceding and 1 following
--   exclude ties) as sum_rows FROM generate_series(1, 10) i;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- CREATE OR REPLACE TEMP VIEW v_window AS
-- SELECT i, sum(i) over (order by i rows between 1 preceding and 1 following
--   exclude no others) as sum_rows FROM generate_series(1, 10) i;
-- SELECT * FROM v_window;

-- [SPARK-28648] Adds support to `groups` unit type in window clauses
-- CREATE OR REPLACE TEMP VIEW v_window AS
-- SELECT i.id, sum(i.id) over (order by i.id groups between 1 preceding and 1 following) as sum_rows FROM range(1, 11) i;
-- SELECT * FROM v_window;

DROP VIEW v_window;
-- [SPARK-29540] Thrift in some cases can't parse string to date
-- DROP TABLE empsalary;
DROP VIEW tenk2;
DROP VIEW int4_tbl;
