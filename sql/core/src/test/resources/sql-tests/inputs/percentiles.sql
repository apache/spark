-- Test data.
CREATE OR REPLACE TEMPORARY VIEW aggr AS SELECT * FROM VALUES
(0, 0), (0, 10), (0, 20), (0, 30), (0, 40), (1, 10), (1, 20), (2, 10), (2, 20), (2, 25), (2, 30), (3, 60), (4, null)
AS aggr(k, v);

CREATE OR REPLACE TEMPORARY VIEW basic_pays AS SELECT * FROM VALUES
('Diane Murphy','Accounting',8435),
('Mary Patterson','Accounting',9998),
('Jeff Firrelli','Accounting',8992),
('William Patterson','Accounting',8870),
('Gerard Bondur','Accounting',11472),
('Anthony Bow','Accounting',6627),
('Leslie Jennings','IT',8113),
('Leslie Thompson','IT',5186),
('Julie Firrelli','Sales',9181),
('Steve Patterson','Sales',9441),
('Foon Yue Tseng','Sales',6660),
('George Vanauf','Sales',10563),
('Loui Bondur','SCM',10449),
('Gerard Hernandez','SCM',6949),
('Pamela Castillo','SCM',11303),
('Larry Bott','SCM',11798),
('Barry Jones','SCM',10586)
AS basic_pays(employee_name, department, salary);

SELECT
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v) FILTER (WHERE k > 0),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC) FILTER (WHERE k > 0)
FROM aggr;

SELECT
  k,
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v) FILTER (WHERE k > 0),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC) FILTER (WHERE k > 0)
FROM aggr
GROUP BY k
ORDER BY k;

SELECT
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v) FILTER (WHERE k > 0),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v DESC),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v DESC) FILTER (WHERE k > 0)
FROM aggr;

SELECT
  k,
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v) FILTER (WHERE k > 0),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v DESC),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v DESC) FILTER (WHERE k > 0)
FROM aggr
GROUP BY k
ORDER BY k;

SELECT
  median(v),
  percentile(v, 0.5),
  percentile_cont(0.5) WITHIN GROUP (ORDER BY v)
FROM aggr;

SELECT
  round(v, 0) WITHIN GROUP (ORDER BY v)
FROM aggr;

SELECT
  round(v, 0) WITHIN GROUP (ORDER BY v) OVER (PARTITION BY k)
FROM aggr;

SELECT
  percentile(v, 0.5) WITHIN GROUP (ORDER BY v)
FROM aggr;

SELECT
  percentile(v, 0.5) WITHIN GROUP (ORDER BY v) OVER (PARTITION BY k)
FROM aggr;

SELECT
  percentile_cont(DISTINCT 0.5) WITHIN GROUP (ORDER BY v)
FROM aggr;

SELECT
  percentile_cont(DISTINCT 0.5) WITHIN GROUP (ORDER BY v) OVER (PARTITION BY k)
FROM aggr;

SELECT
  percentile_cont() WITHIN GROUP (ORDER BY v)
FROM aggr;

SELECT
  percentile_cont() WITHIN GROUP (ORDER BY v) OVER (PARTITION BY k)
FROM aggr;

SELECT
  percentile_cont(0.5)
FROM aggr;

SELECT
  percentile_cont(0.5) OVER (PARTITION BY k)
FROM aggr;

SELECT
  percentile_cont(0.5) WITHIN GROUP (ORDER BY k, v)
FROM aggr;

SELECT
  percentile_cont(k, 0.5) WITHIN GROUP (ORDER BY v)
FROM aggr;

SELECT
  k,
  median(v),
  percentile(v, 0.5),
  percentile_cont(0.5) WITHIN GROUP (ORDER BY v)
FROM aggr
GROUP BY k
ORDER BY k;

SELECT
    employee_name,
    department,
    salary,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department),
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department),
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER (PARTITION BY department),
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER (PARTITION BY department)
FROM basic_pays
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department ORDER BY salary),
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER (PARTITION BY department ORDER BY salary)
FROM basic_pays
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department ORDER BY salary),
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER (PARTITION BY department ORDER BY salary)
FROM basic_pays
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    median(salary) OVER (PARTITION BY department ORDER BY salary)
FROM basic_pays
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING),
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER (PARTITION BY department ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)
FROM basic_pays
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING),
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER (PARTITION BY department ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)
FROM basic_pays
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    median(salary) OVER (PARTITION BY department ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)
FROM basic_pays
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary) OVER w,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary) OVER w,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER w,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER w
FROM basic_pays
WINDOW w AS (PARTITION BY department)
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    median(salary) OVER w,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY salary) OVER w,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY salary) OVER w,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY salary DESC) OVER w,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY salary DESC) OVER w
FROM basic_pays
WHERE salary > 8900
WINDOW w AS (PARTITION BY department)
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary) OVER w,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER w
FROM basic_pays
WINDOW w AS (PARTITION BY department ORDER BY salary)
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary) OVER w,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER w
FROM basic_pays
WINDOW w AS (PARTITION BY department ORDER BY salary)
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    median(salary) OVER w
FROM basic_pays
WINDOW w AS (PARTITION BY department ORDER BY salary)
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary) OVER w,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER w
FROM basic_pays
WINDOW w AS (PARTITION BY department ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary) OVER w,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER w
FROM basic_pays
WINDOW w AS (PARTITION BY department ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    median(salary) OVER w
FROM basic_pays
WINDOW w AS (PARTITION BY department ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)
ORDER BY salary;

CREATE OR REPLACE TEMPORARY VIEW intervals AS SELECT * FROM VALUES
(0, INTERVAL '0' MONTH, INTERVAL '0' SECOND, INTERVAL '0' MINUTE),
(0, INTERVAL '10' MONTH, INTERVAL '10' SECOND, INTERVAL '10' MINUTE),
(0, INTERVAL '20' MONTH, INTERVAL '20' SECOND, INTERVAL '20' MINUTE),
(0, INTERVAL '30' MONTH, INTERVAL '30' SECOND, INTERVAL '30' MINUTE),
(0, INTERVAL '40' MONTH, INTERVAL '40' SECOND, INTERVAL '40' MINUTE),
(1, INTERVAL '10' MONTH, INTERVAL '10' SECOND, INTERVAL '10' MINUTE),
(1, INTERVAL '20' MONTH, INTERVAL '20' SECOND, INTERVAL '20' MINUTE),
(2, INTERVAL '10' MONTH, INTERVAL '10' SECOND, INTERVAL '10' MINUTE),
(2, INTERVAL '20' MONTH, INTERVAL '20' SECOND, INTERVAL '20' MINUTE),
(2, INTERVAL '25' MONTH, INTERVAL '25' SECOND, INTERVAL '25' MINUTE),
(2, INTERVAL '30' MONTH, INTERVAL '30' SECOND, INTERVAL '30' MINUTE),
(3, INTERVAL '60' MONTH, INTERVAL '60' SECOND, INTERVAL '60' MINUTE),
(4, null, null, null)
AS intervals(k, dt, ym, dt2);

SELECT
  percentile_cont(0.25) WITHIN GROUP (ORDER BY dt),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY dt DESC)
FROM intervals;

SELECT
  k,
  percentile_cont(0.25) WITHIN GROUP (ORDER BY ym),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY ym DESC)
FROM intervals
GROUP BY k
ORDER BY k;

SELECT
  k,
  percentile_cont(0.25) WITHIN GROUP (ORDER BY dt2),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY dt2 DESC)
FROM intervals
GROUP BY k
ORDER BY k;

SELECT
  percentile_disc(0.25) WITHIN GROUP (ORDER BY dt),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY dt DESC)
FROM intervals;

SELECT
  k,
  percentile_disc(0.25) WITHIN GROUP (ORDER BY ym),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY ym DESC)
FROM intervals
GROUP BY k
ORDER BY k;

SELECT
  k,
  percentile_disc(0.25) WITHIN GROUP (ORDER BY dt2),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY dt2 DESC)
FROM intervals
GROUP BY k
ORDER BY k;

SELECT
  median(dt),
  percentile(dt, 0.5),
  percentile_cont(0.5) WITHIN GROUP (ORDER BY dt)
FROM intervals;

SELECT
  k,
  median(ym),
  percentile(ym, 0.5),
  percentile_cont(0.5) WITHIN GROUP (ORDER BY ym)
FROM intervals
GROUP BY k
ORDER BY k;

SELECT
  k,
  median(dt2),
  percentile(dt2, 0.5),
  percentile_cont(0.5) WITHIN GROUP (ORDER BY dt2)
FROM intervals
GROUP BY k
ORDER BY k;

-- SPARK-44871: Fix percentile_disc behaviour
SELECT
  percentile_disc(0.0) WITHIN GROUP (ORDER BY a) as p0,
  percentile_disc(0.1) WITHIN GROUP (ORDER BY a) as p1,
  percentile_disc(0.2) WITHIN GROUP (ORDER BY a) as p2,
  percentile_disc(0.3) WITHIN GROUP (ORDER BY a) as p3,
  percentile_disc(0.4) WITHIN GROUP (ORDER BY a) as p4,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY a) as p5,
  percentile_disc(0.6) WITHIN GROUP (ORDER BY a) as p6,
  percentile_disc(0.7) WITHIN GROUP (ORDER BY a) as p7,
  percentile_disc(0.8) WITHIN GROUP (ORDER BY a) as p8,
  percentile_disc(0.9) WITHIN GROUP (ORDER BY a) as p9,
  percentile_disc(1.0) WITHIN GROUP (ORDER BY a) as p10
FROM VALUES (0) AS v(a);

SELECT
  percentile_disc(0.0) WITHIN GROUP (ORDER BY a) as p0,
  percentile_disc(0.1) WITHIN GROUP (ORDER BY a) as p1,
  percentile_disc(0.2) WITHIN GROUP (ORDER BY a) as p2,
  percentile_disc(0.3) WITHIN GROUP (ORDER BY a) as p3,
  percentile_disc(0.4) WITHIN GROUP (ORDER BY a) as p4,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY a) as p5,
  percentile_disc(0.6) WITHIN GROUP (ORDER BY a) as p6,
  percentile_disc(0.7) WITHIN GROUP (ORDER BY a) as p7,
  percentile_disc(0.8) WITHIN GROUP (ORDER BY a) as p8,
  percentile_disc(0.9) WITHIN GROUP (ORDER BY a) as p9,
  percentile_disc(1.0) WITHIN GROUP (ORDER BY a) as p10
FROM VALUES (0), (1) AS v(a);

SELECT
  percentile_disc(0.0) WITHIN GROUP (ORDER BY a) as p0,
  percentile_disc(0.1) WITHIN GROUP (ORDER BY a) as p1,
  percentile_disc(0.2) WITHIN GROUP (ORDER BY a) as p2,
  percentile_disc(0.3) WITHIN GROUP (ORDER BY a) as p3,
  percentile_disc(0.4) WITHIN GROUP (ORDER BY a) as p4,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY a) as p5,
  percentile_disc(0.6) WITHIN GROUP (ORDER BY a) as p6,
  percentile_disc(0.7) WITHIN GROUP (ORDER BY a) as p7,
  percentile_disc(0.8) WITHIN GROUP (ORDER BY a) as p8,
  percentile_disc(0.9) WITHIN GROUP (ORDER BY a) as p9,
  percentile_disc(1.0) WITHIN GROUP (ORDER BY a) as p10
FROM VALUES (0), (1), (2) AS v(a);

SELECT
  percentile_disc(0.0) WITHIN GROUP (ORDER BY a) as p0,
  percentile_disc(0.1) WITHIN GROUP (ORDER BY a) as p1,
  percentile_disc(0.2) WITHIN GROUP (ORDER BY a) as p2,
  percentile_disc(0.3) WITHIN GROUP (ORDER BY a) as p3,
  percentile_disc(0.4) WITHIN GROUP (ORDER BY a) as p4,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY a) as p5,
  percentile_disc(0.6) WITHIN GROUP (ORDER BY a) as p6,
  percentile_disc(0.7) WITHIN GROUP (ORDER BY a) as p7,
  percentile_disc(0.8) WITHIN GROUP (ORDER BY a) as p8,
  percentile_disc(0.9) WITHIN GROUP (ORDER BY a) as p9,
  percentile_disc(1.0) WITHIN GROUP (ORDER BY a) as p10
FROM VALUES (0), (1), (2), (3), (4) AS v(a);

SET spark.sql.legacy.percentileDiscCalculation = true;

SELECT
  percentile_disc(0.0) WITHIN GROUP (ORDER BY a) as p0,
  percentile_disc(0.1) WITHIN GROUP (ORDER BY a) as p1,
  percentile_disc(0.2) WITHIN GROUP (ORDER BY a) as p2,
  percentile_disc(0.3) WITHIN GROUP (ORDER BY a) as p3,
  percentile_disc(0.4) WITHIN GROUP (ORDER BY a) as p4,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY a) as p5,
  percentile_disc(0.6) WITHIN GROUP (ORDER BY a) as p6,
  percentile_disc(0.7) WITHIN GROUP (ORDER BY a) as p7,
  percentile_disc(0.8) WITHIN GROUP (ORDER BY a) as p8,
  percentile_disc(0.9) WITHIN GROUP (ORDER BY a) as p9,
  percentile_disc(1.0) WITHIN GROUP (ORDER BY a) as p10
FROM VALUES (0), (1), (2), (3), (4) AS v(a);

SELECT
  percentile_cont(b) WITHIN GROUP (ORDER BY a DESC) as p0
FROM values (12, 0.25), (13, 0.25), (22, 0.25) as v(a, b);

SET spark.sql.legacy.percentileDiscCalculation = false;
