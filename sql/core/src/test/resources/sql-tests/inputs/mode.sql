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
  mode() WITHIN GROUP (ORDER BY v DESC)
FROM aggr;

SELECT
  mode() WITHIN GROUP (ORDER BY v),
  mode() WITHIN GROUP (ORDER BY v) FILTER (WHERE k > 0)
FROM aggr;

SELECT
  k,
  mode() WITHIN GROUP (ORDER BY v),
  mode() WITHIN GROUP (ORDER BY v) FILTER (WHERE k > 0)
FROM aggr
GROUP BY k
ORDER BY k;

SELECT
    employee_name,
    department,
    salary,
    mode() WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department)
FROM basic_pays
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    mode() WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department ORDER BY salary)
FROM basic_pays
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    mode() WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)
FROM basic_pays
ORDER BY salary;

SELECT
    employee_name,
    department,
    salary,
    median(salary) OVER w,
    mode() WITHIN GROUP (ORDER BY salary) OVER w
FROM basic_pays
WHERE salary > 8900
WINDOW w AS (PARTITION BY department)
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
  mode() WITHIN GROUP (ORDER BY dt)
FROM intervals;

SELECT
  k,
  mode() WITHIN GROUP (ORDER BY ym)
FROM intervals
GROUP BY k
ORDER BY k;
