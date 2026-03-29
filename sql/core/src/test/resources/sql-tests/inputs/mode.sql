-- Test data.
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

SELECT mode(department), mode(salary) FROM basic_pays;
SELECT department, mode(salary) FROM basic_pays GROUP BY department ORDER BY department;
SELECT department, mode(DISTINCT salary) FROM basic_pays GROUP BY department ORDER BY department;

-- SPARK-45034: Support deterministic mode function
SELECT mode(col) FROM VALUES (-10), (0), (10) AS tab(col);
SELECT mode(col, false) FROM VALUES (-10), (0), (10) AS tab(col);
SELECT mode(col, true) FROM VALUES (-10), (0), (10) AS tab(col);
SELECT mode(col, 'true') FROM VALUES (-10), (0), (10) AS tab(col);
SELECT mode(col, null) FROM VALUES (-10), (0), (10) AS tab(col);
SELECT mode(col, b) FROM VALUES (-10, false), (0, false), (10, false) AS tab(col, b);
SELECT mode(col) FROM VALUES (map(1, 'a')) AS tab(col);
SELECT mode(col, false) FROM VALUES (map(1, 'a')) AS tab(col);
SELECT mode(col, true) FROM VALUES (map(1, 'a')) AS tab(col);

SELECT
  mode() WITHIN GROUP (ORDER BY col),
  mode() WITHIN GROUP (ORDER BY col DESC)
FROM VALUES (null), (null), (null) AS tab(col);

SELECT
  mode() WITHIN GROUP (ORDER BY salary),
  mode() WITHIN GROUP (ORDER BY salary DESC)
FROM basic_pays
WHERE salary > 20000;

SELECT
  mode() WITHIN GROUP (ORDER BY salary),
  mode() WITHIN GROUP (ORDER BY salary DESC)
FROM basic_pays;

SELECT
  mode() WITHIN GROUP (ORDER BY salary),
  mode() WITHIN GROUP (ORDER BY salary) FILTER (WHERE salary > 10000)
FROM basic_pays;

SELECT
  department,
  mode() WITHIN GROUP (ORDER BY salary),
  mode() WITHIN GROUP (ORDER BY salary) FILTER (WHERE salary > 10000)
FROM basic_pays
GROUP BY department
ORDER BY department;

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
    mode() WITHIN GROUP (ORDER BY salary) OVER w
FROM basic_pays
WHERE salary > 8900
WINDOW w AS (PARTITION BY department)
ORDER BY salary;

SELECT
  mode(DISTINCT salary) WITHIN GROUP (ORDER BY salary)
FROM basic_pays;

SELECT
  mode()
FROM basic_pays;

SELECT
  mode(salary) WITHIN GROUP (ORDER BY salary)
FROM basic_pays;

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
  mode() WITHIN GROUP (ORDER BY dt),
  mode() WITHIN GROUP (ORDER BY dt DESC)
FROM intervals;

SELECT
  k,
  mode() WITHIN GROUP (ORDER BY ym),
  mode() WITHIN GROUP (ORDER BY dt DESC)
FROM intervals
GROUP BY k
ORDER BY k;
