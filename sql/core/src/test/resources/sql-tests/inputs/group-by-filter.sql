-- Test filter clause for aggregate expression.

-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

-- Aggregate with filter and empty GroupBy expressions.
SELECT a, COUNT(b) FILTER (WHERE a >= 2) FROM testData;
SELECT COUNT(a) FILTER (WHERE a = 1), COUNT(b) FILTER (WHERE a > 1) FROM testData;

-- Aggregate with filter and non-empty GroupBy expressions.
SELECT a, COUNT(b) FILTER (WHERE a >= 2) FROM testData GROUP BY a;
SELECT a, COUNT(b) FILTER (WHERE a != 2) FROM testData GROUP BY b;
SELECT COUNT(a) FILTER (WHERE a >= 0), COUNT(b) FILTER (WHERE a >= 3) FROM testData GROUP BY a;

-- Aggregate with filter and grouped by literals.
SELECT 'foo', COUNT(a) FILTER (WHERE b <= 2) FROM testData GROUP BY 1;

-- Aggregate with filter and grouped by literals (hash aggregate).
SELECT 'foo', APPROX_COUNT_DISTINCT(a) FILTER (WHERE b >= 0) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate with filter and grouped by literals (sort aggregate).
SELECT 'foo', MAX(STRUCT(a)) FILTER (WHERE b >= 1) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate with filter and complex GroupBy expressions.
SELECT a + b, COUNT(b) FILTER (WHERE b >= 2) FROM testData GROUP BY a + b;
SELECT a + 2, COUNT(b) FILTER (WHERE b IN (1, 2)) FROM testData GROUP BY a + 1;
SELECT a + 1 + 1, COUNT(b) FILTER (WHERE b > 0) FROM testData GROUP BY a + 1;

-- Aggregate with filter, foldable input and multiple distinct groups.
SELECT COUNT(DISTINCT b) FILTER (WHERE b > 0), COUNT(DISTINCT b, c) FILTER (WHERE b > 0 AND c > 2)
FROM (SELECT 1 AS a, 2 AS b, 3 AS c) GROUP BY a;

-- Aliases in SELECT could be used in GROUP BY
SELECT a AS k, COUNT(b) FILTER (WHERE b = 1 OR b = 2) FROM testData GROUP BY k;
SELECT a AS k, COUNT(b) FILTER (WHERE NOT b < 0) FROM testData GROUP BY k HAVING k > 1;

-- Aggregate functions cannot be used in GROUP BY
SELECT COUNT(b) FILTER (WHERE a > 0) AS k FROM testData GROUP BY k;

-- Check analysis exceptions
SELECT a AS k, COUNT(b) FILTER (WHERE b > 0) FROM testData GROUP BY k;

-- Aggregate with filter, empty input and non-empty GroupBy expressions.
SELECT a, COUNT(1) FILTER (WHERE b > 1) FROM testData WHERE false GROUP BY a;

-- Aggregate with filter, empty input and empty GroupBy expressions.
SELECT COUNT(1) FILTER (WHERE b = 2) FROM testData WHERE false;
SELECT 1 FROM (SELECT COUNT(1) FILTER (WHERE a >= 3 OR b <= 1) FROM testData WHERE false) t;

CREATE TEMPORARY VIEW EMP AS SELECT * FROM VALUES
  (100, "emp 1", date "2005-01-01", 100.00D, 10),
  (100, "emp 1", date "2005-01-01", 100.00D, 10),
  (200, "emp 2", date "2003-01-01", 200.00D, 10),
  (300, "emp 3", date "2002-01-01", 300.00D, 20),
  (400, "emp 4", date "2005-01-01", 400.00D, 30),
  (500, "emp 5", date "2001-01-01", 400.00D, NULL),
  (600, "emp 6 - no dept", date "2001-01-01", 400.00D, 100),
  (700, "emp 7", date "2010-01-01", 400.00D, 100),
  (800, "emp 8", date "2016-01-01", 150.00D, 70)
AS EMP(id, emp_name, hiredate, salary, dept_id);

CREATE TEMPORARY VIEW DEPT AS SELECT * FROM VALUES
  (10, "dept 1", "CA"),
  (20, "dept 2", "NY"),
  (30, "dept 3", "TX"),
  (40, "dept 4 - unassigned", "OR"),
  (50, "dept 5 - unassigned", "NJ"),
  (70, "dept 7", "FL")
AS DEPT(dept_id, dept_name, state);

-- Aggregate with filter contains exists subquery
SELECT emp.dept_id,
       avg(salary),
       avg(salary) FILTER (WHERE EXISTS (SELECT state
               FROM dept
               WHERE dept.dept_id = emp.dept_id))
FROM emp
GROUP BY dept_id;
SELECT emp.dept_id, 
       Sum(salary),
       Sum(salary) FILTER (WHERE  NOT EXISTS (SELECT state 
                   FROM dept 
                   WHERE dept.dept_id = emp.dept_id))
FROM emp 
GROUP BY dept_id; 

-- Aggregate with filter contains in subquery
SELECT emp.dept_id, 
       avg(salary),
       avg(salary) FILTER (WHERE emp.dept_id IN (SELECT DISTINCT dept_id
               FROM dept))
FROM emp 
GROUP BY dept_id; 
SELECT emp.dept_id, 
       Sum(salary),
       Sum(salary) FILTER (WHERE emp.dept_id NOT IN (SELECT DISTINCT dept_id
               FROM dept))
FROM emp 
GROUP BY dept_id; 

-- Aggregate with filter is subquery
SELECT t1.a, t1.b FROM (SELECT a, COUNT(b) FILTER (WHERE a >= 2) AS b FROM testData) t1;
