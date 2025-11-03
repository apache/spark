-- Test filter clause for aggregate expression with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=true
--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=false

-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

CREATE OR REPLACE TEMPORARY VIEW EMP AS SELECT * FROM VALUES
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

CREATE OR REPLACE TEMPORARY VIEW DEPT AS SELECT * FROM VALUES
  (10, "dept 1", "CA"),
  (20, "dept 2", "NY"),
  (30, "dept 3", "TX"),
  (40, "dept 4 - unassigned", "OR"),
  (50, "dept 5 - unassigned", "NJ"),
  (70, "dept 7", "FL")
AS DEPT(dept_id, dept_name, state);

CREATE OR REPLACE TEMPORARY VIEW FilterExpressionTestData AS SELECT * FROM VALUES
  (1, 2, "asd"),
  (3, 4, "fgh"),
  (5, 6, "jkl")
AS FilterExpressionTestData(num1, num2, str);

-- Aggregate with filter and empty GroupBy expressions.
SELECT a, COUNT(b) FILTER (WHERE a >= 2) FROM testData;
SELECT COUNT(a) FILTER (WHERE a = 1), COUNT(b) FILTER (WHERE a > 1) FROM testData;
SELECT COUNT(id) FILTER (WHERE hiredate = date "2001-01-01") FROM emp;
SELECT COUNT(id) FILTER (WHERE hiredate = to_date('2001-01-01 00:00:00')) FROM emp;
SELECT COUNT(id) FILTER (WHERE hiredate = to_timestamp("2001-01-01 00:00:00")) FROM emp;
SELECT COUNT(id) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd") = "2001-01-01") FROM emp;
SELECT COUNT(DISTINCT id) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd HH:mm:ss") = "2001-01-01 00:00:00") FROM emp;
SELECT COUNT(DISTINCT id), COUNT(DISTINCT id) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd HH:mm:ss") = "2001-01-01 00:00:00") FROM emp;
SELECT COUNT(DISTINCT id) FILTER (WHERE hiredate = to_timestamp("2001-01-01 00:00:00")), COUNT(DISTINCT id) FILTER (WHERE hiredate = to_date('2001-01-01 00:00:00')) FROM emp;
SELECT SUM(salary), COUNT(DISTINCT id), COUNT(DISTINCT id) FILTER (WHERE hiredate = date "2001-01-01") FROM emp;
SELECT COUNT(DISTINCT 1) FILTER (WHERE a = 1) FROM testData;
SELECT COUNT(DISTINCT id) FILTER (WHERE true) FROM emp;
SELECT COUNT(DISTINCT id) FILTER (WHERE false) FROM emp;
SELECT COUNT(DISTINCT 2), COUNT(DISTINCT 2,3) FILTER (WHERE dept_id = 40) FROM emp;
SELECT COUNT(DISTINCT 2), COUNT(DISTINCT 3,2) FILTER (WHERE dept_id = 40) FROM emp;
SELECT COUNT(DISTINCT 2), COUNT(DISTINCT 2,3) FILTER (WHERE dept_id > 0) FROM emp;
SELECT COUNT(DISTINCT 2), COUNT(DISTINCT 3,2) FILTER (WHERE dept_id > 0) FROM emp;
SELECT COUNT(DISTINCT id), COUNT(DISTINCT 2,3) FILTER (WHERE dept_id = 40) FROM emp;
SELECT COUNT(DISTINCT id), COUNT(DISTINCT 3,2) FILTER (WHERE dept_id = 40) FROM emp;
SELECT COUNT(DISTINCT id), COUNT(DISTINCT 2,3) FILTER (WHERE dept_id > 0) FROM emp;
SELECT COUNT(DISTINCT id), COUNT(DISTINCT 3,2) FILTER (WHERE dept_id > 0) FROM emp;

-- Aggregate with filter and non-empty GroupBy expressions.
SELECT a, COUNT(b) FILTER (WHERE a >= 2) FROM testData GROUP BY a;
SELECT a, COUNT(b) FILTER (WHERE a != 2) FROM testData GROUP BY b;
SELECT COUNT(a) FILTER (WHERE a >= 0), COUNT(b) FILTER (WHERE a >= 3) FROM testData GROUP BY a;
SELECT dept_id, SUM(salary) FILTER (WHERE hiredate > date "2003-01-01") FROM emp GROUP BY dept_id;
SELECT dept_id, SUM(salary) FILTER (WHERE hiredate > to_date("2003-01-01")) FROM emp GROUP BY dept_id;
SELECT dept_id, SUM(salary) FILTER (WHERE hiredate > to_timestamp("2003-01-01 00:00:00")) FROM emp GROUP BY dept_id;
SELECT dept_id, SUM(salary) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd") > "2003-01-01") FROM emp GROUP BY dept_id;
SELECT dept_id, SUM(DISTINCT salary) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd HH:mm:ss") > "2001-01-01 00:00:00") FROM emp GROUP BY dept_id;
SELECT dept_id, SUM(DISTINCT salary), SUM(DISTINCT salary) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd HH:mm:ss") > "2001-01-01 00:00:00") FROM emp GROUP BY dept_id;
SELECT dept_id, SUM(DISTINCT salary) FILTER (WHERE hiredate > date "2001-01-01"), SUM(DISTINCT salary) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd HH:mm:ss") > "2001-01-01 00:00:00") FROM emp GROUP BY dept_id;
SELECT dept_id, COUNT(id), SUM(DISTINCT salary), SUM(DISTINCT salary) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd") > "2001-01-01") FROM emp GROUP BY dept_id;
SELECT b, COUNT(DISTINCT 1) FILTER (WHERE a = 1) FROM testData GROUP BY b;

-- Aggregate with filter and grouped by literals.
SELECT 'foo', COUNT(a) FILTER (WHERE b <= 2) FROM testData GROUP BY 1;
SELECT 'foo', SUM(salary) FILTER (WHERE hiredate >= date "2003-01-01") FROM emp GROUP BY 1;
SELECT 'foo', SUM(salary) FILTER (WHERE hiredate >= to_date("2003-01-01")) FROM emp GROUP BY 1;
SELECT 'foo', SUM(salary) FILTER (WHERE hiredate >= to_timestamp("2003-01-01")) FROM emp GROUP BY 1;

-- Aggregate with filter, more than one aggregate function goes with distinct.
select dept_id, count(distinct emp_name), count(distinct hiredate), sum(salary), sum(salary) filter (where id > 200) from emp group by dept_id;
select dept_id, count(distinct emp_name), count(distinct hiredate), sum(salary), sum(salary) filter (where id + dept_id > 500) from emp group by dept_id;
select dept_id, count(distinct emp_name), count(distinct hiredate), sum(salary) filter (where salary < 400.00D), sum(salary) filter (where id > 200) from emp group by dept_id;
select dept_id, count(distinct emp_name), count(distinct hiredate), sum(salary) filter (where salary < 400.00D), sum(salary) filter (where id + dept_id > 500) from emp group by dept_id;
select dept_id, count(distinct emp_name) filter (where id > 200), sum(salary) from emp group by dept_id;
select dept_id, count(distinct emp_name) filter (where id + dept_id > 500), sum(salary) from emp group by dept_id;
select dept_id, count(distinct emp_name), count(distinct emp_name) filter (where id > 200), sum(salary) from emp group by dept_id;
select dept_id, count(distinct emp_name), count(distinct emp_name) filter (where id + dept_id > 500), sum(salary) from emp group by dept_id;
select dept_id, count(distinct emp_name), count(distinct emp_name) filter (where id > 200), sum(salary), sum(salary) filter (where id > 200) from emp group by dept_id;
select dept_id, count(distinct emp_name), count(distinct emp_name) filter (where id + dept_id > 500), sum(salary), sum(salary) filter (where id > 200) from emp group by dept_id;
select dept_id, count(distinct emp_name) filter (where id > 200), count(distinct hiredate), sum(salary) from emp group by dept_id;
select dept_id, count(distinct emp_name) filter (where id > 200), count(distinct hiredate) filter (where hiredate > date "2003-01-01"), sum(salary) from emp group by dept_id;
select dept_id, count(distinct emp_name) filter (where id > 200), count(distinct hiredate) filter (where hiredate > date "2003-01-01"), sum(salary) filter (where salary < 400.00D) from emp group by dept_id;
select dept_id, count(distinct emp_name) filter (where id > 200), count(distinct hiredate) filter (where hiredate > date "2003-01-01"), sum(salary) filter (where salary < 400.00D), sum(salary) filter (where id > 200) from emp group by dept_id;
select dept_id, count(distinct emp_name) filter (where id > 200), count(distinct emp_name), sum(salary) from emp group by dept_id;
select dept_id, count(distinct emp_name) filter (where id > 200), count(distinct emp_name) filter (where hiredate > date "2003-01-01"), sum(salary) from emp group by dept_id;
select dept_id, sum(distinct (id + dept_id)) filter (where id > 200), count(distinct hiredate), sum(salary) from emp group by dept_id;
select dept_id, sum(distinct (id + dept_id)) filter (where id > 200), count(distinct hiredate) filter (where hiredate > date "2003-01-01"), sum(salary) from emp group by dept_id;
select dept_id, avg(distinct (id + dept_id)) filter (where id > 200), count(distinct hiredate) filter (where hiredate > date "2003-01-01"), sum(salary) filter (where salary < 400.00D) from emp group by dept_id;
select dept_id, count(distinct emp_name, hiredate) filter (where id > 200), sum(salary) from emp group by dept_id;
select dept_id, count(distinct emp_name, hiredate) filter (where id > 0), sum(salary) from emp group by dept_id;
select dept_id, count(distinct 1), count(distinct 1) filter (where id > 200), sum(salary) from emp group by dept_id;

-- Aggregate with filter and grouped by literals (hash aggregate), here the input table is filtered using WHERE.
SELECT 'foo', APPROX_COUNT_DISTINCT(a) FILTER (WHERE b >= 0) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate with filter and grouped by literals (sort aggregate), here the input table is filtered using WHERE.
SELECT 'foo', MAX(STRUCT(a)) FILTER (WHERE b >= 1) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate with filter and complex GroupBy expressions.
SELECT a + b, COUNT(b) FILTER (WHERE b >= 2) FROM testData GROUP BY a + b;
SELECT a + 2, COUNT(b) FILTER (WHERE b IN (1, 2)) FROM testData GROUP BY a + 1;
SELECT a + 1 + 1, COUNT(b) FILTER (WHERE b > 0) FROM testData GROUP BY a + 1;

-- Aggregate with filter, foldable input and multiple distinct groups.
SELECT COUNT(DISTINCT b) FILTER (WHERE b > 0), COUNT(DISTINCT b, c) FILTER (WHERE b > 0 AND c > 2)
FROM (SELECT 1 AS a, 2 AS b, 3 AS c) GROUP BY a;

-- Check analysis exceptions
SELECT a AS k, COUNT(b) FILTER (WHERE b > 0) FROM testData GROUP BY k;

-- Aggregate with filter contains exists subquery
SELECT emp.dept_id,
       avg(salary),
       avg(salary) FILTER (WHERE id > (SELECT 200))
FROM emp
GROUP BY dept_id;

SELECT emp.dept_id,
       avg(salary),
       avg(salary) FILTER (WHERE emp.dept_id = (SELECT dept_id FROM dept LIMIT 1))
FROM emp
GROUP BY dept_id;

-- [SPARK-30220] Support Filter expression uses IN/EXISTS predicate sub-queries
SELECT emp.dept_id,
       avg(salary),
       avg(salary) FILTER (WHERE EXISTS (SELECT state
               FROM dept
               WHERE dept.dept_id = emp.dept_id))
FROM emp
GROUP BY dept_id;

SELECT emp.dept_id, 
       Sum(salary),
       Sum(salary) FILTER (WHERE NOT EXISTS (SELECT state 
                   FROM dept 
                   WHERE dept.dept_id = emp.dept_id))
FROM emp 
GROUP BY dept_id; 

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
SELECT t1.b FROM (SELECT COUNT(b) FILTER (WHERE a >= 2) AS b FROM testData) t1;

-- SPARK-47256: Wrong use of FILTER expression in aggregate functions
SELECT count(num1) FILTER (WHERE rand(int(num2)) > 1) FROM FilterExpressionTestData;

SELECT sum(num1) FILTER (WHERE str) FROM FilterExpressionTestData;

SELECT sum(num1) FILTER (WHERE max(num2) > 1) FROM FilterExpressionTestData;

SELECT sum(num1) FILTER (WHERE nth_value(num2, 2) OVER(ORDER BY num2) > 1) FROM FilterExpressionTestData;
