-- Tests EXISTS subquery support with ORDER BY and LIMIT clauses.

-- Test sort operator with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

CREATE TEMPORARY VIEW EMP(id, emp_name, hiredate, salary, dept_id) AS VALUES
  (100, 'emp 1', date '2005-01-01', double(100.00), 10),
  (100, 'emp 1', date '2005-01-01', double(100.00), 10),
  (200, 'emp 2', date '2003-01-01', double(200.00), 10),
  (300, 'emp 3', date '2002-01-01', double(300.00), 20),
  (400, 'emp 4', date '2005-01-01', double(400.00), 30),
  (500, 'emp 5', date '2001-01-01', double(400.00), NULL),
  (600, 'emp 6 - no dept', date '2001-01-01', double(400.00), 100),
  (700, 'emp 7', date '2010-01-01', double(400.00), 100),
  (800, 'emp 8', date '2016-01-01', double(150.00), 70);

CREATE TEMPORARY VIEW DEPT(dept_id, dept_name, state) AS VALUES
  (10, 'dept 1', 'CA'),
  (20, 'dept 2', 'NY'),
  (30, 'dept 3', 'TX'),
  (40, 'dept 4 - unassigned', 'OR'),
  (50, 'dept 5 - unassigned', 'NJ'),
  (70, 'dept 7', 'FL');

CREATE TEMPORARY VIEW BONUS(emp_name, bonus_amt) AS VALUES
  ('emp 1', double(10.00)),
  ('emp 1', double(20.00)),
  ('emp 2', double(300.00)),
  ('emp 2', double(100.00)),
  ('emp 3', double(300.00)),
  ('emp 4', double(100.00)),
  ('emp 5', double(1000.00)),
  ('emp 6 - no dept', double(500.00));

-- order by in both outer and/or inner query block
-- TC.01.01
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT dept.dept_id 
               FROM   dept 
               WHERE  emp.dept_id = dept.dept_id 
               ORDER  BY state) 
ORDER  BY hiredate; 

-- TC.01.02
SELECT id, 
       hiredate 
FROM   emp 
WHERE  EXISTS (SELECT dept.dept_id 
               FROM   dept 
               WHERE  emp.dept_id = dept.dept_id 
               ORDER  BY state) 
ORDER  BY hiredate DESC;

SELECT *
FROM   emp
WHERE  EXISTS (SELECT dept.dept_id
               FROM   dept
               WHERE  emp.dept_id = dept.dept_id
               ORDER  BY state
               LIMIT 1)
ORDER  BY hiredate;

SELECT *
FROM   emp
WHERE  EXISTS (SELECT dept.dept_id
               FROM   dept
               WHERE  emp.dept_id = dept.dept_id
               ORDER  BY state
               LIMIT 0)
ORDER  BY hiredate;

-- order by with not exists 
-- TC.01.03
SELECT * 
FROM   emp 
WHERE  NOT EXISTS (SELECT dept.dept_id 
                   FROM   dept 
                   WHERE  emp.dept_id = dept.dept_id 
                   ORDER  BY state) 
ORDER  BY hiredate; 

-- group by + order by with not exists
-- TC.01.04
SELECT emp_name 
FROM   emp 
WHERE  NOT EXISTS (SELECT max(dept.dept_id) a 
                   FROM   dept 
                   WHERE  dept.dept_id = emp.dept_id 
                   GROUP  BY state 
                   ORDER  BY state);
-- TC.01.05
SELECT count(*) 
FROM   emp 
WHERE  NOT EXISTS (SELECT max(dept.dept_id) a 
                   FROM   dept 
                   WHERE  dept.dept_id = emp.dept_id 
                   GROUP  BY dept_id 
                   ORDER  BY dept_id); 

SELECT *
FROM   emp
WHERE  NOT EXISTS (SELECT dept.dept_id
                   FROM   dept
                   WHERE  emp.dept_id = dept.dept_id
                   ORDER  BY state
                   LIMIT 1)
ORDER  BY hiredate;

SELECT *
FROM   emp
WHERE  NOT EXISTS (SELECT dept.dept_id
                   FROM   dept
                   WHERE  emp.dept_id = dept.dept_id
                   ORDER  BY state
                   LIMIT 0)
ORDER  BY hiredate;

-- limit in the exists subquery block.
-- TC.02.01
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT dept.dept_name 
               FROM   dept 
               WHERE  dept.dept_id > 10 
               LIMIT  1); 

-- limit in the exists subquery block with aggregate.
-- TC.02.02
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT max(dept.dept_id) 
               FROM   dept 
               GROUP  BY state 
               LIMIT  1); 

-- limit in the not exists subquery block.
-- TC.02.03
SELECT * 
FROM   emp 
WHERE  NOT EXISTS (SELECT dept.dept_name 
                   FROM   dept 
                   WHERE  dept.dept_id > 100 
                   LIMIT  1); 

-- limit in the not exists subquery block with aggregates.
-- TC.02.04
SELECT * 
FROM   emp 
WHERE  NOT EXISTS (SELECT max(dept.dept_id) 
                   FROM   dept 
                   WHERE  dept.dept_id > 100 
                   GROUP  BY state 
                   LIMIT  1);

SELECT emp_name
FROM   emp
WHERE  NOT EXISTS (SELECT max(dept.dept_id) a
                   FROM   dept
                   WHERE  dept.dept_id = emp.dept_id
                   GROUP  BY state
                   ORDER  BY state
                   LIMIT 2
                   OFFSET 1);

-- limit and offset in the exists subquery block.
-- TC.03.01
SELECT *
FROM   emp
WHERE  EXISTS (SELECT dept.dept_name
               FROM   dept
               WHERE  dept.dept_id > 10
               LIMIT  1
               OFFSET 2);

SELECT *
FROM   emp
WHERE  EXISTS (SELECT dept.dept_name
               FROM   dept
               WHERE  dept.dept_id > emp.dept_id
               LIMIT  1);

-- limit and offset in the exists subquery block with aggregate.
-- TC.03.02
SELECT *
FROM   emp
WHERE  EXISTS (SELECT max(dept.dept_id)
               FROM   dept
               GROUP  BY state
               LIMIT  1
               OFFSET 2);

SELECT *
FROM   emp
WHERE  EXISTS (SELECT max(dept.dept_id)
               FROM   dept
               WHERE  dept.dept_id <> emp.dept_id
               GROUP  BY state
               LIMIT  1);

-- SPARK-46526: LIMIT over correlated predicate that references only the outer table.
SELECT *
FROM   emp
WHERE  EXISTS (SELECT max(dept.dept_id)
               FROM   dept
               WHERE  emp.salary > 200
               LIMIT  1);

-- SPARK-46526: LIMIT over correlated predicate that references only the outer table,
-- and a group by.
SELECT *
FROM   emp
WHERE  EXISTS (SELECT state, max(dept.dept_name)
               FROM   dept
               WHERE  emp.salary > 200
               GROUP BY state
               LIMIT  1);

-- limit and offset in the not exists subquery block.
-- TC.03.03
SELECT *
FROM   emp
WHERE  NOT EXISTS (SELECT dept.dept_name
                   FROM   dept
                   WHERE  dept.dept_id > 100
                   LIMIT  1
                   OFFSET 2);

-- limit and offset in the not exists subquery block with aggregates.
-- TC.03.04
SELECT *
FROM   emp
WHERE  NOT EXISTS (SELECT max(dept.dept_id)
                   FROM   dept
                   WHERE  dept.dept_id > 100
                   GROUP  BY state
                   LIMIT  1
                   OFFSET 2);

SELECT *
FROM   emp
WHERE  EXISTS (SELECT dept.dept_name
               FROM   dept
               WHERE  dept.dept_id <> emp.dept_id
               LIMIT  1
               OFFSET 2);

-- offset in the exists subquery block.
-- TC.04.01
SELECT *
FROM   emp
WHERE  EXISTS (SELECT dept.dept_name
               FROM   dept
               WHERE  dept.dept_id > 10
               OFFSET 2);

-- offset in the exists subquery block with aggregate.
-- TC.04.02
SELECT *
FROM   emp
WHERE  EXISTS (SELECT max(dept.dept_id)
               FROM   dept
               GROUP  BY state
               OFFSET 2);

-- limit in the not exists subquery block.
-- TC.04.03
SELECT *
FROM   emp
WHERE  NOT EXISTS (SELECT dept.dept_name
                   FROM   dept
                   WHERE  dept.dept_id > 100
                   OFFSET 2);

-- limit in the not exists subquery block with aggregates.
-- TC.04.04
SELECT *
FROM   emp
WHERE  NOT EXISTS (SELECT max(dept.dept_id)
                   FROM   dept
                   WHERE  dept.dept_id > 100
                   GROUP  BY state
                   OFFSET 2);
