-- Tests EXISTS subquery support. Tests Exists subquery
-- used in Joins (Both when joins occurs in outer and suquery blocks)

-- There are 2 dimensions we want to test
--  1. run with broadcast hash join, sort merge join or shuffle hash join.
--  2. run with whole-stage-codegen, operator codegen or no codegen.

--CONFIG_DIM1 spark.sql.autoBroadcastJoinThreshold=10485760
--CONFIG_DIM1 spark.sql.autoBroadcastJoinThreshold=-1,spark.sql.join.preferSortMergeJoin=true
--CONFIG_DIM1 spark.sql.autoBroadcastJoinThreshold=-1,spark.sql.join.forceApplyShuffledHashJoin=true

--CONFIG_DIM2 spark.sql.codegen.wholeStage=true
--CONFIG_DIM2 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM2 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

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

-- Join in outer query block
-- TC.01.01
SELECT * 
FROM   emp, 
       dept 
WHERE  emp.dept_id = dept.dept_id 
       AND EXISTS (SELECT * 
                   FROM   bonus 
                   WHERE  bonus.emp_name = emp.emp_name); 

-- Join in outer query block with ON condition 
-- TC.01.02
SELECT * 
FROM   emp 
       JOIN dept 
         ON emp.dept_id = dept.dept_id 
WHERE  EXISTS (SELECT * 
               FROM   bonus 
               WHERE  bonus.emp_name = emp.emp_name);

-- Left join in outer query block with ON condition 
-- TC.01.03
SELECT * 
FROM   emp 
       LEFT JOIN dept 
              ON emp.dept_id = dept.dept_id 
WHERE  EXISTS (SELECT * 
               FROM   bonus 
               WHERE  bonus.emp_name = emp.emp_name); 

-- Join in outer query block + NOT EXISTS
-- TC.01.04
SELECT * 
FROM   emp, 
       dept 
WHERE  emp.dept_id = dept.dept_id 
       AND NOT EXISTS (SELECT * 
                       FROM   bonus 
                       WHERE  bonus.emp_name = emp.emp_name); 


-- inner join in subquery.
-- TC.01.05
SELECT * 
FROM   bonus 
WHERE  EXISTS (SELECT * 
                 FROM   emp 
                        JOIN dept 
                          ON dept.dept_id = emp.dept_id 
                 WHERE  bonus.emp_name = emp.emp_name); 

-- right join in subquery
-- TC.01.06
SELECT * 
FROM   bonus 
WHERE  EXISTS (SELECT * 
                 FROM   emp 
                        RIGHT JOIN dept 
                                ON dept.dept_id = emp.dept_id 
                 WHERE  bonus.emp_name = emp.emp_name); 


-- Aggregation and join in subquery
-- TC.01.07
SELECT * 
FROM   bonus 
WHERE  EXISTS (SELECT dept.dept_id, 
                        emp.emp_name, 
                        Max(salary), 
                        Count(*) 
                 FROM   emp 
                        JOIN dept 
                          ON dept.dept_id = emp.dept_id 
                 WHERE  bonus.emp_name = emp.emp_name 
                 GROUP  BY dept.dept_id, 
                           emp.emp_name 
                 ORDER  BY emp.emp_name);

-- Aggregations in outer and subquery + join in subquery
-- TC.01.08
SELECT emp_name, 
       Sum(bonus_amt) 
FROM   bonus 
WHERE  EXISTS (SELECT emp_name, 
                        Max(salary) 
                 FROM   emp 
                        JOIN dept 
                          ON dept.dept_id = emp.dept_id 
                 WHERE  bonus.emp_name = emp.emp_name 
                 GROUP  BY emp_name 
                 HAVING Count(*) > 1 
                 ORDER  BY emp_name)
GROUP  BY emp_name; 

-- TC.01.09
SELECT emp_name, 
       Sum(bonus_amt) 
FROM   bonus 
WHERE  NOT EXISTS (SELECT emp_name, 
                          Max(salary) 
                   FROM   emp 
                          JOIN dept 
                            ON dept.dept_id = emp.dept_id 
                   WHERE  bonus.emp_name = emp.emp_name 
                   GROUP  BY emp_name 
                   HAVING Count(*) > 1 
                   ORDER  BY emp_name) 
GROUP  BY emp_name;

-- Set operations along with EXISTS subquery
-- union
-- TC.02.01 
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT * 
               FROM   dept 
               WHERE  dept_id < 30 
               UNION 
               SELECT * 
               FROM   dept 
               WHERE  dept_id >= 30 
                      AND dept_id <= 50); 

-- intersect 
-- TC.02.02 
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT * 
                 FROM   dept 
                 WHERE  dept_id < 30 
                 INTERSECT 
                 SELECT * 
                 FROM   dept 
                 WHERE  dept_id >= 30 
                        AND dept_id <= 50);

-- intersect + not exists 
-- TC.02.03                
SELECT * 
FROM   emp 
WHERE  NOT EXISTS (SELECT * 
                     FROM   dept 
                     WHERE  dept_id < 30 
                     INTERSECT 
                     SELECT * 
                     FROM   dept 
                     WHERE  dept_id >= 30 
                            AND dept_id <= 50); 

-- Union all in outer query and except,intersect in subqueries. 
-- TC.02.04       
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT * 
                 FROM   dept 
                 EXCEPT 
                 SELECT * 
                 FROM   dept 
                 WHERE  dept_id > 50)
UNION ALL 
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT * 
                 FROM   dept 
                 WHERE  dept_id < 30 
                 INTERSECT 
                 SELECT * 
                 FROM   dept 
                 WHERE  dept_id >= 30 
                        AND dept_id <= 50);

-- Union in outer query and except,intersect in subqueries. 
-- TC.02.05       
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT * 
                 FROM   dept 
                 EXCEPT 
                 SELECT * 
                 FROM   dept 
                 WHERE  dept_id > 50)
UNION
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT * 
                 FROM   dept 
                 WHERE  dept_id < 30 
                 INTERSECT 
                 SELECT * 
                 FROM   dept 
                 WHERE  dept_id >= 30 
                        AND dept_id <= 50);

-- Correlated predicates under set ops - unsupported
SELECT *
FROM   emp
WHERE  EXISTS (SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "CA"
               UNION
               SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "TX");

SELECT *
FROM   emp
WHERE NOT EXISTS (SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "CA"
               UNION
               SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "TX");

SELECT *
FROM   emp
WHERE  EXISTS (SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "CA"
               INTERSECT ALL
               SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "TX");

SELECT *
FROM   emp
WHERE EXISTS (SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "CA"
               INTERSECT DISTINCT
               SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "TX");

SELECT *
FROM   emp
WHERE  EXISTS (SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "CA"
               EXCEPT ALL
               SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "TX");

SELECT *
FROM   emp
WHERE  EXISTS (SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "CA"
               EXCEPT DISTINCT
               SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "TX");

SELECT *
FROM   emp
WHERE NOT EXISTS (SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "CA"
               INTERSECT ALL
               SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "TX");

SELECT *
FROM   emp
WHERE NOT EXISTS (SELECT *
               FROM   dept
               WHERE  dept_id = emp.dept_id and state = "CA"
               EXCEPT DISTINCT
               SELECT * 
               FROM   dept 
               WHERE  dept_id = emp.dept_id and state = "TX");
