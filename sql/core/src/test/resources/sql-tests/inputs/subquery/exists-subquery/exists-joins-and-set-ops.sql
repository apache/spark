-- Tests EXISTS subquery support. Tests Exists subquery
-- used in Joins (Both when joins occurs in outer and suquery blocks)

-- There are 2 dimensions we want to test
--  1. run with broadcast hash join, sort merge join or shuffle hash join.
--  2. run with whole-stage-codegen, operator codegen or no codegen.

--CONFIG_DIM1 spark.sql.autoBroadcastJoinThreshold=10485760
--CONFIG_DIM1 spark.sql.autoBroadcastJoinThreshold=-1,spark.sql.join.preferSortMergeJoin=true
--CONFIG_DIM1 spark.sql.autoBroadcastJoinThreshold=-1,spark.sql.join.preferSortMergeJoin=false

--CONFIG_DIM2 spark.sql.codegen.wholeStage=true
--CONFIG_DIM2 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM2 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

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

CREATE TEMPORARY VIEW BONUS AS SELECT * FROM VALUES
  ("emp 1", 10.00D),
  ("emp 1", 20.00D),
  ("emp 2", 300.00D),
  ("emp 2", 100.00D),
  ("emp 3", 300.00D),
  ("emp 4", 100.00D),
  ("emp 5", 1000.00D),
  ("emp 6 - no dept", 500.00D)
AS BONUS(emp_name, bonus_amt);

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

