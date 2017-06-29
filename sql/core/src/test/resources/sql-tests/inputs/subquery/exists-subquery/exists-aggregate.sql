-- Tests aggregate expressions in outer query and EXISTS subquery.

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

-- Aggregate in outer query block.
-- TC.01.01
SELECT emp.dept_id, 
       avg(salary),
       sum(salary)
FROM   emp 
WHERE  EXISTS (SELECT state 
               FROM   dept 
               WHERE  dept.dept_id = emp.dept_id) 
GROUP  BY dept_id; 

-- Aggregate in inner/subquery block
-- TC.01.02
SELECT emp_name 
FROM   emp 
WHERE  EXISTS (SELECT max(dept.dept_id) a 
               FROM   dept 
               WHERE  dept.dept_id = emp.dept_id 
               GROUP  BY dept.dept_id); 

-- Aggregate expression in both outer and inner query block.
-- TC.01.03
SELECT count(*) 
FROM   emp 
WHERE  EXISTS (SELECT max(dept.dept_id) a 
               FROM   dept 
               WHERE  dept.dept_id = emp.dept_id 
               GROUP  BY dept.dept_id); 

-- Nested exists with aggregate expression in inner most query block.
-- TC.01.04
SELECT * 
FROM   bonus 
WHERE  EXISTS (SELECT 1 
               FROM   emp 
               WHERE  emp.emp_name = bonus.emp_name 
                      AND EXISTS (SELECT max(dept.dept_id) 
                                  FROM   dept 
                                  WHERE  emp.dept_id = dept.dept_id 
                                  GROUP  BY dept.dept_id));

-- Not exists with Aggregate expression in outer
-- TC.01.05
SELECT emp.dept_id, 
       Avg(salary), 
       Sum(salary) 
FROM   emp 
WHERE  NOT EXISTS (SELECT state 
                   FROM   dept 
                   WHERE  dept.dept_id = emp.dept_id) 
GROUP  BY dept_id; 

-- Not exists with Aggregate expression in subquery block
-- TC.01.06
SELECT emp_name 
FROM   emp 
WHERE  NOT EXISTS (SELECT max(dept.dept_id) a 
                   FROM   dept 
                   WHERE  dept.dept_id = emp.dept_id 
                   GROUP  BY dept.dept_id); 

-- Not exists with Aggregate expression in outer and subquery block
-- TC.01.07
SELECT count(*) 
FROM   emp 
WHERE  NOT EXISTS (SELECT max(dept.dept_id) a 
                   FROM   dept 
                   WHERE  dept.dept_id = emp.dept_id 
                   GROUP  BY dept.dept_id); 

-- Nested not exists and exists with aggregate expression in inner most query block.
-- TC.01.08
SELECT * 
FROM   bonus 
WHERE  NOT EXISTS (SELECT 1 
                   FROM   emp 
                   WHERE  emp.emp_name = bonus.emp_name 
                          AND EXISTS (SELECT Max(dept.dept_id) 
                                      FROM   dept 
                                      WHERE  emp.dept_id = dept.dept_id 
                                      GROUP  BY dept.dept_id));
