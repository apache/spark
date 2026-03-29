-- Tests EXISTS subquery support. Tests basic form 
-- of EXISTS subquery (both EXISTS and NOT EXISTS)
--ONLY_IF spark

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

-- uncorrelated exist query 
-- TC.01.01
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT 1 
               FROM   dept 
               WHERE  dept.dept_id > 10 
                      AND dept.dept_id < 30); 

-- simple correlated predicate in exist subquery
-- TC.01.02
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT dept.dept_name 
               FROM   dept 
               WHERE  emp.dept_id = dept.dept_id); 

-- correlated outer isnull predicate
-- TC.01.03
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT dept.dept_name 
               FROM   dept 
               WHERE  emp.dept_id = dept.dept_id 
                       OR emp.dept_id IS NULL);

-- Simple correlation with a local predicate in outer query
-- TC.01.04
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT dept.dept_name 
               FROM   dept 
               WHERE  emp.dept_id = dept.dept_id) 
       AND emp.id > 200; 

-- Outer references (emp.id) should not be pruned from outer plan
-- TC.01.05
SELECT emp.emp_name 
FROM   emp 
WHERE  EXISTS (SELECT dept.state 
               FROM   dept 
               WHERE  emp.dept_id = dept.dept_id) 
       AND emp.id > 200;

-- not exists with correlated predicate
-- TC.01.06
SELECT * 
FROM   dept 
WHERE  NOT EXISTS (SELECT emp_name 
                   FROM   emp 
                   WHERE  emp.dept_id = dept.dept_id);

-- not exists with correlated predicate + local predicate
-- TC.01.07
SELECT * 
FROM   dept 
WHERE  NOT EXISTS (SELECT emp_name 
                   FROM   emp 
                   WHERE  emp.dept_id = dept.dept_id 
                           OR state = 'NJ');

-- not exist both equal and greaterthan predicate
-- TC.01.08
SELECT * 
FROM   bonus 
WHERE  NOT EXISTS (SELECT * 
                   FROM   emp 
                   WHERE  emp.emp_name = emp_name 
                          AND bonus_amt > emp.salary); 

-- select employees who have not received any bonus
-- TC 01.09
SELECT emp.*
FROM   emp
WHERE  NOT EXISTS (SELECT NULL
                   FROM   bonus
                   WHERE  bonus.emp_name = emp.emp_name);

-- Nested exists
-- TC.01.10
SELECT * 
FROM   bonus 
WHERE  EXISTS (SELECT emp_name 
               FROM   emp 
               WHERE  bonus.emp_name = emp.emp_name 
                      AND EXISTS (SELECT state 
                                  FROM   dept 
                                  WHERE  dept.dept_id = emp.dept_id)); 
