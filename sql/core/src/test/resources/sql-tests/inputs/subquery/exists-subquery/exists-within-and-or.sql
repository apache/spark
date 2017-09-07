-- Tests EXISTS subquery support. Tests EXISTS 
-- subquery within a AND or OR expression.

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


-- Or used in conjunction with exists - ExistenceJoin
-- TC.02.01
SELECT emp.emp_name 
FROM   emp 
WHERE  EXISTS (SELECT dept.state 
               FROM   dept 
               WHERE  emp.dept_id = dept.dept_id) 
        OR emp.id > 200;

-- all records from emp including the null dept_id 
-- TC.02.02
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT dept.dept_name 
               FROM   dept 
               WHERE  emp.dept_id = dept.dept_id) 
        OR emp.dept_id IS NULL; 

-- EXISTS subquery in both LHS and RHS of OR. 
-- TC.02.03
SELECT emp.emp_name 
FROM   emp 
WHERE  EXISTS (SELECT dept.state 
               FROM   dept 
               WHERE  emp.dept_id = dept.dept_id 
                      AND dept.dept_id = 20) 
        OR EXISTS (SELECT dept.state 
                   FROM   dept 
                   WHERE  emp.dept_id = dept.dept_id 
                          AND dept.dept_id = 30); 
;

-- not exists and exists predicate within OR
-- TC.02.04
SELECT * 
FROM   bonus 
WHERE  ( NOT EXISTS (SELECT * 
                     FROM   emp 
                     WHERE  emp.emp_name = emp_name 
                            AND bonus_amt > emp.salary) 
          OR EXISTS (SELECT * 
                     FROM   emp 
                     WHERE  emp.emp_name = emp_name 
                             OR bonus_amt < emp.salary) );

-- not exists and in predicate within AND
-- TC.02.05
SELECT * FROM bonus WHERE NOT EXISTS 
( 
       SELECT * 
       FROM   emp 
       WHERE  emp.emp_name = emp_name 
       AND    bonus_amt > emp.salary) 
AND 
emp_name IN 
( 
       SELECT emp_name 
       FROM   emp 
       WHERE  bonus_amt < emp.salary);

