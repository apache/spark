-- Tests EXISTS subquery support. Tests EXISTS 
-- subquery within a AND or OR expression.

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

