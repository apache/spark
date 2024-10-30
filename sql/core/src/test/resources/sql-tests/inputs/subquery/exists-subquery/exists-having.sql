-- Tests HAVING clause in subquery.

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

-- simple having in subquery. 
-- TC.01.01
SELECT dept_id, count(*) 
FROM   emp 
GROUP  BY dept_id 
HAVING EXISTS (SELECT 1 
               FROM   bonus 
               WHERE  bonus_amt < min(emp.salary)); 

-- nested having in subquery
-- TC.01.02
SELECT * 
FROM   dept 
WHERE  EXISTS (SELECT dept_id, 
                      Count(*) 
               FROM   emp 
               GROUP  BY dept_id 
               HAVING EXISTS (SELECT 1 
                              FROM   bonus 
                              WHERE bonus_amt < Min(emp.salary)));

-- aggregation in outer and inner query block with having
-- TC.01.03
SELECT dept_id, 
       Max(salary) 
FROM   emp gp 
WHERE  EXISTS (SELECT dept_id, 
                      Count(*) 
               FROM   emp p
               GROUP  BY dept_id 
               HAVING EXISTS (SELECT 1 
                              FROM   bonus 
                              WHERE  bonus_amt < Min(p.salary))) 
GROUP  BY gp.dept_id;

-- more aggregate expressions in projection list of subquery
-- TC.01.04
SELECT * 
FROM   dept 
WHERE  EXISTS (SELECT dept_id, 
                        Count(*) 
                 FROM   emp 
                 GROUP  BY dept_id 
                 HAVING EXISTS (SELECT 1 
                                FROM   bonus 
                                WHERE  bonus_amt > Min(emp.salary)));

-- multiple aggregations in nested subquery
-- TC.01.05
SELECT * 
FROM   dept 
WHERE  EXISTS (SELECT dept_id, 
                      count(emp.dept_id)
               FROM   emp 
               WHERE  dept.dept_id = dept_id 
               GROUP  BY dept_id 
               HAVING EXISTS (SELECT 1 
                              FROM   bonus 
                              WHERE  ( bonus_amt > min(emp.salary) 
                                       AND count(emp.dept_id) > 1 )));
