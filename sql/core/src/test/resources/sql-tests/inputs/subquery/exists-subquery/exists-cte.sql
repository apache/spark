-- Tests EXISTS subquery used along with 
-- Common Table Expressions(CTE)

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

-- CTE used inside subquery with correlated condition 
-- TC.01.01 
WITH bonus_cte 
     AS (SELECT * 
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
                          ORDER  BY emp.emp_name)) 
SELECT * 
FROM   bonus a 
WHERE  a.bonus_amt > 30 
       AND EXISTS (SELECT 1 
                   FROM   bonus_cte b 
                   WHERE  a.emp_name = b.emp_name); 

-- Inner join between two CTEs with correlated condition
-- TC.01.02
WITH emp_cte 
     AS (SELECT * 
         FROM   emp 
         WHERE  id >= 100 
                AND id <= 300), 
     dept_cte 
     AS (SELECT * 
         FROM   dept 
         WHERE  dept_id = 10) 
SELECT * 
FROM   bonus 
WHERE  EXISTS (SELECT * 
               FROM   emp_cte a 
                      JOIN dept_cte b 
                        ON a.dept_id = b.dept_id 
               WHERE  bonus.emp_name = a.emp_name); 

-- Left outer join between two CTEs with correlated condition
-- TC.01.03
WITH emp_cte 
     AS (SELECT * 
         FROM   emp 
         WHERE  id >= 100 
                AND id <= 300), 
     dept_cte 
     AS (SELECT * 
         FROM   dept 
         WHERE  dept_id = 10) 
SELECT DISTINCT b.emp_name, 
                b.bonus_amt 
FROM   bonus b, 
       emp_cte e, 
       dept d 
WHERE  e.dept_id = d.dept_id 
       AND e.emp_name = b.emp_name 
       AND EXISTS (SELECT * 
                   FROM   emp_cte a 
                          LEFT JOIN dept_cte b 
                                 ON a.dept_id = b.dept_id 
                   WHERE  e.emp_name = a.emp_name); 

-- Joins inside cte and aggregation on cte referenced subquery with correlated condition 
-- TC.01.04 
WITH empdept 
     AS (SELECT id, 
                salary, 
                emp_name, 
                dept.dept_id 
         FROM   emp 
                LEFT JOIN dept 
                       ON emp.dept_id = dept.dept_id 
         WHERE  emp.id IN ( 100, 200 )) 
SELECT emp_name, 
       Sum(bonus_amt) 
FROM   bonus 
WHERE  EXISTS (SELECT dept_id, 
                      max(salary) 
               FROM   empdept 
               GROUP  BY dept_id 
               HAVING count(*) > 1) 
GROUP  BY emp_name; 

-- Using not exists 
-- TC.01.05      
WITH empdept 
     AS (SELECT id, 
                salary, 
                emp_name, 
                dept.dept_id 
         FROM   emp 
                LEFT JOIN dept 
                       ON emp.dept_id = dept.dept_id 
         WHERE  emp.id IN ( 100, 200 )) 
SELECT emp_name, 
       Sum(bonus_amt) 
FROM   bonus 
WHERE  NOT EXISTS (SELECT dept_id, 
                          Max(salary) 
                   FROM   empdept 
                   GROUP  BY dept_id 
                   HAVING count(*) < 1) 
GROUP  BY emp_name; 
