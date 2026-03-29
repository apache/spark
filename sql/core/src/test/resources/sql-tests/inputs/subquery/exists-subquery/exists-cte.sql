-- Tests EXISTS subquery used along with 
-- Common Table Expressions(CTE)
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
