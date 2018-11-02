-- Tests HAVING clause in subquery.

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
