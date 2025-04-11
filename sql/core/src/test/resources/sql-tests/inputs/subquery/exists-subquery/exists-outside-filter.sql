-- Tests EXISTS subquery support where the subquery is used outside the WHERE clause.

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

-- uncorrelated select exist
-- TC.01.01
SELECT
  emp_name,
  EXISTS (SELECT 1
          FROM   dept
          WHERE  dept.dept_id > 10
            AND dept.dept_id < 30)
FROM   emp;

-- correlated select exist
-- TC.01.02
SELECT
  emp_name,
  EXISTS (SELECT 1
          FROM   dept
          WHERE  emp.dept_id = dept.dept_id)
FROM   emp;

-- uncorrelated exist in aggregate filter
-- TC.01.03
SELECT
  sum(salary),
  sum(salary) FILTER (WHERE EXISTS (SELECT 1
                                    FROM   dept
                                    WHERE  dept.dept_id > 10
                                      AND dept.dept_id < 30))
FROM   emp;

-- correlated exist in aggregate filter
-- TC.01.04
SELECT
  sum(salary),
  sum(salary) FILTER (WHERE EXISTS (SELECT 1
                                    FROM   dept
                                    WHERE  emp.dept_id = dept.dept_id))
FROM   emp;

-- Multiple correlated exist in aggregate filter
-- TC.01.05
SELECT
    sum(salary),
    sum(salary) FILTER (WHERE EXISTS (SELECT 1
                                    FROM   dept
                                    WHERE  emp.dept_id = dept.dept_id)
                        OR EXISTS (SELECT 1
                                    FROM   bonus
                                    WHERE  emp.emp_name = bonus.emp_name))
FROM   emp;

-- correlated exist in DISTINCT aggregate filter
-- TC.01.06
SELECT
    sum(DISTINCT salary),
    count(DISTINCT hiredate) FILTER (WHERE EXISTS (SELECT 1
                                    FROM   dept
                                    WHERE  emp.dept_id = dept.dept_id))
FROM   emp;

-- correlated exist in group by of an aggregate
-- TC.01.07
SELECT
    count(hiredate),
    sum(salary)
FROM   emp
GROUP BY EXISTS (SELECT 1
                FROM   dept
                WHERE  emp.dept_id = dept.dept_id);

-- correlated exist in group by of a distinct aggregate
-- TC.01.08
SELECT
    count(DISTINCT hiredate),
    sum(DISTINCT salary)
FROM   emp
GROUP BY EXISTS (SELECT 1
                 FROM   dept
                 WHERE  emp.dept_id = dept.dept_id);

-- uncorrelated exist in aggregate function
-- TC.01.09
SELECT
    count(CASE WHEN EXISTS (SELECT 1
                            FROM   dept
                            WHERE  dept.dept_id > 10
                              AND dept.dept_id < 30) THEN 1 END),
    sum(CASE WHEN EXISTS (SELECT 1
                          FROM   dept
                          WHERE  dept.dept_id > 10
                            AND dept.dept_id < 30) THEN salary END)
FROM   emp;

-- correlated exist in aggregate function
-- TC.01.10
SELECT
    count(CASE WHEN EXISTS (SELECT 1
                            FROM   dept
                            WHERE  emp.dept_id = dept.dept_id) THEN 1 END),
    sum(CASE WHEN EXISTS (SELECT 1
                          FROM   dept
                          WHERE  emp.dept_id = dept.dept_id) THEN salary END)
FROM   emp;

-- uncorrelated exist in window
-- TC.01.11
SELECT
    emp_name,
    sum(salary) OVER (PARTITION BY EXISTS (SELECT 1
                                           FROM   dept
                                           WHERE  dept.dept_id > 10
                                             AND dept.dept_id < 30))
FROM   emp;

-- correlated exist in window
-- TC.01.12
SELECT
    emp_name,
    sum(salary) OVER (PARTITION BY EXISTS (SELECT 1
                                           FROM   dept
                                           WHERE  emp.dept_id = dept.dept_id))
FROM   emp;
