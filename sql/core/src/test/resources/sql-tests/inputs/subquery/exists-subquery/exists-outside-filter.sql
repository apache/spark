-- Tests EXISTS subquery support where the subquery is used outside the WHERE clause.

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
