-- Tests NOT-IN subqueries nested inside OR expression(s).

--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=true
--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=false

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

CREATE TEMPORARY VIEW ADDRESS(id, emp_name, address) AS VALUES
  (100, "emp 1", "addr1"),
  (200, null, "addr2"),
  (null, "emp 3", "addr3"),
  (null, null, "addr4"),
  (600, "emp 6", "addr6"),
  (800, "emp 8", "addr8");

CREATE TEMPORARY VIEW S1(a, b) AS VALUES
  (null, null), (5, 5), (8, 8), (11, 11);
CREATE TEMPORARY VIEW S2(c, d) AS VALUES
  (7, 7), (8, 8), (11, 11), (null, null);

-- null produced from both sides.
-- TC.01.01
SELECT id, 
       dept_id 
FROM   emp 
WHERE  id = 600
       OR id = 500 
       OR dept_id NOT IN (SELECT dept_id 
                          FROM   emp);

-- null produced from right side
-- TC.01.02
SELECT id, 
       dept_id 
FROM   emp 
WHERE  id = 800 
       OR (dept_id IS NOT NULL 
           AND dept_id NOT IN (SELECT dept_id 
                                FROM   emp));

-- null produced on left side
-- TC.01.03
SELECT id,
       dept_id
FROM   emp
WHERE  id = 100
       OR dept_id NOT IN (SELECT dept_id
                           FROM   emp
                           WHERE dept_id IS NOT NULL);

-- no null in both left and right
-- TC.01.04
SELECT id, 
       dept_id 
FROM   emp 
WHERE  id = 200 
       OR (dept_id IS NOT NULL        
       AND dept_id + 100 NOT IN (SELECT dept_id 
                           FROM   emp
                           WHERE dept_id IS NOT NULL));

-- complex nesting
-- TC.01.05
SELECT id, 
       dept_id, 
       emp_name 
FROM   emp 
WHERE  emp_name IN (SELECT emp_name 
                    FROM   bonus) 
        OR (dept_id IS NOT NULL 
            AND dept_id NOT IN (SELECT dept_id 
                                FROM   dept));

-- complex nesting, exists in disjunction with not-in
-- TC.01.06
SELECT id, 
       dept_id, 
       emp_name 
FROM   emp 
WHERE  EXISTS (SELECT emp_name 
               FROM   bonus 
               WHERE  emp.emp_name = bonus.emp_name) 
       OR (dept_id IS NOT NULL 
           AND dept_id NOT IN (SELECT dept_id 
                               FROM   dept));

-- multiple columns in not-in
-- TC.01.07
SELECT id,
       dept_id,
       emp_name
FROM   emp
WHERE  dept_id = 10
OR (id, emp_name) NOT IN (SELECT id, emp_name FROM address);

-- multiple columns in not-in
-- TC.01.08
SELECT id, 
       dept_id, 
       emp_name 
FROM   emp 
WHERE  dept_id = 10 
        OR (( id, emp_name ) NOT IN (SELECT id, 
                                             emp_name 
                                      FROM   address 
                                      WHERE  id IS NOT NULL 
                                             AND emp_name IS NOT NULL) 
             AND id > 400 );
-- correlated not-in along with disjunction
-- TC.01.09
SELECT id, 
       dept_id, 
       emp_name 
FROM   emp 
WHERE  dept_id = 10 
       OR emp_name NOT IN (SELECT emp_name 
                                  FROM   address 
                                  WHERE  id IS NOT NULL 
                                  AND emp_name IS NOT NULL
                                  AND emp.id = address.id);

-- multiple not-in(s) in side disjunction`
-- TC.01.10
SELECT id, 
       dept_id, 
       emp_name 
FROM   emp 
WHERE  id NOT IN (SELECT id 
                         FROM   address 
                         WHERE  id IS NOT NULL 
                         AND emp_name IS NOT NULL
                         AND id >= 400)
       OR emp_name NOT IN (SELECT emp_name 
                                  FROM   address 
                                  WHERE  id IS NOT NULL 
                                  AND emp_name IS NOT NULL
                                  AND emp.id = address.id
                                  AND id < 400);

-- NOT (NOT IN (SUBQ))
SELECT * 
FROM   s1 
WHERE  NOT (a NOT IN (SELECT c 
                      FROM   s2));

-- NOT (OR (expression, IN-SUBQ)) 
SELECT * 
FROM   s1 
WHERE  NOT (a > 5 
            OR a IN (SELECT c 
                     FROM   s2));

-- NOT (OR (expression, NOT-IN-SUB)
SELECT * 
FROM   s1 
WHERE  NOT (a > 5 
            OR a NOT IN (SELECT c 
                         FROM   s2));

-- NOT (AND (expression, IN-SUB))
SELECT * 
FROM   s1 
WHERE  NOT (a > 5 
            AND a IN (SELECT c 
                      FROM   s2));
 
-- NOT (AND (expression, NOT-IN-SUBQ))
SELECT * 
FROM   s1 
WHERE  NOT (a > 5 
            AND a NOT IN (SELECT c 
                          FROM   s2));
