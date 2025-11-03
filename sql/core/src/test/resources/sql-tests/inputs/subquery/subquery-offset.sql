--ONLY_IF spark
drop table if exists x;
drop table if exists y;

create table x(x1 int, x2 int) using json;
insert into x values (1, 1), (2, 2);
create table y(y1 int, y2 int) using json;
insert into y values (1, 1), (1, 2), (2, 4);

select * from x where exists (select * from y where x1 = y1 limit 1 offset 2);
select * from x join lateral (select * from y where x1 = y1 limit 1 offset 2);
select * from x where x1 in (select y1 from y limit 1 offset 2);
select * from x where (select sum(y2) from y where x1 = y1 limit 1 offset 2) > 2;

select * from x where exists (select * from y where x1 = y1 offset 2);
select * from x join lateral (select * from y where x1 = y1 offset 2);
select * from x where x1 in (select y1 from y offset 2);
select * from x where (select sum(y2) from y where x1 = y1 offset 2) > 2;

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

SELECT emp_name
FROM   emp
WHERE EXISTS (SELECT max(dept.dept_id) a
                   FROM   dept
                   WHERE  dept.dept_id = emp.dept_id
                   GROUP  BY state
                   ORDER  BY state
                   LIMIT 2
                   OFFSET 1);

SELECT emp_name
FROM   emp
JOIN LATERAL (SELECT max(dept.dept_id) a
                   FROM   dept
                   WHERE  dept.dept_id = emp.dept_id
                   GROUP  BY state
                   ORDER  BY state
                   LIMIT 2
                   OFFSET 1);

SELECT emp_name
FROM   emp
WHERE EXISTS (SELECT max(dept.dept_id) a
                   FROM   dept
                   WHERE  dept.dept_id = emp.dept_id
                   GROUP  BY state
                   ORDER  BY state
                   OFFSET 1);

SELECT emp_name
FROM   emp
JOIN LATERAL (SELECT max(dept.dept_id) a
                   FROM   dept
                   WHERE  dept.dept_id = emp.dept_id
                   GROUP  BY state
                   ORDER  BY state
                   OFFSET 1);

drop table x;
drop table y;
