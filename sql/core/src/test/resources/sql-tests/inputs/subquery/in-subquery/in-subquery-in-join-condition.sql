-- Test that correlated EXISTS subqueries in join conditions are supported.

-- Permutations of the test:
-- 1. In / Not In
-- 2. Reference left / right child
-- 3. Join type: inner / left outer
-- 4. AND or OR for the join condition

CREATE TEMP VIEW x(x1, x2) AS VALUES
    (0, 1),
    (1, 1),
    (1, 2),
    (3, 4),
    (5, 6),
    (9, 10);

CREATE TEMP VIEW y(y1, y2) AS VALUES
    (0, 2),
    (1, 4),
    (1, 5),
    (2, 6),
    (3, 8),
    (4, 11);

CREATE TEMP VIEW z(z1, z2) AS VALUES
    (0, 2),
    (1, 4),
    (3, 3),
    (3, 6),
    (8, 11);


--Correlated IN, REFERENCE LEFT, INNER JOIN
select * from x inner join y on x1 = y1 and x2 IN (select z1 from z where z2 = x2) order by x1, x2, y1, y2;

--Correlated NOT IN, REFERENCE LEFT, INNER JOIN
select * from x inner join y on x1 = y1 and x2 not IN (select z1 from z where z2 = x2) order by x1, x2, y1, y2;

--Correlated IN, REFERENCE LEFT, LEFT OUTER JOIN
select * from x left join y on x1 = y1 and x2 IN (select z1 from z where z2 = x2) order by x1, x2, y1, y2;

--Correlated NOT IN, REFERENCE LEFT, LEFT OUTER JOIN
select * from x left join y on x1 = y1 and x2 not IN (select z1 from z where z2 = x2) order by x1, x2, y1, y2;

--Correlated IN, REFERENCE RIGHT, INNER JOIN
select * from x inner join y on x1 = y1 and y2 IN (select z1 from z where z2 = y2) order by x1, x2, y1, y2;

--Correlated NOT IN, REFERENCE RIGHT, INNER JOIN
select * from x inner join y on x1 = y1 and y2 not IN (select z1 from z where z2 = y2) order by x1, x2, y1, y2;

--Correlated IN, REFERENCE RIGHT, LEFT OUTER JOIN
select * from x left join y on x1 = y1 and y2 IN (select z1 from z where z2 = y2) order by x1, x2, y1, y2;

--Correlated NOT IN, REFERENCE RIGHT, LEFT OUTER JOIN
select * from x left join y on x1 = y1 and y2 not IN (select z1 from z where z2 = y2) order by x1, x2, y1, y2;

-- Queries below are same as above, but with OR inside of AND in the join condition
select * from x inner join y on x1 = y1 or x2 IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x inner join y on x1 = y1 or x2 not IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 or x2 IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 or x2 not IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x inner join y on x1 = y1 or y2 IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
select * from x inner join y on x1 = y1 or y2 not IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 or y2 IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 or y2 not IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;

-- Queries below are same as above, but with transitive predicates to test if inferring filters
-- can cause issues.
select * from x inner join y on x1 = y1 and x2 IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x inner join y on x1 = y1 and x2 not IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 and x2 IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 and x2 not IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x inner join y on x1 = y1 and y2 IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
select * from x inner join y on x1 = y1 and y2 not IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 and y2 IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 and y2 not IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
