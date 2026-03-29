-- Test that correlated EXISTS subqueries in join conditions are supported.
--ONLY_IF spark

-- Permutations of the test:
-- 1. In / Not In
-- 2. Reference left / right child
-- 3. Join type: inner / left outer / right outer / full outer / left semi / left anti
-- 4. AND or OR for the join condition

CREATE TEMP VIEW x(x1, x2) AS VALUES
    (2, 1),
    (1, 1),
    (3, 4);

CREATE TEMP VIEW y(y1, y2) AS VALUES
    (0, 2),
    (1, 4),
    (4, 11);

CREATE TEMP VIEW z(z1, z2) AS VALUES
    (4, 2),
    (3, 3),
    (8, 1);


--Correlated IN, REFERENCE LEFT, INNER JOIN
select * from x inner join y on x1 = y1 and x2 IN (select z1 from z where z2 = x2) order by x1, x2, y1, y2;

--Correlated NOT IN, REFERENCE LEFT, INNER JOIN
select * from x inner join y on x1 = y1 and x2 not IN (select z1 from z where z2 = x2) order by x1, x2, y1, y2;

--Correlated IN, REFERENCE RIGHT, INNER JOIN
select * from x inner join y on x1 = y1 and y2 IN (select z1 from z where z2 = y2) order by x1, x2, y1, y2;

--Correlated NOT IN, REFERENCE RIGHT, INNER JOIN
select * from x inner join y on x1 = y1 and y2 not IN (select z1 from z where z2 = y2) order by x1, x2, y1, y2;

-- Same as above, but for left join
select * from x left join y on x1 = y1 and x2 IN (select z1 from z where z2 = x2) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 and x2 not IN (select z1 from z where z2 = x2) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 and y2 IN (select z1 from z where z2 = y2) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 and y2 not IN (select z1 from z where z2 = y2) order by x1, x2, y1, y2;

-- Same as above, but for right join
select * from x right join y on x1 = y1 and x2 IN (select z1 from z where z2 = x2) order by x1, x2, y1, y2;
select * from x right join y on x1 = y1 and x2 not IN (select z1 from z where z2 = x2) order by x1, x2, y1, y2;
select * from x right join y on x1 = y1 and y2 IN (select z1 from z where z2 = y2) order by x1, x2, y1, y2;
select * from x right join y on x1 = y1 and y2 not IN (select z1 from z where z2 = y2) order by x1, x2, y1, y2;

-- Same as above, but for left semi join
select * from x left semi join y on x1 = y1 and x2 IN (select z1 from z where z2 = x2) order by x1, x2;
select * from x left semi join y on x1 = y1 and x2 not IN (select z1 from z where z2 = x2) order by x1, x2;
select * from x left semi join y on x1 = y1 and y2 IN (select z1 from z where z2 = y2) order by x1, x2;
select * from x left semi join y on x1 = y1 and y2 not IN (select z1 from z where z2 = y2) order by x1, x2;

-- Same as above, but for left anti join
select * from x left anti join y on x1 = y1 and x2 IN (select z1 from z where z2 = x2) order by x1, x2;
select * from x left anti join y on x1 = y1 and x2 not IN (select z1 from z where z2 = x2) order by x1, x2;
select * from x left anti join y on x1 = y1 and y2 IN (select z1 from z where z2 = y2) order by x1, x2;
select * from x left anti join y on x1 = y1 and y2 not IN (select z1 from z where z2 = y2) order by x1, x2;

-- Same as above, but for full outer join
select * from x full outer join y on x1 = y1 and x2 IN (select z1 from z where z2 = x2) order by x1, x2, y1, y2;
select * from x full outer join y on x1 = y1 and x2 not IN (select z1 from z where z2 = x2) order by x1, x2, y1, y2;
select * from x full outer join y on x1 = y1 and y2 IN (select z1 from z where z2 = y2) order by x1, x2, y1, y2;
select * from x full outer join y on x1 = y1 and y2 not IN (select z1 from z where z2 = y2) order by x1, x2, y1, y2;

-- OR instead of AND in the join condition
select * from x inner join y on x1 = y1 or x2 IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x inner join y on x1 = y1 or x2 not IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 or x2 IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 or x2 not IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x inner join y on x1 = y1 or y2 IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
select * from x inner join y on x1 = y1 or y2 not IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 or y2 IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 or y2 not IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;

-- Transitive predicates to test if inferring filters can cause issues.
select * from x inner join y on x1 = y1 and x2 IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x inner join y on x1 = y1 and x2 not IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 and x2 IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 and x2 not IN (select z1 from z where z1 = x1) order by x1, x2, y1, y2;
select * from x inner join y on x1 = y1 and y2 IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
select * from x inner join y on x1 = y1 and y2 not IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 and y2 IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 and y2 not IN (select z1 from z where z1 = y1) order by x1, x2, y1, y2;

-- Correlated subquery references both left and right children, errors
select * from x left join y on x1 = y1 and x2 IN (select z1 from z where z2 = x2 AND z2 = y2) order by x1, x2, y1, y2;
select * from x left join y on x1 = y1 and x2 not IN (select z1 from z where z2 = x2 AND z2 = y2) order by x1, x2, y1, y2;
