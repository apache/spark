-- Tests for scalar subquery with a group-by. Only a group-by that guarantees a single row result is allowed. See SPARK-48503

--ONLY_IF spark

create temp view x (x1, x2) as values (1, 1), (2, 2);
create temp view y (y1, y2) as values (2, 0), (3, -1);
create temp view z (z1, z2) as values (1, 0), (1, 1);

-- Legal queries
select * from x where (select count(*) from y where y1 = x1 group by y1) = 1;
select * from x where (select count(*) from y where y1 = x1 group by x1) = 1;
select * from x where (select count(*) from y where y1 > x1 group by x1) = 1;

-- Group-by column equal to constant - legal
select *, (select count(*) from y where x1 = y1 and y2 = 1 group by y2) from x;
-- Group-by column equal to expression with constants and outer refs - legal
select *, (select count(*) from y where x1 = y1 and y2 = x1 + 1 group by y2) from x;

-- Illegal queries
select * from x where (select count(*) from y where y1 > x1 group by y1) = 1;
select *, (select count(*) from y where y1 + y2 = x1 group by y1) from x;

-- Certain other operators like OUTER JOIN or UNION between the correlating filter and the group-by also can cause the scalar subquery to return multiple values and hence make the query illegal.
select *, (select count(*) from (select * from y where y1 = x1 union all select * from y) sub group by y1) from x;
select *, (select count(*) from y left join (select * from z where z1 = x1) sub on y2 = z2 group by z1) from x; -- The correlation below the join is unsupported in Spark anyway, but when we do support it this query should still be disallowed.

-- Test legacy behavior conf
set spark.sql.legacy.scalarSubqueryAllowGroupByNonEqualityCorrelatedPredicate = true;
select * from x where (select count(*) from y where y1 > x1 group by y1) = 1;
reset spark.sql.legacy.scalarSubqueryAllowGroupByNonEqualityCorrelatedPredicate;
