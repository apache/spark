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
-- Group-by expression is the same as the one we filter on - legal
select *, (select count(*) from y where x1 = y1 and cast(y2 as double) = x1 + 1
           group by cast(y2 as double)) from x;
-- Group-by expression equal to an expression that depends on 2 outer refs -- legal
select *, (select count(*) from y where y2 + 1 = x1 + x2 group by y2 + 1) from x;


-- Illegal queries (single join disabled)
set spark.sql.optimizer.scalarSubqueryUseSingleJoin = false;
select * from x where (select count(*) from y where y1 > x1 group by y1) = 1;
select *, (select count(*) from y where y1 + y2 = x1 group by y1) from x;
select *, (select count(*) from y where x1 = y1 and y2 + 10 = x1 + 1 group by y2) from x;

-- Same queries, with LeftSingle join
set spark.sql.optimizer.scalarSubqueryUseSingleJoin = true;
select * from x where (select count(*) from y where y1 > x1 group by y1) = 1;
select *, (select count(*) from y where y1 + y2 = x1 group by y1) from x;
select *, (select count(*) from y where x1 = y1 and y2 + 10 = x1 + 1 group by y2) from x;


-- Certain other operators like OUTER JOIN or UNION between the correlating filter and the group-by also can cause the scalar subquery to return multiple values and hence make the query illegal.
select *, (select count(*) from (select * from y where y1 = x1 union all select * from y) sub group by y1) from x;
select *, (select count(*) from y left join (select * from z where z1 = x1) sub on y2 = z2 group by z1) from x; -- The correlation below the join is unsupported in Spark anyway, but when we do support it this query should still be disallowed.

-- Test legacy behavior conf
set spark.sql.legacy.scalarSubqueryAllowGroupByNonEqualityCorrelatedPredicate = true;
set spark.sql.optimizer.scalarSubqueryUseSingleJoin = false;
select * from x where (select count(*) from y where y1 > x1 group by y1) = 1;
reset spark.sql.legacy.scalarSubqueryAllowGroupByNonEqualityCorrelatedPredicate;

-- Scalar subqueries with a WITH clause are validated with their CTEs inlined.
set spark.sql.legacy.scalarSubqueryAllowGroupByNonEqualityCorrelatedPredicate = false;
set spark.sql.optimizer.scalarSubqueryUseSingleJoin = false;
-- Legal queries
select *, (with cte as (select y1, count(*) as cnt from y where y1 = x1 group by y1) select cnt from cte) from x;
select *, (with cte as (select * from y where y1 = x1) select count(*) from cte group by y1) from x;
select *, (with cte1 as (select * from y where y1 = x1), cte2 as (select y1, count(*) as cnt from cte1 group by y1) select cnt from cte2) from x;
select *, (with cte as (select * from y where y1 = x1) select count(*) from cte left join z on y2 = z2 group by y1) from x;
-- The group-by column comes from the first reference to the CTE, which keeps its attributes
select *, (with cte as (select * from y where y1 = x1) select count(*) from cte a join cte b on a.y2 = b.y2 group by a.y1) from x;
-- The CTE is referenced only from a nested subquery
select *, (with cte as (select * from y) select count(*) from z where z1 = x1 and exists (select * from cte) group by z1) from x;
-- The aggregate is reached through a reference to a CTE that another CTE references first
select *, (with cte as (select y1, count(*) as cnt from y where y1 = 2 group by y1), other as (select * from cte) select cnt + x1 + (select count(*) from other) from cte) from x;
-- An unreferenced CTE references the correlated CTE on the right side of a left join
select *, (with cte as (select * from y where y1 = x1), unused as (select * from z left join cte on z1 = y1) select count(*) from z) from x;
select *, (with cte as (select * from y where y1 = x1), unused as (select * from z left join cte on z1 = y1) select count(*) from cte) from x;
-- The scalar subquery sits in the body of a CTE that the main query references
with t as (select *, (with cte as (select y1, count(*) as cnt from y where y1 = x1 group by y1) select cnt from cte) as n from x) select * from t;
-- Illegal queries
select *, (with cte as (select y1, count(*) as cnt from y where y1 > x1 group by y1) select cnt from cte) from x;
select *, (with cte as (select y2 from y where y1 = x1) select y2 from cte) from x;
select *, (with unused as (select 1 as one), cte as (select * from y where y1 = x1) select count(*) from cte group by y1) from x;
-- The correlated CTE is on the right side of a left join
select *, (with cte as (select * from y where y1 = x1) select count(*) from z left join cte on z1 = y1 where z1 = x1 group by z1) from x;
select *, (with cte as (select * from y where y1 = x1) select count(*) from z left join cte on z1 = y1) from x;
-- The correlated CTE is read on both sides of a left join, so each reference is checked in its own position
select *, (with cte as (select * from y where y1 = x1) select count(*) from cte a left join cte b on a.y2 = b.y2) from x;
-- The correlated CTE is referenced only from a nested subquery
select *, (with cte as (select y1 from y full outer join (select * from z where z1 = x1) s on y1 = s.z1) select count(*) from z where exists (select * from cte)) from x;
-- The group-by column is equal to a constant only in another reference to the CTE
select *, (with cte as (select * from y where y1 = 2) select count(*) from z left join cte a on z2 = a.y2 join cte b on true where z1 = x1 group by a.y1) from x;
select *, (with cte as (select * from y where y1 = 2) select count(*) from (select y1 from cte union all select z1 from z) u join cte b on true where b.y2 = x1 group by u.y1) from x;
-- The group-by column comes from a later reference to the CTE, whose equalities are not tracked
select *, (with cte as (select * from y where y1 = x1) select count(*) from cte a join cte b on a.y2 = b.y2 group by b.y1) from x;
-- Also when it comes from a reference to a CTE that passes on the columns of another reference
select *, (with cte as (select * from y where y1 = x1), other as (select * from cte) select count(*) from other a join cte b on a.y2 = b.y2 group by a.y1) from x;
-- The fixed-point analyzer keeps a CTE that nothing references with its original body, so it validates the scalar subquery
-- in that body without inlining the subquery's own CTE. This case runs with the fixed-point analyzer only.
set spark.sql.analyzer.singlePassResolver.dualRunWithLegacy = false;
with unused as (select *, (with cte as (select y1, count(*) as cnt from y where y1 = x1 group by y1) select cnt from cte) as n from x) select 1;
reset spark.sql.analyzer.singlePassResolver.dualRunWithLegacy;
reset spark.sql.legacy.scalarSubqueryAllowGroupByNonEqualityCorrelatedPredicate;
reset spark.sql.optimizer.scalarSubqueryUseSingleJoin;
