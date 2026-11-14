--ONLY_IF spark
CREATE OR REPLACE VIEW t1(a1, a2) as values (0, 1), (1, 2);
CREATE OR REPLACE VIEW t2(b1, b2) as values (0, 2), (0, 3);
CREATE OR REPLACE VIEW t3(c1, c2) as values (0, 2), (0, 3);

set spark.sql.optimizer.decorrelateInnerQuery.enabled=true;
set spark.sql.legacy.scalarSubqueryCountBugBehavior=false;

-- test for count bug in nested aggregates in correlated scalar subqueries
select ( select sum(cnt) from (select count(*) cnt from t2 where t1.a1 = t2.b1) ) a from t1 order by a desc;

-- test for count bug in nested counts in correlated scalar subqueries
select ( select count(*) from (select count(*) cnt from t2 where t1.a1 = t2.b1) ) a from t1 order by a desc;

-- test for count bug in correlated scalar subqueries with nested aggregates with multiple counts
select (
  select SUM(l.cnt + r.cnt)
  from (select count(*) cnt from t2 where t1.a1 = t2.b1 having cnt = 0) l
  join (select count(*) cnt from t3 where t1.a1 = t3.c1 having cnt = 0) r
  on l.cnt = r.cnt
) a from t1 order by a desc;

-- same as above, without HAVING clause
select (
  select sum(l.cnt + r.cnt)
  from (select count(*) cnt from t2 where t1.a1 = t2.b1) l
  join (select count(*) cnt from t3 where t1.a1 = t3.c1) r
  on l.cnt = r.cnt
) a from t1 order by a desc;

-- test for count bug in nested aggregates whose lower aggregate has no function with a default
-- result, such as COUNT: the lower aggregate returns one row even when no row of t2 matches
select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select count(*) from (select min(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select count(*) from (select sum(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select count(*) from (select avg(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select sum(s) from (select coalesce(sum(b2), 10) s from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with a filter between the aggregates
select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1)
  where m is null ) a
from t1 order by a1;

-- with a literal next to the lower aggregate function
select a1, ( select sum(s) from (select 10 s, max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with a non-equality correlated predicate
select a1, ( select sum(s) from (select coalesce(sum(b2), 10) s from t2 where t2.b1 < t1.a1) ) a
from t1 order by a1;

-- with three levels of aggregates
select a1, ( select count(*) from (select max(m) m2 from (select max(b2) m from t2
  where t1.a1 = t2.b1)) ) a
from t1 order by a1;

-- with a grouped aggregate between the aggregates
select a1, ( select count(*) from (select m from (select max(b2) m from t2
  where t1.a1 = t2.b1) group by m) ) a
from t1 order by a1;

-- with a grouped top aggregate
select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1) group by m ) a
from t1 order by a1;

-- with correlated predicates in both aggregates
select a1, ( select sum(s) from (select coalesce(sum(b2), 10) s from t2 where t1.a1 = t2.b1)
  where t1.a2 > 1 ) a
from t1 order by a1;

-- with an outer row whose correlated column is null
select a1, ( select count(*) from (select max(b2) m from t2 where t.a1 = t2.b1) ) a
from values (0), (1), (null) as t(a1) order by a1;

-- with a join, a union, an intersect, a limit or a window function over the lower aggregates
select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1) l,
  (select min(b2) n from t2 where t1.a1 = t2.b1) r ) a
from t1 order by a1;

select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1
  union all select min(b2) from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1
  intersect select max(b2) from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1 limit 1) ) a
from t1 order by a1;

select a1, ( select count(*) from (select m, rank() over (order by m) r from
  (select max(b2) m from t2 where t1.a1 = t2.b1)) ) a
from t1 order by a1;

-- with an uncorrelated lower aggregate and a correlated predicate above it
select a1, ( select count(*) from (select max(b2) m from t2) where m > t1.a2 + 1 ) a
from t1 order by a1;

select a1, ( select count(*) from (select count(*) c from t2) where c > t1.a2 ) a
from t1 order by a1;

-- with top aggregates over the one row of a lower aggregate that returns NULL on empty input:
-- functions that return over a NULL row what they return over no rows, such as SUM and
-- COLLECT_LIST, and others, such as COUNT(*), with FILTER, DISTINCT, decimal and bigint inputs
select a1, ( select collect_list(m) from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select sum(m) + 1 from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select stddev_samp(m) from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select struct(sum(m), count(m), max(m), min(m), avg(m), collect_set(m))
  from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select collect_list(m) respect nulls from (select max(b2) m from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select collect_set(m) respect nulls from (select max(b2) m from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select first(m) respect nulls from (select max(b2) m from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select struct(count(*), collect_list(m)) from (select max(b2) m from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select struct(count(*) filter (where m is null), count(*) filter (where m > 2))
  from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select struct(sum(m) filter (where m > 2), sum(m) filter (where m > 3),
  count(m) filter (where m is null), collect_list(m) filter (where m is null))
  from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- a FILTER condition that fails on the NULL row fails for it, as in the LATERAL and uncorrelated
-- forms of the query
select a1, ( select sum(m) filter (where 10 / coalesce(m, 0) > 1) from (select max(b2) m from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select struct(first(m, true), last(m, true), first(m), last(m)) from
  (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select struct(count(distinct m), sum(distinct m)) from (select max(b2) m from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select struct(sum(m), avg(m)) from (select max(cast(b2 as decimal(10, 2))) m
  from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select struct(sum(m), avg(m), max(m)) from (select max(cast(b2 as bigint)
  + 9223372036854775800) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select struct(stddev_samp(m), var_pop(m), first(m), count(*) + max(m)) from
  (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with a lower function with a default result, such as COUNT or LISTAGG, which the top
-- aggregate does not use or whose default result is NULL
select a1, ( select sum(m) from (select max(b2) m, count(b2) c from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select count(s) from (select listagg(cast(b2 as string)) s from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with nested subqueries in the top aggregate: an uncorrelated one next to the aggregate
-- function, over a lower MAX next to a COUNT and alone; a correlated one in its argument, over a
-- lower MAX; a correlated one in a FILTER, over a lower MAX alone, next to a COUNT, and over an
-- uncorrelated lower COUNT; an uncorrelated one in a FILTER, and a correlated one in a Project
-- between that a FILTER uses, over a lower MAX
select a1, ( select sum(m) + (select max(c2) from t3) from (select max(b2) m, count(b2) c from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select sum(m) + (select max(c2) from t3) from (select max(b2) m from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select sum((select max(c2) from t3 where t3.c2 = m)) from (select max(b2) m from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select sum(m) filter (where (select max(c2) from t3 where t3.c2 = m) is not null)
  from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select sum(m) filter (where (select max(c2) from t3) > 2)
  from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select sum(m) filter (where t is not null) from (select m,
  (select max(c2) from t3 where t3.c2 = m) t from (select max(b2) m from t2
  where t1.a1 = t2.b1)) ) a
from t1 order by a1;

select a1, ( select sum(m) filter (where (select max(c2) from t3 where t3.c2 = m) is not null)
  from (select max(b2) m, count(b2) c from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select count(*) filter (where (select max(c2) from t3 where t3.c2 = c) is not null)
  from (select count(*) c from t2) where c > t1.a2 ) a
from t1 order by a1;

-- with a grouped top aggregate, a function not in the list, and three levels
select a1, ( select collect_list(m) from (select max(b2) m from t2 where t1.a1 = t2.b1)
  group by m ) a
from t1 order by a1;

select a1, ( select percentile(m, 0.5) from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select sum(m) from (select max(c) m from (select count(*) c from t2
  where t1.a1 = t2.b1)) ) a
from t1 order by a1;

-- with a grouped lower aggregate, which does not return a row on empty input
select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1 group by b2) ) a
from t1 order by a1;

-- with projections over a single correlated aggregate
select a1, ( select s + 1 from (select max(b2) s from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select coalesce(c, 10) from (select count(*) c from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with an expression that fails on the value of a lower aggregate over no rows, which is
-- evaluated only for an outer row without matching rows: every outer row matches in the first
-- two queries and the CAST query below, and the query over every row of t1 fails when the value
-- for a1 = 1 is computed. Until SPARK-61444, these five queries, up to the CAST query, all fail
-- while the query is optimized; after it, the first two and the CAST query return results, and
-- the other two fail at runtime.
select a1, ( select count(*) from (select 10 / coalesce(max(b2), 0) d from t2
  where t1.a1 = t2.b1) ) a
from t1 where a1 = 0 order by a1;

select a1, ( select sum(d) from (select 10 / coalesce(max(b2), 0) d from t2
  where t1.a1 = t2.b1) ) a
from t1 where a1 = 0 order by a1;

select a1, ( select sum(d) from (select 10 / coalesce(max(b2), 0) d from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- the expression is also evaluated for an outer row that another condition removes
select a1 from t1 where a1 = 0 and ( select sum(d) from (select 10 / coalesce(max(b2), 0) d
  from t2 where t1.a1 = t2.b1) ) is not null order by a1;

select a1, ( select sum(d) from (select cast(coalesce(max(cast(b2 as string)), 'x') as int) d
  from t2 where t1.a1 = t2.b1) ) a
from t1 where a1 = 0 order by a1;

-- with a top aggregate over an expression of a lower COUNT, and with a nested correlated
-- subquery over a lower COUNT
select a1, ( select max(10 / c) from (select count(*) c from t2 where t1.a1 = t2.b1) ) a
from t1 where a1 = 0 order by a1;

select a1, ( select c + (select max(c2) from t3 where t3.c1 = c or t3.c1 = c - 2) from
  (select count(*) c from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with the legacy behavior, which handles the count bug in decorrelation only if a lower
-- aggregate without GROUP BY has a function with a default result, such as COUNT
set spark.sql.legacy.scalarSubqueryNestedAggregateCountBugBehavior=true;

select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select sum(cnt) from (select count(*) cnt from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

reset spark.sql.legacy.scalarSubqueryNestedAggregateCountBugBehavior;
reset spark.sql.optimizer.decorrelateInnerQuery.enabled;
reset spark.sql.legacy.scalarSubqueryCountBugBehavior;
DROP VIEW t1;
DROP VIEW t2;
DROP VIEW t3;
