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

-- with a join, a union, a limit or a window function over the lower aggregates
select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1) l,
  (select min(b2) n from t2 where t1.a1 = t2.b1) r ) a
from t1 order by a1;

select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1
  union all select min(b2) from t2 where t1.a1 = t2.b1) ) a
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

-- with top aggregates computed over the one row of the lower aggregate: an imperative function
-- such as COLLECT_LIST, alone or with a declarative one, FILTER, DISTINCT, decimal and bigint
-- inputs, and functions with a special result over one row
select a1, ( select collect_list(m) from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select struct(count(*), collect_list(m)) from (select max(b2) m from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select struct(count(*) filter (where m is null), count(*) filter (where m > 2),
  sum(m) filter (where m > 2), sum(m) filter (where m > 3)) from (select max(b2) m from t2
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

-- with a grouped lower aggregate, which does not return exactly one row
select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1 group by b2) ) a
from t1 order by a1;

-- with projections over a single correlated aggregate
select a1, ( select s + 1 from (select max(b2) s from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select coalesce(c, 10) from (select count(*) c from t2 where t1.a1 = t2.b1) ) a
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
