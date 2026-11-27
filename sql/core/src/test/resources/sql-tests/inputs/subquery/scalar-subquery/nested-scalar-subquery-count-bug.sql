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

-- with a filter between the aggregates, where the result for an outer row without matching rows
-- is not computed before decorrelation, which keeps the wrong result of before
select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1)
  where m is null ) a
from t1 order by a1;

-- with a literal next to the lower aggregate function
select a1, ( select sum(s) from (select 10 s, max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with a non-equality correlated predicate, for which decorrelation adds a domain join, under
-- which the computed result is used only where it is the result of every outer row, so this one
-- keeps the wrong result of before
select a1, ( select sum(s) from (select coalesce(sum(b2), 10) s from t2 where t2.b1 < t1.a1) ) a
from t1 order by a1;

-- with a grouped top aggregate over a non-equality correlated predicate: the top sees the one
-- group of the row of the lower aggregate, so its result is that of every outer row
select a1, ( select count(*) from (select max(b2) m from t2 where t2.b1 < t1.a1) group by m ) a
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

-- with a correlated filter between the aggregates, which keeps the wrong result of before
select a1, ( select sum(s) from (select coalesce(sum(b2), 10) s from t2 where t1.a1 = t2.b1)
  where t1.a2 > 1 ) a
from t1 order by a1;

-- with an outer row whose correlated column is null
select a1, ( select count(*) from (select max(b2) m from t2 where t.a1 = t2.b1) ) a
from values (0), (1), (null) as t(a1) order by a1;

-- with a correlated predicate only over outer columns
select a1, ( select count(*) from (select max(b2) m from t2 where t1.a2 > 1) ) a
from t1 order by a1;

-- with a join, a union, an intersect, a limit or a window function over the lower aggregates,
-- which keeps the wrong results of before
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

-- where the result for an outer row without matching rows is NULL: no count-bug branch is added,
-- unlike before, where the result over no rows was not NULL (here 0)
select a1, ( select if(count(*) = 1, null, count(*))
  from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with top aggregates such as SUM, COLLECT_LIST and COUNT(*) over the one row of a lower aggregate
-- that returns NULL on empty input, with FILTER, DISTINCT, decimal and bigint inputs
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

-- a FILTER condition that fails on the NULL row keeps the result of before, unlike the LATERAL
-- and uncorrelated forms of the query, which fail
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
-- between that a FILTER uses, over a lower MAX. A correlated one in the argument or a FILTER over
-- a lower MAX alone fails with INTERNAL_ERROR, as before (SPARK-61615)
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

-- with a grouped top aggregate, another top aggregate function, and three levels
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

-- with an expression that fails on the value of a lower aggregate over no rows: it does not fail
-- a row with matching rows (a1 = 0), and SUM over it keeps the result of before for a row without
-- matching rows, unlike the uncorrelated form of the query, which fails
select a1, ( select count(*) from (select 10 / coalesce(max(b2), 0) d from t2
  where t1.a1 = t2.b1) ) a
from t1 where a1 = 0 order by a1;

select a1, ( select sum(d) from (select 10 / coalesce(max(b2), 0) d from t2
  where t1.a1 = t2.b1) ) a
from t1 where a1 = 0 order by a1;

select a1, ( select sum(d) from (select 10 / coalesce(max(b2), 0) d from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with another condition of the same WHERE
select a1 from t1 where a1 = 0 and ( select sum(d) from (select 10 / coalesce(max(b2), 0) d
  from t2 where t1.a1 = t2.b1) ) is not null order by a1;

-- with a CAST that fails on the value over no rows, where every outer row matches
select a1, ( select sum(d) from (select cast(coalesce(max(cast(b2 as string)), 'x') as int) d
  from t2 where t1.a1 = t2.b1) ) a
from t1 where a1 = 0 order by a1;

-- a COUNT(*) does not use the expression, so its result is computed
select a1, ( select count(*) from (select 10 / coalesce(max(b2), 0) d from t2
  where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with a grouping key that fails on the value over no rows, which keeps the result of before,
-- unlike the uncorrelated form of the query, which fails
select a1, ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1)
  group by 10 / coalesce(m, 0) ) a
from t1 order by a1;

-- with a top aggregate over an expression of a lower COUNT, and with a nested correlated
-- subquery over a lower COUNT
select a1, ( select max(10 / c) from (select count(*) c from t2 where t1.a1 = t2.b1) ) a
from t1 where a1 = 0 order by a1;

select a1, ( select c + (select max(c2) from t3 where t3.c1 = c or t3.c1 = c - 2) from
  (select count(*) c from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with the correlation below a UNION ALL, for which decorrelation adds a domain join
select a1, ( select count(*) from (select max(x) m from
  (select b2 x from t2 where t2.b1 = t1.a1 union all select c2 from t3 where t3.c1 = t1.a1)) ) a
from t1 order by a1;

select a1, ( select sum(m) from (select max(x) m from
  (select b2 x from t2 where t2.b1 = t1.a1 union all select c2 from t3 where t3.c1 = t1.a1)) ) a
from t1 order by a1;

-- with the subquery in a WHERE clause and in an aggregate function
select a1 from t1
where ( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1) ) = 1
order by a1;

select sum(( select count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1) )) a from t1;

-- with a grouped top aggregate that returns only a constant, also when constant folding is not
-- prevented for the count bug
select a1, ( select 7 from (select max(b2) m from t2 where t1.a1 = t2.b1) group by m ) a
from t1 order by a1;

set spark.sql.optimizer.decorrelateSubqueryPreventConstantHoldingForCountBug.enabled=false;

select a1, ( select 7 from (select max(b2) m from t2 where t1.a1 = t2.b1) group by m ) a
from t1 order by a1;

reset spark.sql.optimizer.decorrelateSubqueryPreventConstantHoldingForCountBug.enabled;

-- with COLLECT_LIST over a value computed from the result of the lower aggregate, which skips
-- the NULL, and over a struct of it, which collects the struct
select a1, ( select collect_list(m + 1) from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select collect_list(struct(m)) from
  (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with a lower function without a default result whose result over no rows, NULL, is computed
-- through a fresh buffer
select a1, ( select count(*) filter (where s is null)
  from (select approx_percentile(b2, 0.5) s from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

select a1, ( select count(*) filter (where s is null)
  from (select percentile(b2, 0.5) s from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with a function between the aggregates that is not computed over the row of the lower one,
-- which COUNT(*) does not need, and SUM does, which keeps the result of before
select a1, ( select count(*)
  from (select percentile(m, 0.5) p from (select max(b2) m from t2 where t1.a1 = t2.b1)) ) a
from t1 order by a1;

select a1, ( select sum(p)
  from (select percentile(m, 0.5) p from (select max(b2) m from t2 where t1.a1 = t2.b1)) ) a
from t1 order by a1;

-- with a correlation through a cast of an inner DOUBLE to a string: grouping merges -0.0 into
-- 0.0, so a top that reads the row below keeps the result of before (also the wrong 0 for x,
-- where 1 is correct), and COUNT(*) gets the fix
select o.s, ( select count(*) filter (where m is null)
  from (select max(b.v) m from values (cast('-0.0' as double), 1) as b(d, v)
    where cast(b.d as string) = o.s) ) c
from values ('-0.0'), ('x') as o(s) order by 1;

select o.s, ( select count(*)
  from (select max(b.v) m from values (cast('-0.0' as double), 1) as b(d, v)
    where cast(b.d as string) = o.s) ) c
from values ('-0.0'), ('x') as o(s) order by 1;

-- with an inner FLOAT compared with an outer DOUBLE, through the widening cast that keeps the
-- merged values equal
select o.d, ( select count(*) filter (where m is null)
  from (select max(b.v) m from values (cast('-0.0' as float), 1) as b(f, v) where b.f = o.d) ) c
from values (-0.0D), (2.0D) as o(d) order by 1;

-- with a top aggregate that fails over no rows, which gets the result over the row of the MAX
select a1, ( select 10 / count(*) from (select max(b2) m from t2 where t1.a1 = t2.b1) ) a
from t1 order by a1;

-- with outer values that compare equal but differ, -0.0 and 0.0, correlated through a string
select cast(o.d as string) d, ( select count(*) filter (where m is null)
  from (select max(b.v) m from values ('0.0', 1) as b(s, v) where b.s = cast(o.d as string)) ) c
from values (cast('-0.0' as double)), (cast('0.0' as double)) as o(d) order by 1;

select cast(o.d as string) d, ( select count(*) filter (where m is null)
  from (select max(b.v) m from values ('-0.0', 1) as b(s, v) where b.s = cast(o.d as string)) ) c
from values (cast('-0.0' as double)), (cast('0.0' as double)) as o(d) order by 1;

-- with the legacy behavior, which does not compute the result for an outer row without matching
-- rows before decorrelation
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
