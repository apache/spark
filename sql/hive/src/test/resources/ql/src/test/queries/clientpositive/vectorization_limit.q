SET hive.vectorized.execution.enabled=true;
explain SELECT cbigint, cdouble FROM alltypesorc WHERE cbigint < cdouble and cint > 0 limit 7;
SELECT cbigint, cdouble FROM alltypesorc WHERE cbigint < cdouble and cint > 0 limit 7;

set hive.optimize.reducededuplication.min.reducer=1;
set hive.limit.pushdown.memory.usage=0.3f;

-- HIVE-3562 Some limit can be pushed down to map stage - c/p parts from limit_pushdown

explain
select ctinyint,cdouble,csmallint from alltypesorc where ctinyint is not null order by ctinyint,cdouble limit 20;
select ctinyint,cdouble,csmallint from alltypesorc where ctinyint is not null order by ctinyint,cdouble limit 20;

-- deduped RS
explain
select ctinyint,avg(cdouble + 1) from alltypesorc group by ctinyint order by ctinyint limit 20;
select ctinyint,avg(cdouble + 1) from alltypesorc group by ctinyint order by ctinyint limit 20;

-- distincts
explain
select distinct(ctinyint) from alltypesorc limit 20;
select distinct(ctinyint) from alltypesorc limit 20;

explain
select ctinyint, count(distinct(cdouble)) from alltypesorc group by ctinyint limit 20;
select ctinyint, count(distinct(cdouble)) from alltypesorc group by ctinyint limit 20;

-- limit zero
explain
select ctinyint,cdouble from alltypesorc order by ctinyint limit 0;
select ctinyint,cdouble from alltypesorc order by ctinyint limit 0;

-- 2MR (applied to last RS)
explain
select cdouble, sum(ctinyint) as sum from alltypesorc where ctinyint is not null group by cdouble order by sum, cdouble limit 20;
select cdouble, sum(ctinyint) as sum from alltypesorc where ctinyint is not null group by cdouble order by sum, cdouble limit 20;

