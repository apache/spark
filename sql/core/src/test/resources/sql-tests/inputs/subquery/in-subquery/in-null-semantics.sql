--ONLY_IF spark
create temp view v (c) as values (1), (null);
create temp view v_empty (e) as select 1 where false;

-- Note: tables and temp views hit different optimization/execution codepaths: expressions over temp views are evaled at query compilation time by ConvertToLocalRelation
create table t(c int) using json;
insert into t values (1), (null);
create table t2(d int) using json;
insert into t2 values (2);
create table t_empty(e int) using json;



set spark.sql.legacy.nullInEmptyListBehavior = false;

-- null IN (empty subquery)
-- Correct results: c in (emptylist) should always be false

select c, c in (select e from t_empty) from t;
select c, c in (select e from v_empty) from v;
select c, c not in (select e from t_empty) from t;
select c, c not in (select e from v_empty) from v;

-- constant null IN (empty subquery) - rewritten by NullPropagation rule

select null in (select e from t_empty);
select null in (select e from v_empty);
select null not in (select e from t_empty);
select null not in (select e from v_empty);

-- IN subquery which is not rewritten to join - here we use IN in the ON condition because that is a case that doesn't get rewritten to join in RewritePredicateSubquery, so we can observe the execution behavior of InSubquery directly
-- Correct results: column t2.d should be NULL because the ON condition is always false
select * from t left join t2 on (t.c in (select e from t_empty)) is null;
select * from t left join t2 on (t.c not in (select e from t_empty)) is null;

-- Should have the same results as above with optimize IN subqueries enabled
set spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled=true;

-- IN subquery which IS rewritten to join
select * from t left join t2 on (t.c in (select e from t_empty)) is null;
select * from t left join t2 on (t.c not in (select e from t_empty)) is null;

set spark.sql.legacy.nullInEmptyListBehavior = true;
-- Disable optimize IN subqueries to joins because it affects null semantics
set spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled=false;

-- constant null IN (empty subquery) - rewritten by NullPropagation rule

select null in (select e from t_empty);
select null in (select e from v_empty);
select null not in (select e from t_empty);
select null not in (select e from v_empty);

-- IN subquery which is not rewritten to join - here we use IN in the ON condition because that is a case that doesn't get rewritten to join in RewritePredicateSubquery, so we can observe the execution behavior of InSubquery directly
-- Correct results: column t2.d should be NULL because the ON condition is always false
select * from t left join t2 on (t.c in (select e from t_empty)) is null;
select * from t left join t2 on (t.c not in (select e from t_empty)) is null;

reset spark.sql.legacy.nullInEmptyListBehavior;
reset spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled;

drop table t;
drop table t2;
drop table t_empty;
