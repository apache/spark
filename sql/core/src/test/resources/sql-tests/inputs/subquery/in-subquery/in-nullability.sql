-- SPARK-43413: Tests for IN subquery nullability
--ONLY_IF spark

create temp view t0 as select 1 as a_nonnullable;
create temp view t1 as select cast(null as int) as b_nullable;
create temp view t2 as select 2 as c;

select * from t0 where a_nonnullable in (select b_nullable from t1);
select * from t0 where (a_nonnullable in (select b_nullable from t1)) <=> true;
select * from t0 where a_nonnullable not in (select b_nullable from t1);
select * from t0 where (a_nonnullable not in (select b_nullable from t1)) <=> true;

-- IN subqueries in ON conditions are not rewritten to joins in RewritePredicateSubquery
select * from t0 left join t2 on (a_nonnullable IN (select b_nullable from t1)) is null;
select * from t0 left join t2 on (a_nonnullable IN (select b_nullable from t1)) <=> true;
