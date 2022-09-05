--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- AGGREGATES [Part 4]
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/aggregates.sql#L607-L997

-- Test aggregate operator with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

-- [SPARK-27980] Ordered-Set Aggregate Functions
-- ordered-set aggregates

-- select p, percentile_cont(p) within group (order by x::float8)
-- from generate_series(1,5) x,
--      (values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1)) v(p)
-- group by p order by p;

-- select p, percentile_cont(p order by p) within group (order by x)  -- error
-- from generate_series(1,5) x,
--      (values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1)) v(p)
-- group by p order by p;

-- select p, sum() within group (order by x::float8)  -- error
-- from generate_series(1,5) x,
--      (values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1)) v(p)
-- group by p order by p;

-- select p, percentile_cont(p,p)  -- error
-- from generate_series(1,5) x,
--      (values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1)) v(p)
-- group by p order by p;

select percentile_cont(0.5) within group (order by b) from aggtest;
select percentile_cont(0.5) within group (order by b), sum(b) from aggtest;
select percentile_cont(0.5) within group (order by thousand) from tenk1;
select percentile_disc(0.5) within group (order by thousand) from tenk1;
-- [SPARK-28661] Hypothetical-Set Aggregate Functions
-- select rank(3) within group (order by x)
-- from (values (1),(1),(2),(2),(3),(3),(4)) v(x);
-- select cume_dist(3) within group (order by x)
-- from (values (1),(1),(2),(2),(3),(3),(4)) v(x);
-- select percent_rank(3) within group (order by x)
-- from (values (1),(1),(2),(2),(3),(3),(4),(5)) v(x);
-- select dense_rank(3) within group (order by x)
-- from (values (1),(1),(2),(2),(3),(3),(4)) v(x);

-- [SPARK-27980] Ordered-Set Aggregate Functions
-- select percentile_disc(array[0,0.1,0.25,0.5,0.75,0.9,1]) within group (order by thousand)
-- from tenk1;
-- select percentile_cont(array[0,0.25,0.5,0.75,1]) within group (order by thousand)
-- from tenk1;
-- select percentile_disc(array[[null,1,0.5],[0.75,0.25,null]]) within group (order by thousand)
-- from tenk1;
-- select percentile_cont(array[0,1,0.25,0.75,0.5,1,0.3,0.32,0.35,0.38,0.4]) within group (order by x)
-- from generate_series(1,6) x;

-- [SPARK-27980] Ordered-Set Aggregate Functions
-- [SPARK-28382] Array Functions: unnest
-- select ten, mode() within group (order by string4) from tenk1 group by ten;

-- select percentile_disc(array[0.25,0.5,0.75]) within group (order by x)
-- from unnest('{fred,jim,fred,jack,jill,fred,jill,jim,jim,sheila,jim,sheila}'::text[]) u(x);

-- [SPARK-28669] System Information Functions
-- check collation propagates up in suitable cases:
-- select pg_collation_for(percentile_disc(1) within group (order by x collate "POSIX"))
--   from (values ('fred'),('jim')) v(x);

-- test_rank and test_percentile_disc function created by create_aggregate.sql
-- ordered-set aggs created with CREATE AGGREGATE
-- select test_rank(3) within group (order by x)
-- from (values (1),(1),(2),(2),(3),(3),(4)) v(x);
-- select test_percentile_disc(0.5) within group (order by thousand) from tenk1;

-- [SPARK-28661] Hypothetical-Set Aggregate Functions
-- ordered-set aggs can't use ungrouped vars in direct args:
-- select rank(x) within group (order by x) from generate_series(1,5) x;

-- [SPARK-27980] Ordered-Set Aggregate Functions
-- outer-level agg can't use a grouped arg of a lower level, either:
-- select array(select percentile_disc(a) within group (order by x)
--                from (values (0.3),(0.7)) v(a) group by a)
--   from generate_series(1,5) g(x);

-- [SPARK-28661] Hypothetical-Set Aggregate Functions
-- agg in the direct args is a grouping violation, too:
--select rank(sum(x)) within group (order by x) from generate_series(1,5) x;

-- [SPARK-28661] Hypothetical-Set Aggregate Functions
-- hypothetical-set type unification and argument-count failures:
-- select rank(3) within group (order by x) from (values ('fred'),('jim')) v(x);
-- select rank(3) within group (order by stringu1,stringu2) from tenk1;
-- select rank('fred') within group (order by x) from generate_series(1,5) x;
-- select rank('adam'::text collate "C") within group (order by x collate "POSIX")
--   from (values ('fred'),('jim')) v(x);
-- hypothetical-set type unification successes:
-- select rank('adam'::varchar) within group (order by x) from (values ('fred'),('jim')) v(x);
-- select rank('3') within group (order by x) from generate_series(1,5) x;

-- [SPARK-28661] Hypothetical-Set Aggregate Functions
-- divide by zero check
-- select percent_rank(0) within group (order by x) from generate_series(1,0) x;

-- [SPARK-27980] Ordered-Set Aggregate Functions
-- deparse and multiple features:
-- create view aggordview1 as
-- select ten,
--        percentile_disc(0.5) within group (order by thousand) as p50,
--        percentile_disc(0.5) within group (order by thousand) filter (where hundred=1) as px,
--        rank(5,'AZZZZ',50) within group (order by hundred, string4 desc, hundred)
--   from tenk1
--  group by ten order by ten;

-- select pg_get_viewdef('aggordview1');
-- select * from aggordview1 order by ten;
-- drop view aggordview1;

-- least_agg created by create_aggregate.sql
-- variadic aggregates
-- select least_agg(q1,q2) from int8_tbl;
-- select least_agg(variadic array[q1,q2]) from int8_tbl;


-- Skip these tests because we do not support create type
-- test aggregates with common transition functions share the same states
-- begin work;

-- create type avg_state as (total bigint, count bigint);

-- create or replace function avg_transfn(state avg_state, n int) returns avg_state as
-- $$
-- declare new_state avg_state;
-- begin
-- 	raise notice 'avg_transfn called with %', n;
-- 	if state is null then
-- 		if n is not null then
-- 			new_state.total := n;
-- 			new_state.count := 1;
-- 			return new_state;
-- 		end if;
-- 		return null;
-- 	elsif n is not null then
-- 		state.total := state.total + n;
-- 		state.count := state.count + 1;
-- 		return state;
-- 	end if;
--
-- 	return null;
-- end
-- $$ language plpgsql;

-- create function avg_finalfn(state avg_state) returns int4 as
-- $$
-- begin
-- 	if state is null then
-- 		return NULL;
-- 	else
-- 		return state.total / state.count;
-- 	end if;
-- end
-- $$ language plpgsql;

-- create function sum_finalfn(state avg_state) returns int4 as
-- $$
-- begin
-- 	if state is null then
-- 		return NULL;
-- 	else
-- 		return state.total;
-- 	end if;
-- end
-- $$ language plpgsql;

-- create aggregate my_avg(int4)
-- (
--    stype = avg_state,
--    sfunc = avg_transfn,
--    finalfunc = avg_finalfn
-- );
--
-- create aggregate my_sum(int4)
-- (
--    stype = avg_state,
--    sfunc = avg_transfn,
--    finalfunc = sum_finalfn
-- );

-- aggregate state should be shared as aggs are the same.
-- select my_avg(one),my_avg(one) from (values(1),(3)) t(one);

-- aggregate state should be shared as transfn is the same for both aggs.
-- select my_avg(one),my_sum(one) from (values(1),(3)) t(one);

-- same as previous one, but with DISTINCT, which requires sorting the input.
-- select my_avg(distinct one),my_sum(distinct one) from (values(1),(3),(1)) t(one);

-- shouldn't share states due to the distinctness not matching.
-- select my_avg(distinct one),my_sum(one) from (values(1),(3)) t(one);

-- shouldn't share states due to the filter clause not matching.
-- select my_avg(one) filter (where one > 1),my_sum(one) from (values(1),(3)) t(one);

-- this should not share the state due to different input columns.
-- select my_avg(one),my_sum(two) from (values(1,2),(3,4)) t(one,two);

-- [SPARK-27980] Ordered-Set Aggregate Functions
-- exercise cases where OSAs share state
-- select
--   percentile_cont(0.5) within group (order by a),
--   percentile_disc(0.5) within group (order by a)
-- from (values(1::float8),(3),(5),(7)) t(a);

-- select
--   percentile_cont(0.25) within group (order by a),
--   percentile_disc(0.5) within group (order by a)
-- from (values(1::float8),(3),(5),(7)) t(a);

-- [SPARK-28661] Hypothetical-Set Aggregate Functions
-- these can't share state currently
-- select
--   rank(4) within group (order by a),
--   dense_rank(4) within group (order by a)
-- from (values(1),(3),(5),(7)) t(a);

-- test that aggs with the same sfunc and initcond share the same agg state
-- create aggregate my_sum_init(int4)
-- (
--    stype = avg_state,
--    sfunc = avg_transfn,
--    finalfunc = sum_finalfn,
--    initcond = '(10,0)'
-- );

-- create aggregate my_avg_init(int4)
-- (
--    stype = avg_state,
--    sfunc = avg_transfn,
--    finalfunc = avg_finalfn,
--    initcond = '(10,0)'
-- );

-- create aggregate my_avg_init2(int4)
-- (
--    stype = avg_state,
--    sfunc = avg_transfn,
--    finalfunc = avg_finalfn,
--    initcond = '(4,0)'
-- );

-- state should be shared if INITCONDs are matching
-- select my_sum_init(one),my_avg_init(one) from (values(1),(3)) t(one);

-- Varying INITCONDs should cause the states not to be shared.
-- select my_sum_init(one),my_avg_init2(one) from (values(1),(3)) t(one);

-- rollback;

-- test aggregate state sharing to ensure it works if one aggregate has a
-- finalfn and the other one has none.
-- begin work;

-- create or replace function sum_transfn(state int4, n int4) returns int4 as
-- $$
-- declare new_state int4;
-- begin
-- 	raise notice 'sum_transfn called with %', n;
-- 	if state is null then
-- 		if n is not null then
-- 			new_state := n;
-- 			return new_state;
-- 		end if;
-- 		return null;
-- 	elsif n is not null then
-- 		state := state + n;
-- 		return state;
-- 	end if;
--
-- 	return null;
-- end
-- $$ language plpgsql;

-- create function halfsum_finalfn(state int4) returns int4 as
-- $$
-- begin
-- 	if state is null then
-- 		return NULL;
-- 	else
-- 		return state / 2;
-- 	end if;
-- end
-- $$ language plpgsql;

-- create aggregate my_sum(int4)
-- (
--    stype = int4,
--    sfunc = sum_transfn
-- );

-- create aggregate my_half_sum(int4)
-- (
--    stype = int4,
--    sfunc = sum_transfn,
--    finalfunc = halfsum_finalfn
-- );

-- Agg state should be shared even though my_sum has no finalfn
-- select my_sum(one),my_half_sum(one) from (values(1),(2),(3),(4)) t(one);

-- rollback;


-- test that the aggregate transition logic correctly handles
-- transition / combine functions returning NULL

-- First test the case of a normal transition function returning NULL
-- BEGIN;
-- CREATE FUNCTION balkifnull(int8, int4)
-- RETURNS int8
-- STRICT
-- LANGUAGE plpgsql AS $$
-- BEGIN
--     IF $1 IS NULL THEN
--        RAISE 'erroneously called with NULL argument';
--     END IF;
--     RETURN NULL;
-- END$$;

-- CREATE AGGREGATE balk(int4)
-- (
--     SFUNC = balkifnull(int8, int4),
--     STYPE = int8,
--     PARALLEL = SAFE,
--     INITCOND = '0'
-- );

-- SELECT balk(hundred) FROM tenk1;

-- ROLLBACK;

-- Secondly test the case of a parallel aggregate combiner function
-- returning NULL. For that use normal transition function, but a
-- combiner function returning NULL.
-- BEGIN ISOLATION LEVEL REPEATABLE READ;
-- CREATE FUNCTION balkifnull(int8, int8)
-- RETURNS int8
-- PARALLEL SAFE
-- STRICT
-- LANGUAGE plpgsql AS $$
-- BEGIN
--     IF $1 IS NULL THEN
--        RAISE 'erroneously called with NULL argument';
--     END IF;
--     RETURN NULL;
-- END$$;

-- CREATE AGGREGATE balk(int4)
-- (
--     SFUNC = int4_sum(int8, int4),
--     STYPE = int8,
--     COMBINEFUNC = balkifnull(int8, int8),
--     PARALLEL = SAFE,
--     INITCOND = '0'
-- );

-- force use of parallelism
-- ALTER TABLE tenk1 set (parallel_workers = 4);
-- SET LOCAL parallel_setup_cost=0;
-- SET LOCAL max_parallel_workers_per_gather=4;

-- EXPLAIN (COSTS OFF) SELECT balk(hundred) FROM tenk1;
-- SELECT balk(hundred) FROM tenk1;

-- ROLLBACK;

-- test coverage for aggregate combine/serial/deserial functions
-- BEGIN ISOLATION LEVEL REPEATABLE READ;

-- SET parallel_setup_cost = 0;
-- SET parallel_tuple_cost = 0;
-- SET min_parallel_table_scan_size = 0;
-- SET max_parallel_workers_per_gather = 4;
-- SET enable_indexonlyscan = off;

-- [SPARK-28663] Aggregate Functions for Statistics
-- variance(int4) covers numeric_poly_combine
-- sum(int8) covers int8_avg_combine
-- regr_count(float8, float8) covers int8inc_float8_float8 and aggregates with > 1 arg
-- EXPLAIN (COSTS OFF, VERBOSE)
--   SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8) FROM tenk1;

-- SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8) FROM tenk1;

-- ROLLBACK;

-- [SPARK-28661] Hypothetical-Set Aggregate Functions
-- test coverage for dense_rank
-- SELECT dense_rank(x) WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(1),(2),(2),(3),(3)) v(x) GROUP BY (x) ORDER BY 1;


-- [SPARK-28664] ORDER BY in aggregate function
-- Ensure that the STRICT checks for aggregates does not take NULLness
-- of ORDER BY columns into account. See bug report around
-- 2a505161-2727-2473-7c46-591ed108ac52@email.cz
-- SELECT min(x ORDER BY y) FROM (VALUES(1, NULL)) AS d(x,y);
-- SELECT min(x ORDER BY y) FROM (VALUES(1, 2)) AS d(x,y);

-- [SPARK-28382] Array Functions: unnest
-- check collation-sensitive matching between grouping expressions
-- select v||'a', case v||'a' when 'aa' then 1 else 0 end, count(*)
--   from unnest(array['a','b']) u(v)
--  group by v||'a' order by 1;
-- select v||'a', case when v||'a' = 'aa' then 1 else 0 end, count(*)
--   from unnest(array['a','b']) u(v)
--  group by v||'a' order by 1;

-- Make sure that generation of HashAggregate for uniqification purposes
-- does not lead to array overflow due to unexpected duplicate hash keys
-- see CAFeeJoKKu0u+A_A9R9316djW-YW3-+Gtgvy3ju655qRHR3jtdA@mail.gmail.com
-- explain (costs off)
--   select 1 from tenk1
--    where (hundred, thousand) in (select twothousand, twothousand from onek);
