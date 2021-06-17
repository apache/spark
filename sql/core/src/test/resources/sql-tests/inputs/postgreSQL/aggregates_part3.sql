--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- AGGREGATES [Part 3]
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/aggregates.sql#L352-L605

-- Test aggregate operator with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

-- [SPARK-28865] Table inheritance
-- try it on an inheritance tree
-- create table minmaxtest(f1 int);
-- create table minmaxtest1() inherits (minmaxtest);
-- create table minmaxtest2() inherits (minmaxtest);
-- create table minmaxtest3() inherits (minmaxtest);
-- create index minmaxtesti on minmaxtest(f1);
-- create index minmaxtest1i on minmaxtest1(f1);
-- create index minmaxtest2i on minmaxtest2(f1 desc);
-- create index minmaxtest3i on minmaxtest3(f1) where f1 is not null;

-- insert into minmaxtest values(11), (12);
-- insert into minmaxtest1 values(13), (14);
-- insert into minmaxtest2 values(15), (16);
-- insert into minmaxtest3 values(17), (18);

-- explain (costs off)
--   select min(f1), max(f1) from minmaxtest;
-- select min(f1), max(f1) from minmaxtest;

-- DISTINCT doesn't do anything useful here, but it shouldn't fail
-- explain (costs off)
--   select distinct min(f1), max(f1) from minmaxtest;
-- select distinct min(f1), max(f1) from minmaxtest;

-- drop table minmaxtest cascade;

-- check for correct detection of nested-aggregate errors
select max(min(unique1)) from tenk1;
-- select (select max(min(unique1)) from int8_tbl) from tenk1;

-- These tests only test the explain. Skip these tests.
--
-- Test removal of redundant GROUP BY columns
--

-- create temp table t1 (a int, b int, c int, d int, primary key (a, b));
-- create temp table t2 (x int, y int, z int, primary key (x, y));
-- create temp table t3 (a int, b int, c int, primary key(a, b) deferrable);

-- Non-primary-key columns can be removed from GROUP BY
-- explain (costs off) select * from t1 group by a,b,c,d;

-- No removal can happen if the complete PK is not present in GROUP BY
-- explain (costs off) select a,c from t1 group by a,c,d;

-- Test removal across multiple relations
-- explain (costs off) select *
-- from t1 inner join t2 on t1.a = t2.x and t1.b = t2.y
-- group by t1.a,t1.b,t1.c,t1.d,t2.x,t2.y,t2.z;

-- Test case where t1 can be optimized but not t2
-- explain (costs off) select t1.*,t2.x,t2.z
-- from t1 inner join t2 on t1.a = t2.x and t1.b = t2.y
-- group by t1.a,t1.b,t1.c,t1.d,t2.x,t2.z;

-- Cannot optimize when PK is deferrable
-- explain (costs off) select * from t3 group by a,b,c;

-- drop table t1;
-- drop table t2;
-- drop table t3;

-- [SPARK-27974] Add built-in Aggregate Function: array_agg
--
-- Test combinations of DISTINCT and/or ORDER BY
--

-- select array_agg(a order by b)
--   from (values (1,4),(2,3),(3,1),(4,2)) v(a,b);
-- select array_agg(a order by a)
--   from (values (1,4),(2,3),(3,1),(4,2)) v(a,b);
-- select array_agg(a order by a desc)
--   from (values (1,4),(2,3),(3,1),(4,2)) v(a,b);
-- select array_agg(b order by a desc)
--   from (values (1,4),(2,3),(3,1),(4,2)) v(a,b);

-- select array_agg(distinct a)
--   from (values (1),(2),(1),(3),(null),(2)) v(a);
-- select array_agg(distinct a order by a)
--   from (values (1),(2),(1),(3),(null),(2)) v(a);
-- select array_agg(distinct a order by a desc)
--   from (values (1),(2),(1),(3),(null),(2)) v(a);
-- select array_agg(distinct a order by a desc nulls last)
--   from (values (1),(2),(1),(3),(null),(2)) v(a);

-- Skip the test below because it requires 4 UDAFs: aggf_trans, aggfns_trans, aggfstr, and aggfns
-- multi-arg aggs, strict/nonstrict, distinct/order by

-- select aggfstr(a,b,c)
--   from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c);
-- select aggfns(a,b,c)
--   from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c);

-- select aggfstr(distinct a,b,c)
--   from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
--        generate_series(1,3) i;
-- select aggfns(distinct a,b,c)
--   from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
--        generate_series(1,3) i;

-- select aggfstr(distinct a,b,c order by b)
--   from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
--        generate_series(1,3) i;
-- select aggfns(distinct a,b,c order by b)
--   from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
--        generate_series(1,3) i;

-- test specific code paths

-- [SPARK-28768] Implement more text pattern operators
-- select aggfns(distinct a,a,c order by c using ~<~,a)
--   from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
--        generate_series(1,2) i;
-- select aggfns(distinct a,a,c order by c using ~<~)
--   from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
--        generate_series(1,2) i;
-- select aggfns(distinct a,a,c order by a)
--   from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
--        generate_series(1,2) i;
-- select aggfns(distinct a,b,c order by a,c using ~<~,b)
--   from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
--        generate_series(1,2) i;

-- check node I/O via view creation and usage, also deparsing logic

-- create view agg_view1 as
--   select aggfns(a,b,c)
--     from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c);

-- select * from agg_view1;
-- select pg_get_viewdef('agg_view1'::regclass);

-- create or replace view agg_view1 as
--   select aggfns(distinct a,b,c)
--     from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
--          generate_series(1,3) i;

-- select * from agg_view1;
-- select pg_get_viewdef('agg_view1'::regclass);

-- create or replace view agg_view1 as
--   select aggfns(distinct a,b,c order by b)
--     from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
--          generate_series(1,3) i;

-- select * from agg_view1;
-- select pg_get_viewdef('agg_view1'::regclass);

-- create or replace view agg_view1 as
--   select aggfns(a,b,c order by b+1)
--     from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c);

-- select * from agg_view1;
-- select pg_get_viewdef('agg_view1'::regclass);

-- create or replace view agg_view1 as
--   select aggfns(a,a,c order by b)
--     from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c);

-- select * from agg_view1;
-- select pg_get_viewdef('agg_view1'::regclass);

-- create or replace view agg_view1 as
--   select aggfns(a,b,c order by c using ~<~)
--     from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c);

-- select * from agg_view1;
-- select pg_get_viewdef('agg_view1'::regclass);

-- create or replace view agg_view1 as
--   select aggfns(distinct a,b,c order by a,c using ~<~,b)
--     from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
--          generate_series(1,2) i;

-- select * from agg_view1;
-- select pg_get_viewdef('agg_view1'::regclass);

-- drop view agg_view1;

-- incorrect DISTINCT usage errors

-- select aggfns(distinct a,b,c order by i)
--   from (values (1,1,'foo')) v(a,b,c), generate_series(1,2) i;
-- select aggfns(distinct a,b,c order by a,b+1)
--   from (values (1,1,'foo')) v(a,b,c), generate_series(1,2) i;
-- select aggfns(distinct a,b,c order by a,b,i,c)
--   from (values (1,1,'foo')) v(a,b,c), generate_series(1,2) i;
-- select aggfns(distinct a,a,c order by a,b)
--   from (values (1,1,'foo')) v(a,b,c), generate_series(1,2) i;

-- [SPARK-27978] Add built-in Aggregate Functions: string_agg
-- string_agg tests
-- select string_agg(a,',') from (values('aaaa'),('bbbb'),('cccc')) g(a);
-- select string_agg(a,',') from (values('aaaa'),(null),('bbbb'),('cccc')) g(a);
-- select string_agg(a,'AB') from (values(null),(null),('bbbb'),('cccc')) g(a);
-- select string_agg(a,',') from (values(null),(null)) g(a);

-- check some implicit casting cases, as per bug #5564
-- select string_agg(distinct f1, ',' order by f1) from varchar_tbl;  -- ok
-- select string_agg(distinct f1::text, ',' order by f1) from varchar_tbl;  -- not ok
-- select string_agg(distinct f1, ',' order by f1::text) from varchar_tbl;  -- not ok
-- select string_agg(distinct f1::text, ',' order by f1::text) from varchar_tbl;  -- ok

-- [SPARK-28121] decode can not accept 'hex' as charset
-- string_agg bytea tests
-- CREATE TABLE bytea_test_table(v BINARY) USING parquet;

-- select string_agg(v, '') from bytea_test_table;

-- insert into bytea_test_table values(decode('ff','hex'));

-- select string_agg(v, '') from bytea_test_table;

-- insert into bytea_test_table values(decode('aa','hex'));

-- select string_agg(v, '') from bytea_test_table;
-- select string_agg(v, NULL) from bytea_test_table;
-- select string_agg(v, decode('ee', 'hex')) from bytea_test_table;

-- drop table bytea_test_table;

-- FILTER tests

select min(unique1) filter (where unique1 > 100) from tenk1;

select sum(1/ten) filter (where ten > 0) from tenk1;

-- select ten, sum(distinct four) filter (where four::text ~ '123') from onek a
-- group by ten;

select ten, sum(distinct four) filter (where four > 10) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- [SPARK-28682] ANSI SQL: Collation Support
-- select max(foo COLLATE "C") filter (where (bar collate "POSIX") > '0')
-- from (values ('a', 'b')) AS v(foo,bar);

-- outer reference in FILTER (PostgreSQL extension)
select (select count(*)
        from (values (1)) t0(inner_c))
from (values (2),(3)) t1(outer_c); -- inner query is aggregation query
-- [SPARK-30219] Support Filter expression reference the outer query
-- select (select count(*) filter (where outer_c <> 0)
--         from (values (1)) t0(inner_c))
-- from (values (2),(3)) t1(outer_c); -- outer query is aggregation query
-- select (select count(inner_c) filter (where outer_c <> 0)
--         from (values (1)) t0(inner_c))
-- from (values (2),(3)) t1(outer_c); -- inner query is aggregation query
-- select
--   (select max((select i.unique2 from tenk1 i where i.unique1 = o.unique1))
--      filter (where o.unique1 < 10))
-- from tenk1 o;					-- outer query is aggregation query

-- [SPARK-30220] Support Filter expression uses IN/EXISTS predicate sub-queries
-- subquery in FILTER clause (PostgreSQL extension)
-- select sum(unique1) FILTER (WHERE
--  unique1 IN (SELECT unique1 FROM onek where unique1 < 100)) FROM tenk1;

-- exercise lots of aggregate parts with FILTER
-- select aggfns(distinct a,b,c order by a,c using ~<~,b) filter (where a > 1)
--     from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
--     generate_series(1,2) i;
