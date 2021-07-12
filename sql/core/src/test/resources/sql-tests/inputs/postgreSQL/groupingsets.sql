-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
-- GROUPING SETS
-- https://github.com/postgres/postgres/blob/REL_12_STABLE/src/test/regress/sql/groupingsets.sql

-- test data sources

create temp view gstest1(a,b,v)
  as values (1,1,10),(1,1,11),(1,2,12),(1,2,13),(1,3,14),
            (2,3,15),
            (3,3,16),(3,4,17),
            (4,1,18),(4,1,19);

-- Since Spark doesn't support CREATE TEMPORARY TABLE, we used CREATE TABLE instead
-- create temp table gstest2 (a integer, b integer, c integer, d integer,
--                            e integer, f integer, g integer, h integer);
create table gstest2 (a integer, b integer, c integer, d integer,
                      e integer, f integer, g integer, h integer) using parquet;
-- [SPARK-29386] Copy data between a file and a table
-- copy gstest2 from stdin;
-- 1	1	1	1	1	1	1	1
-- 1	1	1	1	1	1	1	2
-- 1	1	1	1	1	1	2	2
-- 1	1	1	1	1	2	2	2
-- 1	1	1	1	2	2	2	2
-- 1	1	1	2	2	2	2	2
-- 1	1	2	2	2	2	2	2
-- 1	2	2	2	2	2	2	2
-- 2	2	2	2	2	2	2	2
-- \.
insert into gstest2 values
  (1, 1, 1, 1, 1, 1, 1, 1),
  (1, 1, 1, 1, 1, 1, 1, 2),
  (1, 1, 1, 1, 1, 1, 2, 2),
  (1, 1, 1, 1, 1, 2, 2, 2),
  (1, 1, 1, 1, 2, 2, 2, 2),
  (1, 1, 1, 2, 2, 2, 2, 2),
  (1, 1, 2, 2, 2, 2, 2, 2),
  (1, 2, 2, 2, 2, 2, 2, 2),
  (2, 2, 2, 2, 2, 2, 2, 2);

-- Since Spark doesn't support CREATE TEMPORARY TABLE, we used CREATE TABLE instead
-- create temp table gstest3 (a integer, b integer, c integer, d integer);
create table gstest3 (a integer, b integer, c integer, d integer) using parquet;
-- [SPARK-29386] Copy data between a file and a table
-- copy gstest3 from stdin;
-- 1	1	1	1
-- 2	2	2	2
-- \.
insert into gstest3 values
  (1, 1, 1, 1),
  (2, 2, 2, 2);
-- [SPARK-19842] Informational Referential Integrity Constraints Support in Spark
-- alter table gstest3 add primary key (a);

-- Since Spark doesn't support CREATE TEMPORARY TABLE, we used CREATE TABLE instead
-- create temp table gstest4(id integer, v integer,
--                           unhashable_col bit(4), unsortable_col xid);
-- [SPARK-29697] Support bit string types/literals
create table gstest4(id integer, v integer,
                     unhashable_col /* bit(4) */ byte, unsortable_col /* xid */ integer) using parquet;
insert into gstest4
-- values (1,1,b'0000','1'), (2,2,b'0001','1'),
--        (3,4,b'0010','2'), (4,8,b'0011','2'),
--        (5,16,b'0000','2'), (6,32,b'0001','2'),
--        (7,64,b'0010','1'), (8,128,b'0011','1');
values (1,1,tinyint('0'),1), (2,2,tinyint('1'),1),
       (3,4,tinyint('2'),2), (4,8,tinyint('3'),2),
       (5,16,tinyint('0'),2), (6,32,tinyint('1'),2),
       (7,64,tinyint('2'),1), (8,128,tinyint('3'),1);

-- Since Spark doesn't support CREATE TEMPORARY TABLE, we used CREATE TABLE instead
-- create temp table gstest_empty (a integer, b integer, v integer);
create table gstest_empty (a integer, b integer, v integer) using parquet;

-- Spark doesn't handle UDFs in SQL
-- create function gstest_data(v integer, out a integer, out b integer)
--   returns setof record
--   as $f$
--     begin
--       return query select v, i from generate_series(1,3) i;
--     end;
--   $f$ language plpgsql;

-- basic functionality

-- Ignore a PostgreSQL-specific option
-- set enable_hashagg = false;  -- test hashing explicitly later

-- simple rollup with multiple plain aggregates, with and without ordering
-- (and with ordering differing from grouping)

-- [SPARK-29698] Support grouping function with multiple arguments
-- select a, b, grouping(a,b), sum(v), count(*), max(v)
select a, b, grouping(a), grouping(b), sum(v), count(*), max(v)
  from gstest1 group by rollup (a,b);
-- select a, b, grouping(a,b), sum(v), count(*), max(v)
select a, b, grouping(a), grouping(b), sum(v), count(*), max(v)
  from gstest1 group by rollup (a,b) order by a,b;
-- select a, b, grouping(a,b), sum(v), count(*), max(v)
select a, b, grouping(a), grouping(b), sum(v), count(*), max(v)
  from gstest1 group by rollup (a,b) order by b desc, a;
-- select a, b, grouping(a,b), sum(v), count(*), max(v)
select a, b, grouping(a), grouping(b), sum(v), count(*), max(v)
  from gstest1 group by rollup (a,b) order by coalesce(a,0)+coalesce(b,0), a;

-- [SPARK-28664] ORDER BY in aggregate function
-- various types of ordered aggs
-- select a, b, grouping(a,b),
--        array_agg(v order by v),
--        string_agg(string(v:text, ':' order by v desc),
--        percentile_disc(0.5) within group (order by v),
--        rank(1,2,12) within group (order by a,b,v)
--   from gstest1 group by rollup (a,b) order by a,b;

-- [SPARK-28664] ORDER BY in aggregate function
-- test usage of grouped columns in direct args of aggs
-- select grouping(a), a, array_agg(b),
--        rank(a) within group (order by b nulls first),
--        rank(a) within group (order by b nulls last)
--   from (values (1,1),(1,4),(1,5),(3,1),(3,2)) v(a,b)
--  group by rollup (a) order by a;

-- nesting with window functions
-- [SPARK-29699] Different answers in nested aggregates with window functions
select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum
  from gstest2 group by rollup (a,b) order by rsum, a, b;

-- [SPARK-29700] Support nested grouping sets
-- nesting with grouping sets
-- select sum(c) from gstest2
--   group by grouping sets((), grouping sets((), grouping sets(())))
--   order by 1 desc;
-- select sum(c) from gstest2
--   group by grouping sets((), grouping sets((), grouping sets(((a, b)))))
--   order by 1 desc;
-- select sum(c) from gstest2
--   group by grouping sets(grouping sets(rollup(c), grouping sets(cube(c))))
--   order by 1 desc;
-- select sum(c) from gstest2
--   group by grouping sets(a, grouping sets(a, cube(b)))
--   order by 1 desc;
-- select sum(c) from gstest2
--   group by grouping sets(grouping sets((a, (b))))
--   order by 1 desc;
-- select sum(c) from gstest2
--   group by grouping sets(grouping sets((a, b)))
--   order by 1 desc;
-- select sum(c) from gstest2
--   group by grouping sets(grouping sets(a, grouping sets(a), a))
--   order by 1 desc;
-- select sum(c) from gstest2
--   group by grouping sets(grouping sets(a, grouping sets(a, grouping sets(a), ((a)), a, grouping sets(a), (a)), a))
--   order by 1 desc;
-- select sum(c) from gstest2
--   group by grouping sets((a,(a,b)), grouping sets((a,(a,b)),a))
--   order by 1 desc;

-- empty input: first is 0 rows, second 1, third 3 etc.
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),a);
-- [SPARK-29701] Different answers when empty input given in GROUPING SETS
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),());
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),(),(),());
select sum(v), count(*) from gstest_empty group by grouping sets ((),(),());

-- empty input with joins tests some important code paths
-- [SPARK-29701] Different answers when empty input given in GROUPING SETS
select t1.a, t2.b, sum(t1.v), count(*) from gstest_empty t1, gstest_empty t2
 group by grouping sets ((t1.a,t2.b),());

-- simple joins, var resolution, GROUPING on join vars
-- [SPARK-29698] Support grouping function with multiple arguments
-- select t1.a, t2.b, grouping(t1.a, t2.b), sum(t1.v), max(t2.a)
select t1.a, t2.b, grouping(t1.a), grouping(t2.b), sum(t1.v), max(t2.a)
  from gstest1 t1, gstest2 t2
 group by grouping sets ((t1.a, t2.b), ());

-- [SPARK-29698] Support grouping function with multiple arguments
-- select t1.a, t2.b, grouping(t1.a, t2.b), sum(t1.v), max(t2.a)
select t1.a, t2.b, grouping(t1.a), grouping(t2.b), sum(t1.v), max(t2.a)
  from gstest1 t1 join gstest2 t2 on (t1.a=t2.a)
 group by grouping sets ((t1.a, t2.b), ());

-- [SPARK-29698] Support grouping function with multiple arguments
-- select a, b, grouping(a, b), sum(t1.v), max(t2.c)
select a, b, grouping(a), grouping(b), sum(t1.v), max(t2.c)
  from gstest1 t1 join gstest2 t2 using (a,b)
 group by grouping sets ((a, b), ());

-- check that functionally dependent cols are not nulled
-- [SPARK-29698] Support grouping function with multiple arguments
-- [SPARK-19842] Informational Referential Integrity Constraints Support in Spark
-- [SPARK-29702] Resolve group-by columns with functional dependencies
-- select a, d, grouping(a,b,c)
--   from gstest3
--  group by grouping sets ((a,b), (a,c));

-- check that distinct grouping columns are kept separate
-- even if they are equal()
-- explain (costs off)
-- select g as alias1, g as alias2
--   from generate_series(1,3) g
--  group by alias1, rollup(alias2);

-- [SPARK-27767] Built-in function: generate_series
-- [SPARK-29704] Support the combinations of grouping operations
-- select g as alias1, g as alias2
--   from generate_series(1,3) g
--  group by alias1, rollup(alias2);

-- check that pulled-up subquery outputs still go to null when appropriate
select four, x
  from (select four, ten, 'foo' as x from tenk1) as t
  group by grouping sets (four, x)
  having x = 'foo';

select four, x || 'x'
  from (select four, ten, 'foo' as x from tenk1) as t
  group by grouping sets (four, x)
  order by four;

select (x+y)*1, sum(z)
 from (select 1 as x, 2 as y, 3 as z) s
 group by grouping sets (x+y, x);

CREATE TEMP VIEW int8_tbl AS SELECT * FROM VALUES
  (123L, 456L),
  (123L, 4567890123456789L),
  (4567890123456789L, 123L),
  (4567890123456789L, 4567890123456789L),
  (4567890123456789L, -4567890123456789L) as int8_tbl(q1, q2);

select x, not x as not_x, q2 from
  (select *, q1 = 1 as x from int8_tbl i1) as t
  group by grouping sets(x, q2)
  order by x, q2;

DROP VIEW int8_tbl;

-- simple rescan tests

-- Spark doesn't handle UDFs in SQL
-- select a, b, sum(v.x)
--   from (values (1),(2)) v(x), gstest_data(v.x)
--  group by rollup (a,b);

-- Spark doesn't handle UDFs in SQL
-- select *
--   from (values (1),(2)) v(x),
--        lateral (select a, b, sum(v.x) from gstest_data(v.x) group by rollup (a,b)) s;

-- min max optimization should still work with GROUP BY ()
-- explain (costs off)
--   select min(unique1) from tenk1 GROUP BY ();

-- Views with GROUPING SET queries
-- [SPARK-29698] Support grouping function with multiple arguments
-- [SPARK-29705] Support more expressive forms in GroupingSets/Cube/Rollup
-- CREATE VIEW gstest_view AS select a, b, grouping(a,b), sum(c), count(*), max(c)
--   from gstest2 group by rollup ((a,b,c),(c,d));

-- select pg_get_viewdef('gstest_view'::regclass, true);

-- Nested queries with 3 or more levels of nesting
-- [SPARK-29698] Support grouping function with multiple arguments
-- [SPARK-29703] grouping() can only be used with GroupingSets/Cube/Rollup
-- select(select (select grouping(a,b) from (values (1)) v2(c)) from (values (1,2)) v1(a,b) group by (a,b)) from (values(6,7)) v3(e,f) GROUP BY ROLLUP(e,f);
-- select(select (select grouping(e,f) from (values (1)) v2(c)) from (values (1,2)) v1(a,b) group by (a,b)) from (values(6,7)) v3(e,f) GROUP BY ROLLUP(e,f);
-- select(select (select grouping(c) from (values (1)) v2(c) GROUP BY c) from (values (1,2)) v1(a,b) group by (a,b)) from (values(6,7)) v3(e,f) GROUP BY ROLLUP(e,f);

-- Combinations of operations
-- [SPARK-29704] Support the combinations of grouping operations
-- select a, b, c, d from gstest2 group by rollup(a,b),grouping sets(c,d);
-- select a, b from (values (1,2),(2,3)) v(a,b) group by a,b, grouping sets(a);

-- Spark doesn't handle UDFs in SQL
-- Tests for chained aggregates
-- select a, b, grouping(a,b), sum(v), count(*), max(v)
--   from gstest1 group by grouping sets ((a,b),(a+1,b+1),(a+2,b+2)) order by 3,6;
-- select(select (select grouping(a,b) from (values (1)) v2(c)) from (values (1,2)) v1(a,b) group by (a,b)) from (values(6,7)) v3(e,f) GROUP BY ROLLUP((e+1),(f+1));
-- select(select (select grouping(a,b) from (values (1)) v2(c)) from (values (1,2)) v1(a,b) group by (a,b)) from (values(6,7)) v3(e,f) GROUP BY CUBE((e+1),(f+1)) ORDER BY (e+1),(f+1);
-- select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum
--   from gstest2 group by cube (a,b) order by rsum, a, b;
-- select a, b, sum(c) from (values (1,1,10),(1,1,11),(1,2,12),(1,2,13),(1,3,14),(2,3,15),(3,3,16),(3,4,17),(4,1,18),(4,1,19)) v(a,b,c) group by rollup (a,b);
-- select a, b, sum(v.x)
--   from (values (1),(2)) v(x), gstest_data(v.x)
--  group by cube (a,b) order by a,b;

-- Test reordering of grouping sets
-- explain (costs off)
-- select * from gstest1 group by grouping sets((a,b,v),(v)) order by v,b,a;

-- [SPARK-29698] Support grouping function with multiple arguments
-- [SPARK-29703] grouping() can only be used with GroupingSets/Cube/Rollup
-- Agg level check. This query should error out.
-- select (select grouping(a), grouping(b) from gstest2) from gstest2 group by a,b;

--Nested queries
-- [SPARK-29700] Support nested grouping sets
-- select a, b, sum(c), count(*) from gstest2 group by grouping sets (rollup(a,b),a);

-- HAVING queries
select ten, sum(distinct four) from onek a
group by grouping sets((ten,four),(ten))
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- Tests around pushdown of HAVING clauses, partially testing against previous bugs
select a,count(*) from gstest2 group by rollup(a) order by a;
select a,count(*) from gstest2 group by rollup(a) having a is distinct from 1 order by a;
-- explain (costs off)
--   select a,count(*) from gstest2 group by rollup(a) having a is distinct from 1 order by a;

-- [SPARK-29706] Support an empty grouping expression
-- select v.c, (select count(*) from gstest2 group by () having v.c)
--   from (values (false),(true)) v(c) order by v.c;
-- explain (costs off)
--   select v.c, (select count(*) from gstest2 group by () having v.c)
--     from (values (false),(true)) v(c) order by v.c;

-- HAVING with GROUPING queries
select ten, grouping(ten) from onek
group by grouping sets(ten) having grouping(ten) >= 0
order by 2,1;
select ten, grouping(ten) from onek
group by grouping sets(ten, four) having grouping(ten) > 0
order by 2,1;
select ten, grouping(ten) from onek
group by rollup(ten) having grouping(ten) > 0
order by 2,1;
select ten, grouping(ten) from onek
group by cube(ten) having grouping(ten) > 0
order by 2,1;
-- [SPARK-29703] grouping() can only be used with GroupingSets/Cube/Rollup
-- select ten, grouping(ten) from onek
-- group by (ten) having grouping(ten) >= 0
-- order by 2,1;

-- FILTER queries
select ten, sum(distinct four) filter (where string(four) like '123') from onek a
group by rollup(ten);

-- More rescan tests
-- [SPARK-35554] Support outer references in Aggregate
-- select * from (values (1),(2)) v(a) left join lateral (select v.a, four, ten, count(*) from onek group by cube(four,ten)) s on true order by v.a,four,ten;
-- [SPARK-27878] Support ARRAY(sub-SELECT) expressions
-- select array(select row(v.a,s1.*) from (select two,four, count(*) from onek group by cube(two,four) order by two,four) s1) from (values (1),(2)) v(a);

-- [SPARK-29704] Support the combinations of grouping operations
-- Grouping on text columns
-- select sum(ten) from onek group by two, rollup(string(four)) order by 1;
-- select sum(ten) from onek group by rollup(string(four)), two order by 1;

-- hashing support

-- Ignore a PostgreSQL-specific option
-- set enable_hashagg = true;

-- failure cases

-- Since this test is implementation specific for plans, it passes in Spark
select count(*) from gstest4 group by rollup(unhashable_col,unsortable_col);
-- [SPARK-27878] Support ARRAY(sub-SELECT) expressions
-- select array_agg(v order by v) from gstest4 group by grouping sets ((id,unsortable_col),(id));

-- simple cases

-- [SPARK-29698] Support grouping function with multiple arguments
-- select a, b, grouping(a,b), sum(v), count(*), max(v)
select a, b, grouping(a), grouping(b), sum(v), count(*), max(v)
  from gstest1 group by grouping sets ((a),(b)) order by 3,4,1,2 /* 3,1,2 */;
-- explain (costs off) select a, b, grouping(a,b), sum(v), count(*), max(v)
--   from gstest1 group by grouping sets ((a),(b)) order by 3,1,2;

-- [SPARK-29698] Support grouping function with multiple arguments
-- select a, b, grouping(a,b), sum(v), count(*), max(v)
select a, b, grouping(a), grouping(b), sum(v), count(*), max(v)
  from gstest1 group by cube(a,b) order by 3,4,1,2 /* 3,1,2 */;
-- explain (costs off) select a, b, grouping(a,b), sum(v), count(*), max(v)
--   from gstest1 group by cube(a,b) order by 3,1,2;

-- shouldn't try and hash
-- explain (costs off)
--   select a, b, grouping(a,b), array_agg(v order by v)
--     from gstest1 group by cube(a,b);

-- unsortable cases
select unsortable_col, count(*)
  from gstest4 group by grouping sets ((unsortable_col),(unsortable_col))
  order by string(unsortable_col);

-- mixed hashable/sortable cases
-- [SPARK-29698] Support grouping function with multiple arguments
select unhashable_col, unsortable_col,
       -- grouping(unhashable_col, unsortable_col),
       grouping(unhashable_col), grouping(unsortable_col),
       count(*), sum(v)
  from gstest4 group by grouping sets ((unhashable_col),(unsortable_col))
 order by 3, 4, 6 /* 3, 5 */;
-- explain (costs off)
--   select unhashable_col, unsortable_col,
--          grouping(unhashable_col, unsortable_col),
--          count(*), sum(v)
--     from gstest4 group by grouping sets ((unhashable_col),(unsortable_col))
--    order by 3,5;

-- [SPARK-29698] Support grouping function with multiple arguments
select unhashable_col, unsortable_col,
       -- grouping(unhashable_col, unsortable_col),
       grouping(unhashable_col), grouping(unsortable_col),
       count(*), sum(v)
  from gstest4 group by grouping sets ((v,unhashable_col),(v,unsortable_col))
 order by 3, 4, 6 /* 3,5 */;
-- explain (costs off)
--   select unhashable_col, unsortable_col,
--          grouping(unhashable_col, unsortable_col),
--          count(*), sum(v)
--     from gstest4 group by grouping sets ((v,unhashable_col),(v,unsortable_col))
--    order by 3,5;

-- empty input: first is 0 rows, second 1, third 3 etc.
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),a);
-- explain (costs off)
--   select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),a);
-- [SPARK-29701] Different answers when empty input given in GROUPING SETS
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),());
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),(),(),());
-- explain (costs off)
--   select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),(),(),());
-- [SPARK-29701] Different answers when empty input given in GROUPING SETS
select sum(v), count(*) from gstest_empty group by grouping sets ((),(),());
-- explain (costs off)
--   select sum(v), count(*) from gstest_empty group by grouping sets ((),(),());

-- [SPARK-29698] Support grouping function with multiple arguments
-- [SPARK-19842] Informational Referential Integrity Constraints Support in Spark
-- [SPARK-29702] Resolve group-by columns with functional dependencies
-- check that functionally dependent cols are not nulled
-- select a, d, grouping(a,b,c)
--   from gstest3
--  group by grouping sets ((a,b), (a,c));
-- explain (costs off)
--   select a, d, grouping(a,b,c)
--     from gstest3
--    group by grouping sets ((a,b), (a,c));

-- simple rescan tests

-- select a, b, sum(v.x)
--   from (values (1),(2)) v(x), gstest_data(v.x)
--  group by grouping sets (a,b)
--  order by 1, 2, 3;
-- explain (costs off)
--   select a, b, sum(v.x)
--     from (values (1),(2)) v(x), gstest_data(v.x)
--    group by grouping sets (a,b)
--    order by 3, 1, 2;
-- select *
--   from (values (1),(2)) v(x),
--        lateral (select a, b, sum(v.x) from gstest_data(v.x) group by grouping sets (a,b)) s;
-- explain (costs off)
--   select *
--     from (values (1),(2)) v(x),
--          lateral (select a, b, sum(v.x) from gstest_data(v.x) group by grouping sets (a,b)) s;

-- Tests for chained aggregates
-- [SPARK-29698] Support grouping function with multiple arguments
-- select a, b, grouping(a,b), sum(v), count(*), max(v)
select a, b, grouping(a), grouping(b), sum(v), count(*), max(v)
  from gstest1 group by grouping sets ((a,b),(a+1,b+1),(a+2,b+2)) order by 3,4,7 /* 3,6 */;
-- explain (costs off)
--   select a, b, grouping(a,b), sum(v), count(*), max(v)
--     from gstest1 group by grouping sets ((a,b),(a+1,b+1),(a+2,b+2)) order by 3,6;
-- [SPARK-29699] Different answers in nested aggregates with window functions
select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum
  from gstest2 group by cube (a,b) order by rsum, a, b;
-- explain (costs off)
--   select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum
--     from gstest2 group by cube (a,b) order by rsum, a, b;
-- select a, b, sum(v.x)
--   from (values (1),(2)) v(x), gstest_data(v.x)
--  group by cube (a,b) order by a,b;
-- explain (costs off)
--   select a, b, sum(v.x)
--     from (values (1),(2)) v(x), gstest_data(v.x)
--    group by cube (a,b) order by a,b;

-- Verify that we correctly handle the child node returning a
-- non-minimal slot, which happens if the input is pre-sorted,
-- e.g. due to an index scan.
-- BEGIN;
-- Ignore a PostgreSQL-specific option
-- SET LOCAL enable_hashagg = false;
-- EXPLAIN (COSTS OFF) SELECT a, b, count(*), max(a), max(b) FROM gstest3 GROUP BY GROUPING SETS(a, b,()) ORDER BY a, b;
SELECT a, b, count(*), max(a), max(b) FROM gstest3 GROUP BY GROUPING SETS(a, b,()) ORDER BY a, b;
-- Ignore a PostgreSQL-specific option
-- SET LOCAL enable_seqscan = false;
-- EXPLAIN (COSTS OFF) SELECT a, b, count(*), max(a), max(b) FROM gstest3 GROUP BY GROUPING SETS(a, b,()) ORDER BY a, b;
-- SELECT a, b, count(*), max(a), max(b) FROM gstest3 GROUP BY GROUPING SETS(a, b,()) ORDER BY a, b;
-- COMMIT;

-- More rescan tests
-- [SPARK-35554] Support outer references in Aggregate
-- select * from (values (1),(2)) v(a) left join lateral (select v.a, four, ten, count(*) from onek group by cube(four,ten)) s on true order by v.a,four,ten;
-- [SPARK-27878] Support ARRAY(sub-SELECT) expressions
-- select array(select row(v.a,s1.*) from (select two,four, count(*) from onek group by cube(two,four) order by two,four) s1) from (values (1),(2)) v(a);

-- Rescan logic changes when there are no empty grouping sets, so test
-- that too:
-- [SPARK-35554] Support outer references in Aggregate
-- select * from (values (1),(2)) v(a) left join lateral (select v.a, four, ten, count(*) from onek group by grouping sets(four,ten)) s on true order by v.a,four,ten;
-- [SPARK-27878] Support ARRAY(sub-SELECT) expressions
-- select array(select row(v.a,s1.*) from (select two,four, count(*) from onek group by grouping sets(two,four) order by two,four) s1) from (values (1),(2)) v(a);

-- test the knapsack

-- Ignore a PostgreSQL-specific option
-- set enable_indexscan = false;
-- set work_mem = '64kB';
-- explain (costs off)
--   select unique1,
--          count(two), count(four), count(ten),
--          count(hundred), count(thousand), count(twothousand),
--          count(*)
--     from tenk1 group by grouping sets (unique1,twothousand,thousand,hundred,ten,four,two);
-- explain (costs off)
--   select unique1,
--          count(two), count(four), count(ten),
--          count(hundred), count(thousand), count(twothousand),
--          count(*)
--     from tenk1 group by grouping sets (unique1,hundred,ten,four,two);

-- Ignore a PostgreSQL-specific option
-- set work_mem = '384kB';
-- explain (costs off)
--   select unique1,
--          count(two), count(four), count(ten),
--          count(hundred), count(thousand), count(twothousand),
--          count(*)
--     from tenk1 group by grouping sets (unique1,twothousand,thousand,hundred,ten,four,two);

-- check collation-sensitive matching between grouping expressions
-- (similar to a check for aggregates, but there are additional code
-- paths for GROUPING, so check again here)

-- [SPARK-28382] Array Functions: unnest
select v||'a', case grouping(v||'a') when 1 then 1 else 0 end, count(*)
  -- from unnest(array[1,1], array['a','b']) u(i,v)
  from values (1, 'a'), (1, 'b') u(i,v)
 group by rollup(i, v||'a') order by 1,3;
select v||'a', case when grouping(v||'a') = 1 then 1 else 0 end, count(*)
  -- from unnest(array[1,1], array['a','b']) u(i,v)
  from values (1, 'a'), (1, 'b') u(i,v)
 group by rollup(i, v||'a') order by 1,3;

-- end

DROP VIEW gstest1;
DROP TABLE gstest2;
DROP TABLE gstest3;
DROP TABLE gstest4;
DROP TABLE gstest_empty;
