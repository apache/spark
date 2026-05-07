--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- JOIN
-- Test JOIN clauses
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/join.sql
--

-- There are 2 dimensions we want to test
--  1. run with broadcast hash join, sort merge join or shuffle hash join.
--  2. run with whole-stage-codegen, operator codegen or no codegen.

--CONFIG_DIM1 spark.sql.autoBroadcastJoinThreshold=10485760
--CONFIG_DIM1 spark.sql.autoBroadcastJoinThreshold=-1,spark.sql.join.preferSortMergeJoin=true
--CONFIG_DIM1 spark.sql.autoBroadcastJoinThreshold=-1,spark.sql.join.forceApplyShuffledHashJoin=true

--CONFIG_DIM2 spark.sql.codegen.wholeStage=true
--CONFIG_DIM2 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM2 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

CREATE OR REPLACE TEMPORARY VIEW INT2_TBL(f1) AS VALUES
  (smallint(trim('0   '))),
  (smallint(trim('  1234 '))),
  (smallint(trim('    -1234'))),
  (smallint('32767')),
  (smallint('-32767'));
CREATE OR REPLACE TEMPORARY VIEW INT4_TBL AS SELECT * FROM
  (VALUES (0), (123456), (-123456), (2147483647), (-2147483647))
  AS v(f1);
CREATE OR REPLACE TEMPORARY VIEW INT8_TBL AS SELECT * FROM
  (VALUES
    (123, 456),
    (123, 4567890123456789),
    (4567890123456789, 123),
    (4567890123456789, 4567890123456789),
    (4567890123456789, -4567890123456789))
  AS v(q1, q2);
CREATE OR REPLACE TEMPORARY VIEW FLOAT8_TBL AS SELECT * FROM
  (VALUES (0.0), (1004.30), (-34.84),
    (cast('1.2345678901234e+200' as double)), (cast('1.2345678901234e-200' as double)))
  AS v(f1);
CREATE OR REPLACE TEMPORARY VIEW TEXT_TBL AS SELECT * FROM
  (VALUES ('doh!'), ('hi de ho neighbor'))
  AS v(f1);
CREATE OR REPLACE TEMPORARY VIEW tenk2 AS SELECT * FROM tenk1;

CREATE TABLE J1_TBL (
  i integer,
  j integer,
  t string
) USING parquet;

CREATE TABLE J2_TBL (
  i integer,
  k integer
) USING parquet;


INSERT INTO J1_TBL VALUES (1, 4, 'one');
INSERT INTO J1_TBL VALUES (2, 3, 'two');
INSERT INTO J1_TBL VALUES (3, 2, 'three');
INSERT INTO J1_TBL VALUES (4, 1, 'four');
INSERT INTO J1_TBL VALUES (5, 0, 'five');
INSERT INTO J1_TBL VALUES (6, 6, 'six');
INSERT INTO J1_TBL VALUES (7, 7, 'seven');
INSERT INTO J1_TBL VALUES (8, 8, 'eight');
INSERT INTO J1_TBL VALUES (0, NULL, 'zero');
INSERT INTO J1_TBL VALUES (NULL, NULL, 'null');
INSERT INTO J1_TBL VALUES (NULL, 0, 'zero');

INSERT INTO J2_TBL VALUES (1, -1);
INSERT INTO J2_TBL VALUES (2, 2);
INSERT INTO J2_TBL VALUES (3, -3);
INSERT INTO J2_TBL VALUES (2, 4);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (0, NULL);
INSERT INTO J2_TBL VALUES (NULL, NULL);
INSERT INTO J2_TBL VALUES (NULL, 0);

-- [SPARK-20856] Do not need onerow because it only used for test statement using nested joins
-- useful in some tests below
-- create temp table onerow();
-- insert into onerow default values;
-- analyze onerow;


--
-- CORRELATION NAMES
-- Make sure that table/column aliases are supported
-- before diving into more complex join syntax.
--

SELECT '' AS `xxx`, *
  FROM J1_TBL AS tx;

SELECT '' AS `xxx`, *
  FROM J1_TBL tx;

SELECT '' AS `xxx`, *
  FROM J1_TBL AS t1 (a, b, c);

SELECT '' AS `xxx`, *
  FROM J1_TBL t1 (a, b, c);

SELECT '' AS `xxx`, *
  FROM J1_TBL t1 (a, b, c), J2_TBL t2 (d, e);

-- [SPARK-28377] Fully support correlation names in the FROM clause
-- SELECT '' AS "xxx", t1.a, t2.e
--   FROM J1_TBL t1 (a, b, c), J2_TBL t2 (d, e)
--   WHERE t1.a = t2.d;


--
-- CROSS JOIN
-- Qualifications are not allowed on cross joins,
-- which degenerate into a standard unqualified inner join.
--

SELECT '' AS `xxx`, *
  FROM J1_TBL CROSS JOIN J2_TBL;

-- ambiguous column
SELECT '' AS `xxx`, i, k, t
  FROM J1_TBL CROSS JOIN J2_TBL;

-- resolve previous ambiguity by specifying the table name
SELECT '' AS `xxx`, t1.i, k, t
  FROM J1_TBL t1 CROSS JOIN J2_TBL t2;

SELECT '' AS `xxx`, ii, tt, kk
  FROM (J1_TBL CROSS JOIN J2_TBL)
    AS tx (ii, jj, tt, ii2, kk);

-- [SPARK-28377] Fully support correlation names in the FROM clause
-- SELECT '' AS `xxx`, tx.ii, tx.jj, tx.kk
--   FROM (J1_TBL t1 (a, b, c) CROSS JOIN J2_TBL t2 (d, e))
--     AS tx (ii, jj, tt, ii2, kk);

SELECT '' AS `xxx`, *
  FROM J1_TBL CROSS JOIN J2_TBL a CROSS JOIN J2_TBL b;


--
--
-- Inner joins (equi-joins)
--
--

--
-- Inner joins (equi-joins) with USING clause
-- The USING syntax changes the shape of the resulting table
-- by including a column in the USING clause only once in the result.
--

-- Inner equi-join on specified column
SELECT '' AS `xxx`, *
  FROM J1_TBL INNER JOIN J2_TBL USING (i);

-- Same as above, slightly different syntax
SELECT '' AS `xxx`, *
  FROM J1_TBL JOIN J2_TBL USING (i);

SELECT '' AS `xxx`, *
  FROM J1_TBL t1 (a, b, c) JOIN J2_TBL t2 (a, d) USING (a)
  ORDER BY a, d;

-- [SPARK-28377] Fully support correlation names in the FROM clause
-- SELECT '' AS `xxx`, *
--   FROM J1_TBL t1 (a, b, c) JOIN J2_TBL t2 (a, b) USING (b)
--   ORDER BY b, t1.a;


--
-- NATURAL JOIN
-- Inner equi-join on all columns with the same name
--

SELECT '' AS `xxx`, *
  FROM J1_TBL NATURAL JOIN J2_TBL;

SELECT '' AS `xxx`, *
  FROM J1_TBL t1 (a, b, c) NATURAL JOIN J2_TBL t2 (a, d);

SELECT '' AS `xxx`, *
  FROM J1_TBL t1 (a, b, c) NATURAL JOIN J2_TBL t2 (d, a);

-- [SPARK-28377] Fully support correlation names in the FROM clause
-- mismatch number of columns
-- currently, Postgres will fill in with underlying names
-- SELECT '' AS `xxx`, *
--   FROM J1_TBL t1 (a, b) NATURAL JOIN J2_TBL t2 (a);


--
-- Inner joins (equi-joins)
--

SELECT '' AS `xxx`, *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i);

SELECT '' AS `xxx`, *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.k);


--
-- Non-equi-joins
--

SELECT '' AS `xxx`, *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i <= J2_TBL.k);


--
-- Outer joins
-- Note that OUTER is a noise word
--

SELECT '' AS `xxx`, *
  FROM J1_TBL LEFT OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS `xxx`, *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS `xxx`, *
  FROM J1_TBL RIGHT OUTER JOIN J2_TBL USING (i);

SELECT '' AS `xxx`, *
  FROM J1_TBL RIGHT JOIN J2_TBL USING (i);

SELECT '' AS `xxx`, *
  FROM J1_TBL FULL OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS `xxx`, *
  FROM J1_TBL FULL JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS `xxx`, *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (k = 1);

SELECT '' AS `xxx`, *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (i = 1);

--
-- semijoin selectivity for <>
--
-- explain (costs off)
-- select * from int4_tbl i4, tenk1 a
-- where exists(select * from tenk1 b
--              where a.twothousand = b.twothousand and a.fivethous <> b.fivethous)
--       and i4.f1 = a.tenthous;


--
-- More complicated constructs
--

--
-- Multiway full join
--

CREATE TABLE t1 (name STRING, n INTEGER) USING parquet;
CREATE TABLE t2 (name STRING, n INTEGER) USING parquet;
CREATE TABLE t3 (name STRING, n INTEGER) USING parquet;

INSERT INTO t1 VALUES ( 'bb', 11 );
INSERT INTO t2 VALUES ( 'bb', 12 );
INSERT INTO t2 VALUES ( 'cc', 22 );
INSERT INTO t2 VALUES ( 'ee', 42 );
INSERT INTO t3 VALUES ( 'bb', 13 );
INSERT INTO t3 VALUES ( 'cc', 23 );
INSERT INTO t3 VALUES ( 'dd', 33 );

SELECT * FROM t1 FULL JOIN t2 USING (name) FULL JOIN t3 USING (name);

--
-- Test interactions of join syntax and subqueries
--

-- Basic cases (we expect planner to pull up the subquery here)
SELECT * FROM
(SELECT * FROM t2) as s2
INNER JOIN
(SELECT * FROM t3) s3
USING (name);

SELECT * FROM
(SELECT * FROM t2) as s2
LEFT JOIN
(SELECT * FROM t3) s3
USING (name);

SELECT * FROM
(SELECT * FROM t2) as s2
FULL JOIN
(SELECT * FROM t3) s3
USING (name);

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL LEFT JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t1) as s1
NATURAL INNER JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t1) as s1
NATURAL FULL JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

SELECT * FROM
(SELECT name, n as s1_n FROM t1) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n FROM t2) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t3) as s3
  ) ss2;

SELECT * FROM
(SELECT name, n as s1_n FROM t1) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t3) as s3
  ) ss2;

-- Constants as join keys can also be problematic
SELECT * FROM
  (SELECT name, n as s1_n FROM t1) as s1
FULL JOIN
  (SELECT name, 2 as s2_n FROM t2) as s2
ON (s1_n = s2_n);


-- Test for propagation of nullability constraints into sub-joins

create or replace temporary view x as select * from
  (values (1,11), (2,22), (3,null), (4,44), (5,null))
  as v(x1, x2);

create or replace temporary view y as select * from
  (values (1,111), (2,222), (3,333), (4,null))
  as v(y1, y2);

select * from x;
select * from y;

select * from x left join y on (x1 = y1 and x2 is not null);
select * from x left join y on (x1 = y1 and y2 is not null);

select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and x2 is not null);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and y2 is not null);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and xx2 is not null);
-- these should NOT give the same answers as above
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (x2 is not null);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (y2 is not null);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (xx2 is not null);

--
-- regression test: check for bug with propagation of implied equality
-- to outside an IN
--
select count(*) from tenk1 a where unique1 in
  (select unique1 from tenk1 b join tenk1 c using (unique1)
   where b.unique2 = 42);

--
-- regression test: check for failure to generate a plan with multiple
-- degenerate IN clauses
--
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);

-- try that with GEQO too
-- begin;
-- set geqo = on;
-- set geqo_threshold = 2;
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);
-- rollback;

-- Skip this test because table b inherits from table a and we do not support this feature, see inherits.sql
--
-- regression test: be sure we cope with proven-dummy append rels
--
-- explain (costs off)
-- select aa, bb, unique1, unique1
--   from tenk1 right join b on aa = unique1
--   where bb < bb and bb is null;

-- select aa, bb, unique1, unique1
--   from tenk1 right join b on aa = unique1
--   where bb < bb and bb is null;

--
-- regression test: check handling of empty-FROM subquery underneath outer join
--
-- explain (costs off)
-- select * from int8_tbl i1 left join (int8_tbl i2 join
--   (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
-- order by 1, 2;

select * from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 1, 2;

--
-- regression test: check a case where join_clause_is_movable_into() gives
-- an imprecise result, causing an assertion failure
--
select count(*)
from
  (select t3.tenthous as x1, coalesce(t1.stringu1, t2.stringu1) as x2
   from tenk1 t1
   left join tenk1 t2 on t1.unique1 = t2.unique1
   join tenk1 t3 on t1.unique2 = t3.unique2) ss,
  tenk1 t4,
  tenk1 t5
where t4.thousand = t5.unique1 and ss.x1 = t4.tenthous and ss.x2 = t5.stringu1;

--
-- regression test: check a case where we formerly missed including an EC
-- enforcement clause because it was expected to be handled at scan level
--
-- explain (costs off)
-- select a.f1, b.f1, t.thousand, t.tenthous from
--   tenk1 t,
--   (select sum(f1)+1 as f1 from int4_tbl i4a) a,
--   (select sum(f1) as f1 from int4_tbl i4b) b
-- where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;

select a.f1, b.f1, t.thousand, t.tenthous from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl i4a) a,
  (select sum(f1) as f1 from int4_tbl i4b) b
where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;

--
-- check a case where we formerly got confused by conflicting sort orders
-- in redundant merge join path keys
--
-- explain (costs off)
-- select * from
--   j1_tbl full join
--   (select * from j2_tbl order by j2_tbl.i desc, j2_tbl.k asc) j2_tbl
--   on j1_tbl.i = j2_tbl.i and j1_tbl.i = j2_tbl.k;

select * from
  j1_tbl full join
  (select * from j2_tbl order by j2_tbl.i desc, j2_tbl.k asc) j2_tbl
  on j1_tbl.i = j2_tbl.i and j1_tbl.i = j2_tbl.k;

--
-- a different check for handling of redundant sort keys in merge joins
--
-- explain (costs off)
-- select count(*) from
--   (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
--   left join
--   (select * from tenk1 y order by y.unique2) y
--   on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;

select count(*) from
  (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
  left join
  (select * from tenk1 y order by y.unique2) y
  on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;


--
-- Clean up
--

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

DROP TABLE J1_TBL;
DROP TABLE J2_TBL;

-- Both DELETE and UPDATE allow the specification of additional tables
-- to "join" against to determine which rows should be modified.

-- CREATE TEMP TABLE t1 (a int, b int);
-- CREATE TEMP TABLE t2 (a int, b int);
-- CREATE TEMP TABLE t3 (x int, y int);

-- INSERT INTO t1 VALUES (5, 10);
-- INSERT INTO t1 VALUES (15, 20);
-- INSERT INTO t1 VALUES (100, 100);
-- INSERT INTO t1 VALUES (200, 1000);
-- INSERT INTO t2 VALUES (200, 2000);
-- INSERT INTO t3 VALUES (5, 20);
-- INSERT INTO t3 VALUES (6, 7);
-- INSERT INTO t3 VALUES (7, 8);
-- INSERT INTO t3 VALUES (500, 100);

-- DELETE FROM t3 USING t1 table1 WHERE t3.x = table1.a;
-- SELECT * FROM t3;
-- DELETE FROM t3 USING t1 JOIN t2 USING (a) WHERE t3.x > t1.a;
-- SELECT * FROM t3;
-- DELETE FROM t3 USING t3 t3_other WHERE t3.x = t3_other.x AND t3.y = t3_other.y;
-- SELECT * FROM t3;

-- Test join against inheritance tree

-- create temp table t2a () inherits (t2);

-- insert into t2a values (200, 2001);

-- select * from t1 left join t2 on (t1.a = t2.a);

-- Test matching of column name with wrong alias

-- select t1.x from t1 join t3 on (t1.a = t3.x);

--
-- regression test for 8.1 merge right join bug
--

create or replace temporary view tt1 as select * from
  (values (1, 11), (2, NULL))
  as v(tt1_id, joincol);

create or replace temporary view tt2 as select * from
  (values (21, 11), (22, 11))
  as v(tt2_id, joincol);

-- set enable_hashjoin to off;
-- set enable_nestloop to off;

-- these should give the same results

select tt1.*, tt2.* from tt1 left join tt2 on tt1.joincol = tt2.joincol;

select tt1.*, tt2.* from tt2 right join tt1 on tt1.joincol = tt2.joincol;

-- reset enable_hashjoin;
-- reset enable_nestloop;

--
-- regression test for bug #13908 (hash join with skew tuples & nbatch increase)
--

-- set work_mem to '64kB';
-- set enable_mergejoin to off;

-- explain (costs off)
-- select count(*) from tenk1 a, tenk1 b
--   where a.hundred = b.thousand and (b.fivethous % 10) < 10;
select count(*) from tenk1 a, tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;

-- reset work_mem;
-- reset enable_mergejoin;

--
-- regression test for 8.2 bug with improper re-ordering of left joins
--

create or replace temporary view tt3 as select * from
  (SELECT cast(x.id as int), repeat('xyzzy', 100) FROM range(1,10001) x)
  as v(f1, f2);
-- create index tt3i on tt3(f1);
-- analyze tt3;

create or replace temporary view tt4 as select * from
  (values (0), (1), (9999))
  as v(f1);
-- analyze tt4;

SELECT a.f1
FROM tt4 a
LEFT JOIN (
        SELECT b.f1
        FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)
        WHERE c.f1 IS NULL
) AS d ON (a.f1 = d.f1)
WHERE d.f1 IS NULL;

--
-- regression test for proper handling of outer joins within antijoins
--

-- create temp table tt4x(c1 int, c2 int, c3 int);

-- explain (costs off)
-- select * from tt4x t1
-- where not exists (
--   select 1 from tt4x t2
--     left join tt4x t3 on t2.c3 = t3.c1
--     left join ( select t5.c1 as c1
--                 from tt4x t4 left join tt4x t5 on t4.c2 = t5.c1
--               ) a1 on t3.c2 = a1.c1
--   where t1.c1 = t2.c2
-- );

--
-- regression test for problems of the sort depicted in bug #3494
--

create or replace temporary view tt5 as select * from
  (values (1, 10), (1, 11))
  as v(f1, f2);
create or replace temporary view tt6 as select * from
  (values (1, 9), (1, 2), (2, 9))
  as v(f1, f2);

select * from tt5,tt6 where tt5.f1 = tt6.f1 and tt5.f1 = tt5.f2 - tt6.f2;

--
-- regression test for problems of the sort depicted in bug #3588
--

create or replace temporary view xx as select * from
  (values (1), (2), (3))
  as v(pkxx);
create or replace temporary view yy as select * from
  (values (101, 1), (201, 2), (301, NULL))
  as v(pkyy, pkxx);

select yy.pkyy as yy_pkyy, yy.pkxx as yy_pkxx, yya.pkyy as yya_pkyy,
       xxa.pkxx as xxa_pkxx, xxb.pkxx as xxb_pkxx
from yy
     left join (SELECT * FROM yy where pkyy = 101) as yya ON yy.pkyy = yya.pkyy
     left join xx xxa on yya.pkxx = xxa.pkxx
     left join xx xxb on coalesce (xxa.pkxx, 1) = xxb.pkxx;

--
-- regression test for improper pushing of constants across outer-join clauses
-- (as seen in early 8.2.x releases)
--

create or replace temporary view zt1 as select * from
  (values (53))
  as v(f1);
create or replace temporary view zt2 as select * from
  (values (53))
  as v(f2);
create or replace temporary view zt3(f3 int) using parquet;

select * from
  zt2 left join zt3 on (f2 = f3)
      left join zt1 on (f3 = f1)
where f2 = 53;

create temp view zv1 as select *,'dummy' AS junk from zt1;

select * from
  zt2 left join zt3 on (f2 = f3)
      left join zv1 on (f3 = f1)
where f2 = 53;

--
-- regression test for improper extraction of OR indexqual conditions
-- (as seen in early 8.3.x releases)
--

select a.unique2, a.ten, b.tenthous, b.unique2, b.hundred
from tenk1 a left join tenk1 b on a.unique2 = b.tenthous
where a.unique1 = 42 and
      ((b.unique2 is null and a.ten = 2) or b.hundred = 3);

--
-- test proper positioning of one-time quals in EXISTS (8.4devel bug)
--
-- prepare foo(bool) as
--   select count(*) from tenk1 a left join tenk1 b
--     on (a.unique2 = b.unique1 and exists
--         (select 1 from tenk1 c where c.thousand = b.unique2 and $1));
-- execute foo(true);
-- execute foo(false);

--
-- test for sane behavior with noncanonical merge clauses, per bug #4926
--

-- begin;

-- set enable_mergejoin = 1;
-- set enable_hashjoin = 0;
-- set enable_nestloop = 0;

create or replace temporary view a (i integer) using parquet;
create or replace temporary view b (x integer, y integer) using parquet;

select * from a left join b on i = x and i = y and x = i;

-- rollback;

--
-- test handling of merge clauses using record_ops
--
-- begin;

-- create type mycomptype as (id int, v bigint);

-- create temp table tidv (idv mycomptype);
-- create index on tidv (idv);

-- explain (costs off)
-- select a.idv, b.idv from tidv a, tidv b where a.idv = b.idv;

-- set enable_mergejoin = 0;

-- explain (costs off)
-- select a.idv, b.idv from tidv a, tidv b where a.idv = b.idv;

-- rollback;

--
-- test NULL behavior of whole-row Vars, per bug #5025
--
--- [SPARK-34199] changed the `count(t2.*)` to `count(t2.q1, t2.q2)` since we have
--- blocked `count(tblName.*)`. Besides this, in pgsql, `count(t2.*)` of outter join
--- means how many matching rows produced by t2 while Spark SQL doesn't have this semantic.
--- So here we use `count(t2.q1, t2.q2)` instead of `count(1)` to keep the query output
--- unchanged.
select t1.q2, count(t2.q1, t2.q2)
from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

select t1.q2, count(t2.q1, t2.q2)
from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

-- [SPARK-28330] Enhance query limit
-- select t1.q2, count(t2.q1, t2.q2)
-- from int8_tbl t1 left join (select * from int8_tbl offset 0) t2 on (t1.q2 = t2.q1)
-- group by t1.q2 order by 1;

select t1.q2, count(t2.q1, t2.q2)
from int8_tbl t1 left join
  (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2
  on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--
-- test incorrect failure to NULL pulled-up subexpressions
--
-- begin;
create or replace temporary view a as select * from
  (values ('p'), ('q'))
  as v(code);
create or replace temporary view b as select * from
  (values ('p', 1), ('p', 2))
  as v(a, num);
create or replace temporary view c as select * from
  (values ('A', 'p'), ('B', 'q'), ('C', null))
  as v(name, a);

select c.name, ss.code, ss.b_cnt, ss.const
from c left join
  (select a.code, coalesce(b_grp.cnt, 0) as b_cnt, -1 as const
   from a left join
     (select count(1) as cnt, b.a from b group by b.a) as b_grp
     on a.code = b_grp.a
  ) as ss
  on (c.a = ss.code)
order by c.name;

-- rollback;

--
-- test incorrect handling of placeholders that only appear in targetlists,
-- per bug #6154
--
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, sub4.value2, COALESCE(sub4.value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

-- test the path using join aliases, too
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, value2, COALESCE(value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

--
-- test case where a PlaceHolderVar is used as a nestloop parameter
--

-- EXPLAIN (COSTS OFF)
-- SELECT qq, unique1
--   FROM
--   ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
--   FULL OUTER JOIN
--   ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
--   USING (qq)
--   INNER JOIN tenk1 c ON qq = unique2;

SELECT qq, unique1
  FROM
  ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
  FULL OUTER JOIN
  ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
  USING (qq)
  INNER JOIN tenk1 c ON qq = unique2;

--
-- nested nestloops can require nested PlaceHolderVars
--

create or replace temporary view nt1 as select * from
  (values(1,true,true), (2,true,false), (3,false,false))
  as v(id, a1, a2);
create or replace temporary view nt2 as select * from
  (values(1,1,true,true), (2,2,true,false), (3,3,false,false))
  as v(id, nt1_id, b1, b2);
create or replace temporary view nt3 as select * from
  (values(1,1,true), (2,2,false), (3,3,true))
  as v(id, nt2_id, c1);
-- explain (costs off)
-- select nt3.id
-- from nt3 as nt3
--   left join
--     (select nt2.*, (nt2.b1 and ss1.a3) AS b3
--      from nt2 as nt2
--        left join
--          (select nt1.*, (nt1.id is not null) as a3 from nt1) as ss1
--          on ss1.id = nt2.nt1_id
--     ) as ss2
--     on ss2.id = nt3.nt2_id
-- where nt3.id = 1 and ss2.b3;

select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 and ss1.a3) AS b3
     from nt2 as nt2
       left join
         (select nt1.*, (nt1.id is not null) as a3 from nt1) as ss1
         on ss1.id = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

-- Accessing outer query column is not allowed in LocalLimit
--
-- test case where a PlaceHolderVar is propagated into a subquery
--

-- explain (costs off)
-- select * from
--   int8_tbl t1 left join
--   (select q1 as x, 42 as y from int8_tbl t2) ss
--   on t1.q2 = ss.x
-- where
--   1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
-- order by 1,2;

-- select * from
--   int8_tbl t1 left join
--   (select q1 as x, 42 as y from int8_tbl t2) ss
--   on t1.q2 = ss.x
-- where
--   1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
-- order by 1,2;

--
-- test the corner cases FULL JOIN ON TRUE and FULL JOIN ON FALSE
--
select * from int4_tbl a full join int4_tbl b on true;
select * from int4_tbl a full join int4_tbl b on false;

--
-- test for ability to use a cartesian join when necessary
--

-- explain (costs off)
-- select * from
--   tenk1 join int4_tbl on f1 = twothousand,
--   int4(sin(1)) q1,
--   int4(sin(0)) q2
-- where q1 = thousand or q2 = thousand;

-- explain (costs off)
-- select * from
--   tenk1 join int4_tbl on f1 = twothousand,
--   int4(sin(1)) q1,
--   int4(sin(0)) q2
-- where thousand = (q1 + q2);

--
-- test ability to generate a suitable plan for a star-schema query
--

-- explain (costs off)
-- select * from
--   tenk1, int8_tbl a, int8_tbl b
-- where thousand = a.q1 and tenthous = b.q1 and a.q2 = 1 and b.q2 = 2;

--
-- test a corner case in which we shouldn't apply the star-schema optimization
--

-- explain (costs off)
-- select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
--   tenk1 t1
--   inner join int4_tbl i1
--     left join (select v1.x2, v2.y1, 11 AS d1
--                from (select 1,0 from onerow) v1(x1,x2)
--                left join (select 3,1 from onerow) v2(y1,y2)
--                on v1.x1 = v2.y2) subq1
--     on (i1.f1 = subq1.x2)
--   on (t1.unique2 = subq1.d1)
--   left join tenk1 t2
--   on (subq1.y1 = t2.unique1)
-- where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

-- [SPARK-20856] support statement using nested joins
-- select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
--   tenk1 t1
--   inner join int4_tbl i1
--     left join (select v1.x2, v2.y1, 11 AS d1
--                from (select 1,0 from onerow) v1(x1,x2)
--                left join (select 3,1 from onerow) v2(y1,y2)
--                on v1.x1 = v2.y2) subq1
--     on (i1.f1 = subq1.x2)
--   on (t1.unique2 = subq1.d1)
--   left join tenk1 t2
--   on (subq1.y1 = t2.unique1)
-- where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

-- variant that isn't quite a star-schema case

-- [SPARK-16452] basic INFORMATION_SCHEMA support
-- select ss1.d1 from
--   tenk1 as t1
--   inner join tenk1 as t2
--   on t1.tenthous = t2.ten
--   inner join
--     int8_tbl as i8
--     left join int4_tbl as i4
--       inner join (select 64::information_schema.cardinal_number as d1
--                   from tenk1 t3,
--                        lateral (select abs(t3.unique1) + random()) ss0(x)
--                   where t3.fivethous < 0) as ss1
--       on i4.f1 = ss1.d1
--     on i8.q1 = i4.f1
--   on t1.tenthous = ss1.d1
-- where t1.unique1 < i4.f1;

-- this variant is foldable by the remove-useless-RESULT-RTEs code

-- explain (costs off)
-- select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
--   tenk1 t1
--   inner join int4_tbl i1
--     left join (select v1.x2, v2.y1, 11 AS d1
--                from (values(1,0)) v1(x1,x2)
--                left join (values(3,1)) v2(y1,y2)
--                on v1.x1 = v2.y2) subq1
--     on (i1.f1 = subq1.x2)
--   on (t1.unique2 = subq1.d1)
--   left join tenk1 t2
--   on (subq1.y1 = t2.unique1)
-- where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

-- [SPARK-20856] support statement using nested joins
-- select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
--   tenk1 t1
--   inner join int4_tbl i1
--     left join (select v1.x2, v2.y1, 11 AS d1
--                from (values(1,0)) v1(x1,x2)
--                left join (values(3,1)) v2(y1,y2)
--                on v1.x1 = v2.y2) subq1
--     on (i1.f1 = subq1.x2)
--   on (t1.unique2 = subq1.d1)
--   left join tenk1 t2
--   on (subq1.y1 = t2.unique1)
-- where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

--
-- test extraction of restriction OR clauses from join OR clause
-- (we used to only do this for indexable clauses)
--

-- explain (costs off)
-- select * from tenk1 a join tenk1 b on
--   (a.unique1 = 1 and b.unique1 = 2) or (a.unique2 = 3 and b.hundred = 4);
-- explain (costs off)
-- select * from tenk1 a join tenk1 b on
--   (a.unique1 = 1 and b.unique1 = 2) or (a.unique2 = 3 and b.ten = 4);
-- explain (costs off)
-- select * from tenk1 a join tenk1 b on
--   (a.unique1 = 1 and b.unique1 = 2) or
--   ((a.unique2 = 3 or a.unique2 = 7) and b.hundred = 4);

--
-- test placement of movable quals in a parameterized join tree
--

-- explain (costs off)
-- select * from tenk1 t1 left join
--   (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
--   on t1.hundred = t2.hundred and t1.ten = t3.ten
-- where t1.unique1 = 1;

-- explain (costs off)
-- select * from tenk1 t1 left join
--   (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
--   on t1.hundred = t2.hundred and t1.ten + t2.ten = t3.ten
-- where t1.unique1 = 1;

-- explain (costs off)
-- select count(*) from
--   tenk1 a join tenk1 b on a.unique1 = b.unique2
--   left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
--   join int4_tbl on b.thousand = f1;

select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

-- explain (costs off)
-- select b.unique1 from
--   tenk1 a join tenk1 b on a.unique1 = b.unique2
--   left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
--   join int4_tbl i1 on b.thousand = f1
--   right join int4_tbl i2 on i2.f1 = b.tenthous
--   order by 1;

select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

-- explain (costs off)
-- select * from
-- (
--   select unique1, q1, coalesce(unique1, -1) + q1 as fault
--   from int8_tbl left join tenk1 on (q2 = unique2)
-- ) ss
-- where fault = 122
-- order by fault;

select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

-- explain (costs off)
-- select * from
-- (values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
-- left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
-- left join unnest(v1ys) as u1(u1y) on u1y = v2y;

-- [SPARK-28382] Array Functions: unnest
-- select * from
-- (values (1, array(10,20)), (2, array(20,30))) as v1(v1x,v1ys)
-- left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
-- left join unnest(v1ys) as u1(u1y) on u1y = v2y;

--
-- test handling of potential equivalence clauses above outer joins
--

-- explain (costs off)
-- select q1, unique2, thousand, hundred
--   from int8_tbl a left join tenk1 b on q1 = unique2
--   where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

-- explain (costs off)
-- select f1, unique2, case when unique2 is null then f1 else 0 end
--   from int4_tbl a left join tenk1 b on f1 = unique2
--   where (case when unique2 is null then f1 else 0 end) = 0;

select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

--
-- another case with equivalence clauses above outer joins (bug #8591)
--

-- explain (costs off)
-- select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
--   from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
--   where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

--
-- check handling of join aliases when flattening multiple levels of subquery
--

-- explain (verbose, costs off)
-- select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
--   (values (0),(1)) foo1(join_key)
-- left join
--   (select join_key, bug_field from
--     (select ss1.join_key, ss1.bug_field from
--       (select f1 as join_key, 666 as bug_field from int4_tbl i1) ss1
--     ) foo2
--    left join
--     (select unique2 as join_key from tenk1 i2) ss2
--    using (join_key)
--   ) foo3
-- using (join_key);


-- [SPARK-28377] Fully support correlation names in the FROM clause
-- select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
--   (values (0),(1)) foo1(join_key)
-- left join
--   (select join_key, bug_field from
--     (select ss1.join_key, ss1.bug_field from
--       (select f1 as join_key, 666 as bug_field from int4_tbl i1) ss1
--     ) foo2
--    left join
--     (select unique2 as join_key from tenk1 i2) ss2
--    using (join_key)
--   ) foo3
-- using (join_key);

-- [SPARK-20856] Support statement using nested joins
--
-- test successful handling of nested outer joins with degenerate join quals
--

-- explain (verbose, costs off)
-- select t1.* from
--   text_tbl t1
--   left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
--     left join int8_tbl i8
--       left join (select *, null::int as d2 from int8_tbl i8b2) b2
--       on (i8.q1 = b2.q1)
--     on (b2.d2 = b1.q2)
--   on (t1.f1 = b1.d1)
--   left join int4_tbl i4
--   on (i8.q2 = i4.f1);

-- select t1.* from
--   text_tbl t1
--   left join (select *, string('***') as d1 from int8_tbl i8b1) b1
--     left join int8_tbl i8
--       left join (select *, int(null) as d2 from int8_tbl i8b2) b2
--       on (i8.q1 = b2.q1)
--     on (b2.d2 = b1.q2)
--   on (t1.f1 = b1.d1)
--   left join int4_tbl i4
--   on (i8.q2 = i4.f1);

-- explain (verbose, costs off)
-- select t1.* from
--   text_tbl t1
--   left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
--     left join int8_tbl i8
--       left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
--       on (i8.q1 = b2.q1)
--     on (b2.d2 = b1.q2)
--   on (t1.f1 = b1.d1)
--   left join int4_tbl i4
--   on (i8.q2 = i4.f1);

-- select t1.* from
--   text_tbl t1
--   left join (select *, string('***') as d1 from int8_tbl i8b1) b1
--     left join int8_tbl i8
--       left join (select *, int(null) as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
--       on (i8.q1 = b2.q1)
--     on (b2.d2 = b1.q2)
--   on (t1.f1 = b1.d1)
--   left join int4_tbl i4
--   on (i8.q2 = i4.f1);

-- explain (verbose, costs off)
-- select t1.* from
--   text_tbl t1
--   left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
--     left join int8_tbl i8
--       left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
--                  where q1 = f1) b2
--       on (i8.q1 = b2.q1)
--     on (b2.d2 = b1.q2)
--   on (t1.f1 = b1.d1)
--   left join int4_tbl i4
--   on (i8.q2 = i4.f1);

-- select t1.* from
--   text_tbl t1
--   left join (select *, string('***') as d1 from int8_tbl i8b1) b1
--     left join int8_tbl i8
--       left join (select *, int(null) as d2 from int8_tbl i8b2, int4_tbl i4b2
--                  where q1 = f1) b2
--       on (i8.q1 = b2.q1)
--     on (b2.d2 = b1.q2)
--   on (t1.f1 = b1.d1)
--   left join int4_tbl i4
--   on (i8.q2 = i4.f1);

-- explain (verbose, costs off)
-- select * from
--   text_tbl t1
--   inner join int8_tbl i8
--   on i8.q2 = 456
--   right join text_tbl t2
--   on t1.f1 = 'doh!'
--   left join int4_tbl i4
--   on i8.q1 = i4.f1;

select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

-- [SPARK-27877] ANSI SQL: LATERAL derived table(T491)
--
-- test for appropriate join order in the presence of lateral references
--

-- explain (verbose, costs off)
select * from
   text_tbl t1
   left join int8_tbl i8
   on i8.q2 = 123,
   lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

-- explain (verbose, costs off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

-- explain (verbose, costs off)
select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

--
-- check a case in which a PlaceHolderVar forces join order
--

-- explain (verbose, costs off)
-- select ss2.* from
--   int4_tbl i41
--   left join int8_tbl i8
--     join (select i42.f1 as c1, i43.f1 as c2, 42 as c3
--           from int4_tbl i42, int4_tbl i43) ss1
--     on i8.q1 = ss1.c2
--   on i41.f1 = ss1.c1,
--   lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2
-- where ss1.c2 = 0;

-- select ss2.* from
--   int4_tbl i41
--   left join int8_tbl i8
--     join (select i42.f1 as c1, i43.f1 as c2, 42 as c3
--           from int4_tbl i42, int4_tbl i43) ss1
--     on i8.q1 = ss1.c2
--   on i41.f1 = ss1.c1,
--   lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2
-- where ss1.c2 = 0;

--
-- test successful handling of full join underneath left join (bug #14105)
--

-- explain (costs off)
-- select * from
--   (select 1 as id) as xx
--   left join
--     (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
--   on (xx.id = coalesce(yy.id));

select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

--
-- test ability to push constants through outer join clauses
--

-- explain (costs off)
--   select * from int4_tbl a left join tenk1 b on f1 = unique2 where f1 = 0;

-- explain (costs off)
--   select * from tenk1 a full join tenk1 b using(unique2) where unique2 = 42;

--
-- test that quals attached to an outer join have correct semantics,
-- specifically that they don't re-use expressions computed below the join;
-- we force a mergejoin so that coalesce(b.q1, 1) appears as a join input
--

-- set enable_hashjoin to off;
-- set enable_nestloop to off;

-- explain (verbose, costs off)
--   select a.q2, b.q1
--     from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
--     where coalesce(b.q1, 1) > 0;
select a.q2, b.q1
  from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
  where coalesce(b.q1, 1) > 0;

-- reset enable_hashjoin;
-- reset enable_nestloop;

-- Skip these test because it only test explain
--
-- test join removal
--

-- begin;

-- CREATE TEMP TABLE a (id int PRIMARY KEY, b_id int);
-- CREATE TEMP TABLE b (id int PRIMARY KEY, c_id int);
-- CREATE TEMP TABLE c (id int PRIMARY KEY);
-- CREATE TEMP TABLE d (a int, b int);
-- INSERT INTO a VALUES (0, 0), (1, NULL);
-- INSERT INTO b VALUES (0, 0), (1, NULL);
-- INSERT INTO c VALUES (0), (1);
-- INSERT INTO d VALUES (1,3), (2,2), (3,1);

-- all three cases should be optimizable into a simple seqscan
-- explain (costs off) SELECT a.* FROM a LEFT JOIN b ON a.b_id = b.id;
-- explain (costs off) SELECT b.* FROM b LEFT JOIN c ON b.c_id = c.id;
-- explain (costs off)
--   SELECT a.* FROM a LEFT JOIN (b left join c on b.c_id = c.id)
--   ON (a.b_id = b.id);

-- check optimization of outer join within another special join
-- explain (costs off)
-- select id from a where id in (
-- 	select b.id from b left join c on b.id = c.id
-- );

-- check that join removal works for a left join when joining a subquery
-- that is guaranteed to be unique by its GROUP BY clause
-- explain (costs off)
-- select d.* from d left join (select * from b group by b.id, b.c_id) s
--   on d.a = s.id and d.b = s.c_id;

-- similarly, but keying off a DISTINCT clause
-- explain (costs off)
-- select d.* from d left join (select distinct * from b) s
--   on d.a = s.id and d.b = s.c_id;

-- join removal is not possible when the GROUP BY contains a column that is
-- not in the join condition.  (Note: as of 9.6, we notice that b.id is a
-- primary key and so drop b.c_id from the GROUP BY of the resulting plan;
-- but this happens too late for join removal in the outer plan level.)
-- explain (costs off)
-- select d.* from d left join (select * from b group by b.id, b.c_id) s
--   on d.a = s.id;

-- similarly, but keying off a DISTINCT clause
-- explain (costs off)
-- select d.* from d left join (select distinct * from b) s
--   on d.a = s.id;

-- check join removal works when uniqueness of the join condition is enforced
-- by a UNION
-- explain (costs off)
-- select d.* from d left join (select id from a union select id from b) s
--   on d.a = s.id;

-- check join removal with a cross-type comparison operator
-- explain (costs off)
-- select i8.* from int8_tbl i8 left join (select f1 from int4_tbl group by f1) i4
--   on i8.q1 = i4.f1;

-- check join removal with lateral references
-- explain (costs off)
-- select 1 from (select a.id FROM a left join b on a.b_id = b.id) q,
-- 			  lateral generate_series(1, q.id) gs(i) where q.id = gs.i;

-- rollback;

create or replace temporary view parent as select * from
  (values (1, 10), (2, 20), (3, 30))
  as v(k, pd);
create or replace temporary view child as select * from
  (values (1, 100), (4, 400))
  as v(k, cd);

-- this case is optimizable
select p.* from parent p left join child c on (p.k = c.k);
-- explain (costs off)
--   select p.* from parent p left join child c on (p.k = c.k);

-- this case is not
select p.*, linked from parent p
  left join (select c.*, true as linked from child c) as ss
  on (p.k = ss.k);
-- explain (costs off)
--   select p.*, linked from parent p
--     left join (select c.*, true as linked from child c) as ss
--     on (p.k = ss.k);

-- check for a 9.0rc1 bug: join removal breaks pseudoconstant qual handling
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;
-- explain (costs off)
-- select p.* from
--   parent p left join child c on (p.k = c.k)
--   where p.k = 1 and p.k = 2;

select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;
-- explain (costs off)
-- select p.* from
--   (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
--   where p.k = 1 and p.k = 2;

-- bug 5255: this is not optimizable by join removal
-- begin;

create or replace temporary view a as select * from
  (values (0), (1))
  as v(id);
create or replace temporary view b as select * from
  (values (0, 0), (1, NULL))
  as v(id, a_id);

SELECT * FROM b LEFT JOIN a ON (b.a_id = a.id) WHERE (a.id IS NULL OR a.id > 0);
SELECT b.* FROM b LEFT JOIN a ON (b.a_id = a.id) WHERE (a.id IS NULL OR a.id > 0);

-- rollback;

-- another join removal bug: this is not optimizable, either
-- begin;

create or replace temporary view innertab as select * from
  (values (123L, 42L))
  as v(id, dat1);

SELECT * FROM
    (SELECT 1 AS x) ss1
  LEFT JOIN
    (SELECT q1, q2, COALESCE(dat1, q1) AS y
     FROM int8_tbl LEFT JOIN innertab ON q2 = id) ss2
  ON true;

-- rollback;

-- another join removal bug: we must clean up correctly when removing a PHV
-- begin;

-- create temp table uniquetbl (f1 text unique);

-- explain (costs off)
-- select t1.* from
--   uniquetbl as t1
--   left join (select *, '***'::text as d1 from uniquetbl) t2
--   on t1.f1 = t2.f1
--   left join uniquetbl t3
--   on t2.d1 = t3.f1;

-- explain (costs off)
-- select t0.*
-- from
--  text_tbl t0
--  left join
--    (select case t1.ten when 0 then 'doh!'::text else null::text end as case1,
--            t1.stringu2
--      from tenk1 t1
--      join int4_tbl i4 ON i4.f1 = t1.unique2
--      left join uniquetbl u1 ON u1.f1 = t1.string4) ss
--   on t0.f1 = ss.case1
-- where ss.stringu2 !~* ss.case1;

-- [SPARK-27987] Support POSIX Regular Expressions
-- select t0.*
-- from
--  text_tbl t0
--  left join
--    (select case t1.ten when 0 then 'doh!'::text else null::text end as case1,
--            t1.stringu2
--      from tenk1 t1
--      join int4_tbl i4 ON i4.f1 = t1.unique2
--      left join uniquetbl u1 ON u1.f1 = t1.string4) ss
--   on t0.f1 = ss.case1
-- where ss.stringu2 !~* ss.case1;

-- rollback;

-- bug #8444: we've historically allowed duplicate aliases within aliased JOINs

select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y) j on q1 = f1; -- error
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y) j on q1 = y.f1; -- error
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y(ff)) j on q1 = f1; -- ok

--
-- Test hints given on incorrect column references are useful
--

select t1.uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, prefer "t1" suggestion
select t2.uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, prefer "t2" suggestion
select uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, suggest both at once

-- Skip this test because it is a PostgreSQL specific case
--
-- Take care to reference the correct RTE
--

-- -- select atts.relid::regclass, s.* from pg_stats s join
-- --     pg_attribute a on s.attname = a.attname and s.tablename =
-- --     a.attrelid::regclass::text join (select unnest(indkey) attnum,
-- --     indexrelid from pg_index i) atts on atts.attnum = a.attnum where
--     schemaname != 'pg_catalog';

-- [SPARK-27877] ANSI SQL: LATERAL derived table(T491)
--
-- Test LATERAL
--
select unique2, x.*
from tenk1 a, lateral (select * from int4_tbl b where f1 = a.unique1) x;
-- explain (costs off)
--   select unique2, x.*
--   from tenk1 a, lateral (select * from int4_tbl b where f1 = a.unique1) x;
select unique2, x.*
from int4_tbl x, lateral (select unique2 from tenk1 where f1 = unique1) ss;
-- explain (costs off)
--   select unique2, x.*
--   from int4_tbl x, lateral (select unique2 from tenk1 where f1 = unique1) ss;
-- explain (costs off)
--   select unique2, x.*
--   from int4_tbl x cross join lateral (select unique2 from tenk1 where f1 = unique1) ss;
select unique2, x.*
from int4_tbl x left join lateral (select unique1, unique2 from tenk1 where f1 = unique1) ss on true;
-- explain (costs off)
--   select unique2, x.*
--   from int4_tbl x left join lateral (select unique1, unique2 from tenk1 where f1 = unique1) ss on true;

-- [SPARK-27877] ANSI SQL: LATERAL derived table(T491)
-- check scoping of lateral versus parent references
-- the first of these should return int8_tbl.q2, the second int8_tbl.q1
-- Expressions referencing the outer query are not supported outside of WHERE/HAVING clauses
-- select *, (select r from (select q1 as q2) x, (select q2 as r) y) from int8_tbl;
-- select *, (select r from (select q1 as q2) x, lateral (select q2 as r) y) from int8_tbl;

-- lateral with function in FROM
-- select count(*) from tenk1 a, lateral generate_series(1,two) g;
-- explain (costs off)
--   select count(*) from tenk1 a, lateral generate_series(1,two) g;
-- explain (costs off)
--   select count(*) from tenk1 a cross join lateral generate_series(1,two) g;
-- don't need the explicit LATERAL keyword for functions
-- explain (costs off)
--   select count(*) from tenk1 a, generate_series(1,two) g;

-- lateral with UNION ALL subselect
-- explain (costs off)
--   select * from generate_series(100,200) g,
--     lateral (select * from int8_tbl a where g = q1 union all
--              select * from int8_tbl b where g = q2) ss;
-- select * from generate_series(100,200) g,
--   lateral (select * from int8_tbl a where g = q1 union all
--            select * from int8_tbl b where g = q2) ss;

-- lateral with VALUES
-- explain (costs off)
--   select count(*) from tenk1 a,
--     tenk1 b join lateral (values(a.unique1)) ss(x) on b.unique2 = ss.x;
-- select count(*) from tenk1 a,
--   tenk1 b join lateral (values(a.unique1)) ss(x) on b.unique2 = ss.x;

-- lateral with VALUES, no flattening possible
-- explain (costs off)
--   select count(*) from tenk1 a,
--     tenk1 b join lateral (values(a.unique1),(-1)) ss(x) on b.unique2 = ss.x;
-- select count(*) from tenk1 a,
--   tenk1 b join lateral (values(a.unique1),(-1)) ss(x) on b.unique2 = ss.x;

-- lateral injecting a strange outer join condition
-- explain (costs off)
--   select * from int8_tbl a,
--     int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
--       on x.q2 = ss.z
--   order by a.q1, a.q2, x.q1, x.q2, ss.z;
-- select * from int8_tbl a,
--   int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
--     on x.q2 = ss.z
--   order by a.q1, a.q2, x.q1, x.q2, ss.z;

-- lateral reference to a join alias variable
 select * from (select f1/2 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1,
   lateral (select x) ss2(y);
-- select * from (select f1 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1,
--   lateral (values(x)) ss2(y);
 select * from ((select f1/2 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1) j,
   lateral (select x) ss2(y);

-- lateral references requiring pullup
-- select * from (values(1)) x(lb),
--   lateral generate_series(lb,4) x4;
-- select * from (select f1/1000000000 from int4_tbl) x(lb),
--   lateral generate_series(lb,4) x4;
-- select * from (values(1)) x(lb),
--   lateral (values(lb)) y(lbcopy);
-- select * from (values(1)) x(lb),
--   lateral (select lb from int4_tbl) y(lbcopy);
-- select * from
--   int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
--   lateral (values(x.q1,y.q1,y.q2)) v(xq1,yq1,yq2);
select * from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);
select x.* from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);
-- Accessing outer query column is not allowed in Union
-- select v.* from
--   (int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1)
--   left join int4_tbl z on z.f1 = x.q2,
--   lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy);
-- Accessing outer query column is not allowed in Union
-- select v.* from
--   (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
--   left join int4_tbl z on z.f1 = x.q2,
--   lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy);
-- Expressions referencing the outer query are not supported outside of WHERE/HAVING clauses
-- select v.* from
--   (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
--   left join int4_tbl z on z.f1 = x.q2,
--   lateral (select x.q1,y.q1 from onerow union all select x.q2,y.q2 from onerow) v(vx,vy);

-- explain (verbose, costs off)
-- select * from
--   int8_tbl a left join
--   lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;
select * from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;
-- explain (verbose, costs off)
-- select * from
--   int8_tbl a left join
--   lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;
select * from
  int8_tbl a left join
  lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;

-- lateral can result in join conditions appearing below their
-- real semantic level
-- explain (verbose, costs off)
-- select * from int4_tbl i left join
--   lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;
select * from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;
-- explain (verbose, costs off)
-- select * from int4_tbl i left join
--   lateral (select coalesce(i) from int2_tbl j where i.f1 = j.f1) k on true;
-- select * from int4_tbl i left join
--   lateral (select coalesce(i) from int2_tbl j where i.f1 = j.f1) k on true;
-- explain (verbose, costs off)
-- select * from int4_tbl a,
--   lateral (
--     select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
--   ) ss;
-- select * from int4_tbl a,
--   lateral (
--     select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
--   ) ss;

-- lateral reference in a PlaceHolderVar evaluated at join level
-- explain (verbose, costs off)
-- select * from
--   int8_tbl a left join lateral
--   (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
--    int8_tbl b cross join int8_tbl c) ss
--   on a.q2 = ss.bq1;
select * from
  int8_tbl a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   int8_tbl b cross join int8_tbl c) ss
  on a.q2 = ss.bq1;

-- case requiring nested PlaceHolderVars
-- explain (verbose, costs off)
-- select * from
--   int8_tbl c left join (
--     int8_tbl a left join (select q1, coalesce(q2,42) as x from int8_tbl b) ss1
--       on a.q2 = ss1.q1
--     cross join
--     lateral (select q1, coalesce(ss1.x,q2) as y from int8_tbl d) ss2
--   ) on c.q2 = ss2.q1,
--   lateral (select ss2.y offset 0) ss3;

-- case that breaks the old ph_may_need optimization
-- explain (verbose, costs off)
-- select c.*,a.*,ss1.q1,ss2.q1,ss3.* from
--   int8_tbl c left join (
--     int8_tbl a left join
--       (select q1, coalesce(q2,f1) as x from int8_tbl b, int4_tbl b2
--        where q1 < f1) ss1
--       on a.q2 = ss1.q1
--     cross join
--     lateral (select q1, coalesce(ss1.x,q2) as y from int8_tbl d) ss2
--   ) on c.q2 = ss2.q1,
--   lateral (select * from int4_tbl i where ss2.y > f1) ss3;

-- check processing of postponed quals (bug #9041)
-- explain (verbose, costs off)
-- select * from
--   (select 1 as x offset 0) x cross join (select 2 as y offset 0) y
--   left join lateral (
--     select * from (select 3 as z offset 0) z where z.z = x.x
--   ) zz on zz.z = y.y;

-- check dummy rels with lateral references (bug #15694)
-- explain (verbose, costs off)
-- select * from int8_tbl i8 left join lateral
--   (select *, i8.q2 from int4_tbl where false) ss on true;
-- explain (verbose, costs off)
-- select * from int8_tbl i8 left join lateral
--   (select *, i8.q2 from int4_tbl i1, int4_tbl i2 where false) ss on true;

-- check handling of nested appendrels inside LATERAL
-- select * from
--   ((select 2 as v) union all (select 3 as v)) as q1
--   cross join lateral
--   ((select * from
--       ((select 4 as v) union all (select 5 as v)) as q3)
--    union all
--    (select q1.v)
--   ) as q2;

-- check we don't try to do a unique-ified semijoin with LATERAL
-- explain (verbose, costs off)
-- select * from
--   (values (0,9998), (1,1000)) v(id,x),
--   lateral (select f1 from int4_tbl
--            where f1 = any (select unique1 from tenk1
--                            where unique2 = v.x offset 0)) ss;
-- select * from
--   (values (0,9998), (1,1000)) v(id,x),
--   lateral (select f1 from int4_tbl
--            where f1 = any (select unique1 from tenk1
--                            where unique2 = v.x offset 0)) ss;

-- check proper extParam/allParam handling (this isn't exactly a LATERAL issue,
-- but we can make the test case much more compact with LATERAL)
-- explain (verbose, costs off)
-- select * from (values (0), (1)) v(id),
-- lateral (select * from int8_tbl t1,
--          lateral (select * from
--                     (select * from int8_tbl t2
--                      where q1 = any (select q2 from int8_tbl t3
--                                      where q2 = (select greatest(t1.q1,t2.q2))
--                                        and (select v.id=0)) offset 0) ss2) ss
--          where t1.q1 = ss.q2) ss0;

-- select * from (values (0), (1)) v(id),
-- lateral (select * from int8_tbl t1,
--          lateral (select * from
--                     (select * from int8_tbl t2
--                      where q1 = any (select q2 from int8_tbl t3
--                                      where q2 = (select greatest(t1.q1,t2.q2))
--                                        and (select v.id=0)) offset 0) ss2) ss
--          where t1.q1 = ss.q2) ss0;

-- test some error cases where LATERAL should have been used but wasn't
select f1,g from int4_tbl a, (select f1 as g) ss;
select f1,g from int4_tbl a, (select a.f1 as g) ss;
select f1,g from int4_tbl a cross join (select f1 as g) ss;
select f1,g from int4_tbl a cross join (select a.f1 as g) ss;
-- SQL:2008 says the left table is in scope but illegal to access here
-- select f1,g from int4_tbl a right join lateral generate_series(0, a.f1) g on true;
-- select f1,g from int4_tbl a full join lateral generate_series(0, a.f1) g on true;
-- check we complain about ambiguous table references
-- select * from
--   int8_tbl x cross join (int4_tbl x cross join lateral (select x.f1) ss);
-- LATERAL can be used to put an aggregate into the FROM clause of its query
-- select 1 from tenk1 a, lateral (select max(a.unique1) from int4_tbl b) ss;

-- check behavior of LATERAL in UPDATE/DELETE

-- create temp table xx1 as select f1 as x1, -f1 as x2 from int4_tbl;

-- error, can't do this:
-- update xx1 set x2 = f1 from (select * from int4_tbl where f1 = x1) ss;
-- update xx1 set x2 = f1 from (select * from int4_tbl where f1 = xx1.x1) ss;
-- can't do it even with LATERAL:
-- update xx1 set x2 = f1 from lateral (select * from int4_tbl where f1 = x1) ss;
-- we might in future allow something like this, but for now it's an error:
-- update xx1 set x2 = f1 from xx1, lateral (select * from int4_tbl where f1 = x1) ss;

-- also errors:
-- delete from xx1 using (select * from int4_tbl where f1 = x1) ss;
-- delete from xx1 using (select * from int4_tbl where f1 = xx1.x1) ss;
-- delete from xx1 using lateral (select * from int4_tbl where f1 = x1) ss;

-- [SPARK-25411] Implement range partition in Spark
--
-- test LATERAL reference propagation down a multi-level inheritance hierarchy
-- produced for a multi-level partitioned table hierarchy.
--
-- create table join_pt1 (a int, b int, c varchar) partition by range(a);
-- create table join_pt1p1 partition of join_pt1 for values from (0) to (100) partition by range(b);
-- create table join_pt1p2 partition of join_pt1 for values from (100) to (200);
-- create table join_pt1p1p1 partition of join_pt1p1 for values from (0) to (100);
-- insert into join_pt1 values (1, 1, 'x'), (101, 101, 'y');
-- create table join_ut1 (a int, b int, c varchar);
-- insert into join_ut1 values (101, 101, 'y'), (2, 2, 'z');
-- explain (verbose, costs off)
-- select t1.b, ss.phv from join_ut1 t1 left join lateral
--               (select t2.a as t2a, t3.a t3a, least(t1.a, t2.a, t3.a) phv
-- 					  from join_pt1 t2 join join_ut1 t3 on t2.a = t3.b) ss
--               on t1.a = ss.t2a order by t1.a;
-- select t1.b, ss.phv from join_ut1 t1 left join lateral
--               (select t2.a as t2a, t3.a t3a, least(t1.a, t2.a, t3.a) phv
-- 					  from join_pt1 t2 join join_ut1 t3 on t2.a = t3.b) ss
--               on t1.a = ss.t2a order by t1.a;
--
-- drop table join_pt1;
-- drop table join_ut1;
--
-- test that foreign key join estimation performs sanely for outer joins
--

-- begin;

-- create table fkest (a int, b int, c int unique, primary key(a,b));
-- create table fkest1 (a int, b int, primary key(a,b));

-- insert into fkest select x/10, x%10, x from generate_series(1,1000) x;
-- insert into fkest1 select x/10, x%10 from generate_series(1,1000) x;

-- alter table fkest1
--   add constraint fkest1_a_b_fkey foreign key (a,b) references fkest;

-- analyze fkest;
-- analyze fkest1;

-- explain (costs off)
-- select *
-- from fkest f
--   left join fkest1 f1 on f.a = f1.a and f.b = f1.b
--   left join fkest1 f2 on f.a = f2.a and f.b = f2.b
--   left join fkest1 f3 on f.a = f3.a and f.b = f3.b
-- where f.c = 1;

-- rollback;

-- Skip these test because it only test explain
--
-- test planner's ability to mark joins as unique
--

-- create table j1 (id int primary key);
-- create table j2 (id int primary key);
-- create table j3 (id int);

-- insert into j1 values(1),(2),(3);
-- insert into j2 values(1),(2),(3);
-- insert into j3 values(1),(1);

-- analyze j1;
-- analyze j2;
-- analyze j3;

-- ensure join is properly marked as unique
-- explain (verbose, costs off)
-- select * from j1 inner join j2 on j1.id = j2.id;

-- ensure join is not unique when not an equi-join
-- explain (verbose, costs off)
-- select * from j1 inner join j2 on j1.id > j2.id;

-- ensure non-unique rel is not chosen as inner
-- explain (verbose, costs off)
-- select * from j1 inner join j3 on j1.id = j3.id;

-- ensure left join is marked as unique
-- explain (verbose, costs off)
-- select * from j1 left join j2 on j1.id = j2.id;

-- ensure right join is marked as unique
-- explain (verbose, costs off)
-- select * from j1 right join j2 on j1.id = j2.id;

-- ensure full join is marked as unique
-- explain (verbose, costs off)
-- select * from j1 full join j2 on j1.id = j2.id;

-- a clauseless (cross) join can't be unique
-- explain (verbose, costs off)
-- select * from j1 cross join j2;

-- ensure a natural join is marked as unique
-- explain (verbose, costs off)
-- select * from j1 natural join j2;

-- ensure a distinct clause allows the inner to become unique
-- explain (verbose, costs off)
-- select * from j1
-- inner join (select distinct id from j3) j3 on j1.id = j3.id;

-- ensure group by clause allows the inner to become unique
-- explain (verbose, costs off)
-- select * from j1
-- inner join (select id from j3 group by id) j3 on j1.id = j3.id;

-- drop table if exists j1;
-- drop table if exists j2;
-- drop table if exists j3;

-- test more complex permutations of unique joins

CREATE TABLE j1 (id1 int, id2 int) USING parquet;
CREATE TABLE j2 (id1 int, id2 int) USING parquet;
-- create table j3 (id1 int, id2 int) using parquet;

INSERT INTO j1 values(1,1),(1,2);
INSERT INTO j2 values(1,1);
-- insert into j3 values(1,1);

-- analyze j1;
-- analyze j2;
-- analyze j3;

-- ensure there's no unique join when not all columns which are part of the
-- unique index are seen in the join clause
-- explain (verbose, costs off)
-- select * from j1
-- inner join j2 on j1.id1 = j2.id1;

-- ensure proper unique detection with multiple join quals
-- explain (verbose, costs off)
-- select * from j1
-- inner join j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2;

-- ensure we don't detect the join to be unique when quals are not part of the
-- join condition
-- explain (verbose, costs off)
-- select * from j1
-- inner join j2 on j1.id1 = j2.id1 where j1.id2 = 1;

-- as above, but for left joins.
-- explain (verbose, costs off)
-- select * from j1
-- left join j2 on j1.id1 = j2.id1 where j1.id2 = 1;

-- validate logic in merge joins which skips mark and restore.
-- it should only do this if all quals which were used to detect the unique
-- are present as join quals, and not plain quals.
-- set enable_nestloop to 0;
-- set enable_hashjoin to 0;
-- set enable_sort to 0;

-- create indexes that will be preferred over the PKs to perform the join
-- create index j1_id1_idx on j1 (id1) where id1 % 1000 = 1;
-- create index j2_id1_idx on j2 (id1) where id1 % 1000 = 1;

-- need an additional row in j2, if we want j2_id1_idx to be preferred
INSERT INTO j2 values(1,2);
-- analyze j2;

-- explain (costs off) select * from j1
-- inner join j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2
-- where j1.id1 % 1000 = 1 and j2.id1 % 1000 = 1;

select * from j1
inner join j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2
where j1.id1 % 1000 = 1 and j2.id1 % 1000 = 1;

-- reset enable_nestloop;
-- reset enable_hashjoin;
-- reset enable_sort;

drop table j1;
drop table j2;
-- drop table j3;

-- Skip these tests because it only test explain
-- check that semijoin inner is not seen as unique for a portion of the outerrel
-- explain (verbose, costs off)
-- select t1.unique1, t2.hundred
-- from onek t1, tenk1 t2
-- where exists (select 1 from tenk1 t3
--               where t3.thousand = t1.unique1 and t3.tenthous = t2.hundred)
--       and t1.unique1 < 1;

-- ... unless it actually is unique
-- create table j3 as select unique1, tenthous from onek;
-- vacuum analyze j3;
-- create unique index on j3(unique1, tenthous);

-- explain (verbose, costs off)
-- select t1.unique1, t2.hundred
-- from onek t1, tenk1 t2
-- where exists (select 1 from j3
--               where j3.unique1 = t1.unique1 and j3.tenthous = t2.hundred)
--       and t1.unique1 < 1;

-- drop table j3;
