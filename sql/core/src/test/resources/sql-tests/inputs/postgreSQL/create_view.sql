-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
-- CREATE VIEW
-- https://github.com/postgres/postgres/blob/REL_12_STABLE/src/test/regress/sql/create_view.sql

-- [SPARK-27764] Support geometric types
-- CREATE VIEW street AS
--    SELECT r.name, r.thepath, c.cname AS cname
--    FROM ONLY road r, real_city c
--    WHERE c.outline ## r.thepath;

-- [SPARK-27764] Support geometric types
-- CREATE VIEW iexit AS
--    SELECT ih.name, ih.thepath,
-- 	interpt_pp(ih.thepath, r.thepath) AS exit
--    FROM ihighway ih, ramp r
--    WHERE ih.thepath ## r.thepath;

CREATE TABLE emp (
  name string,
  age int,
  -- [SPARK-27764] Support geometric types
  -- location point
  salary int,
  manager string
) USING parquet;

CREATE VIEW toyemp AS
   SELECT name, age, /* location ,*/ 12*salary AS annualsal
   FROM emp;

-- [SPARK-29659] Support COMMENT ON syntax
-- Test comments
-- COMMENT ON VIEW noview IS 'no view';
-- COMMENT ON VIEW toyemp IS 'is a view';
-- COMMENT ON VIEW toyemp IS NULL;

DROP VIEW toyemp;
DROP TABLE emp;

-- These views are left around mainly to exercise special cases in pg_dump.

-- [SPARK-19842] Informational Referential Integrity Constraints Support in Spark
CREATE TABLE view_base_table (key int /* PRIMARY KEY */, data varchar(20)) USING PARQUET;
--
CREATE VIEW key_dependent_view AS
   SELECT * FROM view_base_table GROUP BY key;
--
-- [SPARK-19842] Informational Referential Integrity Constraints Support in Spark
-- ALTER TABLE view_base_table DROP CONSTRAINT view_base_table_pkey;  -- fails

CREATE VIEW key_dependent_view_no_cols AS
   SELECT FROM view_base_table GROUP BY key HAVING length(data) > 0;

--
-- CREATE OR REPLACE VIEW
--

CREATE TABLE viewtest_tbl (a int, b int) using parquet;
-- [SPARK-29386] Copy data between a file and a table
-- COPY viewtest_tbl FROM stdin;
-- 5	10
-- 10	15
-- 15	20
-- 20	25
-- \.
INSERT INTO viewtest_tbl VALUES (5, 10), (10, 15), (15, 20), (20, 25);

CREATE OR REPLACE VIEW viewtest AS
	SELECT * FROM viewtest_tbl;

CREATE OR REPLACE VIEW viewtest AS
	SELECT * FROM viewtest_tbl WHERE a > 10;

SELECT * FROM viewtest;

CREATE OR REPLACE VIEW viewtest AS
	SELECT a, b FROM viewtest_tbl WHERE a > 5 ORDER BY b DESC;

SELECT * FROM viewtest;

-- should fail
-- [SPARK-29660] Dropping columns and changing column names/types are prohibited in VIEW definition
CREATE OR REPLACE VIEW viewtest AS
	SELECT a FROM viewtest_tbl WHERE a <> 20;

-- should fail
-- [SPARK-29660] Dropping columns and changing column names/types are prohibited in VIEW definition
CREATE OR REPLACE VIEW viewtest AS
	SELECT 1, * FROM viewtest_tbl;

-- should fail
-- [SPARK-29660] Dropping columns and changing column names/types are prohibited in VIEW definition
CREATE OR REPLACE VIEW viewtest AS
	SELECT a, decimal(b) FROM viewtest_tbl;

-- should work
CREATE OR REPLACE VIEW viewtest AS
	SELECT a, b, 0 AS c FROM viewtest_tbl;

DROP VIEW viewtest;
DROP TABLE viewtest_tbl;

-- tests for temporary views

-- [SPARK-29661] Support cascaded syntax in CREATE SCHEMA
-- CREATE SCHEMA temp_view_test
--     CREATE TABLE base_table (a int, id int) using parquet
--     CREATE TABLE base_table2 (a int, id int) using parquet;
CREATE SCHEMA temp_view_test;
CREATE TABLE temp_view_test.base_table (a int, id int) using parquet;
CREATE TABLE temp_view_test.base_table2 (a int, id int) using parquet;

-- Replace SET with USE
-- SET search_path TO temp_view_test, public;
USE temp_view_test;

-- Since Spark doesn't support CREATE TEMPORARY TABLE, we used CREATE TEMPORARY VIEW instead
-- CREATE TEMPORARY TABLE temp_table (a int, id int);
CREATE TEMPORARY VIEW temp_table AS SELECT * FROM VALUES
  (1, 1) as temp_table(a, id);

-- should be created in temp_view_test schema
CREATE VIEW v1 AS SELECT * FROM base_table;
DESC TABLE EXTENDED v1;
-- should be created in temp object schema
-- [SPARK-29628] Forcibly create a temporary view in CREATE VIEW if referencing a temporary view
CREATE VIEW v1_temp AS SELECT * FROM temp_table;
-- should be created in temp object schema
CREATE TEMP VIEW v2_temp AS SELECT * FROM base_table;
DESC TABLE EXTENDED v2_temp;
-- should be created in temp_views schema
CREATE VIEW temp_view_test.v2 AS SELECT * FROM base_table;
DESC TABLE EXTENDED temp_view_test.v2;
-- should fail
-- [SPARK-29628] Forcibly create a temporary view in CREATE VIEW if referencing a temporary view
CREATE VIEW temp_view_test.v3_temp AS SELECT * FROM temp_table;
-- should fail
-- [SPARK-29661] Support cascaded syntax in CREATE SCHEMA
-- CREATE SCHEMA test_view_schema
--     CREATE TEMP VIEW testview AS SELECT 1;

-- joins: if any of the join relations are temporary, the view
-- should also be temporary

-- should be non-temp
CREATE VIEW v3 AS
    SELECT t1.a AS t1_a, t2.a AS t2_a
    FROM base_table t1, base_table2 t2
    WHERE t1.id = t2.id;
DESC TABLE EXTENDED v3;
-- should be temp (one join rel is temp)
-- [SPARK-29628] Forcibly create a temporary view in CREATE VIEW if referencing a temporary view
CREATE VIEW v4_temp AS
    SELECT t1.a AS t1_a, t2.a AS t2_a
    FROM base_table t1, temp_table t2
    WHERE t1.id = t2.id;
-- should be temp
-- [SPARK-29628] Forcibly create a temporary view in CREATE VIEW if referencing a temporary view
CREATE VIEW v5_temp AS
    SELECT t1.a AS t1_a, t2.a AS t2_a, t3.a AS t3_a
    FROM base_table t1, base_table2 t2, temp_table t3
    WHERE t1.id = t2.id and t2.id = t3.id;

-- subqueries
CREATE VIEW v4 AS SELECT * FROM base_table WHERE id IN (SELECT id FROM base_table2);
DESC TABLE EXTENDED v4;
CREATE VIEW v5 AS SELECT t1.id, t2.a FROM base_table t1, (SELECT * FROM base_table2) t2;
DESC TABLE EXTENDED v5;
CREATE VIEW v6 AS SELECT * FROM base_table WHERE EXISTS (SELECT 1 FROM base_table2);
DESC TABLE EXTENDED v6;
CREATE VIEW v7 AS SELECT * FROM base_table WHERE NOT EXISTS (SELECT 1 FROM base_table2);
DESC TABLE EXTENDED v7;
CREATE VIEW v8 AS SELECT * FROM base_table WHERE EXISTS (SELECT 1);
DESC TABLE EXTENDED v8;

-- [SPARK-29628] Forcibly create a temporary view in CREATE VIEW if referencing a temporary view
CREATE VIEW v6_temp AS SELECT * FROM base_table WHERE id IN (SELECT id FROM temp_table);
CREATE VIEW v7_temp AS SELECT t1.id, t2.a FROM base_table t1, (SELECT * FROM temp_table) t2;
CREATE VIEW v8_temp AS SELECT * FROM base_table WHERE EXISTS (SELECT 1 FROM temp_table);
CREATE VIEW v9_temp AS SELECT * FROM base_table WHERE NOT EXISTS (SELECT 1 FROM temp_table);

-- a view should also be temporary if it references a temporary view
-- [SPARK-29628] Forcibly create a temporary view in CREATE VIEW if referencing a temporary view
CREATE VIEW v10_temp AS SELECT * FROM v7_temp;
CREATE VIEW v11_temp AS SELECT t1.id, t2.a FROM base_table t1, v10_temp t2;
CREATE VIEW v12_temp AS SELECT true FROM v11_temp;

-- [SPARK-27764] Support ANSI SQL CREATE SEQUENCE
-- a view should also be temporary if it references a temporary sequence
-- CREATE SEQUENCE seq1;
-- CREATE TEMPORARY SEQUENCE seq1_temp;
-- CREATE VIEW v9 AS SELECT seq1.is_called FROM seq1;
-- CREATE VIEW v13_temp AS SELECT seq1_temp.is_called FROM seq1_temp;

-- Skip the tests below because of PostgreSQL specific cases
-- SELECT relname FROM pg_class
--     WHERE relname LIKE 'v_'
--     AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'temp_view_test')
--     ORDER BY relname;
-- SELECT relname FROM pg_class
--     WHERE relname LIKE 'v%'
--     AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_temp%')
--     ORDER BY relname;

CREATE SCHEMA testviewschm2;
-- Replace SET with USE
-- SET search_path TO testviewschm2, public;
USE testviewschm2;

CREATE TABLE t1 (num int, name string) using parquet;
CREATE TABLE t2 (num2 int, value string) using parquet;
-- Since Spark doesn't support CREATE TEMPORARY TABLE, we used CREATE TEMPORARY VIEW instead
-- CREATE TEMP TABLE tt (num2 int, value string);
CREATE TEMP VIEW tt AS SELECT * FROM VALUES
  (1, 'a') AS tt(num2, value);

CREATE VIEW nontemp1 AS SELECT * FROM t1 CROSS JOIN t2;
DESC TABLE EXTENDED nontemp1;
-- [SPARK-29628] Forcibly create a temporary view in CREATE VIEW if referencing a temporary view
CREATE VIEW temporal1 AS SELECT * FROM t1 CROSS JOIN tt;
CREATE VIEW nontemp2 AS SELECT * FROM t1 INNER JOIN t2 ON t1.num = t2.num2;
DESC TABLE EXTENDED nontemp2;
-- [SPARK-29628] Forcibly create a temporary view in CREATE VIEW if referencing a temporary view
CREATE VIEW temporal2 AS SELECT * FROM t1 INNER JOIN tt ON t1.num = tt.num2;
CREATE VIEW nontemp3 AS SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num2;
DESC TABLE EXTENDED nontemp3;
-- [SPARK-29628] Forcibly create a temporary view in CREATE VIEW if referencing a temporary view
CREATE VIEW temporal3 AS SELECT * FROM t1 LEFT JOIN tt ON t1.num = tt.num2;
CREATE VIEW nontemp4 AS SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num2 AND t2.value = 'xxx';
DESC TABLE EXTENDED nontemp4;
-- [SPARK-29628] Forcibly create a temporary view in CREATE VIEW if referencing a temporary view
CREATE VIEW temporal4 AS SELECT * FROM t1 LEFT JOIN tt ON t1.num = tt.num2 AND tt.value = 'xxx';
CREATE VIEW temporal5 AS SELECT * FROM t1 WHERE num IN (SELECT num FROM t1 WHERE EXISTS (SELECT 1 FROM tt));

-- Skip the tests below because of PostgreSQL specific cases
-- SELECT relname FROM pg_class
--     WHERE relname LIKE 'nontemp%'
--     AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'testviewschm2')
--     ORDER BY relname;
-- SELECT relname FROM pg_class
--     WHERE relname LIKE 'temporal%'
--     AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_temp%')
--     ORDER BY relname;

CREATE TABLE tbl1 ( a int, b int) using parquet;
CREATE TABLE tbl2 (c int, d int) using parquet;
CREATE TABLE tbl3 (e int, f int) using parquet;
CREATE TABLE tbl4 (g int, h int) using parquet;
-- Since Spark doesn't support CREATE TEMPORARY TABLE, we used CREATE TABLE instead
-- CREATE TEMP TABLE tmptbl (i int, j int);
CREATE TABLE tmptbl (i int, j int) using parquet;
INSERT INTO tmptbl VALUES (1, 1);

--Should be in testviewschm2
CREATE   VIEW  pubview AS SELECT * FROM tbl1 WHERE tbl1.a
BETWEEN (SELECT d FROM tbl2 WHERE c = 1) AND (SELECT e FROM tbl3 WHERE f = 2)
AND EXISTS (SELECT g FROM tbl4 LEFT JOIN tbl3 ON tbl4.h = tbl3.f);
DESC TABLE EXTENDED pubview;

-- Skip the test below because of PostgreSQL specific cases
-- SELECT count(*) FROM pg_class where relname = 'pubview'
-- AND relnamespace IN (SELECT OID FROM pg_namespace WHERE nspname = 'testviewschm2');

--Should be in temp object schema
CREATE   VIEW  mytempview AS SELECT * FROM tbl1 WHERE tbl1.a
BETWEEN (SELECT d FROM tbl2 WHERE c = 1) AND (SELECT e FROM tbl3 WHERE f = 2)
AND EXISTS (SELECT g FROM tbl4 LEFT JOIN tbl3 ON tbl4.h = tbl3.f)
AND NOT EXISTS (SELECT g FROM tbl4 LEFT JOIN tmptbl ON tbl4.h = tmptbl.j);
DESC TABLE EXTENDED mytempview;

-- Skip the test below because of PostgreSQL specific cases
-- SELECT count(*) FROM pg_class where relname LIKE 'mytempview'
-- And relnamespace IN (SELECT OID FROM pg_namespace WHERE nspname LIKE 'pg_temp%');

--
-- CREATE VIEW and WITH(...) clause
-- CREATE VIEW mysecview1
--        AS SELECT * FROM tbl1 WHERE a = 0;
--
-- Skip the tests below because Spark doesn't support `WITH options`
-- CREATE VIEW mysecview2 WITH (security_barrier=true)
--        AS SELECT * FROM tbl1 WHERE a > 0;
-- CREATE VIEW mysecview3 WITH (security_barrier=false)
--        AS SELECT * FROM tbl1 WHERE a < 0;
-- CREATE VIEW mysecview4 WITH (security_barrier)
--        AS SELECT * FROM tbl1 WHERE a <> 0;
-- Spark cannot support options in WITH clause
-- CREATE VIEW mysecview5 WITH (security_barrier=100)	-- Error
--        AS SELECT * FROM tbl1 WHERE a > 100;
-- CREATE VIEW mysecview6 WITH (invalid_option)		-- Error
--        AS SELECT * FROM tbl1 WHERE a < 100;
-- Skip the test below because of PostgreSQL specific cases
-- SELECT relname, relkind, reloptions FROM pg_class
--        WHERE oid in ('mysecview1'::regclass, 'mysecview2'::regclass,
--                      'mysecview3'::regclass, 'mysecview4'::regclass)
--        ORDER BY relname;

-- CREATE OR REPLACE VIEW mysecview1
--        AS SELECT * FROM tbl1 WHERE a = 256;
-- CREATE OR REPLACE VIEW mysecview2
--        AS SELECT * FROM tbl1 WHERE a > 256;
-- CREATE OR REPLACE VIEW mysecview3 WITH (security_barrier=true)
--        AS SELECT * FROM tbl1 WHERE a < 256;
-- CREATE OR REPLACE VIEW mysecview4 WITH (security_barrier=false)
--        AS SELECT * FROM tbl1 WHERE a <> 256;
-- Skip the test below because of PostgreSQL specific cases
-- SELECT relname, relkind, reloptions FROM pg_class
--        WHERE oid in ('mysecview1'::regclass, 'mysecview2'::regclass,
--                      'mysecview3'::regclass, 'mysecview4'::regclass)
--        ORDER BY relname;

-- Check that unknown literals are converted to "text" in CREATE VIEW,
-- so that we don't end up with unknown-type columns.

-- Skip the tests below because of PostgreSQL specific cases
-- CREATE VIEW unspecified_types AS
--   SELECT 42 as i, 42.5 as num, 'foo' as u, 'foo'::unknown as u2, null as n;
-- \d+ unspecified_types
-- SELECT * FROM unspecified_types;

-- This test checks that proper typmods are assigned in a multi-row VALUES

CREATE VIEW tt1 AS
  SELECT * FROM (
    VALUES
       ('abc', '0123456789', 42, 'abcd'),
       ('0123456789', 'abc', 42.12, 'abc')
  ) vv(a,b,c,d);
-- Replace the PostgreSQL meta command `\d` with `DESC`
-- \d+ tt1
SELECT * FROM tt1;
SELECT string(a) FROM tt1;
DROP VIEW tt1;

-- Test view decompilation in the face of relation renaming conflicts

CREATE TABLE tt1 (f1 int, f2 int, f3 string) using parquet;
CREATE TABLE tx1 (x1 int, x2 int, x3 string) using parquet;
CREATE TABLE temp_view_test.tt1 (y1 int, f2 int, f3 string) using parquet;

CREATE VIEW aliased_view_1 AS
  select * from tt1
    where exists (select 1 from tx1 where tt1.f1 = tx1.x1);
CREATE VIEW aliased_view_2 AS
  select * from tt1 a1
    where exists (select 1 from tx1 where a1.f1 = tx1.x1);
CREATE VIEW aliased_view_3 AS
  select * from tt1
    where exists (select 1 from tx1 a2 where tt1.f1 = a2.x1);
CREATE VIEW aliased_view_4 AS
  select * from temp_view_test.tt1
    where exists (select 1 from tt1 where temp_view_test.tt1.y1 = tt1.f1);

-- Replace the PostgreSQL meta command `\d` with `DESC`
-- \d+ aliased_view_1
DESC TABLE aliased_view_1;
-- \d+ aliased_view_2
DESC TABLE aliased_view_2;
-- \d+ aliased_view_3
DESC TABLE aliased_view_3;
-- \d+ aliased_view_4
DESC TABLE aliased_view_4;

ALTER TABLE tx1 RENAME TO a1;

-- Replace the PostgreSQL meta command `\d` with `DESC`
-- \d+ aliased_view_1
DESC TABLE aliased_view_1;
-- \d+ aliased_view_2
DESC TABLE aliased_view_2;
-- \d+ aliased_view_3
DESC TABLE aliased_view_3;
-- \d+ aliased_view_4
DESC TABLE aliased_view_4;

ALTER TABLE tt1 RENAME TO a2;

-- Replace the PostgreSQL meta command `\d` with `DESC`
-- \d+ aliased_view_1
DESC TABLE aliased_view_1;
-- \d+ aliased_view_2
DESC TABLE aliased_view_2;
-- \d+ aliased_view_3
DESC TABLE aliased_view_3;
-- \d+ aliased_view_4
DESC TABLE aliased_view_4;

ALTER TABLE a1 RENAME TO tt1;

-- Replace the PostgreSQL meta command `\d` with `DESC`
-- \d+ aliased_view_1
DESC TABLE aliased_view_1;
-- \d+ aliased_view_2
DESC TABLE aliased_view_2;
-- \d+ aliased_view_3
DESC TABLE aliased_view_3;
-- \d+ aliased_view_4
DESC TABLE aliased_view_4;

ALTER TABLE a2 RENAME TO tx1;
-- [SPARK-29632] Support ALTER TABLE [relname] SET SCHEMA [dbname]
-- ALTER TABLE tx1 SET SCHEMA temp_view_test;

-- \d+ aliased_view_1
-- \d+ aliased_view_2
-- \d+ aliased_view_3
-- \d+ aliased_view_4

-- [SPARK-29632] Support ALTER TABLE [relname] SET SCHEMA [dbname]
-- ALTER TABLE temp_view_test.tt1 RENAME TO tmp1;
-- ALTER TABLE temp_view_test.tmp1 SET SCHEMA testviewschm2;
-- ALTER TABLE tmp1 RENAME TO tx1;

-- Replace the PostgreSQL meta command `\d` with `DESC`
-- \d+ aliased_view_1
-- \d+ aliased_view_2
-- \d+ aliased_view_3
-- \d+ aliased_view_4

-- Test aliasing of joins

create view view_of_joins as
select * from
  (select * from (tbl1 cross join tbl2) same) ss,
  (tbl3 cross join tbl4) same;

-- Replace the PostgreSQL meta command `\d` with `DESC`
-- \d+ view_of_joins

-- Test view decompilation in the face of column addition/deletion/renaming

create table tt2 (a int, b int, c int) using parquet;
create table tt3 (ax bigint, b short, c decimal) using parquet;
create table tt4 (ay int, b int, q int) using parquet;

create view v1 as select * from tt2 natural join tt3;
create view v1a as select * from (tt2 natural join tt3) j;
create view v2 as select * from tt2 join tt3 using (b,c) join tt4 using (b);
create view v2a as select * from (tt2 join tt3 using (b,c) join tt4 using (b)) j;
create view v3 as select * from tt2 join tt3 using (b,c) full join tt4 using (b);

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('v1', true);
DESC TABLE v1;
-- select pg_get_viewdef('v1a', true);
DESC TABLE v1a;
-- select pg_get_viewdef('v2', true);
DESC TABLE v2;
-- select pg_get_viewdef('v2a', true);
DESC TABLE v2a;
-- select pg_get_viewdef('v3', true);
DESC TABLE v3;

alter table tt2 add column d int;
alter table tt2 add column e int;

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('v1', true);
DESC TABLE v1;
-- select pg_get_viewdef('v1a', true);
DESC TABLE v1a;
-- select pg_get_viewdef('v2', true);
DESC TABLE v2;
-- select pg_get_viewdef('v2a', true);
DESC TABLE v2a;
-- select pg_get_viewdef('v3', true);
DESC TABLE v3;

-- [SPARK-27764] Make COLUMN optional in ALTER TABLE
-- [SPARK-27589] Spark file source V2 (For supporting RENAME COLUMN in ALTER TABLE)
-- alter table tt3 rename c to d;
drop table tt3;
create table tt3 (ax bigint, b short, d decimal) using parquet;

-- select pg_get_viewdef('v1', true);
-- select pg_get_viewdef('v1a', true);
-- select pg_get_viewdef('v2', true);
-- select pg_get_viewdef('v2a', true);
-- select pg_get_viewdef('v3', true);

alter table tt3 add column c int;
alter table tt3 add column e int;

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('v1', true);
DESC TABLE v1;
-- select pg_get_viewdef('v1a', true);
DESC TABLE v1a;
-- select pg_get_viewdef('v2', true);
DESC TABLE v2;
-- select pg_get_viewdef('v2a', true);
DESC TABLE v2a;
-- select pg_get_viewdef('v3', true);
DESC TABLE v3;

-- [SPARK-27589] Spark file source V2 (For supporting DROP COLUMN in ALTER TABLE)
-- alter table tt2 drop column d;

-- select pg_get_viewdef('v1', true);
-- select pg_get_viewdef('v1a', true);
-- select pg_get_viewdef('v2', true);
-- select pg_get_viewdef('v2a', true);
-- select pg_get_viewdef('v3', true);

create table tt5 (a int, b int) using parquet;
create table tt6 (c int, d int) using parquet;
create view vv1 as select * from (tt5 cross join tt6) j(aa,bb,cc,dd);
-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('vv1', true);
DESC TABLE vv1;
alter table tt5 add column c int;
-- select pg_get_viewdef('vv1', true);
DESC TABLE vv1;
alter table tt5 add column cc int;
-- select pg_get_viewdef('vv1', true);
DESC TABLE vv1;
-- [SPARK-27589] Spark file source V2 (For supporting DROP COLUMN in ALTER TABLE)
-- alter table tt5 drop column c;
-- select pg_get_viewdef('vv1', true);

-- Unnamed FULL JOIN USING is lots of fun too

-- [SPARK-27589] Spark file source V2 (For supporting DROP COLUMN in ALTER TABLE)
create table tt7 (x int, /* xx int, */ y int) using parquet;
-- alter table tt7 drop column xx;
create table tt8 (x int, z int) using parquet;

create view vv2 as
select * from (values(1,2,3,4,5)) v(a,b,c,d,e)
union all
select * from tt7 full join tt8 using (x), tt8 tt8x;

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('vv2', true);
DESC TABLE vv2;

create view vv3 as
select * from (values(1,2,3,4,5,6)) v(a,b,c,x,e,f)
union all
select * from
  tt7 full join tt8 using (x),
  tt7 tt7x full join tt8 tt8x using (x);

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('vv3', true);
DESC TABLE vv3;

create view vv4 as
select * from (values(1,2,3,4,5,6,7)) v(a,b,c,x,e,f,g)
union all
select * from
  tt7 full join tt8 using (x),
  tt7 tt7x full join tt8 tt8x using (x) full join tt8 tt8y using (x);

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('vv4', true);
DESC TABLE vv4;

alter table tt7 add column zz int;
alter table tt7 add column z int;
-- [SPARK-27589] Spark file source V2 (For supporting DROP COLUMN in ALTER TABLE)
-- alter table tt7 drop column zz;
alter table tt8 add column z2 int;

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('vv2', true);
DESC TABLE vv2;
-- select pg_get_viewdef('vv3', true);
DESC TABLE vv3;
-- select pg_get_viewdef('vv4', true);
DESC TABLE vv4;

-- Implicit coercions in a JOIN USING create issues similar to FULL JOIN

-- [SPARK-27589] Spark file source V2 (For supporting DROP COLUMN in ALTER TABLE)
create table tt7a (x date, /* xx int, */ y int) using parquet;
-- alter table tt7a drop column xx;
create table tt8a (x timestamp, z int) using parquet;

-- To pass the query, added exact column names in the select stmt
create view vv2a as
select * from (values(now(),2,3,now(),5)) v(a,b,c,d,e)
union all
select * from tt7a left join tt8a using (x), tt8a tt8ax;

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('vv4', true);
DESC TABLE vv4;
-- select pg_get_viewdef('vv2a', true);
DESC TABLE vv2a;

--
-- Also check dropping a column that existed when the view was made
--

create table tt9 (x int, xx int, y int) using parquet;
create table tt10 (x int, z int) using parquet;

create view vv5 as select x,y,z from tt9 join tt10 using(x);

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('vv5', true);
DESC TABLE vv5;

-- [SPARK-27589] Spark file source V2 (For supporting DROP COLUMN in ALTER TABLE)
-- alter table tt9 drop column xx;

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('vv5', true);
DESC TABLE vv5;

--
-- Another corner case is that we might add a column to a table below a
-- JOIN USING, and thereby make the USING column name ambiguous
--

create table tt11 (x int, y int) using parquet;
create table tt12 (x int, z int) using parquet;
create table tt13 (z int, q int) using parquet;

create view vv6 as select x,y,z,q from
  (tt11 join tt12 using(x)) join tt13 using(z);

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('vv6', true);
DESC TABLE vv6;

alter table tt11 add column z int;

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('vv6', true);
DESC TABLE vv6;

--
-- Check cases involving dropped/altered columns in a function's rowtype result
--

-- Skip the tests below because Spark doesn't support PostgreSQL-specific UDFs/transactions
-- create table tt14t (f1 text, f2 text, f3 text, f4 text);
-- insert into tt14t values('foo', 'bar', 'baz', '42');
--
-- alter table tt14t drop column f2;
--
-- create function tt14f() returns setof tt14t as
-- $$
-- declare
--     rec1 record;
-- begin
--     for rec1 in select * from tt14t
--     loop
--         return next rec1;
--     end loop;
-- end;
-- $$
-- language plpgsql;
--
-- create view tt14v as select t.* from tt14f() t;
--
-- select pg_get_viewdef('tt14v', true);
-- select * from tt14v;
--
-- begin;
--
-- -- this perhaps should be rejected, but it isn't:
-- alter table tt14t drop column f3;
--
-- -- f3 is still in the view ...
-- select pg_get_viewdef('tt14v', true);
-- -- but will fail at execution
-- select f1, f4 from tt14v;
-- select * from tt14v;
--
-- rollback;
--
-- begin;
--
-- -- this perhaps should be rejected, but it isn't:
-- alter table tt14t alter column f4 type integer using f4::integer;
--
-- -- f4 is still in the view ...
-- select pg_get_viewdef('tt14v', true);
-- -- but will fail at execution
-- select f1, f3 from tt14v;
-- select * from tt14v;
--
-- rollback;

-- check display of whole-row variables in some corner cases

-- Skip the tests below because we do not support creating types
-- create type nestedcomposite as (x int8_tbl);
-- create view tt15v as select row(i)::nestedcomposite from int8_tbl i;
-- select * from tt15v;
-- select pg_get_viewdef('tt15v', true);
-- select row(i.*::int8_tbl)::nestedcomposite from int8_tbl i;
--
-- create view tt16v as select * from int8_tbl i, lateral(values(i)) ss;
-- select * from tt16v;
-- select pg_get_viewdef('tt16v', true);
-- select * from int8_tbl i, lateral(values(i.*::int8_tbl)) ss;
--
-- create view tt17v as select * from int8_tbl i where i in (values(i));
-- select * from tt17v;
-- select pg_get_viewdef('tt17v', true);
-- select * from int8_tbl i where i.* in (values(i.*::int8_tbl));

-- check unique-ification of overlength names

CREATE TABLE int8_tbl (q1 int, q2 int) USING parquet;

create view tt18v as
  select * from int8_tbl xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxy
  union all
  select * from int8_tbl xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxz;
-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('tt18v', true);
DESC TABLE tt18v;
-- explain (costs off) select * from tt18v;

-- check display of ScalarArrayOp with a sub-select

-- Skip the tests below because of PostgreSQL specific cases
-- select 'foo'::text = any(array['abc','def','foo']::text[]);
-- select 'foo'::text = any((select array['abc','def','foo']::text[]));  -- fail
-- select 'foo'::text = any((select array['abc','def','foo']::text[])::text[]);
--
-- create view tt19v as
-- select 'foo'::text = any(array['abc','def','foo']::text[]) c1,
--        'foo'::text = any((select array['abc','def','foo']::text[])::text[]) c2;
-- select pg_get_viewdef('tt19v', true);

-- check display of assorted RTE_FUNCTION expressions

-- [SPARK-28682] ANSI SQL: Collation Support
-- create view tt20v as
-- select * from
--   coalesce(1,2) as c,
--   collation for ('x'::text) col,
--   current_date as d,
--   localtimestamp(3) as t,
--   cast(1+2 as int4) as i4,
--   cast(1+2 as int8) as i8;
-- select pg_get_viewdef('tt20v', true);

-- corner cases with empty join conditions

create view tt21v as
select * from tt5 natural inner join tt6;
-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('tt21v', true);
DESC TABLE tt21v;

create view tt22v as
select * from tt5 natural left join tt6;
-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('tt22v', true);
DESC TABLE tt22v;

-- check handling of views with immediately-renamed columns

create view tt23v (col_a, col_b) as
select q1 as other_name1, q2 as other_name2 from int8_tbl
union
select 42, 43;

-- Replace `pg_get_viewdef` with `DESC`
-- select pg_get_viewdef('tt23v', true);
DESC TABLE tt23v;
-- Skip the test below because of PostgreSQL specific cases
-- select pg_get_ruledef(oid, true) from pg_rewrite
--   where ev_class = 'tt23v'::regclass and ev_type = '1';

-- clean up all the random objects we made above
DROP SCHEMA temp_view_test CASCADE;
DROP SCHEMA testviewschm2 CASCADE;

DROP VIEW temp_table;
DROP VIEW tt;
