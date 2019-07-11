--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- SELECT
-- Test int8 64-bit integers.
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/select.sql
--
create or replace temporary view onek2 as select * from onek;
create or replace temporary view INT8_TBL as select * from values
  (cast(trim('  123   ') as bigint), cast(trim('  456') as bigint)),
  (cast(trim('123   ') as bigint),cast('4567890123456789' as bigint)),
  (cast('4567890123456789' as bigint),cast('123' as bigint)),
  (cast(+4567890123456789 as bigint),cast('4567890123456789' as bigint)),
  (cast('+4567890123456789' as bigint),cast('-4567890123456789' as bigint))
  as INT8_TBL(q1, q2);

-- btree index
-- awk '{if($1<10){print;}else{next;}}' onek.data | sort +0n -1
--
SELECT * FROM onek
   WHERE onek.unique1 < 10
   ORDER BY onek.unique1;

-- [SPARK-28010] Support ORDER BY ... USING syntax
--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 DESC;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY stringu1 ASC;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1d -2 +0nr -1
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 ASC, unique1 DESC;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1dr -2 +0n -1
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 DESC, unique1 ASC;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0nr -1 +1d -2
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 DESC, string4 ASC;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0n -1 +1dr -2
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 ASC, string4 DESC;

--
-- test partial btree indexes
--
-- As of 7.2, planner probably won't pick an indexscan without stats,
-- so ANALYZE first.  Also, we want to prevent it from picking a bitmapscan
-- followed by sort, because that could hide index ordering problems.
--
-- ANALYZE onek2;

-- SET enable_seqscan TO off;
-- SET enable_bitmapscan TO off;
-- SET enable_sort TO off;

--
-- awk '{if($1<10){print $0;}else{next;}}' onek.data | sort +0n -1
--
SELECT onek2.* FROM onek2 WHERE onek2.unique1 < 10;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
SELECT onek2.unique1, onek2.stringu1 FROM onek2
    WHERE onek2.unique1 < 20
    ORDER BY unique1 DESC;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
SELECT onek2.unique1, onek2.stringu1 FROM onek2
   WHERE onek2.unique1 > 980;

-- RESET enable_seqscan;
-- RESET enable_bitmapscan;
-- RESET enable_sort;

-- [SPARK-28329] SELECT INTO syntax
-- SELECT two, stringu1, ten, string4
--    INTO TABLE tmp
--    FROM onek;
CREATE TABLE tmp USING parquet AS
SELECT two, stringu1, ten, string4
FROM onek;

-- Skip the person table because there is a point data type that we don't support.
--
-- awk '{print $1,$2;}' person.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - emp.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=2){print $4,$5;}else{print;}}' - stud_emp.data
--
-- SELECT name, age FROM person*; ??? check if different
-- SELECT p.name, p.age FROM person* p;

--
-- awk '{print $1,$2;}' person.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - emp.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=1){print $4,$5;}else{print;}}' - stud_emp.data |
-- sort +1nr -2
--
-- SELECT p.name, p.age FROM person* p ORDER BY age DESC, name;

-- [SPARK-28330] Enhance query limit
--
-- Test some cases involving whole-row Var referencing a subquery
--
select foo.* from (select 1) as foo;
select foo.* from (select null) as foo;
select foo.* from (select 'xyzzy',1,null) as foo;

--
-- Test VALUES lists
--
select * from onek, values(147, 'RFAAAA'), (931, 'VJAAAA') as v (i, j)
    WHERE onek.unique1 = v.i and onek.stringu1 = v.j;

-- [SPARK-28296] Improved VALUES support
-- a more complex case
-- looks like we're coding lisp :-)
-- select * from onek,
--   (values ((select i from
--     (values(10000), (2), (389), (1000), (2000), ((select 10029))) as foo(i)
--     order by i asc limit 1))) bar (i)
--   where onek.unique1 = bar.i;

-- try VALUES in a subquery
-- select * from onek
--     where (unique1,ten) in (values (1,1), (20,0), (99,9), (17,99))
--     order by unique1;

-- VALUES is also legal as a standalone query or a set-operation member
VALUES (1,2), (3,4+4), (7,77.7);

VALUES (1,2), (3,4+4), (7,77.7)
UNION ALL
SELECT 2+2, 57
UNION ALL
TABLE int8_tbl;

--
-- Test ORDER BY options
--

CREATE OR REPLACE TEMPORARY VIEW foo AS
SELECT * FROM (values(42),(3),(10),(7),(null),(null),(1)) as foo (f1);

-- [SPARK-28333] NULLS FIRST for DESC and NULLS LAST for ASC
SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 ASC;	-- same thing
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

-- check if indexscans do the right things
-- CREATE INDEX fooi ON foo (f1);
-- SET enable_sort = false;

-- SELECT * FROM foo ORDER BY f1;
-- SELECT * FROM foo ORDER BY f1 NULLS FIRST;
-- SELECT * FROM foo ORDER BY f1 DESC;
-- SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

-- DROP INDEX fooi;
-- CREATE INDEX fooi ON foo (f1 DESC);

-- SELECT * FROM foo ORDER BY f1;
-- SELECT * FROM foo ORDER BY f1 NULLS FIRST;
-- SELECT * FROM foo ORDER BY f1 DESC;
-- SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

-- DROP INDEX fooi;
-- CREATE INDEX fooi ON foo (f1 DESC NULLS LAST);

-- SELECT * FROM foo ORDER BY f1;
-- SELECT * FROM foo ORDER BY f1 NULLS FIRST;
-- SELECT * FROM foo ORDER BY f1 DESC;
-- SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

--
-- Test planning of some cases with partial indexes
--

-- partial index is usable
-- explain (costs off)
-- select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
-- actually run the query with an analyze to use the partial index
-- explain (costs off, analyze on, timing off, summary off)
-- select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
-- explain (costs off)
-- select unique2 from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
select unique2 from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
-- partial index predicate implies clause, so no need for retest
-- explain (costs off)
-- select * from onek2 where unique2 = 11 and stringu1 < 'B';
select * from onek2 where unique2 = 11 and stringu1 < 'B';
-- explain (costs off)
-- select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
-- but if it's an update target, must retest anyway
-- explain (costs off)
-- select unique2 from onek2 where unique2 = 11 and stringu1 < 'B' for update;
-- select unique2 from onek2 where unique2 = 11 and stringu1 < 'B' for update;
-- partial index is not applicable
-- explain (costs off)
-- select unique2 from onek2 where unique2 = 11 and stringu1 < 'C';
select unique2 from onek2 where unique2 = 11 and stringu1 < 'C';
-- partial index implies clause, but bitmap scan must recheck predicate anyway
-- SET enable_indexscan TO off;
-- explain (costs off)
-- select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
-- RESET enable_indexscan;
-- check multi-index cases too
-- explain (costs off)
-- select unique1, unique2 from onek2
--   where (unique2 = 11 or unique1 = 0) and stringu1 < 'B';
select unique1, unique2 from onek2
  where (unique2 = 11 or unique1 = 0) and stringu1 < 'B';
-- explain (costs off)
-- select unique1, unique2 from onek2
--   where (unique2 = 11 and stringu1 < 'B') or unique1 = 0;
select unique1, unique2 from onek2
  where (unique2 = 11 and stringu1 < 'B') or unique1 = 0;

--
-- Test some corner cases that have been known to confuse the planner
--

-- ORDER BY on a constant doesn't really need any sorting
SELECT 1 AS x ORDER BY x;

-- But ORDER BY on a set-valued expression does
-- create function sillysrf(int) returns setof int as
--   'values (1),(10),(2),($1)' language sql immutable;

-- select sillysrf(42);
-- select sillysrf(-1) order by 1;

-- drop function sillysrf(int);

-- X = X isn't a no-op, it's effectively X IS NOT NULL assuming = is strict
-- (see bug #5084)
select * from (values (2),(null),(1)) v(k) where k = k order by k;
select * from (values (2),(null),(1)) v(k) where k = k;

-- Test partitioned tables with no partitions, which should be handled the
-- same as the non-inheritance case when expanding its RTE.
-- create table list_parted_tbl (a int,b int) partition by list (a);
-- create table list_parted_tbl1 partition of list_parted_tbl
--   for values in (1) partition by list(b);
-- explain (costs off) select * from list_parted_tbl;
-- drop table list_parted_tbl;
drop table tmp;
