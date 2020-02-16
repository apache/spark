-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
-- LIMIT
-- https://github.com/postgres/postgres/blob/REL_12_STABLE/src/test/regress/sql/limit.sql

SELECT '' AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 50
		ORDER BY unique1 LIMIT 2;
SELECT '' AS five, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 60
		ORDER BY unique1 LIMIT 5;
SELECT '' AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 60 AND unique1 < 63
		ORDER BY unique1 LIMIT 5;
-- [SPARK-28330] ANSI SQL: Top-level <result offset clause> in <query expression>
-- SELECT '' AS three, unique1, unique2, stringu1
-- 		FROM onek WHERE unique1 > 100
-- 		ORDER BY unique1 LIMIT 3 OFFSET 20;
-- SELECT '' AS zero, unique1, unique2, stringu1
-- 		FROM onek WHERE unique1 < 50
-- 		ORDER BY unique1 DESC LIMIT 8 OFFSET 99;
-- SELECT '' AS eleven, unique1, unique2, stringu1
-- 		FROM onek WHERE unique1 < 50
-- 		ORDER BY unique1 DESC LIMIT 20 OFFSET 39;
-- SELECT '' AS ten, unique1, unique2, stringu1
-- 		FROM onek
-- 		ORDER BY unique1 OFFSET 990;
-- SELECT '' AS five, unique1, unique2, stringu1
-- 		FROM onek
-- 		ORDER BY unique1 OFFSET 990 LIMIT 5;
-- SELECT '' AS five, unique1, unique2, stringu1
-- 		FROM onek
-- 		ORDER BY unique1 LIMIT 5 OFFSET 900;

CREATE OR REPLACE TEMPORARY VIEW INT8_TBL AS SELECT * FROM
  (VALUES
    (123, 456),
    (123, 4567890123456789),
    (4567890123456789, 123),
    (4567890123456789, 4567890123456789),
    (4567890123456789, -4567890123456789))
  AS v(q1, q2);

-- Test null limit and offset.  The planner would discard a simple null
-- constant, so to ensure executor is exercised, do this:
-- [SPARK-29650] Discard a NULL constant in LIMIT
select * from int8_tbl limit (case when random() < 0.5 then bigint(null) end);
-- [SPARK-28330] ANSI SQL: Top-level <result offset clause> in <query expression>
-- select * from int8_tbl offset (case when random() < 0.5 then bigint(null) end);

-- Test assorted cases involving backwards fetch from a LIMIT plan node
-- [SPARK-20965] Support PREPARE/EXECUTE/DECLARE/FETCH statements
-- begin;
--
-- declare c1 cursor for select * from int8_tbl limit 10;
-- fetch all in c1;
-- fetch 1 in c1;
-- fetch backward 1 in c1;
-- fetch backward all in c1;
-- fetch backward 1 in c1;
-- fetch all in c1;
--
-- declare c2 cursor for select * from int8_tbl limit 3;
-- fetch all in c2;
-- fetch 1 in c2;
-- fetch backward 1 in c2;
-- fetch backward all in c2;
-- fetch backward 1 in c2;
-- fetch all in c2;
--
-- declare c3 cursor for select * from int8_tbl offset 3;
-- fetch all in c3;
-- fetch 1 in c3;
-- fetch backward 1 in c3;
-- fetch backward all in c3;
-- fetch backward 1 in c3;
-- fetch all in c3;
--
-- declare c4 cursor for select * from int8_tbl offset 10;
-- fetch all in c4;
-- fetch 1 in c4;
-- fetch backward 1 in c4;
-- fetch backward all in c4;
-- fetch backward 1 in c4;
-- fetch all in c4;
--
-- rollback;

DROP VIEW INT8_TBL;

-- Stress test for variable LIMIT in conjunction with bounded-heap sorting

-- [SPARK-28330] ANSI SQL: Top-level <result offset clause> in <query expression>
-- SELECT
--   (SELECT n
--      FROM (VALUES (1)) AS x,
--           (SELECT n FROM generate_series(1,10) AS n
--              ORDER BY n LIMIT 1 OFFSET s-1) AS y) AS z
--   FROM generate_series(1,10) AS s;

--
-- Test behavior of volatile and set-returning functions in conjunction
-- with ORDER BY and LIMIT.
--

-- [SPARK-29631] Support ANSI SQL CREATE SEQUENCE
-- create temp sequence testseq;

-- explain (verbose, costs off)
-- select unique1, unique2, nextval('testseq')
--   from tenk1 order by unique2 limit 10;

-- select unique1, unique2, nextval('testseq')
--   from tenk1 order by unique2 limit 10;

-- select currval('testseq');

-- explain (verbose, costs off)
-- select unique1, unique2, nextval('testseq')
--   from tenk1 order by tenthous limit 10;

-- select unique1, unique2, nextval('testseq')
--   from tenk1 order by tenthous limit 10;

-- select currval('testseq');

-- explain (verbose, costs off)
-- select unique1, unique2, generate_series(1,10)
--   from tenk1 order by unique2 limit 7;

-- [SPARK-27767] Built-in function: generate_series
-- select unique1, unique2, generate_series(1,10)
--   from tenk1 order by unique2 limit 7;

-- explain (verbose, costs off)
-- select unique1, unique2, generate_series(1,10)
--   from tenk1 order by tenthous limit 7;

-- [SPARK-27767] Built-in function: generate_series
-- select unique1, unique2, generate_series(1,10)
--   from tenk1 order by tenthous limit 7;

-- use of random() is to keep planner from folding the expressions together
-- explain (verbose, costs off)
-- select generate_series(0,2) as s1, generate_series((random()*.1)::int,2) as s2;

-- [SPARK-27767] Built-in function: generate_series
-- select generate_series(0,2) as s1, generate_series((random()*.1)::int,2) as s2;

-- explain (verbose, costs off)
-- select generate_series(0,2) as s1, generate_series((random()*.1)::int,2) as s2
-- order by s2 desc;

-- [SPARK-27767] Built-in function: generate_series
-- select generate_series(0,2) as s1, generate_series((random()*.1)::int,2) as s2
-- order by s2 desc;

-- test for failure to set all aggregates' aggtranstype
-- explain (verbose, costs off)
-- select sum(tenthous) as s1, sum(tenthous) + random()*0 as s2
--   from tenk1 group by thousand order by thousand limit 3;

select sum(tenthous) as s1, sum(tenthous) + random()*0 as s2
  from tenk1 group by thousand order by thousand limit 3;
