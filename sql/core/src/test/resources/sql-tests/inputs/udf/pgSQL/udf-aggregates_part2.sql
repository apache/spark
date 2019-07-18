--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- AGGREGATES [Part 2]
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/aggregates.sql#L145-L350
--
-- This test file was converted from pgSQL/aggregates_part2.sql.

create temporary view int4_tbl as select * from values
  (0),
  (123456),
  (-123456),
  (2147483647),
  (-2147483647)
  as int4_tbl(f1);

-- Test handling of Params within aggregate arguments in hashed aggregation.
-- Per bug report from Jeevan Chalke.
-- [SPARK-27877] Implement SQL-standard LATERAL subqueries
-- explain (verbose, costs off)
-- select s1, s2, sm
-- from generate_series(1, 3) s1,
--      lateral (select s2, sum(s1 + s2) sm
--               from generate_series(1, 3) s2 group by s2) ss
-- order by 1, 2;
-- select s1, s2, sm
-- from generate_series(1, 3) s1,
--      lateral (select s2, sum(s1 + s2) sm
--               from generate_series(1, 3) s2 group by s2) ss
-- order by 1, 2;

-- [SPARK-27878] Support ARRAY(sub-SELECT) expressions
-- explain (verbose, costs off)
-- select array(select sum(x+y) s
--             from generate_series(1,3) y group by y order by s)
--   from generate_series(1,3) x;
-- select array(select sum(x+y) s
--             from generate_series(1,3) y group by y order by s)
--   from generate_series(1,3) x;

-- [SPARK-27879] Implement bitwise integer aggregates(BIT_AND and BIT_OR)
--
-- test for bitwise integer aggregates
--
-- CREATE TEMPORARY TABLE bitwise_test(
--   i2 INT2,
--   i4 INT4,
--   i8 INT8,
--   i INTEGER,
--   x INT2,
--   y BIT(4)
-- );

-- empty case
-- SELECT
--   BIT_AND(i2) AS "?",
--   BIT_OR(i4)  AS "?"
-- FROM bitwise_test;

-- COPY bitwise_test FROM STDIN NULL 'null';
-- 1	1	1	1	1	B0101
-- 3	3	3	null	2	B0100
-- 7	7	7	3	4	B1100
-- \.

-- SELECT
--   BIT_AND(i2) AS "1",
--   BIT_AND(i4) AS "1",
--   BIT_AND(i8) AS "1",
--   BIT_AND(i)  AS "?",
--   BIT_AND(x)  AS "0",
--   BIT_AND(y)  AS "0100",
--
--   BIT_OR(i2)  AS "7",
--   BIT_OR(i4)  AS "7",
--   BIT_OR(i8)  AS "7",
--   BIT_OR(i)   AS "?",
--   BIT_OR(x)   AS "7",
--   BIT_OR(y)   AS "1101"
-- FROM bitwise_test;

--
-- test boolean aggregates
--
-- first test all possible transition and final states

-- The result is inconsistent with PostgreSQL because our AND does not have strict mode
SELECT
  -- boolean and transitions
  -- null because strict
  (NULL AND NULL) IS NULL AS `t`,
  (TRUE AND NULL) IS NULL AS `t`,
  (FALSE AND NULL) IS NULL AS `t`,
  (NULL AND TRUE) IS NULL AS `t`,
  (NULL AND FALSE) IS NULL AS `t`,
  -- and actual computations
  (TRUE AND TRUE) AS `t`,
  NOT (TRUE AND FALSE) AS `t`,
  NOT (FALSE AND TRUE) AS `t`,
  NOT (FALSE AND FALSE) AS `t`;

-- The result is inconsistent with PostgreSQL because our OR does not have strict mode
SELECT
  -- boolean or transitions
  -- null because strict
  (NULL OR NULL) IS NULL AS `t`,
  (TRUE OR NULL) IS NULL AS `t`,
  (FALSE OR NULL) IS NULL AS `t`,
  (NULL OR TRUE) IS NULL AS `t`,
  (NULL OR FALSE) IS NULL AS `t`,
  -- actual computations
  (TRUE OR TRUE) AS `t`,
  (TRUE OR FALSE) AS `t`,
  (FALSE OR TRUE) AS `t`,
  NOT (FALSE OR FALSE) AS `t`;

-- [SPARK-27880] Implement boolean aggregates(BOOL_AND, BOOL_OR and EVERY)
-- CREATE TEMPORARY TABLE bool_test(
--   b1 BOOL,
--   b2 BOOL,
--   b3 BOOL,
--   b4 BOOL);

-- empty case
-- SELECT
--   BOOL_AND(b1)   AS "n",
--   BOOL_OR(b3)    AS "n"
-- FROM bool_test;

-- COPY bool_test FROM STDIN NULL 'null';
-- TRUE	null	FALSE	null
-- FALSE	TRUE	null	null
-- null	TRUE	FALSE	null
-- \.

-- SELECT
--   BOOL_AND(b1)     AS "f",
--   BOOL_AND(b2)     AS "t",
--   BOOL_AND(b3)     AS "f",
--   BOOL_AND(b4)     AS "n",
--   BOOL_AND(NOT b2) AS "f",
--   BOOL_AND(NOT b3) AS "t"
-- FROM bool_test;

-- SELECT
--   EVERY(b1)     AS "f",
--   EVERY(b2)     AS "t",
--   EVERY(b3)     AS "f",
--   EVERY(b4)     AS "n",
--   EVERY(NOT b2) AS "f",
--   EVERY(NOT b3) AS "t"
-- FROM bool_test;

-- SELECT
--   BOOL_OR(b1)      AS "t",
--   BOOL_OR(b2)      AS "t",
--   BOOL_OR(b3)      AS "f",
--   BOOL_OR(b4)      AS "n",
--   BOOL_OR(NOT b2)  AS "f",
--   BOOL_OR(NOT b3)  AS "t"
-- FROM bool_test;

--
-- Test cases that should be optimized into indexscans instead of
-- the generic aggregate implementation.
--

-- Basic cases
-- explain
--  select min(unique1) from tenk1;
select min(udf(unique1)) from tenk1;
-- explain
--  select max(unique1) from tenk1;
select udf(max(unique1)) from tenk1;
-- explain
--  select max(unique1) from tenk1 where unique1 < 42;
select max(unique1) from tenk1 where udf(unique1) < 42;
-- explain
--  select max(unique1) from tenk1 where unique1 > 42;
select max(unique1) from tenk1 where unique1 > udf(42);

-- the planner may choose a generic aggregate here if parallel query is
-- enabled, since that plan will be parallel safe and the "optimized"
-- plan, which has almost identical cost, will not be.  we want to test
-- the optimized plan, so temporarily disable parallel query.
-- begin;
-- set local max_parallel_workers_per_gather = 0;
-- explain
--  select max(unique1) from tenk1 where unique1 > 42000;
select max(unique1) from tenk1 where udf(unique1) > 42000;
-- rollback;

-- multi-column index (uses tenk1_thous_tenthous)
-- explain
--  select max(tenthous) from tenk1 where thousand = 33;
select max(tenthous) from tenk1 where udf(thousand) = 33;
-- explain
--  select min(tenthous) from tenk1 where thousand = 33;
select min(tenthous) from tenk1 where udf(thousand) = 33;

-- [SPARK-17348] Correlated column is not allowed in a non-equality predicate
-- check parameter propagation into an indexscan subquery
-- explain
--  select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
--    from int4_tbl;
-- select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
--  from int4_tbl;

-- check some cases that were handled incorrectly in 8.3.0
-- explain
--  select distinct max(unique2) from tenk1;
select distinct max(udf(unique2)) from tenk1;
-- explain
--  select max(unique2) from tenk1 order by 1;
select max(unique2) from tenk1 order by udf(1);
-- explain
--  select max(unique2) from tenk1 order by max(unique2);
select max(unique2) from tenk1 order by max(udf(unique2));
-- explain
--  select max(unique2) from tenk1 order by max(unique2)+1;
select udf(max(udf(unique2))) from tenk1 order by udf(max(unique2))+1;
-- explain
--  select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;
select t1.max_unique2, udf(g) from (select max(udf(unique2)) as max_unique2 FROM tenk1) t1 LATERAL VIEW explode(array(1,2,3)) t2 AS g order by g desc;

-- interesting corner case: constant gets optimized into a seqscan
-- explain
--  select max(100) from tenk1;
select udf(max(100)) from tenk1;
