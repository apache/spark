--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- AGGREGATES [Part 2]
-- https://github.com/postgres/postgres/blob/REL_12_BETA1/src/test/regress/sql/aggregates.sql#L145-L350

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

-- SELECT
     -- boolean and transitions
     -- null because strict
--   booland_statefunc(NULL, NULL)  IS NULL AS "t",
--   booland_statefunc(TRUE, NULL)  IS NULL AS "t",
--   booland_statefunc(FALSE, NULL) IS NULL AS "t",
--   booland_statefunc(NULL, TRUE)  IS NULL AS "t",
--   booland_statefunc(NULL, FALSE) IS NULL AS "t",
     -- and actual computations
--   booland_statefunc(TRUE, TRUE) AS "t",
--   NOT booland_statefunc(TRUE, FALSE) AS "t",
--   NOT booland_statefunc(FALSE, TRUE) AS "t",
--   NOT booland_statefunc(FALSE, FALSE) AS "t";

-- SELECT
     -- boolean or transitions
     -- null because strict
--   boolor_statefunc(NULL, NULL)  IS NULL AS "t",
--   boolor_statefunc(TRUE, NULL)  IS NULL AS "t",
--   boolor_statefunc(FALSE, NULL) IS NULL AS "t",
--   boolor_statefunc(NULL, TRUE)  IS NULL AS "t",
--   boolor_statefunc(NULL, FALSE) IS NULL AS "t",
     -- actual computations
--   boolor_statefunc(TRUE, TRUE) AS "t",
--   boolor_statefunc(TRUE, FALSE) AS "t",
--   boolor_statefunc(FALSE, TRUE) AS "t",
--   NOT boolor_statefunc(FALSE, FALSE) AS "t";

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
explain
  select min(unique1) from tenk1;
select min(unique1) from tenk1;
explain
  select max(unique1) from tenk1;
select max(unique1) from tenk1;
explain
  select max(unique1) from tenk1 where unique1 < 42;
select max(unique1) from tenk1 where unique1 < 42;
explain
  select max(unique1) from tenk1 where unique1 > 42;
select max(unique1) from tenk1 where unique1 > 42;

-- the planner may choose a generic aggregate here if parallel query is
-- enabled, since that plan will be parallel safe and the "optimized"
-- plan, which has almost identical cost, will not be.  we want to test
-- the optimized plan, so temporarily disable parallel query.
-- begin;
-- set local max_parallel_workers_per_gather = 0;
explain
  select max(unique1) from tenk1 where unique1 > 42000;
select max(unique1) from tenk1 where unique1 > 42000;
-- rollback;

-- multi-column index (uses tenk1_thous_tenthous)
explain
  select max(tenthous) from tenk1 where thousand = 33;
select max(tenthous) from tenk1 where thousand = 33;
explain
  select min(tenthous) from tenk1 where thousand = 33;
select min(tenthous) from tenk1 where thousand = 33;

-- check parameter propagation into an indexscan subquery
explain
  select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
    from int4_tbl;
select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
  from int4_tbl;

-- check some cases that were handled incorrectly in 8.3.0
explain
  select distinct max(unique2) from tenk1;
select distinct max(unique2) from tenk1;
explain
  select max(unique2) from tenk1 order by 1;
select max(unique2) from tenk1 order by 1;
explain
  select max(unique2) from tenk1 order by max(unique2);
select max(unique2) from tenk1 order by max(unique2);
explain
  select max(unique2) from tenk1 order by max(unique2)+1;
select max(unique2) from tenk1 order by max(unique2)+1;
explain
  select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;
select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;

-- interesting corner case: constant gets optimized into a seqscan
explain
  select max(100) from tenk1;
select max(100) from tenk1;
