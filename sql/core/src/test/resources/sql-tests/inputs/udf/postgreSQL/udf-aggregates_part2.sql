--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- AGGREGATES [Part 2]
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/aggregates.sql#L145-L350
--
-- This test file was converted from postgreSQL/aggregates_part2.sql.

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
CREATE OR REPLACE TEMPORARY VIEW bitwise_test AS SELECT * FROM VALUES
  (1, 1, 1, 1L),
  (3, 3, 3, null),
  (7, 7, 7, 3L) AS bitwise_test(b1, b2, b3, b4);

-- empty case
SELECT BIT_AND(b1) AS n1, BIT_OR(b2)  AS n2 FROM bitwise_test where 1 = 0;

-- null case
SELECT BIT_AND(b4) AS n1, BIT_OR(b4)  AS n2 FROM bitwise_test where b4 is null;


SELECT
 BIT_AND(cast(b1 as tinyint)) AS a1,
 BIT_AND(cast(b2 as smallint)) AS b1,
 BIT_AND(b3) AS c1,
 BIT_AND(b4) AS d1,
 BIT_OR(cast(b1 as tinyint))  AS e7,
 BIT_OR(cast(b2 as smallint))  AS f7,
 BIT_OR(b3)  AS g7,
 BIT_OR(b4)  AS h3
FROM bitwise_test;

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
CREATE OR REPLACE TEMPORARY VIEW bool_test AS SELECT * FROM VALUES
  (TRUE, null, FALSE, null),
  (FALSE, TRUE, null, null),
  (null, TRUE, FALSE, null) AS bool_test(b1, b2, b3, b4);

-- empty case
SELECT BOOL_AND(b1) AS n1, BOOL_OR(b3) AS n2 FROM bool_test WHERE 1 = 0;

SELECT
  BOOL_AND(b1)     AS f1,
  BOOL_AND(b2)     AS t2,
  BOOL_AND(b3)     AS f3,
  BOOL_AND(b4)     AS n4,
  BOOL_AND(NOT b2) AS f5,
  BOOL_AND(NOT b3) AS t6
FROM bool_test;

SELECT
  EVERY(b1)     AS f1,
  EVERY(b2)     AS t2,
  EVERY(b3)     AS f3,
  EVERY(b4)     AS n4,
  EVERY(NOT b2) AS f5,
  EVERY(NOT b3) AS t6
FROM bool_test;

SELECT
  BOOL_OR(b1)      AS t1,
  BOOL_OR(b2)      AS t2,
  BOOL_OR(b3)      AS f3,
  BOOL_OR(b4)      AS n4,
  BOOL_OR(NOT b2)  AS f5,
  BOOL_OR(NOT b3)  AS t6
FROM bool_test;

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
