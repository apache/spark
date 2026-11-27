-- Tests for query hints.
--
-- Covers the two families of hints Spark SQL understands:
--   * Partitioning hints: COALESCE, REPARTITION, REPARTITION_BY_RANGE, REBALANCE and
--     REBALANCE_BY_SIZE. These are resolved by ResolveHints.ResolveCoalesceHints.
--   * Join strategy hints: BROADCAST, MERGE, SHUFFLE_HASH and SHUFFLE_REPLICATE_NL (with their
--     aliases). These are resolved by ResolveHints.ResolveJoinStrategyHints.
--
-- Hint names are case-insensitive. Unrecognized hints, and hints that reference a relation not
-- present in the query, are dropped during analysis (a warning is logged) and leave the plan
-- unchanged.
--
-- Adaptive query execution is enabled by default. A dedicated section near the end turns it off,
-- both to feed the EXPLAIN examples deterministic physical plans and to show that REBALANCE and
-- REBALANCE_BY_SIZE only take effect when AQE is enabled.

-- Test relations. Temp views over inline VALUES keep the analyzed plans focused on the hint nodes
-- (ResolvedHint / Repartition / RebalancePartitions) without a file-scan or warehouse-path surface.
CREATE TEMPORARY VIEW t1 AS SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t1(c1, c2);
CREATE TEMPORARY VIEW t2 AS SELECT * FROM VALUES (1, 'x'), (2, 'y'), (4, 'z') AS t2(c1, c3);

-- ---------------------------------------------------------------------------------------------
-- Partitioning hints
-- ---------------------------------------------------------------------------------------------

-- COALESCE reduces the number of partitions and only accepts a partition number.
SELECT /*+ COALESCE(2) */ * FROM t1;

-- REPARTITION accepts a partition number, column names, or both.
SELECT /*+ REPARTITION(3) */ * FROM t1;

SELECT /*+ REPARTITION(c1) */ * FROM t1;

SELECT /*+ REPARTITION(3, c1) */ * FROM t1;

SELECT /*+ REPARTITION(3, c1, c2) */ * FROM t1;

-- Hint names are case-insensitive.
SELECT /*+ repartition(3) */ * FROM t1;

-- REPARTITION_BY_RANGE requires column names; the partition number is optional.
SELECT /*+ REPARTITION_BY_RANGE(c1) */ * FROM t1;

SELECT /*+ REPARTITION_BY_RANGE(3, c1) */ * FROM t1;

-- REBALANCE accepts no parameters, a partition number, column names, or both.
SELECT /*+ REBALANCE */ * FROM t1;

SELECT /*+ REBALANCE(3) */ * FROM t1;

SELECT /*+ REBALANCE(c1) */ * FROM t1;

SELECT /*+ REBALANCE(3, c1) */ * FROM t1;

-- REBALANCE_BY_SIZE takes an advisory partition size as its first parameter, optionally followed
-- by column names. The size may be a number of bytes or a size string such as '10MB'.
SELECT /*+ REBALANCE_BY_SIZE(1024) */ * FROM t1;

SELECT /*+ REBALANCE_BY_SIZE('10MB') */ * FROM t1;

SELECT /*+ REBALANCE_BY_SIZE('10MB', c1) */ * FROM t1;

-- Multiple partitioning hints all appear (stacked) in the analyzed plan. The optimizer later keeps
-- only the leftmost; see the EXPLAIN EXTENDED example in the optimizer section below.
SELECT /*+ REPARTITION(100), COALESCE(500), REPARTITION_BY_RANGE(3, c1) */ * FROM t1;

-- ---------------------------------------------------------------------------------------------
-- Join strategy hints
-- ---------------------------------------------------------------------------------------------

-- BROADCAST and its aliases BROADCASTJOIN and MAPJOIN.
SELECT /*+ BROADCAST(t1) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

SELECT /*+ BROADCASTJOIN(t1) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

SELECT /*+ MAPJOIN(t1) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- MERGE (shuffle sort merge join) and its aliases SHUFFLE_MERGE and MERGEJOIN.
SELECT /*+ MERGE(t1) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

SELECT /*+ SHUFFLE_MERGE(t1) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

SELECT /*+ MERGEJOIN(t1) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- SHUFFLE_HASH (shuffle hash join).
SELECT /*+ SHUFFLE_HASH(t1) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- SHUFFLE_REPLICATE_NL (shuffle-and-replicate nested loop join).
SELECT /*+ SHUFFLE_REPLICATE_NL(t1) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- A join strategy hint with no relation argument applies to the whole subtree.
SELECT /*+ BROADCAST */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- A join strategy hint may reference more than one relation.
SELECT /*+ MERGE(t1, t2) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- A join strategy hint resolves against a table alias.
SELECT /*+ BROADCAST(x) */ * FROM t1 AS x JOIN t2 ON x.c1 = t2.c1;

-- A join strategy hint resolves against a CTE name.
WITH cte AS (SELECT * FROM t1)
SELECT /*+ BROADCAST(cte) */ * FROM cte JOIN t2 ON cte.c1 = t2.c1;

-- Not every strategy applies to every join type; BROADCAST of the right side of a LEFT JOIN is
-- allowed.
SELECT /*+ BROADCAST(t2) */ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.c1;

-- When conflicting join strategy hints are given, Spark prioritizes BROADCAST over MERGE over
-- SHUFFLE_HASH over SHUFFLE_REPLICATE_NL. See the EXPLAIN EXTENDED example in the optimizer section
-- below for the strategy that is finally chosen.
SELECT /*+ BROADCAST(t1), MERGE(t1, t2) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- Join strategy and partitioning hints can be combined.
SELECT /*+ BROADCAST(t1), COALESCE(2) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- ---------------------------------------------------------------------------------------------
-- Negative and edge cases
-- ---------------------------------------------------------------------------------------------

-- COALESCE only accepts a partition number; a column name is rejected.
SELECT /*+ COALESCE(c1) */ * FROM t1;

-- Empty hint parentheses are a syntax error: a hint takes either no parentheses or arguments.
SELECT /*+ COALESCE() */ * FROM t1;

-- REPARTITION_BY_RANGE with only a partition count (no columns) is accepted, producing a range
-- repartition with no ordering expressions.
SELECT /*+ REPARTITION_BY_RANGE(3) */ * FROM t1;

-- REBALANCE_BY_SIZE rejects a non-positive advisory partition size.
SELECT /*+ REBALANCE_BY_SIZE(0) */ * FROM t1;

-- An unrecognized hint name is ignored and leaves the plan unchanged.
SELECT /*+ UNKNOWN_HINT(t1) */ * FROM t1;

-- A join strategy hint that references a relation not present in the query is ignored.
SELECT /*+ BROADCAST(non_existent) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- ---------------------------------------------------------------------------------------------
-- Optimizer and planner behavior (EXPLAIN EXTENDED)
-- ---------------------------------------------------------------------------------------------
--
-- The analyzed plans above show how hints are resolved. The queries below use EXPLAIN EXTENDED to
-- show what the optimizer and planner then do with them. AQE is turned off first so the physical
-- plans are deterministic (the same reason explain.sql disables it).
SET spark.sql.adaptive.enabled=false;

-- Stacked partitioning hints: the analyzed plan keeps all three, but the optimizer collapses them
-- to the leftmost REPARTITION.
EXPLAIN EXTENDED
SELECT /*+ REPARTITION(100), COALESCE(500), REPARTITION_BY_RANGE(3, c1) */ * FROM t1;

-- Conflicting join strategy hints: BROADCAST wins over MERGE, so the planner picks a broadcast
-- join.
EXPLAIN EXTENDED
SELECT /*+ BROADCAST(t1), MERGE(t1, t2) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- ---------------------------------------------------------------------------------------------
-- REBALANCE hints require adaptive query execution
-- ---------------------------------------------------------------------------------------------
--
-- AQE is still disabled from the section above. ResolveCoalesceHints only recognizes REBALANCE /
-- REBALANCE_BY_SIZE when AQE is enabled, so here the hints are dropped (a warning is logged) and
-- the plan is left unchanged -- contrast with the RebalancePartitions nodes in the partitioning
-- section above.
SELECT /*+ REBALANCE */ * FROM t1;

SELECT /*+ REBALANCE_BY_SIZE(1024) */ * FROM t1;

-- Clean up.
DROP VIEW t1;
DROP VIEW t2;
