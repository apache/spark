-- Tests for query hints.
--
-- Covers the two families of hints Spark SQL understands:
--   * Partitioning hints: COALESCE, REPARTITION, REPARTITION_BY_RANGE, REBALANCE and
--     REBALANCE_BY_SIZE. These are resolved by ResolveHints.ResolveCoalesceHints.
--   * Join strategy hints: BROADCAST, MERGE, SHUFFLE_HASH and SHUFFLE_REPLICATE_NL (with their
--     aliases). These are resolved by ResolveHints.ResolveJoinStrategyHints.
--
-- Unrecognized hints, and hints that reference a relation not present in the query, are dropped
-- during analysis (a warning is logged) and leave the plan unchanged.

-- REBALANCE and REBALANCE_BY_SIZE only take effect when adaptive query execution is enabled.
--SET spark.sql.adaptive.enabled=true

-- Test tables.
CREATE TABLE t1 (c1 INT, c2 STRING) USING parquet;
INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c');

CREATE TABLE t2 (c1 INT, c3 STRING) USING parquet;
INSERT INTO t2 VALUES (1, 'x'), (2, 'y'), (4, 'z');

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

-- Multiple partitioning hints stack in the analyzed plan; the optimizer keeps only the leftmost.
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

-- When conflicting join strategy hints are given, Spark prioritizes BROADCAST over MERGE over
-- SHUFFLE_HASH over SHUFFLE_REPLICATE_NL. The overridden hint is dropped with a warning.
SELECT /*+ BROADCAST(t1), MERGE(t1, t2) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- Join strategy and partitioning hints can be combined.
SELECT /*+ BROADCAST(t1), COALESCE(2) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- ---------------------------------------------------------------------------------------------
-- Negative and edge cases
-- ---------------------------------------------------------------------------------------------

-- COALESCE only accepts a partition number; a column name is rejected.
SELECT /*+ COALESCE(c1) */ * FROM t1;

-- REBALANCE_BY_SIZE rejects a non-positive advisory partition size.
SELECT /*+ REBALANCE_BY_SIZE(0) */ * FROM t1;

-- An unrecognized hint name is ignored and leaves the plan unchanged.
SELECT /*+ UNKNOWN_HINT(t1) */ * FROM t1;

-- A join strategy hint that references a relation not present in the query is ignored.
SELECT /*+ BROADCAST(non_existent) */ * FROM t1 JOIN t2 ON t1.c1 = t2.c1;

-- Clean up.
DROP TABLE t1;
DROP TABLE t2;
