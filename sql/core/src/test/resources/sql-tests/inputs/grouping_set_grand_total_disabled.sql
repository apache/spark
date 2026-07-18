-- Kill-switch coverage for the grand-total GROUP BY GROUPING SETS (()) lowering. With
-- spark.sql.analyzer.lowerEmptyGroupingSetToGlobalAggregate.enabled = false, a
-- grand-total GROUP BY GROUPING SETS (()) reverts to the legacy Expand-based lowering, which
-- returns no rows over empty input. Runs under both analyzers (dual-run) to lock down parity in
-- the flag-off state.
--SET spark.sql.analyzer.lowerEmptyGroupingSetToGlobalAggregate.enabled=false

-- Empty input: legacy lowering returns no rows (the pre-fix behavior the kill switch restores).
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) WHERE k > 100 GROUP BY GROUPING SETS (());

-- Same meaningful query as in grouping_set.sql: with the fix off this returns no rows; with the
-- fix on it returns one row of NULL measures with count 0. The contrast is the correctness fix.
SELECT sum(v) AS total, avg(v) AS mean, max(v) AS hi, count(*) AS c
FROM VALUES (10), (20), (30) AS t(v) WHERE v > 100 GROUP BY GROUPING SETS (());

-- Non-empty input still returns the single aggregated row.
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS (());

-- grouping_id() in HAVING/ORDER BY resolves to the Expand spark_grouping_id (value 0).
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS (()) HAVING grouping_id() = 0;
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS (()) ORDER BY grouping_id();

-- GROUP BY GROUPING SETS ((), ()) is a duplicated empty grouping set that stays on the Expand path
-- regardless of the flag, so its behavior is identical flag-on and flag-off: two rows in the
-- SELECT list (grouping_id() = 0 each), and a grouping function in HAVING/ORDER BY still errors.
SELECT count(*) AS c, grouping_id() AS g
FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS ((), ());
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS ((), ()) HAVING grouping_id() = 0;
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS ((), ()) ORDER BY grouping_id();
