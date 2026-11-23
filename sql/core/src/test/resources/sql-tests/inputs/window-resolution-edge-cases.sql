--SET spark.sql.catalog.testcat=org.apache.spark.sql.connector.catalog.InMemoryTableCatalog

CREATE TABLE t1(col1 INT, col2 INT);
INSERT INTO t1 VALUES (1, 1), (2, 1), (3, 2);

-- Nested window function: MAX(ROW_NUMBER() OVER (...)) OVER (...)
-- The nested ROW_NUMBER() must be extracted to a separate window layer, producing the nested
-- window at the bottom of the plan.
SELECT
  col1,
  MAX(ROW_NUMBER() OVER (ORDER BY col1)) OVER () AS max_rn
FROM t1;

-- Window function nested in a QUALIFY window function's specification. The nested window in the
-- spec must be extracted to its own window layer below the one whose spec references it.
-- Nested in ORDER BY.
SELECT col1 FROM t1
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY col1 ORDER BY COUNT(*) OVER (PARTITION BY col1, col2) DESC) <= 1;
-- Nested in PARTITION BY.
SELECT col1 FROM t1
QUALIFY SUM(col2) OVER (PARTITION BY COUNT(*) OVER (PARTITION BY col1, col2)) > 0;
-- Nested in the spec with GROUP BY (Aggregate-backed child).
SELECT col1 FROM t1 GROUP BY col1
QUALIFY ROW_NUMBER() OVER (ORDER BY DENSE_RANK() OVER (ORDER BY col1)) <= 1;
-- Nested in the window function argument, itself with a nested window in its spec.
SELECT col1 FROM t1
QUALIFY MAX(RANK() OVER (ORDER BY COUNT(*) OVER (PARTITION BY col1))) OVER (PARTITION BY col1) > 0;

-- Nested windows in every part of an outer window expression must be extracted into separate
-- window layers.
SELECT n_nationkey, n_regionkey
FROM VALUES (0L, 0L), (1L, 0L), (2L, 1L), (3L, 1L)
  AS nation(n_nationkey, n_regionkey)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY n_regionkey
  ORDER BY COUNT(*) OVER (PARTITION BY n_nationkey % 3) DESC, n_nationkey
) = 1
ORDER BY n_regionkey, n_nationkey;

SELECT id
FROM VALUES
  (0L, 0L, 10L), (1L, 0L, 20L), (2L, 1L, 30L),
  (3L, 2L, 40L), (4L, 2L, 50L), (5L, 2L, 60L)
  AS input(id, grp, value)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY COUNT(*) OVER (PARTITION BY grp)
  ORDER BY id
) = 1
ORDER BY id;

SELECT id, grp, value
FROM VALUES
  (0L, 0L, 10L), (1L, 0L, 20L), (2L, 1L, 30L),
  (3L, 2L, 40L), (4L, 2L, 50L), (5L, 2L, 60L)
  AS input(id, grp, value)
QUALIFY ROW_NUMBER() OVER (
  ORDER BY COALESCE(SUM(value) OVER (PARTITION BY grp), 0L) DESC, id
) = 1
ORDER BY id;

SELECT id
FROM VALUES
  (0L, 0L, 10L), (1L, 0L, 20L), (2L, 1L, 30L),
  (3L, 2L, 40L), (4L, 2L, 50L), (5L, 2L, 60L)
  AS input(id, grp, value)
QUALIFY SUM(ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id)) OVER (
  ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) = 10L
ORDER BY id;

SELECT id, grp, value
FROM VALUES
  (0L, 0L, 10L), (1L, 0L, 20L), (2L, 1L, 30L),
  (3L, 2L, 40L), (4L, 2L, 50L), (5L, 2L, 60L)
  AS input(id, grp, value)
QUALIFY ROW_NUMBER() OVER (
  ORDER BY COUNT(*) OVER (PARTITION BY grp) DESC,
    SUM(value) OVER (PARTITION BY grp) DESC,
    id
) = 1
ORDER BY id;

SELECT id, grp
FROM VALUES
  (0L, 0L, 10L), (1L, 0L, 20L), (2L, 1L, 30L),
  (3L, 2L, 40L), (4L, 2L, 50L), (5L, 2L, 60L)
  AS input(id, grp, value)
QUALIFY ROW_NUMBER() OVER (
  ORDER BY SUM(ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id))
    OVER (PARTITION BY grp), id
) = 1
ORDER BY id;

-- Nested windows in the SELECT list: other outer and inner functions, two levels of nesting, a
-- nested window in the outer window specification, and a named window.
SELECT col1, SUM(RANK() OVER (ORDER BY col1)) OVER (PARTITION BY col2) AS s FROM t1;
SELECT col1, MAX(SUM(ROW_NUMBER() OVER (ORDER BY col1)) OVER ()) OVER () AS m FROM t1;
SELECT col1, ROW_NUMBER() OVER (ORDER BY COUNT(*) OVER (PARTITION BY col2), col1) AS rn FROM t1;
SELECT col1, MAX(ROW_NUMBER() OVER w) OVER () AS max_rn FROM t1 WINDOW w AS (ORDER BY col1);

-- Nested windows over an aggregate, and in QUALIFY together with HAVING.
SELECT col1, MAX(RANK() OVER (ORDER BY SUM(col2))) OVER () AS m FROM t1 GROUP BY col1;
SELECT col1 FROM t1 GROUP BY col1, col2 HAVING col2 > 0
QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(col2) OVER (PARTITION BY col1) DESC, col1) = 1;

-- HAVING must be evaluated before every nested window layer.
SELECT
  k,
  SUM(RANK() OVER (ORDER BY SUM(v) DESC)) OVER () AS rank_sum
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY k
HAVING k = 'keep'
ORDER BY k;

-- HAVING must also precede windows nested in the outer window specification.
SELECT
  k,
  SUM(RANK() OVER (ORDER BY SUM(v) DESC)) OVER (
    PARTITION BY RANK() OVER (ORDER BY SUM(v) DESC)) AS partition_rank_sum,
  SUM(RANK() OVER (ORDER BY SUM(v) DESC)) OVER (
    ORDER BY RANK() OVER (ORDER BY SUM(v) DESC)) AS ordered_rank_sum
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY k
HAVING k = 'keep'
ORDER BY k;

-- Three nested window levels, mixed with single-level and two-level windows.
SELECT
  k,
  COUNT(*) OVER () AS group_count,
  SUM(RANK() OVER (ORDER BY SUM(v) DESC)) OVER () AS rank_sum,
  MAX(SUM(RANK() OVER (ORDER BY SUM(v) DESC)) OVER ()) OVER () AS max_rank_sum
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY k
HAVING k = 'keep'
ORDER BY k;

-- HAVING on an aggregate alias must precede every nested window layer.
SELECT
  k,
  COUNT(*) AS row_count,
  SUM(RANK() OVER (ORDER BY SUM(v) DESC)) OVER () AS rank_sum
FROM VALUES ('drop', 1), ('drop', 1), ('keep', 1) AS input(k, v)
GROUP BY k
HAVING row_count = 1
ORDER BY k;

-- A window alias is not valid in HAVING.
SELECT
  k,
  SUM(RANK() OVER (ORDER BY SUM(v) DESC)) OVER () AS rank_sum
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY k
HAVING rank_sum = 1;

-- Grouping analytics HAVING must be evaluated before every nested window layer.
SELECT
  k,
  SUM(RANK() OVER (ORDER BY SUM(v) DESC)) OVER () AS rank_sum
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep'
ORDER BY k;

-- Grouping analytics HAVING must also precede a single window layer.
SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY GROUPING SETS ((k), ())
HAVING k = 'keep'
ORDER BY k;

-- Lateral aliases must be resolved before grouping analytics HAVING is deferred.
SELECT
  k,
  SUM(v) AS s,
  s * 2 AS s2,
  s2 + 1 AS s3,
  SUM(SUM(v)) OVER (PARTITION BY k) AS partition_sum
FROM VALUES ('a', 1), ('b', 2) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k IS NOT NULL
ORDER BY k;

-- Lateral aliases do not prevent HAVING from preceding the window.
SELECT
  k,
  SUM(v) AS s,
  s * 2 AS s2,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep'
ORDER BY k;

-- HAVING on a lifted aggregate alias must precede the window.
SELECT
  k,
  SUM(v) AS s,
  s * 2 AS s2,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING s = 1L
ORDER BY k;

-- Grouping analytics HAVING on a window alias keeps its existing analysis behavior.
SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY GROUPING SETS ((k), ())
HAVING group_count = 3L
ORDER BY k;

-- Grouping functions keep their existing placement and must still be rewritten.
SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY GROUPING SETS ((k), ())
HAVING GROUPING_ID() = 1L
ORDER BY k;

-- Aggregates used only by HAVING must remain available until the filter is evaluated.
SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 1), ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING COUNT(*) = 1L
ORDER BY k;

-- Grouping functions keep the existing lateral-alias resolution path.
SELECT
  k,
  SUM(v) AS s,
  s * 2 AS s2,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING GROUPING_ID() = 0L
ORDER BY k;

-- Subqueries in grouping analytics HAVING must remain resolved after the filter is moved.
SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = (SELECT 'keep')
ORDER BY k;

-- A window in a SELECT-list subquery is not a SELECT-list window output.
SELECT
  k,
  (SELECT MAX(rn) FROM (
    SELECT ROW_NUMBER() OVER (ORDER BY x) AS rn FROM VALUES (1) AS query_input(x))) AS sq
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep'
ORDER BY SUM(v);

-- A subquery alias containing a window does not keep HAVING above a real SELECT-list window.
SELECT
  k,
  (SELECT MAX(rn) FROM (
    SELECT ROW_NUMBER() OVER (ORDER BY x) AS rn FROM VALUES (1) AS query_input(x))) AS sq,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING sq = 1 AND k = 'keep'
ORDER BY k;

-- QUALIFY sees the windows after deferred HAVING is placed.
SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep'
QUALIFY group_count > 0
ORDER BY k;

-- QUALIFY also resolves with lateral aliases and grouping columns outside the SELECT list.
SELECT
  SUM(v) AS s,
  s * 2 AS s2,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep'
QUALIFY k = 'keep' AND group_count > 0
ORDER BY s;

-- Nondeterministic HAVING expressions remain below the SELECT-list windows.
SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep' AND RAND(0) < 2D
ORDER BY k;

-- Analysis-only commands must not retain a deferred HAVING marker.
CREATE NAMESPACE testcat.having_window;

CREATE TABLE testcat.having_window.ctas USING parquet AS
SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep';

SELECT * FROM testcat.having_window.ctas ORDER BY k;

CREATE TABLE testcat.having_window.rtas (k STRING, group_count BIGINT) USING parquet;
REPLACE TABLE testcat.having_window.rtas USING parquet AS
SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep';

SELECT * FROM testcat.having_window.rtas ORDER BY k;

CREATE TABLE testcat.having_window.lateral_ctas USING parquet AS
SELECT
  k,
  SUM(v) AS s,
  s * 2 AS s2,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep';

SELECT * FROM testcat.having_window.lateral_ctas ORDER BY k;

CREATE VIEW having_window_view AS
SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k IN (SELECT value FROM VALUES ('keep') AS keep_values(value));

SELECT * FROM having_window_view ORDER BY k;

ALTER VIEW having_window_view AS
SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep' AND EXISTS (
  SELECT 1 FROM VALUES ('keep') AS keep_values(value) WHERE value = 'keep');

SELECT * FROM having_window_view ORDER BY k;

CACHE TABLE having_window_cache AS
SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k IN (SELECT value FROM VALUES ('keep') AS keep_values(value));

SELECT * FROM having_window_cache ORDER BY k;
UNCACHE TABLE having_window_cache;
DROP VIEW having_window_view;
DROP TABLE testcat.having_window.ctas;
DROP TABLE testcat.having_window.rtas;
DROP TABLE testcat.having_window.lateral_ctas;
DROP NAMESPACE testcat.having_window;

-- The legacy configuration preserves evaluation after the inner window.
SET spark.sql.legacy.evaluateHavingAfterInnerWindow=true;

SELECT
  k,
  SUM(v) AS s,
  s * 2 AS s2,
  s2 + 1 AS s3,
  SUM(SUM(v)) OVER (PARTITION BY k) AS partition_sum
FROM VALUES ('a', 1), ('b', 2) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k IS NOT NULL
ORDER BY k;

SELECT
  k,
  SUM(v) AS s,
  s * 2 AS s2,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep'
ORDER BY k;

SELECT
  k,
  SUM(v) AS s,
  s * 2 AS s2,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING s = 1L
ORDER BY k;

SELECT
  k,
  SUM(RANK() OVER (ORDER BY SUM(v) DESC)) OVER () AS rank_sum
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY k
HAVING k = 'keep'
ORDER BY k;

SELECT
  k,
  SUM(RANK() OVER (ORDER BY SUM(v) DESC)) OVER () AS rank_sum
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep'
ORDER BY k;

SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY GROUPING SETS ((k), ())
HAVING k = 'keep'
ORDER BY k;

SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY GROUPING SETS ((k), ())
HAVING group_count = 3L
ORDER BY k;

SELECT
  k,
  (SELECT MAX(rn) FROM (
    SELECT ROW_NUMBER() OVER (ORDER BY x) AS rn FROM VALUES (1) AS query_input(x))) AS sq,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING sq = 1 AND k = 'keep'
ORDER BY k;

SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep'
QUALIFY group_count > 0
ORDER BY k;

SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING k = 'keep' AND RAND(0) < 2D
ORDER BY k;

SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY GROUPING SETS ((k), ())
HAVING GROUPING_ID() = 1L
ORDER BY k;

SELECT
  k,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 1), ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING COUNT(*) = 1L
ORDER BY k;

SELECT
  k,
  SUM(v) AS s,
  s * 2 AS s2,
  COUNT(*) OVER () AS group_count
FROM VALUES ('drop', 2), ('keep', 1) AS input(k, v)
GROUP BY ROLLUP(k)
HAVING GROUPING_ID() = 0L
ORDER BY k;

RESET spark.sql.legacy.evaluateHavingAfterInnerWindow;

-- Window + star should preserve original column name case
SELECT lead(COL1) OVER (ORDER BY COL1) AS flag, * FROM t1;
SELECT *, lead(COL1) OVER (ORDER BY COL1) AS flag FROM t1;
SELECT T1.*, lag(Col2) OVER (ORDER BY cOL1) AS flag FROM t1;
SELECT * EXCEPT (col2), lead(COL1) OVER (ORDER BY COL1) AS flag FROM t1;
SELECT lead(COL1) OVER (PARTITION BY COL2 ORDER BY COL1) AS flag, * FROM t1 ORDER BY COL1;

-- Nondeterministic expressions are allowed in aggregate functions used as window functions.
SELECT col1, count(rand(0)) OVER (PARTITION BY col1) AS cnt FROM t1 ORDER BY col1, col2;
SELECT count(rand(0)) OVER () AS cnt FROM t1;
SELECT col1, count(randn(0)) OVER (ORDER BY col1) AS cnt FROM t1;
SELECT col1, count(uuid()) OVER (PARTITION BY col2) AS cnt FROM t1 ORDER BY col1;
SELECT col1, max(rand(0) < 2) OVER () AS m FROM t1 ORDER BY col1;
-- Nondeterministic expressions are also allowed in window partitioning expressions.
SELECT col1, row_number() OVER (PARTITION BY rand(0) < 2 ORDER BY col1) AS rn FROM t1;

DROP TABLE t1;
