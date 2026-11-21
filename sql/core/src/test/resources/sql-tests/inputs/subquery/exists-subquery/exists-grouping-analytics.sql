-- Tests correlated subqueries over ROLLUP/CUBE/GROUPING SETS grouping keys.
-- ResolveGroupingAnalytics must re-point the subquery's outer references at the post-Expand
-- grouping attributes, in the SELECT list and in HAVING.

--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

-- Correlated EXISTS with ROLLUP
SELECT
  col1,
  SUM(col2) AS s,
  EXISTS (SELECT 1 FROM VALUES (1), (2) AS v(x) WHERE v.x = col1) AS has_match
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- Correlated EXISTS with CUBE
SELECT
  col1,
  SUM(col2) AS s,
  EXISTS (SELECT 1 FROM VALUES (1), (2) AS v(x) WHERE v.x = col1) AS has_match
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY CUBE(col1)
ORDER BY col1 NULLS LAST;

-- Correlated EXISTS with GROUPING SETS
SELECT
  col1,
  SUM(col2) AS s,
  EXISTS (SELECT 1 FROM VALUES (1), (2) AS v(x) WHERE v.x = col1) AS has_match
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY GROUPING SETS ((col1), ())
ORDER BY col1 NULLS LAST;

-- Uncorrelated scalar subquery that IS the ROLLUP grouping key.
-- The grand-total row must show NULL for the key, not the subquery value.
SELECT (SELECT max(x) FROM VALUES (7) AS cfg(x)) AS v, count(*) AS c
FROM VALUES (1), (2), (3) AS t(id)
GROUP BY ROLLUP((SELECT max(x) FROM VALUES (7) AS cfg(x)))
ORDER BY v NULLS LAST;

-- Uncorrelated scalar subquery grouping key with CUBE.
SELECT (SELECT max(x) FROM VALUES (7) AS cfg(x)) AS v, count(*) AS c
FROM VALUES (1), (2), (3) AS t(id)
GROUP BY CUBE((SELECT max(x) FROM VALUES (7) AS cfg(x)))
ORDER BY v NULLS LAST;

-- Scalar subquery grouping key alongside a normal key with ROLLUP.
SELECT kind, (SELECT max(x) FROM VALUES (7) AS cfg(x)) AS v, count(*) AS c
FROM VALUES (1, 'a'), (2, 'b'), (3, 'a') AS t(id, kind)
GROUP BY ROLLUP(kind, (SELECT max(x) FROM VALUES (7) AS cfg(x)))
ORDER BY kind NULLS LAST, v NULLS LAST;

-- Correlated EXISTS in SELECT list referencing the ROLLUP grouping key.
-- Super-aggregate rows see NULL for the key, so EXISTS is false there.
SELECT col1,
  EXISTS (SELECT 1 FROM VALUES (1), (2) AS v(x) WHERE v.x = col1) AS has_match
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- Grouping key is itself an outer reference: ROLLUP over an outer column inside a
-- correlated EXISTS subquery. Rejected: the outer ref is materialized into the Expand's
-- grouping projection, which trips ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED.
SELECT col1
FROM VALUES (1), (2) AS t(col1)
WHERE EXISTS (
  SELECT 1 FROM VALUES (10), (20) AS s(v)
  GROUP BY ROLLUP(col1)
)
ORDER BY col1;

-- Outer reference (not a grouping key) in the pre-group WHERE (below the Expand) of a
-- grouping-analytics subquery. Rejected with ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED.
SELECT a
FROM VALUES (1), (2) AS t(a)
WHERE EXISTS (
  SELECT count(*) AS c
  FROM VALUES (10, 1), (20, 2) AS s(v, g)
  WHERE s.v = a * 10
  GROUP BY ROLLUP(g)
)
ORDER BY a;

-- Correlated NOT EXISTS in HAVING over ROLLUP. The HAVING subquery correlates to the grouping
-- key, which must bind to the rebuilt aggregate's post-Expand output.
SELECT col1, count(*) AS c
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
HAVING NOT EXISTS (SELECT 1 FROM VALUES (1) AS v(x) WHERE v.x = col1)
ORDER BY col1 NULLS LAST;

-- Same over CUBE.
SELECT col1, count(*) AS c
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY CUBE(col1)
HAVING NOT EXISTS (SELECT 1 FROM VALUES (1) AS v(x) WHERE v.x = col1)
ORDER BY col1 NULLS LAST;

-- Same over GROUPING SETS.
SELECT col1, count(*) AS c
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY GROUPING SETS ((col1), ())
HAVING NOT EXISTS (SELECT 1 FROM VALUES (1) AS v(x) WHERE v.x = col1)
ORDER BY col1 NULLS LAST;

-- Grouping key selected under an alias; the correlation references the base column col1 (the
-- grouping input), which must map to the aliased aggregate output.
SELECT col1 AS k, count(*) AS c
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
HAVING NOT EXISTS (SELECT 1 FROM VALUES (1) AS v(x) WHERE v.x = col1)
ORDER BY k NULLS LAST;

-- Grouping key col1 is not in the SELECT list, so it only enters the aggregate output through
-- the extra expressions pulled up from HAVING; the correlation must still map to it.
SELECT col2, count(*) AS c
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1, col2)
HAVING NOT EXISTS (SELECT 1 FROM VALUES (1) AS v(x) WHERE v.x = col1)
ORDER BY col2 NULLS LAST, c;

-- SELECT-list scalar subquery whose aggregate mixes an inner column with the grouping key is
-- rejected (AGGREGATE_FUNCTION_MIXED_OUTER_LOCAL_REFERENCES).
SELECT col1, (SELECT max(v.x + col1) FROM VALUES (1), (2) AS v(x)) AS m
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- SELECT-list scalar subquery whose aggregate is over the grouping key alone. A correlated scalar
-- subquery in the aggregate list must itself be a grouping expression, so CheckAnalysis rejects it
-- before any outer-reference rewrite.
SELECT col1, (SELECT max(col1) FROM VALUES (1), (2) AS v(x)) AS m
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- SELECT-list IN subquery whose aggregate is over the grouping key alone. Unlike a scalar subquery
-- this passes CheckAnalysis, and the outer reference inside max() must move to the post-Expand key
-- like any other SELECT-list correlation (nothing lifts it to the outer aggregate).
SELECT col1, count(*) IN (SELECT max(col1) FROM VALUES (1), (2) AS v(x)) AS m
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- Grouping key selected twice, once under an alias. The correlation must bind to a single
-- consistent aggregate output in both analyzers.
SELECT col1 AS a, col1, count(*) AS c
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
HAVING NOT EXISTS (SELECT 1 FROM VALUES (1) AS v(x) WHERE v.x = col1)
ORDER BY a NULLS LAST;

-- Same key selected twice, bare column first.
SELECT col1, col1 AS a, count(*) AS c
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
HAVING NOT EXISTS (SELECT 1 FROM VALUES (1) AS v(x) WHERE v.x = col1)
ORDER BY a NULLS LAST;

-- Same key selected under two aliases; the correlation binds to the first alias.
SELECT col1 AS a, col1 AS b, count(*) AS c
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
HAVING NOT EXISTS (SELECT 1 FROM VALUES (1) AS v(x) WHERE v.x = col1)
ORDER BY a NULLS LAST;

-- HAVING subquery correlated through an aggregate. The outer reference inside max() is lifted to
-- the outer aggregate and must keep its pre-Expand attribute (left untouched).
SELECT col1, count(*) AS c
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
HAVING EXISTS (SELECT 1 FROM VALUES (1) AS v(x) WHERE v.x = max(col1))
ORDER BY col1 NULLS LAST;

-- Aliased grouping key; the correlation goes through an aggregate over the base column.
SELECT col1 AS k, count(*) AS c
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
HAVING EXISTS (SELECT 1 FROM VALUES (1) AS v(x) WHERE v.x = max(col1))
ORDER BY k NULLS LAST;

-- Aliased grouping key; ORDER BY references the base column, which must bind to the alias in
-- both analyzers.
SELECT col1 AS k, count(*) AS c
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- Aliased grouping key under a window function; the QUALIFY subquery correlates to the base
-- column.
SELECT col1 AS k, RANK() OVER (ORDER BY col1) AS r
FROM VALUES (1, 10), (1, 20), (2, 30) AS t(col1, col2)
GROUP BY ROLLUP(col1)
QUALIFY EXISTS (SELECT 1 FROM VALUES (1), (2) AS v(x) WHERE v.x = col1)
ORDER BY k NULLS LAST;
