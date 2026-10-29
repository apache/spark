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

-- Window + star should preserve original column name case
SELECT lead(COL1) OVER (ORDER BY COL1) AS flag, * FROM t1;

-- Nondeterministic expressions are allowed in aggregate functions used as window functions.
SELECT col1, count(rand(0)) OVER (PARTITION BY col1) AS cnt FROM t1 ORDER BY col1, col2;
SELECT count(rand(0)) OVER () AS cnt FROM t1;

DROP TABLE t1;
