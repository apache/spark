-- Test cases for NEAREST BY top-K ranking join.

CREATE VIEW users(user_id, score) AS VALUES (1, 10.0), (2, 20.0), (3, 30.0);
CREATE VIEW products(product, pscore) AS VALUES ('A', 11.0), ('B', 22.0), ('C', 5.0);

-- Basic APPROX NEAREST BY SIMILARITY with k = 1
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 1 BY SIMILARITY -abs(u.score - p.pscore);

-- APPROX NEAREST BY DISTANCE with k = 2
SELECT u.user_id, p.product, p.pscore
FROM users u JOIN products p
  APPROX NEAREST 2 BY DISTANCE abs(u.score - p.pscore);

-- EXACT NEAREST BY SIMILARITY with default k = 1
SELECT u.user_id, p.product
FROM users u INNER JOIN products p
  EXACT NEAREST BY SIMILARITY -abs(u.score - p.pscore);

-- LEFT OUTER JOIN with NEAREST BY, empty right side
SELECT u.user_id, p.product
FROM users u LEFT OUTER JOIN (SELECT * FROM products WHERE false) p
  APPROX NEAREST 1 BY SIMILARITY -abs(u.score - p.pscore);

-- Explicit INNER keyword
SELECT u.user_id, p.product
FROM users u INNER JOIN products p
  APPROX NEAREST 1 BY DISTANCE abs(u.score - p.pscore);

-- Self-join: same relation on both sides. Exercises DeduplicateRelations' NearestByJoin
-- arm, which rewrites the right side with fresh ExprIds so the join resolves. Each row's
-- nearest match by `abs(score - score)` is itself, so the output is deterministic.
SELECT a.user_id AS a_id, b.user_id AS b_id
FROM users a JOIN users b
  APPROX NEAREST 1 BY DISTANCE abs(a.score - b.score)
ORDER BY a.user_id, b.user_id;

-- Error: unsupported join type (RIGHT OUTER)
SELECT u.user_id, p.product
FROM users u RIGHT OUTER JOIN products p
  APPROX NEAREST 1 BY SIMILARITY -abs(u.score - p.pscore);

-- Error: num_results out of range (0)
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 0 BY SIMILARITY -abs(u.score - p.pscore);

-- Error: num_results out of range (100001)
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 100001 BY SIMILARITY -abs(u.score - p.pscore);

-- Error: non-orderable ranking expression
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 1 BY SIMILARITY map(u.score, p.pscore);

-- Error: EXACT mode with nondeterministic ranking expression
SELECT u.user_id, p.product
FROM users u JOIN products p
  EXACT NEAREST 1 BY SIMILARITY rand() + p.pscore;

-- APPROX permits a nondeterministic ranking expression (per the SPIP). Rows differ run to
-- run, so we only assert the row count: one match per left row when k = 1.
SELECT COUNT(*) AS num_rows
FROM (
  SELECT u.user_id, p.product
  FROM users u JOIN products p
    APPROX NEAREST 1 BY SIMILARITY rand() + p.pscore
);

-- Same with k = 2 to exercise the multi-match path with rand().
SELECT COUNT(*) AS num_rows
FROM (
  SELECT u.user_id, p.product
  FROM users u JOIN products p
    APPROX NEAREST 2 BY DISTANCE rand() + p.pscore
);

-- EXPLAIN of APPROX + nondeterministic ranking. Locks in the plan shape: the rewrite
-- injects a Project above the Join that materializes `rand(0) + p.pscore` as `__ranking__`,
-- An explicit seed is used so the EXPLAIN string is byte-stable across runs (without it,
-- `rand()` synthesizes a fresh random seed each time and the seed appears in the EXPLAIN).
EXPLAIN
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 1 BY SIMILARITY rand(0) + p.pscore;

-- spark.sql.crossJoin.enabled = false must NOT reject NEAREST BY queries.
-- The synthetic LEFT OUTER cross-join inside the rewrite is recognized structurally
-- by `CheckCartesianProducts` (its parent `Aggregate` contains `MaxMinByK`) and skipped.
SET spark.sql.crossJoin.enabled = false;

-- Basic NEAREST BY with crossJoin disabled.
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 1 BY SIMILARITY -abs(u.score - p.pscore);

-- NEAREST BY with a top-level filter on a right-side column. This exercises the path
-- where filter pushdown / column pruning may run between the rewrite (FinishAnalysis batch)
-- and `CheckCartesianProducts` (a much later batch).
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 2 BY DISTANCE abs(u.score - p.pscore)
WHERE p.product != 'C';

-- NEAREST BY with a top-level filter on a left-side column.
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 2 BY DISTANCE abs(u.score - p.pscore)
WHERE u.user_id > 1;

-- LEFT OUTER NEAREST BY with crossJoin disabled.
SELECT u.user_id, p.product
FROM users u LEFT OUTER JOIN products p
  EXACT NEAREST 1 BY DISTANCE abs(u.score - p.pscore);

-- EXPLAIN of a query whose left-side predicate (user_id > 1) is pushed down to the left
-- input of the rewrite's synthetic join. Demonstrates that CheckCartesianProducts succeeds
-- AFTER pushdown rules run, and that the rewrite's Aggregate -> Join shape is preserved in
-- the optimized plan.
EXPLAIN
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 2 BY DISTANCE abs(u.score - p.pscore)
WHERE u.user_id > 1;

-- EXPLAIN of a query whose right-side predicate (p.product != 'C') cannot push below the
-- rewrite's Generate(inline) and stays above it. Demonstrates that the optimizer pipeline
-- runs end-to-end without CheckCartesianProducts rejecting the synthetic join.
EXPLAIN
SELECT u.user_id, p.product
FROM users u JOIN products p
  APPROX NEAREST 2 BY DISTANCE abs(u.score - p.pscore)
WHERE p.product != 'C';

SET spark.sql.crossJoin.enabled = true;

-- Tie behavior: when multiple right rows have equal ranking values for a given left row,
-- MaxMinByK breaks ties arbitrarily (the SPIP marks tie-break as unspecified). We can't
-- pin specific rows, but the operator must still return exactly `numResults` matches per
-- left row when enough candidates exist.
CREATE OR REPLACE TEMP VIEW tied_products(product, pscore)
  AS VALUES ('A', 10.0), ('B', 10.0), ('C', 10.0);

SELECT u.user_id, COUNT(*) AS num_matches
FROM users u JOIN tied_products p
  APPROX NEAREST 2 BY DISTANCE abs(u.score - p.pscore)
GROUP BY u.user_id
ORDER BY u.user_id;

DROP VIEW tied_products;
DROP VIEW users;
DROP VIEW products;
