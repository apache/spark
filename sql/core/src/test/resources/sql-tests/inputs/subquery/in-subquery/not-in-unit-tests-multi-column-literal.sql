-- Unit tests for simple NOT IN predicate subquery across multiple columns.
--
-- See not-in-single-column-unit-tests.sql for an introduction.
-- This file has the same test cases as not-in-unit-tests-multi-column.sql with literals instead of
-- subqueries. Small changes have been made to the literals to make them typecheck.

--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=true
--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=false
--ONLY_IF spark

CREATE TEMPORARY VIEW m AS SELECT * FROM VALUES
  (null, null),
  (null, 1.0),
  (2, 3.0),
  (4, 5.0)
  AS m(a, b);

-- Case 1 (not possible to write a literal with no rows, so we ignore it.)
-- (subquery is empty -> row is returned)

-- Cases 2, 3 and 4 are currently broken, so I have commented them out here.
-- Filed https://issues.apache.org/jira/browse/SPARK-24395 to fix and restore these test cases.

  -- Case 5
  -- (one null column with no match -> row is returned)
SELECT *
FROM   m
WHERE  b = 1.0 -- Matches (null, 1.0)
       AND (a, b) NOT IN ((2, 3.0));

  -- Case 6
  -- (no null columns with match -> row not returned)
SELECT *
FROM   m
WHERE  b = 3.0 -- Matches (2, 3.0)
       AND (a, b) NOT IN ((2, 3.0));

  -- Case 7
  -- (no null columns with no match -> row is returned)
SELECT *
FROM   m
WHERE  b = 5.0 -- Matches (4, 5.0)
       AND (a, b) NOT IN ((2, 3.0));
