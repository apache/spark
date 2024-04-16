-- Unit tests for simple NOT IN with a literal expression of a single column
--
-- More information can be found in not-in-unit-tests-single-column.sql.
-- This file has the same test cases as not-in-unit-tests-single-column.sql with literals instead of
-- subqueries.

--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=true
--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=false
--ONLY_IF spark

CREATE TEMPORARY VIEW m AS SELECT * FROM VALUES
  (null, 1.0),
  (2, 3.0),
  (4, 5.0)
  AS m(a, b);

  -- Uncorrelated NOT IN Subquery test cases
  -- Case 1 (not possible to write a literal with no rows, so we ignore it.)
  -- (empty subquery -> all rows returned)

  -- Case 2
  -- (subquery includes null -> no rows returned)
SELECT *
FROM   m
WHERE  a NOT IN (null);

  -- Case 3
  -- (probe column is null -> row not returned)
SELECT *
FROM   m
WHERE  b = 1.0 -- Only matches (null, 1.0)
       AND a NOT IN (2);

  -- Case 4
  -- (probe column matches subquery row -> row not returned)
SELECT *
FROM   m
WHERE  b = 3.0 -- Only matches (2, 3.0)
       AND a NOT IN (2);

  -- Case 5
  -- (probe column does not match subquery row -> row is returned)
SELECT *
FROM   m
WHERE  b = 3.0 -- Only matches (2, 3.0)
       AND a NOT IN (6);
