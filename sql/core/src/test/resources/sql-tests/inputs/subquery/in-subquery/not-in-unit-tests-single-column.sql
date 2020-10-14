-- Unit tests for simple NOT IN predicate subquery across a single column.
--
-- ``col NOT IN expr'' is quite difficult to reason about. There are many edge cases, some of the
-- rules are confusing to the uninitiated, and precedence and treatment of null values is plain
-- unintuitive. To make this simpler to understand, I've come up with a plain English way of
-- describing the expected behavior of this query.
--
-- - If the subquery is empty (i.e. returns no rows), the row should be returned, regardless of
--   whether the filtered columns include nulls.
-- - If the subquery contains a result with all columns null, then the row should not be returned.
-- - If for all non-null filter columns there exists a row in the subquery in which each column
--   either
--   1. is equal to the corresponding filter column or
--   2. is null
--   then the row should not be returned. (This includes the case where all filter columns are
--   null.)
-- - Otherwise, the row should be returned.
--
-- Using these rules, we can come up with a set of test cases for single-column and multi-column
-- NOT IN test cases.
--
-- Test cases for single-column ``WHERE a NOT IN (SELECT c FROM r ...)'':
-- | # | does subquery include null? | is a null? | a = c? | row with a included in result? |
-- | 1 | empty                       |            |        | yes                            |
-- | 2 | yes                         |            |        | no                             |
-- | 3 | no                          | yes        |        | no                             |
-- | 4 | no                          | no         | yes    | no                             |
-- | 5 | no                          | no         | no     | yes                            |
--
-- There are also some considerations around correlated subqueries. Correlated subqueries can
-- cause cases 2, 3, or 4 to be reduced to case 1 by limiting the number of rows returned by the
-- subquery, so the row from the parent table should always be included in the output.

--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=true
--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=false

CREATE TEMPORARY VIEW m AS SELECT * FROM VALUES
  (null, 1.0),
  (2, 3.0),
  (4, 5.0)
  AS m(a, b);

CREATE TEMPORARY VIEW s AS SELECT * FROM VALUES
  (null, 1.0),
  (2, 3.0),
  (6, 7.0)
  AS s(c, d);

  -- Uncorrelated NOT IN Subquery test cases
  -- Case 1
  -- (empty subquery -> all rows returned)
SELECT *
FROM   m
WHERE  a NOT IN (SELECT c
                 FROM   s
                 WHERE  d > 10.0) -- (empty subquery)
;

  -- Case 2
  -- (subquery includes null -> no rows returned)
SELECT *
FROM   m
WHERE  a NOT IN (SELECT c
                 FROM   s
                 WHERE  d = 1.0) -- Only matches (null, 1.0)
;

  -- Case 3
  -- (probe column is null -> row not returned)
SELECT *
FROM   m
WHERE  b = 1.0 -- Only matches (null, 1.0)
       AND a NOT IN (SELECT c
                     FROM   s
                     WHERE  d = 3.0) -- Matches (2, 3.0)
;

  -- Case 4
  -- (probe column matches subquery row -> row not returned)
SELECT *
FROM   m
WHERE  b = 3.0 -- Only matches (2, 3.0)
       AND a NOT IN (SELECT c
                     FROM   s
                     WHERE  d = 3.0) -- Matches (2, 3.0)
;

  -- Case 5
  -- (probe column does not match subquery row -> row is returned)
SELECT *
FROM   m
WHERE  b = 3.0 -- Only matches (2, 3.0)
       AND a NOT IN (SELECT c
                     FROM   s
                     WHERE  d = 7.0) -- Matches (6, 7.0)
;

  -- Correlated NOT IN subquery test cases
  -- Case 2->1
  -- (subquery had nulls but they are removed by correlated subquery -> all rows returned)
SELECT *
FROM   m
WHERE a NOT IN (SELECT c
                FROM   s
                WHERE  d = b + 10) -- Matches no row
;

  -- Case 3->1
  -- (probe column is null but subquery returns no rows -> row is returned)
SELECT *
FROM   m
WHERE  b = 1.0 -- Only matches (null, 1.0)
       AND a NOT IN (SELECT c
                     FROM   s
                     WHERE  d = b + 10) -- Matches no row
;

  -- Case 4->1
  -- (probe column matches row which is filtered out by correlated subquery -> row is returned)
SELECT *
FROM   m
WHERE  b = 3.0 -- Only matches (2, 3.0)
       AND a NOT IN (SELECT c
                     FROM   s
                     WHERE  d = b + 10) -- Matches no row
;
