-- Unit tests for simple NOT IN predicate subquery across multiple columns.
--
-- See not-in-single-column-unit-tests.sql for an introduction.
--
-- Test cases for multi-column ``WHERE a NOT IN (SELECT c FROM r ...)'':
-- | # | does subquery include null?     | do filter columns contain null? | a = c? | b = d? | row included in result? |
-- | 1 | empty                           | *                               | *      | *      | yes                     |
-- | 2 | 1+ row has null for all columns | *                               | *      | *      | no                      |
-- | 3 | no row has null for all columns | (yes, yes)                      | *      | *      | no                      |
-- | 4 | no row has null for all columns | (no, yes)                       | yes    | *      | no                      |
-- | 5 | no row has null for all columns | (no, yes)                       | no     | *      | yes                     |
-- | 6 | no                              | (no, no)                        | yes    | yes    | no                      |
-- | 7 | no                              | (no, no)                        | _      | _      | yes                     |
--
-- This can be generalized to include more tests for more columns, but it covers the main cases
-- when there is more than one column.

CREATE TEMPORARY VIEW m AS SELECT * FROM VALUES
  (null, null),
  (null, 1.0),
  (2, 3.0),
  (4, 5.0)
  AS m(a, b);

CREATE TEMPORARY VIEW s AS SELECT * FROM VALUES
  (null, null),
  (0, 1.0),
  (2, 3.0),
  (4, null)
  AS s(c, d);

  -- Case 1
  -- (subquery is empty -> row is returned)
SELECT *
FROM   m
WHERE  (a, b) NOT IN (SELECT *
                      FROM   s
                      WHERE  d > 5.0) -- Matches no rows
;

  -- Case 2
  -- (subquery contains a row with null in all columns -> row not returned)
SELECT *
FROM   m
WHERE  (a, b) NOT IN (SELECT *
                      FROM s
                      WHERE c IS NULL AND d IS NULL) -- Matches only (null, null)
;

  -- Case 3
  -- (probe-side columns are all null -> row not returned)
SELECT *
FROM   m
WHERE  a IS NULL AND b IS NULL -- Matches only (null, null)
       AND (a, b) NOT IN (SELECT *
                          FROM s
                          WHERE c IS NOT NULL) -- Matches (0, 1.0), (2, 3.0), (4, null)
;

  -- Case 4
  -- (one column null, other column matches a row in the subquery result -> row not returned)
SELECT *
FROM   m
WHERE  b = 1.0 -- Matches (null, 1.0)
       AND (a, b) NOT IN (SELECT *
                          FROM s
                          WHERE c IS NOT NULL) -- Matches (0, 1.0), (2, 3.0), (4, null)
;

  -- Case 5
  -- (one null column with no match -> row is returned)
SELECT *
FROM   m
WHERE  b = 1.0 -- Matches (null, 1.0)
       AND (a, b) NOT IN (SELECT *
                          FROM s
                          WHERE c = 2) -- Matches (2, 3.0)
;

  -- Case 6
  -- (no null columns with match -> row not returned)
SELECT *
FROM   m
WHERE  b = 3.0 -- Matches (2, 3.0)
       AND (a, b) NOT IN (SELECT *
                          FROM s
                          WHERE c = 2) -- Matches (2, 3.0)
;

  -- Case 7
  -- (no null columns with no match -> row is returned)
SELECT *
FROM   m
WHERE  b = 5.0 -- Matches (4, 5.0)
       AND (a, b) NOT IN (SELECT *
                          FROM s
                          WHERE c = 2) -- Matches (2, 3.0)
;
