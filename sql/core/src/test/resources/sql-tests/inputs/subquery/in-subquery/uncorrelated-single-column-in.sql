-- Unit tests for uncorrelated single-column IN subquery
--
-- An IN subquery is uncorrelated only if the left values are literals and the subquery
-- has no outer reference.
-- Example: `3 IN (SELECT b FROM t WHERE t.a > 2)`
-- Assume the subquery returns a result set (b1, b2, b3, ...), then the IN subquery should
-- behave as `(3 = b1) OR (3 = b2) OR (3 = b3) OR ...`. So if there is no '3' in the result set,
-- and there is a 'null', the IN subquery should return 'null'.
--
-- Test cases for uncorrelated single-column IN subquery `a IN (SELECT b FROM t ...)`:
-- | # | is `a` in the subquery's result set? | `a` is null? | does `b` have nulls? | result of IN subquery |
-- | 1 |                no                    |     no       |         no           |        FALSE          |
-- | 2 |                no                    |     no       |         yes          |        UNKNOWN        |
-- | 3 |                no                    |     yes      |         no           |        UNKNOWN        |
-- | 4 |                no                    |     yes      |         yes          |        UNKNOWN        |
-- | 5 |                yes                   |     no       |         no           |        TRUE           |
-- | 6 |                yes                   |     no       |         yes          |        TRUE           |

CREATE TEMPORARY VIEW s AS SELECT * FROM VALUES
   (1, 1),
   (2, 2),
   (3, null)
AS s(sa, sb);

CREATE TEMPORARY VIEW t AS SELECT * FROM VALUES
   (1)
AS t(ta);

-- Note that all of these test cases should return one row.

-- CASE 1
-- `a` is not in the subquery's result set and there is no null.
SELECT *
FROM   t
WHERE  NOT (3 IN (SELECT sb
                  FROM   s
                  WHERE  sa <= 2));

-- CASE 2
-- 'a' is not in the subquery's result set and the subquery has nulls.
SELECT *
FROM   t
WHERE  (3 IN (SELECT sb
              FROM   s)) IS NULL;

-- CASE 3
-- 'a' is null, and the subquery has no nulls.
SELECT *
FROM   t
WHERE  (NULL IN (SELECT sb
                 FROM   s
                 WHERE  sa <= 2)) IS NULL;

-- CASE 4
-- 'a' is null, and the subquery has nulls.
SELECT *
FROM   t
WHERE  (NULL IN (SELECT sb
                 FROM   s)) IS NULL;

-- CASE 5
-- 'a' is in the subquery's result set, and the result set has no nulls.
SELECT *
FROM   t
WHERE  2 IN (SELECT sb
             FROM   s
             WHERE  sa <= 2);

-- CASE 6
-- 'a' is in the subquery's result set, and the result set has nulls.
SELECT *
FROM   t
WHERE  2 IN (SELECT sb
             FROM   s);
