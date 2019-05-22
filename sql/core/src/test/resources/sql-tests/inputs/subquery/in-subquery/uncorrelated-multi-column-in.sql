-- Unit tests for uncorrelated multi-column IN subquery
--
-- Example: `(3, 4) IN (SELECT a, b FROM t WHERE ...)`
-- Assume the subquery returns a result set ((a1, b1), (a2, b2), (a2, b3), ...), then the IN subquery should
-- behave as `(3 = a1 AND 4 = b1) OR (3 = a2 AND 4= b2) OR (3 = a3 AND 4 = b3) OR ...`.
--
-- Test cases for uncorrelated multi-column IN subquery `(a, b) IN (SELECT x, y FROM t ...)`,
-- we have 3 conditions in these tests:
-- C1. exists a tuple makes (a = x AND b = y) return TRUE
-- C2. exists a tuple makes (a = x AND b = y) return FALSE
-- C3. exists a tuple makes (a = x AND b = y) return UNKNOWN
-- | # |  C1  |  C2  |  C3  |  result of the IN subquery |
-- | 1 |  yes |  ?   |   ?  |           TRUE             |
-- | 2 |  no  |  yes |  no  |           FALSE            |
-- | 3 |  no  |  yes |  yes |          UNKNOWN           |
-- | 4 |  no  |  no  |  yes |          UNKNOWN           |
-- | 5 |  empty result set  |           FALSE            |

CREATE TEMPORARY VIEW s AS SELECT * FROM VALUES
   (1, 1),
   (2, 2),
   (3, null)
AS s(x, y);

CREATE TEMPORARY VIEW t AS SELECT * FROM VALUES
   (1)
AS t(ta);

-- Note that all of these test cases should return one row.

-- CASE 1
-- If there is a tuple satisfying (a = x AND b = y), then whatever
-- other tuples are, the IN subquery returns true.

-- CASE 1.1
SELECT *
FROM   t
WHERE  (1, 1) IN (SELECT x, y
                  FROM   s);

-- CASE 1.2
SELECT *
FROM   t
WHERE  (1, 1) IN (SELECT x, y
                  FROM   s
                  WHERE  x <= 2);

-- CASE 1.3
SELECT *
FROM   t
WHERE  (1, 1) IN (SELECT x, y
                  FROM   s
                  WHERE  x != 2);

-- CASE 2
-- If there is no tuple satisfying (a = x AND b = y) and no tuple having nulls,
-- then the IN subquery returns false.
SELECT *
FROM   t
WHERE  NOT ((1, 2) IN (SELECT x, y
                       FROM   s
                       WHERE  x <= 2));

-- CASE 3
-- The subquery returns (1, 1), (2, 2), (3, null),
-- (3 = 1 AND 2 = 1) => false
-- (3 = 2 AND 2 = 2) => fasle
-- (3 = 3 AND 2 = null) => null
-- therefore, the IN subquery returns null.
SELECT *
FROM   t
WHERE  ((3, 2) IN (SELECT x, y
                   FROM   s)) IS NULL;

-- CASE 4
-- If all the expressions '(a = x AND b = y)' return unknown, the IN subquery returns unknown.

-- CASE 4.1
-- The subquery returns (3, null), so that (3 = 3 AND 2 = null) returns null.
SELECT *
FROM   t
WHERE  ((3, 2) IN (SELECT x, y
                   FROM   s
                   WHERE  x = 3)) IS NULL;

-- CASE 4.2
-- The subquery returns (1, 1), (2, 2), (3, null), but the (2 = 3 AND 10 = null) returns false
-- so that the IN subquery returns false here.
SELECT *
FROM   t
WHERE  NOT ((2, 10) IN (SELECT x, y
                        FROM   s));

-- CASE 4.3
SELECT *
FROM   t
WHERE  ((null, 2) IN (SELECT x, y
                      FROM   s)) IS NULL;

-- CASE 5
-- If the subquery's result set is empty, the IN subquery returns false.
SELECT *
FROM   t
WHERE  NOT ((null, 2) IN (SELECT x, y
                          FROM   s
                          WHERE  x > 10));
