-- Test cases for HAVING/ORDER BY with aggregate expressions referencing grouping columns
-- in GROUPING SETS/CUBE/ROLLUP queries. Validates that SUM(b) in HAVING/ORDER BY
-- resolves b to the original pre-Expand column (always holds the real value), not the
-- Expand-output copy (NULL for rolled-up groups).

CREATE TEMPORARY VIEW t AS SELECT * FROM VALUES (1, 10), (2, 20), (1, 30), (2, 40) AS t(a, b);

-- HAVING with aggregate referencing grouping column - GROUPING SETS
SELECT a, b, SUM(b) AS sb
FROM t
GROUP BY GROUPING SETS ((a, b), (a))
HAVING SUM(b) > 15
ORDER BY a, b;

-- HAVING with aggregate referencing grouping column - CUBE
SELECT a, b, SUM(b) AS sb
FROM t
GROUP BY CUBE(a, b)
HAVING SUM(b) > 15
ORDER BY a, b;

-- HAVING with aggregate referencing grouping column - ROLLUP
SELECT a, b, SUM(b) AS sb
FROM t
GROUP BY ROLLUP(a, b)
HAVING SUM(b) > 15
ORDER BY a, b;

-- ORDER BY with aggregate referencing grouping column - GROUPING SETS
SELECT a, b, SUM(b) AS sb
FROM t
GROUP BY GROUPING SETS ((a, b), (a))
ORDER BY SUM(b), a, b;

-- ORDER BY with aggregate referencing grouping column - CUBE
SELECT a, b, SUM(b) AS sb
FROM t
GROUP BY CUBE(a, b)
ORDER BY SUM(b), a, b;

-- ORDER BY with aggregate referencing grouping column - ROLLUP
SELECT a, b, SUM(b) AS sb
FROM t
GROUP BY ROLLUP(a, b)
ORDER BY SUM(b), a, b;

-- HAVING with aggregate NOT in SELECT list
SELECT a, b, SUM(b) AS sb
FROM t
GROUP BY GROUPING SETS ((a, b), (a))
HAVING COUNT(b) > 1
ORDER BY a, b;

-- HAVING with bare grouping column (should resolve to Expand copy, not original)
SELECT a, b, SUM(b) AS sb
FROM t
GROUP BY GROUPING SETS ((a, b), (a))
HAVING b IS NOT NULL
ORDER BY a, b;

-- HAVING with both bare and aggregated reference to same grouping column
SELECT a, b, SUM(b) AS sb
FROM t
GROUP BY GROUPING SETS ((a, b), (a))
HAVING b IS NOT NULL AND SUM(b) > 15
ORDER BY a, b;

-- HAVING with non-grouping column outside aggregate should error
SELECT a, SUM(b) AS sb
FROM VALUES (1, 10), (2, 20) AS t(a, b)
GROUP BY GROUPING SETS ((a))
HAVING b > 10;

-- HAVING with nested aggregate should error
SELECT a, SUM(b) AS sb
FROM VALUES (1, 10), (2, 20) AS t(a, b)
GROUP BY GROUPING SETS ((a, b), (a))
HAVING SUM(MAX(b)) > 10;

-- HAVING with expression grouping key
SELECT a + 1 AS ak, b, SUM(b) AS sb
FROM t
GROUP BY GROUPING SETS ((a + 1, b), (a + 1))
HAVING SUM(b) > 15
ORDER BY ak, b;

-- HAVING with aggregate over nested expression using grouping columns
SELECT a, b, SUM(a + b) AS s
FROM t
GROUP BY GROUPING SETS ((a, b), (a))
HAVING SUM(a + b) > 20
ORDER BY a, b;

-- HAVING and ORDER BY with aggregate over expression grouping key
SELECT a + 1 AS ak, b, SUM(a + 1) AS s
FROM t
GROUP BY GROUPING SETS ((a + 1, b), (a + 1))
HAVING SUM(a + 1) > 3
ORDER BY SUM(a + 1), ak, b;

-- Clean up
DROP VIEW t;
