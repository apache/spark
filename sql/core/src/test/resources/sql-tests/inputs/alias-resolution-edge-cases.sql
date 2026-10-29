CREATE TABLE t1(col1 INT, col2 STRING);
CREATE TABLE t2(col1 STRUCT<a: STRING>, a STRING);

-- Update references to match fixed-point plan when realiasing expressions in LCA resolution
SELECT LEN(LOWER('X')) AS a, 1 AS b, b AS c GROUP BY LOWER('X') ORDER BY LOWER('X');
SELECT LEN(LOWER('X')) AS a, 1 AS b, b AS c GROUP BY LOWER('X') HAVING LOWER('X') = 'x';

-- LCA referencing nested field
SELECT col1.field, field FROM VALUES(named_struct('field', 1));
SELECT col1.field, field FROM VALUES(map('field', 1));

-- LCA attributes shouldn't be counted in UNRESOLVED_GROUP_BY_ALL validation
SELECT COUNT(col1) as alias, SUM(col1) + alias FROM t1 GROUP BY ALL;
SELECT COUNT(col1) as alias, SUM(col1) + alias, SUM(col1) + col1 FROM t1 GROUP BY ALL;

-- A window function inside a scalar subquery does not make a SELECT with lateral column aliases
-- a window query block.
-- LCA in a Project, subquery not laterally referenced.
SELECT 1 AS a, a + 1 AS b,
  (SELECT MAX(rn) FROM (SELECT ROW_NUMBER() OVER (ORDER BY col1) AS rn FROM VALUES (1), (2))) AS c;
-- A window function inside an aggregate function is still rejected.
SELECT 1 AS a, a + 1 AS b, SUM(ROW_NUMBER() OVER (ORDER BY col1)) AS s FROM VALUES (1), (2);
-- LCA in a Project, subquery laterally referenced.
SELECT 1 AS a,
  (SELECT MAX(rn) FROM (SELECT ROW_NUMBER() OVER (ORDER BY col1) AS rn FROM VALUES (1), (2))) AS c,
  c + a AS d;
-- LCA in an Aggregate, subquery not laterally referenced.
SELECT col1 AS a, a + 1 AS b,
  (SELECT MAX(rn) FROM (SELECT ROW_NUMBER() OVER (ORDER BY c) AS rn FROM VALUES (1), (2) AS t(c)))
    AS c
FROM VALUES (1), (2) GROUP BY col1;
-- Window function inside EXISTS and IN subqueries.
SELECT 1 AS a, a + 1 AS b, EXISTS (SELECT ROW_NUMBER() OVER (ORDER BY col1) FROM VALUES (1)) AS c;
SELECT 1 AS a, a + 1 AS b,
  b IN (SELECT ROW_NUMBER() OVER (ORDER BY col1) FROM VALUES (1), (2)) AS c;
-- A window function in the SELECT list next to the subquery makes it a window query block.
SELECT col1 AS a, a + 1 AS b,
  (SELECT MAX(rn) FROM (SELECT ROW_NUMBER() OVER (ORDER BY c) AS rn FROM VALUES (1), (2) AS t(c)))
    AS c,
  ROW_NUMBER() OVER (ORDER BY col1) AS d
FROM VALUES (1), (2);
-- A window function inside an aggregate function: other functions, with GROUP BY, without LCA, and
-- inside a scalar subquery.
SELECT 1 AS a, a + 1 AS b, MAX(RANK() OVER (ORDER BY col1)) AS s FROM VALUES (1), (2);
SELECT col1 AS a, a + 1 AS b, SUM(ROW_NUMBER() OVER (ORDER BY col1)) AS s
FROM VALUES (1), (2) GROUP BY col1;
SELECT SUM(ROW_NUMBER() OVER (ORDER BY col1)) AS s FROM VALUES (1), (2);
SELECT 1 AS a, a + 1 AS b,
  (SELECT SUM(ROW_NUMBER() OVER (ORDER BY col1)) FROM VALUES (1), (2)) AS c;

DROP TABLE t1;
DROP TABLE t2;
