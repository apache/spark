-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);
CREATE OR REPLACE TEMPORARY VIEW v1 AS SELECT 1 AS col1;
CREATE OR REPLACE TEMPORARY VIEW v2 AS SELECT 1 AS col1;

-- ORDER BY a column from a child's output
SELECT a FROM testData ORDER BY a;
SELECT a FROM testData GROUP BY a, b ORDER BY a;

-- ORDER BY a column from an output below a child's one
SELECT b FROM testData WHERE a > 1 ORDER BY a;

-- ORDER BY a column from grouping expressions list
SELECT b FROM testData GROUP BY a, b ORDER BY a;

-- ORDER BY a nested column from an output below a child's one
SELECT col1 FROM VALUES (1, named_struct('f1', 1)) ORDER BY col2.f1;
SELECT col1 FROM VALUES (1, named_struct('f1', named_struct('f2', 1))) ORDER BY col2.f1.f2;

-- ORDER BY column can't reference an outer scope
SELECT a, (SELECT b FROM testData GROUP BY b HAVING b > 1 ORDER BY a) FROM testData;

-- Column resolution from the child's output takes the precedence over `ORDER BY ALL`
SELECT a, (SELECT b FROM VALUES (1, 2) AS innerTestData (all, b) ORDER BY ALL) FROM testData;

-- ORDER BY with scalar subqueries
SELECT * FROM testData ORDER BY (SELECT a FROM testData ORDER BY b);
SELECT * FROM testData ORDER BY (SELECT * FROM testData ORDER BY (SELECT a FROM testData ORDER BY b));

-- Fails because correlation is not allowed in ORDER BY
SELECT * FROM testData ORDER BY (SELECT a FROM VALUES (1));

-- Nondeterministic expression + Aggregate expression in ORDER BY
SELECT col1 FROM VALUES (1, 2) GROUP BY col1 ORDER BY MAX(col2), RAND();

-- Order by table column and alias with the same name
SELECT 1 AS col1, col1 FROM VALUES (10) ORDER BY col1;

-- Order by on top of natural join with count(distinct)
SELECT
  COUNT(DISTINCT col1)
FROM
  v1
NATURAL JOIN
  v2
GROUP BY
  col1
ORDER BY
  col1
;

SELECT
  COUNT(DISTINCT col1)
FROM
  v1
NATURAL JOIN
  v1
GROUP BY
  col1
ORDER BY
  col1
;

-- Order by non-orderable expressions should fail
SELECT 'a' ORDER BY CAST('a' AS VARIANT);
SELECT a, b FROM testData ORDER BY CAST(a + b AS VARIANT);
SELECT a FROM testData ORDER BY CAST(CAST(a AS VARIANT) AS VARIANT);
SELECT a FROM testData GROUP BY a ORDER BY CAST(CAST(a AS STRING) AS VARIANT);
SELECT * FROM testData ORDER BY CAST(null AS VARIANT);
SELECT a FROM testData ORDER BY ARRAY(CAST(true AS VARIANT));
SELECT a FROM testData ORDER BY CAST(ARRAY('a') AS VARIANT);
SELECT a FROM testData ORDER BY CAST(ARRAY(CAST(true AS VARIANT)) AS VARIANT);

-- Clean up
DROP VIEW IF EXISTS testData;
DROP VIEW IF EXISTS v1;
DROP VIEW IF EXISTS v2;
