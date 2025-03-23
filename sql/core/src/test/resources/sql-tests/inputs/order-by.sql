-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

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

-- Clean up
DROP VIEW IF EXISTS testData;
