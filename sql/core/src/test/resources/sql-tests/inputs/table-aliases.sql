-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES (1, 1), (1, 2), (2, 1) AS testData(a, b);

-- Table column aliases in FROM clause
SELECT * FROM testData AS t(col1, col2) WHERE col1 = 1;

SELECT * FROM testData AS t(col1, col2) WHERE col1 = 2;

SELECT col1 AS k, SUM(col2) FROM testData AS t(col1, col2) GROUP BY k;

-- Aliasing the wrong number of columns in the FROM clause
SELECT * FROM testData AS t(col1, col2, col3);

SELECT * FROM testData AS t(col1);

-- Check alias duplication
SELECT a AS col1, b AS col2 FROM testData AS t(c, d);
