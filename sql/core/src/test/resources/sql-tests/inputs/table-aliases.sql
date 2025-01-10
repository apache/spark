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

-- Subquery aliases in FROM clause
SELECT * FROM (SELECT 1 AS a, 1 AS b) t(col1, col2);

SELECT t.* FROM (SELECT 1 AS a, 1 AS b) t(col1, col2);

SELECT col1, col2 FROM (SELECT 1 AS a, 1 AS b) t(col1, col2);

SELECT t.col1, t.col2 FROM (SELECT 1 AS a, 1 AS b) t(col1, col2);

-- Aliases for join relations in FROM clause
CREATE OR REPLACE TEMPORARY VIEW src1 AS SELECT * FROM VALUES (1, "a"), (2, "b"), (3, "c") AS src1(id, v1);

CREATE OR REPLACE TEMPORARY VIEW src2 AS SELECT * FROM VALUES (2, 1.0), (3, 3.2), (1, 8.5) AS src2(id, v2);

SELECT * FROM (src1 s1 INNER JOIN src2 s2 ON s1.id = s2.id) dst(a, b, c, d);

SELECT dst.* FROM (src1 s1 INNER JOIN src2 s2 ON s1.id = s2.id) dst(a, b, c, d);

-- Negative examples after aliasing
SELECT src1.* FROM src1 a ORDER BY id LIMIT 1;

SELECT src1.id FROM (SELECT * FROM src1 ORDER BY id LIMIT 1) a;
