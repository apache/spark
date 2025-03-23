-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

-- ORDER BY alias should work with case insensitive names
SELECT a FROM testData ORDER BY A;

-- Aliases in SELECT could be used in ORDER BY
SELECT a as alias FROM testData ORDER BY ALIAS;

-- ORDER BY literal
SELECT a AS k FROM testData ORDER BY 'k';
SELECT 1 AS k FROM testData ORDER BY 'k';

-- ORDER BY alias with the function name
SELECT concat_ws(' ', a, b) FROM testData ORDER BY `concat_ws( , a, b)`;

-- ORDER BY column with name same as an alias used in the project list
SELECT 1 AS a FROM testData ORDER BY a;
SELECT 1 AS a FROM testData ORDER BY `a`;

-- ORDER BY implicit alias
SELECT 1 ORDER BY `1`;

-- ORDER BY with expression subqueries
SELECT a, b FROM testData ORDER BY a, (SELECT b FROM testData LIMIT 1);

-- ORDER BY more than one row
SELECT a FROM testData ORDER BY (SELECT b FROM testData);

-- Unsupported expressions in ORDER BY
SELECT a, b FROM testData ORDER BY a, b IN (SELECT a FROM testData);
SELECT a, b FROM testData ORDER BY a, EXISTS(SELECT b FROM testData);

-- ORDER BY alias with invalid col in SELECT list
SELECT a AS k, c FROM testData ORDER BY k;
