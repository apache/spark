-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

-- Aliases in SELECT could be used in GROUP BY
SELECT a AS k, COUNT(b) FROM testData GROUP BY k;
SELECT a AS k, COUNT(b) FROM testData GROUP BY k HAVING k > 1;
SELECT col1 AS k, SUM(col2) FROM testData AS t(col1, col2) GROUP BY k;

-- GROUP BY alias with invalid col in SELECT list
SELECT a AS k, COUNT(non_existing) FROM testData GROUP BY k;

-- Aggregate functions cannot be used in GROUP BY
SELECT COUNT(b) AS k FROM testData GROUP BY k;

-- turn off group by aliases
set spark.sql.groupByAliases=false;

-- Check analysis exceptions
SELECT a AS k, COUNT(b) FROM testData GROUP BY k;
