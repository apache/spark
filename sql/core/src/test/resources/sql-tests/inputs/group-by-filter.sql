-- Test filter clause for aggregate expression.

-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

-- Aggregate with filter and empty GroupBy expressions.
SELECT a, COUNT(b) FILTER (WHERE a >= 2) FROM testData;
SELECT COUNT(a) FILTER (WHERE a = 1), COUNT(b) FILTER (WHERE a > 1) FROM testData;

-- Aggregate with filter and non-empty GroupBy expressions.
SELECT a, COUNT(b) FILTER (WHERE a >= 2) FROM testData GROUP BY a;
SELECT a, COUNT(b) FILTER (WHERE a != 2) FROM testData GROUP BY b;
SELECT COUNT(a) FILTER (WHERE a >= 0), COUNT(b) FILTER (WHERE a >= 3) FROM testData GROUP BY a;

-- Aggregate with filter and grouped by literals.
SELECT 'foo', COUNT(a) FILTER (WHERE b <= 2) FROM testData GROUP BY 1;

-- Aggregate with filter and grouped by literals (hash aggregate).
SELECT 'foo', APPROX_COUNT_DISTINCT(a) FILTER (WHERE b >= 0) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate with filter and grouped by literals (sort aggregate).
SELECT 'foo', MAX(STRUCT(a)) FILTER (WHERE b >= 1) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate with filter and complex GroupBy expressions.
SELECT a + b, COUNT(b) FILTER (WHERE b >= 2) FROM testData GROUP BY a + b;
SELECT a + 2, COUNT(b) FILTER (WHERE b IN (1, 2)) FROM testData GROUP BY a + 1;
SELECT a + 1 + 1, COUNT(b) FILTER (WHERE b > 0) FROM testData GROUP BY a + 1;
