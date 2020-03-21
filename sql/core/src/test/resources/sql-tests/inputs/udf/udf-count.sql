-- This test file was converted from count.sql
-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (1, 1), (null, 2), (1, null), (null, null)
AS testData(a, b);

-- count with single expression
SELECT
  udf(count(*)), udf(count(1)), udf(count(null)), udf(count(a)), udf(count(b)), udf(count(a + b)), udf(count((a, b)))
FROM testData;

-- distinct count with single expression
SELECT
  udf(count(DISTINCT 1)),
  udf(count(DISTINCT null)),
  udf(count(DISTINCT a)),
  udf(count(DISTINCT b)),
  udf(count(DISTINCT (a + b))),
  udf(count(DISTINCT (a, b)))
FROM testData;

-- count with multiple expressions
SELECT udf(count(a, b)), udf(count(b, a)), udf(count(testData.*)) FROM testData;

-- distinct count with multiple expressions
SELECT
  udf(count(DISTINCT a, b)), udf(count(DISTINCT b, a)), udf(count(DISTINCT *)), udf(count(DISTINCT testData.*))
FROM testData;
