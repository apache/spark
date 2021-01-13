-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (1, 1), (null, 2), (1, null), (null, null)
AS testData(a, b);

-- count with single expression
SELECT
  count(*), count(1), count(null), count(a), count(b), count(a + b), count((a, b))
FROM testData;

-- distinct count with single expression
SELECT
  count(DISTINCT 1),
  count(DISTINCT null),
  count(DISTINCT a),
  count(DISTINCT b),
  count(DISTINCT (a + b)),
  count(DISTINCT (a, b))
FROM testData;

-- count with multiple expressions
SELECT count(a, b), count(b, a), count(testData.*) FROM testData;

-- distinct count with multiple expressions
SELECT
  count(DISTINCT a, b), count(DISTINCT b, a), count(DISTINCT *), count(DISTINCT testData.*)
FROM testData;

-- distinct count with multiple literals
SELECT count(DISTINCT 3,2);
SELECT count(DISTINCT 2), count(DISTINCT 2,3);
SELECT count(DISTINCT 2), count(DISTINCT 3,2);
SELECT count(DISTINCT a), count(DISTINCT 2,3) FROM testData;
SELECT count(DISTINCT a), count(DISTINCT 3,2) FROM testData;
SELECT count(DISTINCT a), count(DISTINCT 2), count(DISTINCT 2,3) FROM testData;
SELECT count(DISTINCT a), count(DISTINCT 2), count(DISTINCT 3,2) FROM testData;
SELECT count(distinct 0.8), percentile_approx(distinct a, 0.8) FROM testData;

-- legacy behavior: allow calling function count without parameters
set spark.sql.legacy.allowParameterlessCount=true;
SELECT count() FROM testData;

-- count without expressions
set spark.sql.legacy.allowParameterlessCount=false;
SELECT count() FROM testData;
