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
