-- This test file was converted from group-by.sql.
-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

-- Aggregate with empty GroupBy expressions.
SELECT udf(a), udf(COUNT(b)) FROM testData;
SELECT COUNT(udf(a)), udf(COUNT(b)) FROM testData;

-- Aggregate with non-empty GroupBy expressions.
SELECT udf(a), COUNT(udf(b)) FROM testData GROUP BY a;
SELECT udf(a), udf(COUNT(udf(b))) FROM testData GROUP BY b;
SELECT COUNT(udf(a)), COUNT(udf(b)) FROM testData GROUP BY udf(a);

-- Aggregate grouped by literals.
SELECT 'foo', COUNT(udf(a)) FROM testData GROUP BY 1;

-- Aggregate grouped by literals (whole stage code generation).
SELECT 'foo' FROM testData WHERE a = 0 GROUP BY udf(1);

-- Aggregate grouped by literals (hash aggregate).
SELECT 'foo', udf(APPROX_COUNT_DISTINCT(udf(a))) FROM testData WHERE a = 0 GROUP BY udf(1);

-- Aggregate grouped by literals (sort aggregate).
SELECT 'foo', MAX(STRUCT(udf(a))) FROM testData WHERE a = 0 GROUP BY udf(1);

-- Aggregate with complex GroupBy expressions.
SELECT udf(a + b), udf(COUNT(b)) FROM testData GROUP BY a + b;
SELECT udf(a + 2), udf(COUNT(b)) FROM testData GROUP BY a + 1;
SELECT udf(a + 1) + 1, udf(COUNT(b)) FROM testData GROUP BY udf(a + 1);

-- Aggregate with nulls.
SELECT SKEWNESS(udf(a)), udf(KURTOSIS(a)), udf(MIN(a)), MAX(udf(a)), udf(AVG(udf(a))), udf(VARIANCE(a)), STDDEV(udf(a)), udf(SUM(a)), udf(COUNT(a))
FROM testData;

-- Aggregate with foldable input and multiple distinct groups.
SELECT COUNT(DISTINCT udf(b)), udf(COUNT(DISTINCT b, c)) FROM (SELECT 1 AS a, 2 AS b, 3 AS c) GROUP BY udf(a);

-- Aliases in SELECT could be used in GROUP BY
SELECT udf(a) AS k, COUNT(udf(b)) FROM testData GROUP BY k;
SELECT a AS k, udf(COUNT(b)) FROM testData GROUP BY k HAVING k > 1;

-- Aggregate functions cannot be used in GROUP BY
SELECT udf(COUNT(b)) AS k FROM testData GROUP BY k;

-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testDataHasSameNameWithAlias AS SELECT * FROM VALUES
(1, 1, 3), (1, 2, 1) AS testDataHasSameNameWithAlias(k, a, v);
SELECT k AS a, udf(COUNT(udf(v))) FROM testDataHasSameNameWithAlias GROUP BY udf(a);

-- turn off group by aliases
set spark.sql.groupByAliases=false;

-- Check analysis exceptions
SELECT a AS k, udf(COUNT(udf(b))) FROM testData GROUP BY k;

-- Aggregate with empty input and non-empty GroupBy expressions.
SELECT udf(a), COUNT(udf(1)) FROM testData WHERE false GROUP BY udf(a);

-- Aggregate with empty input and empty GroupBy expressions.
SELECT udf(COUNT(1)) FROM testData WHERE false;
SELECT 1 FROM (SELECT udf(COUNT(1)) FROM testData WHERE false) t;

-- Aggregate with empty GroupBy expressions and filter on top
SELECT 1 from (
  SELECT 1 AS z,
  udf(MIN(a.x))
  FROM (select 1 as x) a
  WHERE false
) b
where b.z != b.z;

-- SPARK-24369 multiple distinct aggregations having the same argument set
SELECT corr(DISTINCT x, y), udf(corr(DISTINCT y, x)), count(*)
  FROM (VALUES (1, 1), (2, 2), (2, 2)) t(x, y);

-- SPARK-25708 HAVING without GROUP BY means global aggregate
SELECT udf(1) FROM range(10) HAVING true;

SELECT udf(udf(1)) FROM range(10) HAVING MAX(id) > 0;

SELECT udf(id) FROM range(10) HAVING id > 0;

-- Test data
CREATE OR REPLACE TEMPORARY VIEW test_agg AS SELECT * FROM VALUES
  (1, true), (1, false),
  (2, true),
  (3, false), (3, null),
  (4, null), (4, null),
  (5, null), (5, true), (5, false) AS test_agg(k, v);

-- empty table
SELECT udf(every(v)), udf(some(v)), any(v) FROM test_agg WHERE 1 = 0;

-- all null values
SELECT udf(every(udf(v))), some(v), any(v) FROM test_agg WHERE k = 4;

-- aggregates are null Filtering
SELECT every(v), udf(some(v)), any(v) FROM test_agg WHERE k = 5;

-- group by
SELECT udf(k), every(v), udf(some(v)), any(v) FROM test_agg GROUP BY udf(k);

-- having
SELECT udf(k), every(v) FROM test_agg GROUP BY k HAVING every(v) = false;
SELECT udf(k), udf(every(v)) FROM test_agg GROUP BY udf(k) HAVING every(v) IS NULL;

-- basic subquery path to make sure rewrite happens in both parent and child plans.
SELECT udf(k),
       udf(Every(v)) AS every
FROM   test_agg
WHERE  k = 2
       AND v IN (SELECT Any(v)
                 FROM   test_agg
                 WHERE  k = 1)
GROUP  BY udf(k);

-- basic subquery path to make sure rewrite happens in both parent and child plans.
SELECT udf(udf(k)),
       Every(v) AS every
FROM   test_agg
WHERE  k = 2
       AND v IN (SELECT Every(v)
                 FROM   test_agg
                 WHERE  k = 1)
GROUP  BY udf(udf(k));

-- input type checking Int
SELECT every(udf(1));

-- input type checking Short
SELECT some(udf(1S));

-- input type checking Long
SELECT any(udf(1L));

-- input type checking String
SELECT udf(every("true"));

-- every/some/any aggregates are supported as windows expression.
SELECT k, v, every(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;
SELECT k, udf(udf(v)), some(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;
SELECT udf(udf(k)), v, any(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;

-- Having referencing aggregate expressions is ok.
SELECT udf(count(*)) FROM test_agg HAVING count(*) > 1L;
SELECT k, udf(max(v)) FROM test_agg GROUP BY k HAVING max(v) = true;

-- Aggrgate expressions can be referenced through an alias
SELECT * FROM (SELECT udf(COUNT(*)) AS cnt FROM test_agg) WHERE cnt > 1L;

-- Error when aggregate expressions are in where clause directly
SELECT udf(count(*)) FROM test_agg WHERE count(*) > 1L;
SELECT udf(count(*)) FROM test_agg WHERE count(*) + 1L > 1L;
SELECT udf(count(*)) FROM test_agg WHERE k = 1 or k = 2 or count(*) + 1L > 1L or max(k) > 1;
