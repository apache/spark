-- Test aggregate operator with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

-- Aggregate with empty GroupBy expressions.
SELECT a, udaf(b) FROM testData;
SELECT udaf(a), udaf(b) FROM testData;

-- Aggregate with non-empty GroupBy expressions.
SELECT a, udaf(b) FROM testData GROUP BY a;
SELECT a, udaf(b) FROM testData GROUP BY b;
SELECT udaf(a), udaf(b) FROM testData GROUP BY a;

-- Aggregate grouped by literals.
SELECT 'foo', udaf(a) FROM testData GROUP BY 1;

-- Aggregate grouped by literals (hash aggregate).
SELECT 'foo', udaf(a) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate grouped by literals (sort aggregate).
SELECT 'foo', udaf(STRUCT(a)) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate with complex GroupBy expressions.
SELECT a + b, udaf(b) FROM testData GROUP BY a + b;
SELECT a + 2, udaf(b) FROM testData GROUP BY a + 1;
SELECT a + 1 + 1, udaf(b) FROM testData GROUP BY a + 1;

-- Aggregate with nulls.
SELECT SKEWNESS(a), KURTOSIS(a), udaf(a), udaf(a), AVG(a), VARIANCE(a), STDDEV(a), SUM(a), udaf(a)
FROM testData;

-- Aggregate with foldable input and multiple distinct groups.
SELECT udaf(DISTINCT b), udaf(DISTINCT b, c) FROM (SELECT 1 AS a, 2 AS b, 3 AS c) GROUP BY a;

-- Aliases in SELECT could be used in GROUP BY
SELECT a AS k, udaf(b) FROM testData GROUP BY k;
SELECT a AS k, udaf(b) FROM testData GROUP BY k HAVING k > 1;

-- GROUP BY alias with invalid col in SELECT list
SELECT a AS k, udaf(non_existing) FROM testData GROUP BY k;

-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testDataHasSameNameWithAlias AS SELECT * FROM VALUES
(1, 1, 3), (1, 2, 1) AS testDataHasSameNameWithAlias(k, a, v);
SELECT k AS a, udaf(v) FROM testDataHasSameNameWithAlias GROUP BY a;

-- turn off group by aliases
set spark.sql.groupByAliases=false;

-- Check analysis exceptions
SELECT a AS k, udaf(b) FROM testData GROUP BY k;

-- Aggregate with empty input and non-empty GroupBy expressions.
SELECT a, udaf(1) FROM testData WHERE false GROUP BY a;

-- Aggregate with empty input and empty GroupBy expressions.
SELECT udaf(1) FROM testData WHERE false;
SELECT 1 FROM (SELECT udaf(1) FROM testData WHERE false) t;

-- Aggregate with empty GroupBy expressions and filter on top
SELECT 1 from (
  SELECT 1 AS z,
  udaf(a.x)
  FROM (select 1 as x) a
  WHERE false
) b
where b.z != b.z;

-- SPARK-25708 HAVING without GROUP BY means global aggregate
SELECT 1 FROM range(10) HAVING udaf(id) > 0;

-- Test data
CREATE OR REPLACE TEMPORARY VIEW test_agg AS SELECT * FROM VALUES
  (1, true), (1, false),
  (2, true),
  (3, false), (3, null),
  (4, null), (4, null),
  (5, null), (5, true), (5, false) AS test_agg(k, v);

-- empty table
SELECT udaf(v), some(v), any(v), bool_and(v), bool_or(v) FROM test_agg WHERE 1 = 0;

-- all null values
SELECT udaf(v), some(v), any(v), bool_and(v), bool_or(v) FROM test_agg WHERE k = 4;

-- aggregates are null Filtering
SELECT udaf(v), some(v), any(v), bool_and(v), bool_or(v) FROM test_agg WHERE k = 5;

-- group by
SELECT k, udaf(v), some(v), any(v), bool_and(v), bool_or(v) FROM test_agg GROUP BY k;

-- having
SELECT k, udaf(v) FROM test_agg GROUP BY k HAVING udaf(v) = false;
SELECT k, udaf(v) FROM test_agg GROUP BY k HAVING udaf(v) IS NULL;

-- basic subquery path to make sure rewrite happens in both parent and child plans.
SELECT k,
       udaf(v) AS count
FROM   test_agg
WHERE  k = 2
       AND v IN (SELECT Any(v)
                 FROM   test_agg
                 WHERE  k = 1)
GROUP  BY k;
