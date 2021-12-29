-- Test aggregate operator with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);
CREATE OR REPLACE TEMPORARY VIEW testRegression AS SELECT * FROM VALUES
(1, 10, null), (2, 10, 11), (2, 20, 22), (2, 25, null), (2, 30, 35)
AS testRegression(k, y, x);
CREATE OR REPLACE TEMPORARY VIEW aggr AS SELECT * FROM VALUES
(0, 0), (0, 10), (0, 20), (0, 30), (0, 40), (1, 10), (1, 20), (2, 10), (2, 20), (2, 25), (2, 30), (3, 60), (4, null)
AS aggr(k, v);

-- Aggregate with empty GroupBy expressions.
SELECT a, COUNT(b) FROM testData;
SELECT COUNT(a), COUNT(b) FROM testData;

-- Aggregate with non-empty GroupBy expressions.
SELECT a, COUNT(b) FROM testData GROUP BY a;
SELECT a, COUNT(b) FROM testData GROUP BY b;
SELECT COUNT(a), COUNT(b) FROM testData GROUP BY a;

-- Aggregate grouped by literals.
SELECT 'foo', COUNT(a) FROM testData GROUP BY 1;

-- Aggregate grouped by literals (whole stage code generation).
SELECT 'foo' FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate grouped by literals (hash aggregate).
SELECT 'foo', APPROX_COUNT_DISTINCT(a) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate grouped by literals (sort aggregate).
SELECT 'foo', MAX(STRUCT(a)) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate with complex GroupBy expressions.
SELECT a + b, COUNT(b) FROM testData GROUP BY a + b;
SELECT a + 2, COUNT(b) FROM testData GROUP BY a + 1;
SELECT a + 1 + 1, COUNT(b) FROM testData GROUP BY a + 1;

-- Aggregate with nulls.
SELECT SKEWNESS(a), KURTOSIS(a), MIN(a), MAX(a), AVG(a), VARIANCE(a), STDDEV(a), SUM(a), COUNT(a)
FROM testData;

-- Aggregate with foldable input and multiple distinct groups.
SELECT COUNT(DISTINCT b), COUNT(DISTINCT b, c) FROM (SELECT 1 AS a, 2 AS b, 3 AS c) GROUP BY a;

-- Aliases in SELECT could be used in GROUP BY
SELECT a AS k, COUNT(b) FROM testData GROUP BY k;
SELECT a AS k, COUNT(b) FROM testData GROUP BY k HAVING k > 1;

-- GROUP BY alias with invalid col in SELECT list
SELECT a AS k, COUNT(non_existing) FROM testData GROUP BY k;

-- Aggregate functions cannot be used in GROUP BY
SELECT COUNT(b) AS k FROM testData GROUP BY k;

-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testDataHasSameNameWithAlias AS SELECT * FROM VALUES
(1, 1, 3), (1, 2, 1) AS testDataHasSameNameWithAlias(k, a, v);
SELECT k AS a, COUNT(v) FROM testDataHasSameNameWithAlias GROUP BY a;

-- turn off group by aliases
set spark.sql.groupByAliases=false;

-- Check analysis exceptions
SELECT a AS k, COUNT(b) FROM testData GROUP BY k;

-- Aggregate with empty input and non-empty GroupBy expressions.
SELECT a, COUNT(1) FROM testData WHERE false GROUP BY a;

-- Aggregate with empty input and empty GroupBy expressions.
SELECT COUNT(1) FROM testData WHERE false;
SELECT 1 FROM (SELECT COUNT(1) FROM testData WHERE false) t;

-- Aggregate with empty GroupBy expressions and filter on top
SELECT 1 from (
  SELECT 1 AS z,
  MIN(a.x)
  FROM (select 1 as x) a
  WHERE false
) b
where b.z != b.z;

-- SPARK-24369 multiple distinct aggregations having the same argument set
SELECT corr(DISTINCT x, y), corr(DISTINCT y, x), count(*)
  FROM (VALUES (1, 1), (2, 2), (2, 2)) t(x, y);

-- SPARK-25708 HAVING without GROUP BY means global aggregate
SELECT 1 FROM range(10) HAVING true;

SELECT 1 FROM range(10) HAVING MAX(id) > 0;

SELECT id FROM range(10) HAVING id > 0;

SET spark.sql.legacy.parser.havingWithoutGroupByAsWhere=true;

SELECT 1 FROM range(10) HAVING true;

SELECT 1 FROM range(10) HAVING MAX(id) > 0;

SELECT id FROM range(10) HAVING id > 0;

SET spark.sql.legacy.parser.havingWithoutGroupByAsWhere=false;

-- Test data
CREATE OR REPLACE TEMPORARY VIEW test_agg AS SELECT * FROM VALUES
  (1, true), (1, false),
  (2, true),
  (3, false), (3, null),
  (4, null), (4, null),
  (5, null), (5, true), (5, false) AS test_agg(k, v);

-- empty table
SELECT every(v), some(v), any(v), bool_and(v), bool_or(v) FROM test_agg WHERE 1 = 0;

-- all null values
SELECT every(v), some(v), any(v), bool_and(v), bool_or(v) FROM test_agg WHERE k = 4;

-- aggregates are null Filtering
SELECT every(v), some(v), any(v), bool_and(v), bool_or(v) FROM test_agg WHERE k = 5;

-- group by
SELECT k, every(v), some(v), any(v), bool_and(v), bool_or(v) FROM test_agg GROUP BY k;

-- having
SELECT k, every(v) FROM test_agg GROUP BY k HAVING every(v) = false;
SELECT k, every(v) FROM test_agg GROUP BY k HAVING every(v) IS NULL;

-- basic subquery path to make sure rewrite happens in both parent and child plans.
SELECT k,
       Every(v) AS every
FROM   test_agg
WHERE  k = 2
       AND v IN (SELECT Any(v)
                 FROM   test_agg
                 WHERE  k = 1)
GROUP  BY k;

-- basic subquery path to make sure rewrite happens in both parent and child plans.
SELECT k,
       Every(v) AS every
FROM   test_agg
WHERE  k = 2
       AND v IN (SELECT Every(v)
                 FROM   test_agg
                 WHERE  k = 1)
GROUP  BY k;

-- input type checking Int
SELECT every(1);

-- input type checking Short
SELECT some(1S);

-- input type checking Long
SELECT any(1L);

-- input type checking String
SELECT every("true");

-- input type checking Decimal
SELECT bool_and(1.0);

-- input type checking double
SELECT bool_or(1.0D);

-- every/some/any aggregates/bool_and/bool_or are supported as windows expression.
SELECT k, v, every(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;
SELECT k, v, some(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;
SELECT k, v, any(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;
SELECT k, v, bool_and(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;
SELECT k, v, bool_or(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;

-- Having referencing aggregate expressions is ok.
SELECT count(*) FROM test_agg HAVING count(*) > 1L;
SELECT k, max(v) FROM test_agg GROUP BY k HAVING max(v) = true;

-- Aggrgate expressions can be referenced through an alias
SELECT * FROM (SELECT COUNT(*) AS cnt FROM test_agg) WHERE cnt > 1L;

-- Error when aggregate expressions are in where clause directly
SELECT count(*) FROM test_agg WHERE count(*) > 1L;
SELECT count(*) FROM test_agg WHERE count(*) + 1L > 1L;
SELECT count(*) FROM test_agg WHERE k = 1 or k = 2 or count(*) + 1L > 1L or max(k) > 1;

-- Aggregate with multiple distinct decimal columns
SELECT AVG(DISTINCT decimal_col), SUM(DISTINCT decimal_col) FROM VALUES (CAST(1 AS DECIMAL(9, 0))) t(decimal_col);

-- SPARK-34581: Don't optimize out grouping expressions from aggregate expressions without aggregate function
SELECT not(a IS NULL), count(*) AS c
FROM testData
GROUP BY a IS NULL;

SELECT if(not(a IS NULL), rand(0), 1), count(*) AS c
FROM testData
GROUP BY a IS NULL;


-- Histogram aggregates with different numeric input types
SELECT
  histogram_numeric(col, 2) as histogram_2,
  histogram_numeric(col, 3) as histogram_3,
  histogram_numeric(col, 5) as histogram_5,
  histogram_numeric(col, 10) as histogram_10
FROM VALUES
 (1), (2), (3), (4), (5), (6), (7), (8), (9), (10),
 (11), (12), (13), (14), (15), (16), (17), (18), (19), (20),
 (21), (22), (23), (24), (25), (26), (27), (28), (29), (30),
 (31), (32), (33), (34), (35), (3), (37), (38), (39), (40),
 (41), (42), (43), (44), (45), (46), (47), (48), (49), (50) AS tab(col);
SELECT histogram_numeric(col, 3) FROM VALUES (1), (2), (3) AS tab(col);
SELECT histogram_numeric(col, 3) FROM VALUES (1L), (2L), (3L) AS tab(col);
SELECT histogram_numeric(col, 3) FROM VALUES (1F), (2F), (3F) AS tab(col);
SELECT histogram_numeric(col, 3) FROM VALUES (1D), (2D), (3D) AS tab(col);
SELECT histogram_numeric(col, 3) FROM VALUES (1S), (2S), (3S) AS tab(col);
SELECT histogram_numeric(col, 3) FROM VALUES
  (CAST(1 AS BYTE)), (CAST(2 AS BYTE)), (CAST(3 AS BYTE)) AS tab(col);
SELECT histogram_numeric(col, 3) FROM VALUES
  (CAST(1 AS TINYINT)), (CAST(2 AS TINYINT)), (CAST(3 AS TINYINT)) AS tab(col);
SELECT histogram_numeric(col, 3) FROM VALUES
  (CAST(1 AS SMALLINT)), (CAST(2 AS SMALLINT)), (CAST(3 AS SMALLINT)) AS tab(col);
SELECT histogram_numeric(col, 3) FROM VALUES
  (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT)) AS tab(col);
SELECT histogram_numeric(col, 3) FROM VALUES (TIMESTAMP '2017-03-01 00:00:00'),
  (TIMESTAMP '2017-04-01 00:00:00'), (TIMESTAMP '2017-05-01 00:00:00') AS tab(col);
SELECT histogram_numeric(col, 3) FROM VALUES (INTERVAL '100-00' YEAR TO MONTH),
  (INTERVAL '110-00' YEAR TO MONTH), (INTERVAL '120-00' YEAR TO MONTH) AS tab(col);
SELECT histogram_numeric(col, 3) FROM VALUES (INTERVAL '12 20:4:0' DAY TO SECOND),
  (INTERVAL '12 21:4:0' DAY TO SECOND), (INTERVAL '12 22:4:0' DAY TO SECOND) AS tab(col);
SELECT histogram_numeric(col, 3)
FROM VALUES (NULL), (NULL), (NULL) AS tab(col);
SELECT histogram_numeric(col, 3)
FROM VALUES (CAST(NULL AS DOUBLE)), (CAST(NULL AS DOUBLE)), (CAST(NULL AS DOUBLE)) AS tab(col);
SELECT histogram_numeric(col, 3)
FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS tab(col);


-- SPARK-37613: Support ANSI Aggregate Function: regr_count
SELECT regr_count(y, x) FROM testRegression;
SELECT regr_count(y, x) FROM testRegression WHERE x IS NOT NULL;
SELECT k, count(*), regr_count(y, x) FROM testRegression GROUP BY k;
SELECT k, count(*) FILTER (WHERE x IS NOT NULL), regr_count(y, x) FROM testRegression GROUP BY k;

-- SPARK-37613: Support ANSI Aggregate Function: regr_r2
SELECT regr_r2(y, x) FROM testRegression;
SELECT regr_r2(y, x) FROM testRegression WHERE x IS NOT NULL;
SELECT k, corr(y, x), regr_r2(y, x) FROM testRegression GROUP BY k;
SELECT k, corr(y, x) FILTER (WHERE x IS NOT NULL), regr_r2(y, x) FROM testRegression GROUP BY k;

-- SPARK-27974: Support ANSI Aggregate Function: array_agg
SELECT
  collect_list(col),
  array_agg(col)
FROM VALUES
  (1), (2), (1) AS tab(col);
SELECT
  a,
  collect_list(b),
  array_agg(b)
FROM VALUES
  (1,4),(2,3),(1,4),(2,4) AS v(a,b)
GROUP BY a;

-- SPARK-37614: Support ANSI Aggregate Function: regr_avgx & regr_avgy
SELECT regr_avgx(y, x), regr_avgy(y, x) FROM testRegression;
SELECT regr_avgx(y, x), regr_avgy(y, x) FROM testRegression WHERE x IS NOT NULL AND y IS NOT NULL;
SELECT k, avg(x), avg(y), regr_avgx(y, x), regr_avgy(y, x) FROM testRegression GROUP BY k;
SELECT k, avg(x) FILTER (WHERE x IS NOT NULL AND y IS NOT NULL), avg(y) FILTER (WHERE x IS NOT NULL AND y IS NOT NULL), regr_avgx(y, x), regr_avgy(y, x) FROM testRegression GROUP BY k;

-- SPARK-37672: Support ANSI Aggregate Function: regr_sxx
SELECT regr_sxx(y, x) FROM testRegression;
SELECT regr_sxx(y, x) FROM testRegression WHERE x IS NOT NULL AND y IS NOT NULL;
SELECT k, regr_sxx(y, x) FROM testRegression GROUP BY k;
SELECT k, regr_sxx(y, x) FROM testRegression WHERE x IS NOT NULL AND y IS NOT NULL GROUP BY k;

-- SPARK-37676: Support ANSI Aggregation Function: percentile_cont
SELECT
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC)
FROM aggr;
SELECT
  k,
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC)
FROM aggr
GROUP BY k
ORDER BY k;

-- SPARK-37691: Support ANSI Aggregation Function: percentile_disc
SELECT
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v DESC)
FROM aggr;
SELECT
  k,
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v DESC)
FROM aggr
GROUP BY k
ORDER BY k;
