-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testRegression AS SELECT * FROM VALUES
(1, 10, null), (2, 10, 11), (2, 20, 22), (2, 25, null), (2, 30, 35)
AS testRegression(k, y, x);

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

-- SPARK-37681: Support ANSI Aggregate Function: regr_sxy
SELECT regr_sxy(y, x) FROM testRegression;
SELECT regr_sxy(y, x) FROM testRegression WHERE x IS NOT NULL AND y IS NOT NULL;
SELECT k, regr_sxy(y, x) FROM testRegression GROUP BY k;
SELECT k, regr_sxy(y, x) FROM testRegression WHERE x IS NOT NULL AND y IS NOT NULL GROUP BY k;

-- SPARK-37702: Support ANSI Aggregate Function: regr_syy
SELECT regr_syy(y, x) FROM testRegression;
SELECT regr_syy(y, x) FROM testRegression WHERE x IS NOT NULL AND y IS NOT NULL;
SELECT k, regr_syy(y, x) FROM testRegression GROUP BY k;
SELECT k, regr_syy(y, x) FROM testRegression WHERE x IS NOT NULL AND y IS NOT NULL GROUP BY k;

-- SPARK-39230: Support ANSI Aggregate Function: regr_slope
SELECT regr_slope(y, x) FROM testRegression;
SELECT regr_slope(y, x) FROM testRegression WHERE x IS NOT NULL AND y IS NOT NULL;
SELECT k, regr_slope(y, x) FROM testRegression GROUP BY k;
SELECT k, regr_slope(y, x) FROM testRegression WHERE x IS NOT NULL AND y IS NOT NULL GROUP BY k;
