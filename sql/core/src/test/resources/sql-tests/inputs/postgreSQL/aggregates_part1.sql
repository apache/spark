--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- AGGREGATES [Part 1]
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/aggregates.sql#L1-L143

-- avoid bit-exact output here because operations may not be bit-exact.
-- SET extra_float_digits = 0;

-- Test aggregate operator with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

SELECT avg(four) AS avg_1 FROM onek;

SELECT avg(a) AS avg_32 FROM aggtest WHERE a < 100;

-- In 7.1, avg(float4) is computed using float8 arithmetic.
-- Round the result to 3 digits to avoid platform-specific results.

select CAST(avg(b) AS Decimal(10,3)) AS avg_107_943 FROM aggtest;
-- `student` has a column with data type POINT, which is not supported by Spark [SPARK-27766]
-- SELECT avg(gpa) AS avg_3_4 FROM ONLY student;

SELECT sum(four) AS sum_1500 FROM onek;
SELECT sum(a) AS sum_198 FROM aggtest;
SELECT sum(b) AS avg_431_773 FROM aggtest;
-- `student` has a column with data type POINT, which is not supported by Spark [SPARK-27766]
-- SELECT sum(gpa) AS avg_6_8 FROM ONLY student;

SELECT max(four) AS max_3 FROM onek;
SELECT max(a) AS max_100 FROM aggtest;
SELECT max(aggtest.b) AS max_324_78 FROM aggtest;
-- `student` has a column with data type POINT, which is not supported by Spark [SPARK-27766]
-- SELECT max(student.gpa) AS max_3_7 FROM student;

SELECT stddev_pop(b) FROM aggtest;
SELECT stddev_samp(b) FROM aggtest;
SELECT var_pop(b) FROM aggtest;
SELECT var_samp(b) FROM aggtest;

SELECT stddev_pop(CAST(b AS Decimal(38,0))) FROM aggtest;
SELECT stddev_samp(CAST(b AS Decimal(38,0))) FROM aggtest;
SELECT var_pop(CAST(b AS Decimal(38,0))) FROM aggtest;
SELECT var_samp(CAST(b AS Decimal(38,0))) FROM aggtest;

-- population variance is defined for a single tuple, sample variance
-- is not
SELECT var_pop(1.0), var_samp(2.0);
SELECT stddev_pop(CAST(3.0 AS Decimal(38,0))), stddev_samp(CAST(4.0 AS Decimal(38,0)));


-- verify correct results for null and NaN inputs
select sum(CAST(null AS int)) from range(1,4);
select sum(CAST(null AS long)) from range(1,4);
select sum(CAST(null AS Decimal(38,0))) from range(1,4);
select sum(CAST(null AS DOUBLE)) from range(1,4);
select avg(CAST(null AS int)) from range(1,4);
select avg(CAST(null AS long)) from range(1,4);
select avg(CAST(null AS Decimal(38,0))) from range(1,4);
select avg(CAST(null AS DOUBLE)) from range(1,4);
select sum(CAST('NaN' AS DOUBLE)) from range(1,4);
select avg(CAST('NaN' AS DOUBLE)) from range(1,4);

SELECT avg(CAST(x AS DOUBLE)), var_pop(CAST(x AS DOUBLE))
FROM (VALUES (CAST('1' AS DOUBLE)), (CAST('infinity' AS DOUBLE))) v(x);
SELECT avg(CAST(x AS DOUBLE)), var_pop(CAST(x AS DOUBLE))
FROM (VALUES ('infinity'), ('1')) v(x);
SELECT avg(CAST(x AS DOUBLE)), var_pop(CAST(x AS DOUBLE))
FROM (VALUES ('infinity'), ('infinity')) v(x);
SELECT avg(CAST(x AS DOUBLE)), var_pop(CAST(x AS DOUBLE))
FROM (VALUES ('-infinity'), ('infinity')) v(x);

-- test accuracy with a large input offset
SELECT avg(CAST(x AS DOUBLE)), var_pop(CAST(x AS DOUBLE))
FROM (VALUES (100000003), (100000004), (100000006), (100000007)) v(x);
SELECT avg(CAST(x AS DOUBLE)), var_pop(CAST(x AS DOUBLE))
FROM (VALUES (7000000000005), (7000000000007)) v(x);

-- SQL2003 binary aggregates [SPARK-23907]
SELECT regr_count(b, a) FROM aggtest;
-- SELECT regr_sxx(b, a) FROM aggtest;
-- SELECT regr_syy(b, a) FROM aggtest;
-- SELECT regr_sxy(b, a) FROM aggtest;
-- SELECT regr_avgx(b, a), regr_avgy(b, a) FROM aggtest;
-- SELECT regr_r2(b, a) FROM aggtest;
-- SELECT regr_slope(b, a), regr_intercept(b, a) FROM aggtest;
SELECT covar_pop(b, a), covar_samp(b, a) FROM aggtest;
SELECT corr(b, a) FROM aggtest;


-- test accum and combine functions directly [SPARK-23907]
-- CREATE TABLE regr_test (x float8, y float8);
-- INSERT INTO regr_test VALUES (10,150),(20,250),(30,350),(80,540),(100,200);
-- SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
-- FROM regr_test WHERE x IN (10,20,30,80);
-- SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
-- FROM regr_test;
-- SELECT float8_accum('{4,140,2900}'::float8[], 100);
-- SELECT float8_regr_accum('{4,140,2900,1290,83075,15050}'::float8[], 200, 100);
-- SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
-- FROM regr_test WHERE x IN (10,20,30);
-- SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
-- FROM regr_test WHERE x IN (80,100);
-- SELECT float8_combine('{3,60,200}'::float8[], '{0,0,0}'::float8[]);
-- SELECT float8_combine('{0,0,0}'::float8[], '{2,180,200}'::float8[]);
-- SELECT float8_combine('{3,60,200}'::float8[], '{2,180,200}'::float8[]);
-- SELECT float8_regr_combine('{3,60,200,750,20000,2000}'::float8[],
--                            '{0,0,0,0,0,0}'::float8[]);
-- SELECT float8_regr_combine('{0,0,0,0,0,0}'::float8[],
--                            '{2,180,200,740,57800,-3400}'::float8[]);
-- SELECT float8_regr_combine('{3,60,200,750,20000,2000}'::float8[],
--                            '{2,180,200,740,57800,-3400}'::float8[]);
-- DROP TABLE regr_test;


-- test count, distinct
SELECT count(four) AS cnt_1000 FROM onek;
SELECT count(DISTINCT four) AS cnt_4 FROM onek;

select ten, count(*), sum(four) from onek
group by ten order by ten;

select ten, count(four), sum(DISTINCT four) from onek
group by ten order by ten;

-- user-defined aggregates
-- SELECT newavg(four) AS avg_1 FROM onek;
-- SELECT newsum(four) AS sum_1500 FROM onek;
-- SELECT newcnt(four) AS cnt_1000 FROM onek;
-- SELECT newcnt(*) AS cnt_1000 FROM onek;
-- SELECT oldcnt(*) AS cnt_1000 FROM onek;
-- SELECT sum2(q1,q2) FROM int8_tbl;

-- test for outer-level aggregates

-- this should work
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- this should fail because subquery has an agg of its own in WHERE
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b
               where sum(distinct a.four + b.four) = b.four);

-- [SPARK-27769] Test handling of sublinks within outer-level aggregates.
-- Per bug report from Daniel Grace.
select
  (select max((select i.unique2 from tenk1 i where i.unique1 = o.unique1)))
from tenk1 o;
