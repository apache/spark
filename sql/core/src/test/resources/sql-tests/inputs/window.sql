-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(null, "a"), (1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b"), (null, null), (3, null)
AS testData(val, cate);

-- RowsBetween
SELECT val, cate, count(val) OVER(PARTITION BY cate ORDER BY val ROWS CURRENT ROW) FROM testData
ORDER BY cate, val;
SELECT val, cate, sum(val) OVER(PARTITION BY cate ORDER BY val
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM testData ORDER BY cate, val;

-- RangeBetween
SELECT val, cate, count(val) OVER(PARTITION BY cate ORDER BY val RANGE 1 PRECEDING) FROM testData
ORDER BY cate, val;
SELECT val, cate, sum(val) OVER(PARTITION BY cate ORDER BY val
RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM testData ORDER BY cate, val;

-- RangeBetween with reverse OrderBy
SELECT val, cate, sum(val) OVER(PARTITION BY cate ORDER BY val DESC
RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM testData ORDER BY cate, val;

-- Window functions
SELECT val, cate,
max(val) OVER(PARTITION BY cate ORDER BY val),
min(val) OVER(PARTITION BY cate ORDER BY val),
mean(val) OVER(PARTITION BY cate ORDER BY val),
count(val) OVER(PARTITION BY cate ORDER BY val),
sum(val) OVER(PARTITION BY cate ORDER BY val),
avg(val) OVER(PARTITION BY cate ORDER BY val),
stddev(val) OVER(PARTITION BY cate ORDER BY val),
first_value(val) OVER(PARTITION BY cate ORDER BY val),
first_value(val, true) OVER(PARTITION BY cate ORDER BY val),
first_value(val, false) OVER(PARTITION BY cate ORDER BY val),
last_value(val) OVER(PARTITION BY cate ORDER BY val),
last_value(val, true) OVER(PARTITION BY cate ORDER BY val),
last_value(val, false) OVER(PARTITION BY cate ORDER BY val),
rank() OVER(PARTITION BY cate ORDER BY val),
dense_rank() OVER(PARTITION BY cate ORDER BY val),
cume_dist() OVER(PARTITION BY cate ORDER BY val),
percent_rank() OVER(PARTITION BY cate ORDER BY val),
ntile(2) OVER(PARTITION BY cate ORDER BY val),
row_number() OVER(PARTITION BY cate ORDER BY val),
var_pop(val) OVER(PARTITION BY cate ORDER BY val),
var_samp(val) OVER(PARTITION BY cate ORDER BY val),
approx_count_distinct(val) OVER(PARTITION BY cate ORDER BY val)
FROM testData ORDER BY cate, val;

-- Null inputs
SELECT val, cate, avg(null) OVER(PARTITION BY cate ORDER BY val) FROM testData ORDER BY cate, val;

-- OrderBy not specified
SELECT val, cate, row_number() OVER(PARTITION BY cate) FROM testData ORDER BY cate, val;

-- Over clause is empty
SELECT val, cate, sum(val) OVER(), avg(val) OVER() FROM testData ORDER BY cate, val;

-- first_value()/last_value() over ()
SELECT val, cate,
first_value(false) OVER(),
first_value(false, false) OVER(),
first_value(true, true) OVER(),
last_value(false) OVER(),
last_value(false, false) OVER(),
last_value(true, true) OVER()
FROM testData ORDER BY cate, val;
