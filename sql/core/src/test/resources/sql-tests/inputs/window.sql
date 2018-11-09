-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(null, 1L, 1.0D, date("2017-08-01"), timestamp(1501545600), "a"),
(1, 1L, 1.0D, date("2017-08-01"), timestamp(1501545600), "a"),
(1, 2L, 2.5D, date("2017-08-02"), timestamp(1502000000), "a"),
(2, 2147483650L, 100.001D, date("2020-12-31"), timestamp(1609372800), "a"),
(1, null, 1.0D, date("2017-08-01"), timestamp(1501545600), "b"),
(2, 3L, 3.3D, date("2017-08-03"), timestamp(1503000000), "b"),
(3, 2147483650L, 100.001D, date("2020-12-31"), timestamp(1609372800), "b"),
(null, null, null, null, null, null),
(3, 1L, 1.0D, date("2017-08-01"), timestamp(1501545600), null)
AS testData(val, val_long, val_double, val_date, val_timestamp, cate);

-- RowsBetween
SELECT val, cate, count(val) OVER(PARTITION BY cate ORDER BY val ROWS CURRENT ROW) FROM testData
ORDER BY cate, val;
SELECT val, cate, sum(val) OVER(PARTITION BY cate ORDER BY val
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM testData ORDER BY cate, val;
SELECT val_long, cate, sum(val_long) OVER(PARTITION BY cate ORDER BY val_long
ROWS BETWEEN CURRENT ROW AND 2147483648 FOLLOWING) FROM testData ORDER BY cate, val_long;

-- RangeBetween
SELECT val, cate, count(val) OVER(PARTITION BY cate ORDER BY val RANGE 1 PRECEDING) FROM testData
ORDER BY cate, val;
SELECT val, cate, sum(val) OVER(PARTITION BY cate ORDER BY val
RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM testData ORDER BY cate, val;
SELECT val_long, cate, sum(val_long) OVER(PARTITION BY cate ORDER BY val_long
RANGE BETWEEN CURRENT ROW AND 2147483648 FOLLOWING) FROM testData ORDER BY cate, val_long;
SELECT val_double, cate, sum(val_double) OVER(PARTITION BY cate ORDER BY val_double
RANGE BETWEEN CURRENT ROW AND 2.5 FOLLOWING) FROM testData ORDER BY cate, val_double;
SELECT val_date, cate, max(val_date) OVER(PARTITION BY cate ORDER BY val_date
RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM testData ORDER BY cate, val_date;
SELECT val_timestamp, cate, avg(val_timestamp) OVER(PARTITION BY cate ORDER BY val_timestamp
RANGE BETWEEN CURRENT ROW AND interval 23 days 4 hours FOLLOWING) FROM testData
ORDER BY cate, val_timestamp;

-- RangeBetween with reverse OrderBy
SELECT val, cate, sum(val) OVER(PARTITION BY cate ORDER BY val DESC
RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM testData ORDER BY cate, val;

-- Invalid window frame
SELECT val, cate, count(val) OVER(PARTITION BY cate
ROWS BETWEEN UNBOUNDED FOLLOWING AND 1 FOLLOWING) FROM testData ORDER BY cate, val;
SELECT val, cate, count(val) OVER(PARTITION BY cate
RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM testData ORDER BY cate, val;
SELECT val, cate, count(val) OVER(PARTITION BY cate ORDER BY val, cate
RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM testData ORDER BY cate, val;
SELECT val, cate, count(val) OVER(PARTITION BY cate ORDER BY current_timestamp
RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM testData ORDER BY cate, val;
SELECT val, cate, count(val) OVER(PARTITION BY cate ORDER BY val
RANGE BETWEEN 1 FOLLOWING AND 1 PRECEDING) FROM testData ORDER BY cate, val;
SELECT val, cate, count(val) OVER(PARTITION BY cate ORDER BY val
RANGE BETWEEN CURRENT ROW AND current_date PRECEDING) FROM testData ORDER BY cate, val;


-- Window functions
SELECT val, cate,
max(val) OVER w AS max,
min(val) OVER w AS min,
min(val) OVER w AS min,
count(val) OVER w AS count,
sum(val) OVER w AS sum,
avg(val) OVER w AS avg,
stddev(val) OVER w AS stddev,
first_value(val) OVER w AS first_value,
first_value(val, true) OVER w AS first_value_ignore_null,
first_value(val, false) OVER w AS first_value_contain_null,
last_value(val) OVER w AS last_value,
last_value(val, true) OVER w AS last_value_ignore_null,
last_value(val, false) OVER w AS last_value_contain_null,
rank() OVER w AS rank,
dense_rank() OVER w AS dense_rank,
cume_dist() OVER w AS cume_dist,
percent_rank() OVER w AS percent_rank,
ntile(2) OVER w AS ntile,
row_number() OVER w AS row_number,
var_pop(val) OVER w AS var_pop,
var_samp(val) OVER w AS var_samp,
approx_count_distinct(val) OVER w AS approx_count_distinct,
covar_pop(val, val_long) OVER w AS covar_pop,
corr(val, val_long) OVER w AS corr,
stddev_samp(val) OVER w AS stddev_samp,
stddev_pop(val) OVER w AS stddev_pop,
collect_list(val) OVER w AS collect_list,
collect_set(val) OVER w AS collect_set,
skewness(val_double) OVER w AS skewness,
kurtosis(val_double) OVER w AS kurtosis
FROM testData
WINDOW w AS (PARTITION BY cate ORDER BY val)
ORDER BY cate, val;

-- Null inputs
SELECT val, cate, avg(null) OVER(PARTITION BY cate ORDER BY val) FROM testData ORDER BY cate, val;

-- OrderBy not specified
SELECT val, cate, row_number() OVER(PARTITION BY cate) FROM testData ORDER BY cate, val;

-- Over clause is empty
SELECT val, cate, sum(val) OVER(), avg(val) OVER() FROM testData ORDER BY cate, val;

-- first_value()/last_value() over ()
SELECT val, cate,
first_value(false) OVER w AS first_value,
first_value(true, true) OVER w AS first_value_ignore_null,
first_value(false, false) OVER w AS first_value_contain_null,
last_value(false) OVER w AS last_value,
last_value(true, true) OVER w AS last_value_ignore_null,
last_value(false, false) OVER w AS last_value_contain_null
FROM testData
WINDOW w AS ()
ORDER BY cate, val;

-- parentheses around window reference
SELECT cate, sum(val) OVER (w)
FROM testData
WHERE val is not null
WINDOW w AS (PARTITION BY cate ORDER BY val);
