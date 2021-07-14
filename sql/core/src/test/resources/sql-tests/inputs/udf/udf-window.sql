--This test file was converted from window.sql.
-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(null, 1L, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), "a"),
(1, 1L, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), "a"),
(1, 2L, 2.5D, date("2017-08-02"), timestamp_seconds(1502000000), "a"),
(2, 2147483650L, 100.001D, date("2020-12-31"), timestamp_seconds(1609372800), "a"),
(1, null, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), "b"),
(2, 3L, 3.3D, date("2017-08-03"), timestamp_seconds(1503000000), "b"),
(3, 2147483650L, 100.001D, date("2020-12-31"), timestamp_seconds(1609372800), "b"),
(null, null, null, null, null, null),
(3, 1L, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), null)
AS testData(val, val_long, val_double, val_date, val_timestamp, cate);

-- RowsBetween
SELECT udf(val), cate, count(val) OVER(PARTITION BY cate ORDER BY udf(val) ROWS CURRENT ROW) FROM testData
ORDER BY cate, udf(val);
SELECT udf(val), cate, sum(val) OVER(PARTITION BY cate ORDER BY udf(val)
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM testData ORDER BY cate, udf(val);
SELECT val_long, udf(cate), sum(val_long) OVER(PARTITION BY cate ORDER BY udf(val_long)
ROWS BETWEEN CURRENT ROW AND 2147483648 FOLLOWING) FROM testData ORDER BY udf(cate), val_long;

-- RangeBetween
SELECT udf(val), cate, count(val) OVER(PARTITION BY udf(cate) ORDER BY val RANGE 1 PRECEDING) FROM testData
ORDER BY cate, udf(val);
SELECT val, udf(cate), sum(val) OVER(PARTITION BY udf(cate) ORDER BY val
RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM testData ORDER BY udf(cate), val;
SELECT val_long, udf(cate), sum(val_long) OVER(PARTITION BY udf(cate) ORDER BY val_long
RANGE BETWEEN CURRENT ROW AND 2147483648 FOLLOWING) FROM testData ORDER BY udf(cate), val_long;
SELECT val_double, udf(cate), sum(val_double) OVER(PARTITION BY udf(cate) ORDER BY val_double
RANGE BETWEEN CURRENT ROW AND 2.5 FOLLOWING) FROM testData ORDER BY udf(cate), val_double;
SELECT val_date, udf(cate), max(val_date) OVER(PARTITION BY udf(cate) ORDER BY val_date
RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM testData ORDER BY udf(cate), val_date;
SELECT val_timestamp, udf(cate), avg(val_timestamp) OVER(PARTITION BY udf(cate) ORDER BY val_timestamp
RANGE BETWEEN CURRENT ROW AND interval 23 days 4 hours FOLLOWING) FROM testData
ORDER BY udf(cate), val_timestamp;

-- RangeBetween with reverse OrderBy
SELECT val, udf(cate), sum(val) OVER(PARTITION BY cate ORDER BY val DESC
RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM testData ORDER BY cate, val;

-- Invalid window frame
SELECT udf(val), cate, count(val) OVER(PARTITION BY udf(cate)
ROWS BETWEEN UNBOUNDED FOLLOWING AND 1 FOLLOWING) FROM testData ORDER BY cate, udf(val);
SELECT udf(val), cate, count(val) OVER(PARTITION BY udf(cate)
RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM testData ORDER BY cate, udf(val);
SELECT udf(val), cate, count(val) OVER(PARTITION BY udf(cate) ORDER BY udf(val), cate
RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM testData ORDER BY cate, udf(val);
SELECT udf(val), cate, count(val) OVER(PARTITION BY udf(cate) ORDER BY current_timestamp
RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM testData ORDER BY cate, udf(val);
SELECT udf(val), cate, count(val) OVER(PARTITION BY udf(cate) ORDER BY val
RANGE BETWEEN 1 FOLLOWING AND 1 PRECEDING) FROM testData ORDER BY udf(cate), val;
SELECT udf(val), cate, count(val) OVER(PARTITION BY udf(cate) ORDER BY udf(val)
RANGE BETWEEN CURRENT ROW AND current_date PRECEDING) FROM testData ORDER BY cate, val(val);


-- Window functions
SELECT udf(val), cate,
max(udf(val)) OVER w AS max,
min(udf(val)) OVER w AS min,
min(udf(val)) OVER w AS min,
count(udf(val)) OVER w AS count,
sum(udf(val)) OVER w AS sum,
avg(udf(val)) OVER w AS avg,
stddev(udf(val)) OVER w AS stddev,
first_value(udf(val)) OVER w AS first_value,
first_value(udf(val), true) OVER w AS first_value_ignore_null,
first_value(udf(val), false) OVER w AS first_value_contain_null,
last_value(udf(val)) OVER w AS last_value,
last_value(udf(val), true) OVER w AS last_value_ignore_null,
last_value(udf(val), false) OVER w AS last_value_contain_null,
rank() OVER w AS rank,
dense_rank() OVER w AS dense_rank,
cume_dist() OVER w AS cume_dist,
percent_rank() OVER w AS percent_rank,
ntile(2) OVER w AS ntile,
row_number() OVER w AS row_number,
var_pop(udf(val)) OVER w AS var_pop,
var_samp(udf(val)) OVER w AS var_samp,
approx_count_distinct(udf(val)) OVER w AS approx_count_distinct,
covar_pop(udf(val), udf(val_long)) OVER w AS covar_pop,
corr(udf(val), udf(val_long)) OVER w AS corr,
stddev_samp(udf(val)) OVER w AS stddev_samp,
stddev_pop(udf(val)) OVER w AS stddev_pop,
collect_list(udf(val)) OVER w AS collect_list,
collect_set(udf(val)) OVER w AS collect_set,
skewness(udf(val_double)) OVER w AS skewness,
kurtosis(udf(val_double)) OVER w AS kurtosis
FROM testData
WINDOW w AS (PARTITION BY udf(cate) ORDER BY udf(val))
ORDER BY cate, udf(val);

-- Null inputs
SELECT udf(val), cate, avg(null) OVER(PARTITION BY cate ORDER BY val) FROM testData ORDER BY cate, val;

-- OrderBy not specified
SELECT udf(val), cate, row_number() OVER(PARTITION BY cate) FROM testData ORDER BY cate, udf(val);

-- Over clause is empty
SELECT udf(val), cate, sum(val) OVER(), avg(val) OVER() FROM testData ORDER BY cate, val;

-- first_value()/last_value() over ()
SELECT udf(val), cate,
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
SELECT udf(cate), sum(val) OVER (w)
FROM testData
WHERE val is not null
WINDOW w AS (PARTITION BY cate ORDER BY val);
