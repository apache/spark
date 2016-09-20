
-- window function with specification
SELECT year, quarter, sales,
       first_value(sales) OVER (DISTRIBUTE BY year SORT BY sales ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
       last_value(sales) OVER (DISTRIBUTE BY year SORT BY sales DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM salesdata;

-- different window specifications
SELECT year, quarter, sales,
       first_value(sales) OVER w1 AS fv,
       last_value(sales) OVER w2 AS lv
FROM salesdata
WINDOW w1 AS (DISTRIBUTE BY year SORT BY sales ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
       w2 AS (DISTRIBUTE BY year SORT BY sales DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);

-- row_number, ntile, rank, dense_rank, percent_rank, cume_dist
SELECT year, quarter, sales,
  row_number() OVER w AS n,
  ntile(2) OVER w AS nt,
  rank() OVER w AS r,
  dense_rank() OVER w AS dr,
  cume_dist() OVER w AS cud,
  percent_rank() OVER w AS pr
FROM salesdata
WINDOW w AS (DISTRIBUTE BY year SORT BY sales);

-- statistics
SELECT year, quarter, sales,
  count(sales) OVER w AS ca,
  sum(sales) OVER w AS ca,
  avg(sales) OVER w AS avg,
  stddev(sales) OVER w AS st,
  stddev_pop(sales) OVER w AS st_pop,
  stddev_samp(sales) OVER w AS st_pop,
  variance(sales) OVER w AS var,
  var_pop(sales) OVER w AS var_pop,
  var_samp(sales) OVER w AS var_samp,
  collect_set(sales) OVER w AS uniq_size,
  corr(sales, -sales) OVER w AS cor,
  covar_samp(sales, sales/2) OVER w AS cov_samp,
  covar_pop(sales, sales/2) OVER w AS cov_pop
FROM salesdata
WINDOW w AS (DISTRIBUTE BY year SORT BY sales ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);

-- null
SELECT year, quarter, sales,
  sum(null) OVER(DISTRIBUTE BY year SORT BY sales) AS sum,
  avg(null) OVER(DISTRIBUTE BY year SORT BY sales) AS avg
FROM salesdata;

-- first/last value
SELECT
  FIRST_VALUE(FALSE) OVER () AS c1,
  FIRST_VALUE(FALSE, FALSE) OVER () AS c2,
  FIRST_VALUE(TRUE, TRUE) OVER () AS c3,
  LAST_VALUE(FALSE) OVER () AS c4,
  LAST_VALUE(FALSE, FALSE) OVER () AS c5,
  LAST_VALUE(TRUE, TRUE) OVER () AS c6;
