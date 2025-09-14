SELECT
    SUM(col1) OVER () as window_sum
FROM VALUES (1, 2), (3, 4), (5, 6) AS t(col1, col2)
GROUP BY col1, col2 + 1
ORDER BY SUM(col1), col2 + 1;

SELECT
    col1,
    col2 + 1 as computed_col,
    SUM(col1) OVER () as total_sum,
    AVG(col1) OVER () as avg_col1,
    COUNT(*) OVER () as row_count
FROM VALUES (10, 20), (30, 40), (50, 60), (70, 80) AS t(col1, col2)
GROUP BY col1, col2 + 1
ORDER BY SUM(col1) DESC, col2 + 1 ASC;

SELECT
    col1 % 2 as parity,
  SUM(col1) as group_sum,
  SUM(SUM(col1)) OVER (PARTITION BY col1 % 2) as partition_sum
FROM VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50) AS t(col1, col2)
GROUP BY col1, col2, col1 % 2
ORDER BY SUM(col1), col1 % 2;

SELECT
    col1,
    MAX(col2) as max_col2,
    ROW_NUMBER() OVER (ORDER BY MAX(col2)) as rn,
    RANK() OVER (ORDER BY MAX(col2)) as rnk
FROM VALUES (1, 100), (2, 200), (1, 150), (2, 250) AS t(col1, col2)
GROUP BY col1
ORDER BY MAX(col2), col1;

SELECT
    col1 * 2 as doubled_col1,
    col2 + col1 as sum_cols,
    COUNT(*) as cnt,
    SUM(COUNT(*)) OVER () as total_count
FROM VALUES (1, 10), (2, 20), (3, 30), (4, 40) AS t(col1, col2)
GROUP BY col1 * 2, col2 + col1, col1, col2
ORDER BY COUNT(*) DESC, col1 * 2;

SELECT
    col1,
    SUM(col2) as sum_col2,
    SUM(SUM(col2)) OVER (ORDER BY col1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as rolling_sum
FROM VALUES (1, 5), (2, 10), (3, 15), (4, 20), (5, 25) AS t(col1, col2)
GROUP BY col1, col2
ORDER BY SUM(col2), col1;

SELECT
    col1 / 10 as bucket,
    SUM(col1) as sum_col1,
    AVG(col2) as avg_col2,
    MIN(col1) OVER () as global_min,
    MAX(SUM(col1)) OVER (PARTITION BY col1 / 10) as bucket_max
FROM VALUES (15, 100), (25, 200), (35, 300), (45, 400), (55, 500) AS t(col1, col2)
GROUP BY col1 / 10, col1, col2
ORDER BY SUM(col1), AVG(col2);

SELECT
    col1,
    COUNT(*) as cnt,
    SUM(COUNT(*)) OVER (ORDER BY col1 RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) as range_sum
FROM VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd') AS t(col1, col2)
GROUP BY col1, col2
ORDER BY COUNT(*), col1;

SELECT
    col1,
    col2 + 100 as adjusted_col2,
    COUNT(*) as cnt,
    AVG(COUNT(*)) OVER () as avg_count
FROM VALUES (1, 5), (1, 15), (2, 25), (2, 35), (3, 45) AS t(col1, col2)
GROUP BY col1, col2 + 100, col2
HAVING COUNT(*) >= 1
ORDER BY COUNT(*) DESC, col2 + 100;

SELECT
    col1,
    MAX(col2) as max_col2,
    LAG(MAX(col2)) OVER (ORDER BY col1) as prev_max,
    LEAD(MAX(col2)) OVER (ORDER BY col1) as next_max
FROM VALUES (10, 100), (20, 200), (30, 300), (40, 400) AS t(col1, col2)
GROUP BY col1
ORDER BY MAX(col2), col1;

SELECT
    col1 + col2 as sum_expr,
    COUNT(*) as cnt,
    PERCENT_RANK() OVER (ORDER BY COUNT(*)) as pct_rank,
    CUME_DIST() OVER (ORDER BY COUNT(*)) as cum_dist
FROM VALUES (1, 2), (3, 4), (5, 6), (7, 8) AS t(col1, col2)
GROUP BY col1 + col2, col1, col2
ORDER BY COUNT(*), col1 + col2;

SELECT
    COALESCE(col1, 0) as safe_col1,
    COUNT(col2) as non_null_count,
    SUM(COUNT(col2)) OVER () as total_non_null
FROM VALUES (1, 10), (NULL, 20), (3, NULL), (4, 40) AS t(col1, col2)
GROUP BY COALESCE(col1, 0), col1, col2
ORDER BY COUNT(col2) DESC, COALESCE(col1, 0);

SELECT
    col1,
    col2,
    SUM(col1) as sum_col1,
    AVG(col2) as avg_col2,
    MAX(SUM(col1)) OVER () as max_sum_col1,
    MIN(AVG(col2)) OVER () as min_avg_col2
FROM VALUES (1, 100), (2, 200), (3, 300), (4, 400) AS t(col1, col2)
GROUP BY col1, col2
ORDER BY SUM(col1) + AVG(col2), col1, col2;


SELECT
    col1,
    SUM(col2) as sum_col2,
    SUM(SUM(col2)) OVER () as total_sum
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(col1, col2)
GROUP BY col1;

SELECT
    col1 % 2 as parity,
  COUNT(*) as cnt,
  AVG(col2) as avg_col2,
  SUM(COUNT(*)) OVER () as total_count,
  MAX(AVG(col2)) OVER () as max_avg
FROM VALUES (1, 100), (2, 200), (3, 300), (4, 400) AS t(col1, col2)
GROUP BY col1 % 2, col1, col2;

SELECT
    col1,
    col2,
    SUM(col1) as sum_col1,
    AVG(SUM(col1)) OVER (PARTITION BY col2) as avg_by_col2
FROM VALUES (1, 'A'), (2, 'A'), (3, 'B'), (4, 'B') AS t(col1, col2)
GROUP BY col1, col2;

SELECT
    col1 / 10 as bucket,
    COUNT(*) as cnt,
    MIN(COUNT(*)) OVER () as min_count,
    MAX(COUNT(*)) OVER () as max_count
FROM VALUES (15, 'x'), (25, 'y'), (35, 'z') AS t(col1, col2)
GROUP BY col1 / 10, col1, col2;

SELECT
    col1 % 3 as mod_group,
  col2,
  COUNT(*) as cnt,
  SUM(COUNT(*)) OVER (PARTITION BY col1 % 3) as sum_by_mod,
  AVG(COUNT(*)) OVER (PARTITION BY col2) as avg_by_col2
FROM VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'B'), (5, 'A') AS t(col1, col2)
GROUP BY col1 % 3, col2, col1;

SELECT
    col1,
    MAX(col2) as max_col2,
    FIRST_VALUE(MAX(col2)) OVER (PARTITION BY col1) as first_max
FROM VALUES (1, 100), (1, 150), (2, 200), (2, 250) AS t(col1, col2)
GROUP BY col1;

SELECT
    col1 + col2 as sum_cols,
    COUNT(*) as cnt,
    SUM(COUNT(*)) OVER () as total_rows,
    COUNT(COUNT(*)) OVER () as count_of_groups
FROM VALUES (1, 2), (3, 4), (5, 6) AS t(col1, col2)
GROUP BY col1 + col2, col1, col2;

SELECT
    col1,
    SUM(col2) as sum_col2,
    SUM(SUM(col2)) OVER (ORDER BY col1 ROWS UNBOUNDED PRECEDING) as running_sum
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(col1, col2)
GROUP BY col1, col2;

SELECT
    col1 / 5 as bucket,
    col2 % 2 as parity,
  AVG(col1) as avg_col1,
  RANK() OVER (PARTITION BY col1 / 5, col2 % 2 ORDER BY AVG(col1)) as rank_in_bucket
FROM VALUES (5, 1), (7, 3), (10, 2), (12, 4), (15, 1) AS t(col1, col2)
GROUP BY col1 / 5, col2 % 2, col1, col2;