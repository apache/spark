-- Setup: temp view used across cases
CREATE OR REPLACE TEMPORARY VIEW ts_data AS
SELECT * FROM VALUES
  ('A1', CAST('2021-01-01 00:00:00' AS TIMESTAMP)),
  ('A1', CAST('2021-01-01 00:04:30' AS TIMESTAMP)),
  ('A1', CAST('2021-01-01 00:06:00' AS TIMESTAMP)),
  ('A2', CAST('2021-01-01 00:01:00' AS TIMESTAMP))
AS tab(a, ts);

-- window_with_rollup
SELECT
  a,
  window.start,
  count(*) AS cnt
FROM ts_data
GROUP BY a, window(ts, '5 minutes') WITH ROLLUP
ORDER BY a, start;

-- window_with_cube
SELECT
  a,
  window.start,
  count(*) AS cnt
FROM ts_data
GROUP BY a, window(ts, '5 minutes') WITH CUBE
ORDER BY a, start;

-- window_with_grouping_sets
SELECT
  a,
  window.start,
  count(*) AS cnt
FROM ts_data
GROUP BY GROUPING SETS ((a, window(ts, '5 minutes')), (a))
ORDER BY a, start;

-- window_nested_in_expression
SELECT
  count(*) AS cnt
FROM ts_data
GROUP BY concat(cast(window(ts, '5 minutes').start AS string), a)
ORDER BY cnt;

-- window_having
SELECT
  window.start,
  count(*) AS cnt
FROM ts_data
GROUP BY window(ts, '5 minutes')
HAVING count(*) > 1
ORDER BY window.start;

-- window_in_subquery
SELECT * FROM (
  SELECT
    a,
    window.start AS ws,
    count(*) AS cnt
  FROM ts_data
  GROUP BY a, window(ts, '5 minutes')
) sub
ORDER BY a, ws;

-- record_at_window_start
SELECT
  window.start,
  window.end,
  count(*) AS counts
FROM VALUES
  (CAST('2016-03-27 19:39:30' AS TIMESTAMP), 1, 'a')
AS tab(time, value, id)
GROUP BY window(time, '10 seconds')
ORDER BY window.start;

-- tumbling_window_pre_epoch
SELECT
  window.start,
  window.end,
  count(*) AS cnt
FROM VALUES
  (CAST('1969-12-31 23:59:59.500000' AS TIMESTAMP)),
  (CAST('1969-12-31 23:55:00.000000' AS TIMESTAMP)),
  (CAST('1970-01-01 00:00:00.000000' AS TIMESTAMP))
AS tab(ts)
GROUP BY window(ts, '5 minutes')
ORDER BY start;

-- sliding_window_with_rollup
SELECT
  a,
  window.start,
  count(*) AS cnt
FROM ts_data
GROUP BY a, window(ts, '5 minutes', '1 minute') WITH ROLLUP
ORDER BY a, start;

-- sliding_window_with_cube
SELECT
  a,
  window.start,
  count(*) AS cnt
FROM ts_data
GROUP BY a, window(ts, '5 minutes', '1 minute') WITH CUBE
ORDER BY a, start;

-- sliding_window_with_grouping_sets
SELECT
  a,
  window.start,
  count(*) AS cnt
FROM ts_data
GROUP BY GROUPING SETS ((a, window(ts, '5 minutes', '1 minute')), (a))
ORDER BY a, start;

-- sliding_window_nested_in_expression
SELECT
  count(*) AS cnt
FROM ts_data
GROUP BY concat(cast(window(ts, '5 minutes', '1 minute').start AS string), a)
ORDER BY cnt;

-- sliding_window_having
SELECT
  window.start,
  count(*) AS cnt
FROM ts_data
GROUP BY window(ts, '5 minutes', '1 minute')
HAVING count(*) > 1
ORDER BY window.start;

-- sliding_window_in_subquery
SELECT * FROM (
  SELECT
    a,
    window.start AS ws,
    count(*) AS cnt
  FROM ts_data
  GROUP BY a, window(ts, '5 minutes', '1 minute')
) sub
ORDER BY a, ws;

-- record_at_sliding_window_start
SELECT
  window.start,
  window.end,
  count(*) AS counts
FROM VALUES
  (CAST('2016-03-27 19:39:30' AS TIMESTAMP), 1, 'a')
AS tab(time, value, id)
GROUP BY window(time, '10 seconds', '5 seconds')
ORDER BY window.start;

-- sliding_window_pre_epoch
SELECT
  window.start,
  window.end,
  count(*) AS cnt
FROM VALUES
  (CAST('1969-12-31 23:59:59.500000' AS TIMESTAMP)),
  (CAST('1969-12-31 23:55:00.000000' AS TIMESTAMP)),
  (CAST('1970-01-01 00:00:00.000000' AS TIMESTAMP))
AS tab(ts)
GROUP BY window(ts, '5 minutes', '1 minute')
ORDER BY start;

-- session_window_with_rollup
SELECT
  a,
  session_window.start,
  count(*) AS cnt
FROM ts_data
GROUP BY a, session_window(ts, '5 minutes') WITH ROLLUP
ORDER BY a, start;

-- session_window_with_cube
SELECT
  a,
  session_window.start,
  count(*) AS cnt
FROM ts_data
GROUP BY a, session_window(ts, '5 minutes') WITH CUBE
ORDER BY a, start;

-- session_window_with_grouping_sets
SELECT
  a,
  session_window.start,
  count(*) AS cnt
FROM ts_data
GROUP BY GROUPING SETS ((a, session_window(ts, '5 minutes')), (a))
ORDER BY a, start;

-- session_window_nested_in_expression
SELECT
  count(*) AS cnt
FROM ts_data
GROUP BY concat(cast(session_window(ts, '5 minutes').start AS string), a)
ORDER BY cnt;

-- session_window_having
SELECT
  session_window.start,
  count(*) AS cnt
FROM ts_data
GROUP BY session_window(ts, '5 minutes')
HAVING count(*) > 1
ORDER BY session_window.start;

-- session_window_in_subquery
SELECT * FROM (
  SELECT
    a,
    session_window.start AS ws,
    count(*) AS cnt
  FROM ts_data
  GROUP BY a, session_window(ts, '5 minutes')
) sub
ORDER BY a, ws;

-- session_window_pre_epoch
SELECT
  session_window.start,
  session_window.end,
  count(*) AS cnt
FROM VALUES
  (CAST('1969-12-31 23:59:59.500000' AS TIMESTAMP)),
  (CAST('1969-12-31 23:55:00.000000' AS TIMESTAMP)),
  (CAST('1970-01-01 00:00:00.000000' AS TIMESTAMP))
AS tab(ts)
GROUP BY session_window(ts, '5 minutes')
ORDER BY start;
