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

-- Setup: temp view used by sum/nested-window cases below
CREATE OR REPLACE TEMPORARY VIEW ts_vals AS
SELECT * FROM VALUES
  (CAST('2021-01-01 00:00:00' AS TIMESTAMP), 10),
  (CAST('2021-01-01 00:01:00' AS TIMESTAMP), 20),
  (CAST('2021-01-01 00:06:00' AS TIMESTAMP), 30)
AS tab(ts, v);

-- tumbling_window_basic
SELECT
  a,
  window.start,
  window.end,
  count(*) AS cnt
FROM ts_data
GROUP BY a, window(ts, '5 minutes')
ORDER BY a, start;

-- window_1_hour
SELECT
  window.start,
  window.end,
  count(*) AS cnt
FROM ts_data
GROUP BY window(ts, '1 hour')
ORDER BY start;

-- sliding_window_basic
SELECT
  window.start,
  window.end,
  count(*) AS cnt
FROM ts_data
GROUP BY window(ts, '10 minutes', '5 minutes')
ORDER BY window.start;

-- sliding_window_non_exact_division
SELECT
  window.start,
  window.end,
  count(*) AS cnt
FROM ts_data
GROUP BY window(ts, '7 minutes', '3 minutes')
ORDER BY window.start;

-- sliding_window_ntz
SELECT
  window.start,
  window.end,
  count(*) AS cnt
FROM (
  SELECT TIMESTAMP_NTZ '2021-01-01 00:00:00' AS ts
  UNION ALL SELECT TIMESTAMP_NTZ '2021-01-01 00:04:30'
  UNION ALL SELECT TIMESTAMP_NTZ '2021-01-01 00:06:00'
)
GROUP BY window(ts, '10 minutes', '5 minutes')
ORDER BY window.start;

-- sliding_window_with_start_time
SELECT
  window.start,
  window.end,
  count(*) AS cnt
FROM ts_data
GROUP BY window(ts, '10 minutes', '5 minutes', '2 minutes')
ORDER BY window.start;

-- sliding_window_null_timestamps
SELECT
  window.start,
  window.end,
  sum(value) AS total
FROM VALUES
  (CAST('2016-03-27 09:00:05' AS TIMESTAMP), 1),
  (CAST('2016-03-27 09:00:32' AS TIMESTAMP), 2),
  (CAST(NULL AS TIMESTAMP), 3),
  (CAST(NULL AS TIMESTAMP), 4)
AS tab(time, value)
GROUP BY window(time, '7 seconds', '3 seconds')
ORDER BY window.start;

-- window_order_by_desc
SELECT
  window.start,
  window.end,
  count(*) AS cnt
FROM ts_data
GROUP BY window(ts, '5 minutes')
ORDER BY window.start DESC;

-- window_with_sum_and_count
SELECT
  window.start,
  sum(v),
  count(*) AS cnt
FROM ts_vals
GROUP BY window(ts, '5 minutes')
ORDER BY window.start;

-- window_in_simple_project_tumbling
SELECT window(ts, '5 minutes').start AS bucket_start
FROM ts_data
ORDER BY bucket_start;

-- window_in_simple_project_sliding
SELECT window(ts, '7 minutes', '3 minutes').start AS bucket_start
FROM ts_data
ORDER BY bucket_start;

-- window_slide_exceeds_duration
SELECT count(*)
FROM ts_data
GROUP BY window(ts, '10 minutes', '20 minutes');

-- window_start_time_exceeds_slide
SELECT count(*)
FROM ts_data
GROUP BY window(ts, '10 minutes', '5 minutes', '6 minutes');

-- window_multiple_windows_error
SELECT count(*)
FROM ts_data
GROUP BY window(ts, '5 minutes'), window(ts, '10 minutes');

-- window_multiple_windows_nested_error
SELECT count(*)
FROM ts_data
GROUP BY array(window(ts, '5 minutes'), window(ts, '10 minutes'));

-- window_wrong_type
SELECT count(*)
FROM VALUES (1), (2) AS tab(x)
GROUP BY window(x, '5 minutes');

-- window_negative_duration
SELECT count(*)
FROM ts_data
GROUP BY window(ts, '-5 minutes');

-- tumbling_window_subsecond
SELECT
  window.start,
  window.end,
  count(*) AS cnt
FROM VALUES
  (CAST('2021-01-01 00:04:59.999999' AS TIMESTAMP)),
  (CAST('2021-01-01 00:05:00.000001' AS TIMESTAMP)),
  (CAST('2021-01-01 00:02:30.123456' AS TIMESTAMP))
AS tab(ts)
GROUP BY window(ts, '5 minutes')
ORDER BY start;

-- window_timestamp_ntz
SELECT
  window.start,
  window.end,
  count(*) AS cnt
FROM VALUES
  (CAST('2021-01-01 00:00:00' AS TIMESTAMP_NTZ)),
  (CAST('2021-01-01 00:06:00' AS TIMESTAMP_NTZ))
AS tab(ts)
GROUP BY window(ts, '5 minutes')
ORDER BY start;

-- window_select_star_error
SELECT *
FROM ts_data
GROUP BY a, window(ts, '5 minutes');

-- window_select_star_subquery
SELECT * FROM (
  SELECT
    a,
    window.start,
    window.end,
    count(*) AS cnt
  FROM ts_data
  GROUP BY a, window(ts, '5 minutes')
) sub
ORDER BY a, start;

-- window_wrong_arg_count_1
SELECT count(*)
FROM ts_data
GROUP BY window(ts);

-- window_unparseable_interval
SELECT count(*)
FROM ts_data
GROUP BY window(ts, 'not_an_interval');

-- window_month_based_interval
SELECT count(*)
FROM ts_data
GROUP BY window(ts, '2 months');

-- window_non_literal_duration
SELECT count(*)
FROM ts_data
GROUP BY window(ts, a);

-- window_negative_slide_duration
SELECT count(*)
FROM ts_data
GROUP BY window(ts, '10 minutes', '-5 minutes');

-- window_zero_duration
SELECT count(*)
FROM ts_data
GROUP BY window(ts, '0 minutes');

-- window_with_exists_subquery
SELECT
  window.start,
  count(*) AS cnt
FROM ts_data t1
WHERE EXISTS (
  SELECT 1 FROM ts_data t2 WHERE t2.a = t1.a AND t2.a = 'A1'
)
GROUP BY window(t1.ts, '5 minutes')
ORDER BY start;

-- negative_start_time
SELECT
  window.start,
  window.end,
  count(*) AS counts
FROM VALUES
  (CAST('2016-03-27 19:39:30' AS TIMESTAMP), 1, 'a'),
  (CAST('2016-03-27 19:39:25' AS TIMESTAMP), 2, 'a')
AS tab(time, value, id)
GROUP BY window(time, '10 seconds', '10 seconds', '-5 seconds')
ORDER BY window.start;

-- tumbling_groupby_multi_bucket
SELECT
  window.start,
  window.end,
  count(*) AS counts
FROM VALUES
  (CAST('2016-03-27 19:39:34' AS TIMESTAMP), 1, 'a'),
  (CAST('2016-03-27 19:39:56' AS TIMESTAMP), 2, 'a'),
  (CAST('2016-03-27 19:39:27' AS TIMESTAMP), 4, 'b')
AS tab(time, value, id)
GROUP BY window(time, '10 seconds')
ORDER BY window.start;

-- tumbling_with_start_time
SELECT
  id,
  window.start,
  window.end,
  count(*) AS counts
FROM VALUES
  (CAST('2016-03-27 19:39:34' AS TIMESTAMP), 1, 'a'),
  (CAST('2016-03-27 19:39:56' AS TIMESTAMP), 2, 'a'),
  (CAST('2016-03-27 19:39:27' AS TIMESTAMP), 4, 'b')
AS tab(time, value, id)
GROUP BY window(time, '10 seconds', '10 seconds', '5 seconds'), id
ORDER BY id, window.start;

-- null_timestamps
SELECT
  window.start,
  window.end,
  sum(value) AS total
FROM VALUES
  (CAST('2016-03-27 09:00:05' AS TIMESTAMP), 1),
  (CAST('2016-03-27 09:00:32' AS TIMESTAMP), 2),
  (CAST(NULL AS TIMESTAMP), 3),
  (CAST(NULL AS TIMESTAMP), 4)
AS tab(time, value)
GROUP BY window(time, '10 seconds')
ORDER BY window.start;

-- window_in_select_group_by_all
SELECT window(ts, '5 minutes').end AS bucket_end, count(*) AS cnt
FROM ts_data
GROUP BY ALL
ORDER BY bucket_end;

-- window_in_select_and_group_by
SELECT
  window(ts, '5 minutes').start AS bucket_start,
  window(ts, '5 minutes').end AS bucket_end,
  count(*) AS cnt
FROM ts_data
GROUP BY window(ts, '5 minutes')
ORDER BY bucket_start;

-- window_multi_in_select_group_by_all
SELECT
  window(ts, '5 minutes').start AS bucket_start,
  window(ts, '5 minutes').end AS bucket_end,
  count(*) AS cnt
FROM ts_data
GROUP BY ALL
ORDER BY bucket_start;

-- window_different_durations_rejected
SELECT
  window(ts, '5 minutes').start AS bucket_5,
  window(ts, '10 minutes').end AS bucket_10,
  count(*) AS cnt
FROM ts_data
GROUP BY window(ts, '5 minutes');

-- window_time_basic
SELECT window_time(window) AS event_time, count(*) AS cnt
FROM ts_data
GROUP BY window(ts, '5 minutes')
ORDER BY event_time;

-- window_time_ntz
WITH ts_ntz_data AS (
  SELECT TIMESTAMP_NTZ '2021-01-01 00:00:00' AS ts
  UNION ALL SELECT TIMESTAMP_NTZ '2021-01-01 00:04:30'
  UNION ALL SELECT TIMESTAMP_NTZ '2021-01-01 00:06:00'
)
SELECT window_time(window) AS event_time, count(*) AS cnt
FROM ts_ntz_data
GROUP BY window(ts, '5 minutes')
ORDER BY event_time;

-- window_time_wrong_type
SELECT window_time(ts) FROM ts_data;

-- nested_window_in_group_by
SELECT outer_start, outer_end, total_cnt
FROM (
  SELECT
    window.start AS outer_start,
    window.end AS outer_end,
    sum(v) AS total_cnt
  FROM (
    SELECT
      named_struct('start', ts, 'end', ts + INTERVAL 1 MINUTE) AS inner_window,
      v
    FROM ts_vals
  )
  GROUP BY window(inner_window, '5 minutes')
)
ORDER BY outer_start;

-- nested_window_via_subquery
SELECT outer_window.start AS outer_start, outer_window.end AS outer_end, cnt
FROM (
  SELECT window(inner_window, '5 minutes') AS outer_window, sum(cnt) AS cnt
  FROM (
    SELECT window(ts, '1 minute') AS inner_window, count(*) AS cnt
    FROM ts_data
    GROUP BY window(ts, '1 minute')
  )
  GROUP BY window(inner_window, '5 minutes')
)
ORDER BY outer_start;

-- nested_literal_window_in_group_by
SELECT
  window(window(ts, '1 minute'), '5 minutes').start AS outer_start,
  count(*) AS cnt
FROM ts_data
GROUP BY window(window(ts, '1 minute'), '5 minutes')
ORDER BY outer_start;

-- nested_literal_window_in_group_by_all
SELECT
  window(window(ts, '1 minute'), '5 minutes').start AS outer_start,
  window(window(ts, '1 minute'), '5 minutes').end AS outer_end,
  count(*) AS cnt
FROM ts_data
GROUP BY ALL
ORDER BY outer_start;

-- window_time_wrong_arity
SELECT window_time(window, window) FROM ts_data GROUP BY window(ts, '5 minutes');

-- window_time_struct_wrong_inner_type
SELECT window_time(named_struct('start', 1, 'end', 2)) FROM ts_data;

-- window_time_struct_wrong_field_names
SELECT window_time(named_struct('foo', ts, 'bar', ts)) FROM ts_data;

-- window_time_struct_inline_rejected
SELECT window_time(named_struct('start', ts, 'end', ts + INTERVAL 1 MINUTE)) FROM ts_data;

-- window_time_struct_from_cte_rejected
WITH x AS (SELECT named_struct('start', ts, 'end', ts + INTERVAL 1 MINUTE) AS w FROM ts_data)
SELECT window_time(w) FROM x
ORDER BY 1;

-- window_in_simple_project_repeated_same
SELECT
  window(ts, '5 minutes').start AS bucket_start,
  window(ts, '5 minutes').end AS bucket_end
FROM ts_data
ORDER BY bucket_start;

-- window_in_simple_project_different_windows_error
SELECT
  window(ts, '5 minutes').start AS bucket_5,
  window(ts, '10 minutes').end AS bucket_10
FROM ts_data;

-- window_in_where_clause
SELECT count(*)
FROM ts_data
WHERE window(ts, '5 minutes').start IS NOT NULL;

-- window_in_having_clause_rejected
SELECT count(*) AS cnt
FROM ts_data
GROUP BY ts
HAVING window(ts, '5 minutes').start IS NOT NULL;

-- window_in_order_by_rejected
SELECT count(*) FROM ts_data GROUP BY ts ORDER BY window(ts, '5 minutes').start;

-- window_in_qualify
SELECT ts, row_number() OVER (ORDER BY ts) AS rn FROM ts_data QUALIFY window(ts, '5 minutes').start IS NOT NULL;

-- window_in_pivot_aggregate_rejected
SELECT * FROM ts_data
PIVOT (window(ts, '5 minutes').start FOR a IN ('A1', 'A2'));

-- stacked_sibling_windows_on_same_base_rejected
SELECT
  window(window(ts, '1 minute'), '5 minutes').start,
  window(window(ts, '1 minute'), '10 minutes').end
FROM ts_data;

-- nested_window_bare_reference_ambiguous
SELECT window.start FROM ts_data GROUP BY window(window(ts, '1 minute'), '5 minutes');

-- session_window_basic
SELECT
  a,
  session_window.start,
  session_window.end,
  count(*) AS cnt
FROM ts_data
GROUP BY a, session_window(ts, '5 minutes')
ORDER BY a, start;

-- session_window_with_conditional_gap
SELECT
  a,
  session_window.start,
  session_window.end,
  count(*) AS cnt
FROM VALUES
  ('A1', CAST('2021-01-01 00:00:00' AS TIMESTAMP)),
  ('A1', CAST('2021-01-01 00:04:30' AS TIMESTAMP)),
  ('A1', CAST('2021-01-01 00:10:00' AS TIMESTAMP)),
  ('A2', CAST('2021-01-01 00:01:00' AS TIMESTAMP)),
  ('A2', CAST('2021-01-01 00:04:30' AS TIMESTAMP))
AS tab(a, ts)
GROUP BY a, session_window(ts, CASE WHEN a = 'A1' THEN '5 minutes'
                                    WHEN a = 'A2' THEN '1 minute'
                                    ELSE '10 minutes' END)
ORDER BY a, start;

-- session_window_with_column_gap
SELECT
  a,
  session_window.start,
  session_window.end,
  count(*) AS cnt
FROM VALUES
  ('A1', CAST('2021-01-01 00:00:00' AS TIMESTAMP), '5 minutes'),
  ('A1', CAST('2021-01-01 00:04:30' AS TIMESTAMP), '5 minutes'),
  ('A2', CAST('2021-01-01 00:01:00' AS TIMESTAMP), '1 minute')
AS tab(a, ts, gap)
GROUP BY a, session_window(ts, gap)
ORDER BY a, start;

-- session_window_ntz
SELECT
  session_window.start,
  session_window.end,
  count(*) AS cnt
FROM VALUES
  (CAST('2021-01-01 00:00:00' AS TIMESTAMP_NTZ)),
  (CAST('2021-01-01 00:04:30' AS TIMESTAMP_NTZ)),
  (CAST('2021-01-01 00:10:00' AS TIMESTAMP_NTZ))
AS tab(ts)
GROUP BY session_window(ts, '5 minutes')
ORDER BY start;

-- session_window_null_timestamps
SELECT
  session_window.start,
  session_window.end,
  count(*) AS cnt
FROM VALUES
  (CAST(NULL AS TIMESTAMP)),
  (CAST('2021-01-01 00:00:00' AS TIMESTAMP)),
  (CAST('2021-01-01 00:01:00' AS TIMESTAMP))
AS tab(ts)
GROUP BY session_window(ts, '5 minutes')
ORDER BY start;

-- session_window_subsecond
SELECT
  session_window.start,
  session_window.end,
  count(*) AS cnt
FROM VALUES
  (CAST('2021-01-01 00:00:00.500' AS TIMESTAMP)),
  (CAST('2021-01-01 00:00:00.700' AS TIMESTAMP)),
  (CAST('2021-01-01 00:00:01.200' AS TIMESTAMP))
AS tab(ts)
GROUP BY session_window(ts, '500 milliseconds')
ORDER BY start;

-- session_window_negative_gap
SELECT count(*) FROM ts_data GROUP BY session_window(ts, '-5 minutes');

-- session_window_zero_gap
SELECT count(*) FROM ts_data GROUP BY session_window(ts, '0 seconds');

-- session_window_wrong_arg_count
SELECT count(*) FROM ts_data GROUP BY session_window(ts);

-- session_window_multiple_error
SELECT count(*) FROM ts_data
GROUP BY session_window(ts, '5 minutes'), session_window(ts, '10 minutes');

-- window_time_session_basic_rejected
SELECT window_time(session_window) AS event_time, count(*) AS cnt
FROM ts_data
GROUP BY session_window(ts, '5 minutes')
ORDER BY event_time;

-- session_window_in_where_clause
SELECT count(*)
FROM ts_data
WHERE session_window(ts, '5 minutes').start IS NOT NULL;

-- session_window_in_qualify
SELECT ts, row_number() OVER (ORDER BY ts) AS rn
FROM ts_data
QUALIFY session_window(ts, '5 minutes').start IS NOT NULL;

-- session_window_select_star_error
SELECT * FROM ts_data GROUP BY a, session_window(ts, '5 minutes');
