--SET spark.sql.join.asofJoin.enabled=true

DROP TABLE IF EXISTS streaming_sql_golden_unsupported_source;
DROP TABLE IF EXISTS streaming_sql_golden_unsupported_batch;
DROP TABLE IF EXISTS streaming_sql_golden_distinct_agg_sink;
DROP TABLE IF EXISTS streaming_sql_golden_asof_right_stream_sink;
DROP TABLE IF EXISTS streaming_sql_golden_nearest_by_sink;
DROP TABLE IF EXISTS streaming_sql_golden_streaming_subquery_sink;
DROP TABLE IF EXISTS streaming_sql_golden_mixed_union_sink;
DROP TABLE IF EXISTS streaming_sql_golden_intersect_sink;
DROP TABLE IF EXISTS streaming_sql_golden_except_sink;
DROP TABLE IF EXISTS streaming_sql_golden_full_outer_sink;
DROP TABLE IF EXISTS streaming_sql_golden_sort_sink;
DROP TABLE IF EXISTS streaming_sql_golden_sample_sink;
DROP TABLE IF EXISTS streaming_sql_golden_window_sink;
DROP TABLE IF EXISTS streaming_sql_golden_offset_sink;
DROP TABLE IF EXISTS streaming_sql_golden_monotonic_id_sink;

CREATE TABLE streaming_sql_golden_unsupported_source (
  id INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_unsupported_source VALUES
  (1, TIMESTAMP '2024-01-01 00:00:01'),
  (2, TIMESTAMP '2024-01-01 00:00:02');

CREATE TABLE streaming_sql_golden_unsupported_batch (
  id INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_unsupported_batch VALUES
  (1, TIMESTAMP '2024-01-01 00:00:00'),
  (3, TIMESTAMP '2024-01-01 00:00:03');

CREATE TABLE streaming_sql_golden_distinct_agg_sink (
  event_time TIMESTAMP, distinct_ids BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_distinct_agg_flow AS
INSERT INTO streaming_sql_golden_distinct_agg_sink BY NAME
SELECT event_time, COUNT(DISTINCT id) AS distinct_ids
FROM STREAM(streaming_sql_golden_unsupported_source)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
GROUP BY event_time;

REFRESH FLOW streaming_sql_golden_distinct_agg_flow;

CREATE TABLE streaming_sql_golden_asof_right_stream_sink (
  batch_id INT, stream_id INT) USING parquet;

CREATE FLOW streaming_sql_golden_asof_right_stream_flow AS
INSERT INTO streaming_sql_golden_asof_right_stream_sink BY NAME
SELECT b.id AS batch_id, s.id AS stream_id
FROM streaming_sql_golden_unsupported_batch b
INNER ASOF JOIN STREAM(streaming_sql_golden_unsupported_source) s
  MATCH_CONDITION (b.event_time >= s.event_time)
  ON b.id = s.id;

REFRESH FLOW streaming_sql_golden_asof_right_stream_flow;

CREATE TABLE streaming_sql_golden_nearest_by_sink (
  stream_id INT, batch_id INT) USING parquet;

CREATE FLOW streaming_sql_golden_nearest_by_flow AS
INSERT INTO streaming_sql_golden_nearest_by_sink BY NAME
SELECT s.id AS stream_id, b.id AS batch_id
FROM STREAM(streaming_sql_golden_unsupported_source) s
JOIN streaming_sql_golden_unsupported_batch b
  APPROX NEAREST 1 BY DISTANCE abs(s.id - b.id);

REFRESH FLOW streaming_sql_golden_nearest_by_flow;

CREATE TABLE streaming_sql_golden_streaming_subquery_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_streaming_subquery_flow AS
INSERT INTO streaming_sql_golden_streaming_subquery_sink BY NAME
SELECT b.id
FROM streaming_sql_golden_unsupported_batch b
WHERE EXISTS (
  SELECT 1
  FROM STREAM(streaming_sql_golden_unsupported_source) s
  WHERE s.id = b.id);

REFRESH FLOW streaming_sql_golden_streaming_subquery_flow;

CREATE TABLE streaming_sql_golden_mixed_union_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_mixed_union_flow AS
INSERT INTO streaming_sql_golden_mixed_union_sink BY NAME
SELECT id FROM STREAM(streaming_sql_golden_unsupported_source)
UNION ALL
SELECT id FROM streaming_sql_golden_unsupported_batch;

REFRESH FLOW streaming_sql_golden_mixed_union_flow;

CREATE TABLE streaming_sql_golden_intersect_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_intersect_flow AS
INSERT INTO streaming_sql_golden_intersect_sink BY NAME
SELECT id FROM STREAM(streaming_sql_golden_unsupported_source)
INTERSECT
SELECT id FROM streaming_sql_golden_unsupported_batch;

REFRESH FLOW streaming_sql_golden_intersect_flow;

CREATE TABLE streaming_sql_golden_except_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_except_flow AS
INSERT INTO streaming_sql_golden_except_sink BY NAME
SELECT id FROM STREAM(streaming_sql_golden_unsupported_source)
EXCEPT
SELECT id FROM streaming_sql_golden_unsupported_batch;

REFRESH FLOW streaming_sql_golden_except_flow;

CREATE TABLE streaming_sql_golden_full_outer_sink (
  stream_id INT, batch_id INT) USING parquet;

CREATE FLOW streaming_sql_golden_full_outer_flow AS
INSERT INTO streaming_sql_golden_full_outer_sink BY NAME
SELECT s.id AS stream_id, b.id AS batch_id
FROM STREAM(streaming_sql_golden_unsupported_source) s
FULL OUTER JOIN streaming_sql_golden_unsupported_batch b
  ON s.id = b.id;

REFRESH FLOW streaming_sql_golden_full_outer_flow;

CREATE TABLE streaming_sql_golden_sort_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_sort_flow AS
INSERT INTO streaming_sql_golden_sort_sink BY NAME
SELECT id
FROM STREAM(streaming_sql_golden_unsupported_source)
ORDER BY id;

REFRESH FLOW streaming_sql_golden_sort_flow;

CREATE TABLE streaming_sql_golden_sample_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_sample_flow AS
INSERT INTO streaming_sql_golden_sample_sink BY NAME
SELECT id
FROM (
  SELECT id FROM STREAM(streaming_sql_golden_unsupported_source)
) TABLESAMPLE (50 PERCENT);

REFRESH FLOW streaming_sql_golden_sample_flow;

CREATE TABLE streaming_sql_golden_window_sink (id INT, row_num INT) USING parquet;

CREATE FLOW streaming_sql_golden_window_flow AS
INSERT INTO streaming_sql_golden_window_sink BY NAME
SELECT id, ROW_NUMBER() OVER (ORDER BY event_time) AS row_num
FROM STREAM(streaming_sql_golden_unsupported_source);

REFRESH FLOW streaming_sql_golden_window_flow;

CREATE TABLE streaming_sql_golden_offset_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_offset_flow AS
INSERT INTO streaming_sql_golden_offset_sink BY NAME
SELECT id
FROM STREAM(streaming_sql_golden_unsupported_source)
OFFSET 1;

REFRESH FLOW streaming_sql_golden_offset_flow;

CREATE TABLE streaming_sql_golden_monotonic_id_sink (
  id INT, generated_id BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_monotonic_id_flow AS
INSERT INTO streaming_sql_golden_monotonic_id_sink BY NAME
SELECT id, monotonically_increasing_id() AS generated_id
FROM STREAM(streaming_sql_golden_unsupported_source);

REFRESH FLOW streaming_sql_golden_monotonic_id_flow;

DROP TABLE streaming_sql_golden_unsupported_source;
DROP TABLE streaming_sql_golden_unsupported_batch;
DROP TABLE streaming_sql_golden_distinct_agg_sink;
DROP TABLE streaming_sql_golden_asof_right_stream_sink;
DROP TABLE streaming_sql_golden_nearest_by_sink;
DROP TABLE streaming_sql_golden_streaming_subquery_sink;
DROP TABLE streaming_sql_golden_mixed_union_sink;
DROP TABLE streaming_sql_golden_intersect_sink;
DROP TABLE streaming_sql_golden_except_sink;
DROP TABLE streaming_sql_golden_full_outer_sink;
DROP TABLE streaming_sql_golden_sort_sink;
DROP TABLE streaming_sql_golden_sample_sink;
DROP TABLE streaming_sql_golden_window_sink;
DROP TABLE streaming_sql_golden_offset_sink;
DROP TABLE streaming_sql_golden_monotonic_id_sink;
