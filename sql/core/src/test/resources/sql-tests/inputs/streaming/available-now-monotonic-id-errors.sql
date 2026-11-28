DROP TABLE IF EXISTS streaming_sql_golden_monotonic_id_source;
DROP TABLE IF EXISTS streaming_sql_golden_monotonic_id_sink;

CREATE TABLE streaming_sql_golden_monotonic_id_source (
  id INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_monotonic_id_source VALUES
  (1, TIMESTAMP '2024-01-01 00:00:01'),
  (2, TIMESTAMP '2024-01-01 00:00:02');

CREATE TABLE streaming_sql_golden_monotonic_id_sink (
  id INT, generated_id BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_monotonic_id_flow AS
INSERT INTO streaming_sql_golden_monotonic_id_sink BY NAME
SELECT id, monotonically_increasing_id() AS generated_id
FROM STREAM(streaming_sql_golden_monotonic_id_source)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND;

REFRESH FLOW streaming_sql_golden_monotonic_id_flow;

DROP TABLE streaming_sql_golden_monotonic_id_source;
DROP TABLE streaming_sql_golden_monotonic_id_sink;
