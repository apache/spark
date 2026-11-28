DROP TABLE IF EXISTS streaming_sql_golden_distinct_agg_source;
DROP TABLE IF EXISTS streaming_sql_golden_distinct_agg_sink;

CREATE TABLE streaming_sql_golden_distinct_agg_source (
  id INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_distinct_agg_source VALUES
  (1, TIMESTAMP '2024-01-01 00:00:01'),
  (2, TIMESTAMP '2024-01-01 00:00:02');

CREATE TABLE streaming_sql_golden_distinct_agg_sink (
  event_time TIMESTAMP, distinct_ids BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_distinct_agg_flow AS
INSERT INTO streaming_sql_golden_distinct_agg_sink BY NAME
SELECT event_time, COUNT(DISTINCT id) AS distinct_ids
FROM STREAM(streaming_sql_golden_distinct_agg_source)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
GROUP BY event_time;

REFRESH FLOW streaming_sql_golden_distinct_agg_flow;

DROP TABLE streaming_sql_golden_distinct_agg_source;
DROP TABLE streaming_sql_golden_distinct_agg_sink;
