DROP TABLE IF EXISTS streaming_sql_golden_aggregate_filter_source;
DROP TABLE IF EXISTS streaming_sql_golden_aggregate_filter_sink;

CREATE TABLE streaming_sql_golden_aggregate_filter_source (
  event_time TIMESTAMP, category STRING, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_aggregate_filter_source VALUES
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 2),
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 7),
  (TIMESTAMP '2024-01-01 00:00:02', 'games', 3),
  (TIMESTAMP '2024-01-01 00:00:10', 'watermark', 0);

CREATE TABLE streaming_sql_golden_aggregate_filter_sink (
  event_time TIMESTAMP, category STRING, total BIGINT, large_total BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_aggregate_filter_flow AS
INSERT INTO streaming_sql_golden_aggregate_filter_sink BY NAME
SELECT event_time, category, SUM(amount) AS total,
  SUM(amount) FILTER (WHERE amount >= 5) AS large_total
FROM STREAM(streaming_sql_golden_aggregate_filter_source)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
GROUP BY event_time, category;

REFRESH FLOW streaming_sql_golden_aggregate_filter_flow;

SELECT * FROM streaming_sql_golden_aggregate_filter_sink ORDER BY event_time, category;

DROP TABLE streaming_sql_golden_aggregate_filter_source;
DROP TABLE streaming_sql_golden_aggregate_filter_sink;
