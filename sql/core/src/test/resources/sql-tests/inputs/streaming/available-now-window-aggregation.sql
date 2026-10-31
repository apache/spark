DROP TABLE IF EXISTS streaming_sql_golden_aggregation_events;
DROP TABLE IF EXISTS streaming_sql_golden_aggregation_sink;

CREATE TABLE streaming_sql_golden_aggregation_events (
  event_time TIMESTAMP, category STRING, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_aggregation_events VALUES
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 2),
  (TIMESTAMP '2024-01-01 00:00:05', 'books', 3),
  (TIMESTAMP '2024-01-01 00:01:02', 'games', 7);

CREATE TABLE streaming_sql_golden_aggregation_sink (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  category STRING,
  total BIGINT,
  event_count BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_aggregation_flow AS
INSERT INTO streaming_sql_golden_aggregation_sink BY NAME
SELECT
  window(event_time, '1 minute').start AS window_start,
  window(event_time, '1 minute').end AS window_end,
  category,
  sum(amount) AS total,
  count(*) AS event_count
FROM STREAM(streaming_sql_golden_aggregation_events)
  WATERMARK event_time DELAY OF INTERVAL 10 SECONDS
GROUP BY window(event_time, '1 minute'), category;

REFRESH FLOW streaming_sql_golden_aggregation_flow;

SELECT *
FROM streaming_sql_golden_aggregation_sink
ORDER BY window_start, category;

INSERT INTO streaming_sql_golden_aggregation_events VALUES
  (TIMESTAMP '2024-01-01 00:10:00', '__watermark__', 0);

REFRESH FLOW streaming_sql_golden_aggregation_flow;

SELECT *
FROM streaming_sql_golden_aggregation_sink
ORDER BY window_start, category;

DROP TABLE streaming_sql_golden_aggregation_events;
DROP TABLE streaming_sql_golden_aggregation_sink;
