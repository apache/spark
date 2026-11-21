DROP TABLE IF EXISTS streaming_sql_golden_join_aggregation_events;
DROP TABLE IF EXISTS streaming_sql_golden_join_aggregation_categories;
DROP TABLE IF EXISTS streaming_sql_golden_join_aggregation_sink;

CREATE TABLE streaming_sql_golden_join_aggregation_events (
  value BIGINT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_join_aggregation_events
SELECT value, timestamp_seconds(value) AS event_time
FROM VALUES (15) AS events(value);

CREATE TABLE streaming_sql_golden_join_aggregation_categories (
  value BIGINT, category STRING) USING parquet;
INSERT INTO streaming_sql_golden_join_aggregation_categories
VALUES (10, 'even'), (11, 'odd'), (14, 'even'), (15, 'odd'), (25, 'odd');

CREATE TABLE streaming_sql_golden_join_aggregation_sink (
  window_start BIGINT, category STRING, event_count BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_join_aggregation_flow AS
INSERT INTO streaming_sql_golden_join_aggregation_sink BY NAME
SELECT
  CAST(window(event_time, '5 seconds').start AS BIGINT) AS window_start,
  streaming_sql_golden_join_aggregation_categories.category,
  CAST(count(*) AS BIGINT) AS event_count
FROM STREAM(streaming_sql_golden_join_aggregation_events)
  WATERMARK event_time DELAY OF INTERVAL 10 SECONDS
JOIN streaming_sql_golden_join_aggregation_categories
ON streaming_sql_golden_join_aggregation_events.value =
  streaming_sql_golden_join_aggregation_categories.value
GROUP BY
  streaming_sql_golden_join_aggregation_categories.category,
  window(event_time, '5 seconds');

REFRESH FLOW streaming_sql_golden_join_aggregation_flow;
SELECT * FROM streaming_sql_golden_join_aggregation_sink ORDER BY window_start, category;

INSERT INTO streaming_sql_golden_join_aggregation_events
SELECT value, timestamp_seconds(value) AS event_time
FROM VALUES (10), (11), (14) AS events(value);

REFRESH FLOW streaming_sql_golden_join_aggregation_flow;
SELECT * FROM streaming_sql_golden_join_aggregation_sink ORDER BY window_start, category;

INSERT INTO streaming_sql_golden_join_aggregation_events
SELECT value, timestamp_seconds(value) AS event_time
FROM VALUES (25) AS events(value);

REFRESH FLOW streaming_sql_golden_join_aggregation_flow;
SELECT * FROM streaming_sql_golden_join_aggregation_sink ORDER BY window_start, category;

DROP TABLE streaming_sql_golden_join_aggregation_events;
DROP TABLE streaming_sql_golden_join_aggregation_categories;
DROP TABLE streaming_sql_golden_join_aggregation_sink;
