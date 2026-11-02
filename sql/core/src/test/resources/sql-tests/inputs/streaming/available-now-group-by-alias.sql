DROP TABLE IF EXISTS streaming_sql_golden_group_by_alias_source;
DROP TABLE IF EXISTS streaming_sql_golden_group_by_alias_sink;

CREATE TABLE streaming_sql_golden_group_by_alias_source (
  event_time TIMESTAMP, category STRING, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_group_by_alias_source VALUES
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 2),
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 3),
  (TIMESTAMP '2024-01-01 00:00:02', 'games', 7),
  (TIMESTAMP '2024-01-01 00:00:10', 'watermark', 0);

CREATE TABLE streaming_sql_golden_group_by_alias_sink (
  event_ts TIMESTAMP, kind STRING, total BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_group_by_alias_flow AS
INSERT INTO streaming_sql_golden_group_by_alias_sink BY NAME
SELECT event_time AS event_ts, category AS kind, SUM(amount) AS total
FROM STREAM(streaming_sql_golden_group_by_alias_source)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
GROUP BY event_ts, kind;

REFRESH FLOW streaming_sql_golden_group_by_alias_flow;

SELECT * FROM streaming_sql_golden_group_by_alias_sink ORDER BY event_ts, kind;

DROP TABLE streaming_sql_golden_group_by_alias_source;
DROP TABLE streaming_sql_golden_group_by_alias_sink;
