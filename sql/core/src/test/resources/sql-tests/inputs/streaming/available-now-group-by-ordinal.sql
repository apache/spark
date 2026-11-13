DROP TABLE IF EXISTS streaming_sql_golden_group_by_ordinal_source;
DROP TABLE IF EXISTS streaming_sql_golden_group_by_ordinal_sink;

CREATE TABLE streaming_sql_golden_group_by_ordinal_source (
  event_time TIMESTAMP, category STRING, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_group_by_ordinal_source VALUES
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 2),
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 3),
  (TIMESTAMP '2024-01-01 00:00:02', 'games', 7),
  (TIMESTAMP '2024-01-01 00:00:10', 'watermark', 0);

CREATE TABLE streaming_sql_golden_group_by_ordinal_sink (
  event_time TIMESTAMP, category STRING, total BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_group_by_ordinal_flow AS
INSERT INTO streaming_sql_golden_group_by_ordinal_sink BY NAME
SELECT event_time, category, SUM(amount) AS total
FROM STREAM(streaming_sql_golden_group_by_ordinal_source)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
GROUP BY 1, 2;

REFRESH FLOW streaming_sql_golden_group_by_ordinal_flow;

SELECT * FROM streaming_sql_golden_group_by_ordinal_sink ORDER BY event_time, category;

DROP TABLE streaming_sql_golden_group_by_ordinal_source;
DROP TABLE streaming_sql_golden_group_by_ordinal_sink;
