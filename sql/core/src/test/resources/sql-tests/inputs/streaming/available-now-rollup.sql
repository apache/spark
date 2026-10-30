DROP TABLE IF EXISTS streaming_sql_golden_rollup_source;
DROP TABLE IF EXISTS streaming_sql_golden_rollup_sink;

CREATE TABLE streaming_sql_golden_rollup_source (
  id INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_rollup_source VALUES
  (1, TIMESTAMP '2024-01-01 00:00:01'),
  (2, TIMESTAMP '2024-01-01 00:00:02'),
  (99, TIMESTAMP '2024-01-01 00:00:10');

CREATE TABLE streaming_sql_golden_rollup_sink (
  event_time TIMESTAMP, id INT, total BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_rollup_flow AS
INSERT INTO streaming_sql_golden_rollup_sink BY NAME
SELECT event_time, id, COUNT(*) AS total
FROM STREAM(streaming_sql_golden_rollup_source)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
GROUP BY event_time, ROLLUP(id);

REFRESH FLOW streaming_sql_golden_rollup_flow;
SELECT * FROM streaming_sql_golden_rollup_sink ORDER BY event_time, id;

DROP TABLE streaming_sql_golden_rollup_source;
DROP TABLE streaming_sql_golden_rollup_sink;
