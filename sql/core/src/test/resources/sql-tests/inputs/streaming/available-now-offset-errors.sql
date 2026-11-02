DROP TABLE IF EXISTS streaming_sql_golden_offset_source;
DROP TABLE IF EXISTS streaming_sql_golden_offset_sink;

CREATE TABLE streaming_sql_golden_offset_source (
  id INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_offset_source VALUES
  (1, TIMESTAMP '2024-01-01 00:00:01'),
  (2, TIMESTAMP '2024-01-01 00:00:02');

CREATE TABLE streaming_sql_golden_offset_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_offset_flow AS
INSERT INTO streaming_sql_golden_offset_sink BY NAME
SELECT id
FROM STREAM(streaming_sql_golden_offset_source)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
OFFSET 1;

REFRESH FLOW streaming_sql_golden_offset_flow;

DROP TABLE streaming_sql_golden_offset_source;
DROP TABLE streaming_sql_golden_offset_sink;
