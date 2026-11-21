DROP TABLE IF EXISTS streaming_sql_golden_outer_stream;
DROP TABLE IF EXISTS streaming_sql_golden_outer_batch;
DROP TABLE IF EXISTS streaming_sql_golden_outer_sink;

CREATE TABLE streaming_sql_golden_outer_stream (
  id INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_outer_stream VALUES
  (1, TIMESTAMP '2024-01-01 00:00:01'),
  (2, TIMESTAMP '2024-01-01 00:00:02');

CREATE TABLE streaming_sql_golden_outer_batch (id INT) USING parquet;
INSERT INTO streaming_sql_golden_outer_batch VALUES (1), (3);

CREATE TABLE streaming_sql_golden_outer_sink (
  stream_id INT, batch_id INT) USING parquet;

CREATE FLOW streaming_sql_golden_outer_flow AS
INSERT INTO streaming_sql_golden_outer_sink BY NAME
SELECT s.id AS stream_id, b.id AS batch_id
FROM STREAM(streaming_sql_golden_outer_stream)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND s
FULL OUTER JOIN streaming_sql_golden_outer_batch b
  ON s.id = b.id;

REFRESH FLOW streaming_sql_golden_outer_flow;

DROP TABLE streaming_sql_golden_outer_stream;
DROP TABLE streaming_sql_golden_outer_batch;
DROP TABLE streaming_sql_golden_outer_sink;
