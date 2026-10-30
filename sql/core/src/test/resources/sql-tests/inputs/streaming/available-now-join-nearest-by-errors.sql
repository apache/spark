DROP TABLE IF EXISTS streaming_sql_golden_nearest_stream;
DROP TABLE IF EXISTS streaming_sql_golden_nearest_batch;
DROP TABLE IF EXISTS streaming_sql_golden_nearest_sink;

CREATE TABLE streaming_sql_golden_nearest_stream (id INT) USING parquet;
INSERT INTO streaming_sql_golden_nearest_stream VALUES (1), (2);

CREATE TABLE streaming_sql_golden_nearest_batch (id INT) USING parquet;
INSERT INTO streaming_sql_golden_nearest_batch VALUES (1), (3);

CREATE TABLE streaming_sql_golden_nearest_sink (
  stream_id INT, batch_id INT) USING parquet;

CREATE FLOW streaming_sql_golden_nearest_flow AS
INSERT INTO streaming_sql_golden_nearest_sink BY NAME
SELECT s.id AS stream_id, b.id AS batch_id
FROM STREAM(streaming_sql_golden_nearest_stream) s
JOIN streaming_sql_golden_nearest_batch b
  APPROX NEAREST 1 BY DISTANCE abs(s.id - b.id);

REFRESH FLOW streaming_sql_golden_nearest_flow;

DROP TABLE streaming_sql_golden_nearest_stream;
DROP TABLE streaming_sql_golden_nearest_batch;
DROP TABLE streaming_sql_golden_nearest_sink;
