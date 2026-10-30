DROP TABLE IF EXISTS streaming_sql_golden_outer_stream;
DROP TABLE IF EXISTS streaming_sql_golden_outer_batch;
DROP TABLE IF EXISTS streaming_sql_golden_outer_sink;

CREATE TABLE streaming_sql_golden_outer_stream (id INT) USING parquet;
INSERT INTO streaming_sql_golden_outer_stream VALUES (1), (2);

CREATE TABLE streaming_sql_golden_outer_batch (id INT) USING parquet;
INSERT INTO streaming_sql_golden_outer_batch VALUES (1), (3);

CREATE TABLE streaming_sql_golden_outer_sink (
  stream_id INT, batch_id INT) USING parquet;

CREATE FLOW streaming_sql_golden_outer_flow AS
INSERT INTO streaming_sql_golden_outer_sink BY NAME
SELECT s.id AS stream_id, b.id AS batch_id
FROM STREAM(streaming_sql_golden_outer_stream) s
FULL OUTER JOIN streaming_sql_golden_outer_batch b
  ON s.id = b.id;

REFRESH FLOW streaming_sql_golden_outer_flow;

DROP TABLE streaming_sql_golden_outer_stream;
DROP TABLE streaming_sql_golden_outer_batch;
DROP TABLE streaming_sql_golden_outer_sink;
