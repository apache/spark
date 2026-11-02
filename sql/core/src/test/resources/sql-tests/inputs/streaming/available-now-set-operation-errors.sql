DROP TABLE IF EXISTS streaming_sql_golden_set_stream;
DROP TABLE IF EXISTS streaming_sql_golden_set_batch;
DROP TABLE IF EXISTS streaming_sql_golden_mixed_union_sink;
DROP TABLE IF EXISTS streaming_sql_golden_intersect_sink;
DROP TABLE IF EXISTS streaming_sql_golden_except_sink;

CREATE TABLE streaming_sql_golden_set_stream (
  id INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_set_stream VALUES
  (1, TIMESTAMP '2024-01-01 00:00:01'),
  (2, TIMESTAMP '2024-01-01 00:00:02');

CREATE TABLE streaming_sql_golden_set_batch (id INT) USING parquet;
INSERT INTO streaming_sql_golden_set_batch VALUES (1), (3);

CREATE TABLE streaming_sql_golden_mixed_union_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_mixed_union_flow AS
INSERT INTO streaming_sql_golden_mixed_union_sink BY NAME
SELECT id FROM STREAM(streaming_sql_golden_set_stream)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
UNION ALL
SELECT id FROM streaming_sql_golden_set_batch;

REFRESH FLOW streaming_sql_golden_mixed_union_flow;

CREATE TABLE streaming_sql_golden_intersect_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_intersect_flow AS
INSERT INTO streaming_sql_golden_intersect_sink BY NAME
SELECT id FROM STREAM(streaming_sql_golden_set_stream)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
INTERSECT
SELECT id FROM streaming_sql_golden_set_batch;

REFRESH FLOW streaming_sql_golden_intersect_flow;

CREATE TABLE streaming_sql_golden_except_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_except_flow AS
INSERT INTO streaming_sql_golden_except_sink BY NAME
SELECT id FROM STREAM(streaming_sql_golden_set_stream)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
EXCEPT
SELECT id FROM streaming_sql_golden_set_batch;

REFRESH FLOW streaming_sql_golden_except_flow;

DROP TABLE streaming_sql_golden_set_stream;
DROP TABLE streaming_sql_golden_set_batch;
DROP TABLE streaming_sql_golden_mixed_union_sink;
DROP TABLE streaming_sql_golden_intersect_sink;
DROP TABLE streaming_sql_golden_except_sink;
