DROP TABLE IF EXISTS streaming_sql_golden_subquery_stream;
DROP TABLE IF EXISTS streaming_sql_golden_subquery_batch;
DROP TABLE IF EXISTS streaming_sql_golden_subquery_sink;

CREATE TABLE streaming_sql_golden_subquery_stream (
  id INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_subquery_stream VALUES
  (1, TIMESTAMP '2024-01-01 00:00:01'),
  (2, TIMESTAMP '2024-01-01 00:00:02');

CREATE TABLE streaming_sql_golden_subquery_batch (id INT) USING parquet;
INSERT INTO streaming_sql_golden_subquery_batch VALUES (1), (3);

CREATE TABLE streaming_sql_golden_subquery_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_subquery_flow AS
INSERT INTO streaming_sql_golden_subquery_sink BY NAME
SELECT b.id
FROM streaming_sql_golden_subquery_batch b
WHERE EXISTS (
  SELECT 1
  FROM STREAM(streaming_sql_golden_subquery_stream)
    WATERMARK event_time DELAY OF INTERVAL 1 SECOND s
  WHERE s.id = b.id);

REFRESH FLOW streaming_sql_golden_subquery_flow;

DROP TABLE streaming_sql_golden_subquery_stream;
DROP TABLE streaming_sql_golden_subquery_batch;
DROP TABLE streaming_sql_golden_subquery_sink;
