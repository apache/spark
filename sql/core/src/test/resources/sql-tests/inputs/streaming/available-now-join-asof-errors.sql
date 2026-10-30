--SET spark.sql.join.asofJoin.enabled=true

DROP TABLE IF EXISTS streaming_sql_golden_asof_stream;
DROP TABLE IF EXISTS streaming_sql_golden_asof_batch;
DROP TABLE IF EXISTS streaming_sql_golden_asof_sink;

CREATE TABLE streaming_sql_golden_asof_stream (
  id INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_asof_stream VALUES
  (1, TIMESTAMP '2024-01-01 00:00:01'),
  (2, TIMESTAMP '2024-01-01 00:00:02');

CREATE TABLE streaming_sql_golden_asof_batch (
  id INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_asof_batch VALUES
  (1, TIMESTAMP '2024-01-01 00:00:00'),
  (3, TIMESTAMP '2024-01-01 00:00:03');

CREATE TABLE streaming_sql_golden_asof_sink (
  batch_id INT, stream_id INT) USING parquet;

CREATE FLOW streaming_sql_golden_asof_flow AS
INSERT INTO streaming_sql_golden_asof_sink BY NAME
SELECT b.id AS batch_id, s.id AS stream_id
FROM streaming_sql_golden_asof_batch b
INNER ASOF JOIN STREAM(streaming_sql_golden_asof_stream) s
  MATCH_CONDITION (b.event_time >= s.event_time)
  ON b.id = s.id;

REFRESH FLOW streaming_sql_golden_asof_flow;

DROP TABLE streaming_sql_golden_asof_stream;
DROP TABLE streaming_sql_golden_asof_batch;
DROP TABLE streaming_sql_golden_asof_sink;
