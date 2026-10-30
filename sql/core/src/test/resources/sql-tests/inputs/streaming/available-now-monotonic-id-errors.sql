DROP TABLE IF EXISTS streaming_sql_golden_monotonic_id_source;
DROP TABLE IF EXISTS streaming_sql_golden_monotonic_id_sink;

CREATE TABLE streaming_sql_golden_monotonic_id_source (id INT) USING parquet;
INSERT INTO streaming_sql_golden_monotonic_id_source VALUES (1), (2);

CREATE TABLE streaming_sql_golden_monotonic_id_sink (
  id INT, generated_id BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_monotonic_id_flow AS
INSERT INTO streaming_sql_golden_monotonic_id_sink BY NAME
SELECT id, monotonically_increasing_id() AS generated_id
FROM STREAM(streaming_sql_golden_monotonic_id_source);

REFRESH FLOW streaming_sql_golden_monotonic_id_flow;

DROP TABLE streaming_sql_golden_monotonic_id_source;
DROP TABLE streaming_sql_golden_monotonic_id_sink;
