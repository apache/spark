DROP TABLE IF EXISTS streaming_sql_golden_window_source;
DROP TABLE IF EXISTS streaming_sql_golden_window_sink;

CREATE TABLE streaming_sql_golden_window_source (
  id INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_window_source VALUES
  (1, TIMESTAMP '2024-01-01 00:00:01'),
  (2, TIMESTAMP '2024-01-01 00:00:02');

CREATE TABLE streaming_sql_golden_window_sink (id INT, row_num INT) USING parquet;

CREATE FLOW streaming_sql_golden_window_flow AS
INSERT INTO streaming_sql_golden_window_sink BY NAME
SELECT id, ROW_NUMBER() OVER (ORDER BY event_time) AS row_num
FROM STREAM(streaming_sql_golden_window_source);

REFRESH FLOW streaming_sql_golden_window_flow;

DROP TABLE streaming_sql_golden_window_source;
DROP TABLE streaming_sql_golden_window_sink;
