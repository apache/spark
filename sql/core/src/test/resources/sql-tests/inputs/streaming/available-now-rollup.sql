DROP TABLE IF EXISTS streaming_sql_golden_rollup_source;
DROP TABLE IF EXISTS streaming_sql_golden_rollup_sink;
DROP TABLE IF EXISTS streaming_sql_golden_rollup_two_columns_sink;

CREATE TABLE streaming_sql_golden_rollup_source (
  course STRING, year INT, earnings INT, event_time TIMESTAMP) USING parquet;
-- Match the complete courseSales business dataset in group-analytics.sql. The zero-valued final
-- row advances the watermark in a later event-time group that is not emitted. Thus, all five
-- business input rows and all seven visible two-column ROLLUP output rows match the batch test.
INSERT INTO streaming_sql_golden_rollup_source VALUES
  ('dotNET', 2012, 10000, TIMESTAMP '2024-01-01 00:00:01'),
  ('Java', 2012, 20000, TIMESTAMP '2024-01-01 00:00:01'),
  ('dotNET', 2012, 5000, TIMESTAMP '2024-01-01 00:00:01'),
  ('dotNET', 2013, 48000, TIMESTAMP '2024-01-01 00:00:01'),
  ('Java', 2013, 30000, TIMESTAMP '2024-01-01 00:00:01'),
  (NULL, NULL, 0, TIMESTAMP '2024-01-01 00:00:10');

CREATE TABLE streaming_sql_golden_rollup_sink (
  course STRING, total BIGINT) USING parquet;
CREATE TABLE streaming_sql_golden_rollup_two_columns_sink (
  course STRING, year INT, total BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_rollup_flow AS
INSERT INTO streaming_sql_golden_rollup_sink BY NAME
SELECT course, SUM(earnings) AS total
FROM STREAM(streaming_sql_golden_rollup_source)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
GROUP BY event_time, ROLLUP(course);

CREATE FLOW streaming_sql_golden_rollup_two_columns_flow AS
INSERT INTO streaming_sql_golden_rollup_two_columns_sink BY NAME
SELECT course, year, SUM(earnings) AS total
FROM STREAM(streaming_sql_golden_rollup_source)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
GROUP BY event_time, ROLLUP(course, year);

REFRESH FLOW streaming_sql_golden_rollup_flow;
SELECT * FROM streaming_sql_golden_rollup_sink ORDER BY course;
REFRESH FLOW streaming_sql_golden_rollup_two_columns_flow;
SELECT * FROM streaming_sql_golden_rollup_two_columns_sink
ORDER BY course, year;

DROP TABLE streaming_sql_golden_rollup_source;
DROP TABLE streaming_sql_golden_rollup_sink;
DROP TABLE streaming_sql_golden_rollup_two_columns_sink;
