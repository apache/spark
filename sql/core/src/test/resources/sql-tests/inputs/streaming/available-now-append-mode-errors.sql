DROP TABLE IF EXISTS streaming_sql_golden_union_error_left;
DROP TABLE IF EXISTS streaming_sql_golden_union_error_right;
DROP TABLE IF EXISTS streaming_sql_golden_union_error_sink;
DROP TABLE IF EXISTS streaming_sql_golden_distinct_error_source;
DROP TABLE IF EXISTS streaming_sql_golden_distinct_error_sink;

CREATE TABLE streaming_sql_golden_union_error_left (
  value INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_union_error_left VALUES
  (1, TIMESTAMP '2024-01-01 00:00:01'),
  (2, TIMESTAMP '2024-01-01 00:00:02');
CREATE TABLE streaming_sql_golden_union_error_right (
  value INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_union_error_right VALUES
  (2, TIMESTAMP '2024-01-01 00:00:02'),
  (3, TIMESTAMP '2024-01-01 00:00:03');
CREATE TABLE streaming_sql_golden_union_error_sink (value INT) USING parquet;

-- Projecting out event_time also drops the watermark, so UNION DISTINCT remains unbounded.
CREATE FLOW streaming_sql_golden_union_error_flow AS
INSERT INTO streaming_sql_golden_union_error_sink BY NAME
SELECT value FROM STREAM(streaming_sql_golden_union_error_left)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
UNION
SELECT value FROM STREAM(streaming_sql_golden_union_error_right)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND;

REFRESH FLOW streaming_sql_golden_union_error_flow;

CREATE TABLE streaming_sql_golden_distinct_error_source (
  value INT, event_time TIMESTAMP) USING parquet;
INSERT INTO streaming_sql_golden_distinct_error_source VALUES
  (1, TIMESTAMP '2024-01-01 00:00:01'),
  (1, TIMESTAMP '2024-01-01 00:00:02'),
  (2, TIMESTAMP '2024-01-01 00:00:03');
CREATE TABLE streaming_sql_golden_distinct_error_sink (value INT) USING parquet;

-- The watermark cannot bound DISTINCT because event_time is not one of its keys.
CREATE FLOW streaming_sql_golden_distinct_error_flow AS
INSERT INTO streaming_sql_golden_distinct_error_sink BY NAME
SELECT DISTINCT value
FROM STREAM(streaming_sql_golden_distinct_error_source)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND;

REFRESH FLOW streaming_sql_golden_distinct_error_flow;

DROP TABLE streaming_sql_golden_union_error_left;
DROP TABLE streaming_sql_golden_union_error_right;
DROP TABLE streaming_sql_golden_union_error_sink;
DROP TABLE streaming_sql_golden_distinct_error_source;
DROP TABLE streaming_sql_golden_distinct_error_sink;
