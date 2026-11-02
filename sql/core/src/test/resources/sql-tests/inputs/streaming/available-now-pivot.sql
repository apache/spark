DROP TABLE IF EXISTS streaming_sql_golden_pivot_source;
DROP TABLE IF EXISTS streaming_sql_golden_pivot_sink;

CREATE TABLE streaming_sql_golden_pivot_source (
  event_time TIMESTAMP, category STRING, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_pivot_source VALUES
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 2),
  (TIMESTAMP '2024-01-01 00:00:01', 'games', 3),
  (TIMESTAMP '2024-01-01 00:00:02', 'books', 5),
  (TIMESTAMP '2024-01-01 00:00:10', 'watermark', 0);

CREATE TABLE streaming_sql_golden_pivot_sink (
  event_time TIMESTAMP, books BIGINT, games BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_pivot_flow AS
INSERT INTO streaming_sql_golden_pivot_sink BY NAME
SELECT event_time, books, games
FROM (
  SELECT event_time, category, amount
  FROM STREAM(streaming_sql_golden_pivot_source)
    WATERMARK event_time DELAY OF INTERVAL 1 SECOND
)
PIVOT (
  SUM(amount) FOR category IN ('books' AS books, 'games' AS games)
);

REFRESH FLOW streaming_sql_golden_pivot_flow;

SELECT * FROM streaming_sql_golden_pivot_sink ORDER BY event_time;

DROP TABLE streaming_sql_golden_pivot_source;
DROP TABLE streaming_sql_golden_pivot_sink;
