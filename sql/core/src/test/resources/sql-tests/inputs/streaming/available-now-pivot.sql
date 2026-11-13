DROP TABLE IF EXISTS streaming_sql_golden_pivot_source;
DROP TABLE IF EXISTS streaming_sql_golden_pivot_sink;

CREATE TABLE streaming_sql_golden_pivot_source (
  event_time TIMESTAMP, category STRING, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_pivot_source VALUES
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 2),
  (TIMESTAMP '2024-01-01 00:00:01', 'games', 3),
  (TIMESTAMP '2024-01-01 00:00:02', 'books', 5),
  (TIMESTAMP '2024-01-01 00:00:10', 'games', 7);

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

-- Exercise checkpoint restart, an on-time update to an existing group, new groups,
-- watermark advancement, and rejection of a row older than the previous watermark.
INSERT INTO streaming_sql_golden_pivot_source VALUES
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 100),
  (TIMESTAMP '2024-01-01 00:00:10', 'books', 11),
  (TIMESTAMP '2024-01-01 00:00:11', 'books', 13),
  (TIMESTAMP '2024-01-01 00:00:11', 'games', 17),
  (TIMESTAMP '2024-01-01 00:00:20', 'games', 19);

REFRESH FLOW streaming_sql_golden_pivot_flow;

SELECT * FROM streaming_sql_golden_pivot_sink ORDER BY event_time;

DROP TABLE streaming_sql_golden_pivot_source;
DROP TABLE streaming_sql_golden_pivot_sink;
