DROP TABLE IF EXISTS streaming_sql_golden_having_source;
DROP TABLE IF EXISTS streaming_sql_golden_having_sink;

CREATE TABLE streaming_sql_golden_having_source (
  event_time TIMESTAMP, category STRING, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_having_source VALUES
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 2),
  (TIMESTAMP '2024-01-01 00:00:01', 'books', 3),
  (TIMESTAMP '2024-01-01 00:00:02', 'games', 1),
  (TIMESTAMP '2024-01-01 00:00:10', 'watermark', 0);

CREATE TABLE streaming_sql_golden_having_sink (
  event_time TIMESTAMP, category STRING, total BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_having_flow AS
INSERT INTO streaming_sql_golden_having_sink BY NAME
SELECT event_time, category, SUM(amount) AS total
FROM STREAM(streaming_sql_golden_having_source)
  WATERMARK event_time DELAY OF INTERVAL 1 SECOND
GROUP BY event_time, category
HAVING total > 1;

REFRESH FLOW streaming_sql_golden_having_flow;

SELECT * FROM streaming_sql_golden_having_sink ORDER BY event_time, category;

WITH batch_output AS (
  SELECT event_time, category, SUM(amount) AS total
  FROM streaming_sql_golden_having_source
  GROUP BY event_time, category
  HAVING total > 1
),
streaming_output AS (
  SELECT event_time, category, total FROM streaming_sql_golden_having_sink
)
SELECT
  NOT EXISTS (SELECT * FROM batch_output EXCEPT ALL SELECT * FROM streaming_output)
    AND NOT EXISTS (SELECT * FROM streaming_output EXCEPT ALL SELECT * FROM batch_output)
    AS complete_output_identical;

DROP TABLE streaming_sql_golden_having_source;
DROP TABLE streaming_sql_golden_having_sink;
