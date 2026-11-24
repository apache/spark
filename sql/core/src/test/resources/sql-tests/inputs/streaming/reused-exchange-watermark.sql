-- SPARK-60862: A reused exchange must preserve the watermark of its underlying child.
--SET spark.sql.exchange.reuse=true
--SET spark.sql.shuffle.partitions=1

DROP TABLE IF EXISTS reused_exchange_watermark_source;
DROP TABLE IF EXISTS reused_exchange_watermark_sink;

CREATE TABLE reused_exchange_watermark_source (
  event_time TIMESTAMP,
  value INT) USING parquet;
INSERT INTO reused_exchange_watermark_source VALUES
  (TIMESTAMP '2024-01-01 00:00:00', 0),
  (TIMESTAMP '2024-01-01 00:00:01', 1),
  (TIMESTAMP '2024-01-01 00:00:02', 2),
  (TIMESTAMP '2024-01-01 00:00:03', 3),
  (TIMESTAMP '2024-01-01 00:00:04', 4);

CREATE TABLE reused_exchange_watermark_sink (
  branch STRING,
  window_start TIMESTAMP,
  key INT,
  event_count BIGINT) USING parquet;

CREATE FLOW reused_exchange_watermark_flow AS
INSERT INTO reused_exchange_watermark_sink BY NAME
WITH aggregated AS (
  SELECT
    window(event_time, '5 seconds') AS window,
    value % 2 AS key,
    count(*) AS event_count
  FROM STREAM(reused_exchange_watermark_source)
    WATERMARK event_time DELAY OF INTERVAL 0 SECONDS
  GROUP BY window(event_time, '5 seconds'), value % 2
)
SELECT 'left' AS branch, window.start AS window_start, key, event_count
FROM aggregated
UNION ALL
SELECT 'right' AS branch, window.start AS window_start, key, event_count
FROM aggregated;

REFRESH FLOW reused_exchange_watermark_flow;

SELECT * FROM reused_exchange_watermark_sink ORDER BY branch, key;

INSERT INTO reused_exchange_watermark_source VALUES
  (TIMESTAMP '2024-01-01 00:00:10', 10);

REFRESH FLOW reused_exchange_watermark_flow;

SELECT * FROM reused_exchange_watermark_sink ORDER BY branch, key;

DROP TABLE reused_exchange_watermark_source;
DROP TABLE reused_exchange_watermark_sink;
