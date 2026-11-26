DROP TABLE IF EXISTS streaming_sql_golden_time_bucket_source;
DROP TABLE IF EXISTS streaming_sql_golden_time_bucket_sink;
DROP TABLE IF EXISTS streaming_sql_golden_time_bucket_agg_sink;

CREATE TABLE streaming_sql_golden_time_bucket_source (
  id INT, event_time TIMESTAMP, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_time_bucket_source VALUES
  (1, TIMESTAMP '2024-01-15 10:23:00', 2),
  (2, TIMESTAMP '2024-01-15 10:44:59', 3),
  (3, TIMESTAMP '2024-03-01 00:00:00', 7),
  (4, NULL, NULL);

CREATE TABLE streaming_sql_golden_time_bucket_sink (
  id INT, bucket_15m TIMESTAMP, bucket_month TIMESTAMP, bucket_origin TIMESTAMP) USING parquet;

CREATE FLOW streaming_sql_golden_time_bucket_flow AS
INSERT INTO streaming_sql_golden_time_bucket_sink BY NAME
SELECT id,
  time_bucket(INTERVAL '15' MINUTE, event_time) AS bucket_15m,
  time_bucket(INTERVAL '1' MONTH, event_time) AS bucket_month,
  time_bucket(INTERVAL '1' HOUR, event_time, TIMESTAMP '2024-01-01 00:30:00') AS bucket_origin
FROM STREAM(streaming_sql_golden_time_bucket_source);

REFRESH FLOW streaming_sql_golden_time_bucket_flow;

SELECT * FROM streaming_sql_golden_time_bucket_sink ORDER BY id;

-- An append mode aggregation needs a grouping key that carries the watermark. The time_bucket
-- result does not carry it, so the flow fails when it starts.
CREATE TABLE streaming_sql_golden_time_bucket_agg_sink (
  bucket TIMESTAMP, total BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_time_bucket_agg_flow AS
INSERT INTO streaming_sql_golden_time_bucket_agg_sink BY NAME
SELECT time_bucket(INTERVAL '15' MINUTE, event_time) AS bucket, sum(amount) AS total
FROM STREAM(streaming_sql_golden_time_bucket_source)
  WATERMARK event_time DELAY OF INTERVAL 10 SECONDS
GROUP BY time_bucket(INTERVAL '15' MINUTE, event_time);

REFRESH FLOW streaming_sql_golden_time_bucket_agg_flow;

SELECT * FROM streaming_sql_golden_time_bucket_agg_sink ORDER BY bucket;

DROP TABLE streaming_sql_golden_time_bucket_source;
DROP TABLE streaming_sql_golden_time_bucket_sink;
DROP TABLE streaming_sql_golden_time_bucket_agg_sink;
