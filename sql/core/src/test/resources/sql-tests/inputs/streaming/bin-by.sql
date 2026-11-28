-- BIN BY in a streaming flow. The appended bin columns do not carry the event-time watermark, so
-- an append-mode aggregation also needs a watermarked column in its grouping key.

--SET spark.sql.binByRelationOperator.enabled=true

DROP TABLE IF EXISTS streaming_sql_golden_bin_by_source;
DROP TABLE IF EXISTS streaming_sql_golden_bin_by_sink;
DROP TABLE IF EXISTS streaming_sql_golden_bin_by_bin_agg_sink;
DROP TABLE IF EXISTS streaming_sql_golden_bin_by_range_agg_sink;

CREATE TABLE streaming_sql_golden_bin_by_source (
  ts_start TIMESTAMP, ts_end TIMESTAMP, value DOUBLE) USING parquet;
INSERT INTO streaming_sql_golden_bin_by_source VALUES
  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:05:00', 100.0D),
  (TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:12:00', 300.0D);

-- A stateless BIN BY writes the same rows as the batch query.
CREATE TABLE streaming_sql_golden_bin_by_sink (
  ts_start TIMESTAMP,
  ts_end TIMESTAMP,
  value DOUBLE,
  bin_start TIMESTAMP,
  bin_end TIMESTAMP,
  bin_distribute_ratio DOUBLE) USING parquet;

CREATE FLOW streaming_sql_golden_bin_by_flow AS
INSERT INTO streaming_sql_golden_bin_by_sink BY NAME
SELECT * FROM STREAM(streaming_sql_golden_bin_by_source) BIN BY (
  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE DISTRIBUTE UNIFORM (value)
);

REFRESH FLOW streaming_sql_golden_bin_by_flow;

SELECT * FROM streaming_sql_golden_bin_by_sink ORDER BY ts_start, bin_start;

SELECT * FROM streaming_sql_golden_bin_by_source BIN BY (
  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE DISTRIBUTE UNIFORM (value)
) ORDER BY ts_start, bin_start;

-- `bin_start` is the only grouping key and it has no watermark, so the flow fails.
CREATE TABLE streaming_sql_golden_bin_by_bin_agg_sink (
  bin_start TIMESTAMP, total DOUBLE) USING parquet;

CREATE FLOW streaming_sql_golden_bin_by_bin_agg_flow AS
INSERT INTO streaming_sql_golden_bin_by_bin_agg_sink BY NAME
SELECT bin_start, sum(value) AS total
FROM STREAM(streaming_sql_golden_bin_by_source)
  WATERMARK ts_start DELAY OF INTERVAL 10 MINUTES
BIN BY (
  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE DISTRIBUTE UNIFORM (value)
)
GROUP BY bin_start;

REFRESH FLOW streaming_sql_golden_bin_by_bin_agg_flow;

-- The watermarked `ts_start` is also a grouping key, so the flow writes a group after the
-- watermark passes its `ts_start`.
CREATE TABLE streaming_sql_golden_bin_by_range_agg_sink (
  ts_start TIMESTAMP, bin_start TIMESTAMP, total DOUBLE) USING parquet;

CREATE FLOW streaming_sql_golden_bin_by_range_agg_flow AS
INSERT INTO streaming_sql_golden_bin_by_range_agg_sink BY NAME
SELECT ts_start, bin_start, sum(value) AS total
FROM STREAM(streaming_sql_golden_bin_by_source)
  WATERMARK ts_start DELAY OF INTERVAL 10 MINUTES
BIN BY (
  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE DISTRIBUTE UNIFORM (value)
)
GROUP BY ts_start, bin_start;

REFRESH FLOW streaming_sql_golden_bin_by_range_agg_flow;

SELECT * FROM streaming_sql_golden_bin_by_range_agg_sink ORDER BY ts_start, bin_start;

INSERT INTO streaming_sql_golden_bin_by_source VALUES
  (TIMESTAMP '2024-01-01 01:00:00', TIMESTAMP '2024-01-01 01:01:00', 0.0D);

REFRESH FLOW streaming_sql_golden_bin_by_range_agg_flow;

SELECT * FROM streaming_sql_golden_bin_by_range_agg_sink ORDER BY ts_start, bin_start;

DROP TABLE streaming_sql_golden_bin_by_source;
DROP TABLE streaming_sql_golden_bin_by_sink;
DROP TABLE streaming_sql_golden_bin_by_bin_agg_sink;
DROP TABLE streaming_sql_golden_bin_by_range_agg_sink;
