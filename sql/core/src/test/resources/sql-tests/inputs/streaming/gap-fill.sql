-- GAP FILL is not supported yet, so a streaming flow that uses it fails during analysis.
DROP TABLE IF EXISTS streaming_sql_golden_gap_fill_source;
DROP TABLE IF EXISTS streaming_sql_golden_gap_fill_sink;

CREATE TABLE streaming_sql_golden_gap_fill_source (ts TIMESTAMP_NTZ, v INT) USING parquet;
INSERT INTO streaming_sql_golden_gap_fill_source VALUES
  (TIMESTAMP_NTZ '2024-01-01 00:00:00', 1),
  (TIMESTAMP_NTZ '2024-01-01 00:10:00', 3);

CREATE TABLE streaming_sql_golden_gap_fill_sink (ts TIMESTAMP_NTZ, v INT) USING parquet;

CREATE FLOW streaming_sql_golden_gap_fill_flow AS
INSERT INTO streaming_sql_golden_gap_fill_sink BY NAME
SELECT ts, v FROM STREAM(streaming_sql_golden_gap_fill_source)
GAP FILL (ORDER BY ts BIN WIDTH INTERVAL '5' MINUTE);

REFRESH FLOW streaming_sql_golden_gap_fill_flow;

SELECT * FROM streaming_sql_golden_gap_fill_sink ORDER BY ts;

DROP TABLE streaming_sql_golden_gap_fill_source;
DROP TABLE streaming_sql_golden_gap_fill_sink;
