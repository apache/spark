SET spark.sql.gapFillRelationOperator.enabled = true;

DROP TABLE IF EXISTS streaming_sql_golden_gap_fill_source;

CREATE TABLE streaming_sql_golden_gap_fill_source (p STRING, ts TIMESTAMP_NTZ, v INT) USING parquet;
INSERT INTO streaming_sql_golden_gap_fill_source
VALUES ('p1', TIMESTAMP_NTZ '2026-01-01 00:00:00', 90), ('p1', TIMESTAMP_NTZ '2026-01-01 00:45:00', 75);

-- GAP FILL is not supported on streaming input.
SELECT * FROM STREAM(streaming_sql_golden_gap_fill_source)
GAP FILL (PARTITION BY p ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES);

DROP TABLE streaming_sql_golden_gap_fill_source;
