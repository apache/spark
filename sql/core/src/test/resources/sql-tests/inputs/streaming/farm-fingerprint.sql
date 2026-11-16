DROP TABLE IF EXISTS streaming_sql_golden_farm_fingerprint_source;
DROP TABLE IF EXISTS streaming_sql_golden_farm_fingerprint_sink;

CREATE TABLE streaming_sql_golden_farm_fingerprint_source (
  id INT, s STRING, b BINARY) USING parquet;
INSERT INTO streaming_sql_golden_farm_fingerprint_source VALUES
  (1, 'Spark', X'010203040506'),
  (2, '', X''),
  (3, NULL, NULL);

CREATE TABLE streaming_sql_golden_farm_fingerprint_sink (
  id INT, fp_s BIGINT, fp_b BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_farm_fingerprint_flow AS
INSERT INTO streaming_sql_golden_farm_fingerprint_sink BY NAME
SELECT id, farm_fingerprint(s) AS fp_s, farm_fingerprint(b) AS fp_b
FROM STREAM(streaming_sql_golden_farm_fingerprint_source);

REFRESH FLOW streaming_sql_golden_farm_fingerprint_flow;

SELECT * FROM streaming_sql_golden_farm_fingerprint_sink ORDER BY id;

DROP TABLE streaming_sql_golden_farm_fingerprint_source;
DROP TABLE streaming_sql_golden_farm_fingerprint_sink;
