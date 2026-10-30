DROP TABLE IF EXISTS streaming_sql_golden_sample_source;
DROP TABLE IF EXISTS streaming_sql_golden_sample_sink;

CREATE TABLE streaming_sql_golden_sample_source (id INT) USING parquet;
INSERT INTO streaming_sql_golden_sample_source VALUES (1), (2);

CREATE TABLE streaming_sql_golden_sample_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_sample_flow AS
INSERT INTO streaming_sql_golden_sample_sink BY NAME
SELECT id
FROM (
  SELECT id FROM STREAM(streaming_sql_golden_sample_source)
) TABLESAMPLE (50 PERCENT);

REFRESH FLOW streaming_sql_golden_sample_flow;

DROP TABLE streaming_sql_golden_sample_source;
DROP TABLE streaming_sql_golden_sample_sink;
