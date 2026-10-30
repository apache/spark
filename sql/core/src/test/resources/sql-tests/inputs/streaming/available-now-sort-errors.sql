DROP TABLE IF EXISTS streaming_sql_golden_sort_source;
DROP TABLE IF EXISTS streaming_sql_golden_sort_sink;

CREATE TABLE streaming_sql_golden_sort_source (id INT) USING parquet;
INSERT INTO streaming_sql_golden_sort_source VALUES (1), (2);

CREATE TABLE streaming_sql_golden_sort_sink (id INT) USING parquet;

CREATE FLOW streaming_sql_golden_sort_flow AS
INSERT INTO streaming_sql_golden_sort_sink BY NAME
SELECT id
FROM STREAM(streaming_sql_golden_sort_source)
ORDER BY id;

REFRESH FLOW streaming_sql_golden_sort_flow;

DROP TABLE streaming_sql_golden_sort_source;
DROP TABLE streaming_sql_golden_sort_sink;
