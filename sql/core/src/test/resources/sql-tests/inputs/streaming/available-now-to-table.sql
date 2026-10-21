DROP TABLE IF EXISTS streaming_sql_golden_available_now_source;
DROP TABLE IF EXISTS streaming_sql_golden_available_now_sink;

CREATE TABLE streaming_sql_golden_available_now_source (id INT, value STRING) USING parquet;
INSERT INTO streaming_sql_golden_available_now_source
VALUES (1, 'one'), (2, 'two'), (3, 'three');

CREATE TABLE streaming_sql_golden_available_now_sink (id INT, value STRING) USING parquet;

CREATE FLOW streaming_sql_golden_available_now_flow AS
INSERT INTO streaming_sql_golden_available_now_sink BY NAME
SELECT id, upper(value) AS value
FROM STREAM(streaming_sql_golden_available_now_source)
WHERE id >= 2;

REFRESH FLOW streaming_sql_golden_available_now_flow;

SELECT * FROM streaming_sql_golden_available_now_sink ORDER BY id;

DROP TABLE streaming_sql_golden_available_now_source;
DROP TABLE streaming_sql_golden_available_now_sink;
