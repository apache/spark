DROP TABLE IF EXISTS streaming_sql_golden_union_left;
DROP TABLE IF EXISTS streaming_sql_golden_union_right;
DROP TABLE IF EXISTS streaming_sql_golden_union_sink;

CREATE TABLE streaming_sql_golden_union_left (id INT, value STRING) USING parquet;
INSERT INTO streaming_sql_golden_union_left VALUES (1, 'alpha'), (2, 'beta');

CREATE TABLE streaming_sql_golden_union_right (id INT, value STRING) USING parquet;
INSERT INTO streaming_sql_golden_union_right VALUES (3, 'gamma'), (4, 'delta');

CREATE TABLE streaming_sql_golden_union_sink (
  id INT, origin STRING, value STRING) USING parquet;

CREATE FLOW streaming_sql_golden_union_flow AS
INSERT INTO streaming_sql_golden_union_sink BY NAME
WITH normalized AS (
  SELECT id, 'left' AS origin, upper(value) AS value
  FROM STREAM(streaming_sql_golden_union_left)
  UNION ALL
  SELECT id, 'right' AS origin, upper(value) AS value
  FROM STREAM(streaming_sql_golden_union_right)
)
SELECT id, origin, value FROM normalized;

REFRESH FLOW streaming_sql_golden_union_flow;

SELECT * FROM streaming_sql_golden_union_sink ORDER BY id;

DROP TABLE streaming_sql_golden_union_left;
DROP TABLE streaming_sql_golden_union_right;
DROP TABLE streaming_sql_golden_union_sink;
