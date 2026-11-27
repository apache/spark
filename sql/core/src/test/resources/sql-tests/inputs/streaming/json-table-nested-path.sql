-- JSON_TABLE with NESTED PATH over streaming input: each input row expands into its nested rows,
-- and a row whose nested path matches nothing is kept with NULL nested columns.
DROP TABLE IF EXISTS streaming_sql_golden_json_table_source;
DROP TABLE IF EXISTS streaming_sql_golden_json_table_sink;

CREATE TABLE streaming_sql_golden_json_table_source (doc STRING) USING parquet;
INSERT INTO streaming_sql_golden_json_table_source VALUES
  ('{"id":1,"items":[{"sku":"a","qty":2},{"sku":"b","qty":1}]}'),
  ('{"id":2,"items":[]}');

CREATE TABLE streaming_sql_golden_json_table_sink (
  order_id BIGINT, n BIGINT, sku STRING, qty INT) USING parquet;

CREATE FLOW streaming_sql_golden_json_table_flow AS
INSERT INTO streaming_sql_golden_json_table_sink BY NAME
SELECT t.*
FROM STREAM(streaming_sql_golden_json_table_source) AS s,
LATERAL JSON_TABLE(s.doc, '$' COLUMNS (
  order_id BIGINT PATH '$.id',
  NESTED PATH '$.items[*]' COLUMNS (n FOR ORDINALITY, sku STRING, qty INT))) AS t;

REFRESH FLOW streaming_sql_golden_json_table_flow;

SELECT * FROM streaming_sql_golden_json_table_sink ORDER BY order_id, n;

INSERT INTO streaming_sql_golden_json_table_source VALUES
  ('{"id":3,"items":[{"sku":"c","qty":5}]}');

REFRESH FLOW streaming_sql_golden_json_table_flow;

SELECT * FROM streaming_sql_golden_json_table_sink ORDER BY order_id, n;

DROP TABLE streaming_sql_golden_json_table_source;
DROP TABLE streaming_sql_golden_json_table_sink;
