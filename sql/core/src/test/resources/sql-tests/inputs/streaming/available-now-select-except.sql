DROP TABLE IF EXISTS streaming_sql_golden_select_except_source;
DROP TABLE IF EXISTS streaming_sql_golden_select_except_sink;

CREATE TABLE streaming_sql_golden_select_except_source (
  id INT, category STRING, amount INT, internal_note STRING) USING parquet;
INSERT INTO streaming_sql_golden_select_except_source VALUES
  (1, 'books', 2, 'first'),
  (2, 'games', 7, 'second');

CREATE TABLE streaming_sql_golden_select_except_sink (
  id INT, category STRING, amount INT) USING parquet;

CREATE FLOW streaming_sql_golden_select_except_flow AS
INSERT INTO streaming_sql_golden_select_except_sink BY NAME
SELECT * EXCEPT (internal_note)
FROM STREAM(streaming_sql_golden_select_except_source);

REFRESH FLOW streaming_sql_golden_select_except_flow;

SELECT * FROM streaming_sql_golden_select_except_sink ORDER BY id;

DROP TABLE streaming_sql_golden_select_except_source;
DROP TABLE streaming_sql_golden_select_except_sink;
