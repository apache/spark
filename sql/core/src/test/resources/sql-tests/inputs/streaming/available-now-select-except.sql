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

WITH batch_output AS (
  SELECT * EXCEPT (internal_note)
  FROM streaming_sql_golden_select_except_source
),
streaming_output AS (
  SELECT id, category, amount FROM streaming_sql_golden_select_except_sink
)
SELECT
  NOT EXISTS (SELECT * FROM batch_output EXCEPT ALL SELECT * FROM streaming_output)
    AND NOT EXISTS (SELECT * FROM streaming_output EXCEPT ALL SELECT * FROM batch_output)
    AS complete_output_identical;

DROP TABLE streaming_sql_golden_select_except_source;
DROP TABLE streaming_sql_golden_select_except_sink;
