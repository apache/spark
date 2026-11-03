DROP TABLE IF EXISTS streaming_sql_golden_function_source;
DROP TABLE IF EXISTS streaming_sql_golden_scalar_function_sink;
DROP TABLE IF EXISTS streaming_sql_golden_table_function_sink;
DROP TEMPORARY FUNCTION IF EXISTS streaming_sql_golden_double;
DROP TEMPORARY FUNCTION IF EXISTS streaming_sql_golden_expand;

CREATE TABLE streaming_sql_golden_function_source (
  id INT, value INT) USING parquet;
INSERT INTO streaming_sql_golden_function_source VALUES
  (1, 5),
  (2, 10),
  (3, 15);

CREATE TEMPORARY FUNCTION streaming_sql_golden_double(x INT)
RETURNS INT
RETURN x * 2;

CREATE TABLE streaming_sql_golden_scalar_function_sink (
  id INT, doubled_value INT) USING parquet;

CREATE FLOW streaming_sql_golden_scalar_function_flow AS
INSERT INTO streaming_sql_golden_scalar_function_sink BY NAME
SELECT id, streaming_sql_golden_double(value) AS doubled_value
FROM STREAM(streaming_sql_golden_function_source)
WHERE streaming_sql_golden_double(value) >= 20;

REFRESH FLOW streaming_sql_golden_scalar_function_flow;

SELECT * FROM streaming_sql_golden_scalar_function_sink ORDER BY id;

WITH batch_output AS (
  SELECT id, streaming_sql_golden_double(value) AS doubled_value
  FROM streaming_sql_golden_function_source
  WHERE streaming_sql_golden_double(value) >= 20
),
streaming_output AS (
  SELECT id, doubled_value FROM streaming_sql_golden_scalar_function_sink
)
SELECT
  NOT EXISTS (SELECT * FROM batch_output EXCEPT ALL SELECT * FROM streaming_output)
    AND NOT EXISTS (SELECT * FROM streaming_output EXCEPT ALL SELECT * FROM batch_output)
    AS complete_output_identical;

CREATE TEMPORARY FUNCTION streaming_sql_golden_expand(x INT)
RETURNS TABLE (expanded_value INT)
RETURN SELECT x + 1;

CREATE TABLE streaming_sql_golden_table_function_sink (
  id INT, expanded_value INT) USING parquet;

CREATE FLOW streaming_sql_golden_table_function_flow AS
INSERT INTO streaming_sql_golden_table_function_sink BY NAME
SELECT source.id, expanded.expanded_value
FROM STREAM(streaming_sql_golden_function_source) AS source,
LATERAL streaming_sql_golden_expand(source.value) AS expanded;

REFRESH FLOW streaming_sql_golden_table_function_flow;

SELECT * FROM streaming_sql_golden_table_function_sink ORDER BY id, expanded_value;

WITH batch_output AS (
  SELECT source.id, expanded.expanded_value
  FROM streaming_sql_golden_function_source AS source,
  LATERAL streaming_sql_golden_expand(source.value) AS expanded
),
streaming_output AS (
  SELECT id, expanded_value FROM streaming_sql_golden_table_function_sink
)
SELECT
  NOT EXISTS (SELECT * FROM batch_output EXCEPT ALL SELECT * FROM streaming_output)
    AND NOT EXISTS (SELECT * FROM streaming_output EXCEPT ALL SELECT * FROM batch_output)
    AS complete_output_identical;

DROP TABLE streaming_sql_golden_function_source;
DROP TABLE streaming_sql_golden_scalar_function_sink;
DROP TABLE streaming_sql_golden_table_function_sink;
DROP TEMPORARY FUNCTION streaming_sql_golden_double;
DROP TEMPORARY FUNCTION streaming_sql_golden_expand;
