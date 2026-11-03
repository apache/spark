DROP TABLE IF EXISTS streaming_sql_golden_unnest_source;
DROP TABLE IF EXISTS streaming_sql_golden_unnest_sink;

CREATE TABLE streaming_sql_golden_unnest_source (
  id INT, items ARRAY<INT>) USING parquet;
INSERT INTO streaming_sql_golden_unnest_source VALUES
  (1, ARRAY(10, 20)),
  (2, ARRAY(30)),
  (3, ARRAY()),
  (4, NULL);

CREATE TABLE streaming_sql_golden_unnest_sink (
  id INT, item INT, position BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_unnest_flow AS
INSERT INTO streaming_sql_golden_unnest_sink BY NAME
SELECT id, item, position
FROM STREAM(streaming_sql_golden_unnest_source),
LATERAL UNNEST(items) WITH ORDINALITY AS expanded(item, position);

REFRESH FLOW streaming_sql_golden_unnest_flow;

SELECT * FROM streaming_sql_golden_unnest_sink ORDER BY id, position;

WITH batch_output AS (
  SELECT id, item, position
  FROM streaming_sql_golden_unnest_source,
  LATERAL UNNEST(items) WITH ORDINALITY AS expanded(item, position)
),
streaming_output AS (
  SELECT id, item, position FROM streaming_sql_golden_unnest_sink
)
SELECT
  NOT EXISTS (SELECT * FROM batch_output EXCEPT ALL SELECT * FROM streaming_output)
    AND NOT EXISTS (SELECT * FROM streaming_output EXCEPT ALL SELECT * FROM batch_output)
    AS complete_output_identical;

DROP TABLE streaming_sql_golden_unnest_source;
DROP TABLE streaming_sql_golden_unnest_sink;
