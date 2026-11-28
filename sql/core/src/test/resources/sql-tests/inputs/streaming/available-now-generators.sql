DROP TABLE IF EXISTS streaming_sql_golden_generators_source;
DROP TABLE IF EXISTS streaming_sql_golden_generators_sink;

CREATE TABLE streaming_sql_golden_generators_source (
  id INT, items ARRAY<INT>) USING parquet;
INSERT INTO streaming_sql_golden_generators_source VALUES
  (1, ARRAY(10, 20)),
  (2, ARRAY(30)),
  (3, ARRAY()),
  (4, NULL);

CREATE TABLE streaming_sql_golden_generators_sink (
  id INT, position INT, item INT) USING parquet;

CREATE FLOW streaming_sql_golden_generators_flow AS
INSERT INTO streaming_sql_golden_generators_sink BY NAME
SELECT id, position, item
FROM STREAM(streaming_sql_golden_generators_source)
LATERAL VIEW OUTER POSEXPLODE(items) AS position, item;

REFRESH FLOW streaming_sql_golden_generators_flow;

SELECT * FROM streaming_sql_golden_generators_sink ORDER BY id, position;

WITH batch_output AS (
  SELECT id, position, item
  FROM streaming_sql_golden_generators_source
  LATERAL VIEW OUTER POSEXPLODE(items) AS position, item
),
streaming_output AS (
  SELECT id, position, item FROM streaming_sql_golden_generators_sink
)
SELECT
  NOT EXISTS (SELECT * FROM batch_output EXCEPT ALL SELECT * FROM streaming_output)
    AND NOT EXISTS (SELECT * FROM streaming_output EXCEPT ALL SELECT * FROM batch_output)
    AS complete_output_identical;

DROP TABLE streaming_sql_golden_generators_source;
DROP TABLE streaming_sql_golden_generators_sink;
