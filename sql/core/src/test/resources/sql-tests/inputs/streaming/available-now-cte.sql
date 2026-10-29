DROP TABLE IF EXISTS streaming_sql_golden_cte_source;
DROP TABLE IF EXISTS streaming_sql_golden_cte_sink;

CREATE TABLE streaming_sql_golden_cte_source (value INT) USING parquet;
INSERT INTO streaming_sql_golden_cte_source VALUES (1), (2), (3);

CREATE TABLE streaming_sql_golden_cte_sink (value INT) USING parquet;

CREATE FLOW streaming_sql_golden_cte_flow AS
INSERT INTO streaming_sql_golden_cte_sink BY NAME
WITH streaming_values AS (
  SELECT value FROM STREAM(streaming_sql_golden_cte_source)
)
SELECT value FROM streaming_values;

REFRESH FLOW streaming_sql_golden_cte_flow;

SELECT * FROM streaming_sql_golden_cte_sink ORDER BY value;

DROP TABLE streaming_sql_golden_cte_source;
DROP TABLE streaming_sql_golden_cte_sink;
