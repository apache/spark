DROP TABLE IF EXISTS streaming_sql_golden_unpivot_source;
DROP TABLE IF EXISTS streaming_sql_golden_unpivot_sink;

CREATE TABLE streaming_sql_golden_unpivot_source (
  id INT, jan INT, feb INT, mar INT) USING parquet;
INSERT INTO streaming_sql_golden_unpivot_source VALUES
  (1, 10, 20, 30),
  (2, 40, NULL, 60);

CREATE TABLE streaming_sql_golden_unpivot_sink (
  id INT, month STRING, sales INT) USING parquet;

CREATE FLOW streaming_sql_golden_unpivot_flow AS
INSERT INTO streaming_sql_golden_unpivot_sink BY NAME
SELECT id, month, sales
FROM STREAM(streaming_sql_golden_unpivot_source)
UNPIVOT INCLUDE NULLS (
  sales FOR month IN (jan AS january, feb AS february, mar AS march)
);

REFRESH FLOW streaming_sql_golden_unpivot_flow;

SELECT * FROM streaming_sql_golden_unpivot_sink ORDER BY id, month;

WITH batch_output AS (
  SELECT id, month, sales
  FROM streaming_sql_golden_unpivot_source
  UNPIVOT INCLUDE NULLS (
    sales FOR month IN (jan AS january, feb AS february, mar AS march)
  )
),
streaming_output AS (
  SELECT id, month, sales FROM streaming_sql_golden_unpivot_sink
)
SELECT
  NOT EXISTS (SELECT * FROM batch_output EXCEPT ALL SELECT * FROM streaming_output)
    AND NOT EXISTS (SELECT * FROM streaming_output EXCEPT ALL SELECT * FROM batch_output)
    AS complete_output_identical;

DROP TABLE streaming_sql_golden_unpivot_source;
DROP TABLE streaming_sql_golden_unpivot_sink;
