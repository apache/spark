DROP TABLE IF EXISTS streaming_sql_golden_pipe_source;
DROP TABLE IF EXISTS streaming_sql_golden_pipe_sink;

CREATE TABLE streaming_sql_golden_pipe_source (
  id INT, category STRING, amount INT) USING parquet;
INSERT INTO streaming_sql_golden_pipe_source VALUES
  (1, 'books', 2),
  (2, 'games', 7),
  (3, 'music', 10);

CREATE TABLE streaming_sql_golden_pipe_sink (
  id INT, category STRING, doubled_amount INT) USING parquet;

CREATE FLOW streaming_sql_golden_pipe_flow AS
INSERT INTO streaming_sql_golden_pipe_sink BY NAME
FROM STREAM(streaming_sql_golden_pipe_source)
|> WHERE amount >= 5
|> EXTEND amount * 2 AS doubled_amount
|> SELECT id, upper(category) AS category, doubled_amount;

REFRESH FLOW streaming_sql_golden_pipe_flow;

SELECT * FROM streaming_sql_golden_pipe_sink ORDER BY id;

WITH batch_output AS (
  FROM streaming_sql_golden_pipe_source
  |> WHERE amount >= 5
  |> EXTEND amount * 2 AS doubled_amount
  |> SELECT id, upper(category) AS category, doubled_amount
),
streaming_output AS (
  SELECT id, category, doubled_amount FROM streaming_sql_golden_pipe_sink
)
SELECT
  NOT EXISTS (SELECT * FROM batch_output EXCEPT ALL SELECT * FROM streaming_output)
    AND NOT EXISTS (SELECT * FROM streaming_output EXCEPT ALL SELECT * FROM batch_output)
    AS complete_output_identical;

DROP TABLE streaming_sql_golden_pipe_source;
DROP TABLE streaming_sql_golden_pipe_sink;
