-- A streaming flow that replaces one column and appends another with |> EVAL.
DROP TABLE IF EXISTS streaming_sql_golden_pipe_eval_source;
DROP TABLE IF EXISTS streaming_sql_golden_pipe_eval_sink;

CREATE TABLE streaming_sql_golden_pipe_eval_source (id INT, uri STRING) USING parquet;
INSERT INTO streaming_sql_golden_pipe_eval_source VALUES
  (1, '/API/v1/users'),
  (2, '/home');

CREATE TABLE streaming_sql_golden_pipe_eval_sink (id INT, uri STRING, is_api INT) USING parquet;

CREATE FLOW streaming_sql_golden_pipe_eval_flow AS
INSERT INTO streaming_sql_golden_pipe_eval_sink BY NAME
FROM STREAM(streaming_sql_golden_pipe_eval_source)
|> EVAL uri = lower(uri), is_api = IF(uri LIKE '/api/%', 1, 0);

REFRESH FLOW streaming_sql_golden_pipe_eval_flow;

SELECT * FROM streaming_sql_golden_pipe_eval_sink ORDER BY id;

DROP TABLE streaming_sql_golden_pipe_eval_source;
DROP TABLE streaming_sql_golden_pipe_eval_sink;
