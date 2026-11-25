DROP TABLE IF EXISTS streaming_sql_golden_try_abs_negative_source;
DROP TABLE IF EXISTS streaming_sql_golden_try_abs_negative_sink;

CREATE TABLE streaming_sql_golden_try_abs_negative_source (
  id INT, i INT, l BIGINT) USING parquet;
INSERT INTO streaming_sql_golden_try_abs_negative_source VALUES
  (1, -2147483648, -9223372036854775808L),
  (2, -1, 1L),
  (3, 2147483647, 9223372036854775807L),
  (4, NULL, NULL);

CREATE TABLE streaming_sql_golden_try_abs_negative_sink (
  id INT, abs_i INT, abs_l BIGINT, neg_i INT, neg_l BIGINT) USING parquet;

CREATE FLOW streaming_sql_golden_try_abs_negative_flow AS
INSERT INTO streaming_sql_golden_try_abs_negative_sink BY NAME
SELECT id, try_abs(i) AS abs_i, try_abs(l) AS abs_l,
  try_negative(i) AS neg_i, try_negative(l) AS neg_l
FROM STREAM(streaming_sql_golden_try_abs_negative_source);

REFRESH FLOW streaming_sql_golden_try_abs_negative_flow;

SELECT * FROM streaming_sql_golden_try_abs_negative_sink ORDER BY id;

DROP TABLE streaming_sql_golden_try_abs_negative_source;
DROP TABLE streaming_sql_golden_try_abs_negative_sink;
