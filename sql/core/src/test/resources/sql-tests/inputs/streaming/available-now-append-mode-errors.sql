DROP TABLE IF EXISTS streaming_sql_golden_union_error_left;
DROP TABLE IF EXISTS streaming_sql_golden_union_error_right;
DROP TABLE IF EXISTS streaming_sql_golden_union_error_sink;
DROP TABLE IF EXISTS streaming_sql_golden_distinct_error_source;
DROP TABLE IF EXISTS streaming_sql_golden_distinct_error_sink;

CREATE TABLE streaming_sql_golden_union_error_left (value INT) USING parquet;
INSERT INTO streaming_sql_golden_union_error_left VALUES (1), (2);
CREATE TABLE streaming_sql_golden_union_error_right (value INT) USING parquet;
INSERT INTO streaming_sql_golden_union_error_right VALUES (2), (3);
CREATE TABLE streaming_sql_golden_union_error_sink (value INT) USING parquet;

CREATE FLOW streaming_sql_golden_union_error_flow AS
INSERT INTO streaming_sql_golden_union_error_sink BY NAME
SELECT value FROM STREAM(streaming_sql_golden_union_error_left)
UNION
SELECT value FROM STREAM(streaming_sql_golden_union_error_right);

REFRESH FLOW streaming_sql_golden_union_error_flow;

CREATE TABLE streaming_sql_golden_distinct_error_source (value INT) USING parquet;
INSERT INTO streaming_sql_golden_distinct_error_source VALUES (1), (1), (2);
CREATE TABLE streaming_sql_golden_distinct_error_sink (value INT) USING parquet;

CREATE FLOW streaming_sql_golden_distinct_error_flow AS
INSERT INTO streaming_sql_golden_distinct_error_sink BY NAME
SELECT DISTINCT value FROM STREAM(streaming_sql_golden_distinct_error_source);

REFRESH FLOW streaming_sql_golden_distinct_error_flow;

DROP TABLE streaming_sql_golden_union_error_left;
DROP TABLE streaming_sql_golden_union_error_right;
DROP TABLE streaming_sql_golden_union_error_sink;
DROP TABLE streaming_sql_golden_distinct_error_source;
DROP TABLE streaming_sql_golden_distinct_error_sink;
