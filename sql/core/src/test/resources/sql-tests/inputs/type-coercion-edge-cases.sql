CREATE TABLE t1(col1 TIMESTAMP, col2 MAP<BIGINT, DOUBLE>);

-- Preserve tags after type coercing aggregate expression children
SELECT MEAN(col1) FROM t1;

-- Type coercion is applied to recursive data types
SELECT col2.field FROM t1;

-- Apply type coercion to semi_structured_extract
SELECT NULL:a AS b;

DROP TABLE t1;
