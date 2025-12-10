--SET spark.sql.binaryOutputStyle=HEX

-- Create temporary views
CREATE TEMP VIEW df AS
SELECT * FROM (VALUES ('a', 'b'), ('a', 'c'), ('b', 'c'), ('b', 'd'), (NULL, NULL));

CREATE TEMP VIEW df2 AS
SELECT * FROM (VALUES (1, true), (2, false), (3, false));

-- Test cases for listagg function
WITH t(col) AS (SELECT listagg(col2) FROM df GROUP BY col1) SELECT len(col), regexp_count(col, 'a'), regexp_count(col, 'b'), regexp_count(col, 'c'), regexp_count(col, 'd') FROM t;
WITH t(col) AS (SELECT string_agg(col2) FROM df GROUP BY col1) SELECT len(col), regexp_count(col, 'a'), regexp_count(col, 'b'), regexp_count(col, 'c'), regexp_count(col, 'd') FROM t;
WITH t(col) AS (SELECT listagg(col2, NULL) FROM df GROUP BY col1) SELECT len(col), regexp_count(col, 'a'), regexp_count(col, 'b'), regexp_count(col, 'c'), regexp_count(col, 'd') FROM t;
SELECT listagg(col2) FROM df WHERE 1 != 1;
WITH t(col) AS (SELECT listagg(col2, '|') FROM df GROUP BY col1) SELECT len(col), regexp_count(col, 'a'), regexp_count(col, 'b'), regexp_count(col, 'c'), regexp_count(col, 'd') FROM t;
WITH t(col) AS (SELECT listagg(col1) FROM df) SELECT len(col), regexp_count(col, 'a'), regexp_count(col, 'b') FROM t;
WITH t(col) AS (SELECT listagg(DISTINCT col1) FROM df) SELECT len(col), regexp_count(col, 'a'), regexp_count(col, 'b') FROM t;
SELECT listagg(col1) WITHIN GROUP (ORDER BY col1) FROM df;
SELECT listagg(col1) WITHIN GROUP (ORDER BY col1 DESC) FROM df;
SELECT listagg(col1) WITHIN GROUP (ORDER BY col1 DESC) OVER (PARTITION BY col2) FROM df;
SELECT listagg(col1) WITHIN GROUP (ORDER BY col2) FROM df;
WITH t(col) AS (SELECT listagg(col1) WITHIN GROUP (ORDER BY col2 DESC) FROM df) SELECT (col == 'baba') || (col == 'bbaa') FROM t;
WITH t(col) AS (SELECT listagg(col1, '|') WITHIN GROUP (ORDER BY col2 DESC) FROM df) SELECT (col == 'b|a|b|a') || (col == 'b|b|a|a') FROM t;
SELECT listagg(col1, '|') WITHIN GROUP (ORDER BY col2 DESC) FROM df;
SELECT listagg(col1) WITHIN GROUP (ORDER BY col2 DESC, col1 ASC) FROM df;
SELECT listagg(col1) WITHIN GROUP (ORDER BY col2 DESC, col1 DESC) FROM df;
-- Use valid UTF-8 hex values: X'41' = 'A', X'42' = 'B', X'43' = 'C'
-- These tests verify listagg with binary data when default validation is enabled
-- Related: SPARK-54586
WITH t(col) AS (SELECT listagg(col1) FROM (VALUES (X'41'), (X'42'))) SELECT len(col), regexp_count(col, X'41'), regexp_count(col, X'42') FROM t;
WITH t(col) AS (SELECT listagg(col1, NULL) FROM (VALUES (X'41'), (X'42'))) SELECT len(col), regexp_count(col, X'41'), regexp_count(col, X'42') FROM t;
WITH t(col) AS (SELECT listagg(col1, X'43') FROM (VALUES (X'41'), (X'42'))) SELECT len(col), regexp_count(col, X'43'), regexp_count(col, X'41'), regexp_count(col, X'42') FROM t;

-- Test with invalid UTF-8 binary values (legacy behavior)
-- These tests verify listagg can handle arbitrary binary data including invalid UTF-8
-- Related: SPARK-54586
SET spark.sql.castBinaryToString.validateUtf8=false;
WITH t(col) AS (SELECT listagg(col1) FROM (VALUES (X'DEAD'), (X'BEEF'))) SELECT len(col), regexp_count(col, X'DEAD'), regexp_count(col, X'BEEF') FROM t;
WITH t(col) AS (SELECT listagg(col1, NULL) FROM (VALUES (X'DEAD'), (X'BEEF'))) SELECT len(col), regexp_count(col, X'DEAD'), regexp_count(col, X'BEEF') FROM t;
WITH t(col) AS (SELECT listagg(col1, X'42') FROM (VALUES (X'DEAD'), (X'BEEF'))) SELECT len(col), regexp_count(col, X'42'), regexp_count(col, X'DEAD'), regexp_count(col, X'BEEF') FROM t;
SET spark.sql.castBinaryToString.validateUtf8=true;

WITH t(col1, col2) AS (SELECT listagg(col1), listagg(col2, ',') FROM df2) SELECT len(col1), regexp_count(col1, '1'), regexp_count(col1, '2'), regexp_count(col1, '3'), len(col2), regexp_count(col2, 'true'), regexp_count(col1, 'false') FROM t;

-- Error cases
SELECT listagg(c1) FROM (VALUES (ARRAY('a', 'b'))) AS t(c1);
-- Test error case with valid UTF-8 (SPARK-54586)
SELECT listagg(c1, ', ') FROM (VALUES (X'41'), (X'42')) AS t(c1);
-- Test error case with invalid UTF-8 (legacy behavior, SPARK-54586)
SET spark.sql.castBinaryToString.validateUtf8=false;
SELECT listagg(c1, ', ') FROM (VALUES (X'DEAD'), (X'BEEF')) AS t(c1);
SET spark.sql.castBinaryToString.validateUtf8=true;
SELECT listagg(col2, col1) FROM df GROUP BY col1;
SELECT listagg(col1) OVER (ORDER BY col1) FROM df;
SELECT listagg(col1) WITHIN GROUP (ORDER BY col1) OVER (ORDER BY col1) FROM df;
SELECT string_agg(col1) WITHIN GROUP (ORDER BY col1) OVER (ORDER BY col1) FROM df;
SELECT listagg(DISTINCT col1) OVER (ORDER BY col1) FROM df;
SELECT listagg(DISTINCT col1) WITHIN GROUP (ORDER BY col2) FROM df;
SELECT listagg(DISTINCT col1) WITHIN GROUP (ORDER BY col1, col2) FROM df;
