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
WITH t(col) AS (SELECT listagg(col1) FROM (VALUES (X'DEAD'), (X'BEEF'))) SELECT len(col), regexp_count(col, X'DEAD'), regexp_count(col, X'BEEF') FROM t;
WITH t(col) AS (SELECT listagg(col1, NULL) FROM (VALUES (X'DEAD'), (X'BEEF'))) SELECT len(col), regexp_count(col, X'DEAD'), regexp_count(col, X'BEEF') FROM t;
WITH t(col) AS (SELECT listagg(col1, X'42') FROM (VALUES (X'DEAD'), (X'BEEF'))) SELECT len(col), regexp_count(col, X'42'), regexp_count(col, X'DEAD'), regexp_count(col, X'BEEF') FROM t;
WITH t(col1, col2) AS (SELECT listagg(col1), listagg(col2, ',') FROM df2) SELECT len(col1), regexp_count(col1, '1'), regexp_count(col1, '2'), regexp_count(col1, '3'), len(col2), regexp_count(col2, 'true'), regexp_count(col1, 'false') FROM t;

-- LISTAGG with DISTINCT with implicit cast from non-string types (safe types - should succeed)
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col) FROM VALUES (1), (2), (2), (3) AS t(col);
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col) FROM VALUES (cast(1 as bigint)), (cast(2 as bigint)), (cast(2 as bigint)), (cast(3 as bigint)) AS t(col);
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col) FROM VALUES (cast(1 as smallint)), (cast(2 as smallint)), (cast(2 as smallint)), (cast(3 as smallint)) AS t(col);
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col) FROM VALUES (cast(1 as tinyint)), (cast(2 as tinyint)), (cast(2 as tinyint)), (cast(3 as tinyint)) AS t(col);
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col) FROM VALUES (cast(1.10 as decimal(10,2))), (cast(2.20 as decimal(10,2))), (cast(2.20 as decimal(10,2))) AS t(col);
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col) FROM VALUES (DATE'2024-01-01'), (DATE'2024-01-02'), (DATE'2024-01-01') AS t(col);
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col) FROM VALUES (TIMESTAMP_NTZ'2024-01-01 10:00:00'), (TIMESTAMP_NTZ'2024-01-02 12:00:00'), (TIMESTAMP_NTZ'2024-01-01 10:00:00') AS t(col);
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col) FROM VALUES (true), (false), (true) AS t(col);
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col) FROM VALUES (INTERVAL '1' MONTH), (INTERVAL '2' MONTH), (INTERVAL '1' MONTH) AS t(col);
SELECT listagg(DISTINCT cast(col as string), ',') WITHIN GROUP (ORDER BY col) FROM VALUES (10), (1), (2), (20), (2) AS t(col);
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col DESC) FROM VALUES (1), (10), (2), (20), (2) AS t(col);
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col) FROM VALUES (1), (2), (null), (2), (3) AS t(col);
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col NULLS FIRST) FROM VALUES (1), (null), (2), (null) AS t(col);
SELECT grp, listagg(DISTINCT col) WITHIN GROUP (ORDER BY col) FROM VALUES (1, 'a'), (1, 'b'), (2, 'a'), (2, 'a'), (1, 'b') AS t(grp, col) GROUP BY grp;
WITH t(col) AS (SELECT listagg(DISTINCT col1, X'2C') WITHIN GROUP (ORDER BY col1) FROM (VALUES (X'DEAD'), (X'BEEF'), (X'DEAD'), (X'CAFE'))) SELECT len(col), regexp_count(col, X'DEAD'), regexp_count(col, X'BEEF'), regexp_count(col, X'CAFE') FROM t;
WITH t(col) AS (SELECT listagg(DISTINCT col1, X'7C') WITHIN GROUP (ORDER BY col1) FROM (VALUES (X'BB'), (X'AA'), (NULL), (X'BB'))) SELECT len(col), regexp_count(col, X'AA'), regexp_count(col, X'BB') FROM t;
SELECT grp, hex(listagg(DISTINCT col, X'2C') WITHIN GROUP (ORDER BY col)) FROM VALUES (1, X'AA'), (1, X'BB'), (1, X'AA'), (2, X'CC'), (2, X'CC') AS t(grp, col) GROUP BY grp;

-- Error cases
SELECT listagg(c1) FROM (VALUES (ARRAY('a', 'b'))) AS t(c1);
SELECT listagg(c1, ', ') FROM (VALUES (X'DEAD'), (X'BEEF')) AS t(c1);
SELECT listagg(col2, col1) FROM df GROUP BY col1;
SELECT listagg(col1) OVER (ORDER BY col1) FROM df;
SELECT listagg(col1) WITHIN GROUP (ORDER BY col1) OVER (ORDER BY col1) FROM df;
SELECT string_agg(col1) WITHIN GROUP (ORDER BY col1) OVER (ORDER BY col1) FROM df;
SELECT listagg(DISTINCT col1) OVER (ORDER BY col1) FROM df;
SELECT listagg(DISTINCT col1) WITHIN GROUP (ORDER BY col2) FROM df;
SELECT listagg(DISTINCT col1) WITHIN GROUP (ORDER BY col1, col2) FROM df;
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col) FROM VALUES (cast(1.1 as double)), (cast(2.2 as double)), (cast(2.2 as double)), (cast(3.3 as double)) AS t(col);
SELECT listagg(DISTINCT col, ',') WITHIN GROUP (ORDER BY col) FROM VALUES (cast(1.0 as float)), (cast(2.0 as float)), (cast(2.0 as float)) AS t(col);
