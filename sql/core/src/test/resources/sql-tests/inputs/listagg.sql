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

-- Additional test cases are needed for distinct aggregations that include `ListAgg`.
-- These tests are used to verify that `RewriteDistinctAggregates` works correctly.

-- Test case for handling two distinct LISTAGG aggregations.
SELECT
  LISTAGG(DISTINCT col1) WITHIN GROUP (ORDER BY col1),
  LISTAGG(DISTINCT col2) WITHIN GROUP (ORDER BY col2)
FROM df;

-- Test case for handling two distinct LISTAGG aggregations with only one delimiter specified.
SELECT
  LISTAGG(DISTINCT col1) WITHIN GROUP (ORDER BY col1),
  LISTAGG(DISTINCT col2, '|') WITHIN GROUP (ORDER BY col2)
FROM df;

-- Test case for handling two distinct LISTAGG aggregations with different orderings.
SELECT
  LISTAGG(DISTINCT col1) WITHIN GROUP (ORDER BY col1),
  LISTAGG(DISTINCT col2) WITHIN GROUP (ORDER BY col2 DESC)
FROM df;

-- Test case for handling two distinct LISTAGG aggregations with different orderings and nulls.
SELECT
  LISTAGG(DISTINCT col1) WITHIN GROUP (ORDER BY col1),
  LISTAGG(DISTINCT col2) WITHIN GROUP (ORDER BY col2 DESC NULLS LAST)
FROM df;

-- Test case for handling two distinct LISTAGG aggregations with different orderings and nulls, with a filter.
SELECT
  LISTAGG(DISTINCT col1) WITHIN GROUP (ORDER BY col1),
  LISTAGG(DISTINCT col2) WITHIN GROUP (ORDER BY col2 DESC NULLS LAST)
FROM df WHERE col1 > 'a' AND col2 > 'b';

-- Test cases for handling distinct LISTAGG with other different aggregations (e.g. COUNT DISTINCT).
SELECT
  LISTAGG(DISTINCT col1) WITHIN GROUP (ORDER BY col1),
  COUNT(DISTINCT col2)
FROM df;

SELECT
  col1,
  LISTAGG(DISTINCT col2) WITHIN GROUP (ORDER BY col2 DESC),
  COUNT(DISTINCT col3)
FROM (VALUES ('A', 'x', '1'), ('A', 'y', '2'), ('B', 'y', '2'), ('B', 'z', '3')) AS tbl(col1, col2, col3)
GROUP BY col1;

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
