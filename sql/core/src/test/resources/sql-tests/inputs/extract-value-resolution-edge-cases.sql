CREATE TABLE t1(col1 STRUCT<a: STRING>, a STRING);

-- Correctly fallback from ambiguous reference due to ExtractValue alias
SELECT col1.a, a FROM t1 ORDER BY a;
SELECT col1.a, a FROM t1 ORDER BY col1.a;

-- Indexing a result of a function that returns a complex type
SELECT split(col1, '-')[1] AS a FROM VALUES('a-b') ORDER BY split(col1, '-')[1];

DROP TABLE t1;
