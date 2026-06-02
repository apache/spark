CREATE TABLE t1(col1 STRUCT<a: STRING>, a STRING);

-- Correctly fallback from ambiguous reference due to ExtractValue alias
SELECT col1.a, a FROM t1 ORDER BY a;
SELECT col1.a, a FROM t1 ORDER BY col1.a;

-- Indexing a result of a function that returns a complex type
SELECT split(col1, '-')[1] AS a FROM VALUES('a-b') ORDER BY split(col1, '-')[1];

DROP TABLE t1;

-- SPARK-57186: dotted multipart field access on a NullType base returns NULL instead of throwing
-- INVALID_EXTRACT_BASE_FIELD_TYPE (NULL propagation; a NullType column can arise e.g. from schema
-- evolution with missing columns). The fix is scoped to multipart name resolution (`col.a`); the
-- `col['key']` and `col[0]` forms go through UnresolvedExtractValue and still throw, so the
-- shared ExtractValue utility and the single-pass resolver are unaffected.
SELECT col.a FROM (SELECT null AS col) t;
SELECT col[0] FROM (SELECT null AS col) t;
SELECT col['key'] FROM (SELECT null AS col) t;
