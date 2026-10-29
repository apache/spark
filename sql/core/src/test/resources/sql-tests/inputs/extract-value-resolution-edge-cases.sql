CREATE TABLE t1(col1 STRUCT<a: STRING>, a STRING);

-- Correctly fallback from ambiguous reference due to ExtractValue alias
SELECT col1.a, a FROM t1 ORDER BY a;
SELECT col1.a, a FROM t1 ORDER BY col1.a;

-- Indexing a result of a function that returns a complex type
SELECT split(col1, '-')[1] AS a FROM VALUES('a-b') ORDER BY split(col1, '-')[1];

DROP TABLE t1;

-- SPARK-57186: extracting a field/element/key from a NullType base returns NULL instead of
-- throwing INVALID_EXTRACT_BASE_FIELD_TYPE (SQL NULL propagation; a NullType column can arise e.g.
-- from schema evolution with missing columns). This applies uniformly to dotted field access
-- (`col.a`) and the subscript forms (`col[0]`, `col['key']`), and is implemented at the
-- user-facing resolution sites (ExtractValue.applyOrNull) without changing the shared
-- ExtractValue.extractValue utility.
SELECT col.a FROM (SELECT null AS col) t;
SELECT col[0] FROM (SELECT null AS col) t;
SELECT col['key'] FROM (SELECT null AS col) t;
