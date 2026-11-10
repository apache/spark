CREATE TABLE t1(col1 STRUCT<a: STRING>, a STRING);

-- Correctly fallback from ambiguous reference due to ExtractValue alias
SELECT col1.a, a FROM t1 ORDER BY a;
SELECT col1.a, a FROM t1 ORDER BY col1.a;

-- Indexing a result of a function that returns a complex type
SELECT split(col1, '-')[1] AS a FROM VALUES('a-b') ORDER BY split(col1, '-')[1];

-- Both child and extraction should be checked in ResolverGuard
CREATE TABLE t2(col1 ARRAY<INT>, col2 INT);
SELECT col1[get(filter(col1,k -> k == col2), col2)] FROM t2;
SELECT filter(col1, k -> k == col2)[0] FROM t2;
SELECT col1[element_at(filter(col1, k -> k > col2), 1)] FROM t2;
SELECT col1[col1[get(filter(col1, k -> k == col2), col2)]] FROM t2;
SELECT col2 FROM t2 WHERE col1[get(filter(col1, k -> k == col2), col2)] > 0;
SELECT col2 FROM t2 ORDER BY col1[get(filter(col1, k -> k == col2), col2)];
SELECT col2 FROM t2 GROUP BY col1, col2 HAVING col1[get(filter(col1, k -> k == col2), col2)] > 0;
CREATE TABLE t3(col1 MAP<INT, INT>, col2 INT);
SELECT col1[element_at(filter(map_keys(col1), k -> k == col2), 1)] FROM t3;
SELECT transform(map_values(col1), v -> v + col2)[0] FROM t3;
DROP TABLE t3;
DROP TABLE t2;

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
