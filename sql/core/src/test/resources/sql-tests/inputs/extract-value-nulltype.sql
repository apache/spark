-- Test ExtractValue behavior with NullType base expressions.

-- Struct field access on a NullType column
SELECT col.a FROM (SELECT null AS col) t;

-- Array index on a NullType column
SELECT col[0] FROM (SELECT null AS col) t;

-- Map key access on a NullType column
SELECT col['key'] FROM (SELECT null AS col) t;

-- HAVING with field access on NullType grouped column
SELECT NAMED_STRUCT('a', 1) AS col1
FROM VALUES (NULL)
GROUP BY col1
HAVING col1.a == 1;
