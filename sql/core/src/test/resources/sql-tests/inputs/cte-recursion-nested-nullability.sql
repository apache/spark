--SET spark.sql.caseSensitive=false
--CONFIG_DIM1 spark.sql.ansi.enabled=true
--CONFIG_DIM1 spark.sql.ansi.enabled=false

-- Later iterations can introduce null array elements.
WITH RECURSIVE r(n, a, saw_null) AS (
  SELECT 0, array(1), false
  UNION ALL
  SELECT n + 1, array(CAST(NULL AS INT)), exists(a, x -> x IS NULL)
  FROM r WHERE n < 2
)
SELECT n, a, saw_null FROM r ORDER BY n;

-- Recursive struct widening keeps the anchor's field spelling and BIGINT type.
WITH RECURSIVE r(n, s, saw_null) AS (
  SELECT 0, named_struct('x', CAST(1 AS BIGINT)), false
  UNION ALL
  SELECT n + 1, named_struct('X', CAST(NULL AS INT)), s.x IS NULL
  FROM r WHERE n < 2
)
SELECT n, s, saw_null FROM r ORDER BY n;

-- Widening another column also preserves the anchor's same-width struct field spelling.
WITH RECURSIVE r(n, s) AS (
  SELECT CAST(0 AS BIGINT), named_struct('x', CAST(1 AS BIGINT))
  UNION ALL
  SELECT CAST(n + 1 AS INT), named_struct('X', CAST(NULL AS BIGINT))
  FROM r WHERE n < 1
)
SELECT n, s FROM r ORDER BY n;

-- Nested map values can become null even when the anchor's values are non-null.
WITH RECURSIVE r(n, m, saw_null) AS (
  SELECT 0, map('k', array(1)), false
  UNION ALL
  SELECT n + 1, map('k', array(CAST(NULL AS INT))),
    exists(m['k'], x -> x IS NULL)
  FROM r WHERE n < 2
)
SELECT n, m, saw_null FROM r ORDER BY n;

-- A map lambda must allow a later iteration's entire value to be null.
WITH RECURSIVE r(n, m, saw_null) AS (
  SELECT 0, map('k', array(1)), false
  UNION ALL
  SELECT n + 1, map('k', CAST(NULL AS ARRAY<INT>)),
    transform_values(m, (k, v) -> v IS NULL)['k']
  FROM r WHERE n < 2
)
SELECT n, m, saw_null FROM r ORDER BY n;

-- Map keys remain non-null, but fields inside struct keys can become null.
WITH RECURSIVE r(n, m, saw_null) AS (
  SELECT 0, map(named_struct('x', 1), 1), false
  UNION ALL
  SELECT n + 1, map(named_struct('x', CAST(NULL AS INT)), 1),
    exists(map_keys(m), k -> k.x IS NULL)
  FROM r WHERE n < 2
)
SELECT n, map_keys(m), saw_null FROM r ORDER BY n;

-- Nested nullability can widen, but recursive array element types cannot widen the anchor.
WITH RECURSIVE r(n, a) AS (
  SELECT 0, array(1)
  UNION ALL
  SELECT n + 1, array(CAST(2 AS BIGINT)) FROM r WHERE n < 1
)
SELECT n, a FROM r ORDER BY n;

-- Recursive struct field types also cannot widen the anchor.
WITH RECURSIVE r(n, s) AS (
  SELECT 0, named_struct('x', 1)
  UNION ALL
  SELECT n + 1, named_struct('x', CAST(2 AS BIGINT)) FROM r WHERE n < 1
)
SELECT n, s FROM r ORDER BY n;
