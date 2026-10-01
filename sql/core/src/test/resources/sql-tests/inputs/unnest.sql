-- Tests for the ANSI SQL UNNEST collection derived table in the FROM clause.

CREATE OR REPLACE TEMPORARY VIEW nested AS SELECT * FROM VALUES
(1, array(10, 20, 30), array('a', 'b')),
(2, array(40), array('c', 'd', 'e')),
(3, array(), array()),
(4, cast(null as array<int>), array('f'))
AS nested(id, xs, ys);

-- Single array: one row per element, default column name `col`.
SELECT * FROM UNNEST(array(10, 20, 30));

-- Single array with a table and column alias.
SELECT v FROM UNNEST(array(10, 20, 30)) AS t(v);

-- Empty array produces no rows.
SELECT * FROM UNNEST(array());

-- NULL array is treated as empty and produces no rows.
SELECT * FROM UNNEST(cast(null as array<int>));

-- WITH ORDINALITY appends a 1-based BIGINT position column.
SELECT * FROM UNNEST(array(10, 20, 30)) WITH ORDINALITY;

-- WITH ORDINALITY with column aliases.
SELECT val, pos FROM UNNEST(array('x', 'y')) WITH ORDINALITY AS t(val, pos);

-- Multiple arrays are expanded in parallel and padded with NULLs to the longest length.
SELECT * FROM UNNEST(array(1, 2), array(10, 20, 30)) AS t(a, b);

-- Multiple arrays with WITH ORDINALITY.
SELECT * FROM UNNEST(array(1, 2), array(10, 20, 30)) WITH ORDINALITY AS t(a, b, ord);

-- An array of structs keeps the struct as a single column (unlike inline).
SELECT * FROM UNNEST(array(struct(1, 'a'), struct(2, 'b'))) AS t(s);

-- Correlated UNNEST over a table column, via LATERAL.
SELECT id, elem FROM nested, LATERAL UNNEST(xs) AS t(elem) ORDER BY id, elem;

-- Correlated UNNEST of two arrays with ordinality, via LATERAL.
SELECT id, x, y, ord
FROM nested, LATERAL UNNEST(xs, ys) WITH ORDINALITY AS t(x, y, ord)
ORDER BY id, ord;

-- LEFT JOIN LATERAL preserves outer rows when the array is empty or NULL.
SELECT id, elem
FROM nested LEFT JOIN LATERAL UNNEST(xs) AS t(elem) ON true
ORDER BY id, elem;

-- Nested arrays: the element type is preserved as-is (a single array-typed column).
SELECT * FROM UNNEST(array(array(1, 2), array(3))) AS t(inner);

-- Array elements that are themselves NULL are emitted as NULL rows (distinct from a NULL array).
SELECT * FROM UNNEST(array(1, cast(null as int), 3)) WITH ORDINALITY;

-- The first array being shorter still pads it (not just trailing arrays).
SELECT * FROM UNNEST(array(1), array(10, 20, 30)) AS t(a, b);

-- Non-array argument is rejected.
SELECT * FROM UNNEST(42);

-- A MAP argument is rejected (unlike explode, UNNEST is array-only per the SQL standard).
SELECT * FROM UNNEST(map('a', 1));

-- A mix of array and non-array arguments is rejected.
SELECT * FROM UNNEST(array(1, 2), 3);

-- UNNEST is a non-reserved keyword: a table-valued function named `unnest` can still be invoked
-- by quoting the name, which bypasses the dedicated UNNEST relation syntax. Here it resolves as a
-- generic (unregistered) TVF and fails at analysis, proving the name is not swallowed by the
-- grammar.
SELECT * FROM `unnest`(array(1, 2));
