
-- limit on various data types
SELECT * FROM testdata LIMIT 2;
SELECT * FROM arraydata LIMIT 2;
SELECT * FROM mapdata LIMIT 2;

-- foldable non-literal in limit
SELECT * FROM testdata LIMIT 2 + 1;

SELECT * FROM testdata LIMIT CAST(1 AS int);

-- limit must be non-negative
SELECT * FROM testdata LIMIT -1;
SELECT * FROM testData TABLESAMPLE (-1 ROWS);


SELECT * FROM testdata LIMIT CAST(1 AS INT);
-- evaluated limit must not be null
SELECT * FROM testdata LIMIT CAST(NULL AS INT);

-- limit must be foldable
SELECT * FROM testdata LIMIT key > 3;

-- limit must be integer
SELECT * FROM testdata LIMIT true;
SELECT * FROM testdata LIMIT 'a';

-- limit within a subquery
SELECT * FROM (SELECT * FROM range(10) LIMIT 5) WHERE id > 3;

-- limit ALL
SELECT * FROM testdata WHERE key < 3 LIMIT ALL;
