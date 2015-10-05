-- no input pruning, sample filter
EXPLAIN
SELECT s.key
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 5 on key) s;

SELECT s.key
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 5 on key) s SORT BY key;

