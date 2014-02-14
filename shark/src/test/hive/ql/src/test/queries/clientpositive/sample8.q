-- sampling with join and alias
EXPLAIN EXTENDED
SELECT s.*
FROM srcpart TABLESAMPLE (BUCKET 1 OUT OF 1 ON key) s
JOIN srcpart TABLESAMPLE (BUCKET 1 OUT OF 10 ON key) t
WHERE t.key = s.key and t.value = s.value and s.ds='2008-04-08' and s.hr='11' and s.ds='2008-04-08' and s.hr='11'
DISTRIBUTE BY key, value
SORT BY key, value;

SELECT s.key, s.value
FROM srcpart TABLESAMPLE (BUCKET 1 OUT OF 1 ON key) s
JOIN srcpart TABLESAMPLE (BUCKET 1 OUT OF 10 ON key) t
WHERE s.ds='2008-04-08' and s.hr='11' and s.ds='2008-04-08' and s.hr='11'
DISTRIBUTE BY key, value
SORT BY key, value;
