-- no input pruning, no sample filter
SELECT s.*
FROM srcpart TABLESAMPLE (BUCKET 1 OUT OF 1 ON rand()) s
WHERE s.ds='2008-04-08' and s.hr='11'

