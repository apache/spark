-- input pruning, no sample filter
-- default table sample columns
INSERT OVERWRITE TABLE dest1 SELECT s.* 
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2) s
