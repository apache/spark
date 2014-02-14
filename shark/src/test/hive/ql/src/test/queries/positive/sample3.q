-- sample columns not same as bucket columns
-- no input pruning, sample filter
INSERT OVERWRITE TABLE dest1 SELECT s.* -- here's another test
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2 on key, value) s
