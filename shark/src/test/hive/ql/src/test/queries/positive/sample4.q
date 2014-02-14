-- bucket column is the same as table sample
-- No need for sample filter
INSERT OVERWRITE TABLE dest1 SELECT s.*
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2 on key) s
