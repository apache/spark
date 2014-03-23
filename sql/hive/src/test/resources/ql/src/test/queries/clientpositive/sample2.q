CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

-- input pruning, no sample filter
-- default table sample columns
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest1 SELECT s.* 
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2) s;

INSERT OVERWRITE TABLE dest1 SELECT s.* 
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2) s;

SELECT dest1.* FROM dest1;
