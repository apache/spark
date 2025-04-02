CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

-- no input pruning, sample filter
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest1 SELECT s.* -- here's another test
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 5 on key) s;

INSERT OVERWRITE TABLE dest1 SELECT s.* -- here's another test
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 5 on key) s;

SELECT dest1.* FROM dest1 SORT BY key, value;
