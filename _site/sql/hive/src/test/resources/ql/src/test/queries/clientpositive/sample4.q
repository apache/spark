CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

-- bucket column is the same as table sample
-- No need for sample filter
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest1 SELECT s.*
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2 on key) s;

INSERT OVERWRITE TABLE dest1 SELECT s.*
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2 on key) s;

SELECT dest1.* FROM dest1;
